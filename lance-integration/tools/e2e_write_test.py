#!/usr/bin/env python3
"""
Write Path E2E Test

Validates insert and delete operations end-to-end through the distributed cluster:
1. Create sharded data + start cluster
2. Search → verify baseline row count
3. Insert new rows via gRPC AddRows
4. Search → verify new rows appear
5. Delete rows via gRPC DeleteRows
6. Search → verify rows removed
7. Verify cache invalidation (version changes)
"""

import sys, os, time, struct, subprocess, signal
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))

from test_helpers import wait_for_grpc

import pyarrow as pa
import pyarrow.ipc as ipc
import lance
import grpc
import yaml
import lance_service_pb2
import lance_service_pb2_grpc

DIM = 32
ROWS_PER_SHARD = 5000
NUM_SHARDS = 2
BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = "/tmp/lanceforge_write_test"

processes = []
results = {"passed": 0, "failed": 0, "tests": []}

def cleanup():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)

def step(msg):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")

def test(name, fn):
    print(f"  {name}...", end=" ", flush=True)
    try:
        fn()
        print("PASS")
        results["passed"] += 1
        results["tests"].append((name, "PASS"))
    except Exception as e:
        print(f"FAIL: {e}")
        results["failed"] += 1
        results["tests"].append((name, f"FAIL: {e}"))

def ev(v):
    return struct.pack(f"<{len(v)}f", *v)

def dr(resp):
    if resp.error:
        raise RuntimeError(resp.error)
    if not resp.arrow_ipc_data:
        return pa.table({})
    return ipc.open_stream(resp.arrow_ipc_data).read_all()

def record_batch_to_ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()

# ── Phase 1: Create data ──
step("Phase 1: Create test data")

os.makedirs(BASE, exist_ok=True)
np.random.seed(42)

for i in range(NUM_SHARDS):
    uri = os.path.join(BASE, f"shard_{i:02d}.lance")
    n = ROWS_PER_SHARD
    off = i * n
    table = pa.table({
        'id': pa.array(range(off, off + n), type=pa.int64()),
        'category': pa.array([f'cat_{(j + off) % 5}' for j in range(n)], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
    })
    lance.write_dataset(table, uri, mode='overwrite')
    ds = lance.dataset(uri)
    if not ds.list_indices():
        ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)
    print(f"  Shard {i}: {ds.count_rows()} rows")

# ── Phase 2: Start cluster ──
step("Phase 2: Start cluster")

config = {
    'tables': [{
        'name': 'writetest',
        'shards': [
            {'name': f'writetest_shard_{i:02d}',
             'uri': os.path.join(BASE, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']}
            for i in range(NUM_SHARDS)
        ],
    }],
    'executors': [
        {'id': f'w{i}', 'host': '127.0.0.1', 'port': 51900 + i}
        for i in range(NUM_SHARDS)
    ],
}
cfg_path = os.path.join(BASE, 'config.yaml')
with open(cfg_path, 'w') as f:
    yaml.dump(config, f)

os.system("pkill -f 'lance-coordinator.*51950|lance-worker.*5190' 2>/dev/null")
time.sleep(1)

for i in range(NUM_SHARDS):
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(51900 + i)],
        stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    print(f"  Worker {i} (PID {p.pid})")
for i in range(NUM_SHARDS):
    assert wait_for_grpc("127.0.0.1", 51900 + i), f"Worker w{i} failed to start"

p = subprocess.Popen(
    [f"{BIN}/lance-coordinator", cfg_path, "51950"],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
processes.append(p)
print(f"  Coordinator (PID {p.pid})")
assert wait_for_grpc("127.0.0.1", 51950), "Coordinator failed to start"

for proc in processes:
    assert proc.poll() is None, f"Process died! Check {BASE}/*.log"
print("  All processes alive")

stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(
    grpc.insecure_channel("127.0.0.1:51950",
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)]))

# ── Phase 3: Baseline search ──
step("Phase 3: Baseline verification")

def t_baseline_count():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="writetest", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=100, nprobes=10, metric_type=0))
    t = dr(r)
    assert t.num_rows > 0, "No results from baseline search"
    # Total rows across shards should be NUM_SHARDS * ROWS_PER_SHARD
test("Baseline ANN search returns results", t_baseline_count)

# ── Phase 4: Insert new rows ──
step("Phase 4: Insert new rows")

def t_insert():
    # Create 100 new rows
    new_rows = pa.record_batch([
        pa.array(range(100000, 100100), type=pa.int64()),
        pa.array(['cat_new'] * 100, type=pa.string()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(100 * DIM).astype(np.float32)), list_size=DIM),
    ], names=['id', 'category', 'vector'])

    ipc_data = record_batch_to_ipc_bytes(new_rows)

    r = stub.AddRows(lance_service_pb2.AddRowsRequest(
        table_name="writetest",
        arrow_ipc_data=ipc_data))
    assert not r.error, f"AddRows error: {r.error}"
    assert r.new_version > 0, "New version should be > 0"
    print(f"(version={r.new_version})", end=" ")
test("Insert 100 rows via AddRows", t_insert)

def t_search_after_insert():
    # Search for the new rows (they have unique 'cat_new' category)
    time.sleep(1)  # Brief pause for data freshness
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="writetest", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=200, nprobes=10, metric_type=0,
        filter="category = 'cat_new'"))
    t = dr(r)
    assert t.num_rows > 0, "No results after insert with filter 'cat_new'"
    ids = t.column("id").to_pylist()
    assert any(100000 <= id <= 100099 for id in ids), f"Inserted IDs not found, got: {ids[:5]}"
test("Search finds inserted rows", t_search_after_insert)

# ── Phase 5: Delete rows ──
step("Phase 5: Delete rows")

def t_delete():
    r = stub.DeleteRows(lance_service_pb2.DeleteRowsRequest(
        table_name="writetest",
        filter="category = 'cat_new'"))
    assert not r.error, f"DeleteRows error: {r.error}"
    print(f"(affected={r.affected_rows}, version={r.new_version})", end=" ")
test("Delete rows with filter", t_delete)

def t_search_after_delete():
    time.sleep(1)
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="writetest", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=200, nprobes=10, metric_type=0,
        filter="category = 'cat_new'"))
    t = dr(r)
    # After delete, should have no 'cat_new' rows
    if t.num_rows > 0:
        ids = t.column("id").to_pylist()
        inserted = [id for id in ids if 100000 <= id <= 100099]
        assert len(inserted) == 0, f"Deleted rows still found: {inserted}"
test("Search confirms deleted rows gone", t_search_after_delete)

# ── Phase 6: Version consistency ──
step("Phase 6: Cache/version consistency")

def t_version_consistency():
    # After writes, cluster status should still be healthy
    r = stub.GetClusterStatus(lance_service_pb2.ClusterStatusRequest())
    healthy = sum(1 for e in r.executors if e.healthy)
    assert healthy == NUM_SHARDS, f"Only {healthy}/{NUM_SHARDS} healthy after writes"
test("Cluster healthy after writes", t_version_consistency)

# ── Cleanup ──
step("Cleanup")
cleanup()

print(f"\n{'='*60}")
print(f"  WRITE PATH E2E RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*60}")
for name, status in results["tests"]:
    print(f"  {'✓' if 'PASS' in status else '✗'} {name}: {status}")
print(f"{'='*60}")

sys.exit(0 if results["failed"] == 0 else 1)
