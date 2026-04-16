#!/usr/bin/env python3
"""
Phase 14 E2E: MoveShard admin RPC + slow-query log + per-table Prometheus metrics.
"""
import sys, os, time, struct, subprocess, urllib.request
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import wait_for_grpc

import pyarrow as pa
import pyarrow.ipc as ipc
import grpc, yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

DIM = 8
BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = "/tmp/lanceforge_phase14_test"
COORD_PORT = 55950
REST_PORT = COORD_PORT + 1
WORKER_PORTS = [55900, 55901]
processes = []
results = {"passed": 0, "failed": 0, "tests": []}

def cleanup():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)

def step(m): print(f"\n{'='*60}\n  {m}\n{'='*60}")

def test(name, fn):
    print(f"  {name}...", end=" ", flush=True)
    try:
        fn(); print("PASS"); results["passed"] += 1
        results["tests"].append((name, "PASS"))
    except Exception as e:
        print(f"FAIL: {e}"); results["failed"] += 1
        results["tests"].append((name, f"FAIL: {e}"))

def ev(v): return struct.pack(f"<{len(v)}f", *v)

def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema); w.write_batch(batch); w.close()
    return sink.getvalue().to_pybytes()

# ── Setup ──
step("Setup: cluster with slow_query_ms=1 + REST metrics")
os.system(f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null")
time.sleep(1)
import shutil
if os.path.exists(BASE): shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

config = {
    'tables': [],
    'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]} for i in range(2)],
    'default_table_path': BASE,
    'server': {'slow_query_ms': 1},  # any query should trigger
}
cfg = os.path.join(BASE, 'config.yaml')
with open(cfg, 'w') as f: yaml.dump(config, f)

for i in range(2):
    p = subprocess.Popen([f"{BIN}/lance-worker", cfg, f"w{i}", str(WORKER_PORTS[i])],
        stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
for i in range(2):
    assert wait_for_grpc("127.0.0.1", WORKER_PORTS[i]), f"w{i} failed"

p = subprocess.Popen([f"{BIN}/lance-coordinator", cfg, str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
processes.append(p)
assert wait_for_grpc("127.0.0.1", COORD_PORT), "coord failed"
time.sleep(2)

stub = pbg.LanceSchedulerServiceStub(
    grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)]))

# Create a table and warm up
np.random.seed(7)
N = 2000
batch = pa.record_batch([
    pa.array(range(N), type=pa.int64()),
    pa.array([f'c{i%3}' for i in range(N)], type=pa.string()),
    pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(N*DIM).astype(np.float32)), list_size=DIM),
], names=['id', 'category', 'vector'])
r = stub.CreateTable(pb.CreateTableRequest(table_name="p14", arrow_ipc_data=ipc_bytes(batch)))
assert not r.error, r.error

# ── Scenario A: MoveShard ──
step("A. MoveShard admin RPC")

def t_find_shard():
    loaded = [(None, 0)]
    for _ in range(15):
        status = stub.GetClusterStatus(pb.ClusterStatusRequest())
        loaded = [(e.executor_id, e.loaded_shards) for e in status.executors]
        if all(s > 0 for _, s in loaded): return
        time.sleep(1)
    raise AssertionError(f"not all workers loaded: {loaded}")
test("Each worker holds at least 1 shard before move", t_find_shard)

def t_move_shard():
    # Move shard p14_shard_00 (currently on w0) to w1
    r = stub.MoveShard(pb.MoveShardRequest(
        shard_name="p14_shard_00", to_worker="w1", force=False))
    assert not r.error, r.error
    assert r.to_worker == "w1"
test("MoveShard p14_shard_00 → w1 succeeds", t_move_shard)

def t_count_still_correct():
    time.sleep(2)
    r = stub.CountRows(pb.CountRowsRequest(table_name="p14"))
    assert r.count == N, f"expected {N}, got {r.count}"
test("CountRows unchanged after move (no loss/dup)", t_count_still_correct)

def t_search_still_works():
    r = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="p14", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=10, nprobes=10, metric_type=0))
    assert not r.error, r.error
    assert r.num_rows == 10
test("Search still returns 10 rows after move", t_search_still_works)

def t_move_missing_shard():
    try:
        stub.MoveShard(pb.MoveShardRequest(shard_name="nope_shard", to_worker="w1"))
        raise AssertionError("expected NOT_FOUND")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND
test("MoveShard unknown shard → NOT_FOUND", t_move_missing_shard)

# ── Scenario B: slow-query log ──
step("B. Slow-query log emission")

def t_slow_logged():
    # Issue a few queries; slow_query_ms=1 means almost all should trigger
    for _ in range(3):
        stub.AnnSearch(pb.AnnSearchRequest(
            table_name="p14", vector_column="vector",
            query_vector=ev(np.zeros(DIM, dtype=np.float32)),
            dimension=DIM, k=5, nprobes=10, metric_type=0))
    time.sleep(1)
    with open(os.path.join(BASE, "coord.log")) as f: content = f.read()
    assert "slow_query op=ann_search" in content, \
        "slow_query log missing"
    assert "table=p14" in content
test("slow_query warn log emitted for ann_search", t_slow_logged)

# ── Scenario C: per-table metrics ──
step("C. Per-table Prometheus metrics")

def t_metrics_endpoint():
    body = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/metrics", timeout=5).read().decode()
    assert 'lance_table_query_total{table="p14"}' in body, \
        f"missing per-table metric: {body[:400]}"
    assert "lance_slow_query_total" in body
test("/metrics contains per-table and slow-query counters", t_metrics_endpoint)

def t_metrics_counts():
    body = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/metrics", timeout=5).read().decode()
    import re
    m = re.search(r'lance_table_query_total\{table="p14"\}\s+(\d+)', body)
    assert m and int(m.group(1)) >= 4, f"expected >=4 queries on p14, got {m.group(1) if m else '?'}"
test("Per-table query count reflects issued searches", t_metrics_counts)

# ── Cleanup ──
step("Cleanup"); cleanup()

print(f"\n{'='*60}")
print(f"  PHASE 14 E2E: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*60}")
for name, status in results["tests"]:
    print(f"  {'OK' if 'PASS' in status else 'XX'} {name}: {status}")
print(f"{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
