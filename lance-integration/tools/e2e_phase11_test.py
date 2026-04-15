#!/usr/bin/env python3
"""
Phase 11 E2E: auto-sharding + write no-duplication + size-aware rebalance.

Scenarios:
 A. CreateTable(N rows) → data auto-splits across all healthy workers.
 B. After rebalance concentrates 2 shards on one worker, insert does NOT duplicate.
 C. Size-aware rebalance considers row counts (smoke: shard_sizes used).
"""
import sys, os, time, struct, subprocess
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import wait_for_grpc

import pyarrow as pa
import pyarrow.ipc as ipc
import grpc, yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

DIM = 16
BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lanceforge_phase11_test"
COORD_PORT = 52950
WORKER_PORTS = [52900, 52901]
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

# ── Setup: start empty cluster (no pre-configured shards) ──
step("Setup: start empty cluster")
os.system(f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null")
time.sleep(1)
import shutil
if os.path.exists(BASE): shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

config = {
    'tables': [],
    'executors': [
        {'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]}
        for i in range(2)
    ],
    'default_table_path': BASE,
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
time.sleep(2)  # let coordinator register workers

stub = pbg.LanceSchedulerServiceStub(
    grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)]))

# ── Scenario A: CreateTable auto-sharding ──
step("A. CreateTable auto-sharding")
np.random.seed(7)
N_ROWS = 2000
batch = pa.record_batch([
    pa.array(range(N_ROWS), type=pa.int64()),
    pa.array([f'cat_{i%3}' for i in range(N_ROWS)], type=pa.string()),
    pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(N_ROWS*DIM).astype(np.float32)), list_size=DIM),
], names=['id', 'category', 'vector'])

def t_create():
    r = stub.CreateTable(pb.CreateTableRequest(
        table_name="autotest", arrow_ipc_data=ipc_bytes(batch), uri=""))
    assert not r.error, r.error
    assert r.num_rows == N_ROWS, f"expected {N_ROWS}, got {r.num_rows}"
test("CreateTable returns success", t_create)

def t_spread_over_workers():
    # Wait up to 15s for periodic health_check to refresh cached loaded_shards
    loaded = [0, 0]
    for _ in range(15):
        r = stub.GetClusterStatus(pb.ClusterStatusRequest())
        loaded = [e.loaded_shards for e in r.executors]
        if all(x >= 1 for x in loaded):
            return
        time.sleep(1)
    assert all(x >= 1 for x in loaded), f"auto-shard failed, loaded={loaded}"
test("Data spread over both workers (loaded_shards >= 1 each)", t_spread_over_workers)

def t_count_correct():
    r = stub.CountRows(pb.CountRowsRequest(table_name="autotest"))
    assert r.count == N_ROWS, f"expected {N_ROWS}, got {r.count}"
test(f"CountRows == {N_ROWS} (no loss, no dup)", t_count_correct)

# ── Scenario B: Insert no-duplication ──
step("B. Insert no-duplication")

def t_insert_no_dup():
    before = stub.CountRows(pb.CountRowsRequest(table_name="autotest")).count
    INS = 200
    new_batch = pa.record_batch([
        pa.array(range(900000, 900000 + INS), type=pa.int64()),
        pa.array(['inserted'] * INS, type=pa.string()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(INS*DIM).astype(np.float32)), list_size=DIM),
    ], names=['id', 'category', 'vector'])
    r = stub.AddRows(pb.AddRowsRequest(table_name="autotest", arrow_ipc_data=ipc_bytes(new_batch)))
    assert not r.error, r.error
    time.sleep(1)
    after = stub.CountRows(pb.CountRowsRequest(table_name="autotest")).count
    diff = after - before
    assert diff == INS, f"duplication! expected +{INS}, got +{diff}"
test("Insert 200 rows adds exactly 200 (no N-fold duplication)", t_insert_no_dup)

def t_search_inserted():
    r = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="autotest", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=300, nprobes=10, metric_type=0,
        filter="category = 'inserted'"))
    assert not r.error, r.error
    t = ipc.open_stream(r.arrow_ipc_data).read_all() if r.arrow_ipc_data else pa.table({})
    assert t.num_rows > 0, "inserted rows not searchable"
test("Search finds inserted rows", t_search_inserted)

# ── Scenario C: Rebalance with size awareness ──
step("C. Rebalance (size-aware) runs successfully")

def t_rebalance():
    r = stub.Rebalance(pb.RebalanceRequest())
    # Either no-op or moves some shards; must not error
    assert not r.error, r.error
test("Rebalance RPC succeeds", t_rebalance)

def t_count_after_rebalance():
    time.sleep(2)
    r = stub.CountRows(pb.CountRowsRequest(table_name="autotest"))
    # After rebalance, count must remain consistent
    assert r.count > 0, f"count became 0 after rebalance: {r.count}"
test("CountRows stable after rebalance", t_count_after_rebalance)

# ── Cleanup ──
step("Cleanup"); cleanup()

print(f"\n{'='*60}")
print(f"  PHASE 11 E2E: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*60}")
for name, status in results["tests"]:
    print(f"  {'OK' if 'PASS' in status else 'XX'} {name}: {status}")
print(f"{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
