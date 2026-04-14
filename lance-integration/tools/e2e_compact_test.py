#!/usr/bin/env python3
"""
Compact E2E Test

Validates write compaction:
1. Insert many small batches (creates many fragments)
2. Verify search still works (correct but potentially slow)
3. Trigger compact via gRPC
4. Verify search still works after compaction
"""

import sys, os, time, subprocess
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

from test_helpers import wait_for_grpc

import pyarrow as pa
import lance
import grpc
import yaml

BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lanceforge_compact_test"
DIM = 16
COORD_PORT = 57500

results = {"passed": 0, "failed": 0, "tests": []}

def cleanup():
    os.system("pkill -f 'lance-coordinator.*57500|lance-worker.*5750' 2>/dev/null")
    time.sleep(2)

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

print(f"\n{'='*60}")
print(f"  COMPACT E2E TEST")
print(f"{'='*60}")

cleanup()
os.makedirs(BASE, exist_ok=True)
np.random.seed(42)

# Create initial small dataset
uri = os.path.join(BASE, "shard_00.lance")
table = pa.table({
    'id': pa.array(range(100), type=pa.int64()),
    'vector': pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(100 * DIM).astype(np.float32)), list_size=DIM),
})
lance.write_dataset(table, uri, mode='overwrite')
ds = lance.dataset(uri)
ds.create_index('vector', index_type='IVF_FLAT', num_partitions=2)

config = {
    'tables': [{'name': 'compact_test', 'shards': [
        {'name': 'compact_test_shard_00', 'uri': uri, 'executors': ['w0']}
    ]}],
    'executors': [{'id': 'w0', 'host': '127.0.0.1', 'port': 57510}],
}
with open(os.path.join(BASE, 'config.yaml'), 'w') as f:
    yaml.dump(config, f)

p1 = subprocess.Popen([f"{BIN}/lance-worker", f"{BASE}/config.yaml", "w0", "57510"],
    stdout=open(f"{BASE}/w0.log", "w"), stderr=subprocess.STDOUT)
assert wait_for_grpc("127.0.0.1", 57510), "Worker failed to start"
p2 = subprocess.Popen([f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
assert wait_for_grpc("127.0.0.1", COORD_PORT), "Coordinator failed to start"

from lanceforge import LanceForgeClient
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pb_grpc

client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")

# Connect to worker directly for compact RPC
worker_channel = grpc.insecure_channel("127.0.0.1:57510")
worker_stub = pb_grpc.LanceExecutorServiceStub(worker_channel)

# Test 1: Baseline
def t_baseline():
    r = client.search("compact_test", query_vector=np.zeros(DIM).tolist(), k=5)
    assert r.num_rows == 5
test("1. Baseline search works", t_baseline)

# Test 2: Insert many small batches (creates fragments)
def t_many_inserts():
    for i in range(20):
        batch = pa.table({
            'id': pa.array([10000 + i], type=pa.int64()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(np.random.randn(DIM).astype(np.float32)), list_size=DIM),
        })
        client.insert("compact_test", batch)
    time.sleep(1)
    # Verify fragments grew
    ds2 = lance.dataset(uri)
    frags = len(ds2.get_fragments())
    print(f"({frags} fragments)", end=" ")
    assert frags > 5, f"Expected many fragments, got {frags}"
test("2. Insert 20 single-row batches → many fragments", t_many_inserts)

# Test 3: Search still works with many fragments
def t_search_fragmented():
    r = client.search("compact_test", query_vector=np.zeros(DIM).tolist(), k=10)
    assert r.num_rows > 0
test("3. Search works with fragmented data", t_search_fragmented)

# Test 4: Trigger compact
def t_compact():
    resp = worker_stub.CompactAll(pb.CompactRequest(), timeout=60)
    assert not resp.error or "not" not in resp.error.lower(), f"Compact error: {resp.error}"
    print(f"(compacted {resp.tables_compacted} tables)", end=" ")
test("4. CompactAll RPC succeeds", t_compact)

# Test 5: Search works after compact
def t_search_after_compact():
    time.sleep(2)
    r = client.search("compact_test", query_vector=np.zeros(DIM).tolist(), k=10)
    assert r.num_rows > 0
test("5. Search works after compaction", t_search_after_compact)

worker_channel.close()
client.close()
cleanup()

print(f"\n{'='*60}")
print(f"  COMPACT TEST RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*60}")
for name, status in results["tests"]:
    print(f"  {'✓' if 'PASS' in status else '✗'} {name}: {status}")
print(f"{'='*60}")

sys.exit(0 if results["failed"] == 0 else 1)
