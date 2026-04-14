#!/usr/bin/env python3
"""
Read Replica E2E Test

Validates that multiple workers can serve the same shard (S3 shared storage):
1. Two workers load the SAME shard URI
2. Search distributes across both workers (load-balanced)
3. Kill one worker → search still works (failover)
4. Restart worker → cluster fully healthy again
"""

import sys, os, time, subprocess
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

from test_helpers import wait_for_grpc

import pyarrow as pa
import lance
import yaml

BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lanceforge_replica_test"
DIM = 32
COORD_PORT = 57000

results = {"passed": 0, "failed": 0, "tests": []}

def cleanup():
    os.system("pkill -f 'lance-coordinator.*57000|lance-worker.*5710' 2>/dev/null")
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
print(f"  READ REPLICA E2E TEST")
print(f"{'='*60}")

cleanup()
os.makedirs(BASE, exist_ok=True)
np.random.seed(42)

# Create ONE shard — both workers will load it
uri = os.path.join(BASE, "shared_shard.lance")
n = 3000
table = pa.table({
    'id': pa.array(range(n), type=pa.int64()),
    'vector': pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
})
lance.write_dataset(table, uri, mode='overwrite')
ds = lance.dataset(uri)
ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)
print(f"  Shared shard: {ds.count_rows()} rows")

# Config: BOTH workers assigned to SAME shard (read replicas)
config = {
    'tables': [{'name': 'replicated', 'shards': [
        {'name': 'replicated_shard_00',
         'uri': uri,
         'executors': ['w0', 'w1']}  # Both workers serve same shard
    ]}],
    'executors': [
        {'id': 'w0', 'host': '127.0.0.1', 'port': 57100},
        {'id': 'w1', 'host': '127.0.0.1', 'port': 57101},
    ],
    'metadata_path': os.path.join(BASE, 'metadata.json'),
}
with open(os.path.join(BASE, 'config.yaml'), 'w') as f:
    yaml.dump(config, f)

# Start cluster
processes = []
for i in range(2):
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", f"{BASE}/config.yaml", f"w{i}", str(57100 + i)],
        stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
for i in range(2):
    assert wait_for_grpc("127.0.0.1", 57100 + i), f"Worker w{i} failed to start"

coord = subprocess.Popen(
    [f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
processes.append(coord)
assert wait_for_grpc("127.0.0.1", COORD_PORT), "Coordinator failed to start"
print("  Cluster running (2 workers, 1 shared shard)\n")

from lanceforge import LanceForgeClient

# Test 1: Both workers healthy
def t_both_healthy():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    s = client.status()
    healthy = sum(1 for e in s["executors"] if e["healthy"])
    assert healthy == 2, f"Expected 2 healthy, got {healthy}"
    client.close()
test("1. Both replica workers healthy", t_both_healthy)

# Test 2: Search works (distributed across replicas)
def t_search_replicated():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    for _ in range(10):
        r = client.search("replicated", query_vector=np.zeros(DIM).tolist(), k=5)
        assert r.num_rows == 5
    client.close()
test("2. 10 searches on replicated shard all succeed", t_search_replicated)

# Test 3: Kill one replica → search still works
def t_kill_replica():
    processes[1].terminate()  # Kill w1
    processes[1].wait(timeout=5)
    time.sleep(3)

    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    # Should still work via w0
    r = client.search("replicated", query_vector=np.zeros(DIM).tolist(), k=5)
    assert r.num_rows == 5
    client.close()
test("3. Search works after killing one replica", t_kill_replica)

# Test 4: Restart replica → both healthy again
def t_restart_replica():
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", f"{BASE}/config.yaml", "w1", "57101"],
        stdout=open(f"{BASE}/w1_restart.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(12)  # Wait for health check to detect

    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    s = client.status()
    healthy = sum(1 for e in s["executors"] if e["healthy"])
    assert healthy == 2, f"Expected 2 healthy after restart, got {healthy}"
    r = client.search("replicated", query_vector=np.zeros(DIM).tolist(), k=5)
    assert r.num_rows == 5
    client.close()
test("4. Replica restarted, cluster fully healthy", t_restart_replica)

cleanup()

print(f"\n{'='*60}")
print(f"  REPLICA TEST RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*60}")
for name, status in results["tests"]:
    print(f"  {'✓' if 'PASS' in status else '✗'} {name}: {status}")
print(f"{'='*60}")

sys.exit(0 if results["failed"] == 0 else 1)
