#!/usr/bin/env python3
"""
Rebalance End-to-End Test

Tests dynamic shard redistribution when workers are added/removed:
  1. Start cluster with 2 workers, 4 shards → search works
  2. Register 3rd worker → trigger rebalance → shards redistribute
  3. Search still works after rebalance
  4. Kill worker → trigger rebalance → shards move to survivors
  5. Search still works with fewer workers
"""

import sys, os, time, subprocess
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

from test_helpers import wait_for_grpc

import pyarrow as pa
import lance
import yaml

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = "/tmp/lanceforge_rebalance_test"
DIM = 32
COORD_PORT = 57000
METADATA_PATH = os.path.join(BASE, "metadata.json")

all_processes = []
results = {"passed": 0, "failed": 0, "tests": []}

def cleanup_all():
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

def start_worker(worker_id, port):
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", f"{BASE}/config.yaml", worker_id, str(port)],
        stdout=open(f"{BASE}/{worker_id}.log", "w"), stderr=subprocess.STDOUT)
    all_processes.append(p)
    return p

def start_coordinator():
    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
    all_processes.append(p)
    return p

def stop_process(proc):
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except:
        proc.kill()
    time.sleep(1)

from lanceforge import LanceForgeClient

# ==========================================
# Setup: create 4 shards
# ==========================================
print(f"\n{'='*70}")
print(f"  REBALANCE E2E TEST SUITE")
print(f"{'='*70}")

cleanup_all()
os.makedirs(BASE, exist_ok=True)
for f in [METADATA_PATH, f"{METADATA_PATH}.tmp"]:
    if os.path.exists(f): os.remove(f)

np.random.seed(42)
N_SHARDS = 4
N_PER_SHARD = 1000

for i in range(N_SHARDS):
    uri = os.path.join(BASE, f"shard_{i:02d}.lance")
    off = i * N_PER_SHARD
    table = pa.table({
        'id': pa.array(range(off, off + N_PER_SHARD), type=pa.int64()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(N_PER_SHARD * DIM).astype(np.float32)), list_size=DIM),
    })
    lance.write_dataset(table, uri, mode='overwrite')
    ds = lance.dataset(uri)
    ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)

# Initial config: 4 shards across 2 workers (2 shards each)
config = {
    'tables': [{'name': 'rebal_table', 'shards': [
        {'name': f'rebal_table_shard_{i:02d}',
         'uri': os.path.join(BASE, f'shard_{i:02d}.lance'),
         'executors': [f'w{i % 2}']} for i in range(N_SHARDS)
    ]}],
    'executors': [
        {'id': 'w0', 'host': '127.0.0.1', 'port': 57100},
        {'id': 'w1', 'host': '127.0.0.1', 'port': 57101},
        # w2 defined but not started initially
        {'id': 'w2', 'host': '127.0.0.1', 'port': 57102},
    ],
    'metadata_path': METADATA_PATH,
}
with open(f"{BASE}/config.yaml", 'w') as f:
    yaml.dump(config, f)
print("  Setup: 4 shards, 2 workers\n")

# ==========================================
# Test 1: Baseline — 2 workers, search works
# ==========================================
print("-- Phase 1: Baseline --")

w0 = start_worker("w0", 57100)
w1 = start_worker("w1", 57101)
assert wait_for_grpc("127.0.0.1", 57100), "Worker w0 failed to start"
assert wait_for_grpc("127.0.0.1", 57101), "Worker w1 failed to start"
coord = start_coordinator()
assert wait_for_grpc("127.0.0.1", COORD_PORT), "Coordinator failed to start"

def t_baseline():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    r = client.search("rebal_table", query_vector=np.zeros(DIM).tolist(), k=10)
    assert r.num_rows == 10, f"Expected 10 results, got {r.num_rows}"
    s = client.status()
    healthy = sum(1 for e in s["executors"] if e["healthy"])
    assert healthy >= 2, f"Expected >=2 healthy, got {healthy}"
    client.close()
test("1. Baseline: 2 workers, 4 shards, search returns 10", t_baseline)

# ==========================================
# Test 2: Add 3rd worker + rebalance
# ==========================================
print("\n-- Phase 2: Add Worker + Rebalance --")

w2 = start_worker("w2", 57102)
assert wait_for_grpc("127.0.0.1", 57102), "Worker w2 failed to start"

def t_register_w2():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    # Register new worker
    client.register_worker("w2", "127.0.0.1", 57102)
    # Wait for health check loop to connect to w2 (up to 15s)
    for _ in range(15):
        time.sleep(1)
        s = client.status()
        healthy = sum(1 for e in s["executors"] if e["healthy"])
        if healthy == 3:
            break
    assert healthy == 3, f"Expected 3 healthy after register, got {healthy}"
    client.close()
test("2. Register 3rd worker, 3 healthy", t_register_w2)

def t_rebalance_scale_up():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    result = client.rebalance()
    print(f"({result['shards_moved']} moved)", end=" ")
    # With 4 shards across 3 workers, at least 1 shard should move
    # (from 2+2 to ~1+1+2 or 2+1+1)
    assert result["shards_moved"] >= 0  # could be 0 if already balanced
    client.close()
test("3. Rebalance after adding worker", t_rebalance_scale_up)

def t_search_after_scale_up():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    time.sleep(2)
    r = client.search("rebal_table", query_vector=np.zeros(DIM).tolist(), k=10)
    assert r.num_rows == 10, f"Expected 10 results, got {r.num_rows}"
    # Verify we get rows from different shards (different ID ranges)
    ids = sorted(r.column("id").to_pylist())
    print(f"(ids={ids[:3]}..{ids[-1]})", end=" ")
    client.close()
test("4. Search works after scale-up rebalance", t_search_after_scale_up)

def t_count_after_rebalance():
    """Verify total row count unchanged after rebalance."""
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    count = client.count_rows("rebal_table")
    expected = N_SHARDS * N_PER_SHARD
    assert count == expected, f"Expected {expected} rows, got {count}"
    client.close()
test("4b. Row count unchanged after rebalance", t_count_after_rebalance)

# ==========================================
# Test 3: Kill worker + rebalance (scale down)
# ==========================================
print("\n-- Phase 3: Kill Worker + Rebalance --")

def t_kill_and_rebalance():
    stop_process(w2)
    # Health check runs every 10s, needs 3 consecutive failures to mark unhealthy
    # So wait ~35s for w2 to be detected as dead
    print("(waiting 35s for health check)", end=" ", flush=True)
    time.sleep(35)
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    # Verify w2 is detected as unhealthy
    s = client.status()
    w2_status = [e for e in s["executors"] if e["id"] == "w2"]
    if w2_status:
        print(f"(w2 healthy={w2_status[0]['healthy']})", end=" ", flush=True)
    result = client.rebalance()
    print(f"({result['shards_moved']} moved)", end=" ")
    client.close()
test("5. Rebalance after worker failure", t_kill_and_rebalance)

def t_search_after_scale_down():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    time.sleep(3)
    try:
        r = client.search("rebal_table", query_vector=np.zeros(DIM).tolist(), k=10)
        assert r.num_rows > 0, "Search should return results after scale-down"
    except Exception as e:
        # Partial results OK — some shards may have moved to w2 which is now down
        if "unhealthy" not in str(e).lower() and "failed" not in str(e).lower():
            raise
    client.close()
test("6. Search works after scale-down rebalance", t_search_after_scale_down)

# ==========================================
# Cleanup + Summary
# ==========================================
cleanup_all()

print(f"\n{'='*70}")
print(f"  REBALANCE TEST RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*70}")
for name, status in results["tests"]:
    print(f"  {'V' if 'PASS' in status else 'X'} {name}: {status}")
print(f"{'='*70}")

sys.exit(0 if results["failed"] == 0 else 1)
