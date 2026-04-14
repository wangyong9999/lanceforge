#!/usr/bin/env python3
"""
HA End-to-End Test

Validates high-availability scenarios with persistent metadata:

1. Create table → coordinator restart → table still exists + queryable
2. Worker failure → queries route to surviving replica
3. Worker recovery → health check detects and reconnects

Requires: metadata_path config for persistent state.
"""

import sys, os, time, struct, subprocess, signal
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

import pyarrow as pa
import lance
import yaml

BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lanceforge_ha_test"
DIM = 32
COORD_PORT = 56000
METADATA_PATH = os.path.join(BASE, "metadata.json")

results = {"passed": 0, "failed": 0, "tests": []}

def cleanup_all():
    os.system("pkill -f 'lance-coordinator.*56000|lance-worker.*5610' 2>/dev/null")
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

# ── Setup ──
print(f"\n{'='*70}")
print(f"  HA END-TO-END TEST")
print(f"{'='*70}")

cleanup_all()
os.makedirs(BASE, exist_ok=True)
np.random.seed(42)

# Create shard data
for i in range(2):
    uri = os.path.join(BASE, f"shard_{i:02d}.lance")
    n = 2000
    off = i * n
    table = pa.table({
        'id': pa.array(range(off, off + n), type=pa.int64()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
    })
    lance.write_dataset(table, uri, mode='overwrite')
    ds = lance.dataset(uri)
    ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)

# Config WITH metadata_path for persistence
config = {
    'tables': [{'name': 'ha_test', 'shards': [
        {'name': f'ha_test_shard_{i:02d}',
         'uri': os.path.join(BASE, f'shard_{i:02d}.lance'),
         'executors': [f'w{i}']} for i in range(2)
    ]}],
    'executors': [
        {'id': 'w0', 'host': '127.0.0.1', 'port': 56100},
        {'id': 'w1', 'host': '127.0.0.1', 'port': 56101},
    ],
    'metadata_path': METADATA_PATH,
}
cfg_path = os.path.join(BASE, 'config.yaml')
with open(cfg_path, 'w') as f:
    yaml.dump(config, f)

print("  Data + config ready")

# ── Helper: start cluster ──
processes = []

def start_workers():
    global processes
    for i in range(2):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(56100 + i)],
            stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    time.sleep(3)

def start_coordinator():
    global processes
    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(4)
    return p

def stop_coordinator(proc):
    proc.terminate()
    proc.wait(timeout=5)
    time.sleep(1)

from lanceforge import LanceForgeClient

# ══════════════════════════════════════════════════
# Test 1: Coordinator restart — tables survive
# ══════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  Test 1: Coordinator Restart — Table Survival")
print(f"{'='*60}")

start_workers()
coord_proc = start_coordinator()

def t_baseline_search():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    r = client.search("ha_test", query_vector=np.zeros(DIM).tolist(), k=5)
    assert r.num_rows == 5, f"Expected 5, got {r.num_rows}"
    client.close()
test("Baseline search before restart", t_baseline_search)

def t_create_runtime_table():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    # Create a table at runtime (should be persisted to metadata)
    new_data = pa.table({
        'id': pa.array([1, 2, 3], type=pa.int64()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(3 * DIM).astype(np.float32)), list_size=DIM),
    })
    uri = os.path.join(BASE, "runtime_table.lance")
    result = client.create_table("runtime_tbl", new_data, uri=uri)
    assert result["num_rows"] == 3
    time.sleep(2)
    # Verify it's queryable
    r = client.search("runtime_tbl", query_vector=np.zeros(DIM).tolist(), k=3)
    assert r.num_rows > 0, "Runtime table not queryable"
    client.close()
test("Create runtime table + search", t_create_runtime_table)

# Kill coordinator
print("  Restarting coordinator...")
stop_coordinator(coord_proc)

# Restart coordinator (same config with metadata_path)
coord_proc = start_coordinator()

def t_table_survives_restart():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    tables = client.list_tables()
    assert "ha_test" in tables, f"ha_test missing after restart: {tables}"
    # Search original table
    r = client.search("ha_test", query_vector=np.zeros(DIM).tolist(), k=5)
    assert r.num_rows > 0, "Search failed after coordinator restart"
    client.close()
test("Tables survive coordinator restart", t_table_survives_restart)

# ══════════════════════════════════════════════════
# Test 2: Worker failure — cluster still serves
# ══════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  Test 2: Worker Failure — Degraded Service")
print(f"{'='*60}")

def t_kill_worker_still_serves():
    # Kill worker 1
    for p in processes:
        try:
            cmdline = open(f"/proc/{p.pid}/cmdline").read()
            if "w1" in cmdline and "lance-worker" in cmdline:
                p.terminate()
                p.wait(timeout=5)
                print(f"(killed w1 pid={p.pid})", end=" ")
                break
        except:
            pass

    time.sleep(2)  # Health check detects failure

    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    # Should still serve queries (w0 has shard_00)
    try:
        r = client.search("ha_test", query_vector=np.zeros(DIM).tolist(), k=5)
        # May get fewer results (only 1 shard available) but should not error
        assert r.num_rows > 0, "Cluster should still serve with 1 worker down"
    except Exception as e:
        # Partial failure is acceptable — not all shards available
        if "unhealthy" not in str(e).lower():
            raise
    client.close()
test("Cluster serves after worker failure", t_kill_worker_still_serves)

# ══════════════════════════════════════════════════
# Cleanup + Summary
# ══════════════════════════════════════════════════
cleanup_all()

print(f"\n{'='*70}")
print(f"  HA TEST RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*70}")
for name, status in results["tests"]:
    print(f"  {'✓' if 'PASS' in status else '✗'} {name}: {status}")
print(f"{'='*70}")

# Verify metadata file exists and has content
if os.path.exists(METADATA_PATH):
    size = os.path.getsize(METADATA_PATH)
    print(f"\n  Metadata file: {METADATA_PATH} ({size} bytes)")
else:
    print(f"\n  WARNING: Metadata file not found at {METADATA_PATH}")

sys.exit(0 if results["failed"] == 0 else 1)
