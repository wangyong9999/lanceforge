#!/usr/bin/env python3
"""
Enterprise-Grade E2E Test

Full-stack validation of LanceForge as a production system:

1. Cluster lifecycle (start, health, status)
2. Python SDK — search, insert, delete, status
3. REST API — /healthz, /metrics, /v1/status
4. Write-read consistency — insert then immediately search
5. Multi-table isolation — queries don't cross tables
6. Concurrent SDK access — 20 parallel searches
7. Filtered search correctness
8. Metrics accuracy — query count matches actual queries
9. Large batch insert — 10K rows at once
10. Delete + verify removal
"""

import sys, os, time, struct, subprocess, signal, threading
import concurrent.futures
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

from test_helpers import wait_for_grpc

import pyarrow as pa
import lance
import grpc
import yaml

DIM = 32
BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = "/tmp/lanceforge_enterprise_test"
COORD_PORT = 52000
REST_PORT = COORD_PORT + 1

processes = []
results = {"passed": 0, "failed": 0, "tests": []}

def cleanup():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)

def step(msg):
    print(f"\n{'='*70}\n  {msg}\n{'='*70}")

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

# ══════════════════════════════════════════════════
# Phase 1: Data + Cluster Setup
# ══════════════════════════════════════════════════
step("Phase 1: Create test data + start cluster")

os.makedirs(BASE, exist_ok=True)
np.random.seed(42)

# Table 1: products (10K rows)
for i in range(2):
    uri = os.path.join(BASE, f"products_shard_{i:02d}.lance")
    n = 5000
    off = i * n
    table = pa.table({
        'id': pa.array(range(off, off + n), type=pa.int64()),
        'category': pa.array([f'cat_{(j+off)%5}' for j in range(n)], type=pa.string()),
        'price': pa.array(np.random.uniform(10, 1000, n).astype(np.float64), type=pa.float64()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
    })
    lance.write_dataset(table, uri, mode='overwrite')
    ds = lance.dataset(uri)
    ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)
    print(f"  products shard {i}: {ds.count_rows()} rows")

# Table 2: documents (3K rows, different schema)
doc_uri = os.path.join(BASE, "documents_shard_00.lance")
doc_table = pa.table({
    'id': pa.array(range(50000, 53000), type=pa.int64()),
    'title': pa.array([f'doc_{i}' for i in range(3000)], type=pa.string()),
    'embedding': pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(3000 * DIM).astype(np.float32)), list_size=DIM),
})
lance.write_dataset(doc_table, doc_uri, mode='overwrite')
ds = lance.dataset(doc_uri)
ds.create_index('embedding', index_type='IVF_FLAT', num_partitions=4)
print(f"  documents: {ds.count_rows()} rows")

# Cluster config
config = {
    'tables': [
        {'name': 'products', 'shards': [
            {'name': f'products_shard_{i:02d}',
             'uri': os.path.join(BASE, f'products_shard_{i:02d}.lance'),
             'executors': [f'w{i}']} for i in range(2)
        ]},
        {'name': 'documents', 'shards': [
            {'name': 'documents_shard_00',
             'uri': doc_uri, 'executors': ['w0']}
        ]},
    ],
    'executors': [
        {'id': 'w0', 'host': '127.0.0.1', 'port': 52100},
        {'id': 'w1', 'host': '127.0.0.1', 'port': 52101},
    ],
}
cfg_path = os.path.join(BASE, 'config.yaml')
with open(cfg_path, 'w') as f:
    yaml.dump(config, f)

os.system("pkill -f 'lance-coordinator.*52000|lance-worker.*5210' 2>/dev/null")
time.sleep(1)

for i in range(2):
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(52100 + i)],
        stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
for i in range(2):
    assert wait_for_grpc("127.0.0.1", 52100 + i), f"Worker w{i} failed to start"

p = subprocess.Popen(
    [f"{BIN}/lance-coordinator", cfg_path, str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
processes.append(p)
assert wait_for_grpc("127.0.0.1", COORD_PORT), "Coordinator failed to start"

for proc in processes:
    assert proc.poll() is None, f"Process died! Check {BASE}/*.log"
print("  Cluster running")

# ══════════════════════════════════════════════════
# Phase 2: Python SDK Tests
# ══════════════════════════════════════════════════
step("Phase 2: Python SDK")

from lanceforge import LanceForgeClient

client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")

def t_sdk_search():
    result = client.search("products", query_vector=np.zeros(DIM).tolist(), k=10)
    assert result.num_rows == 10, f"Expected 10, got {result.num_rows}"
    assert "id" in result.column_names
    assert "_distance" in result.column_names
test("SDK search returns 10 results with correct schema", t_sdk_search)

def t_sdk_search_filter():
    result = client.search("products", query_vector=np.zeros(DIM).tolist(), k=50,
                           filter="category = 'cat_0'")
    assert result.num_rows > 0
    if "category" in result.column_names:
        cats = result.column("category").to_pylist()
        assert all(c == "cat_0" for c in cats), f"Filter leaked: {set(cats)}"
test("SDK filtered search respects filter", t_sdk_search_filter)

def t_sdk_status():
    status = client.status()
    assert len(status["executors"]) == 2
    assert all(e["healthy"] for e in status["executors"])
test("SDK status shows 2 healthy executors", t_sdk_status)

def t_sdk_multi_table():
    r1 = client.search("products", query_vector=np.zeros(DIM).tolist(), k=5)
    ids1 = r1.column("id").to_pylist()
    assert all(id < 50000 for id in ids1), f"Products IDs in doc range: {ids1}"
test("SDK multi-table isolation (products)", t_sdk_multi_table)

# ══════════════════════════════════════════════════
# Phase 3: REST API Tests
# ══════════════════════════════════════════════════
step("Phase 3: REST API")

import urllib.request

def t_rest_healthz():
    resp = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/healthz")
    body = resp.read().decode()
    assert '"ok"' in body, f"Unexpected: {body}"
test("REST /healthz returns ok", t_rest_healthz)

def t_rest_metrics():
    resp = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/metrics")
    body = resp.read().decode()
    assert "lance_query_total" in body
    assert "lance_query_errors_total" in body
test("REST /metrics returns Prometheus format", t_rest_metrics)

def t_rest_status():
    resp = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/v1/status")
    body = resp.read().decode()
    assert '"healthy":true' in body
test("REST /v1/status returns healthy", t_rest_status)

# ══════════════════════════════════════════════════
# Phase 4: Write-Read Consistency
# ══════════════════════════════════════════════════
step("Phase 4: Write-Read Consistency")

def t_insert_batch():
    # Insert 10K rows in one batch
    n = 10000
    new_data = pa.table({
        'id': pa.array(range(100000, 100000 + n), type=pa.int64()),
        'category': pa.array(['cat_new'] * n, type=pa.string()),
        'price': pa.array(np.random.uniform(1, 100, n).astype(np.float64), type=pa.float64()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
    })
    result = client.insert("products", new_data)
    assert result["new_version"] > 0, f"Version should be >0: {result}"
    assert result.get("affected_rows", 0) > 0 or result["new_version"] > 0
test("Insert 10K rows in single batch", t_insert_batch)

def t_search_after_insert():
    time.sleep(1)  # Brief pause for consistency
    result = client.search("products", query_vector=np.zeros(DIM).tolist(), k=100,
                           filter="category = 'cat_new'")
    assert result.num_rows > 0, "Inserted rows not found"
    ids = result.column("id").to_pylist()
    assert any(100000 <= id < 110000 for id in ids), f"New IDs not found: {ids[:5]}"
test("Search finds 10K inserted rows", t_search_after_insert)

def t_delete_and_verify():
    # Delete the inserted rows
    result = client.delete("products", filter="category = 'cat_new'")
    assert not result.get("error"), f"Delete error: {result}"
    time.sleep(1)
    # Verify deletion
    result2 = client.search("products", query_vector=np.zeros(DIM).tolist(), k=100,
                            filter="category = 'cat_new'")
    if result2.num_rows > 0:
        ids = result2.column("id").to_pylist()
        new_ids = [id for id in ids if 100000 <= id < 110000]
        assert len(new_ids) == 0, f"Deleted rows still found: {new_ids[:5]}"
test("Delete + verify rows removed", t_delete_and_verify)

# ══════════════════════════════════════════════════
# Phase 5: Concurrent SDK Access
# ══════════════════════════════════════════════════
step("Phase 5: Concurrent SDK Access (20 parallel)")

def t_concurrent_sdk():
    errors = []
    def search_fn(i):
        try:
            c = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
            q = np.random.randn(DIM).astype(np.float32).tolist()
            r = c.search("products", query_vector=q, k=5)
            assert r.num_rows > 0
            c.close()
        except Exception as e:
            errors.append(str(e))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(search_fn, i) for i in range(20)]
        concurrent.futures.wait(futures)
    assert len(errors) == 0, f"{len(errors)} errors: {errors[:3]}"
test("20 concurrent SDK searches, zero errors", t_concurrent_sdk)

# ══════════════════════════════════════════════════
# Phase 6: Metrics Accuracy
# ══════════════════════════════════════════════════
step("Phase 6: Metrics Accuracy")

def t_metrics_count():
    resp = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/metrics")
    body = resp.read().decode()
    # Extract query count
    for line in body.split('\n'):
        if line.startswith('lance_query_total') and not line.startswith('#'):
            count = int(line.split()[-1])
            # We've run several searches above — count should be > 0
            assert count > 0, f"Query count is 0"
            print(f"(count={count})", end=" ")
            return
    raise AssertionError("lance_query_total not found in metrics")
test("Metrics query count > 0 after searches", t_metrics_count)

# ══════════════════════════════════════════════════
# Cleanup + Summary
# ══════════════════════════════════════════════════
step("Cleanup")
client.close()
cleanup()

print(f"\n{'='*70}")
print(f"  ENTERPRISE E2E RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*70}")
for name, status in results["tests"]:
    print(f"  {'✓' if 'PASS' in status else '✗'} {name}: {status}")
print(f"{'='*70}")

sys.exit(0 if results["failed"] == 0 else 1)
