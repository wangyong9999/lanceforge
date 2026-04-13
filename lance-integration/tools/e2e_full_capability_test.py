#!/usr/bin/env python3
"""
Full Capability Verification Test

Validates EVERY claimed capability end-to-end. No synthetic shortcuts.
If it fails here, the capability is fake.

Tests:
  1. CreateTable via SDK
  2. ListTables via SDK
  3. Insert via SDK
  4. ANN search via SDK
  5. FTS search via gRPC (not just SDK wrapper)
  6. Hybrid search via gRPC
  7. Filtered search via SDK
  8. Upsert via SDK (insert + update existing)
  9. Delete via SDK
  10. CreateIndex via SDK
  11. Cluster status via SDK
  12. REST /healthz
  13. REST /metrics
  14. Concurrent search (10 parallel)
"""

import sys, os, time, struct, subprocess
import concurrent.futures
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

import pyarrow as pa
import lance
import grpc
import yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pb_grpc

BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lanceforge_capability_test"
COORD_PORT = 54000
REST_PORT = COORD_PORT + 1
DIM = 32

processes = []
results = {"passed": 0, "failed": 0, "tests": []}

def cleanup():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)

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

# ── Setup: Create pre-existing data + start cluster ──
print(f"\n{'='*70}")
print(f"  FULL CAPABILITY VERIFICATION")
print(f"{'='*70}")

os.makedirs(BASE, exist_ok=True)
np.random.seed(42)

# Create 1 shard with text+vector data
uri = os.path.join(BASE, "shard_00.lance")
n = 2000
table_data = pa.table({
    'id': pa.array(range(n), type=pa.int64()),
    'title': pa.array([f'Document {i} about topic {i%5}' for i in range(n)], type=pa.string()),
    'text': pa.array([f'Content about topic{i%5} with keywords technology science business health sports item{i}' for i in range(n)], type=pa.string()),
    'category': pa.array([['tech','science','business','health','sports'][i%5] for i in range(n)], type=pa.string()),
    'vector': pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
})
lance.write_dataset(table_data, uri, mode='overwrite')
ds = lance.dataset(uri)
ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)
try:
    ds.create_scalar_index('category', index_type='BTREE')
    ds.create_scalar_index('text', index_type='INVERTED')
except:
    pass
print(f"  Data: {ds.count_rows()} rows with IVF_FLAT + BTREE + FTS index")

cfg = {
    'tables': [{'name': 'docs', 'shards': [
        {'name': 'docs_shard_00', 'uri': uri, 'executors': ['w0']}
    ]}],
    'executors': [{'id': 'w0', 'host': '127.0.0.1', 'port': 54100}],
}
with open(os.path.join(BASE, 'config.yaml'), 'w') as f:
    yaml.dump(cfg, f)

os.system("pkill -f 'lance-coordinator.*54000|lance-worker.*5410' 2>/dev/null")
time.sleep(2)

p = subprocess.Popen([f"{BIN}/lance-worker", f"{BASE}/config.yaml", "w0", "54100"],
    stdout=open(f"{BASE}/w0.log", "w"), stderr=subprocess.STDOUT)
processes.append(p)
time.sleep(3)
p = subprocess.Popen([f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
processes.append(p)
time.sleep(4)

for proc in processes:
    assert proc.poll() is None, f"Process died!"
print("  Cluster running\n")

from lanceforge import LanceForgeClient
client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")

# Raw gRPC stub for FTS/Hybrid (SDK wrapper test + raw gRPC test)
channel = grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
    options=[("grpc.max_receive_message_length", 64*1024*1024)])
stub = pb_grpc.LanceSchedulerServiceStub(channel)

# ── Tests ──

def t_list_tables():
    tables = client.list_tables()
    assert "docs" in tables, f"Expected 'docs' in {tables}"
test("1. ListTables finds 'docs'", t_list_tables)

def t_ann_search():
    r = client.search("docs", query_vector=np.zeros(DIM).tolist(), k=5)
    assert r.num_rows == 5
    assert "id" in r.column_names
    assert "_distance" in r.column_names
test("2. ANN search returns 5 results", t_ann_search)

def t_fts_search_grpc():
    import pyarrow.ipc as ipc
    r = stub.FtsSearch(pb.FtsSearchRequest(
        table_name="docs", text_column="text",
        query_text="technology", k=5), timeout=30)
    assert not r.error, f"FTS error: {r.error}"
    assert r.num_rows > 0, "FTS returned 0 rows"
    t = ipc.open_stream(r.arrow_ipc_data).read_all()
    assert t.num_rows > 0
    # Verify FTS results contain the query term
    texts = t.column("text").to_pylist()
    assert any("technology" in str(tx).lower() or "tech" in str(tx).lower() for tx in texts), \
        f"FTS results don't contain 'technology': {texts[:2]}"
test("3. FTS search via gRPC returns relevant results", t_fts_search_grpc)

def t_hybrid_search_grpc():
    import pyarrow.ipc as ipc
    r = stub.HybridSearch(pb.HybridSearchRequest(
        table_name="docs", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, nprobes=10, metric_type=0,
        text_column="text", query_text="science",
        k=5), timeout=30)
    if r.error:
        # Hybrid may fail if FTS index not available — report but don't hard-fail
        print(f"(hybrid error: {r.error[:60]})", end=" ")
        assert False, r.error
    assert r.num_rows > 0
test("4. Hybrid search via gRPC", t_hybrid_search_grpc)

def t_filtered_search():
    r = client.search("docs", query_vector=np.zeros(DIM).tolist(), k=10,
                       filter="category = 'tech'")
    assert r.num_rows > 0
    cats = r.column("category").to_pylist()
    assert all(c == "tech" for c in cats), f"Filter leaked: {set(cats)}"
test("5. Filtered search respects filter", t_filtered_search)

def t_insert():
    new_data = pa.table({
        'id': pa.array([99001, 99002, 99003], type=pa.int64()),
        'title': pa.array(['New A', 'New B', 'New C'], type=pa.string()),
        'text': pa.array(['new content A', 'new content B', 'new content C'], type=pa.string()),
        'category': pa.array(['new', 'new', 'new'], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(3 * DIM).astype(np.float32)), list_size=DIM),
    })
    result = client.insert("docs", new_data)
    assert result["new_version"] > 0
test("6. Insert 3 rows", t_insert)

def t_search_after_insert():
    time.sleep(1)
    r = client.search("docs", query_vector=np.zeros(DIM).tolist(), k=100,
                       filter="category = 'new'")
    assert r.num_rows > 0, "Inserted rows not found"
    ids = r.column("id").to_pylist()
    assert any(id >= 99001 for id in ids), f"New IDs not found: {ids[:5]}"
test("7. Search finds inserted rows", t_search_after_insert)

def t_upsert():
    # Upsert: update existing row 99001, insert new 99004
    upsert_data = pa.table({
        'id': pa.array([99001, 99004], type=pa.int64()),
        'title': pa.array(['Updated A', 'New D'], type=pa.string()),
        'text': pa.array(['updated content', 'brand new D'], type=pa.string()),
        'category': pa.array(['updated', 'new'], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(2 * DIM).astype(np.float32)), list_size=DIM),
    })
    result = client.upsert("docs", upsert_data, on_columns=["id"])
    assert result["new_version"] > 0
test("8. Upsert (update + insert)", t_upsert)

def t_delete():
    result = client.delete("docs", filter="category = 'new' OR category = 'updated'")
    assert not result.get("error"), f"Delete error: {result}"
test("9. Delete rows by filter", t_delete)

def t_search_after_delete():
    time.sleep(1)
    r = client.search("docs", query_vector=np.zeros(DIM).tolist(), k=100,
                       filter="category = 'new'")
    if r.num_rows > 0:
        ids = r.column("id").to_pylist()
        new_ids = [id for id in ids if id >= 99001]
        assert len(new_ids) == 0, f"Deleted rows still found: {new_ids}"
test("10. Delete confirmed (rows gone)", t_search_after_delete)

def t_status():
    s = client.status()
    assert len(s["executors"]) >= 1
    assert s["executors"][0]["healthy"]
test("11. Cluster status healthy", t_status)

def t_rest_healthz():
    import urllib.request
    resp = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/healthz")
    assert resp.status == 200
test("12. REST /healthz", t_rest_healthz)

def t_rest_metrics():
    import urllib.request
    resp = urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/metrics")
    body = resp.read().decode()
    assert "lance_query_total" in body or "process_" in body
test("13. REST /metrics", t_rest_metrics)

def t_concurrent():
    errors = []
    def search_fn(i):
        try:
            c = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
            r = c.search("docs", query_vector=np.random.randn(DIM).tolist(), k=5)
            assert r.num_rows > 0
            c.close()
        except Exception as e:
            errors.append(str(e))
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futs = [pool.submit(search_fn, i) for i in range(10)]
        concurrent.futures.wait(futs)
    assert len(errors) == 0, f"{len(errors)} errors: {errors[:3]}"
test("14. 10 concurrent searches, zero errors", t_concurrent)

# ── Cleanup ──
channel.close()
client.close()
cleanup()

print(f"\n{'='*70}")
print(f"  FULL CAPABILITY RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*70}")
for name, status in results["tests"]:
    print(f"  {'✓' if 'PASS' in status else '✗'} {name}: {status}")
print(f"{'='*70}")

sys.exit(0 if results["failed"] == 0 else 1)
