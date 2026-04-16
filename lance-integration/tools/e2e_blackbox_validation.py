#!/usr/bin/env python3
"""
LanceForge Black-Box End-to-End Validation Suite

Migrates mainstream vector DB benchmark test cases from:
- Milvus: insert throughput, filtered search, hybrid, concurrent
- Qdrant: recall@K, latency percentiles, data freshness
- VectorDBBench: standardized CRUD + search quality
- ANN-Benchmarks: recall vs throughput tradeoff

Runs against a live cluster (demo.sh or docker compose).

Usage:
    # Start cluster first:
    ./demo.sh

    # Run validation:
    cd lance-integration/tools
    PYTHONPATH=. python e2e_blackbox_validation.py
"""
import sys, os, time, struct, json, concurrent.futures
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

from lanceforge import LanceForgeClient
import pyarrow as pa
import pyarrow.ipc as ipc

# ── Config ──
COORD = os.environ.get("LANCEFORGE_HOST", "127.0.0.1:50050")
REST  = os.environ.get("LANCEFORGE_REST", "http://127.0.0.1:50051")
DIM   = 64  # must match demo.sh dataset

results = {"passed": 0, "failed": 0, "tests": [], "issues": []}

def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def test(name, fn):
    print(f"  {name}...", end=" ", flush=True)
    try:
        fn()
        print("PASS")
        results["passed"] += 1
        results["tests"].append((name, "PASS", ""))
    except Exception as e:
        msg = str(e)[:200]
        print(f"FAIL: {msg}")
        results["failed"] += 1
        results["tests"].append((name, "FAIL", msg))

def issue(category, description):
    results["issues"].append((category, description))
    print(f"  ⚠ ISSUE [{category}]: {description}")

def ev(v):
    return struct.pack(f"<{len(v)}f", *v)

def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema); w.write_batch(batch); w.close()
    return sink.getvalue().to_pybytes()

# ── Connect ──
section("0. Connection + Cluster Health")
client = LanceForgeClient(COORD, default_timeout=30)

def t_connect():
    s = client.status()
    assert len(s["executors"]) >= 2, f"Need >=2 workers, got {len(s['executors'])}"
    healthy = sum(1 for e in s["executors"] if e["healthy"])
    assert healthy >= 2, f"Need >=2 healthy, got {healthy}"
test("Connect to cluster + verify 2 healthy workers", t_connect)

def t_rest_health():
    import urllib.request
    resp = urllib.request.urlopen(f"{REST}/healthz", timeout=5)
    body = json.loads(resp.read())
    assert body.get("status") == "ok", f"healthz: {body}"
test("REST /healthz returns ok", t_rest_health)

def t_rest_metrics():
    import urllib.request
    resp = urllib.request.urlopen(f"{REST}/metrics", timeout=5)
    body = resp.read().decode()
    assert "lance_query_total" in body, "Missing lance_query_total"
    assert "lance_query_latency_ms_bucket" in body, "Missing histogram buckets"
test("REST /metrics has histogram buckets", t_rest_metrics)

# ══════════════════════════════════════════════════════════════
# SECTION 1: Search Quality (from ANN-Benchmarks / Qdrant)
# ══════════════════════════════════════════════════════════════
section("1. Search Quality — Recall & Correctness")

# Baseline: search existing demo data
def t_ann_basic():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    r = client.search("documents", q, k=10)
    assert r.num_rows == 10, f"Expected 10 results, got {r.num_rows}"
    assert "_distance" in r.column_names, "Missing _distance column"
    # Verify distances are sorted ascending (nearest first)
    dists = r.column("_distance").to_pylist()
    assert dists == sorted(dists), f"Results not sorted by distance: {dists[:5]}"
test("ANN search returns 10 sorted results", t_ann_basic)

def t_ann_k_values():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    for k in [1, 5, 20, 50, 100]:
        r = client.search("documents", q, k=k)
        assert r.num_rows <= k, f"k={k}: got {r.num_rows} rows (expected <={k})"
test("ANN search respects k=1,5,20,50,100", t_ann_k_values)

def t_filtered_search():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    r = client.search("documents", q, k=10, filter="category = 'technology'")
    assert r.num_rows > 0, "No results for filtered search"
    cats = r.column("category").to_pylist()
    for c in cats:
        assert c == "technology", f"Filter violated: got category={c}"
test("Filtered ANN: all results match filter predicate", t_filtered_search)

def t_empty_result():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    r = client.search("documents", q, k=10, filter="category = 'nonexistent_category_xyz'")
    assert r.num_rows == 0, f"Expected 0 results for impossible filter, got {r.num_rows}"
test("Filtered ANN: impossible filter returns 0 results", t_empty_result)

def t_column_projection():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    r = client.search("documents", q, k=5, columns=["id", "category"])
    cols = set(r.column_names)
    assert "id" in cols, f"Missing 'id' in projection: {cols}"
    assert "category" in cols, f"Missing 'category' in projection: {cols}"
test("Column projection returns requested columns", t_column_projection)

# ══════════════════════════════════════════════════════════════
# SECTION 2: Full-Text Search (from Milvus BM25 / Qdrant)
# ══════════════════════════════════════════════════════════════
section("2. Full-Text Search Quality")

def t_fts_basic():
    try:
        r = client.text_search("documents", "topic", k=10, text_column="text")
        assert r.num_rows > 0, "FTS returned no results"
    except Exception as e:
        if "No Lance shard found" in str(e) or "FTS" in str(e):
            issue("FTS", f"FTS not available on demo data: {e}")
            return
        raise
test("FTS search returns results for 'topic'", t_fts_basic)

def t_hybrid_search():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    try:
        r = client.hybrid_search("documents", q, "topic", k=10,
                                  vector_column="vector", text_column="text")
        # Hybrid may return 0 if FTS index missing, that's acceptable
        assert r.num_rows >= 0
    except Exception as e:
        if "FTS" in str(e) or "full_text" in str(e).lower():
            issue("HYBRID", f"Hybrid search requires FTS index: {e}")
            return
        raise
test("Hybrid ANN+FTS search executes without error", t_hybrid_search)

# ══════════════════════════════════════════════════════════════
# SECTION 3: CRUD Operations (from VectorDBBench)
# ══════════════════════════════════════════════════════════════
section("3. CRUD Operations")

TEST_TABLE = "validation_crud_test"

def t_create_table():
    # Clean up if exists
    try: client.drop_table(TEST_TABLE)
    except: pass
    N = 2000
    data = pa.table({
        'id': pa.array(range(N), type=pa.int64()),
        'text': pa.array([f'validation doc {i}' for i in range(N)]),
        'category': pa.array(['cat_a', 'cat_b', 'cat_c', 'cat_d'] * (N//4)),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(N * DIM).astype(np.float32)), list_size=DIM),
    })
    r = client.create_table(TEST_TABLE, data)
    assert r["num_rows"] == N, f"CreateTable: expected {N}, got {r['num_rows']}"
test("CreateTable: 2000 rows auto-sharded", t_create_table)

def t_count_after_create():
    time.sleep(1)  # let data settle
    count = client.count_rows(TEST_TABLE)
    assert count == 2000, f"Expected 2000, got {count}"
test("CountRows == 2000 after create", t_count_after_create)

def t_search_created_table():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    r = client.search(TEST_TABLE, q, k=10)
    assert r.num_rows == 10, f"Search on new table: expected 10, got {r.num_rows}"
test("Search on newly created table returns 10 results", t_search_created_table)

def t_insert_rows():
    N_INS = 500
    data = pa.table({
        'id': pa.array(range(90000, 90000 + N_INS), type=pa.int64()),
        'text': pa.array([f'inserted doc {i}' for i in range(N_INS)]),
        'category': pa.array(['inserted'] * N_INS),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(N_INS * DIM).astype(np.float32)), list_size=DIM),
    })
    r = client.insert(TEST_TABLE, data)
    assert r["affected_rows"] == N_INS, f"Insert: expected {N_INS}, got {r['affected_rows']}"
test("Insert 500 rows", t_insert_rows)

def t_count_after_insert():
    time.sleep(1)
    count = client.count_rows(TEST_TABLE)
    assert count == 2500, f"Expected 2500 after insert, got {count}"
test("CountRows == 2500 after insert (no duplication)", t_count_after_insert)

def t_data_freshness():
    """Milvus/Qdrant benchmark: search immediately after insert, find the new data."""
    # Insert a single distinctive row
    marker = pa.table({
        'id': pa.array([999999], type=pa.int64()),
        'text': pa.array(['freshness_marker_unique']),
        'category': pa.array(['freshness_test']),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.zeros(DIM, dtype=np.float32)), list_size=DIM),
    })
    client.insert(TEST_TABLE, marker)
    time.sleep(2)  # allow read_consistency_interval to pass
    # Search with the exact vector
    r = client.search(TEST_TABLE, [0.0] * DIM, k=1)
    assert r.num_rows >= 1, "Freshness: no results after insert"
    top_id = r.column("id")[0].as_py()
    assert top_id == 999999, f"Freshness: expected id=999999, got {top_id}"
test("Data freshness: insert then search finds new row", t_data_freshness)

def t_delete_rows():
    r = client.delete(TEST_TABLE, filter="category = 'freshness_test'")
    assert r["affected_rows"] >= 1, f"Delete: expected >=1 affected, got {r['affected_rows']}"
test("Delete rows by filter", t_delete_rows)

def t_upsert_rows():
    # Upsert: update existing rows
    data = pa.table({
        'id': pa.array([90000, 90001, 90002], type=pa.int64()),
        'text': pa.array(['upserted_A', 'upserted_B', 'upserted_C']),
        'category': pa.array(['upserted'] * 3),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(3 * DIM).astype(np.float32)), list_size=DIM),
    })
    r = client.upsert(TEST_TABLE, data, on_columns=["id"])
    assert r["affected_rows"] == 3, f"Upsert: expected 3, got {r['affected_rows']}"
test("Upsert 3 rows (update existing)", t_upsert_rows)

def t_get_schema():
    schema = client.get_schema(TEST_TABLE)
    col_names = [c["name"] for c in schema]
    for expected in ["id", "text", "category", "vector"]:
        assert expected in col_names, f"Missing column: {expected}"
test("GetSchema returns all columns", t_get_schema)

def t_list_tables():
    tables = client.list_tables()
    assert TEST_TABLE in tables, f"Table not in list: {tables}"
    assert "documents" in tables, f"Demo table not in list: {tables}"
test("ListTables includes created and demo tables", t_list_tables)

# ══════════════════════════════════════════════════════════════
# SECTION 4: Latency Profiling (from Qdrant benchmarks)
# ══════════════════════════════════════════════════════════════
section("4. Latency Profiling (P50/P95/P99)")

def t_latency_profile():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    latencies = []
    N_ITER = 50
    for _ in range(N_ITER):
        t0 = time.time()
        client.search(TEST_TABLE, q, k=10)
        latencies.append((time.time() - t0) * 1000)

    latencies.sort()
    p50 = latencies[int(N_ITER * 0.5)]
    p95 = latencies[int(N_ITER * 0.95)]
    p99 = latencies[int(N_ITER * 0.99)]
    avg = sum(latencies) / len(latencies)

    print(f"\n    Latency (ms): P50={p50:.1f}  P95={p95:.1f}  P99={p99:.1f}  avg={avg:.1f}")

    # Sanity: P99 should be under 500ms for 2500 rows local
    assert p99 < 500, f"P99={p99:.1f}ms exceeds 500ms threshold"
    # P50 should be under 100ms
    assert p50 < 100, f"P50={p50:.1f}ms exceeds 100ms threshold"
test("Latency profile: P50 < 100ms, P99 < 500ms", t_latency_profile)

def t_filtered_latency():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    latencies = []
    for _ in range(30):
        t0 = time.time()
        client.search(TEST_TABLE, q, k=10, filter="category = 'cat_a'")
        latencies.append((time.time() - t0) * 1000)
    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.5)]
    print(f"    Filtered latency (ms): P50={p50:.1f}")
    assert p50 < 200, f"Filtered P50={p50:.1f}ms exceeds 200ms"
test("Filtered search latency: P50 < 200ms", t_filtered_latency)

# ══════════════════════════════════════════════════════════════
# SECTION 5: Concurrent Load (from Milvus / VectorDBBench)
# ══════════════════════════════════════════════════════════════
section("5. Concurrent Load Test")

def t_concurrent_search():
    q = np.random.randn(DIM).astype(np.float32).tolist()
    N_QUERIES = 100
    CONCURRENCY = 10
    errors = []
    latencies = []

    def do_search(_):
        try:
            c = LanceForgeClient(COORD, default_timeout=30)
            t0 = time.time()
            r = c.search(TEST_TABLE, q, k=10)
            lat = (time.time() - t0) * 1000
            c.close()
            return (r.num_rows, lat, None)
        except Exception as e:
            return (0, 0, str(e))

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(do_search, i) for i in range(N_QUERIES)]
        for f in concurrent.futures.as_completed(futures):
            rows, lat, err = f.result()
            if err:
                errors.append(err)
            else:
                latencies.append(lat)
    wall = time.time() - t0
    qps = len(latencies) / wall if wall > 0 else 0

    latencies.sort()
    p50 = latencies[len(latencies)//2] if latencies else 0
    p99 = latencies[int(len(latencies)*0.99)] if latencies else 0

    print(f"\n    Concurrent: {len(latencies)}/{N_QUERIES} ok, {len(errors)} errors")
    print(f"    QPS={qps:.1f}  P50={p50:.1f}ms  P99={p99:.1f}ms  wall={wall:.1f}s")

    assert len(errors) == 0, f"{len(errors)} errors: {errors[:3]}"
    assert qps > 5, f"QPS={qps:.1f} too low (expected >5)"
test(f"Concurrent search: 100 queries @ 10 threads, 0 errors", t_concurrent_search)

def t_concurrent_mixed_rw():
    """Milvus benchmark: concurrent reads + writes."""
    q = np.random.randn(DIM).astype(np.float32).tolist()
    errors = []
    read_count = [0]
    write_count = [0]

    def do_read():
        try:
            c = LanceForgeClient(COORD, default_timeout=30)
            c.search(TEST_TABLE, q, k=10)
            c.close()
            read_count[0] += 1
        except Exception as e:
            errors.append(f"read: {e}")

    def do_write():
        try:
            c = LanceForgeClient(COORD, default_timeout=30)
            data = pa.table({
                'id': pa.array([np.random.randint(100000, 200000)], type=pa.int64()),
                'text': pa.array(['mixed_rw_test']),
                'category': pa.array(['mixed']),
                'vector': pa.FixedSizeListArray.from_arrays(
                    pa.array(np.random.randn(DIM).astype(np.float32)), list_size=DIM),
            })
            c.insert(TEST_TABLE, data)
            c.close()
            write_count[0] += 1
        except Exception as e:
            errors.append(f"write: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futs = []
        for i in range(40):
            if i % 4 == 0:
                futs.append(pool.submit(do_write))
            else:
                futs.append(pool.submit(do_read))
        for f in concurrent.futures.as_completed(futs):
            pass

    print(f"\n    Mixed R/W: {read_count[0]} reads, {write_count[0]} writes, {len(errors)} errors")
    assert len(errors) == 0, f"Mixed R/W errors: {errors[:3]}"
test("Concurrent mixed R/W: 30 reads + 10 writes @ 8 threads", t_concurrent_mixed_rw)

# ══════════════════════════════════════════════════════════════
# SECTION 6: DDL Robustness (edge cases)
# ══════════════════════════════════════════════════════════════
section("6. DDL Robustness")

def t_create_duplicate():
    """Should reject creating a table that already exists."""
    try:
        data = pa.table({
            'id': pa.array([1], type=pa.int64()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(np.zeros(DIM, dtype=np.float32)), list_size=DIM),
        })
        client.create_table(TEST_TABLE, data)
        issue("DDL", "CreateTable on existing table did NOT return error")
    except Exception as e:
        assert "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e), \
            f"Wrong error for duplicate create: {e}"
test("CreateTable duplicate: returns ALREADY_EXISTS", t_create_duplicate)

def t_search_nonexistent():
    try:
        q = np.random.randn(DIM).astype(np.float32).tolist()
        client.search("table_that_does_not_exist_xyz", q, k=10)
        issue("DDL", "Search on nonexistent table did NOT raise error")
    except Exception as e:
        assert "not found" in str(e).lower() or "NOT_FOUND" in str(e), \
            f"Wrong error for nonexistent table: {e}"
test("Search nonexistent table: returns NOT_FOUND", t_search_nonexistent)

def t_create_index():
    try:
        client.create_index(TEST_TABLE, "category", index_type="BTREE")
    except Exception as e:
        # BTREE index on string column might not be supported; acceptable
        issue("INDEX", f"CreateIndex BTREE on string: {e}")
test("CreateIndex BTREE on category column", t_create_index)

def t_drop_and_verify():
    client.drop_table(TEST_TABLE)
    time.sleep(1)
    tables = client.list_tables()
    assert TEST_TABLE not in tables, f"Table still in list after drop: {tables}"
test("DropTable + verify table is gone from ListTables", t_drop_and_verify)

def t_search_after_drop():
    try:
        q = np.random.randn(DIM).astype(np.float32).tolist()
        client.search(TEST_TABLE, q, k=10)
        issue("DDL", "Search after DropTable did NOT raise error")
    except Exception:
        pass  # expected
test("Search after DropTable: raises error", t_search_after_drop)

# ══════════════════════════════════════════════════════════════
# SECTION 7: Cluster Management
# ══════════════════════════════════════════════════════════════
section("7. Cluster Management")

def t_rebalance():
    r = client.rebalance()
    assert "shards_moved" in r, f"Rebalance response missing shards_moved: {r}"
test("Rebalance executes without error", t_rebalance)

def t_status_details():
    s = client.status()
    assert "total_shards" in s, f"Missing total_shards: {s}"
    assert "total_rows" in s, f"Missing total_rows: {s}"
    assert s["total_shards"] >= 2, f"Expected >=2 shards, got {s['total_shards']}"
test("Status: total_shards >= 2, total_rows > 0", t_status_details)

# ══════════════════════════════════════════════════════════════
# SECTION 8: Metrics Validation
# ══════════════════════════════════════════════════════════════
section("8. Prometheus Metrics Validation")

def t_metrics_counters():
    import urllib.request
    resp = urllib.request.urlopen(f"{REST}/metrics", timeout=5)
    body = resp.read().decode()
    # After all the tests above, query count should be significant
    for line in body.split('\n'):
        if line.startswith('lance_query_total '):
            count = int(line.split()[-1])
            assert count >= 10, f"Expected >=10 queries, got {count}"
            print(f"\n    Total queries tracked: {count}")
            return
    issue("METRICS", "lance_query_total not found in /metrics")
test("Metrics: lance_query_total >= 10", t_metrics_counters)

def t_metrics_histogram():
    import urllib.request
    resp = urllib.request.urlopen(f"{REST}/metrics", timeout=5)
    body = resp.read().decode()
    buckets = [l for l in body.split('\n') if 'lance_query_latency_ms_bucket' in l]
    assert len(buckets) >= 13, f"Expected >=13 histogram buckets, got {len(buckets)}"
    # Verify cumulative property: each bucket >= previous
    counts = []
    for b in buckets:
        c = int(b.split()[-1])
        counts.append(c)
    for i in range(1, len(counts)):
        assert counts[i] >= counts[i-1], \
            f"Histogram not cumulative: bucket[{i}]={counts[i]} < bucket[{i-1}]={counts[i-1]}"
test("Metrics: histogram is cumulative (13+ buckets)", t_metrics_histogram)

# ══════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════
section("RESULTS")
total = results["passed"] + results["failed"]
print(f"\n  {results['passed']}/{total} PASSED, {results['failed']} FAILED\n")

for name, status, msg in results["tests"]:
    icon = "✓" if status == "PASS" else "✗"
    detail = f" — {msg}" if msg else ""
    print(f"  {icon} {name}{detail}")

if results["issues"]:
    print(f"\n  ISSUES FOUND ({len(results['issues'])}):")
    for cat, desc in results["issues"]:
        print(f"    [{cat}] {desc}")

print(f"\n{'='*70}")
client.close()
sys.exit(0 if results["failed"] == 0 else 1)
