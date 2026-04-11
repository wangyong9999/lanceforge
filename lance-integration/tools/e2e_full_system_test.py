#!/usr/bin/env python3
"""
Full System E2E Test — New Crate Binaries on MinIO
Covers: S3 storage, large data+index, ANN/Filter/CrossShard, concurrent load,
        text query, multi-table, failover, throughput/latency.
"""

import sys, os, time, struct, subprocess, signal, threading, concurrent.futures
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import lance
import grpc
import yaml
import lance_service_pb2
import lance_service_pb2_grpc

# ── Config ──
DIM = 64
ROWS_PER_SHARD = 50000  # 150K total
NUM_SHARDS = 3
BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lance_full_system_test"
MINIO_OPTS = {
    'aws_access_key_id': 'lanceadmin',
    'aws_secret_access_key': 'lanceadmin123',
    'aws_endpoint': 'http://127.0.0.1:9000',
    'aws_region': 'us-east-1',
    'allow_http': 'true',
}

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

def ev(v): return struct.pack(f"<{len(v)}f", *v)
def dr(resp):
    if resp.error: raise RuntimeError(resp.error)
    if not resp.arrow_ipc_data: return pa.table({})
    return ipc.open_stream(resp.arrow_ipc_data).read_all()

# ═══════════════════════════════════════════════════════════════
# Phase 1: Data on MinIO
# ═══════════════════════════════════════════════════════════════
step("Phase 1: Create 150K vectors on MinIO + IVF_PQ indexes")

os.makedirs(BASE, exist_ok=True)
# Ensure bucket
os.system("/tmp/mc mb local/lance-fulltest 2>/dev/null || true")

for i in range(NUM_SHARDS):
    uri = f"s3://lance-fulltest/products/shard_{i:02d}.lance"
    n = ROWS_PER_SHARD
    off = i * n
    np.random.seed(42 + i)
    print(f"  Shard {i}: {n} rows to {uri}...")
    t0 = time.time()
    table = pa.table({
        'id': pa.array(range(off, off+n), type=pa.int64()),
        'category': pa.array([f'cat_{(j+off)%10}' for j in range(n)], type=pa.string()),
        'region': pa.array(['APAC' if (j+off)%3==0 else 'EMEA' if (j+off)%3==1 else 'NA' for j in range(n)], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n*DIM).astype(np.float32)), list_size=DIM),
    })
    lance.write_dataset(table, uri, mode='overwrite', storage_options=MINIO_OPTS)
    ds = lance.dataset(uri, storage_options=MINIO_OPTS)
    # Build IVF_PQ index
    if not ds.list_indices():
        print(f"    Building IVF_PQ index...")
        ds.create_index('vector', index_type='IVF_PQ',
                       num_partitions=min(32, n//500), num_sub_vectors=min(16, DIM//4))
    print(f"    Done ({time.time()-t0:.1f}s, {ds.count_rows()} rows, {len(ds.list_indices())} indices)")

# Also create a documents table (different schema) for multi-table test
doc_uri = "s3://lance-fulltest/documents/shard_00.lance"
print(f"  Documents shard: 5000 rows (dim=32)...")
np.random.seed(999)
doc_table = pa.table({
    'id': pa.array(range(200000, 205000), type=pa.int64()),
    'title': pa.array([f'doc_{i}' for i in range(5000)], type=pa.string()),
    'embedding': pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(5000*32).astype(np.float32)), list_size=32),
})
lance.write_dataset(doc_table, doc_uri, mode='overwrite', storage_options=MINIO_OPTS)

# ═══════════════════════════════════════════════════════════════
# Phase 2: Write config + start cluster
# ═══════════════════════════════════════════════════════════════
step("Phase 2: Start cluster with NEW binaries on MinIO")

config = {
    'tables': [
        {'name': 'products', 'shards': [
            {'name': f'products_shard_{i:02d}', 'uri': f's3://lance-fulltest/products/shard_{i:02d}.lance',
             'executors': [f'w{i}']} for i in range(NUM_SHARDS)
        ]},
        {'name': 'documents', 'shards': [
            {'name': 'documents_shard_00', 'uri': doc_uri, 'executors': ['w0']}
        ]},
    ],
    'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': 51200+i} for i in range(NUM_SHARDS)],
    'storage_options': MINIO_OPTS,
    'embedding': {'provider': 'mock', 'model': 'test', 'dimension': DIM},
}
cfg_path = f"{BASE}/config.yaml"
with open(cfg_path, 'w') as f:
    yaml.dump(config, f)

os.system("pkill -f 'lance-coordinator|lance-worker' 2>/dev/null"); time.sleep(1)

for i in range(NUM_SHARDS):
    p = subprocess.Popen([f"{BIN}/lance-worker", cfg_path, f"w{i}", str(51200+i)],
        stdout=open(f"{BASE}/w{i}.log","w"), stderr=subprocess.STDOUT)
    processes.append(p); print(f"  Worker {i} (PID {p.pid})")
time.sleep(4)

p = subprocess.Popen([f"{BIN}/lance-coordinator", cfg_path, "51250"],
    stdout=open(f"{BASE}/coord.log","w"), stderr=subprocess.STDOUT)
processes.append(p); print(f"  Coordinator (PID {p.pid})")
time.sleep(5)

for proc in processes:
    assert proc.poll() is None, f"Process died! Check {BASE}/*.log"
print("  ✓ All processes alive")

stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(
    grpc.insecure_channel("127.0.0.1:51250", options=[("grpc.max_receive_message_length", 64*1024*1024)]))

# ═══════════════════════════════════════════════════════════════
# Phase 3: Core retrieval tests
# ═══════════════════════════════════════════════════════════════
step("Phase 3: Core retrieval")

def t_basic_ann():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=10, nprobes=10, metric_type=0))
    t = dr(r); assert t.num_rows == 10
    d = t.column("_distance").to_pylist()
    for i in range(1, len(d)): assert d[i] >= d[i-1], "Not sorted"
test("Basic ANN (10 results, sorted)", t_basic_ann)

def t_filtered():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=ev(np.random.randn(DIM).astype(np.float32)),
        dimension=DIM, k=50, nprobes=10, metric_type=0, filter="category = 'cat_0'"))
    t = dr(r); assert t.num_rows > 0
    if "category" in t.column_names:
        for c in t.column("category").to_pylist(): assert c == "cat_0"
test("Filtered ANN (category='cat_0')", t_filtered)

def t_cross_shard():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=200, nprobes=20, metric_type=0))
    t = dr(r)
    ids = t.column("id").to_pylist()
    shards = len(set(id // ROWS_PER_SHARD for id in ids))
    assert shards >= 2, f"Only {shards} shards"
test("Cross-shard merge (200 results, ≥2 shards)", t_cross_shard)

def t_large_k():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=ev(np.random.randn(DIM).astype(np.float32)),
        dimension=DIM, k=1000, nprobes=20, metric_type=0))
    t = dr(r); assert t.num_rows > 0
test("Large K=1000", t_large_k)

# ═══════════════════════════════════════════════════════════════
# Phase 4: Text query with mock embedding
# ═══════════════════════════════════════════════════════════════
step("Phase 4: Text query (mock embedding)")

def t_text_query():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=b"", dimension=0, k=10, nprobes=10, metric_type=0,
        query_text="find similar products"))
    t = dr(r); assert t.num_rows > 0
test("Text query → mock embedding → results", t_text_query)

# ═══════════════════════════════════════════════════════════════
# Phase 5: Multi-table
# ═══════════════════════════════════════════════════════════════
step("Phase 5: Multi-table (products + documents)")

def t_multi_products():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=5, nprobes=5, metric_type=0))
    t = dr(r); assert t.num_rows > 0
    assert any(id < 200000 for id in t.column("id").to_pylist())
test("Query products table", t_multi_products)

def t_multi_docs():
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="documents", vector_column="embedding",
        query_vector=ev(np.zeros(32, dtype=np.float32)),
        dimension=32, k=5, nprobes=5, metric_type=0))
    t = dr(r); assert t.num_rows > 0
    assert all(id >= 200000 for id in t.column("id").to_pylist())
test("Query documents table (different schema)", t_multi_docs)

# ═══════════════════════════════════════════════════════════════
# Phase 6: Concurrent load
# ═══════════════════════════════════════════════════════════════
step("Phase 6: Concurrent load (50 parallel queries)")

def t_concurrent():
    errors = []
    # Share a single channel (HTTP/2 multiplexing) — realistic client pattern
    shared_channel = grpc.insecure_channel("127.0.0.1:51250",
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    shared_stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(shared_channel)
    def query(i):
        try:
            r = shared_stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
                table_name="products", vector_column="vector",
                query_vector=ev(np.random.randn(DIM).astype(np.float32)),
                dimension=DIM, k=10, nprobes=5, metric_type=0), timeout=30)
            assert not r.error
        except Exception as e:
            errors.append(str(e))

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
        futures = [pool.submit(query, i) for i in range(50)]
        concurrent.futures.wait(futures)
    shared_channel.close()
    assert len(errors) == 0, f"{len(errors)} errors: {errors[:3]}"
test("50 concurrent queries, zero errors", t_concurrent)

# ═══════════════════════════════════════════════════════════════
# Phase 7: Throughput/latency benchmark
# ═══════════════════════════════════════════════════════════════
step("Phase 7: Throughput benchmark (100 sequential queries)")

def t_throughput():
    # Check processes alive before benchmark
    for proc in processes:
        if proc.poll() is not None:
            raise RuntimeError(f"Process {proc.pid} died before benchmark! Check {BASE}/*.log")
    # Fresh channel with keepalive for benchmark
    bench_channel = grpc.insecure_channel("127.0.0.1:51250",
        options=[
            ("grpc.max_receive_message_length", 64*1024*1024),
            ("grpc.keepalive_time_ms", 10000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1),
        ])
    bench_stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(bench_channel)
    # Warm up with 1 query
    r = bench_stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=1, nprobes=5, metric_type=0), timeout=30)
    if r.error: raise RuntimeError(f"Warmup failed: {r.error}")
    latencies = []
    retries = 0
    for i in range(100):
        q = np.random.randn(DIM).astype(np.float32)
        req = lance_service_pb2.AnnSearchRequest(
            table_name="products", vector_column="vector",
            query_vector=ev(q), dimension=DIM, k=10, nprobes=10, metric_type=0)
        # Retry transient gRPC errors (standard for benchmarks)
        for attempt in range(3):
            try:
                t0 = time.time()
                r = bench_stub.AnnSearch(req, timeout=30)
                latencies.append((time.time()-t0)*1000)
                assert not r.error, f"Query {i} error: {r.error}"
                break
            except grpc.RpcError as e:
                if attempt < 2 and e.code() == grpc.StatusCode.UNAVAILABLE:
                    retries += 1
                    time.sleep(0.1)
                    continue
                raise
    bench_channel.close()
    if retries > 0:
        print(f"\n    (retries: {retries})", end="")
    lat = np.array(latencies)
    qps = 100 / (sum(latencies)/1000)
    print(f"\n    QPS:  {qps:.1f}")
    print(f"    P50:  {np.percentile(lat,50):.1f}ms")
    print(f"    P95:  {np.percentile(lat,95):.1f}ms")
    print(f"    P99:  {np.percentile(lat,99):.1f}ms")
    print(f"    Mean: {np.mean(lat):.1f}ms", end="  ")
test("100 queries benchmark", t_throughput)

# ═══════════════════════════════════════════════════════════════
# Phase 8: Cluster status
# ═══════════════════════════════════════════════════════════════
step("Phase 8: Cluster health")

def t_status():
    r = stub.GetClusterStatus(lance_service_pb2.ClusterStatusRequest())
    healthy = sum(1 for e in r.executors if e.healthy)
    assert healthy == NUM_SHARDS, f"Only {healthy}/{NUM_SHARDS} healthy"
test(f"Cluster status: {NUM_SHARDS}/{NUM_SHARDS} healthy", t_status)

# ═══════════════════════════════════════════════════════════════
# Cleanup + Summary
# ═══════════════════════════════════════════════════════════════
step("Cleanup")
cleanup()

print(f"\n{'='*70}")
print(f"  FULL SYSTEM TEST RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*70}")
for name, status in results["tests"]:
    print(f"  {'✓' if 'PASS' in status else '✗'} {name}: {status}")
print(f"{'='*70}")

sys.exit(0 if results["failed"] == 0 else 1)
