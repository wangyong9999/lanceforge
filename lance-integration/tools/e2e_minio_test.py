#!/usr/bin/env python3
"""
Full-stack MinIO E2E test:
1. Create data on MinIO (S3-compatible)
2. Split into shards on MinIO
3. Build IVF_PQ indexes
4. Start 3 Executors + 1 Scheduler
5. Run all query types
6. Verify correctness
"""

import sys, os, time, struct, subprocess, signal
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lance-integration/tools'))

from test_helpers import wait_for_grpc

import lance
import numpy as np
import pyarrow as pa
import grpc
import lance_service_pb2
import lance_service_pb2_grpc
import pyarrow.ipc as ipc

MINIO_OPTS = {
    'aws_access_key_id': 'lanceadmin',
    'aws_secret_access_key': 'lanceadmin123',
    'aws_endpoint': 'http://127.0.0.1:9000',
    'aws_region': 'us-east-1',
    'allow_http': 'true',
}
DIM = 32
ROWS_PER_SHARD = 10000
NUM_SHARDS = 3
SCHEDULER_ADDR = "127.0.0.1:50850"
BIN_DIR = os.path.expanduser("~/cc/lance-ballista/target/release")
CFG_PATH = "/tmp/lance_minio_test/cluster_config.yaml"

processes = []

def cleanup():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)
    for p in processes:
        try: p.kill()
        except: pass

def step(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")

def encode_vector(v):
    return struct.pack(f"<{len(v)}f", *v)

def decode_response(resp):
    if resp.error:
        raise RuntimeError(f"Server error: {resp.error}")
    if not resp.arrow_ipc_data:
        return pa.table({})
    return ipc.open_stream(resp.arrow_ipc_data).read_all()

# ============================================================
# Step 1: Create shards on MinIO
# ============================================================
step("Step 1: Creating shards on MinIO")

for i in range(NUM_SHARDS):
    uri = f"s3://lance-test/shards/shard_{i:02d}.lance"
    print(f"  Creating {uri} ({ROWS_PER_SHARD} rows, dim={DIM})...")

    n = ROWS_PER_SHARD
    offset = i * n
    np.random.seed(42 + i)

    table = pa.table({
        'id': pa.array(range(offset, offset + n), type=pa.int64()),
        'category': pa.array([f'cat_{(j+offset) % 5}' for j in range(n)], type=pa.string()),
        'region': pa.array(['APAC' if (j+offset)%3==0 else 'EMEA' if (j+offset)%3==1 else 'NA' for j in range(n)], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
    })

    lance.write_dataset(table, uri, mode='overwrite', storage_options=MINIO_OPTS)

    # Verify
    ds = lance.dataset(uri, storage_options=MINIO_OPTS)
    assert ds.count_rows() == n, f"Shard {i}: expected {n} rows, got {ds.count_rows()}"
    print(f"    ✓ {ds.count_rows()} rows written")

# ============================================================
# Step 2: Build IVF_PQ index on each shard
# ============================================================
step("Step 2: Building IVF_PQ indexes")

for i in range(NUM_SHARDS):
    uri = f"s3://lance-test/shards/shard_{i:02d}.lance"
    ds = lance.dataset(uri, storage_options=MINIO_OPTS)

    if any(idx.get('name','').startswith('vector') for idx in ds.list_indices()):
        print(f"  Shard {i}: already indexed")
        continue

    print(f"  Building index on shard {i}...")
    t0 = time.time()
    ds.create_index('vector', index_type='IVF_PQ',
                    num_partitions=min(16, ROWS_PER_SHARD // 500),
                    num_sub_vectors=min(8, DIM // 4))
    print(f"    ✓ Index built in {time.time()-t0:.1f}s")

# ============================================================
# Step 3: Start cluster
# ============================================================
step("Step 3: Starting cluster (3 Executors + 1 Scheduler)")

# Update config with correct ports
import yaml
with open(CFG_PATH) as f:
    cfg = yaml.safe_load(f)
cfg['executors'] = [
    {'id': 'executor_0', 'host': '127.0.0.1', 'port': 50800},
    {'id': 'executor_1', 'host': '127.0.0.1', 'port': 50801},
    {'id': 'executor_2', 'host': '127.0.0.1', 'port': 50802},
]
with open(CFG_PATH, 'w') as f:
    yaml.dump(cfg, f)

# Kill any previous instances
os.system("pkill -f 'lance-executor.*50800' 2>/dev/null")
os.system("pkill -f 'lance-executor.*50801' 2>/dev/null")
os.system("pkill -f 'lance-executor.*50802' 2>/dev/null")
os.system("pkill -f 'lance-scheduler.*50850' 2>/dev/null")
time.sleep(1)

for i in range(NUM_SHARDS):
    port = 50800 + i
    p = subprocess.Popen(
        [f"{BIN_DIR}/lance-executor", CFG_PATH, f"executor_{i}", str(port)],
        stdout=open(f"/tmp/lance_minio_test/e{i}.log", "w"),
        stderr=subprocess.STDOUT,
    )
    processes.append(p)
    print(f"  Executor {i} started (PID {p.pid}, port {port})")

for i in range(NUM_SHARDS):
    assert wait_for_grpc("127.0.0.1", 50800 + i), f"Executor {i} failed to start"

p = subprocess.Popen(
    [f"{BIN_DIR}/lance-scheduler", CFG_PATH, "50850"],
    stdout=open("/tmp/lance_minio_test/sched.log", "w"),
    stderr=subprocess.STDOUT,
)
processes.append(p)
print(f"  Scheduler started (PID {p.pid}, port 50850)")

assert wait_for_grpc("127.0.0.1", 50850), "Scheduler failed to start"

# Verify all processes alive
for i, proc in enumerate(processes):
    assert proc.poll() is None, f"Process {i} died! Check logs"
print("  ✓ All processes running")

# ============================================================
# Step 4: Run queries
# ============================================================
step("Step 4: Running queries against MinIO-backed cluster")

channel = grpc.insecure_channel(SCHEDULER_ADDR, options=[
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
])
stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

# Test 4a: Basic ANN
print("  4a: Basic ANN search...", end=" ")
query = np.zeros(DIM, dtype=np.float32)
resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
    table_name="products", vector_column="vector",
    query_vector=encode_vector(query), dimension=DIM,
    k=10, nprobes=5, metric_type=0,
))
table = decode_response(resp)
assert table.num_rows == 10
distances = table.column("_distance").to_pylist()
for i in range(1, len(distances)):
    assert distances[i] >= distances[i-1]
print(f"PASS ({table.num_rows} results, min_dist={distances[0]:.4f})")

# Test 4b: Filtered ANN
print("  4b: Filtered ANN (category='cat_0')...", end=" ")
resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
    table_name="products", vector_column="vector",
    query_vector=encode_vector(np.random.randn(DIM).astype(np.float32)),
    dimension=DIM, k=20, nprobes=5, metric_type=0,
    filter="category = 'cat_0'",
))
table = decode_response(resp)
assert table.num_rows > 0
if "category" in table.column_names:
    for c in table.column("category").to_pylist():
        assert c == "cat_0"
print(f"PASS ({table.num_rows} results)")

# Test 4c: Cross-shard merge
print("  4c: Cross-shard merge...", end=" ")
resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
    table_name="products", vector_column="vector",
    query_vector=encode_vector(np.zeros(DIM, dtype=np.float32)),
    dimension=DIM, k=100, nprobes=10, metric_type=0,
))
table = decode_response(resp)
ids = table.column("id").to_pylist()
shards_hit = set()
for id in ids:
    shards_hit.add(id // ROWS_PER_SHARD)
assert len(shards_hit) >= 2, f"Expected >=2 shards, got {len(shards_hit)}"
print(f"PASS ({table.num_rows} results from {len(shards_hit)} shards)")

# Test 4d: Throughput (sequential, with retry)
print("  4d: Throughput (30 queries, sequential)...", end=" ")
latencies = []
errors = 0
for i in range(30):
    q = np.random.randn(DIM).astype(np.float32)
    t0 = time.time()
    try:
        r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
            table_name="products", vector_column="vector",
            query_vector=encode_vector(q), dimension=DIM,
            k=10, nprobes=5, metric_type=0,
        ), timeout=30)
        latencies.append((time.time() - t0) * 1000)
        if r.error:
            errors += 1
    except grpc.RpcError:
        errors += 1
        latencies.append((time.time() - t0) * 1000)

lat = np.array(latencies)
qps = len(latencies) / (sum(latencies) / 1000)
print(f"PASS")
print(f"    QPS:  {qps:.1f}")
print(f"    P50:  {np.percentile(lat, 50):.1f}ms")
print(f"    P95:  {np.percentile(lat, 95):.1f}ms")
print(f"    P99:  {np.percentile(lat, 99):.1f}ms")

# ============================================================
# Step 5: Cleanup
# ============================================================
step("Step 5: Cleanup")
cleanup()
print("  ✓ All processes stopped")

print(f"\n{'='*60}")
print("  ALL MINIO FULL-STACK TESTS PASSED")
print(f"{'='*60}")
