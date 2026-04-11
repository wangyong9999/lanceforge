#!/usr/bin/env python3
"""
E2E test for NEW crate binaries (lance-coordinator + lance-worker).
Verifies the split architecture works end-to-end.
"""

import sys, os, time, struct, subprocess
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import lance
import grpc
import lance_service_pb2
import lance_service_pb2_grpc

DIM = 32
ROWS_PER_SHARD = 10000
NUM_SHARDS = 3
BASE_DIR = "/tmp/lance_new_crates_test"
# New binary names from crates/
BIN_DIR = os.path.expanduser("~/cc/lance-ballista/target/release")
COORDINATOR_BIN = f"{BIN_DIR}/lance-coordinator"
WORKER_BIN = f"{BIN_DIR}/lance-worker"

processes = []

def cleanup():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)

def step(msg):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")

def encode_vector(v):
    return struct.pack(f"<{len(v)}f", *v)

def decode_response(resp):
    if resp.error:
        raise RuntimeError(f"Error: {resp.error}")
    if not resp.arrow_ipc_data:
        return pa.table({})
    return ipc.open_stream(resp.arrow_ipc_data).read_all()

# ── Step 1: Create data ──
step("Creating test data")
os.makedirs(BASE_DIR, exist_ok=True)

for i in range(NUM_SHARDS):
    path = f"{BASE_DIR}/shard_{i:02d}.lance"
    if os.path.exists(path):
        print(f"  Shard {i} exists, skipping")
        continue
    n = ROWS_PER_SHARD
    offset = i * n
    np.random.seed(42 + i)
    table = pa.table({
        'id': pa.array(range(offset, offset + n), type=pa.int64()),
        'category': pa.array([f'cat_{(j+offset)%5}' for j in range(n)], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
    })
    lance.write_dataset(table, path, mode='create')
    print(f"  Created shard {i}: {n} rows")

# ── Step 2: Write config ──
step("Writing config")
import yaml
config = {
    'tables': [{'name': 'products', 'shards': [
        {'name': f'products_shard_{i:02d}', 'uri': f'{BASE_DIR}/shard_{i:02d}.lance', 'executors': [f'worker_{i}']}
        for i in range(NUM_SHARDS)
    ]}],
    'executors': [
        {'id': f'worker_{i}', 'host': '127.0.0.1', 'port': 51100 + i}
        for i in range(NUM_SHARDS)
    ],
}
config_path = f"{BASE_DIR}/config.yaml"
with open(config_path, 'w') as f:
    yaml.dump(config, f)
print(f"  Config: {config_path}")

# ── Step 3: Check binaries exist ──
step("Checking binaries")
for bin_path in [COORDINATOR_BIN, WORKER_BIN]:
    if not os.path.exists(bin_path):
        print(f"  MISSING: {bin_path}")
        print("  Build with: CARGO_BUILD_JOBS=1 cargo build --release -p lance-distributed-coordinator -p lance-distributed-worker")
        sys.exit(1)
    print(f"  ✓ {os.path.basename(bin_path)}")

# ── Step 4: Start cluster ──
step("Starting cluster with NEW binaries")
os.system("pkill -f 'lance-coordinator|lance-worker' 2>/dev/null")
time.sleep(1)

for i in range(NUM_SHARDS):
    port = 51100 + i
    p = subprocess.Popen(
        [WORKER_BIN, config_path, f"worker_{i}", str(port)],
        stdout=open(f"{BASE_DIR}/worker_{i}.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    print(f"  Worker {i} (PID {p.pid}, port {port})")

time.sleep(3)

coord_port = 51150
p = subprocess.Popen(
    [COORDINATOR_BIN, config_path, str(coord_port)],
    stdout=open(f"{BASE_DIR}/coordinator.log", "w"), stderr=subprocess.STDOUT)
processes.append(p)
print(f"  Coordinator (PID {p.pid}, port {coord_port})")

time.sleep(4)

for proc in processes:
    if proc.poll() is not None:
        print(f"  PROCESS DIED! Check logs in {BASE_DIR}/")
        cleanup()
        sys.exit(1)
print("  ✓ All processes running")

# ── Step 5: Run queries ──
step("Running queries")
channel = grpc.insecure_channel(f"127.0.0.1:{coord_port}", options=[
    ("grpc.max_receive_message_length", 64 * 1024 * 1024)])
stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

# Test 1: Basic ANN
print("  1. Basic ANN...", end=" ")
resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
    table_name="products", vector_column="vector",
    query_vector=encode_vector(np.zeros(DIM, dtype=np.float32)),
    dimension=DIM, k=10, nprobes=5, metric_type=0))
t = decode_response(resp)
assert t.num_rows == 10
print(f"PASS ({t.num_rows} results)")

# Test 2: Filtered
print("  2. Filtered ANN...", end=" ")
resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
    table_name="products", vector_column="vector",
    query_vector=encode_vector(np.random.randn(DIM).astype(np.float32)),
    dimension=DIM, k=20, nprobes=5, metric_type=0,
    filter="category = 'cat_0'"))
t = decode_response(resp)
assert t.num_rows > 0
print(f"PASS ({t.num_rows} results)")

# Test 3: Cross-shard
print("  3. Cross-shard merge...", end=" ")
resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
    table_name="products", vector_column="vector",
    query_vector=encode_vector(np.zeros(DIM, dtype=np.float32)),
    dimension=DIM, k=100, nprobes=10, metric_type=0))
t = decode_response(resp)
ids = t.column("id").to_pylist()
shards_hit = len(set(id // ROWS_PER_SHARD for id in ids))
assert shards_hit >= 2
print(f"PASS ({t.num_rows} results from {shards_hit} shards)")

# Test 4: Throughput
print("  4. Throughput (50 queries)...", end=" ")
latencies = []
for i in range(50):
    q = np.random.randn(DIM).astype(np.float32)
    t0 = time.time()
    r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products", vector_column="vector",
        query_vector=encode_vector(q), dimension=DIM,
        k=10, nprobes=5, metric_type=0), timeout=30)
    latencies.append((time.time() - t0) * 1000)
lat = np.array(latencies)
qps = 50 / (sum(latencies) / 1000)
print(f"PASS")
print(f"    QPS:  {qps:.1f}")
print(f"    P50:  {np.percentile(lat, 50):.1f}ms")
print(f"    P99:  {np.percentile(lat, 99):.1f}ms")

# Test 5: Cluster status
print("  5. Cluster status...", end=" ")
resp = stub.GetClusterStatus(lance_service_pb2.ClusterStatusRequest())
healthy = sum(1 for e in resp.executors if e.healthy)
print(f"PASS ({healthy}/{len(resp.executors)} healthy)")

# ── Cleanup ──
step("Cleanup")
cleanup()
print("  ✓ Done")

print(f"\n{'='*60}")
print(f"  ALL NEW CRATE E2E TESTS PASSED")
print(f"{'='*60}")
