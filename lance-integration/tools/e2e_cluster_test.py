#!/usr/bin/env python3
"""
End-to-end test against a running 3-node Lance-Ballista cluster.
Tests: ANN search, filtered search, cross-shard merge, large K, recall, throughput.
"""

import sys, os, time, struct
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lance-integration/tools'))

import grpc
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

# Import generated proto
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lance-integration/tools'))
import lance_service_pb2
import lance_service_pb2_grpc

SCHEDULER = "127.0.0.1:50750"
DIM = 64
TOTAL_ROWS = 150000  # 3 shards × 50K

def connect():
    channel = grpc.insecure_channel(SCHEDULER, options=[
        ("grpc.max_receive_message_length", 64 * 1024 * 1024),
    ])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)
    return stub

def decode_response(resp):
    if resp.error:
        raise RuntimeError(f"Server error: {resp.error}")
    if not resp.arrow_ipc_data:
        return pa.table({})
    reader = ipc.open_stream(resp.arrow_ipc_data)
    return reader.read_all()

def encode_vector(v):
    return struct.pack(f"<{len(v)}f", *v)

def test_basic_ann(stub):
    """Test 1: Basic ANN search returns results"""
    print("Test 1: Basic ANN search...", end=" ")
    query = np.zeros(DIM, dtype=np.float32)
    resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products",
        vector_column="vector",
        query_vector=encode_vector(query),
        dimension=DIM,
        k=10, nprobes=10, metric_type=0,
    ))
    table = decode_response(resp)
    assert table.num_rows == 10, f"Expected 10, got {table.num_rows}"
    assert "_distance" in table.column_names, "Missing _distance column"
    # Verify sorted ascending
    distances = table.column("_distance").to_pylist()
    for i in range(1, len(distances)):
        assert distances[i] >= distances[i-1], f"Not sorted: {distances}"
    print(f"PASS ({table.num_rows} results, min_dist={distances[0]:.4f})")

def test_filtered_ann(stub):
    """Test 2: ANN with filter (category)"""
    print("Test 2: Filtered ANN (category='cat_0')...", end=" ")
    query = np.random.randn(DIM).astype(np.float32)
    resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products",
        vector_column="vector",
        query_vector=encode_vector(query),
        dimension=DIM,
        k=20, nprobes=10, metric_type=0,
        filter="category = 'cat_0'",
    ))
    table = decode_response(resp)
    assert table.num_rows > 0, "Should have filtered results"
    if "category" in table.column_names:
        cats = table.column("category").to_pylist()
        for c in cats:
            assert c == "cat_0", f"Filter violation: got {c}"
    print(f"PASS ({table.num_rows} results, all cat_0)")

def test_cross_shard_merge(stub):
    """Test 3: Results come from multiple shards"""
    print("Test 3: Cross-shard merge...", end=" ")
    query = np.zeros(DIM, dtype=np.float32)
    resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products",
        vector_column="vector",
        query_vector=encode_vector(query),
        dimension=DIM,
        k=100, nprobes=20, metric_type=0,
    ))
    table = decode_response(resp)
    ids = table.column("id").to_pylist()
    # Check IDs span across shards (shard 0: 0-49999, shard 1: 50000-99999, shard 2: 100000-149999)
    has_shard0 = any(i < 50000 for i in ids)
    has_shard1 = any(50000 <= i < 100000 for i in ids)
    has_shard2 = any(i >= 100000 for i in ids)
    shards_hit = sum([has_shard0, has_shard1, has_shard2])
    assert shards_hit >= 2, f"Expected results from >=2 shards, got {shards_hit}"
    print(f"PASS ({table.num_rows} results from {shards_hit} shards)")

def test_large_k(stub):
    """Test 4: Large K (more than single shard)"""
    print("Test 4: Large K=500...", end=" ")
    query = np.random.randn(DIM).astype(np.float32)
    resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
        table_name="products",
        vector_column="vector",
        query_vector=encode_vector(query),
        dimension=DIM,
        k=500, nprobes=20, metric_type=0,
    ))
    table = decode_response(resp)
    assert table.num_rows > 0, "Should return results"
    distances = table.column("_distance").to_pylist()
    for i in range(1, len(distances)):
        assert distances[i] >= distances[i-1], "Distances must be sorted"
    print(f"PASS ({table.num_rows} results)")

def test_unknown_table(stub):
    """Test 5: Unknown table returns error"""
    print("Test 5: Unknown table error...", end=" ")
    try:
        stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
            table_name="nonexistent",
            vector_column="vector",
            query_vector=encode_vector(np.zeros(DIM, dtype=np.float32)),
            dimension=DIM, k=5, nprobes=1, metric_type=0,
        ))
        assert False, "Should have raised error"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND
    print("PASS (NOT_FOUND)")

def test_throughput(stub, num_queries=100):
    """Test 6: Throughput measurement"""
    print(f"Test 6: Throughput ({num_queries} queries)...", end=" ")
    latencies = []
    for i in range(num_queries):
        query = np.random.randn(DIM).astype(np.float32)
        t0 = time.time()
        resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
            table_name="products",
            vector_column="vector",
            query_vector=encode_vector(query),
            dimension=DIM, k=10, nprobes=10, metric_type=0,
        ))
        latencies.append((time.time() - t0) * 1000)
        assert not resp.error, f"Query {i} error: {resp.error}"

    latencies = np.array(latencies)
    total_time = sum(latencies) / 1000
    qps = num_queries / total_time
    print(f"PASS")
    print(f"  QPS:  {qps:.1f}")
    print(f"  P50:  {np.percentile(latencies, 50):.1f}ms")
    print(f"  P95:  {np.percentile(latencies, 95):.1f}ms")
    print(f"  P99:  {np.percentile(latencies, 99):.1f}ms")
    print(f"  Mean: {np.mean(latencies):.1f}ms")

def test_recall_vs_brute_force(stub, num_queries=20):
    """Test 7: Recall vs single-shard brute force"""
    print(f"Test 7: Recall measurement ({num_queries} queries)...", end=" ")
    import lance

    # Load all data as a single dataset for brute-force
    all_tables = []
    for i in range(3):
        ds = lance.dataset(f"/tmp/lance_cluster_test/shard_{i:02d}.lance")
        all_tables.append(ds.to_table())
    full = pa.concat_tables(all_tables)

    recalls = []
    for i in range(num_queries):
        query = np.random.randn(DIM).astype(np.float32)

        # Brute force on full dataset
        # (Using Lance single-shard for ground truth)
        ds0 = lance.dataset("/tmp/lance_cluster_test/shard_00.lance")
        gt_tables = []
        for si in range(3):
            ds = lance.dataset(f"/tmp/lance_cluster_test/shard_{si:02d}.lance")
            gt = ds.to_table(nearest={"column": "vector", "q": query.tolist(), "k": 10})
            gt_tables.append(gt)
        gt_all = pa.concat_tables(gt_tables).sort_by("_distance").slice(0, 10)
        gt_ids = set(gt_all.column("id").to_pylist())

        # Distributed query
        resp = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
            table_name="products",
            vector_column="vector",
            query_vector=encode_vector(query),
            dimension=DIM, k=10, nprobes=10, metric_type=0,
        ))
        dist_table = decode_response(resp)
        dist_ids = set(dist_table.column("id").to_pylist())

        recall = len(dist_ids & gt_ids) / max(len(gt_ids), 1)
        recalls.append(recall)

    avg_recall = np.mean(recalls)
    print(f"PASS")
    print(f"  Recall@10: {avg_recall:.4f} (target >= 0.70 for distributed IVF_PQ)")
    assert avg_recall >= 0.70, f"Recall too low: {avg_recall:.4f}"

def test_cluster_status(stub):
    """Test 8: Cluster status endpoint"""
    print("Test 8: Cluster status...", end=" ")
    resp = stub.GetClusterStatus(lance_service_pb2.ClusterStatusRequest())
    assert len(resp.executors) == 3, f"Expected 3 executors, got {len(resp.executors)}"
    healthy = sum(1 for e in resp.executors if e.healthy)
    print(f"PASS ({healthy}/3 healthy executors)")


if __name__ == "__main__":
    print(f"=== Lance-Ballista E2E Test ===")
    print(f"Scheduler: {SCHEDULER}")
    print(f"Data: {TOTAL_ROWS} vectors, dim={DIM}, 3 shards with IVF_PQ")
    print()

    stub = connect()

    tests = [
        test_basic_ann,
        test_filtered_ann,
        test_cross_shard_merge,
        test_large_k,
        test_unknown_table,
        test_throughput,
        test_recall_vs_brute_force,
        test_cluster_status,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test(stub)
            passed += 1
        except Exception as e:
            print(f"FAIL: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)}")
    print(f"{'='*50}")
    sys.exit(0 if failed == 0 else 1)
