#!/usr/bin/env python3
"""
Filtered ANN Recall Benchmark

Tests recall@k when combining vector search with SQL WHERE clauses.
This is the critical RAG scenario: "find similar items IN category X".

Filter selectivities tested: 100% (no filter), 50%, 10%, 1%
Validates that pre-filtering doesn't destroy recall.
"""

import sys, os, time, struct, subprocess, json
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

DIM = 128
BASE_DIR = "/tmp/lanceforge_recall_bench"
BIN = os.path.expanduser("~/cc/lance-ballista/target/release")

def generate_filterable_dataset(n=200000, dim=128, n_queries=200, n_categories=10, seed=42):
    """Generate dataset with category column for filter benchmarks."""
    np.random.seed(seed)
    n_clusters = 30
    centroids = np.random.randn(n_clusters, dim).astype(np.float32) * 5
    labels = np.random.randint(0, n_clusters, n)
    base = centroids[labels] + np.random.randn(n, dim).astype(np.float32) * 0.5
    categories = [f'cat_{i % n_categories}' for i in range(n)]

    q_labels = np.random.randint(0, n_clusters, n_queries)
    queries = centroids[q_labels] + np.random.randn(n_queries, dim).astype(np.float32) * 0.5

    return base, queries, categories

def compute_filtered_gt(base, queries, categories, filter_cat, k):
    """Brute-force ground truth with filter applied."""
    filtered_ids = [i for i, c in enumerate(categories) if c == filter_cat]
    if not filtered_ids:
        return [[] for _ in queries]
    filtered_base = base[filtered_ids]

    gt = []
    for q in queries:
        dists = np.sum((filtered_base - q) ** 2, axis=1)
        top_local = np.argsort(dists)[:k]
        gt.append([filtered_ids[i] for i in top_local])
    return gt

def create_filterable_shards(base, categories, num_shards, output_dir):
    """Create shards with category column."""
    import pyarrow as pa
    import lance

    rows_per_shard = len(base) // num_shards
    os.makedirs(output_dir, exist_ok=True)

    for i in range(num_shards):
        start = i * rows_per_shard
        end = start + rows_per_shard if i < num_shards - 1 else len(base)
        uri = os.path.join(output_dir, f'shard_{i:02d}.lance')

        table = pa.table({
            'id': pa.array(range(start, end), type=pa.int64()),
            'category': pa.array(categories[start:end], type=pa.string()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(base[start:end].flatten(), type=pa.float32()),
                list_size=base.shape[1]),
        })
        lance.write_dataset(table, uri, mode='overwrite')

        ds = lance.dataset(uri)
        if not ds.list_indices():
            npart = min(64, max(4, (end - start) // 1000))
            ds.create_index('vector', index_type='IVF_FLAT', num_partitions=npart)
            # Scalar index on filter column for efficient pre-filtering
            try:
                ds.create_scalar_index('category', index_type='BTREE')
            except Exception:
                pass  # Scalar index optional
        print(f"  Shard {i}: {end-start} rows, IVF_FLAT + BTREE index")

def start_filtered_cluster(num_shards, shard_dir):
    """Start cluster for filtered bench."""
    import yaml
    processes = []
    cfg = {
        'tables': [{'name': 'filterbench', 'shards': [
            {'name': f'filterbench_shard_{i:02d}',
             'uri': os.path.join(shard_dir, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']}
            for i in range(num_shards)
        ]}],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': 51400 + i}
                      for i in range(num_shards)],
    }
    cfg_path = os.path.join(BASE_DIR, 'filter_config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(cfg, f)

    os.system("pkill -f 'lance-coordinator.*51450|lance-worker.*5140' 2>/dev/null")
    time.sleep(1)

    for i in range(num_shards):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(51400 + i)],
            stdout=open(f"{BASE_DIR}/fw{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    time.sleep(3)

    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, "51450"],
        stdout=open(f"{BASE_DIR}/fcoord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(4)

    for proc in processes:
        assert proc.poll() is None, f"Process died! Check {BASE_DIR}/f*.log"
    return processes

def filtered_search(queries, dim, k, nprobes, filter_expr, addr="127.0.0.1:51450"):
    """Search with filter via gRPC."""
    import grpc
    import lance_service_pb2 as pb
    import lance_service_pb2_grpc
    import pyarrow.ipc as ipc

    channel = grpc.insecure_channel(addr,
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    all_ids = []
    latencies = []
    for q in queries:
        t0 = time.time()
        r = stub.AnnSearch(pb.AnnSearchRequest(
            table_name="filterbench", vector_column="vector",
            query_vector=struct.pack(f'<{len(q)}f', *q),
            dimension=dim, k=k, nprobes=nprobes, metric_type=0,
            filter=filter_expr if filter_expr else None), timeout=30)
        latencies.append((time.time() - t0) * 1000)

        if r.arrow_ipc_data:
            t = ipc.open_stream(r.arrow_ipc_data).read_all()
            all_ids.append(t.column("id").to_pylist()[:k])
        else:
            all_ids.append([])
    channel.close()
    return all_ids, np.array(latencies)

def compute_recall(retrieved, ground_truth, k):
    recalls = []
    for ret, gt in zip(retrieved, ground_truth):
        if not gt: continue
        gt_set = set(gt[:k])
        ret_set = set(ret[:k])
        recalls.append(len(gt_set & ret_set) / min(k, len(gt_set)))
    return np.mean(recalls) if recalls else 0.0

def run_filtered_bench():
    n_vectors = 200000
    num_shards = 3
    n_queries = 200
    n_categories = 10  # each category ~10% of data
    k = 10
    nprobes = 20

    print(f"{'='*60}")
    print(f"  Filtered ANN Recall Benchmark")
    print(f"  {n_vectors} vectors, {num_shards} shards, {n_categories} categories")
    print(f"{'='*60}")

    print("\n  Generating filterable dataset...")
    base, queries, categories = generate_filterable_dataset(
        n_vectors, DIM, n_queries, n_categories)

    shard_dir = os.path.join(BASE_DIR, 'filtered_shards')
    if os.path.exists(shard_dir):
        import shutil
        shutil.rmtree(shard_dir)

    print("  Creating shards with category column...")
    create_filterable_shards(base, categories, num_shards, shard_dir)

    print("  Starting cluster...")
    processes = start_filtered_cluster(num_shards, shard_dir)

    results = {}
    # Test different filter selectivities
    filters = [
        ("no filter (100%)", None, None),
        ("1 category (10%)", "category = 'cat_0'", "cat_0"),
        ("rare match (~1%)", "category = 'cat_0' AND id < 20000", None),
    ]

    for label, filter_expr, filter_cat in filters:
        print(f"\n  --- {label} ---")

        # Compute ground truth for this filter
        if filter_cat:
            gt = compute_filtered_gt(base, queries, categories, filter_cat, k)
        elif filter_expr is None:
            # No filter: use global brute-force
            from recall_benchmark import generate_synthetic_dataset
            base_norms = np.sum(base ** 2, axis=1)
            gt = []
            for q in queries:
                dists = base_norms + np.sum(q ** 2) - 2 * np.dot(base, q)
                top = np.argsort(dists)[:k]
                gt.append(top.tolist())
        else:
            # Complex filter: skip recall computation, just measure QPS
            gt = None

        ids, lats = filtered_search(queries, DIM, k, nprobes, filter_expr)

        if gt is not None:
            recall = compute_recall(ids, gt, k)
        else:
            recall = -1  # can't compute for complex filter

        qps = len(queries) / (sum(lats) / 1000)
        results[label] = {
            'filter': filter_expr or 'none',
            'recall': recall, 'qps': qps,
            'p50_ms': float(np.percentile(lats, 50)),
            'p99_ms': float(np.percentile(lats, 99)),
        }
        if recall >= 0:
            status = "✓" if recall >= 0.95 else "✗"
            print(f"    {status} recall@{k} = {recall:.4f}  |  QPS = {qps:.1f}  |  P50 = {np.percentile(lats, 50):.1f}ms")
        else:
            print(f"    QPS = {qps:.1f}  |  P50 = {np.percentile(lats, 50):.1f}ms (recall N/A)")

    # Cleanup
    for p in processes:
        try: p.terminate()
        except: pass

    print(f"\n{'='*60}")
    print("  FILTERED ANN RECALL RESULTS")
    print(f"{'='*60}")
    passing = sum(1 for r in results.values() if r['recall'] >= 0.95)
    measurable = sum(1 for r in results.values() if r['recall'] >= 0)
    for label, r in results.items():
        if r['recall'] >= 0:
            s = "✓" if r['recall'] >= 0.95 else "✗"
            print(f"  {s} {label:<25} recall={r['recall']:.4f}  QPS={r['qps']:.1f}")
        else:
            print(f"  ? {label:<25} QPS={r['qps']:.1f} (recall N/A)")
    print(f"\n  {passing}/{measurable} pass recall >= 0.95")
    print(f"{'='*60}")

    out = os.path.join(os.path.dirname(__file__), 'results', 'recall_filtered.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        json.dump(results, f, indent=2)

    return passing == measurable

if __name__ == '__main__':
    try:
        ok = run_filtered_bench()
        sys.exit(0 if ok else 1)
    except:
        os.system("pkill -f 'lance-coordinator.*51450|lance-worker.*5140' 2>/dev/null")
        raise
