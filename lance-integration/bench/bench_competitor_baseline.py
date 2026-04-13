#!/usr/bin/env python3
"""
Competitor Baseline Benchmark

Runs the SAME workload on the SAME machine across:
  - LanceForge (our system)
  - Qdrant (standalone binary)
  - Milvus Lite (embedded, via pymilvus)

Workload: 200K×128d vectors, 500 queries
Metrics: recall@10, QPS, P50 latency
Scenarios: unfiltered ANN, filtered ANN (10% selectivity), concurrent (20 parallel)
"""

import sys, os, time, struct, subprocess, json
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
DIM = 128
N_VECTORS = 200000
N_QUERIES = 300
N_CATEGORIES = 10
SEED = 42

# ── Data Generation ──

def generate_data():
    np.random.seed(SEED)
    n_clusters = 50
    centroids = np.random.randn(n_clusters, DIM).astype(np.float32) * 5
    labels = np.random.randint(0, n_clusters, N_VECTORS)
    base = centroids[labels] + np.random.randn(N_VECTORS, DIM).astype(np.float32) * 0.5
    categories = [f'cat_{i % N_CATEGORIES}' for i in range(N_VECTORS)]

    q_labels = np.random.randint(0, n_clusters, N_QUERIES)
    queries = centroids[q_labels] + np.random.randn(N_QUERIES, DIM).astype(np.float32) * 0.3

    # Ground truth (brute-force L2)
    print("  Computing ground truth...")
    base_norms = np.sum(base ** 2, axis=1)
    gt_unfiltered = []
    gt_filtered = []
    for qi, q in enumerate(queries):
        dists = base_norms + np.sum(q ** 2) - 2 * np.dot(base, q)
        gt_unfiltered.append(np.argsort(dists)[:100].tolist())
        # Filtered GT (cat_0 only)
        filtered_ids = [i for i in range(N_VECTORS) if categories[i] == 'cat_0']
        f_dists = dists[filtered_ids]
        f_top = np.argsort(f_dists)[:100]
        gt_filtered.append([filtered_ids[i] for i in f_top])
        if (qi + 1) % 100 == 0:
            print(f"    {qi+1}/{N_QUERIES}...")

    return base, queries, categories, gt_unfiltered, gt_filtered

def compute_recall(retrieved, ground_truth, k):
    recalls = []
    for ret, gt in zip(retrieved, ground_truth):
        gt_set = set(gt[:k])
        ret_set = set(ret[:k])
        recalls.append(len(gt_set & ret_set) / k if k > 0 else 0)
    return np.mean(recalls)

# ── Qdrant Benchmark ──

def bench_qdrant(base, queries, categories, gt_unfiltered, gt_filtered):
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        VectorParams, Distance, PointStruct, Filter,
        FieldCondition, MatchValue, PayloadSchemaType
    )

    print("\n  [Qdrant] Setting up...")
    qdrant_dir = "/tmp/qdrant_bench_data"
    os.makedirs(qdrant_dir, exist_ok=True)

    # Start Qdrant server with config file
    os.makedirs(os.path.join(qdrant_dir, "storage"), exist_ok=True)
    os.makedirs(os.path.join(qdrant_dir, "snapshots"), exist_ok=True)
    config_content = f"""
storage:
  storage_path: {qdrant_dir}/storage
  snapshots_path: {qdrant_dir}/snapshots
service:
  http_port: 6333
  grpc_port: 6334
"""
    config_path = os.path.join(qdrant_dir, "config.yaml")
    with open(config_path, 'w') as f:
        f.write(config_content)
    qdrant_proc = subprocess.Popen(
        ["/tmp/qdrant", "--config-path", config_path],
        stdout=open("/tmp/qdrant_bench.log", "w"),
        stderr=subprocess.STDOUT)
    time.sleep(3)

    client = QdrantClient(host="127.0.0.1", port=6333, timeout=60)

    # Create collection
    try:
        client.delete_collection("bench")
    except:
        pass
    client.create_collection(
        "bench",
        vectors_config=VectorParams(size=DIM, distance=Distance.EUCLID),
    )

    # Insert data in batches
    print("  [Qdrant] Inserting data...")
    batch_size = 10000
    t0 = time.time()
    for start in range(0, N_VECTORS, batch_size):
        end = min(start + batch_size, N_VECTORS)
        points = [
            PointStruct(
                id=i,
                vector=base[i].tolist(),
                payload={"category": categories[i]},
            )
            for i in range(start, end)
        ]
        client.upsert("bench", points)
    insert_time = time.time() - t0
    print(f"  [Qdrant] Inserted {N_VECTORS} vectors in {insert_time:.1f}s")

    # Create payload index for filtered search
    client.create_payload_index("bench", "category", PayloadSchemaType.KEYWORD)

    # Wait for indexing
    time.sleep(2)
    print(f"  [Qdrant] Collection ready: {client.get_collection('bench').points_count} points")

    results = {}

    # Unfiltered ANN
    print("  [Qdrant] Unfiltered ANN...")
    all_ids, latencies = [], []
    for q in queries:
        t0 = time.time()
        hits = client.query_points("bench", query=q.tolist(), limit=10).points
        latencies.append((time.time() - t0) * 1000)
        all_ids.append([h.id for h in hits])
    recall = compute_recall(all_ids, gt_unfiltered, 10)
    lats = np.array(latencies)
    qps = len(queries) / (sum(latencies) / 1000)
    results['unfiltered'] = {'recall': recall, 'qps': qps, 'p50_ms': float(np.percentile(lats, 50))}
    print(f"    recall@10={recall:.4f}  QPS={qps:.1f}  P50={np.percentile(lats, 50):.1f}ms")

    # Filtered ANN (10% — cat_0)
    print("  [Qdrant] Filtered ANN (10%)...")
    all_ids, latencies = [], []
    for q in queries:
        t0 = time.time()
        hits = client.query_points(
            "bench", query=q.tolist(), limit=10,
            query_filter=Filter(must=[
                FieldCondition(key="category", match=MatchValue(value="cat_0"))
            ])
        ).points
        latencies.append((time.time() - t0) * 1000)
        all_ids.append([h.id for h in hits])
    recall = compute_recall(all_ids, gt_filtered, 10)
    lats = np.array(latencies)
    qps = len(queries) / (sum(latencies) / 1000)
    results['filtered_10pct'] = {'recall': recall, 'qps': qps, 'p50_ms': float(np.percentile(lats, 50))}
    print(f"    recall@10={recall:.4f}  QPS={qps:.1f}  P50={np.percentile(lats, 50):.1f}ms")

    # Concurrent (20 parallel)
    print("  [Qdrant] Concurrent (20 parallel)...")
    import concurrent.futures
    concurrent_latencies = []
    def search_fn(i):
        t0 = time.time()
        client.query_points("bench", query=queries[i % len(queries)].tolist(), limit=10)
        return (time.time() - t0) * 1000
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futs = [pool.submit(search_fn, i) for i in range(100)]
        for f in concurrent.futures.as_completed(futs):
            concurrent_latencies.append(f.result())
    clats = np.array(concurrent_latencies)
    cqps = 100 / (sum(concurrent_latencies) / 1000)
    results['concurrent_20'] = {'qps': cqps, 'p50_ms': float(np.percentile(clats, 50))}
    print(f"    QPS={cqps:.1f}  P50={np.percentile(clats, 50):.1f}ms")

    client.close()
    qdrant_proc.terminate()
    time.sleep(1)

    return results

# ── LanceForge Benchmark ──

def bench_lanceforge(base, queries, categories, gt_unfiltered, gt_filtered):
    import lance, pyarrow as pa, yaml

    BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
    base_dir = "/tmp/lanceforge_competitor_bench"
    os.makedirs(base_dir, exist_ok=True)

    print("\n  [LanceForge] Setting up...")

    # Create sharded data
    for i in range(2):
        uri = os.path.join(base_dir, f"shard_{i:02d}.lance")
        n = N_VECTORS // 2
        off = i * n
        table = pa.table({
            'id': pa.array(range(off, off + n), type=pa.int64()),
            'category': pa.array(categories[off:off+n], type=pa.string()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(base[off:off+n].flatten(), type=pa.float32()), list_size=DIM),
        })
        lance.write_dataset(table, uri, mode='overwrite')
        ds = lance.dataset(uri)
        ds.create_index('vector', index_type='IVF_FLAT', num_partitions=32)
        try:
            ds.create_scalar_index('category', index_type='BTREE')
        except:
            pass

    # Cluster config
    cfg = {
        'tables': [{'name': 'bench', 'shards': [
            {'name': f'bench_shard_{i:02d}',
             'uri': os.path.join(base_dir, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']} for i in range(2)
        ]}],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': 52200+i} for i in range(2)],
    }
    cfg_path = os.path.join(base_dir, 'config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(cfg, f)

    os.system("pkill -f 'lance-coordinator.*52300|lance-worker.*5220' 2>/dev/null")
    time.sleep(1)

    procs = []
    for i in range(2):
        p = subprocess.Popen([f"{BIN}/lance-worker", cfg_path, f"w{i}", str(52200+i)],
            stdout=open(f"{base_dir}/w{i}.log","w"), stderr=subprocess.STDOUT)
        procs.append(p)
    time.sleep(3)
    p = subprocess.Popen([f"{BIN}/lance-coordinator", cfg_path, "52300"],
        stdout=open(f"{base_dir}/coord.log","w"), stderr=subprocess.STDOUT)
    procs.append(p)
    time.sleep(4)

    from lanceforge import LanceForgeClient
    client = LanceForgeClient("127.0.0.1:52300")

    results = {}

    # Unfiltered ANN
    print("  [LanceForge] Unfiltered ANN...")
    all_ids, latencies = [], []
    for q in queries:
        t0 = time.time()
        r = client.search("bench", query_vector=q.tolist(), k=10, nprobes=20)
        latencies.append((time.time() - t0) * 1000)
        all_ids.append(r.column("id").to_pylist()[:10])
    recall = compute_recall(all_ids, gt_unfiltered, 10)
    lats = np.array(latencies)
    qps = len(queries) / (sum(latencies) / 1000)
    results['unfiltered'] = {'recall': recall, 'qps': qps, 'p50_ms': float(np.percentile(lats, 50))}
    print(f"    recall@10={recall:.4f}  QPS={qps:.1f}  P50={np.percentile(lats, 50):.1f}ms")

    # Filtered ANN (10%)
    print("  [LanceForge] Filtered ANN (10%)...")
    all_ids, latencies = [], []
    for q in queries:
        t0 = time.time()
        r = client.search("bench", query_vector=q.tolist(), k=10, nprobes=20,
                          filter="category = 'cat_0'")
        latencies.append((time.time() - t0) * 1000)
        all_ids.append(r.column("id").to_pylist()[:10])
    recall = compute_recall(all_ids, gt_filtered, 10)
    lats = np.array(latencies)
    qps = len(queries) / (sum(latencies) / 1000)
    results['filtered_10pct'] = {'recall': recall, 'qps': qps, 'p50_ms': float(np.percentile(lats, 50))}
    print(f"    recall@10={recall:.4f}  QPS={qps:.1f}  P50={np.percentile(lats, 50):.1f}ms")

    # Concurrent (20 parallel)
    print("  [LanceForge] Concurrent (20 parallel)...")
    import concurrent.futures
    concurrent_latencies = []
    def search_fn(i):
        c = LanceForgeClient("127.0.0.1:52300")
        t0 = time.time()
        c.search("bench", query_vector=queries[i % len(queries)].tolist(), k=10)
        lat = (time.time() - t0) * 1000
        c.close()
        return lat
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futs = [pool.submit(search_fn, i) for i in range(100)]
        for f in concurrent.futures.as_completed(futs):
            concurrent_latencies.append(f.result())
    clats = np.array(concurrent_latencies)
    cqps = 100 / (sum(concurrent_latencies) / 1000)
    results['concurrent_20'] = {'qps': cqps, 'p50_ms': float(np.percentile(clats, 50))}
    print(f"    QPS={cqps:.1f}  P50={np.percentile(clats, 50):.1f}ms")

    client.close()
    for p in procs:
        try: p.terminate()
        except: pass

    return results

# ── Main ──

def main():
    print("=" * 70)
    print("  COMPETITOR BASELINE BENCHMARK")
    print(f"  {N_VECTORS/1e3:.0f}K vectors × {DIM}d, {N_QUERIES} queries, {N_CATEGORIES} categories")
    print("=" * 70)

    print("\n  Generating shared dataset + ground truth...")
    base, queries, categories, gt_unfiltered, gt_filtered = generate_data()

    all_results = {}

    # Qdrant
    try:
        all_results['Qdrant'] = bench_qdrant(base, queries, categories, gt_unfiltered, gt_filtered)
    except Exception as e:
        print(f"  [Qdrant] FAILED: {e}")
        all_results['Qdrant'] = {'error': str(e)}

    # LanceForge
    try:
        all_results['LanceForge'] = bench_lanceforge(base, queries, categories, gt_unfiltered, gt_filtered)
    except Exception as e:
        print(f"  [LanceForge] FAILED: {e}")
        all_results['LanceForge'] = {'error': str(e)}

    # Summary
    print(f"\n\n{'='*70}")
    print("  BASELINE COMPARISON")
    print(f"{'='*70}")
    print(f"\n  {'System':<15} {'Scenario':<20} {'Recall@10':>10} {'QPS':>8} {'P50(ms)':>10}")
    print(f"  {'-'*15} {'-'*20} {'-'*10} {'-'*8} {'-'*10}")

    for system, data in all_results.items():
        if 'error' in data:
            print(f"  {system:<15} {'ERROR':<20} {data['error']}")
            continue
        for scenario in ['unfiltered', 'filtered_10pct', 'concurrent_20']:
            if scenario in data:
                r = data[scenario]
                recall_str = f"{r.get('recall', '-'):>10.4f}" if 'recall' in r else f"{'—':>10}"
                print(f"  {system:<15} {scenario:<20} {recall_str} {r['qps']:>8.1f} {r['p50_ms']:>10.1f}")

    print(f"{'='*70}")

    # Save
    os.makedirs(RESULTS_DIR, exist_ok=True)
    out = os.path.join(RESULTS_DIR, 'competitor_baseline.json')
    with open(out, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n  Results saved to {out}")

if __name__ == '__main__':
    try:
        main()
    except:
        os.system("pkill -f 'qdrant|lance-coordinator.*52300|lance-worker.*5220' 2>/dev/null")
        raise
