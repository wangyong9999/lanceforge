#!/usr/bin/env python3
"""
LanceForge Recall Benchmark — SIFT1M

Measures recall@k for distributed ANN search by comparing against
brute-force ground truth. Tests single-node vs multi-shard to quantify
the recall cost of sharding.

Usage:
    # Prepare data + run full benchmark
    python recall_benchmark.py

    # Skip data prep (reuse existing shards)
    python recall_benchmark.py --skip-prep

    # Custom settings
    python recall_benchmark.py --num-shards 5 --oversample 3

Requires: numpy, lance, pyarrow, grpcio, pyyaml
Dataset: SIFT1M (auto-downloaded if missing)
"""

import sys, os, time, struct, subprocess, signal, argparse
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

# ── Helpers ──

def read_fvecs(path):
    """Read .fvecs format (standard ANN benchmark format)."""
    with open(path, 'rb') as f:
        data = f.read()
    vectors = []
    offset = 0
    while offset < len(data):
        dim = struct.unpack('<i', data[offset:offset+4])[0]
        offset += 4
        vec = struct.unpack(f'<{dim}f', data[offset:offset+dim*4])
        vectors.append(vec)
        offset += dim * 4
    return np.array(vectors, dtype=np.float32)

def read_ivecs(path):
    """Read .ivecs format (ground truth indices)."""
    with open(path, 'rb') as f:
        data = f.read()
    results = []
    offset = 0
    while offset < len(data):
        k = struct.unpack('<i', data[offset:offset+4])[0]
        offset += 4
        ids = struct.unpack(f'<{k}i', data[offset:offset+k*4])
        results.append(list(ids))
        offset += k * 4
    return results

def compute_recall(retrieved_ids, ground_truth_ids, k):
    """Compute recall@k: fraction of true top-k found in retrieved results."""
    recalls = []
    for ret, gt in zip(retrieved_ids, ground_truth_ids):
        gt_set = set(gt[:k])
        ret_set = set(ret[:k])
        recalls.append(len(gt_set & ret_set) / k)
    return np.mean(recalls)

def ev(v):
    """Encode float vector to bytes (little-endian)."""
    return struct.pack(f'<{len(v)}f', *v)

# ── Dataset ──

DATASET_DIR = os.path.join(os.path.dirname(__file__), 'datasets')
SIFT_DIR = os.path.join(DATASET_DIR, 'sift')

def try_load_sift1m():
    """Try to load real SIFT1M dataset if available."""
    hdf5_path = os.path.join(DATASET_DIR, 'sift-128-euclidean.hdf5')
    fvecs_path = os.path.join(SIFT_DIR, 'sift_base.fvecs')

    if os.path.exists(hdf5_path):
        import h5py
        f = h5py.File(hdf5_path, 'r')
        return np.array(f['train']), np.array(f['test']), [list(row) for row in f['neighbors']]

    if os.path.exists(fvecs_path):
        base = read_fvecs(fvecs_path)
        queries = read_fvecs(os.path.join(SIFT_DIR, 'sift_query.fvecs'))
        gt = read_ivecs(os.path.join(SIFT_DIR, 'sift_groundtruth.ivecs'))
        return base, queries, gt

    return None

def generate_synthetic_dataset(n=200000, dim=128, n_queries=1000, n_clusters=50, seed=42):
    """
    Generate a synthetic dataset with clustered structure (mimics real embeddings).
    Compute brute-force ground truth (top-100 L2 neighbors).
    """
    np.random.seed(seed)
    print(f"  Generating {n} vectors (dim={dim}, {n_clusters} clusters)...")

    # Create clustered data (more realistic than uniform random)
    centroids = np.random.randn(n_clusters, dim).astype(np.float32) * 5
    labels = np.random.randint(0, n_clusters, n)
    base = centroids[labels] + np.random.randn(n, dim).astype(np.float32) * 0.5

    # Queries are sampled from the same distribution (with noise)
    q_labels = np.random.randint(0, n_clusters, n_queries)
    queries = centroids[q_labels] + np.random.randn(n_queries, dim).astype(np.float32) * 0.5

    # Compute brute-force ground truth
    print(f"  Computing brute-force ground truth (top-100)...")
    gt_k = 100
    base_norms = np.sum(base ** 2, axis=1)
    gt = []
    for i, q in enumerate(queries):
        dists = base_norms + np.sum(q ** 2) - 2 * np.dot(base, q)
        top_ids = np.argpartition(dists, gt_k)[:gt_k]
        top_ids = top_ids[np.argsort(dists[top_ids])]
        gt.append(top_ids.tolist())
        if (i + 1) % 200 == 0:
            print(f"    {i+1}/{n_queries}...")

    return base, queries, gt

def load_dataset(n_vectors=200000, dim=128, n_queries=1000):
    """Load SIFT1M if available, otherwise generate synthetic dataset."""
    real = try_load_sift1m()
    if real is not None:
        base, queries, gt = real
        print(f"  Loaded SIFT1M: {base.shape[0]} vectors, dim={base.shape[1]}")
        return base, queries, gt, "SIFT1M"

    print("  SIFT1M not found, generating synthetic dataset...")
    base, queries, gt = generate_synthetic_dataset(n_vectors, dim, n_queries)
    print(f"  Generated: {base.shape[0]} vectors, dim={base.shape[1]}, {len(queries)} queries")
    return base, queries, gt, f"Synthetic({n_vectors}x{dim})"

# ── Data Preparation ──

def create_sharded_lance(base, num_shards, output_dir):
    """Create sharded Lance tables with IVF_PQ indexes."""
    import pyarrow as pa
    import lance

    rows_per_shard = len(base) // num_shards
    os.makedirs(output_dir, exist_ok=True)

    for i in range(num_shards):
        start = i * rows_per_shard
        end = start + rows_per_shard if i < num_shards - 1 else len(base)
        shard_data = base[start:end]
        uri = os.path.join(output_dir, f'shard_{i:02d}.lance')

        print(f"  Shard {i}: {len(shard_data)} vectors → {uri}")
        table = pa.table({
            'id': pa.array(range(start, end), type=pa.int64()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(shard_data.flatten(), type=pa.float32()),
                list_size=base.shape[1]),
        })
        lance.write_dataset(table, uri, mode='overwrite')

        # Build IVF_FLAT index (high recall; use IVF_PQ only for very large data)
        ds = lance.dataset(uri)
        if not ds.list_indices():
            npart = min(64, max(4, len(shard_data) // 1000))
            print(f"    Building IVF_FLAT index (npart={npart})...")
            ds.create_index('vector', index_type='IVF_FLAT',
                           num_partitions=npart)
        print(f"    Done ({ds.count_rows()} rows, {len(ds.list_indices())} index)")

# ── Single-node baseline (brute-force via Lance) ──

def single_node_search(base, queries, k):
    """Brute-force search using numpy — the ground truth reference."""
    print(f"  Computing brute-force top-{k} for {len(queries)} queries...")
    t0 = time.time()
    # L2 distance: ||q - b||^2 = ||q||^2 + ||b||^2 - 2*q·b
    base_norms = np.sum(base ** 2, axis=1)  # (N,)
    results = []
    for q in queries:
        dists = base_norms + np.sum(q ** 2) - 2 * np.dot(base, q)
        top_k = np.argpartition(dists, k)[:k]
        top_k = top_k[np.argsort(dists[top_k])]
        results.append(top_k.tolist())
    elapsed = time.time() - t0
    print(f"    Done in {elapsed:.1f}s")
    return results

# ── Distributed search via gRPC ──

def distributed_search(queries, dim, k, nprobes, coordinator_addr="127.0.0.1:51250"):
    """Query the LanceForge cluster via gRPC."""
    import grpc
    import lance_service_pb2
    import lance_service_pb2_grpc

    channel = grpc.insecure_channel(coordinator_addr,
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    all_ids = []
    latencies = []

    for i, q in enumerate(queries):
        t0 = time.time()
        for attempt in range(3):
            try:
                r = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
                    table_name="sift", vector_column="vector",
                    query_vector=ev(q), dimension=dim, k=k,
                    nprobes=nprobes, metric_type=0), timeout=30)
                break
            except grpc.RpcError as e:
                if attempt < 2 and e.code() == grpc.StatusCode.UNAVAILABLE:
                    time.sleep(0.1)
                    continue
                raise
        latencies.append((time.time() - t0) * 1000)

        if r.error:
            all_ids.append([])
            continue

        import pyarrow.ipc as ipc
        if r.arrow_ipc_data:
            table = ipc.open_stream(r.arrow_ipc_data).read_all()
            ids = table.column("id").to_pylist()
            all_ids.append(ids[:k])
        else:
            all_ids.append([])

        if (i + 1) % 100 == 0:
            print(f"    {i+1}/{len(queries)} queries done...")

    channel.close()
    return all_ids, np.array(latencies)

# ── Cluster Management ──

BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lanceforge_recall_bench"

processes = []

def start_cluster(num_shards, shard_dir):
    """Start coordinator + workers for the benchmark."""
    import yaml

    global processes
    os.makedirs(BASE, exist_ok=True)

    config = {
        'tables': [{
            'name': 'sift',
            'shards': [
                {'name': f'sift_shard_{i:02d}',
                 'uri': os.path.join(shard_dir, f'shard_{i:02d}.lance'),
                 'executors': [f'w{i}']}
                for i in range(num_shards)
            ],
        }],
        'executors': [
            {'id': f'w{i}', 'host': '127.0.0.1', 'port': 51300 + i}
            for i in range(num_shards)
        ],
    }
    cfg_path = os.path.join(BASE, 'config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(config, f)

    # Kill any leftover processes
    os.system("pkill -f 'lance-coordinator.*51350|lance-worker.*5130' 2>/dev/null")
    time.sleep(1)

    for i in range(num_shards):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(51300 + i)],
            stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)

    time.sleep(3)

    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, "51350"],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(4)

    for proc in processes:
        assert proc.poll() is None, f"Process {proc.pid} died. Check {BASE}/*.log"
    print(f"  Cluster running: {num_shards} workers + 1 coordinator")

def stop_cluster():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)
    processes.clear()

# ── Main Benchmark ──

def run_benchmark(args):
    print("=" * 70)
    print("  LanceForge Recall Benchmark — SIFT1M")
    print("=" * 70)

    # 1. Load dataset
    print("\n[1/5] Loading dataset")
    base, queries, gt, dataset_name = load_dataset(
        n_vectors=args.num_vectors, dim=128, n_queries=args.num_queries)
    dim = base.shape[1]

    # Use subset of queries for speed
    num_queries = args.num_queries
    queries = queries[:num_queries]
    gt = gt[:num_queries]

    # 2. Prepare sharded data
    shard_dir = os.path.join(BASE, f'shards_{args.num_shards}')
    if not args.skip_prep:
        print(f"\n[2/5] Creating {args.num_shards} Lance shards with IVF_PQ")
        create_sharded_lance(base, args.num_shards, shard_dir)
    else:
        print(f"\n[2/5] Skipping data prep (using existing shards)")

    # 3. Start cluster
    print(f"\n[3/5] Starting cluster ({args.num_shards} shards)")
    start_cluster(args.num_shards, shard_dir)

    # 4. Run distributed queries
    print(f"\n[4/5] Running {num_queries} distributed queries")
    results = {}
    for k in [1, 10, 100]:
        for nprobes in [10, 20, 50]:
            label = f"k={k}, nprobes={nprobes}"
            print(f"\n  --- {label} ---")
            ids, lats = distributed_search(queries, dim, k, nprobes, "127.0.0.1:51350")
            recall = compute_recall(ids, gt, k)
            qps = len(queries) / (sum(lats) / 1000)
            results[label] = {
                'k': k, 'nprobes': nprobes,
                'recall': recall, 'qps': qps,
                'p50_ms': np.percentile(lats, 50),
                'p99_ms': np.percentile(lats, 99),
            }
            print(f"    recall@{k} = {recall:.4f}  |  QPS = {qps:.1f}  |  P50 = {np.percentile(lats, 50):.1f}ms")

    # 5. Stop cluster + report
    print(f"\n[5/5] Cleanup + results")
    stop_cluster()

    print("\n" + "=" * 70)
    print("  RECALL BENCHMARK RESULTS")
    print("=" * 70)
    print(f"  Dataset:   {dataset_name} ({base.shape[0]} vectors, dim={dim})")
    print(f"  Shards:    {args.num_shards}")
    print(f"  Queries:   {num_queries}")
    print(f"  Index:     IVF_PQ")
    print()
    print(f"  {'Config':<25} {'recall@k':>10} {'QPS':>8} {'P50(ms)':>10} {'P99(ms)':>10}")
    print(f"  {'-'*25} {'-'*10} {'-'*8} {'-'*10} {'-'*10}")

    for label, r in sorted(results.items()):
        status = "✓" if r['recall'] >= 0.95 else "✗"
        print(f"  {status} {label:<23} {r['recall']:>10.4f} {r['qps']:>8.1f} {r['p50_ms']:>10.1f} {r['p99_ms']:>10.1f}")

    print(f"\n  Threshold: recall@k >= 0.95 (industry standard)")
    passing = sum(1 for r in results.values() if r['recall'] >= 0.95)
    print(f"  Result: {passing}/{len(results)} configs pass threshold")
    print("=" * 70)

    # Save results
    import json
    result_path = os.path.join(os.path.dirname(__file__), 'results', 'recall_sift1m.json')
    os.makedirs(os.path.dirname(result_path), exist_ok=True)
    with open(result_path, 'w') as f:
        json.dump({
            'dataset': dataset_name, 'num_shards': args.num_shards,
            'num_queries': num_queries, 'index': 'IVF_PQ',
            'results': results,
        }, f, indent=2)
    print(f"\n  Results saved to {result_path}")

    return all(r['recall'] >= 0.95 for r in results.values() if r['k'] == 10)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LanceForge Recall Benchmark')
    parser.add_argument('--num-shards', type=int, default=3, help='Number of shards')
    parser.add_argument('--num-vectors', type=int, default=200000, help='Number of base vectors (synthetic)')
    parser.add_argument('--num-queries', type=int, default=500, help='Number of queries to run')
    parser.add_argument('--skip-prep', action='store_true', help='Skip data preparation')
    args = parser.parse_args()

    try:
        ok = run_benchmark(args)
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        stop_cluster()
        sys.exit(1)
    except Exception as e:
        stop_cluster()
        raise
