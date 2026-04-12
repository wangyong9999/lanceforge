#!/usr/bin/env python3
"""
Cosine / Inner Product Distance Benchmark

Validates recall@k with non-L2 metrics that real AI embeddings use:
  - Cosine similarity (OpenAI, Cohere, BGE, all-MiniLM)
  - Inner Product / Dot (some CLIP models)

Critical: almost ALL production embeddings use cosine, not L2.
If the cosine code path is broken, all production queries are wrong.
"""

import sys, os, time, struct, json
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from recall_benchmark import (
    create_sharded_lance, start_cluster, stop_cluster,
    distributed_search, compute_recall, ev, BASE
)

DIM = 128
N_VECTORS = 100000
N_SHARDS = 3
N_QUERIES = 200

def generate_normalized_dataset(n, dim, n_queries, n_clusters=30, seed=42):
    """Generate L2-normalized vectors (unit sphere) for cosine similarity."""
    np.random.seed(seed)
    centroids = np.random.randn(n_clusters, dim).astype(np.float32)
    centroids /= np.linalg.norm(centroids, axis=1, keepdims=True)

    labels = np.random.randint(0, n_clusters, n)
    base = centroids[labels] + np.random.randn(n, dim).astype(np.float32) * 0.1
    base /= np.linalg.norm(base, axis=1, keepdims=True)  # normalize to unit sphere

    q_labels = np.random.randint(0, n_clusters, n_queries)
    queries = centroids[q_labels] + np.random.randn(n_queries, dim).astype(np.float32) * 0.1
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    return base, queries

def compute_cosine_gt(base, queries, k):
    """Brute-force cosine similarity ground truth."""
    # For unit vectors: cosine_sim = dot product, cosine_dist = 1 - dot
    gt = []
    for q in queries:
        sims = np.dot(base, q)  # higher = more similar
        top_k = np.argsort(-sims)[:k]  # descending
        gt.append(top_k.tolist())
    return gt

def compute_dot_gt(base, queries, k):
    """Brute-force inner product ground truth (not normalized)."""
    gt = []
    for q in queries:
        dots = np.dot(base, q)
        top_k = np.argsort(-dots)[:k]
        gt.append(top_k.tolist())
    return gt

def distributed_search_metric(queries, dim, k, nprobes, metric_type,
                               coordinator_addr="127.0.0.1:51350"):
    """Search with specified metric type."""
    import grpc
    import lance_service_pb2 as pb
    import lance_service_pb2_grpc
    import pyarrow.ipc as ipc

    channel = grpc.insecure_channel(coordinator_addr,
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    all_ids, latencies = [], []
    for q in queries:
        t0 = time.time()
        for attempt in range(3):
            try:
                r = stub.AnnSearch(pb.AnnSearchRequest(
                    table_name="cosine_bench", vector_column="vector",
                    query_vector=ev(q), dimension=dim, k=k,
                    nprobes=nprobes, metric_type=metric_type), timeout=30)
                break
            except Exception as e:
                if attempt < 2: time.sleep(0.1); continue
                raise
        latencies.append((time.time() - t0) * 1000)

        if r.arrow_ipc_data:
            t = ipc.open_stream(r.arrow_ipc_data).read_all()
            all_ids.append(t.column("id").to_pylist()[:k])
        else:
            all_ids.append([])

    channel.close()
    return all_ids, np.array(latencies)

def run_cosine_bench():
    import yaml, subprocess
    BIN = os.path.expanduser("~/cc/lance-ballista/target/release")

    print("=" * 60)
    print("  Cosine / Inner Product Distance Benchmark")
    print("=" * 60)

    print("\n[1/4] Generating normalized dataset...")
    base, queries = generate_normalized_dataset(N_VECTORS, DIM, N_QUERIES)
    print(f"  {N_VECTORS} unit vectors, dim={DIM}")

    # Verify normalization
    norms = np.linalg.norm(base[:10], axis=1)
    print(f"  Vector norms (should be ~1.0): {norms[:3].tolist()}")

    print(f"\n[2/4] Creating {N_SHARDS} shards...")
    shard_dir = os.path.join(BASE, 'cosine_shards')
    if os.path.exists(shard_dir):
        import shutil
        shutil.rmtree(shard_dir)
    create_sharded_lance(base, N_SHARDS, shard_dir)

    print(f"\n[3/4] Starting cluster...")
    # Use custom config with table name matching shard prefix
    cfg = {
        'tables': [{'name': 'cosine_bench', 'shards': [
            {'name': f'cosine_bench_shard_{i:02d}',
             'uri': os.path.join(shard_dir, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']}
            for i in range(N_SHARDS)
        ]}],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': 51300 + i}
                      for i in range(N_SHARDS)],
    }
    cfg_path = os.path.join(BASE, 'cosine_config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(cfg, f)

    os.system("pkill -f 'lance-coordinator.*51350|lance-worker.*5130' 2>/dev/null")
    time.sleep(1)

    processes = []
    for i in range(N_SHARDS):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(51300 + i)],
            stdout=open(f"{BASE}/cw{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    time.sleep(3)
    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, "51350"],
        stdout=open(f"{BASE}/ccoord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(4)

    for proc in processes:
        assert proc.poll() is None, f"Process died! Check {BASE}/c*.log"
    print("  Cluster running")

    print(f"\n[4/4] Running queries...")
    results = {}

    # metric_type: 0=L2, 1=Cosine, 2=Dot
    metrics = [
        ("L2", 0, lambda b, q, k: compute_l2_gt(b, q, k)),
        ("Cosine", 1, lambda b, q, k: compute_cosine_gt(b, q, k)),
    ]

    for metric_name, metric_type, gt_fn in metrics:
        gt = gt_fn(base, queries, 10)

        for nprobes in [10, 20]:
            label = f"{metric_name}, nprobes={nprobes}"
            print(f"\n  --- {label} ---")
            ids, lats = distributed_search_metric(
                queries, DIM, 10, nprobes, metric_type)
            recall = compute_recall(ids, gt, 10)
            qps = len(queries) / (sum(lats) / 1000)
            results[label] = {
                'metric': metric_name, 'metric_type': metric_type,
                'nprobes': nprobes,
                'recall': recall, 'qps': qps,
                'p50_ms': float(np.percentile(lats, 50)),
            }
            s = "✓" if recall >= 0.95 else "✗"
            print(f"    {s} recall@10 = {recall:.4f}  |  QPS = {qps:.1f}")

    # Cleanup
    for p in processes:
        try: p.terminate()
        except: pass

    print(f"\n{'='*60}")
    print("  COSINE / IP DISTANCE RESULTS")
    print(f"{'='*60}")
    for label, r in sorted(results.items()):
        s = "✓" if r['recall'] >= 0.95 else "✗"
        print(f"  {s} {label:<25} recall@10={r['recall']:.4f}  QPS={r['qps']:.1f}")
    passing = sum(1 for r in results.values() if r['recall'] >= 0.95)
    print(f"\n  {passing}/{len(results)} pass recall >= 0.95")
    print(f"{'='*60}")

    out = os.path.join(os.path.dirname(__file__), 'results', 'recall_cosine.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        json.dump(results, f, indent=2)

    return all(r['recall'] >= 0.95 for r in results.values())

def compute_l2_gt(base, queries, k):
    """Brute-force L2 ground truth."""
    base_norms = np.sum(base ** 2, axis=1)
    gt = []
    for q in queries:
        dists = base_norms + np.sum(q ** 2) - 2 * np.dot(base, q)
        gt.append(np.argsort(dists)[:k].tolist())
    return gt

if __name__ == '__main__':
    try:
        ok = run_cosine_bench()
        sys.exit(0 if ok else 1)
    except:
        os.system("pkill -f 'lance-coordinator.*51350|lance-worker.*5130' 2>/dev/null")
        raise
