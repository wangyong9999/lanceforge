#!/usr/bin/env python3
"""
Standard Vector Search Performance Benchmark

Industry-standard benchmark using ann-benchmarks datasets:
  - SIFT-128-euclidean (1M vectors, 128d, L2)
  - GloVe-100-angular (1.2M vectors, 100d, Cosine)

Measures: recall@10 vs QPS tradeoff curve at varying nprobes.
Outputs JSON results + comparison-ready format.

If standard datasets are unavailable, falls back to synthetic data
with equivalent characteristics.
"""

import sys, os, time, struct, subprocess, json, shutil
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from recall_benchmark import (
    generate_synthetic_dataset, compute_recall, create_sharded_lance,
    ev, BASE
)

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
DATASET_DIR = os.path.join(os.path.dirname(__file__), 'datasets')
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')

# ── Dataset Loading ──

def load_hdf5_dataset(name):
    """Try to load an ann-benchmarks HDF5 dataset."""
    path = os.path.join(DATASET_DIR, f'{name}.hdf5')
    if not os.path.exists(path):
        return None
    try:
        import h5py
        f = h5py.File(path, 'r')
        train = np.array(f['train'], dtype=np.float32)
        test = np.array(f['test'], dtype=np.float32)
        neighbors = np.array(f['neighbors'])  # ground truth top-100
        distances = np.array(f['distances']) if 'distances' in f else None
        gt = [row.tolist() for row in neighbors]
        print(f"  Loaded {name}: {train.shape[0]} train, {test.shape[0]} test, dim={train.shape[1]}")
        return train, test, gt
    except ImportError:
        print("  h5py not installed, skipping HDF5 dataset")
        return None
    except Exception as e:
        print(f"  Failed to load {name}: {e}")
        return None

def load_or_generate(name, n_vectors, dim, n_queries, metric='euclidean'):
    """Load standard dataset or generate synthetic equivalent."""
    data = load_hdf5_dataset(name)
    if data is not None:
        return data[0], data[1], data[2], name

    print(f"  {name} not available, generating synthetic ({n_vectors}x{dim})...")
    if metric == 'angular':
        # Normalized vectors for cosine similarity
        np.random.seed(42)
        n_clusters = 50
        centroids = np.random.randn(n_clusters, dim).astype(np.float32)
        centroids /= np.linalg.norm(centroids, axis=1, keepdims=True)
        labels = np.random.randint(0, n_clusters, n_vectors)
        base = centroids[labels] + np.random.randn(n_vectors, dim).astype(np.float32) * 0.1
        base /= np.linalg.norm(base, axis=1, keepdims=True)
        q_labels = np.random.randint(0, n_clusters, n_queries)
        queries = centroids[q_labels] + np.random.randn(n_queries, dim).astype(np.float32) * 0.1
        queries /= np.linalg.norm(queries, axis=1, keepdims=True)
        # GT by cosine similarity (= dot product for unit vectors)
        gt = []
        for q in queries:
            sims = np.dot(base, q)
            top = np.argsort(-sims)[:100]
            gt.append(top.tolist())
        return base, queries, gt, f"Synthetic-{dim}d-angular"
    else:
        base, queries, gt = generate_synthetic_dataset(n_vectors, dim, n_queries)
        return base, queries, gt, f"Synthetic-{dim}d-euclidean"

# ── Cluster Management ──

def start_bench_cluster(num_shards, shard_dir, base_port=51800):
    """Start a cluster for benchmarking."""
    import yaml
    coord_port = base_port + 100

    cfg = {
        'tables': [{'name': 'bench', 'shards': [
            {'name': f'bench_shard_{i:02d}',
             'uri': os.path.join(shard_dir, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']}
            for i in range(num_shards)
        ]}],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': base_port + i}
                      for i in range(num_shards)],
    }
    cfg_path = os.path.join(BASE, 'std_bench_config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(cfg, f)

    os.system(f"pkill -f 'lance-coordinator.*{coord_port}|lance-worker.*{base_port}' 2>/dev/null")
    time.sleep(1)

    processes = []
    for i in range(num_shards):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(base_port + i)],
            stdout=open(f"{BASE}/sbw{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    time.sleep(max(3, num_shards))

    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, str(coord_port)],
        stdout=open(f"{BASE}/sbcoord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(4)

    for proc in processes:
        assert proc.poll() is None, f"Process died! Check {BASE}/sb*.log"

    return processes, coord_port

def bench_search(queries, dim, k, nprobes, metric_type, coord_port):
    """Run search queries and measure latency."""
    import grpc
    import lance_service_pb2 as pb
    import lance_service_pb2_grpc
    import pyarrow.ipc as ipc

    channel = grpc.insecure_channel(f"127.0.0.1:{coord_port}",
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    all_ids, latencies = [], []
    for q in queries:
        t0 = time.time()
        for attempt in range(3):
            try:
                r = stub.AnnSearch(pb.AnnSearchRequest(
                    table_name="bench", vector_column="vector",
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

# ── Main Benchmark ──

def run_standard_bench():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(BASE, exist_ok=True)

    benchmarks = [
        {
            'name': 'sift-128-euclidean',
            'n_vectors': 500000, 'dim': 128, 'n_queries': 500,
            'metric': 'euclidean', 'metric_type': 0,
            'num_shards': 3, 'k': 10,
            'nprobes_range': [1, 5, 10, 20, 50],
            'index_type': os.environ.get('BENCH_INDEX_TYPE', 'IVF_FLAT'),
        },
        {
            'name': 'glove-100-angular',
            'n_vectors': 500000, 'dim': 100, 'n_queries': 500,
            'metric': 'angular', 'metric_type': 1,
            'num_shards': 3, 'k': 10,
            'nprobes_range': [1, 5, 10, 20, 50],
            'index_type': os.environ.get('BENCH_INDEX_TYPE', 'IVF_FLAT'),
        },
    ]

    all_results = {}

    for bench in benchmarks:
        name = bench['name']
        print(f"\n{'='*60}")
        print(f"  BENCHMARK: {name}")
        print(f"  {bench['n_vectors']/1e3:.0f}K vectors, dim={bench['dim']}, "
              f"metric={bench['metric']}, {bench['num_shards']} shards")
        print(f"{'='*60}")

        # Load dataset
        print("\n  Loading dataset...")
        base, queries, gt, dataset_name = load_or_generate(
            name, bench['n_vectors'], bench['dim'],
            bench['n_queries'], bench['metric'])
        queries = queries[:bench['n_queries']]
        gt = gt[:bench['n_queries']]

        # Create shards
        shard_dir = os.path.join(BASE, f'std_{name}')
        if os.path.exists(shard_dir):
            shutil.rmtree(shard_dir)
        print(f"  Creating {bench['num_shards']} shards...")
        create_sharded_lance(base, bench['num_shards'], shard_dir)

        # Start cluster
        print("  Starting cluster...")
        processes, coord_port = start_bench_cluster(bench['num_shards'], shard_dir)

        # Run at different nprobes to build recall-QPS curve
        bench_results = []
        for nprobes in bench['nprobes_range']:
            print(f"\n  nprobes={nprobes}:")
            ids, lats = bench_search(
                queries, bench['dim'], bench['k'],
                nprobes, bench['metric_type'], coord_port)
            recall = compute_recall(ids, gt, bench['k'])
            qps = len(queries) / (sum(lats) / 1000)
            p50 = float(np.percentile(lats, 50))
            p99 = float(np.percentile(lats, 99))
            bench_results.append({
                'nprobes': nprobes,
                'recall': recall,
                'qps': qps,
                'p50_ms': p50,
                'p99_ms': p99,
            })
            s = "✓" if recall >= 0.95 else "✗"
            print(f"    {s} recall@{bench['k']}={recall:.4f}  QPS={qps:.1f}  "
                  f"P50={p50:.1f}ms  P99={p99:.1f}ms")

        # Cleanup cluster
        for p in processes:
            try: p.terminate()
            except: pass
        time.sleep(1)

        all_results[name] = {
            'dataset': dataset_name,
            'n_vectors': len(base),
            'dim': bench['dim'],
            'metric': bench['metric'],
            'num_shards': bench['num_shards'],
            'k': bench['k'],
            'results': bench_results,
        }

    # Print summary
    print(f"\n\n{'='*70}")
    print("  STANDARD BENCHMARK SUMMARY")
    print(f"{'='*70}")

    for name, data in all_results.items():
        print(f"\n  {name} ({data['dataset']}, {data['n_vectors']/1e3:.0f}K×{data['dim']}d, "
              f"{data['metric']}, {data['num_shards']} shards)")
        print(f"  {'nprobes':>8} {'recall@10':>10} {'QPS':>8} {'P50(ms)':>10} {'P99(ms)':>10}")
        print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*10} {'-'*10}")
        for r in data['results']:
            s = "✓" if r['recall'] >= 0.95 else "✗"
            print(f"  {s} {r['nprobes']:>6} {r['recall']:>10.4f} {r['qps']:>8.1f} "
                  f"{r['p50_ms']:>10.1f} {r['p99_ms']:>10.1f}")

    # Best operating point per dataset
    print(f"\n  BEST OPERATING POINTS (recall >= 0.95, max QPS):")
    for name, data in all_results.items():
        qualifying = [r for r in data['results'] if r['recall'] >= 0.95]
        if qualifying:
            best = max(qualifying, key=lambda r: r['qps'])
            print(f"    {name}: nprobes={best['nprobes']}, recall={best['recall']:.4f}, "
                  f"QPS={best['qps']:.1f}, P50={best['p50_ms']:.1f}ms")
        else:
            print(f"    {name}: NO CONFIG MEETS recall >= 0.95")

    print(f"{'='*70}")

    # Save results
    out = os.path.join(RESULTS_DIR, 'standard_benchmark.json')
    with open(out, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n  Results saved to {out}")

    # Return pass/fail
    all_pass = all(
        any(r['recall'] >= 0.95 for r in data['results'])
        for data in all_results.values()
    )
    return all_pass

if __name__ == '__main__':
    try:
        ok = run_standard_bench()
        sys.exit(0 if ok else 1)
    except:
        os.system("pkill -f 'lance-coordinator.*51900|lance-worker.*5180' 2>/dev/null")
        raise
