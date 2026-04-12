#!/usr/bin/env python3
"""
High-Dimensional Recall Benchmark — 768d / 1536d

Tests recall@k with dimensions matching real AI embeddings:
  - 768d  (CLIP, CodeBERT, all-MiniLM)
  - 1536d (OpenAI text-embedding-3-small)

Validates that IVF_FLAT maintains high recall at production dimensions.
"""

import sys, os, time, struct, subprocess, json
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from recall_benchmark import (
    generate_synthetic_dataset, compute_recall, create_sharded_lance,
    start_cluster, stop_cluster, distributed_search, ev, BASE
)

def run_highdim_bench():
    results = {}

    for dim in [768, 1536]:
        n_vectors = 100000 if dim == 768 else 50000  # smaller for 1536d (memory)
        n_shards = 3
        n_queries = 100
        print(f"\n{'='*60}")
        print(f"  High-Dim Benchmark: {n_vectors} vectors × {dim}d, {n_shards} shards")
        print(f"{'='*60}")

        print("\n  Generating dataset + ground truth...")
        base, queries, gt = generate_synthetic_dataset(n_vectors, dim, n_queries, n_clusters=30)

        shard_dir = os.path.join(BASE, f'highdim_{dim}d')
        if os.path.exists(shard_dir):
            import shutil
            shutil.rmtree(shard_dir)

        print(f"  Creating {n_shards} shards with IVF_FLAT...")
        create_sharded_lance(base, n_shards, shard_dir)

        print(f"  Starting cluster...")
        start_cluster(n_shards, shard_dir)

        for k in [10]:
            for nprobes in [10, 20]:
                label = f"dim={dim}, k={k}, nprobes={nprobes}"
                print(f"\n  --- {label} ---")
                ids, lats = distributed_search(queries, dim, k, nprobes, "127.0.0.1:51350")
                recall = compute_recall(ids, gt, k)
                qps = len(queries) / (sum(lats) / 1000)
                results[label] = {
                    'dim': dim, 'k': k, 'nprobes': nprobes,
                    'n_vectors': n_vectors,
                    'recall': recall, 'qps': qps,
                    'p50_ms': float(np.percentile(lats, 50)),
                    'p99_ms': float(np.percentile(lats, 99)),
                }
                status = "✓" if recall >= 0.95 else "✗"
                print(f"    {status} recall@{k} = {recall:.4f}  |  QPS = {qps:.1f}  |  P50 = {np.percentile(lats, 50):.1f}ms")

        stop_cluster()

    # Report
    print(f"\n{'='*60}")
    print("  HIGH-DIMENSIONAL RECALL RESULTS")
    print(f"{'='*60}")
    print(f"  {'Config':<35} {'recall@k':>10} {'QPS':>8} {'P50':>8}")
    print(f"  {'-'*35} {'-'*10} {'-'*8} {'-'*8}")
    passing = 0
    for label, r in sorted(results.items()):
        status = "✓" if r['recall'] >= 0.95 else "✗"
        print(f"  {status} {label:<33} {r['recall']:>10.4f} {r['qps']:>8.1f} {r['p50_ms']:>8.1f}")
        if r['recall'] >= 0.95: passing += 1
    print(f"\n  {passing}/{len(results)} pass recall >= 0.95")
    print(f"{'='*60}")

    out = os.path.join(os.path.dirname(__file__), 'results', 'recall_highdim.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        json.dump(results, f, indent=2)

    return all(r['recall'] >= 0.95 for r in results.values())

if __name__ == '__main__':
    try:
        ok = run_highdim_bench()
        sys.exit(0 if ok else 1)
    except:
        stop_cluster()
        raise
