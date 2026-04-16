#!/usr/bin/env python3
"""
Scale Benchmark — 1M vectors

Tests system behavior at million-scale:
  - Index build time
  - Memory footprint per worker
  - QPS and recall@10 at 1M scale
  - 5 shards (200K each)
"""

import sys, os, time, struct, subprocess, json, resource
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from recall_benchmark import (
    generate_synthetic_dataset, compute_recall, ev, BASE
)

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
DIM = 128
N_VECTORS = 1000000
N_SHARDS = 5
N_QUERIES = 200

def create_1m_shards(base, shard_dir):
    """Create 5 shards from 1M vectors, measure build time."""
    import pyarrow as pa
    import lance

    rows_per_shard = len(base) // N_SHARDS
    os.makedirs(shard_dir, exist_ok=True)
    total_build_time = 0

    for i in range(N_SHARDS):
        start = i * rows_per_shard
        end = start + rows_per_shard if i < N_SHARDS - 1 else len(base)
        uri = os.path.join(shard_dir, f'shard_{i:02d}.lance')

        print(f"  Shard {i}: {end-start} vectors...")
        table = pa.table({
            'id': pa.array(range(start, end), type=pa.int64()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(base[start:end].flatten(), type=pa.float32()),
                list_size=DIM),
        })

        t0 = time.time()
        lance.write_dataset(table, uri, mode='overwrite')
        write_time = time.time() - t0

        ds = lance.dataset(uri)
        t0 = time.time()
        npart = min(128, max(8, (end - start) // 1000))
        ds.create_index('vector', index_type='IVF_FLAT', num_partitions=npart)
        index_time = time.time() - t0
        total_build_time += write_time + index_time

        # Measure shard size on disk
        shard_size_mb = sum(
            os.path.getsize(os.path.join(dp, f))
            for dp, _, fns in os.walk(uri) for f in fns
        ) / (1024 * 1024)

        print(f"    Write: {write_time:.1f}s, Index: {index_time:.1f}s, Size: {shard_size_mb:.1f}MB")

    print(f"\n  Total build time: {total_build_time:.1f}s")
    return total_build_time

def start_scale_cluster(shard_dir):
    """Start 5-worker cluster."""
    import yaml
    processes = []

    cfg = {
        'tables': [{'name': 'scale', 'shards': [
            {'name': f'scale_shard_{i:02d}',
             'uri': os.path.join(shard_dir, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']}
            for i in range(N_SHARDS)
        ]}],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': 51500 + i}
                      for i in range(N_SHARDS)],
    }
    cfg_path = os.path.join(BASE, 'scale_config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(cfg, f)

    os.system("pkill -f 'lance-coordinator.*51550|lance-worker.*5150' 2>/dev/null")
    time.sleep(1)

    for i in range(N_SHARDS):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(51500 + i)],
            stdout=open(f"{BASE}/sw{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    time.sleep(5)

    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, "51550"],
        stdout=open(f"{BASE}/scoord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(5)

    for proc in processes:
        assert proc.poll() is None, f"Process died! Check {BASE}/s*.log"

    # Measure worker RSS
    worker_pids = [p.pid for p in processes[:-1]]
    total_rss = 0
    for pid in worker_pids:
        try:
            with open(f'/proc/{pid}/status') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        rss_kb = int(line.split()[1])
                        total_rss += rss_kb
                        break
        except:
            pass
    print(f"  Worker total RSS: {total_rss / 1024:.1f} MB ({total_rss / 1024 / N_SHARDS:.1f} MB/worker)")

    return processes, total_rss

def scale_search(queries, k, nprobes, addr="127.0.0.1:51550"):
    """Search the scale cluster."""
    import grpc
    import lance_service_pb2 as pb
    import lance_service_pb2_grpc
    import pyarrow.ipc as ipc

    channel = grpc.insecure_channel(addr,
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    all_ids, latencies = [], []
    for i, q in enumerate(queries):
        t0 = time.time()
        for attempt in range(3):
            try:
                r = stub.AnnSearch(pb.AnnSearchRequest(
                    table_name="scale", vector_column="vector",
                    query_vector=ev(q), dimension=DIM, k=k,
                    nprobes=nprobes, metric_type=0), timeout=60)
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
        if (i+1) % 50 == 0:
            print(f"    {i+1}/{len(queries)} queries...")
    channel.close()
    return all_ids, np.array(latencies)

def run_scale_bench():
    print(f"{'='*60}")
    print(f"  Scale Benchmark: {N_VECTORS/1e6:.0f}M vectors × {DIM}d, {N_SHARDS} shards")
    print(f"{'='*60}")

    print("\n[1/4] Generating 1M dataset + ground truth...")
    base, queries, gt = generate_synthetic_dataset(N_VECTORS, DIM, N_QUERIES, n_clusters=100)

    shard_dir = os.path.join(BASE, 'scale_1m')
    print("\n[2/4] Creating shards + indexing...")
    build_time = create_1m_shards(base, shard_dir)

    print("\n[3/4] Starting 5-worker cluster...")
    processes, worker_rss_kb = start_scale_cluster(shard_dir)

    print(f"\n[4/4] Running {N_QUERIES} queries...")
    results = {}
    for k in [10, 100]:
        nprobes = 20
        label = f"k={k}, nprobes={nprobes}"
        print(f"\n  --- {label} ---")
        ids, lats = scale_search(queries, k, nprobes)
        recall = compute_recall(ids, gt, k)
        qps = len(queries) / (sum(lats) / 1000)
        results[label] = {
            'k': k, 'nprobes': nprobes,
            'recall': recall, 'qps': qps,
            'p50_ms': float(np.percentile(lats, 50)),
            'p99_ms': float(np.percentile(lats, 99)),
        }
        s = "✓" if recall >= 0.95 else "✗"
        print(f"    {s} recall@{k} = {recall:.4f}  |  QPS = {qps:.1f}  |  P50 = {np.percentile(lats, 50):.1f}ms")

    for p in processes:
        try: p.terminate()
        except: pass

    print(f"\n{'='*60}")
    print("  SCALE BENCHMARK RESULTS")
    print(f"{'='*60}")
    print(f"  Dataset:     {N_VECTORS/1e6:.0f}M × {DIM}d")
    print(f"  Shards:      {N_SHARDS} × {N_VECTORS//N_SHARDS/1e3:.0f}K")
    print(f"  Build time:  {build_time:.1f}s")
    print(f"  Worker RSS:  {worker_rss_kb/1024:.1f} MB total, {worker_rss_kb/1024/N_SHARDS:.1f} MB/worker")
    print()
    for label, r in results.items():
        s = "✓" if r['recall'] >= 0.95 else "✗"
        print(f"  {s} {label}: recall={r['recall']:.4f}  QPS={r['qps']:.1f}  P50={r['p50_ms']:.1f}ms  P99={r['p99_ms']:.1f}ms")
    print(f"{'='*60}")

    out = os.path.join(os.path.dirname(__file__), 'results', 'recall_scale_1m.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        json.dump({
            'n_vectors': N_VECTORS, 'dim': DIM, 'n_shards': N_SHARDS,
            'build_time_s': build_time,
            'worker_rss_mb': worker_rss_kb / 1024,
            'results': results,
        }, f, indent=2)

    return all(r['recall'] >= 0.95 for r in results.values())

if __name__ == '__main__':
    try:
        ok = run_scale_bench()
        sys.exit(0 if ok else 1)
    except:
        os.system("pkill -f 'lance-coordinator.*51550|lance-worker.*5150' 2>/dev/null")
        raise
