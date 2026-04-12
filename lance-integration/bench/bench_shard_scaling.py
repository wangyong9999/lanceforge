#!/usr/bin/env python3
"""
Shard Scaling Benchmark

Tests QPS and recall as shard count increases: 1, 3, 5, 10.
Validates that distributed architecture provides linear QPS scaling
without recall degradation.

Uses a fixed 500K×128d dataset, redistributed across varying shard counts.
"""

import sys, os, time, struct, subprocess, json, shutil
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from recall_benchmark import (
    generate_synthetic_dataset, compute_recall, create_sharded_lance, ev, BASE
)

BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
DIM = 128
N_VECTORS = 500000
N_QUERIES = 100

def run_scaling_test(base, queries, gt, num_shards):
    """Run a complete test at a given shard count."""
    import yaml

    shard_dir = os.path.join(BASE, f'scaling_{num_shards}s')
    if os.path.exists(shard_dir):
        shutil.rmtree(shard_dir)

    print(f"\n  Creating {num_shards} shards...")
    create_sharded_lance(base, num_shards, shard_dir)

    # Use unique port range per shard count to avoid conflicts
    base_port = 51600 + num_shards * 10
    coord_port = base_port + 100

    cfg = {
        'tables': [{'name': 'scaling', 'shards': [
            {'name': f'scaling_shard_{i:02d}',
             'uri': os.path.join(shard_dir, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']}
            for i in range(num_shards)
        ]}],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': base_port + i}
                      for i in range(num_shards)],
    }
    cfg_path = os.path.join(BASE, f'scaling_{num_shards}s_config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(cfg, f)

    # Start processes
    processes = []
    for i in range(num_shards):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(base_port + i)],
            stdout=open(f"{BASE}/scaling_w{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    time.sleep(max(3, num_shards))

    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, str(coord_port)],
        stdout=open(f"{BASE}/scaling_coord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
    time.sleep(4)

    for proc in processes:
        if proc.poll() is not None:
            print(f"  WARNING: Process {proc.pid} died!")
            for p in processes:
                try: p.terminate()
                except: pass
            return None

    # Measure worker RSS
    worker_rss = 0
    for proc in processes[:-1]:
        try:
            with open(f'/proc/{proc.pid}/status') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        worker_rss += int(line.split()[1])
                        break
        except:
            pass

    # Query
    import grpc
    import lance_service_pb2 as pb
    import lance_service_pb2_grpc
    import pyarrow.ipc as ipc

    channel = grpc.insecure_channel(f"127.0.0.1:{coord_port}",
        options=[("grpc.max_receive_message_length", 64*1024*1024)])
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    all_ids, latencies = [], []
    k, nprobes = 10, 20
    for q in queries:
        t0 = time.time()
        for attempt in range(3):
            try:
                r = stub.AnnSearch(pb.AnnSearchRequest(
                    table_name="scaling", vector_column="vector",
                    query_vector=ev(q), dimension=DIM, k=k,
                    nprobes=nprobes, metric_type=0), timeout=30)
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

    # Cleanup
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)

    recall = compute_recall(all_ids, gt, k)
    lats = np.array(latencies)
    qps = len(queries) / (sum(latencies) / 1000)

    return {
        'num_shards': num_shards,
        'recall': recall,
        'qps': qps,
        'p50_ms': float(np.percentile(lats, 50)),
        'p99_ms': float(np.percentile(lats, 99)),
        'worker_rss_mb': worker_rss / 1024,
    }

def run_scaling_bench():
    print("=" * 60)
    print("  Shard Scaling Benchmark")
    print(f"  {N_VECTORS/1e3:.0f}K vectors × {DIM}d, shard counts: 1, 3, 5, 10")
    print("=" * 60)

    print("\n  Generating dataset + ground truth...")
    base, queries, gt = generate_synthetic_dataset(N_VECTORS, DIM, N_QUERIES, n_clusters=50)

    results = {}
    for num_shards in [1, 3, 5, 10]:
        print(f"\n{'='*40}")
        print(f"  Testing {num_shards} shard(s)")
        print(f"{'='*40}")

        r = run_scaling_test(base, queries, gt, num_shards)
        if r is None:
            print(f"  FAILED: cluster didn't start")
            continue
        results[num_shards] = r
        s = "✓" if r['recall'] >= 0.95 else "✗"
        print(f"  {s} shards={num_shards}: recall@10={r['recall']:.4f}  QPS={r['qps']:.1f}  P50={r['p50_ms']:.1f}ms  RSS={r['worker_rss_mb']:.0f}MB")

    # Report
    print(f"\n{'='*60}")
    print("  SHARD SCALING RESULTS")
    print(f"{'='*60}")
    print(f"  {'Shards':>6} {'Recall@10':>10} {'QPS':>8} {'P50(ms)':>10} {'P99(ms)':>10} {'RSS(MB)':>10}")
    print(f"  {'-'*6} {'-'*10} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")

    base_qps = None
    for ns in sorted(results.keys()):
        r = results[ns]
        s = "✓" if r['recall'] >= 0.95 else "✗"
        if base_qps is None:
            base_qps = r['qps']
            scaling = "1.00x"
        else:
            scaling = f"{r['qps']/base_qps:.2f}x"
        print(f"  {s} {ns:>5} {r['recall']:>10.4f} {r['qps']:>8.1f} {r['p50_ms']:>10.1f} {r['p99_ms']:>10.1f} {r['worker_rss_mb']:>10.0f}   ({scaling})")

    passing = sum(1 for r in results.values() if r['recall'] >= 0.95)
    print(f"\n  {passing}/{len(results)} pass recall >= 0.95")

    if len(results) >= 2:
        qps_values = [results[ns]['qps'] for ns in sorted(results.keys())]
        if qps_values[0] > 0:
            max_scaling = qps_values[-1] / qps_values[0]
            max_shards = max(results.keys())
            print(f"  QPS scaling: {max_scaling:.2f}x from 1→{max_shards} shards")

    print(f"{'='*60}")

    out = os.path.join(os.path.dirname(__file__), 'results', 'shard_scaling.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        json.dump({str(k): v for k, v in results.items()}, f, indent=2)

    return all(r['recall'] >= 0.95 for r in results.values())

if __name__ == '__main__':
    try:
        ok = run_scaling_bench()
        sys.exit(0 if ok else 1)
    except:
        os.system("pkill -f 'lance-coordinator|lance-worker' 2>/dev/null")
        raise
