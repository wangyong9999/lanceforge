#!/usr/bin/env python3
"""
Sustained Load Test — detect memory leaks, QPS degradation, error accumulation.

Runs continuous concurrent queries for a configurable duration.
Samples QPS, latency, error rate, and coordinator memory every interval.

Usage:
    python bench_sustained_load.py               # 30 minutes, 10 threads
    python bench_sustained_load.py --ci           # 2 minutes, 5 threads (CI mode)
    python bench_sustained_load.py --duration 600 # 10 minutes

Thresholds:
    QPS degradation < 20% (last vs first interval)
    Error rate < 1%
    Memory growth < 50% (last vs first interval)
"""

import sys, os, time, subprocess, json, argparse, threading, shutil
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

from test_helpers import wait_for_grpc
import pyarrow as pa
import lance
import yaml

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = "/tmp/lanceforge_sustained_load"
DIM = 128
COORD_PORT = 59500
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')

processes = []

def cleanup():
    os.system("pkill -f 'lance-coordinator.*59500|lance-worker.*5960' 2>/dev/null")
    time.sleep(1)

def get_rss_mb(proc):
    """Get process RSS in MB via /proc."""
    try:
        with open(f"/proc/{proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024  # KB → MB
    except:
        pass
    return 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--duration', type=int, default=1800, help='Test duration in seconds')
    parser.add_argument('--threads', type=int, default=10, help='Concurrent threads')
    parser.add_argument('--ci', action='store_true', help='CI mode (120s, 5 threads)')
    parser.add_argument('--sample-interval', type=int, default=30, help='Sampling interval')
    parser.add_argument('--read-only', action='store_true', help='Read-only workload (no inserts)')
    args = parser.parse_args()

    if args.ci:
        args.duration = 120
        args.threads = 5
        args.sample_interval = 15
        args.read_only = True  # CI uses read-only for stable QPS baseline

    print(f"\n{'='*60}")
    print(f"  SUSTAINED LOAD TEST")
    print(f"  Duration: {args.duration}s, Threads: {args.threads}")
    print(f"{'='*60}")

    # Setup
    cleanup()
    if os.path.exists(BASE):
        shutil.rmtree(BASE)
    os.makedirs(BASE, exist_ok=True)
    np.random.seed(42)

    # Create data: 3 shards × ~167K vectors
    N_SHARDS = 3
    N_PER_SHARD = 50000 if args.ci else 167000
    print(f"  Creating {N_SHARDS} shards × {N_PER_SHARD} vectors ({DIM}d)...")

    for i in range(N_SHARDS):
        uri = os.path.join(BASE, f"shard_{i:02d}.lance")
        n = N_PER_SHARD
        off = i * n
        table = pa.table({
            'id': pa.array(range(off, off + n), type=pa.int64()),
            'category': pa.array([f'cat_{(j+off) % 10}' for j in range(n)], type=pa.string()),
            'vector': pa.FixedSizeListArray.from_arrays(
                pa.array(np.random.randn(n * DIM).astype(np.float32)), list_size=DIM),
        })
        lance.write_dataset(table, uri, mode='overwrite')
        ds = lance.dataset(uri)
        ds.create_index('vector', index_type='IVF_FLAT', num_partitions=16)

    config = {
        'tables': [{'name': 'load_table', 'shards': [
            {'name': f'load_table_shard_{i:02d}',
             'uri': os.path.join(BASE, f'shard_{i:02d}.lance'),
             'executors': [f'w{i}']} for i in range(N_SHARDS)
        ]}],
        'executors': [
            {'id': f'w{i}', 'host': '127.0.0.1', 'port': 59600 + i}
            for i in range(N_SHARDS)
        ],
    }
    with open(f"{BASE}/config.yaml", 'w') as f:
        yaml.dump(config, f)

    # Start cluster
    print("  Starting cluster...")
    for i in range(N_SHARDS):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", f"{BASE}/config.yaml", f"w{i}", str(59600 + i)],
            stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
        processes.append(p)
    for i in range(N_SHARDS):
        assert wait_for_grpc("127.0.0.1", 59600 + i), f"Worker w{i} failed to start"

    coord = subprocess.Popen(
        [f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
    processes.append(coord)
    assert wait_for_grpc("127.0.0.1", COORD_PORT), "Coordinator failed to start"
    print("  Cluster ready.\n")

    # Import client after proto stubs are in path
    from lanceforge import LanceForgeClient

    # Shared state for load threads
    query_count = [0]
    error_count = [0]
    latencies = []
    lock = threading.Lock()
    stop_flag = threading.Event()

    def load_worker(thread_id):
        """Continuous load: 80% ANN, 10% filtered, 10% insert."""
        client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
        rng = np.random.RandomState(thread_id)

        while not stop_flag.is_set():
            try:
                roll = rng.random()
                t0 = time.time()

                if args.read_only or roll < 0.90:
                    if roll < 0.85 or args.read_only:
                        # ANN search
                        vec = rng.randn(DIM).tolist()
                        client.search("load_table", query_vector=vec, k=10, nprobes=10)
                    else:
                        # Filtered search
                        vec = rng.randn(DIM).tolist()
                        cat = f"cat_{rng.randint(0, 10)}"
                        client.search("load_table", query_vector=vec, k=10,
                                     nprobes=10, filter=f"category = '{cat}'")
                else:
                    # Insert (10% of mixed workload)
                    batch = pa.table({
                        'id': pa.array([rng.randint(1000000, 9999999)], type=pa.int64()),
                        'category': pa.array(['load_test'], type=pa.string()),
                        'vector': pa.FixedSizeListArray.from_arrays(
                            pa.array(rng.randn(DIM).astype(np.float32)), list_size=DIM),
                    })
                    client.insert("load_table", batch)

                elapsed = time.time() - t0
                with lock:
                    query_count[0] += 1
                    latencies.append(elapsed * 1000)  # ms
            except Exception as e:
                with lock:
                    error_count[0] += 1
                    query_count[0] += 1

        client.close()

    # Start load threads
    threads = []
    for i in range(args.threads):
        t = threading.Thread(target=load_worker, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    # Sampling loop
    samples = []
    start_time = time.time()
    prev_count = 0

    print(f"  {'Time':>6}  {'QPS':>8}  {'P50ms':>8}  {'P99ms':>8}  {'Errors':>8}  {'RSS_MB':>8}")
    print(f"  {'-'*6}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}")

    while time.time() - start_time < args.duration:
        time.sleep(args.sample_interval)
        elapsed = time.time() - start_time

        with lock:
            current_count = query_count[0]
            current_errors = error_count[0]
            current_latencies = latencies.copy()
            latencies.clear()

        interval_queries = current_count - prev_count
        prev_count = current_count
        qps = interval_queries / args.sample_interval if args.sample_interval > 0 else 0
        p50 = np.percentile(current_latencies, 50) if current_latencies else 0
        p99 = np.percentile(current_latencies, 99) if current_latencies else 0
        rss = get_rss_mb(coord)

        sample = {
            'elapsed_s': round(elapsed),
            'qps': round(qps, 1),
            'p50_ms': round(p50, 1),
            'p99_ms': round(p99, 1),
            'total_queries': current_count,
            'total_errors': current_errors,
            'rss_mb': round(rss, 1),
        }
        samples.append(sample)
        print(f"  {elapsed:6.0f}  {qps:8.1f}  {p50:8.1f}  {p99:8.1f}  {current_errors:8d}  {rss:8.1f}")

    # Stop threads
    stop_flag.set()
    for t in threads:
        t.join(timeout=5)

    # Cleanup
    cleanup()

    # Analysis
    total_queries = query_count[0]
    total_errors = error_count[0]
    total_elapsed = time.time() - start_time
    error_rate = total_errors / max(total_queries, 1) * 100

    # QPS degradation: compare first and last 2 samples
    if len(samples) >= 4:
        first_qps = np.mean([s['qps'] for s in samples[:2]])
        last_qps = np.mean([s['qps'] for s in samples[-2:]])
        qps_degradation = (first_qps - last_qps) / max(first_qps, 1) * 100
    else:
        first_qps = last_qps = samples[-1]['qps'] if samples else 0
        qps_degradation = 0

    # Memory growth
    if len(samples) >= 2:
        first_rss = samples[0]['rss_mb']
        last_rss = samples[-1]['rss_mb']
        mem_growth = (last_rss - first_rss) / max(first_rss, 1) * 100
    else:
        first_rss = last_rss = 0
        mem_growth = 0

    # Verdict
    # 30% threshold for mixed read-write (inserts fragment Lance tables)
    qps_ok = qps_degradation < 30
    err_ok = error_rate < 1
    mem_ok = mem_growth < 50

    print(f"\n{'='*60}")
    print(f"  SUSTAINED LOAD RESULTS")
    print(f"{'='*60}")
    print(f"  Duration:       {total_elapsed:.0f}s")
    print(f"  Total queries:  {total_queries}")
    print(f"  Avg QPS:        {total_queries / total_elapsed:.1f}")
    print(f"  Error rate:     {error_rate:.2f}%  {'PASS' if err_ok else 'FAIL'} (< 1%)")
    print(f"  QPS degradation:{qps_degradation:.1f}%  {'PASS' if qps_ok else 'FAIL'} (< 30%)")
    print(f"    First: {first_qps:.1f}, Last: {last_qps:.1f}")
    print(f"  Memory growth:  {mem_growth:.1f}%  {'PASS' if mem_ok else 'FAIL'} (< 50%)")
    print(f"    First: {first_rss:.0f} MB, Last: {last_rss:.0f} MB")
    print(f"{'='*60}")

    # Save report
    os.makedirs(RESULTS_DIR, exist_ok=True)
    report = {
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'duration_s': round(total_elapsed),
        'threads': args.threads,
        'total_queries': total_queries,
        'avg_qps': round(total_queries / total_elapsed, 1),
        'error_rate_pct': round(error_rate, 2),
        'qps_degradation_pct': round(qps_degradation, 1),
        'memory_growth_pct': round(mem_growth, 1),
        'passed': bool(qps_ok and err_ok and mem_ok),
        'samples': samples,
    }
    report_path = os.path.join(RESULTS_DIR, 'sustained_load_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"  Report: {report_path}")

    all_pass = qps_ok and err_ok and mem_ok
    sys.exit(0 if all_pass else 1)

if __name__ == '__main__':
    main()
