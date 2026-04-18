#!/usr/bin/env python3
"""
Phase 17 Benchmark Matrix — quantifies LanceForge's performance envelope
across data scale, vector dimension, shard count, concurrency, and workload.

Usage:
    python3 bench_phase17_matrix.py                 # full matrix (slow)
    python3 bench_phase17_matrix.py --smoke         # 5-min smoke matrix
    python3 bench_phase17_matrix.py --scale 100000  # single scale
    python3 bench_phase17_matrix.py --help

Output:
    results/phase17/matrix_<timestamp>.json
    results/phase17/matrix_<timestamp>.csv
    results/phase17/README_<timestamp>.md  (markdown report)
"""

import sys, os, time, struct, subprocess, json, csv, shutil, argparse
from datetime import datetime
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from test_helpers import wait_for_grpc
import pyarrow as pa
import pyarrow.ipc as ipc
import grpc, yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
OUTDIR = os.path.join(os.path.dirname(__file__), 'results', 'phase17')
os.makedirs(OUTDIR, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────
# Matrix definition
# ──────────────────────────────────────────────────────────────────────────

FULL_MATRIX = {
    'scale':       [10_000, 100_000, 1_000_000],
    'dim':         [128, 768],
    'shards':      [1, 2, 4],
    'concurrency': [1, 10, 50],
    'k':           [10],
    'nprobes':     [10],
}

# Mid-scale: 100K at dim 128, all shard counts. Finishes in ~5-10 min.
MID_MATRIX = {
    'scale':       [100_000],
    'dim':         [128],
    'shards':      [1, 2, 4],
    'concurrency': [1, 10, 50],
    'k':           [10],
    'nprobes':     [10],
}

# Large-scale: 1M at dim 128, reduced shard/conc grid for time budget.
LARGE_MATRIX = {
    'scale':       [1_000_000],
    'dim':         [128],
    'shards':      [1, 2, 4],
    'concurrency': [10, 50],
    'k':           [10],
    'nprobes':     [10],
}

# nprobes recall sweep: validate the empirical formula
# recall ≈ f(nprobes / num_partitions) at fixed scale.
NPROBES_MATRIX = {
    'scale':       [1_000_000],
    'dim':         [128],
    'shards':      [1],
    'concurrency': [10],
    'k':           [10],
    'nprobes':     [5, 10, 20, 40, 80],
}

SMOKE_MATRIX = {
    'scale':       [10_000],
    'dim':         [128],
    'shards':      [1, 2],
    'concurrency': [1, 10],
    'k':           [10],
    'nprobes':     [10],
}

# H12: large-k IPC-cost sweep. Holds everything else constant and
# steps k from 10 to 10000 (the server-default max_k) so the IPC +
# coordinator-side pagination overhead shows up as a distinct curve.
# Expected shape: monotonic decrease — each multiplication of k by
# 10 roughly triples per-query P99 once the IPC payload exceeds
# ~100 KB per shard. Run with `--matrix kk` (see MATRIX_PRESETS).
LARGE_K_MATRIX = {
    'scale':       [100_000],
    'dim':         [128],
    'shards':      [2],
    'concurrency': [10],
    'k':           [10, 100, 1000, 10_000],
    'nprobes':     [10],
}

# Per-config measurement parameters
WARMUP_QUERIES   = 20
MEASURE_QUERIES  = 200    # per thread (default)
GROUND_TRUTH_N   = 50     # queries for recall computation
RECALL_K         = 10


def queries_for_scale(scale):
    """Auto-adjust per-thread query count by dataset size to keep per-config
    runtime under ~2 minutes. 1M IVF queries are ~10x slower than 100K."""
    if scale >= 1_000_000: return 40
    if scale >= 500_000:   return 80
    return MEASURE_QUERIES

# Cluster layout
BASE_PORT = 58000
COORD_PORT = BASE_PORT + 50
WORKER_PORTS = [BASE_PORT + i for i in range(8)]  # up to 8 workers
BASE_DIR = '/tmp/lanceforge_bench17'


# ──────────────────────────────────────────────────────────────────────────
# Cluster lifecycle
# ──────────────────────────────────────────────────────────────────────────

def kill_cluster():
    os.system(f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|lance-worker.*{BASE_PORT}' 2>/dev/null")
    time.sleep(2)


def start_cluster(num_workers, base_dir):
    kill_cluster()
    if os.path.exists(base_dir): shutil.rmtree(base_dir)
    os.makedirs(base_dir)

    config = {
        'tables': [],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]}
                      for i in range(num_workers)],
        'default_table_path': base_dir,
        'server': {'max_k': 50_000, 'slow_query_ms': 0},  # don't log slow in bench
    }
    cfg = os.path.join(base_dir, 'config.yaml')
    with open(cfg, 'w') as f: yaml.dump(config, f)

    procs = []
    for i in range(num_workers):
        p = subprocess.Popen([f"{BIN}/lance-worker", cfg, f"w{i}", str(WORKER_PORTS[i])],
            stdout=open(f"{base_dir}/w{i}.log", "w"), stderr=subprocess.STDOUT)
        procs.append(p)
    for i in range(num_workers):
        if not wait_for_grpc("127.0.0.1", WORKER_PORTS[i]):
            raise RuntimeError(f"worker w{i} failed to start")
    p = subprocess.Popen([f"{BIN}/lance-coordinator", cfg, str(COORD_PORT)],
        stdout=open(f"{base_dir}/coord.log", "w"), stderr=subprocess.STDOUT)
    procs.append(p)
    if not wait_for_grpc("127.0.0.1", COORD_PORT):
        raise RuntimeError("coord failed to start")
    time.sleep(2)
    return procs


def make_stub():
    # Raise both receive and send limits. Client default ~4 MB, server is
    # configured via max_response_bytes + 16 MiB headroom. 512 MiB lets us
    # ingest ~1M × 128d vectors in a single CreateTable call.
    return pbg.LanceSchedulerServiceStub(
        grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
            options=[("grpc.max_receive_message_length", 512 * 1024 * 1024),
                     ("grpc.max_send_message_length", 512 * 1024 * 1024)]))


# ──────────────────────────────────────────────────────────────────────────
# Data & ground truth
# ──────────────────────────────────────────────────────────────────────────

def synthesize(n_rows, dim, seed=42):
    """Deterministic synthetic dataset; isotropic Gaussian."""
    rng = np.random.default_rng(seed)
    vecs = rng.standard_normal((n_rows, dim)).astype(np.float32)
    return vecs


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema); w.write_batch(batch); w.close()
    return sink.getvalue().to_pybytes()


def ev(v): return struct.pack(f"<{len(v)}f", *v.tolist() if hasattr(v, 'tolist') else v)


def _make_batch(ids_range, vecs, dim):
    n = len(vecs)
    return pa.record_batch([
        pa.array(list(ids_range), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(pa.array(vecs.flatten()), list_size=dim),
    ], names=['id', 'vector'])


def ingest(stub, table_name, vecs, dim, chunk_rows=None, create_index=True):
    if chunk_rows is None:
        # ~64 MB per chunk to stay under default server 256 MB limit even
        # with overhead. 64MB / (dim × 4B) rows.
        chunk_rows = max(10_000, (64 * 1024 * 1024) // (dim * 4))
    """Create a table and (if large) stream the rest via AddRows.
    Returns total rows inserted. Chunked to stay under gRPC message limits
    (≈256 MB payload ≈ 500K rows at 128d fp32)."""
    n = len(vecs)
    first = min(chunk_rows, n)
    # CreateTable with the first chunk
    r = stub.CreateTable(pb.CreateTableRequest(
        table_name=table_name,
        arrow_ipc_data=ipc_bytes(_make_batch(range(first), vecs[:first], dim))))
    if r.error: raise RuntimeError(f"CreateTable: {r.error}")
    total = r.num_rows
    # Stream the rest
    off = first
    while off < n:
        end = min(off + chunk_rows, n)
        br = _make_batch(range(off, end), vecs[off:end], dim)
        ar = stub.AddRows(pb.AddRowsRequest(
            table_name=table_name, arrow_ipc_data=ipc_bytes(br)))
        if ar.error: raise RuntimeError(f"AddRows: {ar.error}")
        total += ar.affected_rows
        off = end
    # Create IVF_FLAT index on vector column — crucial for fair latency.
    # Without this, queries scan full table (100x slower at 1M rows).
    if create_index:
        num_partitions = 32 if n < 100_000 else 128
        t_idx = time.perf_counter()
        ir = stub.CreateIndex(pb.CreateIndexRequest(
            table_name=table_name, column="vector",
            index_type="IVF_FLAT", num_partitions=num_partitions))
        if ir.error:
            print(f"[ingest] WARN CreateIndex failed: {ir.error}")
        else:
            print(f"[ingest] Index built in {time.perf_counter()-t_idx:.1f}s ({num_partitions} partitions)", flush=True)
    return total


def brute_force_gt(train, query, k):
    """L2 brute-force top-k for recall ground truth."""
    dists = np.linalg.norm(train - query, axis=1)
    return np.argsort(dists)[:k].tolist()


# ──────────────────────────────────────────────────────────────────────────
# Measurement
# ──────────────────────────────────────────────────────────────────────────

def run_single_query(stub, table, vec, dim, k, nprobes):
    t0 = time.perf_counter()
    r = stub.AnnSearch(pb.AnnSearchRequest(
        table_name=table, vector_column="vector",
        query_vector=ev(vec),
        dimension=dim, k=k, nprobes=nprobes, metric_type=0))
    elapsed = time.perf_counter() - t0
    if r.error: raise RuntimeError(r.error)
    # Decode result IDs
    if r.arrow_ipc_data:
        t = ipc.open_stream(r.arrow_ipc_data).read_all()
        ids = t.column("id").to_pylist() if "id" in t.schema.names else []
    else:
        ids = []
    return elapsed, ids


def measure_config(stub, table, queries, dim, k, nprobes, concurrency, gt_ids, scale=0):
    """Run WARMUP + MEASURE queries at a given concurrency level.
    Returns dict of metrics: p50, p95, p99, qps, recall, errors.

    Each worker thread uses its OWN channel+stub. A single shared channel
    serializes through one HTTP/2 connection (and its stream concurrency
    limit), which Phase 17 profiling revealed as the conc=50 collapse cause.
    """
    # Warmup (single-threaded)
    for i in range(WARMUP_QUERIES):
        run_single_query(stub, table, queries[i % len(queries)], dim, k, nprobes)

    # Measurement
    latencies = []
    recalls = []
    errors = 0
    total = queries_for_scale(scale) if scale else MEASURE_QUERIES

    def worker(tid):
        # Per-thread channel + stub (avoids shared HTTP/2 serialization).
        local_stub = make_stub()
        local_lat = []
        local_rec = []
        local_err = 0
        for i in range(total):
            q_idx = (tid * total + i) % len(queries)
            try:
                dt, ids = run_single_query(local_stub, table, queries[q_idx], dim, k, nprobes)
                local_lat.append(dt)
                if q_idx < len(gt_ids):
                    hits = len(set(ids) & set(gt_ids[q_idx][:RECALL_K]))
                    local_rec.append(hits / RECALL_K)
            except Exception:
                local_err += 1
        return local_lat, local_rec, local_err

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(worker, i) for i in range(concurrency)]
        for f in as_completed(futures):
            lat, rec, err = f.result()
            latencies.extend(lat); recalls.extend(rec); errors += err
    wall = time.perf_counter() - t0
    successful = len(latencies)

    lat_ms = sorted([l * 1000 for l in latencies])
    def pct(p):
        if not lat_ms: return 0
        idx = min(len(lat_ms) - 1, int(len(lat_ms) * p / 100))
        return lat_ms[idx]

    return {
        'qps': successful / wall if wall > 0 else 0,
        'p50_ms': pct(50),
        'p95_ms': pct(95),
        'p99_ms': pct(99),
        'mean_ms': sum(lat_ms) / len(lat_ms) if lat_ms else 0,
        'recall_at_k': sum(recalls) / len(recalls) if recalls else 0,
        'errors': errors,
        'successful': successful,
        'wall_s': wall,
    }


# ──────────────────────────────────────────────────────────────────────────
# Driver
# ──────────────────────────────────────────────────────────────────────────

def run_matrix(matrix, smoke):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = []

    total_runs = 1
    for v in matrix.values(): total_runs *= len(v)
    print(f"Running {total_runs} configurations...\n")

    run_idx = 0
    for scale in matrix['scale']:
        for dim in matrix['dim']:
            # Generate data ONCE per (scale, dim) — reused across shard counts
            print(f"[data] synthesizing {scale} × {dim}", flush=True)
            train = synthesize(scale, dim, seed=42)
            # Scale ground-truth query count with dataset size (big datasets →
            # brute-force GT is expensive, so use fewer queries).
            gt_n = 50 if scale <= 100_000 else 20
            test = synthesize(gt_n, dim, seed=7)
            print(f"[data] computing ground truth for {len(test)} queries (brute force)", flush=True)
            t_gt = time.perf_counter()
            gt = [brute_force_gt(train, q, RECALL_K) for q in test]
            print(f"[data] GT done in {time.perf_counter()-t_gt:.1f}s", flush=True)

            for shards in matrix['shards']:
                # Start cluster with `shards` workers
                print(f"\n[cluster] starting {shards} workers")
                try:
                    procs = start_cluster(shards, BASE_DIR)
                    stub = make_stub()

                    table = f"bench_{scale}_{dim}"
                    print(f"[ingest] {scale} rows, auto-sharding across {shards} workers")
                    t0 = time.perf_counter()
                    ingest(stub, table, train, dim)
                    ingest_s = time.perf_counter() - t0
                    print(f"[ingest] done in {ingest_s:.1f}s ({scale/ingest_s:.0f} rows/s)")

                    for k in matrix['k']:
                        for nprobes in matrix['nprobes']:
                            for conc in matrix['concurrency']:
                                run_idx += 1
                                cfg_label = f"scale={scale} dim={dim} shards={shards} k={k} nprobes={nprobes} conc={conc}"
                                print(f"[{run_idx}/{total_runs}] {cfg_label}")
                                metrics = measure_config(
                                    stub, table, test, dim, k, nprobes, conc, gt, scale=scale)
                                row = {
                                    'scale': scale, 'dim': dim, 'shards': shards,
                                    'k': k, 'nprobes': nprobes, 'concurrency': conc,
                                    'ingest_s': round(ingest_s, 2),
                                    **{k: round(v, 3) if isinstance(v, float) else v
                                       for k, v in metrics.items()},
                                }
                                results.append(row)
                                print(f"   QPS={row['qps']:.1f}  P50={row['p50_ms']:.1f}ms  "
                                      f"P99={row['p99_ms']:.1f}ms  recall={row['recall_at_k']:.3f}  "
                                      f"errors={row['errors']}")
                finally:
                    for p in procs:
                        try: p.terminate()
                        except: pass
                    time.sleep(1)
                    kill_cluster()

    # Persist
    json_path = os.path.join(OUTDIR, f"matrix_{ts}.json")
    csv_path  = os.path.join(OUTDIR, f"matrix_{ts}.csv")
    md_path   = os.path.join(OUTDIR, f"REPORT_{ts}.md")

    with open(json_path, 'w') as f:
        json.dump({'timestamp': ts, 'smoke': smoke, 'matrix': matrix, 'results': results},
                  f, indent=2)
    with open(csv_path, 'w', newline='') as f:
        if results:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            for r in results: w.writerow(r)
    write_report(md_path, ts, smoke, results)

    print(f"\n✓ Results: {json_path}")
    print(f"✓ CSV:     {csv_path}")
    print(f"✓ Report:  {md_path}")
    return results


def write_report(path, ts, smoke, results):
    with open(path, 'w') as f:
        f.write(f"# LanceForge Phase 17 Benchmark Report\n\n")
        f.write(f"Generated: {ts}  Mode: {'SMOKE' if smoke else 'FULL'}  "
                f"Configurations: {len(results)}\n\n")

        f.write("## Summary Table\n\n")
        f.write("| scale | dim | shards | conc | QPS | P50 ms | P99 ms | recall@10 | errors |\n")
        f.write("|------:|----:|-------:|-----:|----:|-------:|-------:|----------:|-------:|\n")
        for r in results:
            f.write(f"| {r['scale']} | {r['dim']} | {r['shards']} | {r['concurrency']} | "
                    f"{r['qps']:.1f} | {r['p50_ms']:.1f} | {r['p99_ms']:.1f} | "
                    f"{r['recall_at_k']:.3f} | {r['errors']} |\n")

        # Scalability analysis
        f.write("\n## Scalability (shards=1 vs shards=2/4)\n\n")
        for scale in sorted(set(r['scale'] for r in results)):
            for dim in sorted(set(r['dim'] for r in results)):
                for conc in sorted(set(r['concurrency'] for r in results)):
                    subset = [r for r in results
                              if r['scale']==scale and r['dim']==dim and r['concurrency']==conc]
                    if len(subset) < 2: continue
                    s1 = next((r for r in subset if r['shards']==1), None)
                    if not s1: continue
                    f.write(f"- scale={scale} dim={dim} conc={conc}: shards=1 QPS={s1['qps']:.1f}")
                    for r in subset:
                        if r['shards'] == 1: continue
                        speedup = r['qps'] / s1['qps'] if s1['qps'] > 0 else 0
                        f.write(f" → shards={r['shards']} QPS={r['qps']:.1f} (×{speedup:.2f})")
                    f.write("\n")

        f.write("\n## Observations\n")
        f.write("\n(Fill in after reviewing data: bottlenecks, surprises, recall@10 trends.)\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--smoke', action='store_true', help='Small matrix (~5 min)')
    ap.add_argument('--mid', action='store_true', help='Mid matrix: 100K × shards 1/2/4 × conc 1/10/50')
    ap.add_argument('--large', action='store_true', help='Large matrix: 1M × 128d × shards 1/2/4 × conc 10/50')
    ap.add_argument('--nprobes-sweep', action='store_true', help='nprobes recall sweep at 1M')
    ap.add_argument('--large-k', action='store_true',
                    help='H12: k-sweep at 100K × 128d × shards=2 × conc=10 '
                         '(k ∈ {10, 100, 1000, 10000}); reveals IPC/pagination cliff')
    ap.add_argument('--scale', type=int, help='Override: single scale only')
    args = ap.parse_args()

    if args.smoke: matrix = SMOKE_MATRIX
    elif args.mid: matrix = MID_MATRIX
    elif args.large: matrix = LARGE_MATRIX
    elif args.nprobes_sweep: matrix = NPROBES_MATRIX
    elif args.large_k: matrix = LARGE_K_MATRIX
    else: matrix = FULL_MATRIX
    if args.scale:
        matrix['scale'] = [args.scale]

    try:
        run_matrix(matrix, smoke=args.smoke)
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        kill_cluster()


if __name__ == '__main__':
    main()
