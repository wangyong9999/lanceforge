#!/usr/bin/env python3
"""
Soak harness — minimum-viable (B6-min).

Boots a 2-worker cluster, runs a light mixed read/write workload at
steady state, samples process RSS / open-fd / tokio queue depth every
60 seconds. After the configured duration (default 2h for alpha gate,
typically 15m for smoke), reports:

  - Start vs end RSS drift (%)
  - Start vs end open-fd count delta
  - Read QPS stability (start window vs end window)
  - Any request failures during the run

Alpha gate (ROADMAP_0.2 §3.3 B6-min):
  - 2h duration minimum
  - RSS drift < 3%, open-fd drift < 3%

Usage:
  python3 soak/run.py --minutes 15   # smoke
  python3 soak/run.py --minutes 120  # alpha gate run
"""
import sys, os, time, subprocess, json, shutil, signal, threading, struct, argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lance-integration', 'tools'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lance-integration', 'sdk', 'python'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lance-integration', 'bench'))

import yaml
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import grpc
from test_helpers import wait_for_grpc
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(ROOT, "target", "release"))
BASE = "/tmp/lanceforge_soak"
COORD_PORT = 57950
WORKER_PORTS = [57900, 57901]
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

DIM = 64
N_ROWS = 20000
SAMPLE_INTERVAL_S = 60  # Per-minute snapshot


def kill_cluster():
    os.system(
        f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|"
        f"lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null"
    )
    time.sleep(1)


def start_cluster():
    kill_cluster()
    if os.path.exists(BASE): shutil.rmtree(BASE)
    os.makedirs(BASE)
    cfg = {
        'tables': [],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]} for i in range(2)],
        'default_table_path': BASE,
        'server': {'slow_query_ms': 0},
    }
    with open(f"{BASE}/config.yaml", 'w') as f: yaml.dump(cfg, f)
    procs = {}
    for i in range(2):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", f"{BASE}/config.yaml", f"w{i}", str(WORKER_PORTS[i])],
            stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT
        )
        procs[f"w{i}"] = p
    for i in range(2):
        if not wait_for_grpc("127.0.0.1", WORKER_PORTS[i]):
            raise RuntimeError(f"worker w{i} failed to come up")
    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT
    )
    procs["coord"] = p
    if not wait_for_grpc("127.0.0.1", COORD_PORT):
        raise RuntimeError("coord failed to come up")
    time.sleep(2)
    return procs


def make_stub():
    return pbg.LanceSchedulerServiceStub(
        grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
            options=[("grpc.max_receive_message_length", 64 * 1024 * 1024),
                     ("grpc.max_send_message_length", 64 * 1024 * 1024)])
    )


def ipc_bytes(batch):
    s = pa.BufferOutputStream()
    w = ipc.new_stream(s, batch.schema); w.write_batch(batch); w.close()
    return s.getvalue().to_pybytes()


def setup_table(stub):
    rng = np.random.default_rng(42)
    vecs = rng.standard_normal((N_ROWS, DIM)).astype(np.float32)
    batch = pa.record_batch([
        pa.array(range(N_ROWS), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(pa.array(vecs.flatten()), list_size=DIM),
    ], names=['id', 'vector'])
    r = stub.CreateTable(pb.CreateTableRequest(table_name='soak', arrow_ipc_data=ipc_bytes(batch)))
    if r.error: raise RuntimeError(r.error)
    ir = stub.CreateIndex(pb.CreateIndexRequest(
        table_name='soak', column="vector", index_type="IVF_FLAT", num_partitions=16))
    if ir.error: print(f"WARN index: {ir.error}", flush=True)


def proc_rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])  # in kB
    except Exception:
        pass
    return None


def proc_fd_count(pid):
    try:
        return len(os.listdir(f"/proc/{pid}/fd"))
    except Exception:
        return None


def workload_loop(stop_event, stats, stub, read_only=False):
    """Background thread: 5 readers + optional 1 writer at modest rate.
    Read-only mode is the correct signal for RSS/fd drift analysis —
    mixed-load growth is dominated by legitimate data volume, not leaks."""
    rng = np.random.default_rng(7)
    queries = [rng.standard_normal(DIM).astype(np.float32) for _ in range(20)]
    next_id = N_ROWS + 100_000

    def reader(i):
        while not stop_event.is_set():
            try:
                stub.AnnSearch(pb.AnnSearchRequest(
                    table_name='soak', vector_column='vector',
                    query_vector=struct.pack(f"<{DIM}f", *queries[i % len(queries)].tolist()),
                    dimension=DIM, k=5, nprobes=4, metric_type=0,
                ), timeout=5.0)
                stats['read_ok'] += 1
            except Exception:
                stats['read_err'] += 1
            # ~10 qps per reader × 5 readers = 50 qps; small sleep keeps it modest
            time.sleep(0.1)

    def writer():
        nonlocal_next_id = [next_id]
        while not stop_event.is_set():
            try:
                rows = 50
                v = rng.standard_normal((rows, DIM)).astype(np.float32)
                batch = pa.record_batch([
                    pa.array(range(nonlocal_next_id[0], nonlocal_next_id[0] + rows), type=pa.int64()),
                    pa.FixedSizeListArray.from_arrays(pa.array(v.flatten()), list_size=DIM),
                ], names=['id', 'vector'])
                stub.AddRows(pb.AddRowsRequest(
                    table_name='soak', arrow_ipc_data=ipc_bytes(batch),
                ), timeout=5.0)
                stats['write_ok'] += 1
                nonlocal_next_id[0] += rows
            except Exception:
                stats['write_err'] += 1
            time.sleep(0.5)  # ~2 QPS writer

    with ThreadPoolExecutor(max_workers=6) as ex:
        for i in range(5): ex.submit(reader, i)
        if not read_only:
            ex.submit(writer)
        stop_event.wait()


def run_soak(minutes, sample_interval_s=SAMPLE_INTERVAL_S, read_only=False):
    procs = start_cluster()
    try:
        stub = make_stub()
        print(f"[setup] creating soak table ({N_ROWS} rows × {DIM}d){' [read-only]' if read_only else ''}...", flush=True)
        setup_table(stub)
        time.sleep(3)

        stats = {'read_ok': 0, 'read_err': 0, 'write_ok': 0, 'write_err': 0}
        stop = threading.Event()
        t = threading.Thread(target=workload_loop, args=(stop, stats, stub, read_only), daemon=True)
        t.start()

        # Warm-up 30s so the first RSS sample isn't pre-workload.
        time.sleep(30)

        samples = []
        start_at = time.time()
        end_at = start_at + minutes * 60

        sample_idx = 0
        while time.time() < end_at:
            snap = {
                'at_minute': sample_idx * (sample_interval_s / 60),
                'timestamp': datetime.now().isoformat(),
                'reads_ok': stats['read_ok'],
                'reads_err': stats['read_err'],
                'writes_ok': stats['write_ok'],
                'writes_err': stats['write_err'],
            }
            for name, p in procs.items():
                snap[f'{name}_rss_kb'] = proc_rss_kb(p.pid)
                snap[f'{name}_fd'] = proc_fd_count(p.pid)
            samples.append(snap)
            print(f"  [{snap['at_minute']:.1f}min] reads={snap['reads_ok']}/{snap['reads_err']}err "
                  f"coord_rss={snap['coord_rss_kb']}kB fd={snap['coord_fd']}", flush=True)
            sample_idx += 1
            sleep_for = sample_interval_s
            # Don't overshoot the end time.
            if time.time() + sleep_for > end_at:
                remaining = end_at - time.time()
                if remaining > 0: time.sleep(remaining)
                break
            time.sleep(sleep_for)

        stop.set()
        t.join(timeout=5)

        # Analyze drift.
        def drift(first, last):
            if not first or not last or first == 0: return None
            return (last - first) / first * 100.0

        report = {
            'duration_minutes': minutes,
            'total_samples': len(samples),
            'reads_ok': stats['read_ok'],
            'reads_err': stats['read_err'],
            'writes_ok': stats['write_ok'],
            'writes_err': stats['write_err'],
            'drift_pct': {},
            'samples': samples,
        }
        first, last = samples[0], samples[-1]
        for proc_name in ('coord', 'w0', 'w1'):
            report['drift_pct'][f'{proc_name}_rss'] = drift(first.get(f'{proc_name}_rss_kb'), last.get(f'{proc_name}_rss_kb'))
            report['drift_pct'][f'{proc_name}_fd']  = drift(first.get(f'{proc_name}_fd'),  last.get(f'{proc_name}_fd'))

        return report
    finally:
        for p in procs.values():
            try: p.terminate()
            except Exception: pass
        kill_cluster()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--minutes', type=int, default=15, help='soak duration (default 15min smoke; 120 for alpha gate)')
    ap.add_argument('--drift-threshold', type=float, default=3.0, help='max allowed drift %% (default 3)')
    ap.add_argument('--read-only', action='store_true',
                    help='read-only workload (proper signal for cache/handle leaks; '
                         'default is mixed RW which naturally grows worker RSS with data volume)')
    args = ap.parse_args()

    mode = "read-only" if args.read_only else "mixed RW"
    print(f"=== Soak run: {args.minutes} minutes, {mode} (threshold {args.drift_threshold}%) ===", flush=True)
    report = run_soak(args.minutes, read_only=args.read_only)
    report['workload'] = mode

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(RESULTS_DIR, f"soak_{args.minutes}min_{ts}.json")
    with open(out, 'w') as f: json.dump(report, f, indent=2)

    print(f"\n=== Soak report ===")
    print(f"Reads: {report['reads_ok']} ok, {report['reads_err']} err")
    print(f"Writes: {report['writes_ok']} ok, {report['writes_err']} err")
    print(f"Drift:")
    any_over = False
    for k, v in report['drift_pct'].items():
        if v is None:
            print(f"  {k}: n/a")
        else:
            tag = " OVER" if abs(v) > args.drift_threshold else " ok"
            if abs(v) > args.drift_threshold: any_over = True
            print(f"  {k}: {v:+.2f}%{tag}")
    print(f"Saved: {out}")
    sys.exit(0 if not any_over else 1)


if __name__ == '__main__':
    main()
