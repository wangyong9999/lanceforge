#!/usr/bin/env python3
"""
Chaos scenario runner — minimum-viable (B5-min).

Reads a YAML scenario, boots a cluster, injects the fault at the
configured time, drives query traffic, and checks that the invariants
hold. Writes one JSON result file per iteration into `chaos/results/`.

Scope (ROADMAP_0.2 §3.3 B5-min):
  - Two scenarios (worker_kill, worker_stall)
  - 10 iterations each, 100% pass rate required
  - Runs on a single-machine LanceForge cluster (2 workers + coord)
  - Not connected to CI yet — that's B5 full (post-alpha)

Usage:
  python3 chaos/runner.py chaos/scenarios/worker_kill.yaml --iters 10
"""
import sys, os, time, subprocess, json, signal, shutil, argparse, threading, struct
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lance-integration', 'tools'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lance-integration', 'sdk', 'python'))

import yaml
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import grpc
from test_helpers import wait_for_grpc

# Proto stubs live under the bench directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lance-integration', 'bench'))
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(ROOT, "target", "release"))
BASE = "/tmp/lanceforge_chaos"
COORD_PORT = 58950
WORKER_PORTS = [58900, 58901]
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

DIM = 32
N_ROWS = 5000  # Small — each iteration needs to be fast


def kill_cluster():
    os.system(
        f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|"
        f"lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null"
    )
    time.sleep(1)


def start_cluster(replica_factor=2):
    kill_cluster()
    if os.path.exists(BASE): shutil.rmtree(BASE)
    os.makedirs(BASE)
    cfg = {
        'tables': [],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]} for i in range(2)],
        'default_table_path': BASE,
        'replica_factor': replica_factor,
        'server': {'slow_query_ms': 0},
        'health_check': {'interval_secs': 2, 'unhealthy_threshold': 2, 'connect_timeout_secs': 2},
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
            raise RuntimeError(f"worker w{i} did not come up")
    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT
    )
    procs["coord"] = p
    if not wait_for_grpc("127.0.0.1", COORD_PORT):
        raise RuntimeError("coordinator did not come up")
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
    w = ipc.new_stream(s, batch.schema)
    w.write_batch(batch)
    w.close()
    return s.getvalue().to_pybytes()


def setup_table(stub, table):
    rng = np.random.default_rng(42)
    vecs = rng.standard_normal((N_ROWS, DIM)).astype(np.float32)
    batch = pa.record_batch([
        pa.array(range(N_ROWS), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(pa.array(vecs.flatten()), list_size=DIM),
    ], names=['id', 'vector'])
    r = stub.CreateTable(pb.CreateTableRequest(table_name=table, arrow_ipc_data=ipc_bytes(batch)))
    if r.error: raise RuntimeError(r.error)
    # Replicate shards so we actually have failover capacity.
    rb = stub.Rebalance(pb.RebalanceRequest())
    if rb.error: raise RuntimeError(f"rebalance: {rb.error}")
    time.sleep(1)


def inject_fault(procs, fault):
    """Blocking: sleeps until fault.at_second, fires the signal. For
    stall faults, spawns a background thread to resume after the
    configured delay."""
    time.sleep(fault['at_second'])
    target = fault['target']
    if target not in procs:
        raise RuntimeError(f"unknown fault target: {target}")
    ftype = fault['type']
    if ftype == 'sigkill':
        procs[target].send_signal(signal.SIGKILL)
    elif ftype == 'sigstop':
        procs[target].send_signal(signal.SIGSTOP)
        def resume():
            time.sleep(fault.get('resume_after_seconds', 5))
            try: procs[target].send_signal(signal.SIGCONT)
            except Exception: pass
        threading.Thread(target=resume, daemon=True).start()
    else:
        raise RuntimeError(f"unknown fault type: {ftype}")


def run_query_workload(stub, table, duration_s, qps):
    """Drive queries at `qps` for `duration_s` seconds. Return
    (attempts, successes, failures, latencies_ms)."""
    rng = np.random.default_rng(7)
    test_queries = [rng.standard_normal(DIM).astype(np.float32) for _ in range(20)]

    end_at = time.perf_counter() + duration_s
    interval = 1.0 / qps
    attempts = 0
    successes = 0
    failures = 0
    latencies = []

    def one_query(i):
        try:
            t0 = time.perf_counter()
            stub.AnnSearch(pb.AnnSearchRequest(
                table_name=table, vector_column="vector",
                query_vector=struct.pack(f"<{DIM}f", *test_queries[i % len(test_queries)].tolist()),
                dimension=DIM, k=5, nprobes=4, metric_type=0,
            ), timeout=3.0)
            return (time.perf_counter() - t0) * 1000, None
        except Exception as e:
            return None, str(e)[:60]

    with ThreadPoolExecutor(max_workers=4) as ex:
        i = 0
        while time.perf_counter() < end_at:
            start = time.perf_counter()
            fut = ex.submit(one_query, i)
            try:
                lat, err = fut.result(timeout=3.5)
                attempts += 1
                if err is None:
                    successes += 1
                    latencies.append(lat)
                else:
                    failures += 1
            except Exception:
                attempts += 1
                failures += 1
            i += 1
            elapsed = time.perf_counter() - start
            sleep_for = max(0, interval - elapsed)
            if sleep_for > 0: time.sleep(sleep_for)

    return attempts, successes, failures, latencies


def run_iteration(scenario, iter_idx):
    """Run a single scenario iteration. Returns a result dict."""
    cluster = scenario['cluster']
    fault = scenario['fault']
    inv = scenario['invariant']

    procs = start_cluster(replica_factor=cluster.get('replica_factor', 2))
    try:
        stub = make_stub()
        setup_table(stub, 'chaos')

        # Spawn fault injection in parallel with the query workload.
        fault_thread = threading.Thread(target=inject_fault, args=(procs, fault), daemon=True)
        fault_thread.start()

        attempts, successes, failures, latencies = run_query_workload(
            stub, 'chaos',
            duration_s=inv['duration_seconds'],
            qps=inv['queries_per_second'],
        )
        fault_thread.join(timeout=inv.get('max_recovery_seconds', 15))

        rate = successes / max(1, attempts)
        passed = rate >= inv['min_query_success_rate']
        return {
            'iteration': iter_idx,
            'timestamp': datetime.now().isoformat(),
            'attempts': attempts,
            'successes': successes,
            'failures': failures,
            'success_rate': rate,
            'min_required': inv['min_query_success_rate'],
            'passed': passed,
            'p50_ms': float(np.percentile(latencies, 50)) if latencies else None,
            'p99_ms': float(np.percentile(latencies, 99)) if latencies else None,
        }
    finally:
        for p in procs.values():
            try:
                # Make sure SIGSTOPped children are CONT-ed before kill so they can die.
                p.send_signal(signal.SIGCONT)
            except Exception: pass
            try: p.terminate()
            except Exception: pass
        kill_cluster()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('scenario', help='Path to scenario YAML')
    ap.add_argument('--iters', type=int, default=10)
    args = ap.parse_args()

    with open(args.scenario) as f:
        scenario = yaml.safe_load(f)

    name = scenario['name']
    print(f"=== Chaos scenario: {name} ({args.iters} iters) ===")
    results = []
    for i in range(args.iters):
        print(f"  iter {i+1}/{args.iters}...", end=" ", flush=True)
        try:
            r = run_iteration(scenario, i + 1)
            results.append(r)
            print(f"success_rate={r['success_rate']:.2%} {'PASS' if r['passed'] else 'FAIL'}")
        except Exception as e:
            results.append({'iteration': i + 1, 'passed': False, 'error': str(e)})
            print(f"ERROR: {e}")

    passed = sum(1 for r in results if r.get('passed'))
    summary = {
        'scenario': name,
        'iterations': args.iters,
        'passed': passed,
        'failed': args.iters - passed,
        'pass_rate': passed / args.iters,
        'results': results,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(RESULTS_DIR, f"{name}_{ts}.json")
    with open(out, 'w') as f: json.dump(summary, f, indent=2)
    print(f"\n{passed}/{args.iters} passed → {out}")
    sys.exit(0 if passed == args.iters else 1)


if __name__ == '__main__':
    main()
