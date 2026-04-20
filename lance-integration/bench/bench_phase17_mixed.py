#!/usr/bin/env python3
"""
Phase 17 mixed read/write benchmark.

Goal: quantify how much write traffic degrades read QPS / latency.
Workload: N reader threads + M writer threads against same table.
Measures read QPS/P99 in baseline (read-only) vs mixed (write_pct=10/30).

Output: appended to results/phase17/.
"""
import sys, os, time, struct, subprocess, json, shutil, threading
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from test_helpers import wait_for_grpc
import pyarrow as pa
import pyarrow.ipc as ipc
import grpc, yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = '/tmp/lanceforge_bench17_mixed'
COORD_PORT = 59950
WORKER_PORTS = [59900, 59901]
OUTDIR = os.path.join(os.path.dirname(__file__), 'results', 'phase17')
os.makedirs(OUTDIR, exist_ok=True)

DIM = 128
N_BASE = 100_000          # baseline rows
DURATION_S = 30           # per scenario
READER_THREADS = 10


def kill():
    os.system(f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null")
    time.sleep(2)


def start_cluster():
    kill()
    if os.path.exists(BASE): shutil.rmtree(BASE)
    os.makedirs(BASE)
    # B2.1 H1 sweep: let env override read_consistency_secs so we can
    # empirically test whether cache-key version churn is the dominant
    # cause of mixed-RW QPS degradation.
    rc_secs = int(os.environ.get("LANCEFORGE_RC_SECS", "3"))
    # B2.2 read/write split: LANCEFORGE_RW_SPLIT=1 pins w0 as write-primary
    # and w1 as read-primary. Default keeps both Either for 0.1.x parity.
    rw_split = os.environ.get("LANCEFORGE_RW_SPLIT", "0") == "1"
    executors = []
    for i in range(2):
        entry = {'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]}
        if rw_split:
            entry['role'] = 'write_primary' if i == 0 else 'read_primary'
        executors.append(entry)
    # B2.6 C2: optional S3/MinIO backend. Set LANCEFORGE_STORAGE_PATH to
    # an s3:// URI to drive the bench against object storage instead of
    # local fs. LANCEFORGE_S3_* env vars populate storage_options. This
    # is the controlled test for "is Mixed-RW 50 QPS also bad on S3, or
    # only on local fs?" — see docs/PROFILE_MIXED_RW.md §8.2.
    storage_path = os.environ.get("LANCEFORGE_STORAGE_PATH", BASE)
    storage_options = {}
    for k in ("aws_access_key_id", "aws_secret_access_key", "aws_endpoint",
              "aws_region", "allow_http"):
        v = os.environ.get(f"LANCEFORGE_S3_{k.upper()}")
        if v:
            storage_options[k] = v
    cfg = {
        'tables': [],
        'executors': executors,
        'default_table_path': storage_path,
        'storage_options': storage_options,
        'server': {'max_k': 50_000, 'slow_query_ms': 0},
        'cache': {'read_consistency_secs': rc_secs},
    }
    with open(f"{BASE}/config.yaml", 'w') as f: yaml.dump(cfg, f)
    procs = []
    for i in range(2):
        p = subprocess.Popen([f"{BIN}/lance-worker", f"{BASE}/config.yaml", f"w{i}", str(WORKER_PORTS[i])],
            stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
        procs.append(p)
    for i in range(2):
        if not wait_for_grpc("127.0.0.1", WORKER_PORTS[i]):
            raise RuntimeError(f"worker w{i} failed")
    p = subprocess.Popen([f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
    procs.append(p)
    if not wait_for_grpc("127.0.0.1", COORD_PORT):
        raise RuntimeError("coord failed")
    time.sleep(2)
    return procs


def make_stub():
    return pbg.LanceSchedulerServiceStub(
        grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
            options=[("grpc.max_receive_message_length", 256 * 1024 * 1024),
                     ("grpc.max_send_message_length", 256 * 1024 * 1024)]))


def ev(v): return struct.pack(f"<{len(v)}f", *v.tolist())
def ipc_bytes(b):
    s = pa.BufferOutputStream(); w = ipc.new_stream(s, b.schema); w.write_batch(b); w.close()
    return s.getvalue().to_pybytes()


def setup_table(stub, table, n_rows):
    rng = np.random.default_rng(42)
    vecs = rng.standard_normal((n_rows, DIM)).astype(np.float32)
    batch = pa.record_batch([
        pa.array(range(n_rows), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(pa.array(vecs.flatten()), list_size=DIM),
    ], names=['id', 'vector'])
    r = stub.CreateTable(pb.CreateTableRequest(table_name=table, arrow_ipc_data=ipc_bytes(batch)))
    if r.error: raise RuntimeError(r.error)
    ir = stub.CreateIndex(pb.CreateIndexRequest(
        table_name=table, column="vector", index_type="IVF_FLAT", num_partitions=32))
    if ir.error: print(f"WARN index: {ir.error}")
    # B2.2: trigger Rebalance so each shard is replicated to replica_factor
    # workers (default 2). Without this, CreateTable produces a 1:1 shard→
    # worker assignment, which makes the read/write role preference a no-op
    # because there's only one candidate per shard.
    if os.environ.get("LANCEFORGE_RW_SPLIT", "0") == "1":
        rb = stub.Rebalance(pb.RebalanceRequest())
        if rb.error: print(f"WARN rebalance: {rb.error}")
        else: print(f"  [rw_split] rebalance moved {rb.shards_moved} shard-slot(s)")
        time.sleep(2)  # give workers a moment to finish loading replicas
    return n_rows


def run_scenario(label, table, write_qps_target):
    stop = threading.Event()
    read_lat = []
    read_err = 0
    write_count = [0]
    write_err = [0]

    rng = np.random.default_rng(7)
    test_queries = [rng.standard_normal(DIM).astype(np.float32) for _ in range(50)]

    def reader(_):
        local_stub = make_stub()
        local_lat = []
        errs = 0
        i = 0
        while not stop.is_set():
            try:
                t0 = time.perf_counter()
                local_stub.AnnSearch(pb.AnnSearchRequest(
                    table_name=table, vector_column="vector",
                    query_vector=ev(test_queries[i % len(test_queries)]),
                    dimension=DIM, k=10, nprobes=10, metric_type=0))
                local_lat.append(time.perf_counter() - t0)
            except Exception:
                errs += 1
            i += 1
        return local_lat, errs

    def writer():
        local_stub = make_stub()
        next_id = N_BASE + 100_000  # avoid colliding with init data
        interval = 1.0 / max(1, write_qps_target)
        rng2 = np.random.default_rng(99)
        while not stop.is_set():
            t0 = time.perf_counter()
            try:
                rows = 100
                v = rng2.standard_normal((rows, DIM)).astype(np.float32)
                batch = pa.record_batch([
                    pa.array(range(next_id, next_id + rows), type=pa.int64()),
                    pa.FixedSizeListArray.from_arrays(pa.array(v.flatten()), list_size=DIM),
                ], names=['id', 'vector'])
                r = local_stub.AddRows(pb.AddRowsRequest(
                    table_name=table, arrow_ipc_data=ipc_bytes(batch)))
                if r.error: write_err[0] += 1
                else: write_count[0] += 1
                next_id += rows
            except Exception:
                write_err[0] += 1
            elapsed = time.perf_counter() - t0
            sleep_for = max(0, interval - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)

    print(f"\n=== {label} (write_qps_target={write_qps_target}) ===")
    print(f"Running {DURATION_S}s with {READER_THREADS} readers + writer thread")

    writer_thread = threading.Thread(target=writer) if write_qps_target > 0 else None
    if writer_thread: writer_thread.start()

    futs = []
    with ThreadPoolExecutor(max_workers=READER_THREADS) as ex:
        for i in range(READER_THREADS):
            futs.append(ex.submit(reader, i))
        time.sleep(DURATION_S)
        stop.set()
        for f in futs:
            lat, err = f.result()
            read_lat.extend(lat); read_err += err

    if writer_thread: writer_thread.join(timeout=5)

    lat_ms = sorted([l * 1000 for l in read_lat])
    def pct(p):
        if not lat_ms: return 0
        return lat_ms[min(len(lat_ms) - 1, int(len(lat_ms) * p / 100))]

    qps = len(lat_ms) / DURATION_S
    metrics = {
        'label': label,
        'write_qps_target': write_qps_target,
        'read_qps': qps,
        'read_p50_ms': pct(50),
        'read_p95_ms': pct(95),
        'read_p99_ms': pct(99),
        'read_errors': read_err,
        'write_completed': write_count[0],
        'write_errors': write_err[0],
    }
    print(f"  Read QPS={qps:.1f}  P50={metrics['read_p50_ms']:.1f}ms  "
          f"P99={metrics['read_p99_ms']:.1f}ms  errors={read_err}")
    print(f"  Write completed={write_count[0]}  errors={write_err[0]}")
    return metrics


def main():
    procs = start_cluster()
    try:
        stub = make_stub()
        print(f"[setup] CreateTable + IVF_FLAT-32 on {N_BASE} rows")
        setup_table(stub, "mixwl", N_BASE)
        time.sleep(2)

        results = []
        results.append(run_scenario("read-only",  "mixwl", 0))
        results.append(run_scenario("read+10qps_writes", "mixwl", 10))
        results.append(run_scenario("read+50qps_writes", "mixwl", 50))

        baseline = results[0]['read_qps']
        print(f"\n--- Read QPS degradation vs read-only ({baseline:.1f}) ---")
        for r in results[1:]:
            ratio = r['read_qps'] / baseline if baseline > 0 else 0
            print(f"  {r['label']}: {r['read_qps']:.1f} QPS ({ratio*100-100:+.1f}%)")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(os.path.join(OUTDIR, f"mixed_{ts}.json"), 'w') as f:
            json.dump({'timestamp': ts, 'baseline_qps': baseline, 'scenarios': results}, f, indent=2)
        print(f"\nResults saved.")
    finally:
        for p in procs:
            try: p.terminate()
            except: pass
        kill()


if __name__ == '__main__':
    main()
