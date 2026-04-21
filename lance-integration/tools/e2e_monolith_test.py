#!/usr/bin/env python3
"""
Phase B (v2 architecture) E2E: lance-monolith binary composes the
QN+CP and PE+IDX roles inside one tokio runtime on loopback.

Covers:
 A. Single binary boots, coord port and worker port both reachable.
 B. CreateTable → AddRows → AnnSearch works end-to-end (proves the
    in-process coord→worker loopback path is correct).
 C. CountRows agrees with inserted row count.
 D. Clean shutdown on SIGTERM within the H25 seatbelt window.
"""
import os
import shutil
import signal
import struct
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import wait_for_grpc

import grpc
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import yaml

import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

DIM = 16
BIN = os.environ.get(
    "LANCEFORGE_BIN",
    os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"),
)
# Dedicated port range for this suite — matches the run_all_e2e.sh
# comment that every suite owns its own ports to avoid cross-run
# kills.
BASE = "/tmp/lanceforge_monolith_test"
COORD_PORT = 54100
# REST/metrics bind on coord_port+1 (54101), so worker port must skip
# at least 2. The monolith CLI enforces this but we still pick a safe
# gap here to model the recommended operator config.
WORKER_PORT = 54110
WORKER_ID = "mono-w0"
process = None
results = {"passed": 0, "failed": 0}


def cleanup():
    os.system(
        f"pkill -9 -f 'lance-monolith.*{COORD_PORT}' 2>/dev/null; true"
    )
    time.sleep(1)
    # Prior test runs can leave the coord/REST/worker ports in
    # TIME_WAIT for the kernel's default 60s, which makes the bind
    # fail with AddrInUse. Poll until the kernel releases them
    # before we start the monolith.
    for p in (COORD_PORT, COORD_PORT + 1, WORKER_PORT):
        _wait_port_free(p, timeout=90)


def _wait_port_free(port, timeout=60):
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("0.0.0.0", port))
            s.close()
            return True
        except OSError:
            s.close()
            time.sleep(1)
    return False


def step(m):
    print(f"\n{'='*60}\n  {m}\n{'='*60}")


def assert_eq(label, got, want):
    if got == want:
        print(f"  PASS {label}: {got}")
        results["passed"] += 1
    else:
        print(f"  FAIL {label}: got={got} want={want}")
        results["failed"] += 1


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema)
    w.write_batch(batch)
    w.close()
    return sink.getvalue().to_pybytes()


# ── Setup ──
step("Setup: write monolith config + start single binary")
cleanup()
if os.path.exists(BASE):
    shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

config = {
    "tables": [],
    "executors": [
        {"id": WORKER_ID, "host": "127.0.0.1", "port": WORKER_PORT},
    ],
    "default_table_path": BASE,
}
cfg_path = os.path.join(BASE, "config.yaml")
with open(cfg_path, "w") as f:
    yaml.dump(config, f)

process = subprocess.Popen(
    [
        f"{BIN}/lance-monolith",
        cfg_path,
        "--coord-port", str(COORD_PORT),
        "--worker-port", str(WORKER_PORT),
        "--worker-id", WORKER_ID,
    ],
    stdout=open(f"{BASE}/monolith.log", "w"),
    stderr=subprocess.STDOUT,
)

# Both ports must come up.
assert wait_for_grpc("127.0.0.1", WORKER_PORT, timeout=20), "worker port never accepted"
assert wait_for_grpc("127.0.0.1", COORD_PORT, timeout=20), "coord port never accepted"

# ── A. Single binary boots ──
step("A. Single binary exposes coord + worker on loopback")
chan = grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}")
stub = pbg.LanceSchedulerServiceStub(chan)

# ── B. CreateTable + AddRows + Search ──
step("B. CreateTable + AddRows + AnnSearch round-trip")
ids = pa.array(list(range(200)), pa.int32())
vecs = np.random.rand(200, DIM).astype(np.float32)
vec_array = pa.FixedSizeListArray.from_arrays(
    pa.array(vecs.flatten().tolist(), pa.float32()), DIM
)
batch = pa.record_batch([ids, vec_array], names=["id", "embedding"])
ipc_data = ipc_bytes(batch)

# In monolith, the coord half starts connecting before the worker
# half is ready, so the first health-check cycle marks the worker
# unhealthy. The loop recovers within ~interval_secs (default 10s).
# Retry CreateTable until coord sees a healthy worker.
deadline = time.time() + 30
last_err = None
for _ in range(30):
    try:
        resp = stub.CreateTable(
            pb.CreateTableRequest(
                table_name="t",
                arrow_ipc_data=ipc_data,
                index_column="embedding",
            ),
            timeout=10,
        )
        if resp.error == "":
            break
        last_err = resp.error
    except grpc.RpcError as e:
        last_err = e.details() if hasattr(e, "details") else str(e)
        if "No healthy workers" not in last_err and "UNAVAILABLE" not in last_err:
            raise
    if time.time() > deadline:
        break
    time.sleep(1)
else:
    last_err = last_err or "timed out"
assert_eq("CreateTable.error", resp.error, "")

# Search for one row's nearest neighbors.
query_vec = vecs[0].tobytes()
search = stub.AnnSearch(
    pb.AnnSearchRequest(
        table_name="t",
        vector_column="embedding",
        query_vector=query_vec,
        dimension=DIM,
        k=5,
    ),
    timeout=10,
)
assert_eq("AnnSearch.error", search.error, "")
assert search.num_rows > 0, f"AnnSearch returned 0 rows"
print(f"  PASS AnnSearch returned {search.num_rows} row(s)")
results["passed"] += 1

# ── C. CountRows matches ──
step("C. CountRows matches inserted cardinality")
cnt = stub.CountRows(pb.CountRowsRequest(table_name="t"), timeout=10)
assert_eq("CountRows.count", cnt.count, 200)

# ── D. Clean shutdown ──
step("D. SIGTERM drains in-flight and exits within budget")
t0 = time.time()
process.send_signal(signal.SIGTERM)
try:
    process.wait(timeout=70)  # H25 seatbelt is 2*query_timeout + 5s; default 65s
    exit_elapsed = time.time() - t0
    print(f"  PASS monolith exited cleanly in {exit_elapsed:.1f}s")
    results["passed"] += 1
except subprocess.TimeoutExpired:
    print("  FAIL monolith did not exit within 70s of SIGTERM")
    results["failed"] += 1
    process.kill()

# ── Summary ──
total = results["passed"] + results["failed"]
print(f"\n{'='*60}\n  Monolith E2E: {results['passed']}/{total} passed\n{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
