#!/usr/bin/env python3
"""
v2 #5.5 E2E: global commit_seq + min_commit_seq read guard.

Flow:
  1. CreateTable
  2. AddRows → WriteResponse.commit_seq = N1
  3. AnnSearch with min_commit_seq=N1 succeeds
  4. AnnSearch with min_commit_seq=N1+999 → FailedPrecondition
  5. Second AddRows → commit_seq > N1 (monotonic)
"""
import os
import shutil
import signal
import socket
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

BIN = os.environ.get(
    "LANCEFORGE_BIN",
    os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"),
)
BASE = "/tmp/lanceforge_commit_seq"
COORD_PORT = 58300
WORKER_PORT = 58310
WORKER_ID = "w0"
DIM = 8

results = {"passed": 0, "failed": 0}


def step(m):
    print(f"\n{'='*60}\n  {m}\n{'='*60}")


def assert_eq(label, got, want):
    if got == want:
        print(f"  PASS {label}: {got}")
        results["passed"] += 1
    else:
        print(f"  FAIL {label}: got={got!r} want={want!r}")
        results["failed"] += 1


def assert_true(label, cond, detail=""):
    if cond:
        print(f"  PASS {label}{(': ' + detail) if detail else ''}")
        results["passed"] += 1
    else:
        print(f"  FAIL {label}: {detail}")
        results["failed"] += 1


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema)
    w.write_batch(batch)
    w.close()
    return sink.getvalue().to_pybytes()


def wait_port_free(port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("0.0.0.0", port))
            s.close()
            return
        except OSError:
            s.close()
            time.sleep(0.5)


def cleanup():
    os.system(
        f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|lance-worker.*{WORKER_PORT}' 2>/dev/null; true"
    )
    time.sleep(1)
    for p in (COORD_PORT, COORD_PORT + 1, WORKER_PORT):
        wait_port_free(p)


step("Setup")
cleanup()
if os.path.exists(BASE):
    shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

cfg = {
    "tables": [],
    "executors": [{"id": WORKER_ID, "host": "127.0.0.1", "port": WORKER_PORT}],
    "default_table_path": BASE,
    "metadata_path": os.path.join(BASE, "meta.json"),
}
cfg_path = os.path.join(BASE, "config.yaml")
with open(cfg_path, "w") as f:
    yaml.dump(cfg, f)

w = subprocess.Popen(
    [f"{BIN}/lance-worker", cfg_path, WORKER_ID, str(WORKER_PORT)],
    stdout=open(f"{BASE}/w.log", "w"),
    stderr=subprocess.STDOUT,
)
assert wait_for_grpc("127.0.0.1", WORKER_PORT, timeout=15), "worker failed"
c = subprocess.Popen(
    [f"{BIN}/lance-coordinator", cfg_path, str(COORD_PORT)],
    stdout=open(f"{BASE}/c.log", "w"),
    stderr=subprocess.STDOUT,
)
assert wait_for_grpc("127.0.0.1", COORD_PORT, timeout=15), "coord failed"
time.sleep(1)

try:
    chan = grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}")
    stub = pbg.LanceSchedulerServiceStub(chan)

    step("A. CreateTable")
    ids = pa.array([1, 2, 3], pa.int32())
    vecs = np.random.rand(3, DIM).astype(np.float32)
    vec_fsl = pa.FixedSizeListArray.from_arrays(
        pa.array(vecs.flatten().tolist(), pa.float32()), DIM
    )
    batch = pa.record_batch([ids, vec_fsl], names=["id", "vector"])
    deadline = time.time() + 30
    resp = None
    while time.time() < deadline:
        try:
            resp = stub.CreateTable(
                pb.CreateTableRequest(
                    table_name="t",
                    arrow_ipc_data=ipc_bytes(batch),
                    index_column="vector",
                ),
                timeout=10,
            )
            if resp.error == "":
                break
        except grpc.RpcError as e:
            if "No healthy" not in (e.details() or ""):
                raise
        time.sleep(1)
    assert_eq("CreateTable.error", resp.error, "")

    step("B. AddRows returns commit_seq")
    more_ids = pa.array([4, 5], pa.int32())
    more_vecs = np.random.rand(2, DIM).astype(np.float32)
    more_fsl = pa.FixedSizeListArray.from_arrays(
        pa.array(more_vecs.flatten().tolist(), pa.float32()), DIM
    )
    more_batch = pa.record_batch([more_ids, more_fsl], names=["id", "vector"])
    wr = stub.AddRows(
        pb.AddRowsRequest(table_name="t", arrow_ipc_data=ipc_bytes(more_batch)),
        timeout=10,
    )
    assert_eq("AddRows.error", wr.error, "")
    assert_true("AddRows commit_seq > 0", wr.commit_seq > 0, f"commit_seq={wr.commit_seq}")
    seq_after_addrows = wr.commit_seq

    step("C. AnnSearch with min_commit_seq <= current succeeds")
    query_vec = vecs[0].tobytes()
    r = stub.AnnSearch(
        pb.AnnSearchRequest(
            table_name="t",
            vector_column="vector",
            query_vector=query_vec,
            dimension=DIM,
            k=3,
            min_commit_seq=seq_after_addrows,
        ),
        timeout=10,
    )
    assert_eq("AnnSearch min=N.error", r.error, "")

    step("D. AnnSearch with min_commit_seq > current → FailedPrecondition")
    try:
        stub.AnnSearch(
            pb.AnnSearchRequest(
                table_name="t",
                vector_column="vector",
                query_vector=query_vec,
                dimension=DIM,
                k=3,
                min_commit_seq=seq_after_addrows + 999,
            ),
            timeout=10,
        )
        print("  FAIL: expected FailedPrecondition")
        results["failed"] += 1
    except grpc.RpcError as e:
        assert_eq("min>current code", e.code(), grpc.StatusCode.FAILED_PRECONDITION)

    step("E. Second AddRows advances commit_seq monotonically")
    wr2 = stub.AddRows(
        pb.AddRowsRequest(table_name="t", arrow_ipc_data=ipc_bytes(more_batch)),
        timeout=10,
    )
    assert_true(
        "second AddRows seq > first",
        wr2.commit_seq > seq_after_addrows,
        f"first={seq_after_addrows} second={wr2.commit_seq}",
    )

    step("F. CountRows with stale min_commit_seq fails; current ok")
    try:
        stub.CountRows(
            pb.CountRowsRequest(table_name="t", min_commit_seq=wr2.commit_seq + 100),
            timeout=10,
        )
        print("  FAIL: expected FailedPrecondition")
        results["failed"] += 1
    except grpc.RpcError as e:
        assert_eq("CountRows stale code", e.code(), grpc.StatusCode.FAILED_PRECONDITION)

    cr = stub.CountRows(
        pb.CountRowsRequest(table_name="t", min_commit_seq=wr2.commit_seq),
        timeout=10,
    )
    assert_true("CountRows current ok", cr.count >= 3, f"count={cr.count}")

finally:
    c.send_signal(signal.SIGTERM)
    w.send_signal(signal.SIGTERM)
    for p in (c, w):
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()

total = results["passed"] + results["failed"]
print(f"\n{'='*60}\n  commit_seq E2E: {results['passed']}/{total} passed\n{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
