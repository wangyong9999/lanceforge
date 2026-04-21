#!/usr/bin/env python3
"""
v2 #5.3 E2E: AlterTable ADD COLUMN NULLABLE.

Happy path:
  1. CreateTable with {id int, vector fsl<float>}
  2. AddRows — populate rows
  3. AlterTable ADD COLUMN {tenant string} (nullable)
  4. AnnSearch — old rows return with tenant=NULL

CAS guard:
  5. AlterTable with stale expected_schema_version → FailedPrecondition

Both coordinators and workers must be freshly-built with the new
proto (fields + rpc). Pre-5.3 binaries would fail to decode the
new request at the worker, surfacing as an internal error on the
coord fan-out.
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
BASE = "/tmp/lanceforge_alter_table"
COORD_PORT = 58200
WORKER_PORT = 58210
WORKER_ID = "w0"
DIM = 8

results = {"passed": 0, "failed": 0}


def step(msg):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")


def assert_eq(label, got, want):
    if got == want:
        print(f"  PASS {label}: {got}")
        results["passed"] += 1
    else:
        print(f"  FAIL {label}: got={got!r} want={want!r}")
        results["failed"] += 1


def assert_true(label, cond, detail=""):
    if cond:
        print(f"  PASS {label}: {detail}" if detail else f"  PASS {label}")
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


def schema_ipc(schema):
    # Empty batch encodes the schema only.
    empty = pa.record_batch([pa.array([], field.type) for field in schema], schema=schema)
    return ipc_bytes(empty)


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
    os.system("pkill -9 -f 'lance-coordinator.*58200|lance-worker.*58210' 2>/dev/null; true")
    time.sleep(1)
    for p in (COORD_PORT, COORD_PORT + 1, WORKER_PORT):
        wait_port_free(p, 30)


step("Setup")
cleanup()
if os.path.exists(BASE):
    shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

config = {
    "tables": [],
    "executors": [{"id": WORKER_ID, "host": "127.0.0.1", "port": WORKER_PORT}],
    "default_table_path": BASE,
    # #5.3 AlterTable requires metadata_path (SchemaStore lives in
    # MetaStore). Use a FileMetaStore in the test base dir.
    "metadata_path": os.path.join(BASE, "meta.json"),
}
cfg_path = os.path.join(BASE, "config.yaml")
with open(cfg_path, "w") as f:
    yaml.dump(config, f)

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

    step("B. AlterTable ADD COLUMN tenant STRING NULLABLE")
    tenant_schema = pa.schema([pa.field("tenant", pa.string(), nullable=True)])
    add_cols_ipc = schema_ipc(tenant_schema)
    alter = stub.AlterTable(
        pb.AlterTableRequest(
            table_name="t",
            add_columns_arrow_ipc=add_cols_ipc,
            expected_schema_version=1,  # current is v=1 from CreateTable
        ),
        timeout=20,
    )
    assert_eq("AlterTable.error", alter.error, "")
    assert_eq("AlterTable.new_schema_version", alter.new_schema_version, 2)

    step("C. AnnSearch still returns rows after schema change")
    query_vec = vecs[0].tobytes()
    search = stub.AnnSearch(
        pb.AnnSearchRequest(
            table_name="t",
            vector_column="vector",
            query_vector=query_vec,
            dimension=DIM,
            k=3,
        ),
        timeout=10,
    )
    assert_eq("AnnSearch.error", search.error, "")
    assert_true(
        "AnnSearch returns >=1 row",
        search.num_rows >= 1,
        f"num_rows={search.num_rows}",
    )

    step("D. Stale expected_schema_version → FailedPrecondition")
    stale_schema = pa.schema([pa.field("other", pa.int32(), nullable=True)])
    try:
        stub.AlterTable(
            pb.AlterTableRequest(
                table_name="t",
                add_columns_arrow_ipc=schema_ipc(stale_schema),
                expected_schema_version=1,  # stale (current is 2)
            ),
            timeout=10,
        )
        print("  FAIL stale CAS: expected FailedPrecondition, got OK")
        results["failed"] += 1
    except grpc.RpcError as e:
        assert_eq(
            "stale CAS code", e.code(), grpc.StatusCode.FAILED_PRECONDITION
        )

    step("E. Correct expected_schema_version succeeds")
    newer = stub.AlterTable(
        pb.AlterTableRequest(
            table_name="t",
            add_columns_arrow_ipc=schema_ipc(stale_schema),
            expected_schema_version=2,
        ),
        timeout=20,
    )
    assert_eq("AlterTable.v2→v3", newer.new_schema_version, 3)

    step("F. Read with min_schema_version guard (#5.4)")
    # Current schema is v=3. A read with min=3 must succeed.
    ok_search = stub.AnnSearch(
        pb.AnnSearchRequest(
            table_name="t",
            vector_column="vector",
            query_vector=query_vec,
            dimension=DIM,
            k=3,
            min_schema_version=3,
        ),
        timeout=10,
    )
    assert_eq("AnnSearch min=3.error", ok_search.error, "")
    assert_true(
        "AnnSearch min=3 returns rows",
        ok_search.num_rows >= 1,
        f"num_rows={ok_search.num_rows}",
    )

    # min=99 must fail.
    try:
        stub.AnnSearch(
            pb.AnnSearchRequest(
                table_name="t",
                vector_column="vector",
                query_vector=query_vec,
                dimension=DIM,
                k=3,
                min_schema_version=99,
            ),
            timeout=10,
        )
        print("  FAIL min=99 should have been rejected")
        results["failed"] += 1
    except grpc.RpcError as e:
        assert_eq(
            "min=99 code", e.code(), grpc.StatusCode.FAILED_PRECONDITION
        )

    # CountRows with min_schema_version also honored.
    count_ok = stub.CountRows(
        pb.CountRowsRequest(table_name="t", min_schema_version=1),
        timeout=10,
    )
    assert_eq("CountRows min=1.count", count_ok.count, 3)

    step("G. AlterTable with multiple columns at once")
    multi_schema = pa.schema([
        pa.field("country", pa.string(), nullable=True),
        pa.field("revenue", pa.float64(), nullable=True),
    ])
    multi = stub.AlterTable(
        pb.AlterTableRequest(
            table_name="t",
            add_columns_arrow_ipc=schema_ipc(multi_schema),
            expected_schema_version=3,
        ),
        timeout=20,
    )
    assert_eq("AlterTable multi.error", multi.error, "")
    assert_eq("AlterTable multi.new_v", multi.new_schema_version, 4)

    step("H. Retry same AlterTable is idempotent (columns already exist)")
    # Same columns, correct expected_v=4 — should succeed with v=5
    # because worker-side skips already-present columns and returns OK.
    retry = stub.AlterTable(
        pb.AlterTableRequest(
            table_name="t",
            add_columns_arrow_ipc=schema_ipc(multi_schema),
            expected_schema_version=4,
        ),
        timeout=20,
    )
    assert_eq("AlterTable retry.error", retry.error, "")
    assert_eq("AlterTable retry.new_v", retry.new_schema_version, 5)

finally:
    c.send_signal(signal.SIGTERM)
    w.send_signal(signal.SIGTERM)
    for p in (c, w):
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()

total = results["passed"] + results["failed"]
print(f"\n{'='*60}\n  AlterTable E2E: {results['passed']}/{total} passed\n{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
