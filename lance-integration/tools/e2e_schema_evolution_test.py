#!/usr/bin/env python3
"""
R3 E2E: AlterTable full schema evolution — ADD, DROP, RENAME columns.

Covers the v0.3 realignment's Lance-native DDL pass-through:
  1. CreateTable with {id, vector}.
  2. AlterTable ADD {tenant string nullable} — schema_version bumps.
  3. AlterTable DROP [tenant] — schema_version bumps, column gone.
  4. AlterTable RENAME {id -> user_id} — column renamed.
  5. Combined ADD+DROP+RENAME in one request.
  6. Idempotent retry: DROP of already-absent column is a no-op.
  7. RENAME of non-existent source fails cleanly.
"""
import os
import shutil
import signal
import socket
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
BASE = "/tmp/lanceforge_schema_evolution"
COORD_PORT = 58400
WORKER_PORT = 58410
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


def schema_ipc(schema):
    empty = pa.record_batch([pa.array([], f.type) for f in schema], schema=schema)
    return ipc_bytes(empty)


def wait_port_free(port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("0.0.0.0", port)); s.close(); return
        except OSError:
            s.close(); time.sleep(0.5)


def cleanup():
    os.system(f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|lance-worker.*{WORKER_PORT}' 2>/dev/null; true")
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
    stdout=open(f"{BASE}/w.log", "w"), stderr=subprocess.STDOUT,
)
assert wait_for_grpc("127.0.0.1", WORKER_PORT, timeout=15), "worker failed"
c = subprocess.Popen(
    [f"{BIN}/lance-coordinator", cfg_path, str(COORD_PORT)],
    stdout=open(f"{BASE}/c.log", "w"), stderr=subprocess.STDOUT,
)
assert wait_for_grpc("127.0.0.1", COORD_PORT, timeout=15), "coord failed"
time.sleep(1)

try:
    chan = grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}")
    stub = pbg.LanceSchedulerServiceStub(chan)

    step("A. CreateTable {id int, vector fsl<f32,8>}")
    ids = pa.array([1, 2, 3], pa.int32())
    vecs = np.random.rand(3, DIM).astype(np.float32)
    vec_fsl = pa.FixedSizeListArray.from_arrays(
        pa.array(vecs.flatten().tolist(), pa.float32()), DIM)
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
                ), timeout=10)
            if resp.error == "": break
        except grpc.RpcError as e:
            if "No healthy" not in (e.details() or ""): raise
        time.sleep(1)
    assert_eq("CreateTable.error", resp.error, "")

    step("B. AlterTable ADD {tenant}")
    tenant_schema = pa.schema([pa.field("tenant", pa.string(), nullable=True)])
    r = stub.AlterTable(pb.AlterTableRequest(
        table_name="t",
        add_columns_arrow_ipc=schema_ipc(tenant_schema),
        expected_schema_version=1,
    ), timeout=20)
    assert_eq("ADD.error", r.error, "")
    assert_eq("ADD.new_v", r.new_schema_version, 2)

    step("C. AlterTable DROP [tenant]")
    r = stub.AlterTable(pb.AlterTableRequest(
        table_name="t",
        drop_columns=["tenant"],
        expected_schema_version=2,
    ), timeout=20)
    assert_eq("DROP.error", r.error, "")
    assert_eq("DROP.new_v", r.new_schema_version, 3)

    step("D. GetSchema: tenant is gone")
    sch = stub.GetSchema(pb.GetSchemaRequest(table_name="t"), timeout=10)
    cols = [c.name for c in sch.columns]
    assert_true("schema lacks 'tenant'", "tenant" not in cols,
                f"cols={cols}")

    step("E. AlterTable RENAME id → user_id")
    r = stub.AlterTable(pb.AlterTableRequest(
        table_name="t",
        rename_columns={"id": "user_id"},
        expected_schema_version=3,
    ), timeout=20)
    assert_eq("RENAME.error", r.error, "")
    assert_eq("RENAME.new_v", r.new_schema_version, 4)

    step("F. GetSchema: id is user_id now")
    sch = stub.GetSchema(pb.GetSchemaRequest(table_name="t"), timeout=10)
    cols = [c.name for c in sch.columns]
    assert_true("schema has 'user_id'", "user_id" in cols, f"cols={cols}")
    assert_true("schema lacks 'id'", "id" not in cols, f"cols={cols}")

    step("G. Combined ADD {country} + DROP [vector] + RENAME {user_id → uid}")
    country_schema = pa.schema([pa.field("country", pa.string(), nullable=True)])
    r = stub.AlterTable(pb.AlterTableRequest(
        table_name="t",
        add_columns_arrow_ipc=schema_ipc(country_schema),
        drop_columns=["vector"],
        rename_columns={"user_id": "uid"},
        expected_schema_version=4,
    ), timeout=20)
    assert_eq("combined.error", r.error, "")
    assert_eq("combined.new_v", r.new_schema_version, 5)
    sch = stub.GetSchema(pb.GetSchemaRequest(table_name="t"), timeout=10)
    cols = [c.name for c in sch.columns]
    assert_true("combined result: has country", "country" in cols, f"cols={cols}")
    assert_true("combined result: has uid", "uid" in cols, f"cols={cols}")
    assert_true("combined result: no vector", "vector" not in cols, f"cols={cols}")

    step("H. DROP of already-absent column is a no-op")
    r = stub.AlterTable(pb.AlterTableRequest(
        table_name="t",
        drop_columns=["nonexistent_col"],
        expected_schema_version=5,
    ), timeout=20)
    assert_eq("no-op DROP.error", r.error, "")
    assert_eq("no-op DROP.new_v", r.new_schema_version, 6)

    step("I. RENAME of non-existent source fails cleanly")
    try:
        r = stub.AlterTable(pb.AlterTableRequest(
            table_name="t",
            rename_columns={"ghost": "phantom"},
            expected_schema_version=6,
        ), timeout=20)
        # Expected: r.error non-empty OR RpcError. If it came back OK, fail.
        if r.error == "":
            print("  FAIL: RENAME of non-existent source succeeded unexpectedly")
            results["failed"] += 1
        else:
            assert_true("RENAME non-existent surfaces error", True, r.error)
    except grpc.RpcError as e:
        assert_true("RENAME non-existent surfaces error", True, e.details() or str(e.code()))

finally:
    c.send_signal(signal.SIGTERM); w.send_signal(signal.SIGTERM)
    for p in (c, w):
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired: p.kill()

total = results["passed"] + results["failed"]
print(f"\n{'='*60}\n  Schema Evolution E2E: {results['passed']}/{total} passed\n{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
