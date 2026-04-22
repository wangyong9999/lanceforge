#!/usr/bin/env python3
"""
R3 E2E: Tags + RestoreTable via Lance pass-through.

Covers:
  1. CreateTag at current version → returns tagged version.
  2. ListTags enumerates created tags.
  3. Second write advances table version; earlier tag still points at
     the old version.
  4. DeleteTag removes the tag from ListTags output.
  5. RestoreTable by version rolls back row set.
  6. RestoreTable by tag is equivalent to RestoreTable by the tagged
     version.
  7. RestoreTable with neither version nor tag returns a clean error.
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
BASE = "/tmp/lanceforge_tags"
COORD_PORT = 58500
WORKER_PORT = 58510
WORKER_ID = "w0"
DIM = 8

results = {"passed": 0, "failed": 0}


def step(m):
    print(f"\n{'='*60}\n  {m}\n{'='*60}")


def assert_eq(label, got, want):
    if got == want:
        print(f"  PASS {label}: {got}"); results["passed"] += 1
    else:
        print(f"  FAIL {label}: got={got!r} want={want!r}"); results["failed"] += 1


def assert_true(label, cond, detail=""):
    if cond:
        print(f"  PASS {label}{(': ' + detail) if detail else ''}"); results["passed"] += 1
    else:
        print(f"  FAIL {label}: {detail}"); results["failed"] += 1


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema); w.write_batch(batch); w.close()
    return sink.getvalue().to_pybytes()


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

    step("A. CreateTable with 3 rows")
    ids = pa.array([1, 2, 3], pa.int32())
    vecs = np.random.rand(3, DIM).astype(np.float32)
    vec_fsl = pa.FixedSizeListArray.from_arrays(
        pa.array(vecs.flatten().tolist(), pa.float32()), DIM)
    batch = pa.record_batch([ids, vec_fsl], names=["id", "vector"])
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            resp = stub.CreateTable(pb.CreateTableRequest(
                table_name="t",
                arrow_ipc_data=ipc_bytes(batch),
                index_column="vector",
            ), timeout=10)
            if resp.error == "": break
        except grpc.RpcError as e:
            if "No healthy" not in (e.details() or ""): raise
        time.sleep(1)
    assert_eq("CreateTable.error", resp.error, "")

    step("B. CreateTag 'v1' at current version")
    r = stub.CreateTag(pb.CreateTagRequest(
        table_name="t", tag_name="v1", version=0,
    ), timeout=10)
    assert_eq("CreateTag.error", r.error, "")
    assert_true("v1 tagged at non-zero version", r.tagged_version > 0,
                f"v={r.tagged_version}")
    v1_version = r.tagged_version

    step("C. ListTags enumerates 'v1'")
    r = stub.ListTags(pb.ListTagsRequest(table_name="t"), timeout=10)
    assert_eq("ListTags.error", r.error, "")
    names = [t.name for t in r.tags]
    assert_true("v1 in tag list", "v1" in names, f"names={names}")
    tagged = {t.name: t.version for t in r.tags}
    assert_eq("v1 version matches CreateTag", tagged.get("v1"), v1_version)

    step("D. AddRows to advance table version")
    more_ids = pa.array([4, 5], pa.int32())
    more_vecs = np.random.rand(2, DIM).astype(np.float32)
    more_fsl = pa.FixedSizeListArray.from_arrays(
        pa.array(more_vecs.flatten().tolist(), pa.float32()), DIM)
    more_batch = pa.record_batch([more_ids, more_fsl], names=["id", "vector"])
    wr = stub.AddRows(pb.AddRowsRequest(
        table_name="t", arrow_ipc_data=ipc_bytes(more_batch),
    ), timeout=10)
    assert_eq("AddRows.error", wr.error, "")
    cr = stub.CountRows(pb.CountRowsRequest(table_name="t"), timeout=10)
    assert_eq("rows after AddRows", cr.count, 5)

    step("E. CreateTag 'v2' at current (should be > v1)")
    r = stub.CreateTag(pb.CreateTagRequest(
        table_name="t", tag_name="v2", version=0,
    ), timeout=10)
    assert_eq("CreateTag v2.error", r.error, "")
    v2_version = r.tagged_version
    assert_true("v2 > v1", v2_version > v1_version,
                f"v1={v1_version} v2={v2_version}")

    step("F. DeleteTag 'v2' removes it from ListTags")
    r = stub.DeleteTag(pb.DeleteTagRequest(
        table_name="t", tag_name="v2",
    ), timeout=10)
    assert_eq("DeleteTag.error", r.error, "")
    r = stub.ListTags(pb.ListTagsRequest(table_name="t"), timeout=10)
    names = [t.name for t in r.tags]
    assert_true("v2 gone", "v2" not in names, f"names={names}")
    assert_true("v1 still present", "v1" in names, f"names={names}")

    step("G. RestoreTable by version (v1) rolls back to 3 rows")
    r = stub.RestoreTable(pb.RestoreTableRequest(
        table_name="t", version=v1_version,
    ), timeout=20)
    assert_eq("RestoreTable by version.error", r.error, "")
    assert_true("new_version advances",
                r.new_version > v1_version,
                f"restored new_v={r.new_version}")
    cr = stub.CountRows(pb.CountRowsRequest(table_name="t"), timeout=10)
    assert_eq("rows after restore to v1 (=3)", cr.count, 3)

    step("H. RestoreTable by tag after re-adding + re-tagging")
    # Re-add rows and tag a new marker, then restore-by-tag
    wr = stub.AddRows(pb.AddRowsRequest(
        table_name="t", arrow_ipc_data=ipc_bytes(more_batch),
    ), timeout=10)
    assert_eq("Re-AddRows.error", wr.error, "")
    cr = stub.CountRows(pb.CountRowsRequest(table_name="t"), timeout=10)
    assert_eq("rows after re-add (=5)", cr.count, 5)
    r = stub.CreateTag(pb.CreateTagRequest(
        table_name="t", tag_name="v_post_readd", version=0,
    ), timeout=10)
    assert_eq("tag v_post_readd.error", r.error, "")
    # Now restore to v1 by tag name
    r = stub.RestoreTable(pb.RestoreTableRequest(
        table_name="t", tag="v1",
    ), timeout=20)
    assert_eq("RestoreTable by tag.error", r.error, "")
    cr = stub.CountRows(pb.CountRowsRequest(table_name="t"), timeout=10)
    assert_eq("rows after restore by tag 'v1' (=3)", cr.count, 3)

    step("I. RestoreTable with neither version nor tag returns error")
    try:
        r = stub.RestoreTable(pb.RestoreTableRequest(
            table_name="t", version=0, tag="",
        ), timeout=10)
        if r.error == "":
            print("  FAIL: empty RestoreTable succeeded unexpectedly")
            results["failed"] += 1
        else:
            assert_true("empty restore surfaces error", True, r.error)
    except grpc.RpcError as e:
        assert_true("empty restore surfaces error", True,
                    e.details() or str(e.code()))

finally:
    c.send_signal(signal.SIGTERM); w.send_signal(signal.SIGTERM)
    for p in (c, w):
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired: p.kill()

total = results["passed"] + results["failed"]
print(f"\n{'='*60}\n  Tags + Restore E2E: {results['passed']}/{total} passed\n{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
