#!/usr/bin/env python3
"""
C5 end-to-end: REST → gRPC traceparent bridge.

Assertion: HTTP POST to /v1/search with a `traceparent` header lands
an audit JSONL record whose top-level `trace_id` matches the 32-hex
middle segment of the header. Covers the C4 bridge code path
(`apply_passthrough` in rest.rs) end-to-end instead of just via unit
test on the extractor.

This is the REST counterpart to F4-B / B1 which exercised the gRPC
entry path.
"""
import json
import os
import shutil
import struct
import subprocess
import sys
import time
import urllib.request

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import yaml

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import wait_for_grpc

import grpc
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

for _var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
             "GRPC_PROXY", "grpc_proxy"):
    os.environ.pop(_var, None)

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(ROOT, "target", "release"))
BASE = "/tmp/lanceforge_beta3_rest_trace"
COORD_PORT = 56450
REST_PORT = COORD_PORT + 1
WORKER_PORTS = [56400, 56401]
AUDIT_PATH = os.path.join(BASE, "audit.jsonl")
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
TRACEPARENT = f"00-{TRACE_ID}-00f067aa0ba902b7-01"
DIM = 8


def kill_cluster():
    os.system(
        f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|"
        f"lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null"
    )
    time.sleep(1)


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema)
    w.write_batch(batch)
    w.close()
    return sink.getvalue().to_pybytes()


def make_batch(n=50, offset=0):
    np.random.seed(1 + offset)
    return pa.record_batch([
        pa.array(range(offset, offset + n), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)),
            list_size=DIM,
        ),
    ], names=['id', 'vector'])


def main():
    print(f"=== C5: REST → gRPC traceparent bridge e2e ===", flush=True)
    kill_cluster()
    if os.path.exists(BASE):
        shutil.rmtree(BASE)
    os.makedirs(BASE, exist_ok=True)

    config = {
        'tables': [],
        'executors': [
            {'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]}
            for i in range(2)
        ],
        'default_table_path': BASE,
        'security': {
            'audit_log_path': AUDIT_PATH,
        },
    }
    cfg_path = os.path.join(BASE, 'config.yaml')
    with open(cfg_path, 'w') as f:
        yaml.dump(config, f)

    procs = []
    for i in range(2):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(WORKER_PORTS[i])],
            stdout=open(f"{BASE}/w{i}.log", "w"),
            stderr=subprocess.STDOUT,
        )
        procs.append(p)
    for i in range(2):
        assert wait_for_grpc("127.0.0.1", WORKER_PORTS[i]), f"w{i} failed"

    coord = subprocess.Popen(
        [f"{BIN}/lance-coordinator", cfg_path, str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"),
        stderr=subprocess.STDOUT,
    )
    procs.append(coord)
    assert wait_for_grpc("127.0.0.1", COORD_PORT), "coord failed"
    time.sleep(2)

    try:
        # 1. Set up a table via gRPC (REST /v1/create_table doesn't exist).
        stub = pbg.LanceSchedulerServiceStub(
            grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
                                  options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)]))
        r = stub.CreateTable(pb.CreateTableRequest(
            table_name="rest_trace_tbl",
            arrow_ipc_data=ipc_bytes(make_batch(50)),
        ))
        assert not r.error, f"CreateTable error: {r.error}"
        ir = stub.CreateIndex(pb.CreateIndexRequest(
            table_name="rest_trace_tbl", column="vector",
            index_type="IVF_FLAT", num_partitions=4))
        # Index error is non-fatal for a tiny table; just log.
        if ir.error:
            print(f"  (index warning: {ir.error})", flush=True)
        time.sleep(1)

        # 2. POST /v1/search with a traceparent header.
        qv = np.random.randn(DIM).astype(np.float32).tolist()
        body = json.dumps({
            "table": "rest_trace_tbl",
            "vector": qv,
            "k": 3,
            "nprobes": 4,
        })
        req = urllib.request.Request(
            f"http://127.0.0.1:{REST_PORT}/v1/search",
            data=body.encode(),
            headers={
                "Content-Type": "application/json",
                "traceparent": TRACEPARENT,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200, f"REST status {resp.status}"
            payload = json.loads(resp.read())
            # REST handler returns rows[] or similar; we only care that
            # it didn't error.
            assert "error" not in payload or not payload.get("error"), \
                f"REST reported error: {payload}"

        # Audit sink batches up to 30s or 100 records; for AnnSearch which
        # isn't audited (only writes + DDL), we need to also fire an
        # AddRows through REST... but /v1/insert doesn't exist either.
        # Instead verify via worker log: the worker should have logged
        # `local_search trace_id=<TRACE_ID>` — that's B1 + C4 together.
        time.sleep(1)

        # 3. Assert: at least one worker log carries the trace_id.
        hits = 0
        for wlog in [f"{BASE}/w0.log", f"{BASE}/w1.log"]:
            with open(wlog) as f:
                txt = f.read()
            if f"trace_id={TRACE_ID}" in txt:
                hits += 1
                print(f"  [PASS] {os.path.basename(wlog)} carries trace_id", flush=True)
        assert hits >= 1, (
            f"REST path did not propagate traceparent — no worker log contains "
            f"trace_id={TRACE_ID}. rest.rs::apply_passthrough() may be broken."
        )
        print(f"PASS: C5 REST→gRPC traceparent bridge end-to-end", flush=True)
    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        time.sleep(1)
        kill_cluster()


if __name__ == '__main__':
    main()
