#!/usr/bin/env python3
"""
D6: `lance_audit_dropped_total` counter end-to-end.

The counter-increment path is unit-tested in audit.rs; this test
closes the gap between "unit test says it increments" and "a
Prometheus scrape actually sees it." We configure the audit sink
against an unwritable path so `tokio::fs::OpenOptions::open` fails,
every submitted record drops, counter goes up, and `/metrics` shows
the non-zero value.

Production alert rule (deploy/grafana/README.md) fires on
`increase(lance_audit_dropped_total[5m]) > 0`. This test is the
control that verifies the metric name + value actually reach the
scrape endpoint under sink failure.
"""
import os
import shutil
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
BASE = "/tmp/lanceforge_audit_dropped"
COORD_PORT = 56650
REST_PORT = COORD_PORT + 1
WORKER_PORTS = [56600, 56601]
# `/proc` is read-only; tokio::fs::create_dir_all + OpenOptions::append
# both fail → sink goes into drain-and-count mode. That's the specific
# failure branch we want to exercise.
UNWRITABLE_AUDIT = "/proc/lanceforge-audit.jsonl"
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


def make_batch(n=20, offset=0):
    np.random.seed(9 + offset)
    return pa.record_batch([
        pa.array(range(offset, offset + n), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)),
            list_size=DIM,
        ),
    ], names=['id', 'vector'])


def metric_value(name):
    """Scrape /metrics and return the integer value for `name` or 0."""
    with urllib.request.urlopen(
        f"http://127.0.0.1:{REST_PORT}/metrics", timeout=3
    ) as r:
        for line in r.read().decode().splitlines():
            if line.startswith("#"):
                continue
            if line.startswith(name + " "):
                return int(float(line.split(maxsplit=1)[1]))
    return 0


def main():
    print("=== D6: audit_dropped_total counter end-to-end ===", flush=True)
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
            'audit_log_path': UNWRITABLE_AUDIT,
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
    time.sleep(1)

    try:
        # Sanity: counter starts at 0.
        before = metric_value("lance_audit_dropped_total")
        assert before == 0, f"counter nonzero at startup: {before}"
        print("  [setup] lance_audit_dropped_total = 0 on fresh coord", flush=True)

        # Fire some audit-worthy RPCs. CreateTable + AddRows both call
        # self.audit(...) → submit to sink → sink fails to open
        # /proc/..., enters drain-and-count loop, every submit bumps
        # the counter.
        stub = pbg.LanceSchedulerServiceStub(
            grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
                                  options=[("grpc.max_receive_message_length",
                                            64 * 1024 * 1024)]))
        r = stub.CreateTable(pb.CreateTableRequest(
            table_name="audit_drop_tbl",
            arrow_ipc_data=ipc_bytes(make_batch(20)),
        ))
        assert not r.error, f"CreateTable failed: {r.error}"

        # A few AddRows, each produces one audit record → one drop.
        for i in range(5):
            r = stub.AddRows(pb.AddRowsRequest(
                table_name="audit_drop_tbl",
                arrow_ipc_data=ipc_bytes(make_batch(20, 100 + i * 100)),
                on_columns=[],
            ))
            assert not r.error, f"AddRows failed: {r.error}"

        # Give the sink task a moment to drain the in-flight channel
        # (drops are counted as records leave the channel).
        time.sleep(1)

        after = metric_value("lance_audit_dropped_total")
        # We fired 1 CreateTable + 5 AddRows = 6 audit records. All
        # should have dropped.
        assert after >= 6, (
            f"expected ≥ 6 dropped records (1 CreateTable + 5 AddRows) but "
            f"/metrics reports lance_audit_dropped_total = {after}. "
            f"Counter not wired to the sink, or sink opened the path despite "
            f"/proc being read-only (check coord.log for 'audit sink:' lines)."
        )
        print(f"  [PASS] /metrics reports lance_audit_dropped_total = {after} "
              f"after 1 CreateTable + 5 AddRows", flush=True)

        print("\nALL AUDIT-DROPPED ASSERTIONS PASSED", flush=True)
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
