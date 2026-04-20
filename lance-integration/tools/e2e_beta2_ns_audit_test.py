#!/usr/bin/env python3
"""
F3 + F4 end-to-end: namespace binding + audit log, exercised through
the full gRPC stack.

Both features already had unit tests covering the logic in isolation,
but nothing verified that the coordinator actually calls into the
enforcement path for every RPC that takes a table name, and nothing
verified that real AddRows / CreateTable traffic lands a JSONL record
in the configured audit file. A future refactor that dropped the
`self.check_ns(...)` call site or the `self.audit(...)` call site
would have slipped through on 0.2.0-beta.1 regression — this closes
the gap.

Scenarios:
 F3-A  Admin-no-ns key → can CreateTable outside any namespace.
 F3-B  tenant-a key → can CreateTable under tenant-a/ prefix.
 F3-C  tenant-a key → CreateTable under tenant-b/ → PermissionDenied.
 F3-D  tenant-a key → AnnSearch on tenant-b/x → PermissionDenied.
 F3-E  tenant-a key → ListTables filters to tenant-a/* only.

 F4-A  CreateTable by admin lands an audit record (op=CreateTable).
 F4-B  AddRows by tenant-a lands an audit record (op=AddRows +
       principal prefix + target has tenant-a/).
 F4-C  With a W3C traceparent in the request, the audit record's
       top-level `trace_id` field matches the 32-hex middle segment
       (F9 verification). The `details` substring `trace_id=<hex>`
       is also present for back-compat.

Both scenarios share one coord + 2 worker cluster.
"""
import sys, os, time, struct, subprocess, json, shutil
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import wait_for_grpc

import pyarrow as pa
import pyarrow.ipc as ipc
import grpc, yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

# Strip proxies so localhost RPC isn't routed through a WSL2 system
# proxy (H24 / covered by F8 on the SDK side — but this test predates
# the SDK fix landing in all callers).
for _var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
             "GRPC_PROXY", "grpc_proxy"):
    os.environ.pop(_var, None)

DIM = 8
BIN = os.environ.get("LANCEFORGE_BIN",
                    os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
BASE = "/tmp/lanceforge_beta2_nsaudit"
COORD_PORT = 55950
WORKER_PORTS = [55900, 55901]
AUDIT_PATH = os.path.join(BASE, "audit.jsonl")
TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
TRACEPARENT = f"00-{TRACE_ID}-b7ad6b7169203331-01"

processes = []
results = {"passed": 0, "failed": 0}


def cleanup():
    for p in processes:
        try:
            p.terminate()
        except Exception:
            pass
    time.sleep(1)


def step(msg):
    print(f"\n{'=' * 60}\n  {msg}\n{'=' * 60}")


def test(name, fn):
    print(f"  {name}...", end=" ", flush=True)
    try:
        fn()
        print("PASS")
        results["passed"] += 1
    except Exception as e:
        print(f"FAIL: {e}")
        results["failed"] += 1


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema)
    w.write_batch(batch)
    w.close()
    return sink.getvalue().to_pybytes()


def make_stub(key=None, traceparent=None):
    mds = []
    if key:
        mds.append(("authorization", f"Bearer {key}"))
    if traceparent:
        mds.append(("traceparent", traceparent))

    class Interceptor(grpc.UnaryUnaryClientInterceptor):
        def intercept_unary_unary(self, cont, call_details, req):
            md = list(call_details.metadata or [])
            md.extend(mds)
            call_details = call_details._replace(metadata=md)
            return cont(call_details, req)

    ch = grpc.insecure_channel(
        f"127.0.0.1:{COORD_PORT}",
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)],
    )
    if mds:
        ch = grpc.intercept_channel(ch, Interceptor())
    return pbg.LanceSchedulerServiceStub(ch)


def make_batch(n=50, offset=0):
    np.random.seed(7 + offset)
    return pa.record_batch([
        pa.array(range(offset, offset + n), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)),
            list_size=DIM,
        ),
    ], names=['id', 'vector'])


# ── Setup ─────────────────────────────────────────────────────────────
step("Setup: cluster with RBAC + namespace bindings + audit sink")
os.system(
    f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|"
    f"lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null"
)
time.sleep(1)
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
        'api_keys_rbac': [
            {'key': 'admin-no-ns', 'role': 'admin'},
            {'key': 'tenant-a-admin', 'role': 'admin', 'namespace': 'tenant-a'},
            {'key': 'tenant-a-writer', 'role': 'write', 'namespace': 'tenant-a'},
            {'key': 'tenant-b-admin', 'role': 'admin', 'namespace': 'tenant-b'},
        ],
    },
}
cfg_path = os.path.join(BASE, 'config.yaml')
with open(cfg_path, 'w') as f:
    yaml.dump(config, f)

for i in range(2):
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", cfg_path, f"w{i}", str(WORKER_PORTS[i])],
        stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT,
    )
    processes.append(p)
for i in range(2):
    assert wait_for_grpc("127.0.0.1", WORKER_PORTS[i]), f"w{i} failed"

coord = subprocess.Popen(
    [f"{BIN}/lance-coordinator", cfg_path, str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT,
)
processes.append(coord)
assert wait_for_grpc("127.0.0.1", COORD_PORT), "coord failed"
time.sleep(2)

# ── F3: namespace enforcement ────────────────────────────────────────
step("F3: namespace binding end-to-end")


def t_f3a_admin_no_ns_unrestricted():
    stub = make_stub("admin-no-ns")
    stub.CreateTable(pb.CreateTableRequest(
        table_name="global-table", arrow_ipc_data=ipc_bytes(make_batch(50))))
test("F3-A admin-no-ns → CreateTable('global-table') succeeds", t_f3a_admin_no_ns_unrestricted)


def t_f3b_tenant_a_own_ns_ok():
    stub = make_stub("tenant-a-admin")
    stub.CreateTable(pb.CreateTableRequest(
        table_name="tenant-a/orders", arrow_ipc_data=ipc_bytes(make_batch(50, 100))))
test("F3-B tenant-a → CreateTable('tenant-a/orders') succeeds", t_f3b_tenant_a_own_ns_ok)


def t_f3c_tenant_a_cross_ns_create_denied():
    stub = make_stub("tenant-a-admin")
    try:
        stub.CreateTable(pb.CreateTableRequest(
            table_name="tenant-b/x", arrow_ipc_data=ipc_bytes(make_batch(50, 200))))
        raise AssertionError("Expected PermissionDenied")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.PERMISSION_DENIED, \
            f"got {e.code()}: {e.details()}"
        assert "tenant-a" in e.details() or "namespace" in e.details(), \
            f"err must name the namespace: {e.details()}"
test("F3-C tenant-a → CreateTable('tenant-b/x') PERMISSION_DENIED",
     t_f3c_tenant_a_cross_ns_create_denied)


def t_f3d_tenant_a_cross_ns_search_denied():
    # First make sure tenant-b has a table we'd be poking at.
    make_stub("tenant-b-admin").CreateTable(pb.CreateTableRequest(
        table_name="tenant-b/leads", arrow_ipc_data=ipc_bytes(make_batch(50, 300))))

    stub = make_stub("tenant-a-admin")
    try:
        qv = np.random.randn(DIM).astype(np.float32).tolist()
        stub.AnnSearch(pb.AnnSearchRequest(
            table_name="tenant-b/leads", vector_column="vector",
            query_vector=struct.pack(f"<{DIM}f", *qv),
            dimension=DIM, k=5, nprobes=1, metric_type=0,
        ))
        raise AssertionError("Expected PermissionDenied on cross-ns search")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.PERMISSION_DENIED, \
            f"got {e.code()}: {e.details()}"
test("F3-D tenant-a → AnnSearch('tenant-b/leads') PERMISSION_DENIED",
     t_f3d_tenant_a_cross_ns_search_denied)


def t_f3e_list_tables_filtered():
    # admin-no-ns sees everything.
    admin_list = make_stub("admin-no-ns").ListTables(pb.ListTablesRequest())
    assert "global-table" in admin_list.table_names
    assert "tenant-a/orders" in admin_list.table_names
    assert "tenant-b/leads" in admin_list.table_names, \
        f"admin must see all tables, got {list(admin_list.table_names)}"

    # tenant-a sees only tenant-a/*.
    a_list = make_stub("tenant-a-admin").ListTables(pb.ListTablesRequest())
    a_names = list(a_list.table_names)
    assert all(n.startswith("tenant-a/") for n in a_names), \
        f"tenant-a must only see tenant-a/* tables, got {a_names}"
    assert "tenant-a/orders" in a_names
    assert "tenant-b/leads" not in a_names, \
        f"cross-tenant leak: tenant-a saw tenant-b's table in ListTables: {a_names}"
test("F3-E ListTables filters by caller namespace", t_f3e_list_tables_filtered)


# ── F4: audit log lands on real RPC ──────────────────────────────────
step("F4: audit log file")


def t_f4a_create_audit_record():
    # The CreateTable calls above should have produced JSONL records.
    # Sleep briefly so the sink has time to flush.
    time.sleep(1)
    assert os.path.exists(AUDIT_PATH), f"audit file missing: {AUDIT_PATH}"
    with open(AUDIT_PATH) as f:
        lines = [json.loads(line) for line in f if line.strip()]
    ops = [ln["op"] for ln in lines]
    assert "CreateTable" in ops, \
        f"no CreateTable record in audit log, got ops={ops}"
    # At least one of them must target "tenant-a/orders" by principal
    # tenant-a-admin (prefix "key:tenant-a").
    ta_creates = [
        ln for ln in lines
        if ln["op"] == "CreateTable"
        and ln["target"] == "tenant-a/orders"
        and ln["principal"].startswith("key:tenant-a")
    ]
    assert ta_creates, (
        "no CreateTable record attributed to tenant-a-admin for "
        "tenant-a/orders — audit wiring broken on create_table?"
    )
test("F4-A audit file contains CreateTable record for tenant-a", t_f4a_create_audit_record)


def t_b1_trace_id_reaches_worker_log():
    # B1: coord should re-attach traceparent on outbound
    # execute_local_search. Worker prints an info! with the trace_id
    # so cross-process log grep works.
    stub = make_stub("tenant-a-admin", traceparent=TRACEPARENT)
    qv = np.random.randn(DIM).astype(np.float32).tolist()
    stub.AnnSearch(pb.AnnSearchRequest(
        table_name="tenant-a/orders", vector_column="vector",
        query_vector=struct.pack(f"<{DIM}f", *qv),
        dimension=DIM, k=5, nprobes=1, metric_type=0,
    ))
    time.sleep(0.5)

    # At least one of the two worker logs must carry the trace_id.
    hits = 0
    for wlog in [f"{BASE}/w0.log", f"{BASE}/w1.log"]:
        with open(wlog) as f:
            if f"trace_id={TRACE_ID}" in f.read():
                hits += 1
    assert hits >= 1, (
        f"no worker log carries trace_id={TRACE_ID} — coord is not "
        f"propagating traceparent on outbound scatter-gather"
    )
test("B1 worker log carries client's trace_id across gRPC hop",
     t_b1_trace_id_reaches_worker_log)


def t_f4b_addrows_audit_record_with_traceparent():
    # Fire one AddRows with an explicit W3C traceparent header.
    stub = make_stub("tenant-a-writer", traceparent=TRACEPARENT)
    stub.AddRows(pb.AddRowsRequest(
        table_name="tenant-a/orders",
        arrow_ipc_data=ipc_bytes(make_batch(20, 1000)),
    ))
    time.sleep(1)

    with open(AUDIT_PATH) as f:
        lines = [json.loads(line) for line in f if line.strip()]
    addrows = [
        ln for ln in lines
        if ln["op"] == "AddRows"
        and ln["target"] == "tenant-a/orders"
    ]
    assert addrows, f"no AddRows audit record, got {[ln['op'] for ln in lines]}"
    # F9: top-level trace_id field must match.
    rec = addrows[-1]
    assert rec.get("trace_id") == TRACE_ID, (
        f"F9 dedicated trace_id field mismatch: got {rec.get('trace_id')}, "
        f"expected {TRACE_ID}"
    )
    # Back-compat: details substring still carries trace_id=<hex>.
    assert f"trace_id={TRACE_ID}" in rec["details"], (
        f"back-compat: details must still embed trace_id=<hex> "
        f"for one version (got details={rec['details']!r})"
    )
    # Principal must be the 8-char prefix form, not leak the full key.
    # "tenant-a-writer" truncates to "tenant-a…" — by design collapses
    # writer/admin of the same tenant to the same short principal.
    assert rec["principal"].startswith("key:tenant-a"), \
        f"principal must start with key:tenant-a, got {rec['principal']!r}"
    assert "tenant-a-writer" not in rec["principal"], \
        f"principal must not leak full key, got {rec['principal']!r}"
    assert rec["principal"].endswith("…") or rec["principal"].endswith("..."), \
        f"principal must be a truncation marker, got {rec['principal']!r}"
test("F4-B AddRows audit record w/ traceparent → trace_id top-level field",
     t_f4b_addrows_audit_record_with_traceparent)


# ── Teardown ─────────────────────────────────────────────────────────
cleanup()

print(f"\n{'=' * 60}\n  RESULTS: {results['passed']} passed, {results['failed']} failed\n{'=' * 60}")
sys.exit(0 if results["failed"] == 0 else 1)
