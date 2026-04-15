#!/usr/bin/env python3
"""
Phase 13 E2E: pagination (offset) + response size cap + audit logging + auto-compaction.
"""
import sys, os, time, struct, subprocess
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import wait_for_grpc

import pyarrow as pa
import pyarrow.ipc as ipc
import grpc, yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

DIM = 8
BIN = os.path.expanduser("~/cc/lance-ballista/target/release")
BASE = "/tmp/lanceforge_phase13_test"
COORD_PORT = 54950
WORKER_PORTS = [54900, 54901]
processes = []
results = {"passed": 0, "failed": 0, "tests": []}

def cleanup():
    for p in processes:
        try: p.terminate()
        except: pass
    time.sleep(1)

def step(m): print(f"\n{'='*60}\n  {m}\n{'='*60}")

def test(name, fn):
    print(f"  {name}...", end=" ", flush=True)
    try:
        fn(); print("PASS"); results["passed"] += 1
        results["tests"].append((name, "PASS"))
    except Exception as e:
        print(f"FAIL: {e}"); results["failed"] += 1
        results["tests"].append((name, f"FAIL: {e}"))

def ev(v): return struct.pack(f"<{len(v)}f", *v)

def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema); w.write_batch(batch); w.close()
    return sink.getvalue().to_pybytes()

def decode(resp):
    if not resp.arrow_ipc_data:
        return pa.table({})
    return ipc.open_stream(resp.arrow_ipc_data).read_all()

# ── Setup ──
step("Setup: small max_response_bytes + admin key for audit")
os.system(f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null")
time.sleep(1)
import shutil
if os.path.exists(BASE): shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

config = {
    'tables': [],
    'executors': [
        {'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]}
        for i in range(2)
    ],
    'default_table_path': BASE,
    'security': {
        'api_keys_rbac': [{'key': 'admin-k', 'role': 'admin'}],
    },
    'server': {
        'max_response_bytes': 4096,   # tiny cap to force truncation
        'max_k': 1000,
    },
    'compaction': {
        'enabled': True,
        'interval_secs': 5,           # fast cycle for test
    },
}
cfg = os.path.join(BASE, 'config.yaml')
with open(cfg, 'w') as f: yaml.dump(config, f)

for i in range(2):
    p = subprocess.Popen([f"{BIN}/lance-worker", cfg, f"w{i}", str(WORKER_PORTS[i])],
        stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT)
    processes.append(p)
for i in range(2):
    assert wait_for_grpc("127.0.0.1", WORKER_PORTS[i]), f"w{i} failed"

p = subprocess.Popen([f"{BIN}/lance-coordinator", cfg, str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
processes.append(p)
assert wait_for_grpc("127.0.0.1", COORD_PORT), "coord failed"
time.sleep(2)

class AuthInterceptor(grpc.UnaryUnaryClientInterceptor):
    def intercept_unary_unary(self, cont, call_details, req):
        md = list(call_details.metadata or [])
        md.append(("authorization", "Bearer admin-k"))
        return cont(call_details._replace(metadata=md), req)

ch = grpc.intercept_channel(
    grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)]),
    AuthInterceptor())
stub = pbg.LanceSchedulerServiceStub(ch)

# Create a table
np.random.seed(7)
N = 500
batch = pa.record_batch([
    pa.array(range(N), type=pa.int64()),
    pa.array([f'c{i}' for i in range(N)], type=pa.string()),
    pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(N*DIM).astype(np.float32)), list_size=DIM),
], names=['id', 'category', 'vector'])

r = stub.CreateTable(pb.CreateTableRequest(table_name="paginate", arrow_ipc_data=ipc_bytes(batch)))
assert not r.error, r.error

# ── Scenario A: pagination ──
step("A. Pagination via offset")

def t_page0():
    r = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="paginate", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=5, nprobes=10, metric_type=0, offset=0))
    t = decode(r)
    assert t.num_rows == 5, f"page0 expected 5, got {t.num_rows}"
test("offset=0 k=5 returns 5 rows", t_page0)

def t_page2():
    r = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="paginate", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=5, nprobes=10, metric_type=0, offset=10))
    t = decode(r)
    assert t.num_rows == 5, f"page2 expected 5, got {t.num_rows}"
test("offset=10 k=5 returns 5 rows", t_page2)

def t_pages_distinct():
    r1 = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="paginate", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=5, nprobes=10, metric_type=0, offset=0))
    r2 = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="paginate", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=5, nprobes=10, metric_type=0, offset=5))
    ids1 = decode(r1).column("id").to_pylist()
    ids2 = decode(r2).column("id").to_pylist()
    assert set(ids1).isdisjoint(set(ids2)), f"pages overlap: {ids1} vs {ids2}"
test("offset=0 and offset=5 return disjoint id sets", t_pages_distinct)

def t_max_k_enforced():
    try:
        stub.AnnSearch(pb.AnnSearchRequest(
            table_name="paginate", vector_column="vector",
            query_vector=ev(np.zeros(DIM, dtype=np.float32)),
            dimension=DIM, k=999, nprobes=10, metric_type=0, offset=500))
        raise AssertionError("expected INVALID_ARGUMENT for k+offset > max_k")
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.INVALID_ARGUMENT, f"got {e.code()}"
test("k+offset > max_k → INVALID_ARGUMENT", t_max_k_enforced)

# ── Scenario B: response size cap ──
step("B. Response size truncation")

def t_truncated_when_huge_k():
    # Request large k → coord caps response by max_response_bytes (4 KiB)
    r = stub.AnnSearch(pb.AnnSearchRequest(
        table_name="paginate", vector_column="vector",
        query_vector=ev(np.zeros(DIM, dtype=np.float32)),
        dimension=DIM, k=500, nprobes=10, metric_type=0, offset=0))
    assert r.truncated, "should be truncated by 4 KiB cap"
    assert r.next_offset > 0, "next_offset should suggest continuation"
    assert len(r.arrow_ipc_data) <= 4096, f"payload {len(r.arrow_ipc_data)} > cap"
test("Large k → truncated=true, next_offset>0, payload<=cap", t_truncated_when_huge_k)

# ── Scenario C: audit log ──
step("C. Audit log emitted for admin ops")

def t_audit_emitted():
    # CreateTable was already called above. Check coord.log for audit line.
    log_path = os.path.join(BASE, "coord.log")
    time.sleep(1)
    with open(log_path) as f: content = f.read()
    assert "audit op=CreateTable principal=key:admin-k" in content, \
        f"CreateTable audit line missing in coord.log"
test("CreateTable emits audit log with principal", t_audit_emitted)

def t_audit_drop():
    stub.DropTable(pb.DropTableRequest(table_name="paginate"))
    time.sleep(1)
    log_path = os.path.join(BASE, "coord.log")
    with open(log_path) as f: content = f.read()
    assert "audit op=DropTable" in content and "target=paginate" in content, \
        "DropTable audit line missing"
test("DropTable emits audit log with target", t_audit_drop)

# ── Scenario D: auto-compaction ──
step("D. Auto-compaction loop runs")

def t_compaction_logged():
    # interval_secs=5 → after 6+s the worker should log "Auto-compaction"
    # First create a table so there's something to compact
    new_batch = pa.record_batch([
        pa.array(range(100), type=pa.int64()),
        pa.array(['x']*100, type=pa.string()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(100*DIM).astype(np.float32)), list_size=DIM),
    ], names=['id', 'category', 'vector'])
    stub.CreateTable(pb.CreateTableRequest(table_name="compactme", arrow_ipc_data=ipc_bytes(new_batch)))
    time.sleep(7)
    found = False
    for i in range(2):
        log_path = os.path.join(BASE, f"w{i}.log")
        with open(log_path) as f:
            if "Auto-compaction" in f.read():
                found = True; break
    assert found, "Auto-compaction log line not found in any worker log"
test("Auto-compaction periodic loop fires", t_compaction_logged)

# ── Cleanup ──
step("Cleanup"); cleanup()

print(f"\n{'='*60}")
print(f"  PHASE 13 E2E: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*60}")
for name, status in results["tests"]:
    print(f"  {'OK' if 'PASS' in status else 'XX'} {name}: {status}")
print(f"{'='*60}")
sys.exit(0 if results["failed"] == 0 else 1)
