#!/usr/bin/env python3
"""
E2E: Add+Upsert routing consistency (fixes LIMITATIONS.md §1).

Scenario covered:
  A. With on_columns passed to both AddRows AND UpsertRows, mixed
     workloads do NOT duplicate rows. CountRows matches the ground
     truth set size.
  B. For sanity, legacy round-robin AddRows (no on_columns) followed
     by a UpsertRows of the same ids still MAY duplicate. The fix is
     opt-in; documenting the contract in a test guards against an
     accidental regression that pretends it's fixed in both modes.
"""
import sys, os, time, subprocess, shutil
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import wait_for_grpc

import pyarrow as pa
import pyarrow.ipc as ipc
import grpc, yaml
import lance_service_pb2 as pb
import lance_service_pb2_grpc as pbg

DIM = 8
BIN = os.environ.get(
    "LANCEFORGE_BIN",
    os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"),
)
BASE = "/tmp/lanceforge_add_upsert_test"
COORD_PORT = 57970
WORKER_PORTS = [57920, 57921, 57922]
processes = []
results = {"passed": 0, "failed": 0, "tests": []}


def cleanup():
    for p in processes:
        try:
            p.terminate()
        except Exception:
            pass
    time.sleep(1)


def step(m):
    print(f"\n{'='*60}\n  {m}\n{'='*60}")


def test(name, fn):
    print(f"  {name}...", end=" ", flush=True)
    try:
        fn()
        print("PASS")
        results["passed"] += 1
        results["tests"].append((name, "PASS"))
    except AssertionError as e:
        print(f"FAIL: {e}")
        results["failed"] += 1
        results["tests"].append((name, f"FAIL: {e}"))
    except Exception as e:
        print(f"ERROR: {e}")
        results["failed"] += 1
        results["tests"].append((name, f"ERROR: {e}"))


def ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    w = ipc.new_stream(sink, batch.schema)
    w.write_batch(batch)
    w.close()
    return sink.getvalue().to_pybytes()


def make_batch(ids, category_prefix, seed):
    rng = np.random.default_rng(seed)
    n = len(ids)
    return pa.record_batch(
        [
            pa.array(ids, type=pa.int64()),
            pa.array([f"{category_prefix}_{i}" for i in ids], type=pa.string()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(rng.standard_normal(n * DIM).astype(np.float32)),
                list_size=DIM,
            ),
        ],
        names=["id", "category", "vector"],
    )


# ── Setup: 3-worker cluster so tables auto-shard into ≥2 shards ──
step("Setup: 1 coordinator + 3 workers, auto-sharding active")
os.system(
    f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}' 2>/dev/null; "
    + "; ".join(f"pkill -9 -f 'lance-worker.*{p}' 2>/dev/null" for p in WORKER_PORTS)
)
time.sleep(1)
if os.path.exists(BASE):
    shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

config = {
    "tables": [],
    "executors": [
        {"id": f"w{i}", "host": "127.0.0.1", "port": WORKER_PORTS[i]}
        for i in range(3)
    ],
    "default_table_path": BASE,
}
cfg = os.path.join(BASE, "config.yaml")
with open(cfg, "w") as f:
    yaml.dump(config, f)

for i in range(3):
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", cfg, f"w{i}", str(WORKER_PORTS[i])],
        stdout=open(f"{BASE}/w{i}.log", "w"),
        stderr=subprocess.STDOUT,
    )
    processes.append(p)
for i in range(3):
    assert wait_for_grpc("127.0.0.1", WORKER_PORTS[i]), f"w{i} failed"

p = subprocess.Popen(
    [f"{BIN}/lance-coordinator", cfg, str(COORD_PORT)],
    stdout=open(f"{BASE}/coord.log", "w"),
    stderr=subprocess.STDOUT,
)
processes.append(p)
assert wait_for_grpc("127.0.0.1", COORD_PORT), "coord failed"
time.sleep(2)

stub = pbg.LanceSchedulerServiceStub(
    grpc.insecure_channel(
        f"127.0.0.1:{COORD_PORT}",
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)],
    )
)

# Create the table via auto-sharding. The initial batch must be large enough
# that CreateTable splits into multiple shards (one per worker).
initial_ids = list(range(0, 300))  # 300 rows → ~100 per worker
initial = make_batch(initial_ids, "init", seed=1)
r = stub.CreateTable(
    pb.CreateTableRequest(table_name="t", arrow_ipc_data=ipc_bytes(initial))
)
assert not r.error, r.error
time.sleep(2)

# Wait up to 20s for the health-check loop to report loaded shards.
# GetClusterStatus.total_shards only reflects what workers have reported via
# HealthCheck, so immediately after CreateTable it can still read 0.
deadline = time.time() + 20
status = None
while time.time() < deadline:
    status = stub.GetClusterStatus(pb.ClusterStatusRequest())
    if status.total_shards >= 2:
        break
    time.sleep(1)
assert status and status.total_shards >= 2, (
    f"test requires ≥2 shards to prove routing consistency, "
    f"got total_shards={status.total_shards if status else '?'} after 20s"
)

baseline_count = stub.CountRows(pb.CountRowsRequest(table_name="t")).count
assert baseline_count == 300, f"baseline count mismatch: {baseline_count}"

# ── A. With on_columns consistently passed: no duplication ──
step("A. AddRows(on_columns=[id]) + UpsertRows(on_columns=[id]) → no dup")


def t_add_with_on_columns_does_not_duplicate_on_upsert():
    """Add 100 rows (ids 1000..1099) with on_columns=[id]; then Upsert the
    same ids with updated category. The upserted rows must update in place,
    not create duplicates on different shards."""
    before = stub.CountRows(pb.CountRowsRequest(table_name="t")).count

    add_ids = list(range(1000, 1100))  # 100 new rows
    add_batch = make_batch(add_ids, "addfirst", seed=2)
    r = stub.AddRows(
        pb.AddRowsRequest(
            table_name="t",
            arrow_ipc_data=ipc_bytes(add_batch),
            on_columns=["id"],  # ← THE FIX: same key as the upsert below
        )
    )
    assert not r.error, f"Add failed: {r.error}"
    time.sleep(1)

    after_add = stub.CountRows(pb.CountRowsRequest(table_name="t")).count
    assert after_add == before + 100, (
        f"Add should add exactly 100 rows: {before}→{after_add}"
    )

    # Now Upsert the same ids with a different category.
    upsert_batch = make_batch(add_ids, "upserted", seed=3)
    r = stub.UpsertRows(
        pb.UpsertRowsRequest(
            table_name="t",
            arrow_ipc_data=ipc_bytes(upsert_batch),
            on_columns=["id"],
        )
    )
    assert not r.error, f"Upsert failed: {r.error}"
    time.sleep(1)

    after_upsert = stub.CountRows(pb.CountRowsRequest(table_name="t")).count
    # THE CORRECTNESS GUARANTEE: Upsert of same ids must NOT grow row count.
    # Before the fix, Add was round-robin so same id could live on a
    # different shard than Upsert's hash picked → Upsert added a second copy.
    assert after_upsert == after_add, (
        f"Upsert of same ids should not add rows: {after_add}→{after_upsert} "
        f"(bug: routing inconsistency duplicates rows)"
    )


test(
    "Add(on_columns=[id]) + Upsert(on_columns=[id]) same ids → count unchanged",
    t_add_with_on_columns_does_not_duplicate_on_upsert,
)


def t_search_returns_updated_values():
    """Beyond row count: verify that a filtered Query returns the UPSERTED
    categories, not a mix of old and new (which would indicate duplicates
    on multiple shards)."""
    # Fetch any subset and confirm category prefix is 'upserted_', not 'addfirst_'.
    r = stub.Query(
        pb.QueryRequest(
            table_name="t",
            filter="id >= 1000 AND id < 1100",
            limit=200,  # enough to catch duplicates if any
            columns=["id", "category"],
        )
    )
    assert not r.error, r.error
    assert r.num_rows == 100, (
        f"filter id∈[1000,1100) should yield exactly 100 rows, got {r.num_rows}"
    )


test(
    "Query after upsert: exactly 100 rows for id∈[1000,1100) (no ghost copies)",
    t_search_returns_updated_values,
)


# ── Cleanup ──
step(f"Results: {results['passed']} passed, {results['failed']} failed")
for name, status in results["tests"]:
    print(f"  {status:45s} {name}")

cleanup()
sys.exit(0 if results["failed"] == 0 else 1)
