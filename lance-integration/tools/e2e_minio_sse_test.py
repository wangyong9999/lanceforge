#!/usr/bin/env python3
"""
D1: MinIO SSE-S3 (bucket-level default encryption) end-to-end.

Validates that **bucket-level** server-side encryption is transparent
to LanceForge: we enable `ServerSideEncryptionByDefault: AES256` on
the target bucket, LanceForge writes through its normal
`storage_options` path (no SSE-specific config), and MinIO
auto-encrypts every object on the way in.

**What this test does NOT cover** (and why):

- **SSE-C (customer-provided keys)**: requires per-PUT request
  headers for every object; `object_store` crate's `ObjectStore`
  trait doesn't expose that hook (headers are per-client config,
  not per-put). An earlier version of this test tried SSE-C
  through `storage_options.aws_server_side_encryption_customer_*`
  and observed MinIO storing objects unencrypted — the headers
  never reach the wire. Tracked as a 0.3 item; SECURITY.md §6.3
  now reflects the truth (bucket-level works, SSE-C requires a
  custom object_store wrapper).
- **SSE-KMS with a real KMS**: requires AWS credentials or a
  LocalStack equivalent. Out of scope for single-binary MinIO
  runs; SSE-S3 is the equivalent "server manages key" mode and
  exercises the same LanceForge code path.

Flow:
  1. Assume MinIO is running on MINIO_ENDPOINT with
     MINIO_KMS_SECRET_KEY env set (required for bucket-level SSE).
  2. Ensure bucket exists and has default-encryption set to AES256.
  3. Launch a 2-worker coord with normal storage_options (no SSE).
  4. CreateTable + AddRows + AnnSearch round-trip.
  5. Probe MinIO: every written object must report
     `ServerSideEncryption: AES256`.
"""
import os
import shutil
import struct
import subprocess
import sys
import time

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
BASE = "/tmp/lanceforge_minio_sse"
COORD_PORT = 56550
WORKER_PORTS = [56500, 56501]
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_KEY = os.environ.get("MINIO_ROOT_USER", "lanceadmin")
MINIO_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "lanceadmin123")
BUCKET = os.environ.get("MINIO_SSE_BUCKET", "lance-sse-test")
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
    np.random.seed(3 + offset)
    return pa.record_batch([
        pa.array(range(offset, offset + n), type=pa.int64()),
        pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * DIM).astype(np.float32)),
            list_size=DIM,
        ),
    ], names=['id', 'vector'])


def check_minio():
    try:
        import urllib.request
        urllib.request.urlopen(f"{MINIO_ENDPOINT}/minio/health/live",
                               timeout=3).read()
        return True
    except Exception:
        return False


def ensure_bucket_with_encryption():
    """Create bucket if missing; ensure bucket-level default
    encryption is AES256; wipe any residue from previous runs."""
    import boto3
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name='us-east-1',
    )
    try:
        s3.create_bucket(Bucket=BUCKET)
    except Exception as e:
        if "already" not in str(e).lower() and "BucketAlreadyOwnedByYou" not in str(e):
            raise
    s3.put_bucket_encryption(
        Bucket=BUCKET,
        ServerSideEncryptionConfiguration={
            'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]
        },
    )
    # Wipe residue.
    while True:
        r = s3.list_objects_v2(Bucket=BUCKET)
        keys = [{'Key': o['Key']} for o in r.get('Contents', [])]
        if not keys:
            break
        s3.delete_objects(Bucket=BUCKET, Delete={'Objects': keys})
        if not r.get('IsTruncated'):
            break


def probe_all_encrypted():
    import boto3
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        region_name='us-east-1',
    )
    resp = s3.list_objects_v2(Bucket=BUCKET)
    objects = [o['Key'] for o in resp.get('Contents', []) if o.get('Size', 0) > 0]
    if not objects:
        return False, 0, 0, "no objects in bucket — cluster did not write"
    encrypted = 0
    plain = 0
    for k in objects:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        if h.get('ServerSideEncryption') == 'AES256':
            encrypted += 1
        else:
            plain += 1
    ok = plain == 0 and encrypted > 0
    detail = f"{encrypted} encrypted, {plain} plain (out of {len(objects)})"
    return ok, encrypted, plain, detail


def main():
    print("=== D1: MinIO SSE-S3 (bucket-level) e2e ===", flush=True)
    if not check_minio():
        print(f"SKIP: MinIO not reachable at {MINIO_ENDPOINT}", flush=True)
        sys.exit(0)  # skip, not fail — for local runs without MinIO

    ensure_bucket_with_encryption()
    print(f"  [setup] bucket {BUCKET} has default SSE-S3 AES256", flush=True)

    kill_cluster()
    if os.path.exists(BASE):
        shutil.rmtree(BASE)
    os.makedirs(BASE, exist_ok=True)

    storage_options = {
        'aws_access_key_id': MINIO_KEY,
        'aws_secret_access_key': MINIO_SECRET,
        'aws_endpoint': MINIO_ENDPOINT,
        'aws_region': 'us-east-1',
        'allow_http': 'true',
    }
    config = {
        'tables': [],
        'executors': [
            {'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]}
            for i in range(2)
        ],
        'default_table_path': f's3://{BUCKET}/sse-lance',
        'storage_options': storage_options,
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
        stub = pbg.LanceSchedulerServiceStub(
            grpc.insecure_channel(f"127.0.0.1:{COORD_PORT}",
                                  options=[("grpc.max_receive_message_length",
                                            64 * 1024 * 1024)]))

        r = stub.CreateTable(pb.CreateTableRequest(
            table_name="sse_tbl",
            arrow_ipc_data=ipc_bytes(make_batch(80)),
        ))
        assert not r.error, f"CreateTable failed: {r.error}"
        print("  [PASS] CreateTable succeeded on encrypted bucket", flush=True)

        r = stub.AddRows(pb.AddRowsRequest(
            table_name="sse_tbl",
            arrow_ipc_data=ipc_bytes(make_batch(30, 1000)),
            on_columns=[],
        ))
        assert not r.error, f"AddRows failed: {r.error}"
        print("  [PASS] AddRows succeeded", flush=True)

        qv = np.random.randn(DIM).astype(np.float32).tolist()
        r = stub.AnnSearch(pb.AnnSearchRequest(
            table_name="sse_tbl", vector_column="vector",
            query_vector=struct.pack(f"<{DIM}f", *qv),
            dimension=DIM, k=5, nprobes=1, metric_type=0,
        ))
        assert not r.error, f"AnnSearch failed: {r.error}"
        assert r.num_rows > 0, "AnnSearch returned zero rows"
        print(f"  [PASS] AnnSearch returned {r.num_rows} rows", flush=True)

        time.sleep(1)
        ok, encrypted, plain, detail = probe_all_encrypted()
        if not ok:
            raise AssertionError(
                f"encryption enforcement failed: {detail}. Bucket-level "
                f"default encryption was set but some LanceForge objects "
                f"landed unencrypted — this should be impossible with "
                f"MinIO's default-encryption policy, and indicates that "
                f"either (a) LanceForge bypassed the default or (b) MinIO "
                f"default-encryption wasn't actually active."
            )
        print(f"  [PASS] every stored object is SSE-S3 encrypted: {detail}",
              flush=True)

        print("\nALL SSE-S3 ASSERTIONS PASSED", flush=True)
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
