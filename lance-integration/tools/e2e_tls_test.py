#!/usr/bin/env python3
"""
TLS End-to-End Test

Tests full TLS encryption chain:
  1. Generate self-signed CA + server certs
  2. Start worker with TLS (server cert)
  3. Start coordinator with TLS (server cert + CA cert for worker connections)
  4. Python SDK connects via TLS (CA cert)
  5. ANN search works over encrypted channel
  6. Verify plaintext connection is rejected
"""

import sys, os, time, subprocess, shutil
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sdk', 'python'))

from test_helpers import wait_for_grpc, wait_for_grpc_tls

import pyarrow as pa
import lance
import yaml

BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(os.path.dirname(__file__), "..", "..", "target", "release"))
TOOLS = os.path.dirname(__file__)
BASE = "/tmp/lanceforge_tls_test"
DIM = 32
COORD_PORT = 58000
CERTS = os.path.join(BASE, "certs")

all_processes = []
results = {"passed": 0, "failed": 0, "tests": []}

def cleanup_all():
    os.system("pkill -f 'lance-coordinator.*58000|lance-worker.*5810' 2>/dev/null")
    time.sleep(2)

def test(name, fn):
    print(f"  {name}...", end=" ", flush=True)
    try:
        fn()
        print("PASS")
        results["passed"] += 1
        results["tests"].append((name, "PASS"))
    except Exception as e:
        print(f"FAIL: {e}")
        results["failed"] += 1
        results["tests"].append((name, f"FAIL: {e}"))

# ==========================================
# Setup
# ==========================================
print(f"\n{'='*70}")
print(f"  TLS END-TO-END TEST SUITE")
print(f"{'='*70}")

cleanup_all()
if os.path.exists(BASE):
    shutil.rmtree(BASE)
os.makedirs(BASE, exist_ok=True)

# Step 1: Generate certs
def t_gen_certs():
    result = subprocess.run(
        ["bash", os.path.join(TOOLS, "gen_test_certs.sh"), CERTS],
        capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, f"Cert generation failed: {result.stderr}"
    for f in ["ca.pem", "server.pem", "server-key.pem"]:
        assert os.path.exists(os.path.join(CERTS, f)), f"Missing {f}"
test("1. Generate self-signed certificates", t_gen_certs)

# Step 2: Create test data
np.random.seed(42)
uri = os.path.join(BASE, "shard_00.lance")
table = pa.table({
    'id': pa.array(range(1000), type=pa.int64()),
    'vector': pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(1000 * DIM).astype(np.float32)), list_size=DIM),
})
lance.write_dataset(table, uri, mode='overwrite')
ds = lance.dataset(uri)
ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)

# Step 3: Write config with TLS enabled
config = {
    'tables': [{'name': 'tls_table', 'shards': [
        {'name': 'tls_table_shard_00', 'uri': uri, 'executors': ['w0']}
    ]}],
    'executors': [
        {'id': 'w0', 'host': '127.0.0.1', 'port': 58100},
    ],
    'security': {
        'tls_cert': os.path.join(CERTS, 'server.pem'),
        'tls_key': os.path.join(CERTS, 'server-key.pem'),
        'tls_ca_cert': os.path.join(CERTS, 'ca.pem'),
    },
}
with open(f"{BASE}/config.yaml", 'w') as f:
    yaml.dump(config, f)

# Step 4: Start worker with TLS
def t_start_worker():
    p = subprocess.Popen(
        [f"{BIN}/lance-worker", f"{BASE}/config.yaml", "w0", "58100"],
        stdout=open(f"{BASE}/w0.log", "w"), stderr=subprocess.STDOUT)
    all_processes.append(p)
    assert wait_for_grpc_tls("127.0.0.1", 58100, os.path.join(CERTS, "ca.pem")), "Worker failed to start"
    assert p.poll() is None, "Worker crashed on startup"
    # Check that TLS is enabled in logs
    with open(f"{BASE}/w0.log") as f:
        log = f.read()
    assert "TLS enabled" in log, f"TLS not enabled in worker log"
test("2. Worker starts with TLS", t_start_worker)

# Step 5: Start coordinator with TLS
def t_start_coordinator():
    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT)
    all_processes.append(p)
    assert wait_for_grpc_tls("127.0.0.1", COORD_PORT, os.path.join(CERTS, "ca.pem")), "Coordinator failed to start"
    assert p.poll() is None, "Coordinator crashed on startup"
    with open(f"{BASE}/coord.log") as f:
        log = f.read()
    assert "TLS enabled" in log, f"TLS not enabled in coordinator log"
test("3. Coordinator starts with TLS", t_start_coordinator)

# Step 6: SDK connects with TLS and searches
from lanceforge import LanceForgeClient

def t_tls_search():
    client = LanceForgeClient(
        f"127.0.0.1:{COORD_PORT}",
        tls_ca_cert=os.path.join(CERTS, "ca.pem"))
    r = client.search("tls_table", query_vector=np.zeros(DIM).tolist(), k=5)
    assert r.num_rows == 5, f"Expected 5 results, got {r.num_rows}"
    client.close()
test("4. SDK search over TLS succeeds", t_tls_search)

def t_tls_status():
    client = LanceForgeClient(
        f"127.0.0.1:{COORD_PORT}",
        tls_ca_cert=os.path.join(CERTS, "ca.pem"))
    s = client.status()
    healthy = sum(1 for e in s["executors"] if e["healthy"])
    assert healthy == 1, f"Expected 1 healthy, got {healthy}"
    client.close()
test("5. Cluster status over TLS", t_tls_status)

# Step 7: Plaintext connection should fail
def t_plaintext_rejected():
    client = LanceForgeClient(f"127.0.0.1:{COORD_PORT}")
    try:
        client.search("tls_table", query_vector=np.zeros(DIM).tolist(), k=1)
        client.close()
        raise AssertionError("Plaintext search should have failed against TLS server")
    except Exception as e:
        if "AssertionError" in str(type(e)):
            raise
        # Expected: connection error or handshake failure
        pass
test("6. Plaintext connection rejected by TLS server", t_plaintext_rejected)

# ==========================================
# Cleanup + Summary
# ==========================================
cleanup_all()

print(f"\n{'='*70}")
print(f"  TLS TEST RESULTS: {results['passed']} passed, {results['failed']} failed")
print(f"{'='*70}")
for name, status in results["tests"]:
    print(f"  {'V' if 'PASS' in status else 'X'} {name}: {status}")
print(f"{'='*70}")

sys.exit(0 if results["failed"] == 0 else 1)
