#!/usr/bin/env bash
set -e

# ══════════════════════════════════════════════════════════
#  LanceForge — 5 Minute Demo
#
#  Creates sample data, starts a cluster, runs queries.
#  No external dependencies needed (no MinIO, no Docker).
#
#  Usage: ./demo.sh
#  Cleanup: ./demo.sh clean
# ══════════════════════════════════════════════════════════

DEMO_DIR="/tmp/lanceforge_demo"
BIN_DIR="$(cd "$(dirname "$0")" && pwd)/target/release"
TOOLS_DIR="$(cd "$(dirname "$0")" && pwd)/lance-integration/tools"
SDK_DIR="$(cd "$(dirname "$0")" && pwd)/lance-integration/sdk/python"
COORD_PORT=50050
REST_PORT=50051

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

cleanup() {
    pkill -f "lance-coordinator|lance-worker" 2>/dev/null || true
    sleep 2
}

if [ "$1" = "clean" ]; then
    echo "Cleaning up..."
    cleanup
    rm -rf "$DEMO_DIR"
    echo "Done."
    exit 0
fi

echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  LanceForge Demo — Distributed Multimodal Retrieval${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"

# ── Step 0: Check prerequisites ──
echo -e "\n${BLUE}[0/5] Checking prerequisites...${NC}"

if [ ! -f "$BIN_DIR/lance-coordinator" ] || [ ! -f "$BIN_DIR/lance-worker" ]; then
    echo "  Building release binaries (first time only, ~5 min)..."
    cargo build --release -p lance-distributed-coordinator -p lance-distributed-worker 2>&1 | tail -1
fi

python3 -c "import lance, pyarrow, numpy, yaml, grpc" 2>/dev/null || {
    echo "  Installing Python dependencies..."
    pip install --break-system-packages lance pyarrow numpy pyyaml grpcio grpcio-tools 2>/dev/null || \
    pip install lance pyarrow numpy pyyaml grpcio grpcio-tools 2>/dev/null
}

echo -e "  ${GREEN}✓ Prerequisites ready${NC}"

# ── Step 1: Create sample data ──
echo -e "\n${BLUE}[1/5] Creating sample dataset (5000 documents)...${NC}"

cleanup
mkdir -p "$DEMO_DIR"

python3 - "$DEMO_DIR" "$TOOLS_DIR" <<'PYEOF'
import sys, os, numpy as np
import pyarrow as pa
import lance

demo_dir = sys.argv[1]
np.random.seed(42)

DIM = 64
N = 5000

# Generate documents with vectors + text + metadata
categories = ['technology', 'science', 'business', 'health', 'sports']
titles = []
texts = []
cats = []
for i in range(N):
    cat = categories[i % len(categories)]
    cats.append(cat)
    titles.append(f"Document {i}: {cat} article")
    texts.append(f"{cat} related content about topic {i % 100} with keywords {cat} innovation research")

# Cluster vectors by category for realistic distribution
centroids = np.random.randn(5, DIM).astype(np.float32) * 3
labels = np.array([i % 5 for i in range(N)])
vectors = centroids[labels] + np.random.randn(N, DIM).astype(np.float32) * 0.5

# Split into 2 shards
for shard_id in range(2):
    start = shard_id * (N // 2)
    end = start + N // 2
    uri = os.path.join(demo_dir, f"shard_{shard_id:02d}.lance")

    table = pa.table({
        'id': pa.array(range(start, end), type=pa.int64()),
        'title': pa.array(titles[start:end], type=pa.string()),
        'text': pa.array(texts[start:end], type=pa.string()),
        'category': pa.array(cats[start:end], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(vectors[start:end].flatten(), type=pa.float32()), list_size=DIM),
    })
    lance.write_dataset(table, uri, mode='overwrite')
    ds = lance.dataset(uri)
    ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)
    try:
        ds.create_scalar_index('category', index_type='BTREE')
    except:
        pass
    try:
        ds.create_scalar_index('text', index_type='INVERTED')
    except:
        pass
    print(f"  Shard {shard_id}: {ds.count_rows()} rows ({uri})")

# Write cluster config
import yaml
config = {
    'tables': [{'name': 'documents', 'shards': [
        {'name': f'documents_shard_{i:02d}',
         'uri': os.path.join(demo_dir, f'shard_{i:02d}.lance'),
         'executors': [f'w{i}']} for i in range(2)
    ]}],
    'executors': [
        {'id': 'w0', 'host': '127.0.0.1', 'port': 50100},
        {'id': 'w1', 'host': '127.0.0.1', 'port': 50101},
    ],
}
with open(os.path.join(demo_dir, 'config.yaml'), 'w') as f:
    yaml.dump(config, f)
print("  Config written")
PYEOF

echo -e "  ${GREEN}✓ Dataset ready${NC}"

# ── Step 2: Start cluster ──
echo -e "\n${BLUE}[2/5] Starting cluster (1 coordinator + 2 workers)...${NC}"

"$BIN_DIR/lance-worker" "$DEMO_DIR/config.yaml" w0 50100 &>"$DEMO_DIR/w0.log" &
"$BIN_DIR/lance-worker" "$DEMO_DIR/config.yaml" w1 50101 &>"$DEMO_DIR/w1.log" &
sleep 3
"$BIN_DIR/lance-coordinator" "$DEMO_DIR/config.yaml" $COORD_PORT &>"$DEMO_DIR/coord.log" &
sleep 3

# Verify — check gRPC port is listening (more reliable than REST)
sleep 1
if PYTHONPATH="$TOOLS_DIR:$SDK_DIR" python3 -c "
from lanceforge import LanceForgeClient
c = LanceForgeClient('127.0.0.1:${COORD_PORT}')
s = c.status()
assert len(s['executors']) > 0
c.close()
" 2>/dev/null; then
    echo -e "  ${GREEN}✓ Cluster healthy${NC}"
    echo "    Coordinator: 127.0.0.1:${COORD_PORT} (gRPC)"
    echo "    REST/Metrics: 127.0.0.1:${REST_PORT} (if port available)"
    echo "    Workers: 127.0.0.1:50100, 127.0.0.1:50101"
else
    echo -e "  ${RED}✗ Cluster failed to start. Check $DEMO_DIR/*.log${NC}"
    cat "$DEMO_DIR/coord.log" 2>/dev/null | tail -5
    exit 1
fi

# ── Step 3: Generate proto stubs ──
echo -e "\n${BLUE}[3/5] Preparing Python client...${NC}"

cd "$TOOLS_DIR"
python3 -m grpc_tools.protoc -I../../crates/proto/proto \
    --python_out=. --grpc_python_out=. ../../crates/proto/proto/lance_service.proto 2>/dev/null || true
echo -e "  ${GREEN}✓ Client ready${NC}"

# ── Step 4: Run demo queries ──
echo -e "\n${BLUE}[4/5] Running demo queries...${NC}"

PYTHONPATH="$TOOLS_DIR:$SDK_DIR" python3 - "$COORD_PORT" <<'PYEOF'
import sys, numpy as np
from lanceforge import LanceForgeClient

port = sys.argv[1]
client = LanceForgeClient(f"127.0.0.1:{port}")

print("\n  ── ANN Vector Search (top 5) ──")
q = np.random.randn(64).astype(np.float32).tolist()
results = client.search("documents", query_vector=q, k=5)
for i in range(min(5, results.num_rows)):
    title = results.column("title")[i].as_py()
    cat = results.column("category")[i].as_py()
    dist = results.column("_distance")[i].as_py()
    print(f"    [{cat:>12}] {title}  (dist={dist:.2f})")

print("\n  ── Filtered Search (category='technology', top 3) ──")
results = client.search("documents", query_vector=q, k=3, filter="category = 'technology'")
for i in range(min(3, results.num_rows)):
    title = results.column("title")[i].as_py()
    print(f"    {title}")

print("\n  ── Cluster Status ──")
status = client.status()
for e in status["executors"]:
    health = "✓ healthy" if e["healthy"] else "✗ unhealthy"
    print(f"    Worker {e['id']}: {e['host']}:{e['port']} — {health}")

print("\n  ── Insert 10 new rows ──")
import pyarrow as pa
new_data = pa.table({
    'id': pa.array(range(90000, 90010), type=pa.int64()),
    'title': pa.array([f'New Document {i}' for i in range(10)], type=pa.string()),
    'text': pa.array(['freshly inserted content'] * 10, type=pa.string()),
    'category': pa.array(['demo'] * 10, type=pa.string()),
    'vector': pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(10 * 64).astype(np.float32)), list_size=64),
})
result = client.insert("documents", new_data)
print(f"    Inserted: version={result['new_version']}")

import time; time.sleep(1)
results = client.search("documents", query_vector=q, k=3, filter="category = 'demo'")
print(f"    Search after insert: {results.num_rows} results found")

print("\n  ── Metrics ──")
import urllib.request
resp = urllib.request.urlopen(f"http://127.0.0.1:{int(port)+1}/metrics")
for line in resp.read().decode().split('\n'):
    if line and not line.startswith('#'):
        print(f"    {line.strip()}")

client.close()
PYEOF

echo -e "\n  ${GREEN}✓ All queries successful${NC}"

# ── Step 5: Print next steps ──
echo -e "\n${BLUE}[5/5] What's next?${NC}"
echo ""
echo -e "  ${BOLD}Cluster is running. Try these:${NC}"
echo ""
echo "  # Python SDK"
echo "  export PYTHONPATH=$TOOLS_DIR:$SDK_DIR"
echo "  python3 -c '"
echo "  from lanceforge import LanceForgeClient"
echo "  client = LanceForgeClient(\"127.0.0.1:${COORD_PORT}\")"
echo "  results = client.search(\"documents\", [0.1]*64, k=5)"
echo "  print(results.to_pydict())"
echo "  '"
echo ""
echo "  # REST API"
echo "  curl http://127.0.0.1:${REST_PORT}/healthz"
echo "  curl http://127.0.0.1:${REST_PORT}/metrics"
echo ""
echo "  # Cleanup"
echo "  ./demo.sh clean"
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  Demo complete! LanceForge is ready to use.${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════${NC}"
