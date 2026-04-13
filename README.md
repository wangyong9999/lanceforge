<p align="center">
  <h1 align="center">LanceForge</h1>
  <p align="center">
    <b>Distributed Multimodal Retrieval Engine for Lance Tables</b>
    <br/>
    Horizontal scaling for vector search, full-text search, and hybrid retrieval
  </p>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#features">Features</a> ·
  <a href="#performance">Performance</a> ·
  <a href="#python-sdk">SDK</a> ·
  <a href="#benchmarks">Benchmarks</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange" alt="Rust"/>
  <img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"/>
  <img src="https://img.shields.io/badge/tests-99%20unit%20%7C%2029%20integration%20%7C%2021%20E2E-green" alt="Tests"/>
  <img src="https://img.shields.io/badge/recall%4010-1.0000-brightgreen" alt="Recall"/>
</p>

---

## What is LanceForge?

LanceForge extends [LanceDB](https://github.com/lancedb/lancedb) from an **embedded single-node** database to a **horizontally scalable distributed** query engine — without changing your data format.

```
Single-node LanceDB           LanceForge (distributed)
┌──────────┐                  ┌────────────────────────────────┐
│  App     │                  │  App                            │
│    ↓     │                  │    ↓ gRPC / Python SDK          │
│ LanceDB  │    ──────►       │  Coordinator (scatter-gather)   │
│    ↓     │   same .lance    │    ↓         ↓         ↓       │
│  .lance  │     files        │  Worker 0  Worker 1  Worker 2  │
└──────────┘                  │    ↓         ↓         ↓       │
                              │     S3 / MinIO / Local FS      │
                              └────────────────────────────────┘
```

**Same Lance files. Same queries. Horizontal throughput.**

## Architecture

```
                     ┌──────────────────────────────────────────┐
                     │              Coordinator                  │
  Client ──gRPC────►│                                          │
  Python SDK        │  ┌─────────┐ ┌──────────┐ ┌──────────┐  │
  REST API          │  │ Scatter │→│  Merge   │→│  Return  │  │
                     │  │ Gather  │ │  TopK    │ │  Result  │  │
                     │  └────┬────┘ └──────────┘ └──────────┘  │
                     │       │ parallel fan-out                  │
                     └───────┼──────────────────────────────────┘
                  ┌──────────┼──────────┐
                  ▼          ▼          ▼
           ┌──────────┐┌──────────┐┌──────────┐
           │ Worker 0 ││ Worker 1 ││ Worker 2 │
           │          ││          ││          │
           │ lancedb  ││ lancedb  ││ lancedb  │
           │ Table API││ Table API││ Table API│
           └────┬─────┘└────┬─────┘└────┬─────┘
                │           │           │
                ▼           ▼           ▼
           ┌──────────────────────────────────┐
           │    S3 / MinIO / Local Storage     │
           │    Lance tables + indexes         │
           └──────────────────────────────────┘
```

Each Worker uses the **lancedb Table API** directly — vector search, FTS, hybrid, and writes all go through the same battle-tested lancedb code path. LanceForge adds only the distributed coordination layer on top.

## Quick Start

**One command:**

```bash
git clone https://github.com/wangyong9999/lanceforge
cd lanceforge
./demo.sh      # Creates data, starts cluster, runs queries
```

**Or step by step:**

```bash
# Build
cargo build --release -p lance-distributed-coordinator -p lance-distributed-worker

# Start (see demo.sh for full example)
./target/release/lance-worker config.yaml worker_0 50100
./target/release/lance-coordinator config.yaml 50050
```

## Features

### Retrieval

| Feature | Description | Status |
|---------|-------------|--------|
| 🔍 **ANN Vector Search** | IVF_FLAT/HNSW over sharded data, global TopK merge | ✅ Verified |
| 📝 **Full-Text Search** | BM25 via Tantivy, distributed merge | ✅ Verified |
| 🔀 **Hybrid Search** | ANN + FTS with RRF reranking (lancedb native) | ✅ Verified |
| 🏷️ **Filtered Search** | ANN + SQL WHERE with smart index/brute-force selection | ✅ Verified |
| 🧬 **Multi-vector** | ColBERT late-interaction (lancedb native) | ✅ Supported |
| ✏️ **Sparse / BM25** | Keyword-based retrieval via FTS | ✅ Supported |

### Data Management

| Feature | Description | Status |
|---------|-------------|--------|
| ➕ **Insert** | Add rows with round-robin shard distribution | ✅ Verified |
| ❌ **Delete** | Filter-based deletion, fan-out to all shards | ✅ Verified |
| 🔄 **Upsert** | merge_insert with conflict resolution | ✅ Verified |
| 🏗️ **CreateTable** | Create table via API → auto-register + worker hot-load | ✅ Verified |
| 📊 **CreateIndex** | Fan-out index creation (IVF_FLAT, BTREE, FTS) | ✅ Verified |
| 📋 **GetSchema** | Column names + types from worker | ✅ Verified |
| 🔢 **CountRows** | Distributed row count across shards | ✅ Verified |
| 🗑️ **DropTable** | Unload shards + clear metadata | ✅ Verified |
| 📑 **ListTables** | All tables in cluster | ✅ Verified |

### Infrastructure

| Feature | Description | Status |
|---------|-------------|--------|
| 🔐 **API Key Auth** | Bearer token authentication | ✅ |
| 🔒 **TLS** | Server certificate loading (coordinator + worker) | ✅ |
| 📊 **Prometheus Metrics** | /metrics endpoint with query count, latency, errors | ✅ |
| 💚 **Health Check** | /healthz for load balancers and K8s | ✅ |
| 🛡️ **Backpressure** | Semaphore-based admission control (max 200 concurrent) | ✅ |
| 🔄 **Failover** | Primary/secondary executor routing per shard | ✅ |
| ⚡ **Auto-Shard** | Fragment-based automatic shard discovery | ✅ |
| 📝 **Structured Logging** | JSON (production) / pretty (dev) via tracing | ✅ |

## Python SDK

```bash
pip install grpcio pyarrow  # dependencies
```

```python
from lanceforge import LanceForgeClient

client = LanceForgeClient("localhost:50050")

# Create table
client.create_table("products", initial_data, index_column="vector")

# Vector search
results = client.search("products", query_vector=[0.1, 0.2, ...], k=10,
                         filter="category = 'electronics'")

# Full-text search
results = client.text_search("products", "wireless headphones", k=10)

# Hybrid search (ANN + FTS with RRF fusion)
results = client.hybrid_search("products", query_vector=[...],
                                query_text="headphones", k=10)

# Write operations
client.insert("products", new_data)
client.upsert("products", data, on_columns=["id"])
client.delete("products", filter="category = 'discontinued'")

# DDL
client.create_index("products", "description", index_type="INVERTED")
schema = client.list_tables()
client.drop_table("old_table")
```

### LangChain Integration

```python
from lanceforge.langchain import LanceForgeVectorStore

store = LanceForgeVectorStore(
    host="localhost:50050",
    table_name="documents",
    embedding=OpenAIEmbeddings(),
)
docs = store.similarity_search("retrieval augmented generation", k=5)
```

## Performance

### Recall & Throughput (200K×128d, 3 shards, local SSD)

```
Scenario              Recall@10    QPS      P50 Latency
──────────────────    ─────────    ───      ───────────
Unfiltered ANN        1.0000      129      7.7 ms
Filtered ANN (10%)    1.0000      181      5.3 ms
High-dim 1536d        1.0000       37      22  ms
1M scale (5 shards)   0.9995       40      23  ms
Concurrent (50)       —            48      14  ms
```

### vs Qdrant (Same Machine, Same Dataset)

```
Scenario          Qdrant    LanceForge    Difference
────────          ──────    ──────────    ──────────
Unfiltered QPS       79          106      +34% faster ✅
Filtered QPS        107           89      comparable
Concurrent QPS       12           47      +4x faster  ✅
Recall@10           1.0          1.0      equal
```

## Cluster Configuration

```yaml
tables:
  - name: products
    shards:
      - name: products_shard_00
        uri: "s3://bucket/products/shard_00.lance"
        executors: ["worker_0"]
      - name: products_shard_01
        uri: "s3://bucket/products/shard_01.lance"
        executors: ["worker_1"]

executors:
  - id: worker_0
    host: "10.0.1.1"
    port: 50100

storage_options:
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  aws_endpoint: "http://minio:9000"

# Optional
security:
  api_keys: ["your-key"]
  tls_cert: "/path/to/cert.pem"
  tls_key: "/path/to/key.pem"
```

## Crate Structure

```
crates/
├── proto/          gRPC service definitions (15 RPCs)
├── common/         Config, shard state, auto-shard, metrics, IPC
├── coordinator/    Scatter-gather, merge, connection pool, auth, REST
└── worker/         lancedb Table query execution, caching, dynamic loading

lance-integration/
├── tests/          29 Rust integration tests
├── tools/          E2E tests + Python tools
├── bench/          9 benchmark suites + regression runner
└── sdk/python/     Python SDK + LangChain adapter
```

## Testing

```bash
# Unit tests (99 across 4 crates)
cargo test -p lance-distributed-common --lib
cargo test -p lance-distributed-coordinator --lib
cargo test -p lance-distributed-worker --lib

# Full capability verification (21 tests)
cd lance-integration/tools
python3 e2e_full_capability_test.py

# Benchmark regression suite
cd lance-integration/bench
python3 run_all.py --quick   # 4 suites, ~2 min
python3 run_all.py           # 9 suites, ~30 min

# One-command demo
./demo.sh
```

## Docker

```bash
docker compose -f lance-integration/docker-compose.yaml up
# Generates sample data automatically, starts coordinator + 2 workers
# gRPC: localhost:50050 | REST: localhost:50051
```

## Comparison

|  | LanceForge | LanceDB Cloud | Milvus | Qdrant |
|---|:---:|:---:|:---:|:---:|
| **Open source** | ✅ Apache-2.0 | ❌ Proprietary | ✅ Apache-2.0 | ✅ Apache-2.0 |
| **Data format** | Lance (open) | Lance (open) | Proprietary | Proprietary |
| **Multimodal storage** | ✅ Native | ✅ Native | ❌ Vectors only | ❌ Vectors only |
| **Dense ANN** | ✅ IVF/HNSW | ✅ | ✅ +GPU | ✅ HNSW |
| **Full-text search** | ✅ Tantivy | ✅ | ✅ BM25 | ✅ |
| **Hybrid (RRF)** | ✅ Native | ✅ | ✅ | ✅ |
| **Multi-vector (ColBERT)** | ✅ | ✅ | ✅ | ✅ |
| **Insert/Delete/Upsert** | ✅ | ✅ | ✅ | ✅ |
| **Python SDK** | ✅ | ✅ | ✅ | ✅ |
| **LangChain** | ✅ | ✅ | ✅ | ✅ |
| **Data versioning** | ✅ Lance native | ✅ | ❌ | ❌ |
| **Lakehouse interop** | ✅ DuckDB/Spark | ✅ DuckDB | ❌ | ❌ |
| **Embedded → distributed** | ✅ Same files | ❌ Migration | ❌ N/A | ❌ N/A |

## Roadmap

- [x] ~~Distributed ANN/FTS/Hybrid search~~
- [x] ~~Insert/Delete/Upsert via gRPC~~
- [x] ~~CreateTable/CreateIndex/DropTable DDL~~
- [x] ~~Python SDK + LangChain~~
- [x] ~~TLS support~~
- [x] ~~Prometheus metrics + health checks~~
- [x] ~~Benchmark regression CI~~
- [ ] Kubernetes Helm chart
- [ ] RBAC (role-based access control)
- [ ] SQL query path via DuckDB/Ballista
- [ ] True sparse vector index (SPLADE/BGE-M3)
- [ ] CDC / incremental sync
- [ ] GPU-accelerated index search

## License

Apache License 2.0
