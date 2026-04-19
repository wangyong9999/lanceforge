<p align="center">
  <h1 align="center">LanceForge</h1>
  <p align="center">
    <b>Distributed Multimodal Retrieval Engine for Lance Tables</b>
    <br/>
    Horizontal scaling for vector search, full-text search, and hybrid retrieval
  </p>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#architecture">Architecture</a> &middot;
  <a href="#features">Features</a> &middot;
  <a href="#python-sdk">SDK</a> &middot;
  <a href="#performance">Performance</a> &middot;
  <a href="#deployment">Deployment</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange" alt="Rust"/>
  <img src="https://img.shields.io/badge/license-Apache%202.0-blue" alt="License"/>
  <img src="https://img.shields.io/badge/tests-311%20unit%20%7C%2029%20integration%20%7C%2021%20E2E-green" alt="Tests"/>
  <img src="https://img.shields.io/badge/recall%4010-1.0000-brightgreen" alt="Recall"/>
</p>

---

## What is LanceForge?

LanceForge extends [LanceDB](https://github.com/lancedb/lancedb) from an **embedded single-node** database to a **horizontally scalable distributed** query engine -- without changing your data format.

```
Single-node LanceDB            LanceForge (distributed)

+----------+                   +--------------------------------+
|  App     |                   |  App                           |
|    |     |                   |    | gRPC / Python SDK         |
| LanceDB  |   --------->     |  Coordinator (scatter-gather)  |
|    |     |   same .lance     |    |         |         |       |
|  .lance  |     files         |  Worker 0  Worker 1  Worker 2 |
+----------+                   |    |         |         |       |
                               |    S3 / MinIO / Local FS      |
                               +--------------------------------+
```

**Same Lance files. Same queries. Horizontal throughput.**

---

## Quick Start

### Option A: One-Command Demo (local, no Docker)

```bash
git clone https://github.com/wangyong9999/lanceforge && cd lanceforge
./demo.sh           # builds, creates sample data, starts cluster, runs queries
./demo.sh clean     # tear down
```

### Option B: Docker Compose (recommended)

```bash
cd lance-integration
docker compose up    # 1 coordinator + 2 workers + sample data
# gRPC endpoint:  localhost:50050
# REST endpoint:  localhost:50051
```

### Option C: Kubernetes (Helm)

```bash
helm install lanceforge deploy/helm/lanceforge/ \
  --set storage.s3Bucket=my-bucket \
  --set storage.s3Endpoint=http://minio:9000
```

Includes: resource limits, PodDisruptionBudget, HPA (opt-in), health probes.

### Option D: Manual

```bash
cargo build --release -p lance-distributed-coordinator -p lance-distributed-worker
./target/release/lance-worker   config.yaml worker_0 50100 &
./target/release/lance-worker   config.yaml worker_1 50101 &
./target/release/lance-coordinator config.yaml 50050 &
```

---

## Architecture

```
                  +------------------------------------------+
                  |             Coordinator                   |
 Client --gRPC-->|                                          |
 Python SDK      |  [Scatter] --> [Merge TopK] --> [Return] |
 REST API        |      |     parallel fan-out              |
                  +------+----------------------------------+
                         |
              +----------+----------+
              |          |          |
              v          v          v
         +--------+ +--------+ +--------+
         |Worker 0| |Worker 1| |Worker 2|
         |        | |        | |        |
         |lancedb | |lancedb | |lancedb |
         |Table   | |Table   | |Table   |
         +---+----+ +---+----+ +---+----+
             |           |           |
             v           v           v
         +------------------------------------+
         |   S3 / MinIO / Local Storage       |
         |   Lance tables + indexes           |
         +------------------------------------+
```

Each Worker uses the **lancedb Table API** directly -- vector search, FTS, hybrid, and writes all go through the same battle-tested lancedb code path. LanceForge adds only the distributed coordination layer on top.

---

## Features

### Retrieval

| Feature | Description | Status |
|---------|-------------|--------|
| **ANN Vector Search** | IVF_FLAT/HNSW over sharded data, global TopK merge | Verified |
| **Full-Text Search** | BM25 via Tantivy, distributed merge | Verified |
| **Hybrid Search** | ANN + FTS with RRF reranking (lancedb native) | Verified |
| **Filtered Search** | ANN + SQL WHERE with scalar index acceleration | Verified |
| **Multi-vector** | ColBERT late-interaction (lancedb native) | Supported |
| **Sparse / BM25** | Keyword-based retrieval via FTS | Supported |

### Data Management

| Feature | Description | Status |
|---------|-------------|--------|
| **Insert** | Add rows with round-robin shard distribution | Verified |
| **Delete** | Filter-based deletion, fan-out to all shards | Verified |
| **Upsert** | merge_insert with hash-partitioned routing | Verified |
| **CreateTable** | Auto-shard across all workers (no manual splitting) | Verified |
| **CreateIndex** | Fan-out index creation (IVF_FLAT, BTREE, FTS) | Verified |
| **DropTable** | Unload shards + purge storage + clear metadata | Verified |
| **Rebalance** | Size-aware shard redistribution | Verified |
| **MoveShard** | Manual shard migration between workers | Verified |

### Infrastructure

| Feature | Description |
|---------|-------------|
| **API Key Auth + RBAC** | Bearer token authentication with three-role permissions (Read / Write / Admin) |
| **Namespace Binding** *(0.2-beta)* | API key → namespace (`{ns}/` table prefix check + ListTables filter). Minimum-viable multi-tenant; see `docs/SECURITY.md` §5 |
| **Per-Key QPS Quota** *(0.2-beta)* | Token-bucket rate limit per API key; exceeding the cap returns `ResourceExhausted` |
| **Persistent Audit Log** *(0.2-beta)* | Every DDL / write RPC is appended to a JSONL file (local; OBS path tracked for 0.3) with principal, target, trace_id |
| **TLS** | mTLS between coordinator and workers |
| **Encryption at Rest** *(0.2-beta)* | Documentation + config pattern for SSE-KMS / SSE-S3 / SSE-C; see `docs/SECURITY.md` §6 |
| **Traceparent Pass-Through** *(0.2-beta)* | W3C `traceparent` header parsed and correlated into the audit line + JSONL record for cross-process debugging |
| **Prometheus Metrics** | `/metrics` with latency histogram (P50/P95/P99), per-table counters |
| **Health Check** | `/healthz` for load balancers and K8s probes |
| **Backpressure** | Semaphore-based admission control (configurable max concurrent) |
| **Failover** | Primary/secondary executor routing per shard |
| **Read/Write Split** *(0.2)* | Soft role preference per worker — isolates query traffic from write load (see `docs/LIMITATIONS.md` §11 for the recommended config) |
| **Auto-Shard** | CreateTable automatically distributes data across all workers |
| **Metadata Persistence** | Shard state + URIs survive coordinator restart (file / S3) |
| **Runtime Key Rotation** *(0.2)* | API key registry lives in MetaStore and hot-reloads every 60 s — no coordinator restart required |
| **SaaS Deployment Guard** *(0.2)* | `deployment_profile: saas` rejects any local-disk MetaStore at startup so pod replacement stays stateless |
| **Schema Versioning** *(0.2)* | MetaStore snapshots carry `schema_version`; older versions auto-upgrade on write, future versions fail closed |
| **`lance-admin` CLI** *(0.2-beta)* | Operator tool: MetaStore dump / restore / list + shard-URI inspection. Data copies use cloud-native tooling (`aws s3 sync`) |
| **Structured Logging** | JSON (production) / pretty (dev) via tracing |
| **Graceful Shutdown** | All background tasks cooperatively stop on SIGTERM |

---

## Python SDK

```bash
# Install from GitHub (recommended)
pip install git+https://github.com/wangyong9999/lanceforge.git#subdirectory=lance-integration/sdk/python

# Or from local clone
cd lance-integration/sdk/python && pip install .
```

```python
import numpy as np
import pyarrow as pa
from lanceforge import LanceForgeClient

client = LanceForgeClient("localhost:50050",
    api_key="your-key",         # optional
    default_timeout=30,         # seconds
    max_retries=3,              # exponential backoff on transient errors
)

# Create table (auto-shards across all workers)
data = pa.table({
    'id':       pa.array(range(10000), type=pa.int64()),
    'text':     pa.array([f'document {i}' for i in range(10000)]),
    'category': pa.array(['tech', 'science', 'health', 'biz', 'sports'] * 2000),
    'vector':   pa.FixedSizeListArray.from_arrays(
                    pa.array(np.random.randn(10000 * 128).astype(np.float32)),
                    list_size=128),
})
client.create_table("docs", data, index_column="vector")

# Vector search
results = client.search("docs", query_vector=np.random.randn(128).tolist(),
                         k=10, filter="category = 'tech'")

# Full-text search
results = client.text_search("docs", "document", k=10, text_column="text")

# Hybrid ANN + FTS with RRF fusion
results = client.hybrid_search("docs",
    query_vector=np.random.randn(128).tolist(),
    query_text="document", k=10)

# Write operations
client.insert("docs", new_data)
client.upsert("docs", data, on_columns=["id"])
client.delete("docs", filter="id > 9000")

# Admin
print(client.status())             # cluster health
print(client.count_rows("docs"))   # distributed row count
client.rebalance()                 # re-distribute shards
client.create_index("docs", "text", index_type="INVERTED")
client.drop_table("docs")
client.close()
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

### Supported Query Types

| Type | SDK Method | Description |
|------|-----------|-------------|
| Dense ANN | `search()` | IVF_FLAT / HNSW vector similarity |
| Full-text | `text_search()` | BM25 via Tantivy inverted index |
| Hybrid | `hybrid_search()` | ANN + FTS with RRF reranking |
| Filtered | `search(filter=...)` | ANN with SQL WHERE predicate |
| Sparse / BM25 | `sparse_search()` | Alias for FTS (SPLADE/BGE-M3 compatible) |

---

## REST API

```bash
# Health check (for load balancers / K8s)
curl http://localhost:50051/healthz
# {"status":"ok"}

# Prometheus metrics (histogram, per-table, latency)
curl http://localhost:50051/metrics

# Cluster status
curl http://localhost:50051/v1/status
# {"query_count":1234,"query_errors":0,"healthy":true}
```

> Search endpoints use gRPC (port 50050). Use the Python SDK or any gRPC client.

---

## Performance

### Recall & Throughput (200K x 128d, 3 shards, local SSD)

```
Scenario              Recall@10    QPS      P50 Latency
--------------------------------------------------------------
Unfiltered ANN        1.0000      129      7.7 ms
Filtered ANN (10%)    1.0000      181      5.3 ms
High-dim 1536d        1.0000       37      22  ms
1M scale (5 shards)   0.9995       40      23  ms
Concurrent (50)       --           48      14  ms
```

### vs Qdrant (Same Machine, Same Dataset)

```
Scenario          Qdrant    LanceForge    Difference
--------------------------------------------------------------
Unfiltered QPS       79          106      +34% faster
Filtered QPS        107           89      comparable
Concurrent QPS       12           47      +4x faster
Recall@10           1.0          1.0      equal
```

---

## Deployment

### Cluster Configuration

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
  - id: worker_1
    host: "10.0.1.2"
    port: 50100

storage_options:
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  aws_endpoint: "http://minio:9000"

# Or: use auto-sharding (no table/shard config needed)
default_table_path: "s3://bucket/tables"
metadata_path: "s3://bucket/metadata.json"

security:
  api_keys: ["your-key"]
  tls_cert: "/path/to/cert.pem"
  tls_key: "/path/to/key.pem"
```

### Production Checklist

- [ ] Set `metadata_path` (S3 or local) for state persistence across restarts
- [ ] Set `default_table_path` to S3 for durable runtime-created tables
- [ ] Enable TLS (`security.tls_cert` / `security.tls_key`)
- [ ] Configure API keys (`security.api_keys`)
- [ ] Set `replica_factor: 2` for read failover
- [ ] Monitor `/metrics` endpoint with Prometheus + Grafana
- [ ] Enable orphan GC (`orphan_gc.enabled: true`) if using DropTable frequently
- [ ] Tune `cache.max_cache_bytes` (default 512 MB per worker)
- [ ] Set `server.slow_query_ms` for slow query alerting (default 1000 ms)

---

## Crate Structure

```
crates/
  proto/          gRPC service definitions (17 RPCs, 2 services)
  common/         Config, shard state, auto-shard, metrics, IPC
  coordinator/    Scatter-gather, merge, connection pool, auth, REST
  worker/         lancedb Table execution, caching, dynamic shard loading
  meta/           MetaStore (file / S3 / etcd), shard manager, session

lance-integration/
  tests/          29 Rust integration tests
  tools/          21 E2E test suites + Python tools
  bench/          9 benchmark suites + regression runner
  sdk/python/     Python SDK (gRPC) + LangChain adapter

deploy/
  helm/           Kubernetes Helm chart (PDB, HPA, health probes)
```

## Testing

```bash
# Unit tests (272 across 5 crates)
cargo test --workspace --lib

# E2E: auto-shard + write dedup + rebalance (7 tests)
cd lance-integration/tools && python3 e2e_phase11_test.py

# Full capability verification (21 tests)
python3 e2e_full_capability_test.py

# HA: worker kill/restart, coordinator restart (7 tests)
python3 e2e_ha_test.py

# Benchmark regression
cd lance-integration/bench && python3 run_all.py --ci
```

## Comparison

|  | LanceForge | LanceDB Cloud | Milvus | Qdrant |
|---|:---:|:---:|:---:|:---:|
| **Open source** | Apache-2.0 | Proprietary | Apache-2.0 | Apache-2.0 |
| **Data format** | Lance (open) | Lance (open) | Proprietary | Proprietary |
| **Multimodal storage** | Native | Native | Vectors only | Vectors only |
| **Dense ANN** | IVF/HNSW | IVF/HNSW | IVF/HNSW+GPU | HNSW |
| **Full-text search** | Tantivy | Tantivy | BM25 | BM25 |
| **Hybrid (RRF)** | Native | Native | Manual | Manual |
| **Multi-vector (ColBERT)** | Native | Native | Partial | Partial |
| **Auto-sharding** | CreateTable | Managed | Collection | Collection |
| **Data versioning** | Lance native | Lance native | -- | -- |
| **Lakehouse interop** | DuckDB/Spark | DuckDB | -- | -- |
| **Embedded to distributed** | Same files | Migration | N/A | N/A |

## Roadmap

- [x] Distributed ANN / FTS / Hybrid search
- [x] Insert / Delete / Upsert via gRPC
- [x] CreateTable / CreateIndex / DropTable DDL
- [x] Auto-sharding (CreateTable splits across workers)
- [x] Python SDK + LangChain integration
- [x] TLS + API Key Auth + RBAC
- [x] Prometheus metrics (histogram) + health checks
- [x] Kubernetes Helm chart (PDB, HPA, health probes)
- [x] Size-aware rebalance + MoveShard
- [x] Metadata persistence (survives restarts)
- [x] Graceful shutdown + background task lifecycle
- [ ] SQL query path via DuckDB/Ballista
- [ ] Native sparse vector index (SPLADE/BGE-M3)
- [ ] CDC / incremental sync from data lake
- [ ] GPU-accelerated index search

## License

Apache License 2.0
