# LanceForge

**Distributed multimodal retrieval engine for Lance tables.**

LanceForge extends [LanceDB](https://github.com/lancedb/lancedb) from an embedded single-node database to a horizontally scalable distributed query engine. It supports ANN vector search, full-text search, and hybrid retrieval over Lance tables stored on S3/MinIO or local storage.

## Architecture

```
                              ┌─────────────────────────────────────┐
                              │         Coordinator                  │
  Client ──── gRPC ──────────►│  ● Scatter queries to workers       │
  (Python/Rust/gRPC)          │  ● Shard-aware routing + pruning    │
                              │  ● Merge global TopK results        │
                              │  ● Text → vector embedding          │
                              │  ● API key authentication           │
                              └──────────┬──────────────────────────┘
                                         │ gRPC (parallel fan-out)
                       ┌─────────────────┼─────────────────┐
                       ▼                 ▼                  ▼
                ┌────────────┐   ┌────────────┐   ┌────────────┐
                │  Worker 0  │   │  Worker 1  │   │  Worker 2  │
                │  shard 0,1 │   │  shard 2,3 │   │  shard 4,5 │
                │  Lance+DF  │   │  Lance+DF  │   │  Lance+DF  │
                └──────┬─────┘   └──────┬─────┘   └──────┬─────┘
                       └────────────────┼─────────────────┘
                                        ▼
                              S3 / MinIO / Local FS
                              (Lance tables + IVF_PQ indexes)
```

Each Worker runs the same DataFusion + Lance query path as single-node LanceDB — no custom engine code, no data format conversion.

## Key Features

- **Distributed ANN Search** — Scatter-gather over sharded Lance tables with IVF_PQ/HNSW indexes
- **Full-Text Search** — BM25-based FTS with distributed merge
- **Hybrid Search** — ANN + FTS combined via Reciprocal Rank Fusion (RRF)
- **Multi-Table** — Query different tables with different schemas and dimensions
- **Text Query** — Auto-embed text to vectors using OpenAI or SentenceTransformers
- **Shard Pruning** — Skip irrelevant shards based on column statistics
- **Two-Phase Shard Management** — Atomic shard reassignment with readiness gates
- **Failover** — Primary/secondary executor routing per shard
- **Query Cache** — TTL + LRU cache with dataset-version coherence
- **Data Freshness** — Background dataset version refresh (non-blocking)
- **Multi-Active HA** — All coordinator instances active behind load balancer
- **API Key Auth** — Bearer token authentication via config
- **Structured Logging** — JSON output for production, human-readable for dev

## Quick Start

### Build

```bash
cargo build --release -p lance-distributed-coordinator -p lance-distributed-worker
```

### Create test data

```bash
cd lance-integration/tools
python3 shard_splitter.py /path/to/source.lance ./shards/ --num-shards 3
```

### Start cluster

```bash
# Workers
./target/release/lance-worker cluster_config.yaml worker_0 50100
./target/release/lance-worker cluster_config.yaml worker_1 50101
./target/release/lance-worker cluster_config.yaml worker_2 50102

# Coordinator
./target/release/lance-coordinator cluster_config.yaml 50050
```

### Query

```python
from lance_distributed_client import LanceDistributedClient

client = LanceDistributedClient("localhost:50050")
results = client.search(
    table_name="products",
    query_vector=[0.1, 0.2, ...],  # dim=128
    k=10,
    nprobes=20,
    filter="category = 'electronics'"
)
print(results.to_pandas())
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
  - id: worker_1
    host: "10.0.1.2"
    port: 50101

storage_options:
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  aws_endpoint: "http://minio:9000"
  aws_region: "us-east-1"
  allow_http: "true"

# Optional: text query embedding
embedding:
  provider: "openai"
  model: "text-embedding-3-small"
  dimension: 1536

# Optional: server tuning
server:
  query_timeout_secs: 30
  keepalive_interval_secs: 10
  concurrency_limit: 256

# Optional: API key auth
security:
  api_keys:
    - "your-api-key-here"
```

## Supported Query Types

| Type | Description | Use Case |
|------|-------------|----------|
| **ANN** | IVF_PQ/HNSW vector similarity | Image/embedding search |
| **FTS** | Full-text BM25 search | Document retrieval |
| **Hybrid** | ANN + FTS with RRF reranking | RAG pipelines |
| **Filtered ANN** | ANN + SQL WHERE clause | Faceted vector search |
| **Text Query** | Auto-embed text → ANN | End-user natural language |

## Crate Structure

```
crates/
├── proto/          lance-distributed-proto     gRPC service definitions
├── common/         lance-distributed-common    Config, shard state, metrics, IPC
├── coordinator/    lance-distributed-coordinator   Query routing, scatter-gather, merge
└── worker/         lance-distributed-worker    Lance query execution, caching

lance-integration/  Integration tests, Python tools, Dockerfile
```

| Crate | Unit Tests | Lines |
|-------|-----------|-------|
| proto | — | ~350 |
| common | 34 | ~1800 |
| coordinator | 40 | ~2000 |
| worker | 25 | ~900 |
| **Total** | **99** | **~5050** |

Plus 29 integration tests, 28 E2E tests, and 9 benchmark suites.

## Docker

```bash
# Build
docker compose -f lance-integration/docker-compose.yaml build

# Run (1 coordinator + 3 workers)
docker compose -f lance-integration/docker-compose.yaml up
```

## Testing

```bash
# Unit tests (90 tests across 4 crates)
cargo test -p lance-distributed-common --lib
cargo test -p lance-distributed-coordinator --lib
cargo test -p lance-distributed-worker --lib

# Integration tests (29 tests, serial)
for t in test_distributed_ann test_edge_cases test_data_freshness \
         test_text_query test_multi_table test_grpc_distributed \
         test_shard_routing test_benchmark test_durability test_ha; do
  cargo test -p lanceforge --test $t -- --test-threads=1
done

# Python tests
cd lance-integration/tools
python3 -m pytest test_client.py test_shard_splitter.py -v

# Full system E2E (requires MinIO)
python3 e2e_full_system_test.py
```

## Performance

Benchmarked on 200K×128d vectors, 3 shards (local SSD):

| Scenario | Recall@10 | QPS | P50 |
|----------|-----------|-----|-----|
| Unfiltered ANN | 1.0000 | 129 | 7.7ms |
| Filtered ANN (10%) | 1.0000 | 181 | 5.3ms |
| High-dim 1536d | 1.0000 | 37 | 22ms |
| 1M scale (5 shards) | 0.9995 | 40 | 23ms |
| 50 concurrent | — | 48 | 14ms |

### vs Qdrant (same machine, same dataset)

| Scenario | Qdrant | LanceForge | |
|----------|--------|------------|---|
| Unfiltered QPS | 79 | **106** | +34% faster |
| Filtered QPS | 107 | **89** | Comparable |
| Concurrent QPS | 12 | **47** | 4x faster |
| Recall | 1.0 | 1.0 | Equal |

## Comparison

| | LanceForge | LanceDB Cloud | Milvus | Qdrant |
|---|---|---|---|---|
| Open source | Apache-2.0 | Proprietary | Apache-2.0 | Apache-2.0 |
| Data format | Lance (open) | Lance (open) | Proprietary | Proprietary |
| Multimodal storage | Native | Native | Vectors only | Vectors only |
| Dense ANN | IVF_FLAT/HNSW | IVF/HNSW | IVF/HNSW/GPU | HNSW |
| Hybrid search (RRF) | Built-in | Built-in | Built-in | Fusion API |
| Multi-vector (ColBERT) | Native | Native | Yes | Native |
| Write (insert/delete/upsert) | Yes | Yes | Yes | Yes |
| Python SDK | Yes | Yes | Yes | Yes |
| LangChain | Yes | Yes | Yes | Yes |
| Lakehouse interop | DataFusion/DuckDB | DuckDB | None | None |
| Embedded → distributed | Same .lance format | Migration | N/A | N/A |

## Roadmap

- [ ] Data durability (write-ahead intent log)
- [ ] Dynamic cluster management (online join/leave)
- [ ] RBAC and TLS
- [ ] SQL query path via DuckDB/Ballista
- [ ] Kubernetes operator
- [ ] True sparse vector index (learned embeddings)

## License

Apache License 2.0
