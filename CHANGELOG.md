# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-16

First public release.

### Added
- Distributed scatter-gather query engine over sharded Lance tables
- ANN vector search (IVF_FLAT/HNSW) with global TopK merge
- Full-text search (BM25 via Tantivy) with distributed merge
- Hybrid search (ANN + FTS) with RRF reranking
- Filtered search with scalar index acceleration
- Insert / Delete / Upsert via gRPC with shard-level routing
- CreateTable with auto-sharding across all healthy workers
- CreateIndex fan-out (IVF_FLAT, BTREE, INVERTED)
- DropTable with storage purge and metadata cleanup
- Size-aware Rebalance and manual MoveShard
- Python SDK (`pip install lanceforge`) with 16 methods
- LangChain VectorStore adapter
- API Key authentication with RBAC (Admin/ReadWrite/ReadOnly)
- TLS support (coordinator + worker mTLS)
- Prometheus metrics with latency histogram (P50/P95/P99)
- Health check endpoint (`/healthz`)
- REST status endpoint (`/v1/status`)
- Metadata persistence via MetaStore (file / S3) — survives restarts
- Graceful shutdown with cooperative background task lifecycle
- Query cache with byte-level memory cap (default 512MB)
- Config runtime validation on startup
- Kubernetes Helm chart with PDB, HPA, health probes
- Docker Compose one-command deployment
- Demo script (`./demo.sh`) for 5-minute local trial
- 272 unit tests + 27 SDK tests + 21 E2E test suites + 9 benchmark suites
