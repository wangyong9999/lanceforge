# LanceForge Development Plan

## Completed

### Phase 1-4: MVP → Architecture → Hardening → Critical Gaps
### Phase 5: Developer Experience
  - Python SDK (search/insert/delete/upsert/status)
  - LangChain VectorStore adapter
  - REST API (/metrics, /healthz, /v1/status)
  - Metrics instrumented on all query paths
  - HNSW/Reranker extensibility via lancedb
### Phase 5.5: Core Retrieval Parity
  - Filtered ANN QPS: 12.5 → 181 (surpasses Qdrant on same hardware)
  - Upsert (merge_insert) via lancedb
  - Multi-vector (ColBERT) via lancedb native
  - Sparse Vector API (via FTS/BM25)
  - Backpressure: semaphore-based query admission control (max 200)
  - Delete row count fix (pre/post count instead of placeholder)
  - Benchmark regression suite (run_all.py) + CI integration
  - Competitor baseline: LanceForge vs Qdrant verified on same hardware

### Benchmarks (Verified)
  - SIFT-128 L2: recall=1.0, QPS=175 @ nprobes=5
  - GloVe-100 Cosine: recall=1.0, QPS=211 @ nprobes=5
  - High-dim 768d/1536d: recall=1.0
  - 1M scale: recall=0.9995, QPS=40
  - Filtered 10%: recall=1.0, QPS=181
  - vs Qdrant: unfiltered +34%, concurrent +4.1x

## Next: Phase 6 — Production Reliability

### P0: Data Durability
- [ ] Write-ahead intent log (crash recovery for in-flight writes)
- [ ] Periodic checkpoint of shard state
- [ ] Recovery test: kill worker mid-write, verify data consistency

### P0: Dynamic Cluster Management
- [ ] Worker online registration (join without restart)
- [ ] Automatic shard rebalance on node join/leave
- [ ] Health-triggered failover with state transfer

### P1: Real-World Validation
- [ ] RAG pipeline E2E: LangChain + OpenAI embeddings + real documents
- [ ] Production-like load test: sustained 100+ QPS for 1 hour
- [ ] Memory leak detection (long-running stability)

## Phase 7: Enterprise Features
- [ ] RBAC (collection-level roles)
- [ ] TLS actual loading
- [ ] Multi-tenancy (namespace isolation)
- [ ] Audit logging

## Phase 8: Differentiation
- [ ] SQL via DuckDB Lance extension
- [ ] True sparse vector index (learned embeddings)
- [ ] Kubernetes Operator
- [ ] CDC / incremental sync

## Stats
- Code: 5,063 lines Rust + 2,561 integration tests + 3,000 bench/tools
- Tests: 99 unit | 29 integration | 28 E2E | 9 benchmark suites
- CI: unit + integration + benchmark regression + ASan/TSan
