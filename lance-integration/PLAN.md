# LanceForge Development Plan

## Completed

### Phase 1: MVP (Distributed Retrieval Core)
- Scatter-gather distributed ANN/FTS/Hybrid search
- Multi-table, multi-shard support with gRPC
- Shard pruning, two-phase shard state, failover
- Connection pool with health check and auto-reconnect
- Embedding (mock/OpenAI/SentenceTransformers), RRF reranking
- Query cache with dataset-version coherence
- Multi-Active Coordinator HA

### Phase 2: Crate Architecture
- 4 independent crates: proto, common, coordinator, worker
- Dedicated lance-coordinator and lance-worker binaries
- Legacy service.rs removed

### Phase 3: Commercial Quality Hardening
- Production unwrap() eliminated, core module unit tests
- gRPC stability (tcp_nodelay, http2_adaptive_window, keepalive)
- CI + ASan/TSan, TLS config + API key auth
- Externalized config, Dockerfile, structured logging
- Input validation, global timeout, performance regression tests

### Phase 3.5: lancedb Table API Migration
- Worker query path: lance::Scanner → lancedb::Table API
- ANN/FTS/Hybrid all via lancedb native API
- Zero architectural duplication (audit confirmed)

### Phase 4: Critical Capability Gaps
- Filtered ANN recall 0.60 → 1.0000 (bypass_vector_index)
- Write path end-to-end (AddRows/DeleteRows gRPC → lancedb Table API)
- Auto-sharding fragment discovery + CLI integration

### Benchmarks Verified
- SIFT-128 L2: recall=1.0, QPS=175 @ nprobes=5
- GloVe-100 Cosine: recall=1.0, QPS=211 @ nprobes=5
- High-dim 768d/1536d: recall=1.0
- 1M scale: recall=0.9995, QPS=40
- Filtered 10%: recall=1.0000
- Shard scaling 1→10: recall stable 0.999

## Next: Phase 5 — Production Readiness

### P0: Write Path Verification + Metrics
- [ ] E2E test: insert → search → delete → verify
- [ ] Write throughput benchmark
- [ ] Wire metrics.rs into query/write paths
- [ ] Expose /metrics HTTP endpoint

### P1: Developer Experience
- [ ] REST API (Axum alongside gRPC)
- [ ] Python SDK (pip install lanceforge-client)
- [ ] LangChain VectorStore adapter

### P1: Kubernetes
- [ ] Helm chart
- [ ] HPA config

### P2: Advanced
- [ ] RBAC, TLS loading, Multi-vector/ColBERT
- [ ] Graceful shutdown (CancellationToken)

## Phase 6: Differentiation
- [ ] SQL via Ballista/DataFusion
- [ ] DuckDB/Spark/Trino connectors
- [ ] CDC, hot/cold storage, GPU index

## Stats
- Unit tests: 98 | Integration: 29 | E2E: 10 | Benchmarks: 8 suites
