# LanceForge Development Plan

## Completed

### Phase 1: MVP (Distributed Retrieval Core)
- Scatter-gather distributed ANN/FTS/Hybrid search
- Multi-table, multi-shard support
- gRPC coordinator↔worker communication
- Shard pruning with column statistics
- Two-phase shard state (current/next target with readiness gate)
- Connection pool with health check and auto-reconnect
- Mock/OpenAI/SentenceTransformers embedding for text queries
- Reciprocal Rank Fusion (RRF) via lancedb reranker
- RefreshableDataset for background data freshness
- Query cache with dataset-version coherence
- Multi-Active Coordinator HA

### Phase 2: Crate Architecture
- Split into 4 independent crates: proto, common, coordinator, worker
- Dedicated lance-coordinator and lance-worker binaries
- Integration tests migrated to new crate APIs
- Legacy service.rs removed

### Phase 3: Commercial Quality Hardening
- Eliminated all production unwrap() calls (proper error handling)
- Unit tests for all core modules (90 tests total)
- gRPC connection stability (tcp_nodelay, http2_adaptive_window, keepalive)
- CI pipelines for 4 crates + integration tests + MinIO E2E
- ASan/TSan sanitizer CI workflows
- TLS config + API key authentication
- Externalized configuration (ServerConfig, CacheConfig, HealthCheckConfig)
- Dockerfile HEALTHCHECK + non-root user
- Structured logging via tracing-subscriber (JSON/pretty)
- Performance regression tests for merge and shard pruning

## Next

### Phase 4: Production Deployment
- [ ] etcd-backed ShardState (replace static YAML config)
- [ ] TLS encryption for coordinator↔worker channels
- [ ] Dynamic shard splitting and rebalancing
- [ ] Kubernetes operator with auto-scaling
- [ ] Prometheus metrics scrape endpoint

### Phase 5: SQL Path
- [ ] Ballista/DataFusion SQL query integration
- [ ] SQL over distributed Lance tables
- [ ] Spark/Trino connector compatibility
