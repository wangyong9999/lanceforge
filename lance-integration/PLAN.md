# LanceForge Development Plan

## Completed (Archived)

### Phase 1-2: MVP + Crate Architecture
### Phase 3: Commercial Quality Hardening
### Phase 3.5: lancedb Table API Migration
### Phase 4: Critical Capability Gaps (filtered recall, write path, auto-shard)

### Phase 5 P0 (Partial)
- [x] Write E2E test: insert → search → delete → verify (6/6 PASS)
- [x] Metrics: ann_search latency/count instrumented
- [x] /metrics HTTP endpoint on coordinator port+1
- [ ] ~~Write throughput benchmark~~ (deferred — correctness verified, throughput not priority)
- [ ] Metrics: fts_search/hybrid_search/write paths not yet instrumented

## Current: Phase 5 — Developer Experience + Completion

### Step 1: Complete metrics instrumentation (30 min)
Metrics record_query only wired in ann_search. Add to fts_search, hybrid_search.
- File: `crates/coordinator/src/service.rs`
- Verify: unit tests pass, /metrics shows counts after FTS/hybrid queries

### Step 2: Python SDK — lanceforge-client (1 day)
pip-installable client wrapping gRPC. NOT reimplementing lancedb Python — this is
a thin distributed client that talks to LanceForge coordinator.
- File: `lance-integration/sdk/python/lanceforge/__init__.py`
- API: `LanceForgeClient(host).search(table, vector, k)`, `.insert(table, data)`, `.delete(table, filter)`
- Reuse: generated proto stubs (lance_service_pb2)
- Verify: pytest against running cluster

### Step 3: LangChain VectorStore adapter (2 hours)
LangChain VectorStore subclass that delegates to lanceforge-client.
- File: `lance-integration/sdk/python/lanceforge/langchain.py`
- Class: `LanceForgeVectorStore(VectorStore)` with `similarity_search()`, `add_texts()`
- Reuse: lanceforge-client from Step 2
- Verify: LangChain retriever test

### Step 4: REST API via Axum (1 day)
Lightweight HTTP server alongside gRPC, same coordinator process.
- File: `crates/coordinator/src/rest.rs` (new module)
- Routes: `POST /v1/search`, `POST /v1/insert`, `POST /v1/delete`, `GET /v1/status`
- Reuse: CoordinatorService internals (same scatter_gather, same pool)
- NOT a separate binary — runs in coordinator alongside gRPC
- Verify: curl tests

### Step 5: HNSW index support (2 hours)
lancedb already supports IVF_HNSW_SQ. Just need to:
- Update shard_splitter.py default index type
- Add index_type config option to ClusterConfig
- Benchmark: IVF_FLAT vs IVF_HNSW_SQ recall-QPS tradeoff
- Note: for unfiltered queries IVF_FLAT recall=1.0 already; HNSW helps large datasets

### Step 6: Reranker extension (Cohere/CrossEncoder) (2 hours)
lancedb already has Cohere and CrossEncoder rerankers. Expose via config.
- Add reranker config to ClusterConfig (type: rrf|cohere|cross_encoder)
- Wire into coordinator scatter_gather merge phase
- Reuse: lancedb::rerankers module

## Phase 6: Production Operations
- [ ] K8s Helm chart
- [ ] TLS actual loading
- [ ] RBAC (collection-level)
- [ ] Graceful shutdown (CancellationToken)

## Phase 7: Differentiation
- [ ] SQL via DuckDB Lance extension
- [ ] Sparse Vector support
- [ ] Multi-vector / ColBERT
- [ ] CDC, hot/cold storage

## Stats
- Unit tests: 98 | Integration: 29 | E2E: 10+6 | Benchmarks: 8 suites
