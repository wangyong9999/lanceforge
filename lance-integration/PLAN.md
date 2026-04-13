# LanceForge Development Plan

## Archived (Verified Complete)

### Phase 1-4: MVP → Architecture → Hardening → Critical Gaps
### Phase 5: Core Retrieval + SDK + Benchmarks
  - ANN/FTS/Hybrid/Filtered search — E2E verified, recall=1.0
  - Insert/Delete/Upsert — E2E verified
  - Python SDK — search/insert/delete/upsert E2E verified
  - Metrics on all query paths — E2E verified
  - Backpressure (semaphore 200) — implemented
  - Write round-robin — implemented
  - Worker dynamic shard loading (LoadShard RPC) — implemented
  - Benchmark suite (9 suites) + CI integration
  - Competitor baseline vs Qdrant — verified same hardware
  - Real RAG validation (sentence-transformers 384d) — 6/6 PASS
  - Full capability test (14/14 PASS) — in CI

## Current: Phase 6 — Commercial Readiness

Goal: close every gap between "working demo" and "deployable product".

### Step 1: CreateTable E2E — end-to-end create + query [P0]
CreateTable code exists but never verified. Test: create table via SDK →
insert rows → search → verify results.
- Fix: ensure coordinator registers in shard_state + worker loads shard
- Test: add CreateTable test to e2e_full_capability_test.py
- Files: coordinator/service.rs, e2e_full_capability_test.py

### Step 2: CreateIndex real implementation [P0]
Current handler just logs. Implement: fan out to workers to create index
on their local lancedb Table.
- Worker needs CreateIndex RPC (table.create_index via lancedb)
- Coordinator fans out to all workers for the table
- Test: create table → create index → search uses index

### Step 3: DropTable real implementation [P0]
Current handler just logs. Implement: remove from shard_state + tell
workers to unload shard.
- Test: create table → drop → list_tables should not include it

### Step 4: GetSchema + CountRows real implementation [P1]
Current handlers return "not implemented". Implement via worker RPCs.
- GetSchema: coordinator asks worker for table.schema()
- CountRows: fan out to workers, sum counts
- Test: create table → get schema → verify columns

### Step 5: LangChain verification [P1]
LanceForgeVectorStore class exists but never tested.
- Test with actual embedding model (sentence-transformers)
- Add to e2e tests
- Fix any API mismatches

### Step 6: Docker Compose working [P1]
Current docker-compose.yaml references paths that don't exist.
- Create docker-entrypoint.sh that generates sample data
- Test: docker compose up → curl healthz → SDK search works

### Step 7: TLS actual loading [P1]
SecurityConfig has tls_cert/tls_key fields. Server doesn't read them.
- Load certs in coordinator/worker binary
- Test with self-signed cert

### Step 8: Auto-shard E2E verification [P2]
--auto-shard CLI flag exists but never tested.
- Test: create large table → start with --auto-shard → search works

## Phase 7: Scale + Enterprise
- [ ] K8s Helm chart
- [ ] RBAC (collection-level)
- [ ] Sustained load testing (1 hour)
- [ ] 10M+ scale benchmark

## Stats
- Code: ~5,100 lines Rust
- Tests: 99 unit | 29 integration | 14 capability | 12 enterprise | 6 write | 6 RAG
- CI: unit → integration → capability → benchmark regression
