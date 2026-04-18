# LanceForge Development Plan

## Completed (Phase 1-7)

Phase 1-6: MVP → Architecture → Hardening → Capability → DX → DDL
Phase 7: HA (meta crate, MetaStore CAS, SessionManager, ShardManager,
  MetaShardState, read replicas, rebalance, compact, worker register)
  HA E2E: 8/8 (restart survival, worker failure/recovery, lifecycle)

## Current: Phase 8 — Validate Everything Real

No new features. Verify every "implemented but untested" capability.

### Step 1: Read Replica E2E
Two workers load same shard → search distributes across both →
kill one → search still works → restart → both serving again.

### Step 2: Rebalance E2E
Start 2 workers → add 3rd worker → trigger rebalance →
verify shards redistributed → new worker serves queries.

### Step 3: TLS E2E
Generate self-signed cert → configure tls_cert/tls_key →
start cluster with TLS → SDK connects with TLS → search works.

### Step 4: Compact E2E
Insert 100 small batches (1 row each) → compact → verify
fragment count reduced → search still works + faster.

### Step 5: Complete CI Pipeline
Add to run_all.py --ci:
  - HA test (8 tests)
  - Full capability (21 tests)
  - Recall benchmark
  - Filtered benchmark
Verify: run_all.py --ci passes end-to-end.

## Phase 9: Production Deployment
- [ ] S3 MetaStore (replace FileMetaStore for multi-node)
- [ ] K8s Helm chart
- [ ] RBAC
- [ ] 10M+ scale validation
- [ ] Sustained load test (1 hour)

## Stats
- Code: ~6,500 lines Rust (5 crates)
- Tests: 125 unit | 29 integration | 21 capability | 8 HA | 12 enterprise
- CI: unit → integration → HA → capability → benchmark
