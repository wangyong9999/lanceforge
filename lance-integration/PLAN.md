# LanceForge Development Plan

## Completed (Phase 1-6)

All archived. Key milestones:
- Distributed ANN/FTS/Hybrid retrieval (lancedb Table API)
- Insert/Delete/Upsert, CreateTable/CreateIndex/DropTable DDL
- Python SDK + LangChain + REST + Metrics + TLS
- 21/21 capability tests, 99 unit tests, 9 benchmark suites
- vs Qdrant: unfiltered +34%, concurrent +4x, recall=1.0
- Real RAG validation with sentence-transformers

## Architecture Insight: S3 Changes Everything

Unlike Milvus/Qdrant/Weaviate (local disk, need WAL + replication + Raft):

```
Our S3-backed architecture eliminates:
  ✗ WAL          — S3 writes are atomic (lance manifest commit)
  ✗ Data replication — S3 is shared storage, all workers read same data
  ✗ Raft consensus  — etcd handles metadata, no leader election needed
  ✗ Data backup     — S3 is durable by design
  ✗ Shard transfer  — LoadShard same URI (zero-copy)

What we still need:
  ✓ Shared metadata store (etcd) — coordinator state survives restarts
  ✓ Read replicas — multiple workers load same shard for availability
  ✓ Online rebalance — redistribute shards without restart
  ✓ Write batching — avoid S3 fragment explosion from frequent writes
  ✓ Compact — merge small fragments for read performance
```

## Phase 7: Distributed Reliability (S3-Native)

### Step 1: etcd ShardState Activation [P0]

The single most important infrastructure gap. Without it:
- Coordinator restart = lose all runtime-created tables
- Multi-Active HA is fake (no shared state between coordinators)
- Dynamic rebalance results don't persist

Implementation:
- Activate existing EtcdShardState (271 lines, feature-gated)
- Add etcd-client dependency to common crate
- ClusterConfig: `metadata_store: { type: etcd, endpoints: [...] }`
- Coordinator binary: choose StaticShardState vs EtcdShardState based on config
- Test: create table on coordinator A → restart → table still exists

### Step 2: Read Replicas (Multi-Worker Same Shard) [P0]

S3-native replica: multiple workers load the same shard URI.
No data copying, no sync protocol.

Implementation:
- ShardConfig.executors already supports Vec<String> (multiple executors)
- scatter_gather already routes to primary, failover to secondary
- Change: load-balance reads across all healthy executors (not just primary)
- Connection pool: add round-robin or least-loaded read routing
- Test: 1 shard on 2 workers → kill 1 → queries still work

### Step 3: Online Rebalance [P1]

All building blocks exist: LoadShard, UnloadShard, auto_shard discovery.
Need: trigger rebalance without restart.

Implementation:
- New RPC: `RebalanceCluster` on coordinator
- Logic: discover fragments → compute new assignment → diff current vs desired →
  LoadShard new + UnloadShard old + update shard_state
- Trigger: manual RPC, or auto on worker join/leave (health check integration)
- Test: add worker → trigger rebalance → new worker gets shards

### Step 4: Write Batching + Compact [P1]

Frequent small writes → many tiny S3 fragments → read perf degrades.

Implementation:
- Coordinator-side write buffer: accumulate rows for N seconds or M rows
- Flush as single table.add() batch
- Background compact: periodically call lancedb table.optimize() to merge fragments
- Test: insert 1000 rows one-by-one → compact → verify read perf improves

### Step 5: Cluster Membership (Worker Auto-Register) [P2]

Workers announce themselves to coordinator on startup.

Implementation:
- Worker binary: on startup, send Register RPC to coordinator
- Coordinator: register_executor in shard_state → trigger rebalance
- On worker health check failure → remove_executor → trigger rebalance
- Already partially implemented (health check loop does remove)
- Test: start new worker → automatically gets assigned shards

## Phase 8: Scale & Enterprise

- [ ] K8s Helm chart (coordinator Deployment + worker StatefulSet + etcd)
- [ ] RBAC (collection-level roles stored in etcd)
- [ ] Sustained load test (1 hour, 200 QPS)
- [ ] 10M+ scale benchmark
- [ ] SQL path (DuckDB Lance extension)

## Stats
- Code: ~5,500 lines Rust | Tests: 99 unit + 29 integration + 21 capability + 9 bench
- GitHub: 36 commits on main
