# v0.2 → v0.3 Parity Checklist

Every row is either:
- **Parity**: v0.3 provides the same capability (possibly via a
  different code path).
- **Enhanced**: v0.3 is strictly better.
- **Removed**: v0.3 intentionally drops the capability (reason
  noted in Comment column).
- **Deferred**: not in v0.3.0-alpha.1, scheduled for a later release.

Each row is checked off in the Phase column where it lands.

## RPC surface

| Capability | v0.2 status | v0.3 target | Phase | Comment |
|---|---|---|---|---|
| CreateTable | fan-out CreateLocalShard | single-worker create | R1b | DDL lease retained |
| DropTable | fan-out | single-worker | R1b | DDL lease retained |
| AddRows | partition_batch_by_hash → fan-out | single-worker, whole batch | R1b | Lance IVF handles intra-dataset distribution |
| UpsertRows on_columns | hash-route, reject non-partition keys | Lance `merge_insert` pass-through | R3 | on_columns decouples from partitioning |
| DeleteRows | fan-out filter | single-worker | R1b | |
| AnnSearch | scatter/gather top-K | single-worker, Lance parallel scan | R1b | Parity |
| AnnSearch + min_schema_version | CAS guard via schema_store | Lance manifest version compare | R1b | Parity |
| AnnSearch + min_commit_seq | cross-shard commit_log | per-table commit_seq = Lance version | R1b | Semantic simplified |
| AnnSearch + **version** | not supported | `table.checkout(v)` | R3 | Enhanced |
| AnnSearch + **tag** | not supported | `table.checkout_tag(name)` | R3 | Enhanced |
| FtsSearch | scatter/gather | single-worker | R1b | |
| HybridSearch (ANN + FTS + RRF) | scatter/gather, RRF at coord | single-worker, Lance native | R1b | Simpler merge |
| CountRows | sum over shards | single Lance call | R1b | |
| CreateIndex IVF_FLAT | per-shard fan-out | single-worker | R1b | Parity |
| CreateIndex IVF_PQ | fallthrough bug (becomes IVF_FLAT) | explicit dispatch | R3 | Fixed |
| CreateIndex IVF_HNSW_SQ | fallthrough bug | explicit dispatch | R3 | Fixed |
| CreateIndex BTREE | per-shard | single-worker | R1b | |
| CreateIndex BITMAP | not supported | new | R3 | Enhanced |
| CreateIndex LABEL_LIST | not supported | new | R3 | Enhanced |
| CreateIndex INVERTED (FTS) | per-shard | single-worker | R1b | |
| AlterTable ADD COLUMN | fan-out + CAS schema_version | Lance `add_columns` | R3 | Atomicity shifts to Lance |
| AlterTable DROP COLUMN | not supported | `drop_columns` | R3 | New |
| AlterTable RENAME COLUMN | not supported | `alter_columns` | R3 | New |
| AlterTable ALTER TYPE | not supported | `alter_columns` (limited) | R3 | New |
| Tags: create/list/delete/get | not supported | `table.tags()` pass-through | R3 | New |
| RestoreTable | not supported | `table.restore(v)` | R3 | New |
| LoadShard | fan-out | **Removed** | R1c | Concept gone |
| UnloadShard | | **Removed** | R1c | |
| MoveShard | | **Removed** | R1c | Rebalance is implicit |
| CreateLocalShard | worker RPC | **Removed** (internal only) | R1c | Replaced by CreateTable direct call |
| HealthCheck `shard_names` field | | **Removed** | R1c | Replaced by `table_names` |

## Operational features

| Feature | v0.2 | v0.3 | Phase | Comment |
|---|---|---|---|---|
| Multi-coord HA (MetaStore CAS) | ✅ | ✅ | R1c | Parity; state shape slimmed |
| DDL lease | all DDL | CreateTable/DropTable only | R1c | AlterTable uses Lance CAS |
| Per-API-key QPS quota | ✅ (G6) | ✅ | R1c | Enforcement point unchanged |
| Audit log to stdout + S3 rolling | ✅ | ✅ | R5 | shard_name field dropped |
| Prometheus metrics | per-shard + global | per-table + global | R5 | Dashboard templates updated |
| OTEL trace_id propagation | not implemented | new | R5 | Enhanced |
| Worker health check | ✅ | ✅ | R2 | |
| Stale worker eviction | ✅ | ✅ | R2 | |
| Graceful shutdown (SIGTERM) | ✅ | ✅ | R2 | |
| IDX auto-compaction | per-shard loop | per-table loop | R3 | |
| IDX runtime table discovery | MetaStore poll | MetaStore poll (simpler keys) | R3 | |

## Data & consistency

| Property | v0.2 | v0.3 | Phase | Comment |
|---|---|---|---|---|
| Read-your-own-writes | commit_seq cross-shard barrier | Lance version + commit_seq = Lance version | R1b | Semantically equivalent per-table |
| Schema-version consistency | CAS in schema_store | Lance manifest CAS | R1b | Lance-native |
| DDL atomicity (single coord) | per-shard lock + DDL lease | Lance manifest CAS | R3 | Stronger |
| DDL atomicity (multi-coord) | DDL lease + per-shard | Lance manifest CAS (AlterTable) + lease (Create/Drop) | R3 | |
| Bounded staleness across shards | N/A (single dataset has no "across") | N/A | — | Concept retired |
| Read consistency on stale cache | per-shard version poll (5s) | per-table version poll (5s) | R2 | Parity |

## Client / SDK

| Feature | v0.2 | v0.3 | Phase | Comment |
|---|---|---|---|---|
| Python sync client | ✅ | ✅ | R1c | Field signatures update |
| Python async client | ✅ | ✅ | R1c | |
| LangChain integration | ✅ | ✅ | R1c | Surface unchanged |
| `target_shard` client-side hint | present | **Removed** | R1c | Silently stripped on the wire |

## Deployment shapes

| Shape | v0.2 | v0.3 | Phase |
|---|---|---|---|
| Monolith (lance-monolith) | ✅ | ✅ | R2 |
| Split (coord + worker + idx + cp) | ✅ | ✅ | R2 |
| Helm chart | ✅ | ✅ | R5 |

## Tests & chaos

| Suite | v0.2 | v0.3 | Phase |
|---|---|---|---|
| E2E integration (13 tests) | ✅ | retained + updated | R1a–R3 |
| Chaos (kill shard owner, stall) | ✅ | reframed: kill any worker | R2 |
| Schema evolution E2E (ADD only) | ✅ | extended: ADD/DROP/RENAME | R3 |
| Time travel E2E | none | new | R3 |
| Tags E2E | none | new | R3 |
| Index types E2E | IVF_FLAT + BTREE + FTS | + IVF_PQ, IVF_HNSW_SQ, BITMAP, LABEL_LIST | R3 |
| Migration E2E | none | new (v2 → v3 correctness) | R4 |
| Smoke benchmark | ✅ | ✅ (updated topology) | R0.5, R6 |

## Deferred (not in v0.3.0-alpha.1)

| Item | Why deferred | Target |
|---|---|---|
| NVMe fragment cache with consistent hash | independent effort, not coupled to realignment | v0.4 |
| Cross-region replication | separate project | v0.4+ |
| Namespace recursion | stacked on realignment, deserves its own pass | v0.3.0 GA |
| `lance-namespace` crate adoption | waits for crate 1.0 | v1.0 |
| Embedding provider auto-call | product layer, not server | v0.4+ |
| Reranker plugins | same | v0.4+ |
| Column-level encryption | upstream dependency | when Lance exposes stable API |

## Acceptance

A phase "passes parity" when every row it claims in the Phase column
is ticked in this file and backed by either (a) a green test, (b) a
code pointer showing the new path, or (c) a deprecation note in
CHANGELOG.
