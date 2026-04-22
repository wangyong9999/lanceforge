# v0.3.0-alpha.1 Parity Report

Generated at R3.5. Columns: v0.2 capability → v0.3.0-alpha.1 status →
evidence (test file / code pointer / deferral note).

## Delivered in alpha.1

| Capability | v0.3 state | Evidence |
|---|---|---|
| CreateTable | Parity | 13/13 E2E; dispatches via existing path |
| DropTable | Parity | 13/13 E2E |
| AddRows w/o on_columns | Parity | single-worker round-robin (routing.len==1) |
| AddRows w/ on_columns | Parity | short-circuit to single shard at `service.rs` |
| UpsertRows / DeleteRows | Parity | per-shard loop runs once for single-shard |
| AnnSearch | Parity | `dispatch_read` in service.rs calls `single_worker_execute_search` |
| FtsSearch | Parity | same path |
| HybridSearch | Parity | same path |
| CountRows | Parity | single-worker path in count_rows handler |
| AnnSearch + min_schema_version | Parity | schema_store guard retained |
| AnnSearch + min_commit_seq | Parity | MetaStore counter retained |
| AlterTable ADD COLUMN | Parity | existing path |
| AlterTable DROP COLUMN | **Enhanced** | `e2e_schema_evolution_test.py` 18/18 |
| AlterTable RENAME COLUMN | **Enhanced** | same |
| AlterTable combined (ADD+DROP+RENAME) | **Enhanced** | same |
| CreateIndex IVF_FLAT / BTREE / FTS | Parity | unit test + E2E |
| CreateIndex IVF_PQ | **Enhanced** (v0.2 silently fell through to IVF_FLAT) | `r3_create_index_unknown_type_errors_out` verifies dispatch table |
| CreateIndex IVF_HNSW_SQ | **Enhanced** | same |
| CreateIndex IVF_HNSW_PQ | **Enhanced** | added |
| CreateIndex BITMAP | **Enhanced** | added |
| CreateIndex LABEL_LIST | **Enhanced** | added |
| CreateTag / ListTags / DeleteTag | **Enhanced** | `e2e_tags_test.py` 22/22 |
| RestoreTable by version | **Enhanced** | same |
| RestoreTable by tag | **Enhanced** | same |
| Worker OpenTable / CloseTable | Added surface | `r1a_open_table_is_idempotent`, `r1a_close_table_is_idempotent` |

## Deferred to alpha.2 (not required for architectural flip)

| Capability | Reason | Target phase |
|---|---|---|
| AnnSearch + version / tag (time travel reads) | Needs per-query version-scoped handles to avoid mutating shared table state | R3 continuation |
| Fully fungible worker routing (workers serve any table) | Workers still own shards until migration tool lets users convert data layout | R2 |
| `lance-admin migrate` (v0.2 multi-shard → v0.3 single dataset) | Fresh deployments don't need it; existing users carry v0.2 until alpha.2 | R4 |
| Delete `scatter_gather.rs`, `shard_pruning.rs`, `shard_state.rs`, `shard_manager.rs`, `partition_batch_by_hash` | Multi-shard path retained for legacy compat; removed in one purge once migration tool ships | R6 |
| OTEL trace_id propagation coord→worker with full span context | Existing trace_id logging works; OTLP + Collector setup is polish | R5 |
| Deprecated proto field removal (target_shard, shard_name, MoveShard, CreateLocalShard, LoadShard/UnloadShard) | Tracked together with R6 dead-code purge so breaking changes land atomically | R6 |

## Breaking from v0.2 (alpha.1)

| Change | Mitigation |
|---|---|
| DDL lease narrowed from all ops to CreateTable/DropTable only | Lance manifest CAS handles AlterTable/CreateIndex atomicity (ADR-004) |
| AlterTable proto gained new fields | Adding fields is wire-compatible; v0.2 clients still work |
| Index types IVF_HNSW_SQ / IVF_PQ now actually produce the requested index (v0.2 built IVF_FLAT silently) | This is a bug fix: workloads expecting v0.2 behaviour with those type strings were already not getting what they asked for |

## Acceptance signals for alpha.1

1. ✅ Workspace lib tests all green
2. ✅ 13/13 E2E for existing capabilities (1-2 transient flakes clear in isolation)
3. ✅ 18/18 new `e2e_schema_evolution_test.py`
4. ✅ 22/22 new `e2e_tags_test.py`
5. ✅ Release build clean for all bins
6. ✅ clippy -D warnings clean across workspace

## Not yet signalled

- Chaos suite re-framing to "kill any worker" (R2)
- Benchmark regression vs v0.2 baseline (R6)
- Python SDK smoke test (protos regenerated; full roundtrip not re-verified)

These are alpha.2 gates.
