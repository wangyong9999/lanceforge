# Realignment Plan — v0.2 → v0.3.0-alpha.1

Companion to `ARCHITECTURE_V3_SINGLE_DATASET.md` and
`REALIGNMENT_PARITY_CHECKLIST.md`. Holds the phase-by-phase
implementation sequence with gates and test obligations.

## Phase R0 — Preparation

**Deliverables** (this phase):
- `docs/ARCHITECTURE_V3_SINGLE_DATASET.md`
- `docs/REALIGNMENT_PARITY_CHECKLIST.md`
- `docs/REALIGNMENT_PLAN.md` (this file)
- `CHANGELOG.md` v0.3.0-alpha.1 entry
- `realignment` branch cut from `lanceforge-clean`

**Gate**: docs reviewable, branch pushed. No production code change.

## Phase R0.5 — Pre-flight benchmarks

Three benchmarks whose results drive concrete implementation
choices downstream. Results are captured as ADRs under `docs/adr/`.

### B1 — Lance OCC contention under N writers

**Question**: Does open multi-writer OCC on a single dataset hold up
under the write concurrency LanceForge supports today, or do we
need to pin one primary writer per table?

**Setup**: 8 worker processes concurrently calling `add_rows` on
the same Lance dataset (local disk and S3 variants). 10k rows per
call, sustained for 60s. Measure commit conflict rate, retry
distribution, end-to-end throughput.

**Decision output (ADR-001)**:
- If conflict rate < 10% at target QPS → open multi-writer allowed.
- If 10–30% → multi-writer allowed with at-coord batching.
- If > 30% → single-primary-writer mandatory in v0.3.

### B2 — Cold worker start latency

**Question**: When a worker first opens a Lance dataset it hasn't
served before, how long from `open_table` to first query response?

**Setup**: Spawn fresh worker, issue first query, measure p50/p99
for cold-start. Repeat 50 times each for (a) local disk, (b) S3 in
same region, (c) S3 cross-region.

**Decision output (ADR-002)**:
- If p99 < 500ms → no warm-pool needed.
- If 500ms–2s → warm-pool optional, add MetaStore "hot tables"
  hint but don't force preload.
- If > 2s → R2 must add explicit warm-pool preloading.

### B3 — Scalar BTREE vs shard hash routing

**Question**: Does Lance's scalar BTREE index + predicate pushdown
deliver query performance at least as good as our v2 hash-partition
shard routing for equality filters?

**Setup**: 10M rows, column `tenant_id` with 100 distinct values.
- v2 baseline: hash-partition on `tenant_id`, 10 shards.
- v3 candidate: single dataset, BTREE index on `tenant_id`.
Query: `AnnSearch` with `WHERE tenant_id = 'tenant_42'` at 50 QPS,
concurrency 10. Measure p50/p99 latency, recall.

**Decision output (ADR-003)**:
- If BTREE matches or beats hash routing → replacement confirmed.
- If BTREE is 10–30% slower → accept the loss, document.
- If > 30% slower → evaluate Lance partition columns as the
  structural replacement for v2 partitioning.

**Gate for R0.5**: three ADRs written, each with measured numbers
and a clear decision.

## Phase R1 — Data model flip

Broken into three sub-phases so failure rolls back at fine
granularity. Each sub-phase ends with 13/13 E2E pass.

### R1a — Worker LanceTableRegistry per-table

**Scope**:
- `LanceTableRegistry`: internal `HashMap<String, lancedb::Table>`
  keyed by `table_name` (was `shard_name`).
- `OpenTable(name, uri)` / `CloseTable(name)` RPCs added as the
  new handle-management surface. Old `LoadShard`/`UnloadShard` RPCs
  remain but delegate to OpenTable/CloseTable (compat shim).
- `CreateLocalShard` handler keeps accepting requests but interprets
  the shard as the table (1:1).
- Coord unchanged externally — still sees "one shard per table".

**Test obligations**:
- Existing E2E: all 13 pass unchanged.
- New worker unit tests: OpenTable / CloseTable lifecycle,
  duplicate open, open at non-existent URI.
- Soak: `e2e_minio_sse_test` + `e2e_alter_table_test` runs pre/post.

**Exit gate**: 13/13 E2E + ≥10 new worker unit tests + clippy clean.

### R1b — Coord single-worker dispatch

**Scope**:
- `service.rs` AnnSearch/FtsSearch/HybridSearch/CountRows: replace
  scatter/gather with `pick_worker(table) → single RPC`.
- AddRows/UpsertRows/DeleteRows: remove `partition_batch_by_hash`,
  send whole batch to one worker.
- AlterTable: drop fan-out, call one worker.
- CreateIndex: drop fan-out, call one worker.
- `CreateTable`: DDL lease → pick worker → one RPC.
- `commit_seq` semantics: becomes `table.version()` directly;
  `min_commit_seq` guard compares to Lance version.
- Idempotency: worker holds a 5-minute LRU of `idempotency_key →
  last_commit_seq` for write retry dedup.

**Test obligations**:
- E2E 13/13 still pass.
- New: idempotency retry unit test on worker.
- New: `e2e_single_dispatch_test.py` covering write/read/DDL happy
  paths end-to-end in the new path.
- Benchmark: smoke bench, p50/p99 not worse than R1a exit by >10%.

**Exit gate**: 13/13 E2E + new test green + bench within bounds.

### R1c — MetaStore / proto / SDK slim

**Scope**:
- Remove proto messages: `CreateLocalShardRequest`,
  `LoadShardRequest`, `UnloadShardRequest`, `MoveShardRequest`,
  `ShardRoutingEntry`, `ShardUri`.
- Remove fields: `target_shard`, `shard_name` where irrelevant,
  `HealthCheckResponse::shard_names` → `table_names`.
- MetaStore: `shard_uris` → `table_uris` (HashMap<String, String>);
  retire `shard_mapping`, `target_state`.
- `crates/common/src/shard_state.rs` deleted.
- `crates/meta/src/shard_manager.rs` deleted.
- Python SDK: field-signature update, smoke test roundtrip.
- Config schema: `replica_factor` / `shards` deprecated with
  WARN-logged tolerance (not a hard error in alpha.1).

**Test obligations**:
- 13/13 E2E green.
- SDK smoke test (Python) passes.
- Config with old `shards:` section loads with WARN + uses
  defaults.
- clippy `-D warnings` clean.
- Workspace lib tests all green.

**Exit gate**: above + LOC reduction ≥ 500 net (sanity check
against plan).

## Phase R2 — Fungible worker routing

**Scope**:
- `RoutingClient` rewrites: worker pool + consistent-hash of
  `(table_name, request_id)`; TTL cache preserved.
- Health-checked pool (existing machinery retained).
- Cold-start handling per R0.5/B2 outcome (ADR-002).
- Chaos suite: reframed from "kill shard owner" to "kill any
  worker; traffic continues". 20 iterations.

**Test obligations**:
- Chaos 20/20 green.
- New: `e2e_fungible_routing_test.py` — kill worker mid-query,
  assert retry succeeds on another worker.
- Cold-start micro-benchmark retained under `bench/cold_start/`.

**Exit gate**: chaos 20/20 + new E2E green + ADR-002 action
implemented.

## Phase R3 — Lance native pass-through

**Scope**:
- AlterTable op enum extended: ADD / DROP / RENAME / ALTER_TYPE,
  each dispatched to the matching lancedb method.
- Tags: new RPCs `CreateTag`, `ListTags`, `GetTag`, `DeleteTag`.
- Time travel: `AnnSearch` / `FtsSearch` / `HybridSearch` /
  `CountRows` accept `version: Option<u64>` and
  `tag: Option<String>`.
- `RestoreTable` RPC.
- Index types: fix the `IVF_HNSW_SQ` fallthrough (was silently
  becoming IVF_FLAT); add `IVF_PQ`; add scalar `BITMAP`,
  `LABEL_LIST`.

**Test obligations**:
- New E2E per capability:
  - `e2e_schema_evolution_test.py` (ADD/DROP/RENAME/ALTER_TYPE)
  - `e2e_tags_test.py`
  - `e2e_time_travel_test.py`
  - `e2e_restore_test.py`
  - `e2e_index_types_test.py`
- Unit tests for AlterTable op enum dispatch.

**Exit gate**: all five E2E green + parity checklist R3 rows
ticked.

## Phase R3.5 — Parity verification

**Scope**:
- Walk parity checklist. For every row with phase ≤ R3, verify via:
  (a) a passing test, or
  (b) a code pointer to the new path, or
  (c) an explicit "Removed" / "Deferred" entry with comment.
- Generate `docs/REALIGNMENT_PARITY_REPORT.md` — machine output
  from the checklist traversal.

**Exit gate**: 100% rows up to R3 accounted for.

## Phase R4 — Migration tool

**Scope** (`lance-admin migrate` subcommand):
- Input: v0.2 config + MetaStore snapshot + shard URIs.
- Output: v0.3 layout at a target URI.
- Algorithm: for each table, open every shard as a Lance dataset,
  stream `RecordBatch`es into a new dataset at the target URI.
  Record source fragment checksums; verify row-count + SHA256
  digest match on completion.
- Safety: never deletes source until user confirms with `--commit`.
- Resumability: `migration_state.json` tracks progress per table;
  restart resumes from last completed shard.
- Dry-run mode: `--dry-run` prints plan and exits.

**Test obligations**:
- `e2e_migration_test.py`: build synthetic v0.2 multi-shard data,
  run migrate, read from v0.3, assert row-count + vector-equality.
- Resumability test: kill migrate mid-run, restart, assert output
  identical to uninterrupted run.

**Exit gate**: both tests pass; `lance-admin --help` documents the
flag set.

## Phase R5 — Docs + observability

**Scope**:
- `docs/ARCHITECTURE.md`: rewrite as v3, move v2 to `archive/`.
- `docs/MIGRATION_V2_TO_V3.md`: step-by-step upgrade.
- `docs/QUICKSTART.md`: single-dataset walkthrough.
- OTEL: `tracing::instrument` on all RPC handlers, trace context
  headers on coord → worker calls, docs for Collector setup.
- Prometheus metrics: remove `shard_id` label, add `table_name`
  label where needed. Grafana dashboard updated.

**Test obligations**:
- `e2e_otel_trace_test.py`: client generates trace_id, verify it
  reaches worker logs end-to-end.
- Docs lint (markdownlint) clean.

**Exit gate**: OTEL test green + docs review complete.

## Phase R6 — Dead code purge + regression

**Scope**:
- Delete: `shard_state.rs`, `shard_manager.rs`, `shard_pruning.rs`,
  `scatter_gather.rs`, `partition.rs::partition_batch_by_hash`.
- Any remaining `shard_` references in non-migration code: resolve
  or explicitly keep with a comment.
- Full workspace compile + test.
- Full E2E + chaos 20 iterations.
- Smoke benchmark vs v0.2.0-beta.5 baseline.
- Coverage report: confirm delta is proportional to deletion
  (not a real coverage regression).
- Tag `v0.3.0-alpha.1` (annotated, undercover).

**Exit gate**: all above green; tag pushed.

## Total estimate

6–7 weeks single-person. Branching structure:

```
lanceforge-clean  →  realignment  →  (dev proceeds here)
                                 └── v0.3.0-alpha.1 tag at R6 exit
```

Merge back to `lanceforge-clean` at tag cut; `lanceforge-clean`
effectively becomes v0.3 line and v0.2 is archived at the
`v0.2.0-beta.5` tag.
