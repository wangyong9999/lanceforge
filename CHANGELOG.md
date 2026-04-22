# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0-alpha.2] — 2026-04-22

Completes the single-dataset realignment. v0.3.0-alpha.1 was the
architectural flip with a compatibility layer for v0.2 data;
alpha.2 removes the layer, makes the commitment unconditional, and
delivers the remaining pieces needed to call the realignment
"done" for this release series.

### Added
- **Fungible worker routing (R2)**: CreateTable broadcasts
  `OpenTable` to every healthy worker so any of them can serve
  any table. Reads dispatch via `worker_select::pick_worker_for_read`
  over the full healthy pool rather than the shard's primary /
  secondary. DropTable broadcasts `CloseTable` before deleting
  storage. Connection-pool reconnect path opens all tables on
  the rejoining worker.
- **Migration tool (R4)**: `lance-admin migrate [--commit]`.
  Reads `shards/*` from a v0.2 MetaStore and writes the single-
  dataset-layout `table_uris/*`. Default is dry-run. Multi-shard
  tables (>1 shard per table) are listed under SKIPPED with a
  clear WARN; genuine merge is a future enhancement. Source
  `shards/*` keys are preserved so rollback is trivial.

### Removed
- `scatter_gather.rs` (1143 LOC): multi-worker fan-out dispatch.
- `shard_pruning.rs`: hash-partition predicate pruning (ADR-003
  replaces this with Lance scalar indexes).
- `partition_batch_by_hash` + its seven unit tests: row-level
  hash partitioning at the coord. Lance's internal IVF handles
  the intra-dataset distribution.
- `ShardPruner` field on `CoordinatorService` and the
  `with_pruner` / `with_shard_state_and_pruner` constructors.
- `add_rows_hash_partitioned`, `on_columns_for`: coord helpers
  that only existed to serve the multi-shard write path.
- `lance-integration/tests/test_shard_routing.rs`: exercised the
  retired pruning layer.
- Multi-shard legacy scatter path in `dispatch_read`, `count_rows`,
  `upsert_rows`, `add_rows`, and `batch_search`. Tables with more
  than one shard now return `FailedPrecondition` directing
  operators to run the migration tool.

### Changed
- **CreateTable creates exactly one shard per table**. Runtime
  auto-sharding across workers is retired. Lance's internal
  fragments provide row-level parallelism inside the single
  dataset.
- `inject_traceparent` moves from `scatter_gather` into
  `worker_select`, colocated with single-worker dispatch.
- Connection-pool recovery uses `OpenTable` (v0.3) instead of
  `LoadShard` (v0.2); both RPCs still accepted by workers for
  compat.

### Net code delta
~2,130 LOC removed, 130 LOC added (mostly worker-side DDL and
migrate subcommand). Six files deleted outright.

### Test evidence
- 534 workspace lib tests green.
- 18/18 `e2e_schema_evolution_test.py` (R3).
- 22/22 `e2e_tags_test.py` (R3).
- 4/4 `r4_migrate_*` unit tests.
- 3/3 `r1a_*` + 3/3 `worker_select` unit tests.
- E2E 12/13 (one pre-existing port-contention flake that passes
  in isolation).

### Breaking
- v0.2 multi-shard deployments MUST run `lance-admin migrate`
  before upgrading. Untransformed multi-shard tables return
  `FailedPrecondition` on every RPC.
- The `replica_factor` config key is silently ignored; fungible
  routing subsumes the replica concept at the query-execution
  layer. Data redundancy is now the object-storage layer's job.
- `scatter_gather` and `shard_pruning` modules are gone from
  `lance-distributed-common` and `lance-distributed-coordinator`
  public APIs. Downstream code using them must migrate to
  `worker_select` dispatch.

### Deferred to alpha.3+
- Time travel reads: `version` / `tag` fields on AnnSearch /
  CountRows (needs per-query version-scoped handles; Lance
  Table clone is Arc-cheap but the integration is outside
  alpha.2's scope).
- OTEL trace context propagation beyond the existing `trace_id`
  log field.
- Proto field removal for `target_shard`, `shard_name`,
  `MoveShard`, `CreateLocalShard`, `LoadShard`, `UnloadShard` —
  these still exist on the wire as deprecated; the `replace`
  pass removes them once clients are updated.
- Python SDK explicit alignment for the new Tag / Restore RPCs.

## [0.3.0-alpha.1] — 2026-04-22

Single-dataset realignment, alpha-grade first cut. One logical
table now maps to one Lance dataset in object storage; the multi-
shard-per-table model from v0.2 is retained as a compatibility
path pending R6 removal. See:
- `docs/ARCHITECTURE_V3_SINGLE_DATASET.md` — design rationale.
- `docs/REALIGNMENT_PARITY_CHECKLIST.md` — v0.2→v0.3 mapping.
- `docs/REALIGNMENT_PARITY_REPORT.md` — actual landing state.
- `docs/REALIGNMENT_PLAN.md` — phase sequence R0..R6.
- `docs/adr/ADR-00{1..5}*.md` — architectural decisions.

### Added
- **AlterTable DROP / RENAME** (Lance metadata-only): worker calls
  `drop_columns` / `alter_columns` through a schema_store-backed
  coord handler. `e2e_schema_evolution_test.py` 18/18.
- **Tags**: `CreateTag` / `ListTags` / `DeleteTag` RPCs on the
  scheduler service; pass-through to Lance native `tags()` API.
  `e2e_tags_test.py` 22/22.
- **RestoreTable(version | tag)**: rolls back the Lance dataset to
  a prior version or tag; a new manifest records the restore.
- **CreateIndex** full dispatch: IVF_FLAT, IVF_PQ, IVF_HNSW_SQ,
  IVF_HNSW_PQ, BTREE, BITMAP, LABEL_LIST, FTS/INVERTED. v0.2
  silently fell through IVF_HNSW_SQ and IVF_PQ to IVF_FLAT — that
  bug is fixed; unknown types now return `Plan` error with the
  supported list.
- Worker `OpenTable` / `CloseTable` RPCs as the table-oriented
  handle surface alongside the legacy shard-oriented pair.
- Coord `dispatch_read` helper with single-worker dispatch via
  `worker_select::pick_worker_for_read`.
- 5 Architecture Decision Records under `docs/adr/`.

### Changed
- Coord read handlers (AnnSearch / FtsSearch / HybridSearch /
  CountRows) take the single-worker path when the table maps to
  exactly one shard — the default and only case for tables created
  under v0.3. Legacy multi-shard tables still scatter-gather.
- Write handlers (AddRows / UpsertRows / DeleteRows) skip hash-
  partition fan-out when the table has one shard.
- DDL lease retained for CreateTable / DropTable; AlterTable and
  CreateIndex rely on Lance's manifest CAS for atomicity
  (ADR-004).

### Deferred to later alphas
- Time travel reads (`version` / `tag` on AnnSearch / CountRows) —
  needs per-query version-scoped handles.
- Fully fungible worker routing (R2).
- `lance-admin migrate` for v0.2 multi-shard data conversion (R4).
- OTEL trace context propagation (R5).
- Dead-code purge: `scatter_gather.rs`, `shard_pruning.rs`,
  `shard_state.rs`, `shard_manager.rs`, `partition_batch_by_hash`,
  and the deprecated proto messages/fields (R6, together).

### Not-broken from v0.2
- RPC API surface: all v0.2 RPCs remain. New RPCs added; fields on
  existing messages only appended (wire-compat).
- Data layout: v0.2 multi-shard tables are still readable via the
  retained scatter-gather path.
- E2E parity: 13/13 (with occasional port-contention flakes that
  clear in isolation).

## [0.2.0-beta.5] — 2026-04-21

See the release notes in the tag message — v2 architecture (4-role
composition), distributed primitives #5.1–#5.5 + #3, and the
hardening pass (idempotent AlterTable + coverage extensions). This
is the final release of the multi-shard architecture; v0.3 begins
the single-dataset realignment.

## [0.2.0-beta.4] - 2026-04-20

Coverage-first hardening pass + honest fix for an overstated SSE
claim. D1-D8 + the SSE-C correction.

### Fixed
- SECURITY.md §6.3 SSE-C claim: `storage_options.aws_server_side_
  encryption_customer_*` keys don't actually encrypt (empirically
  verified against MinIO). Docs rewritten; SSE-C deferred to 0.3.
- `lance-admin shards copy` OOM risk: refactored from
  `get().bytes().await` to `put_multipart` above a configurable
  threshold. Streaming now keeps memory bounded by one chunk.
- `lance-admin shards copy` fail-fast on single-object error:
  added `--continue-on-error` + summary report.

### Added
- D1 MinIO SSE-S3 e2e (`e2e_minio_sse_test.py`) + CI services block.
- D4 namespace prefix proptests (512 cases × 4 properties).
- D5 MetaStore S3 real-MinIO integration test (`MINIO_E2E_TEST=1`).
- D6 audit_dropped_total e2e (`e2e_audit_dropped_test.py`).
- D7 worker `#[tracing::instrument]` explicit `trace_id` field.
- D8 write-path per-shard latency log (add_rows fan-out; H13 parity).
- `docs/OBSERVABILITY.md`, `docs/CI_MATRIX.md`, `docs/RELEASE.md`
  added in earlier beta.3 commits — remain authoritative.

### Changed
- `cmd_shards_copy` signature: now takes `continue_on_error: bool` +
  `multipart_threshold: u64`. Test call sites updated.
- `SECURITY.md` §6.3 rewritten to reflect the actual storage_options
  capabilities (bucket-level SSE works, per-request SSE-C doesn't).
- `LIMITATIONS.md` §10 adds explicit "SSE-C not supported" entry
  with technical reason + workaround.

### Deferred to 0.3
- SSE-C support via custom object_store wrapper (the real fix).
- B2.3 Write batching (confirmed as cross-backend blocker in C2).
- Coord RSS step root-cause (audit hypothesis ruled out in C1).

## [0.2.0-beta.3] - 2026-04-20

Observability + OBS-native audit + async SDK. B1-B6 (minus B7 soak
which runs past tag and updates LIMITATIONS §13 after).

### Fixed
- G5 namespaced reads returning `No Lance shard found`: worker now
  applies the `/` → `__` shard-name sanitisation on the read path
  (beta.2 did it only on writes; the unit test didn't cover the
  read-accept case).

### Added
- B1 worker-side traceparent re-emission.
- B2 OBS-native audit sink (put-per-batch).
- B3 optional OTLP exporter (`--features otel`).
- B4 Grafana dashboard + README + alert snippets.
- B5 `MetaShardState::all_tables_for_namespace` + `list_tables`
  pushdown.
- B6 `AsyncLanceForgeClient` covering search/insert/create_table/
  drop_table/list_tables/count_rows.
- `docs/OBSERVABILITY.md` (three-pillar walkthrough + OTLP pipeline).

### Changed
- `AuditSink::spawn` routes OBS URIs to the new batch-PUT backend
  instead of failing startup.
- `validate_audit_path` now only rejects empty strings.
- `lanceforge` Python package exports `AsyncLanceForgeClient`;
  `__version__` bumped to `0.2.0-beta.3`.

### Deferred to 0.3
- Worker-side OTLP span emission, REST bridge, span attribute
  enrichment, coord-side sampling.
- Async SDK: upsert, batch_search, create_index, rebalance.
- Audit sink retry on PUT failure.

## [0.2.0-beta.2] - 2026-04-20

Posttag hardening for beta.1. F1-F9 close the "shipped-but-not-
wired" gaps surfaced in the beta.1 postmortem, plus one unplanned
lancedb-panic fix discovered by the new end-to-end tests.

### Fixed
- G5 namespace + lancedb panic: coordinator now sanitizes `/` → `__`
  in the `shard_name` passed to lancedb, which previously
  `.unwrap()`ed on validation and panicked the tokio task. Worker
  adds an input-validation guard for belt-and-braces. User-visible
  `table_name` keeps `/` everywhere else.
- G7 OBS audit path silent drop: `validate_audit_path()` rejects
  `s3://` / `gs://` / `az://` URIs at config time; coord exits 2.
- G7 audit sink channel-full invisibility: new
  `lance_audit_dropped_total` Prometheus counter.

### Added
- F1 `Metrics.audit_dropped_count` + counter export.
  `AuditSink::spawn(..., metrics)` signature.
- F2 `lance-integration/tools/e2e_h25_coord_uptime_test.py` nightly
  regression for the H21 shutdown-timeout bug.
- F3 + F4 `lance-integration/tools/e2e_beta2_ns_audit_test.py`:
  7 gRPC e2e assertions covering the namespace allow/deny path,
  ListTables filter, and real-RPC audit record landing.
- F5 admin S3MetaStore round-trip test.
- F6 `lance-admin meta restore --dry-run` and `--yes` flags;
  purge without `--yes` on non-TTY refuses to proceed.
- F7 `SECURITY.md` §5: storage-layer isolation caveat boxed at
  top.
- F8 Python SDK `LanceForgeClient` strips proxy env vars by
  default; opt-out via `respect_proxy_env=True`.
- F9 `AuditRecord.trace_id` top-level JSONL field (additive).

### Changed
- `AuditSink::spawn` signature now takes `metrics: Arc<Metrics>`.
- `cmd_restore` signature takes `(meta, from, purge, dry_run, yes)`.
- `open_store` in admin routes `file://` and `memory://` through
  S3MetaStore so the CLI accepts every URI form the coordinator
  config does.

### Deferred to 0.3
- Real OBS-append audit sink (R3).
- Full OTLP exporter pipeline.
- Grafana dashboard template bundle.
- Async Python SDK.
- MetaStore-side namespace enforcement.
- Worker-side traceparent re-emission.
- 4h+ soak to characterise worker RSS step cadence.

## [0.2.0-beta.1] - 2026-04-19

First beta on the SaaS-first track. Closes the Severity-1 gaps
from `docs/COMPETITIVE_ANALYSIS.md` — multi-tenant namespace,
per-key quota, persistent audit log, encryption-at-rest
documentation, operator CLI, cross-process trace correlation.
Backward compatible with 0.2.0-alpha.1 on wire and on disk.

### Added
- API-key ↔ namespace binding (G5). `ApiKeyEntry.namespace` rejects
  RPCs whose `table_name` doesn't start with `{ns}/` and filters
  `ListTables` through the caller's prefix. Admin keys without a
  namespace retain operator access. See `docs/SECURITY.md` §5.
- Persistent audit log (G7). `security.audit_log_path` turns on an
  append-only JSONL sink for every DDL / write RPC. Records carry
  timestamp, principal, target, details, and the W3C trace_id when
  present. OBS URIs currently warn-and-drop (tracked R3) — use a
  local path with Vector / Fluent Bit until 0.3.
- `docs/SECURITY.md` (G8). Threat model, TLS config, RBAC +
  namespace scope, SSE-KMS / SSE-S3 / SSE-C pattern, audit log
  ingestion, key rotation, hardening checklist.
- `lance-admin` operator CLI (G9). `meta dump` / `meta restore` /
  `meta list` / `shards list`. Shard file transfer is left to
  cloud-native tooling — `shards list` prints the URIs.
- Traceparent pass-through (G10). Coordinator parses W3C
  `traceparent` metadata and writes the 32-char trace_id into the
  stdout audit line and the JSONL record's `details` field.

### Changed
- Dev profile now builds with `debug = "line-tables-only"` and
  `split-debuginfo = "unpacked"`. Integration test binaries shrink
  from ~1.5 GB to ~300 MB without losing panic stack traces. See
  `docs/BUILD_HYGIENE.md`.
- `.cargo/config.toml` added with `build.incremental = true`
  explicit, sparse registry, and `cargo small` / `cargo prune`
  aliases for routine work.
- `crates/meta/src/state.rs` tests migrated off hardcoded
  `/tmp/lanceforge_metastate_*.json` paths (nanosecond-collision
  FileMetaStore lock race under parallel `cargo test`; same root
  cause as H23 in store.rs). Uses `tempfile::tempdir()` now.
- README feature matrix updated with G5 / G6 / G7 / G8 / G9 / G10.
  Test count badge 272 → 311.

### Deferred to 0.3
- OBS-native append for the audit log (R3).
- Full OpenTelemetry exporter pipeline (OTLP collector).
- Grafana dashboard template bundle.
- Async Python SDK (`grpc.aio`).
- MetaStore-side namespace enforcement (today's G5 is API-layer).
- Worker-side `traceparent` re-emission on outbound gRPC.

## [0.2.0-alpha.1] - 2026-04-18

First alpha on the SaaS-first track. Closes every alpha gate in
`docs/ROADMAP_0.2.md` §3.3 at minimum-viable depth. Backward
compatible with 0.1.0 configs and persistence.

### Added
- Runtime API-key rotation (B1.2.3). When `metadata_path` is
  configured the coordinator bootstraps config keys into the
  MetaStore and reloads the live registry every 60 s. Rotating a
  key no longer requires a coordinator restart.
- Read/write role routing (B2.2). New `ExecutorRole` config field
  (`either` / `read_primary` / `write_primary`); coordinator routes
  queries toward `ReadPrimary` and writes toward `WritePrimary`
  with a soft-preference fallback. Recommended high-write config
  (`replica_factor: 2` + `read_consistency_secs: 60` + role split)
  recovers mixed-RW read QPS to 82.5 % of the read-only baseline
  (was 38 % on 0.1.0).
- Compatibility policy (`docs/COMPAT_POLICY.md`, B3.5). Three-
  dimensional contract covering wire / persistence / config evolution
  rules.
- Branch-coverage baseline on the four critical modules
  (`docs/COVERAGE_MATRIX.md`, B4-min). Numbers are the honest
  starting point; future PRs may only ratchet upward.
- Chaos harness (`chaos/runner.py`, B5-min) with two scenarios:
  `worker_kill` (SIGKILL during traffic) and `worker_stall`
  (SIGSTOP for 5 s). Ten iterations each, 100 % pass gate.
- Soak harness (`soak/run.py`, B6-min). Samples per-process RSS
  and open-fd count every 60 s; alpha gate is 2 h run with < 3 %
  drift.

### Changed
- `ApiKeyInterceptor.keys` moved from `Arc<HashMap>` to
  `Arc<RwLock<Arc<HashMap>>>` so reloads atomically swap the
  registry without blocking request handlers.
- `docs/LIMITATIONS.md` §11 now carries the B2.2 numbers and the
  recommended production config explicitly.

### Notes for Operators
- If you've pinned `deployment_profile: saas` in 0.2.0-pre.1, no
  further action needed — the key hot-reload activates as soon as
  you restart against the 0.2.0-alpha.1 binary.
- Legacy `api_keys` (non-RBAC form) still default to `Admin`; flip
  to `Read` is scheduled for 0.3.0. `COMPAT_POLICY.md` §9 tracks it.

## [0.2.0-pre.1] - 2026-04-18

**Pre-alpha on the SaaS-first track.** Opens the 0.2 cycle with the
statelessness audit, configuration guards, and schema-versioning
foundations. See `docs/ROADMAP_0.2.md` for the full 0.2 milestone
exit criteria; this release meets none of them yet but lays the
plumbing every later step needs. Backward compatible with 0.1.0
configs and data snapshots.

### Added
- `deployment_profile` config field (`dev` / `self_hosted` / `saas`).
  `saas` hard-rejects local `metadata_path` values at startup; new
  deployments should always pin this explicitly. See
  `docs/STATELESS_INVARIANTS.md` (Gaps C + D now closed).
- MetaStore snapshot now carries `schema_version`. Current version is
  1. Pre-0.2 snapshots (no field) auto-upgrade on next write; future
  versions (>CURRENT) are rejected at load time so an older server
  can't corrupt forward-version state.
- `HealthCheckResponse` advertises `server_version` (populated from
  CARGO_PKG_VERSION). Clients can use it for capability negotiation.
- `docs/ROADMAP_0.2.md` — single forward-looking plan for the 0.2
  cycle. Supersedes the now-archived Phase-1..17 plans under
  `docs/archive/`.
- `docs/STATELESS_INVARIANTS.md` — SaaS-readiness audit. ~75% of
  state is already object-store-hosted or local-cache-safe; the
  remaining 25% lives in 4 specific gaps (A-D), 2 of which close in
  this release.
- `docs/PROFILE_MIXED_RW.md` — ongoing profile for mixed read/write
  performance (B2). Includes the first empirical falsification:
  `read_consistency_secs` sweep shows H1 (version-tick cache churn)
  only explains ~25% of the 10 QPS writes degradation.

### Changed
- Connection pool now replaces the whole `WorkerState` (including
  the tonic Channel) whenever a worker transitions from unhealthy
  back to healthy, closing the post-restart UNAVAILABLE flake.
- `LIMITATIONS.md` §11: read/write mixed performance numbers refreshed
  against a Phase-18-enabled cluster. The worst-case degradation at
  10 QPS writes is now -62% (was -91% pre-Phase-18, measured on the
  same hardware).
- `--auto-shard` CLI flag now requires `deployment_profile=dev`.
  Non-dev profiles exit with a specific error; the `/tmp` output
  path is downgraded from info to warn so operators stop leaning on
  it. The full MetaStore-backed auto-shard bootstrap is tracked as
  B1.2.2b.
- Cargo workspace uses `default-members` to restrict plain
  `cargo build` / `cargo test` / `cargo clippy` to the six LanceForge
  crates. Ballista stays in the workspace for future SQL-analytics
  reuse (Moat M2) but no longer compiles by default.

### Removed
- `crates/common/src/persistent_state.rs` (562 LOC). Replaced by
  `MetaShardState` + the File/S3 MetaStore split in Phase 7; kept ~10
  self-referential tests propping up a dead code path. No production
  callers.

### Fixed
- Post-worker-restart flake in `e2e_ha_test.py` scenario 7 ("worker
  recovered, cluster fully healthy"). The retry window is now tight
  (3×1s) precisely because the warm-up fix above is supposed to
  converge within one health-check tick.

### Notes for Operators
- The default `deployment_profile` is `dev` to preserve 0.1.0
  behavior; 0.1-era configs work unchanged. Production deployments
  should pin `deployment_profile: saas` (for managed offerings) or
  `self_hosted` (for customer-run installs). Running without a
  pinned profile in production is now tracked as an audit finding.
- Short-term mitigation for mixed read/write QPS loss: set
  `cache.read_consistency_secs: 30` (or higher). Data shows this
  recovers ~25% of the QPS gap at 10 QPS writes without changing
  any code. Proper fix lands in B2.2 (planned: read/write path
  physical split).

## [0.1.0] - 2026-04-16

First public release.

### Added
- Distributed scatter-gather query engine over sharded Lance tables
- ANN vector search (IVF_FLAT/HNSW) with global TopK merge
- Full-text search (BM25 via Tantivy) with distributed merge
- Hybrid search (ANN + FTS) with RRF reranking
- Filtered search with scalar index acceleration
- Insert / Delete / Upsert via gRPC with shard-level routing
- CreateTable with auto-sharding across all healthy workers
- CreateIndex fan-out (IVF_FLAT, BTREE, INVERTED)
- DropTable with storage purge and metadata cleanup
- Size-aware Rebalance and manual MoveShard
- Python SDK (`pip install lanceforge`) with 16 methods
- LangChain VectorStore adapter
- API Key authentication with RBAC (Admin/ReadWrite/ReadOnly)
- TLS support (coordinator + worker mTLS)
- Prometheus metrics with latency histogram (P50/P95/P99)
- Health check endpoint (`/healthz`)
- REST status endpoint (`/v1/status`)
- Metadata persistence via MetaStore (file / S3) — survives restarts
- Graceful shutdown with cooperative background task lifecycle
- Query cache with byte-level memory cap (default 512MB)
- Config runtime validation on startup
- Kubernetes Helm chart with PDB, HPA, health probes
- Docker Compose one-command deployment
- Demo script (`./demo.sh`) for 5-minute local trial
- 272 unit tests + 27 SDK tests + 21 E2E test suites + 9 benchmark suites
