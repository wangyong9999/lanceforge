# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
