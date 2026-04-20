# LanceForge Release Notes

## 0.2.0-beta.4 — 2026-04-20

**Coverage hardening + one real bug found.** The beta.3 postmortem
identified eight potential issues across performance / functionality /
coverage. beta.4 closes all eight plus one more: the SECURITY.md §6.3
SSE-C claim turned out to be **overstated** — empirically the
customer-key headers never reach MinIO. Fixed the docs, tested what
actually works (bucket-level SSE-S3), and filed SSE-C per-request
keys as a 0.3 item with concrete technical reasoning.

### What was broken or understated in beta.3

- **SSE-C was documented as supported, but wasn't.** `object_store`'s
  `ObjectStore` trait has no hook for per-request headers; the
  `aws_server_side_encryption_customer_*` keys in `storage_options`
  pass through to `parse_url_opts` but `parse_url_opts` doesn't
  recognise per-put headers. Objects land unencrypted. Verified
  empirically against MinIO; SECURITY.md §6.3 rewritten.
- **`lance-admin shards copy` loaded whole objects into memory**
  (`get().bytes().await` on every object). A 100 MB shard data file
  would OOM the CLI. Now streams via `put_multipart` above a
  configurable threshold (default 8 MB).
- **Single failing object aborted the whole DR copy**. Now there's
  `--continue-on-error` + failure-summary report so operators can
  finish a catch-up sync.
- **Namespace prefix matching had unit tests only**. Added
  property-based tests (512 cases each) covering: always-denies-
  outside, always-allows-inside, filter preserves order + is
  subset, unbound-key is pass-through.
- **MetaStore S3 branch was "tested" via `file://` URLs**. Added a
  real-MinIO integration test (`MINIO_E2E_TEST=1` gate) covering
  CAS conflicts, cross-restart persistence, list_keys prefix
  filter.
- **Write path had no per-shard latency log** (H13 covered reads
  only). Added in add_rows — operators can now spot which shard
  is the write outlier.
- **`audit_dropped_total` counter was only unit-tested**. Added an
  e2e that configures audit_log_path at an unwritable path,
  fires 1 CreateTable + 5 AddRows, scrapes `/metrics`, asserts
  counter ≥ 6.
- **Worker OTLP spans didn't surface `trace_id` explicitly**. Now
  recorded as a first-class field on the span so Jaeger / Tempo
  UI filters work without raw `traceparent` parsing.

### Changes in detail

- **D1 MinIO SSE-S3 e2e** (`e2e_minio_sse_test.py`): starts a
  2-worker cluster against a MinIO bucket with default-encryption
  AES256 set, runs CreateTable + AddRows + AnnSearch, asserts
  every written object has `ServerSideEncryption: AES256` in MinIO
  response headers. Wired into `nightly-soak-chaos.yml` via a
  GitHub Actions `services: minio:` block — no extra plumbing
  needed on the user side.
- **D2 `lance-admin shards copy` streaming**: refactored the
  per-object copy to pick `put` for small objects and
  `put_multipart` for large ones. Configurable threshold via
  `--multipart-threshold`. 1 MB payload with 256 KB threshold
  test confirms byte-exact round-trip through the multipart path.
- **D3 `--continue-on-error`**: flag + `failures: Vec<(String,
  String)>` + end-of-run summary. Test covers the "partial copy"
  case where one shard directory is missing.
- **D4 namespace proptest** (auth.rs): four properties,
  `ProptestConfig::cases = 512` each → ~2000 generated cases every
  run. Covers prefix-collision (tenant-a vs tenant-abc) + order-
  preservation + unbound-key pass-through.
- **D5 MetaStore S3 integration test**: `MINIO_E2E_TEST=1`
  activates; real HTTP round-trip against the local MinIO. CI
  gate runs it inside the `minio-sse` job. Auto-creates the
  bucket via a small boto3 bootstrap so CI doesn't need manual
  provisioning.
- **D6 audit_dropped e2e** (`e2e_audit_dropped_test.py`): triggers
  the sink-open failure path, scrapes `/metrics`, asserts counter
  goes from 0 to exactly 6 after 1 CreateTable + 5 AddRows.
- **D7 worker `trace_id` span field**: explicit
  `fields(trace_id = tracing::field::Empty)` + runtime
  `Span::current().record("trace_id", tid)`. Same pattern on
  execute_local_search and execute_local_write.
- **D8 write-path per-shard latency log**: H13 parity for
  add_rows fan-out. Slow-write threshold = `server.slow_query_ms`;
  timeout error messages now include observed ms.

### Regression results

| Gate | beta.3 | **beta.4** |
|---|---:|---:|
| workspace lib | 320 | **324** (+4 proptest, +1 meta) |
| admin lib | 6 | **10** (+2 D2+D3, +2 D5-related were already there) |
| SDK unit | 38 | **38** |
| E2E suites | 9 | **11** (+e2e_minio_sse +e2e_audit_dropped) |
| Chaos | 10+10 | 10+10 |
| Soak 1h | 0 err | 0 err |
| **Total tests** | **373** | **410+** (proptest cases count separately) |

### Deferred to 0.3 (with new specificity)

- **SSE-C per-request customer keys**: requires a custom
  `object_store::ObjectStore` wrapper that injects SSE-C headers
  on every `put`/`get`. Not a config-only fix — explicit
  engineering work.
- **B2.3 Write batching**: still the biggest mixed-RW lever; C2
  data confirmed it applies to both local fs and S3.
- **Coord RSS step root-cause**: audit channel hypothesis ruled
  out (C1); remaining candidates tracked in LIMITATIONS §13.

### Upgrade from 0.2.0-beta.3

- No wire or on-disk schema break.
- SECURITY.md §6.3 rewritten. If you had set
  `aws_server_side_encryption_customer_*` keys in
  `storage_options` expecting SSE-C, please be aware those keys
  **were not** encrypting anything. Objects in your bucket are
  plain. Move to bucket-level SSE-S3 / SSE-KMS for transparent
  at-rest encryption today.
- `lance-admin shards copy` command surface gained two flags
  (`--continue-on-error`, `--multipart-threshold`). Defaults
  match previous behaviour for `--multipart-threshold` only (8 MB
  means small-object-first-then-multipart is new; previously
  everything was single-put). `--continue-on-error` defaults off
  (fail-fast, same as before).

Full change list: [`CHANGELOG.md`](./CHANGELOG.md).

---

## 0.2.0-beta.3 — 2026-04-20

**Observability + OBS-native audit + async SDK.** Closes six of the
seven 0.3-deferred items from beta.2 at minimum-viable depth (B7
characterisation soak is still running at tag time; result merged as
LIMITATIONS §13 update in a follow-up). Plus one unrelated bug
surfaced by the new B1 end-to-end coverage: beta.2 namespaced reads
never worked because the worker's shard-name matcher didn't apply
the same `/` → `__` sanitisation as the writer. **You'll see real
results from G5 namespaces for the first time in this release.**

### What was broken or missing in beta.2

- **G5 namespaced reads returned `No Lance shard found`.** The unit
  test covered `check_namespace()` logic; `t_f3d` exercised the
  *rejected* path; no test exercised the *accepted* read path on a
  namespaced table. beta.3 adds the test + the
  `resolve_tables_inner` sanitisation fix.
- **Audit sink rejected OBS URIs at startup.** Fine for compliance
  integrity, useless for operators who actually wanted OBS
  destinations. beta.3 ships a put-per-batch sink (100 records
  OR 30s) that creates one object per batch under the configured
  prefix; SIEM shippers (Vector, Fluent Bit) ingest via
  prefix-list + sort.
- **Worker logs didn't carry the client's trace_id.** beta.2 wrote
  it to coord audit lines (F9) but coord never re-emitted
  `traceparent` on outbound worker RPCs. beta.3 does.
- **`ListTables` did full-scan + client-filter for namespaced
  callers.** Fine on small clusters, wasteful at scale. beta.3
  pushes the filter down to the MetaStore key level.
- **No Grafana dashboard.** beta.3 ships one.
- **No OTLP integration.** beta.3 ships a feature-gated OTLP
  exporter.
- **Sync-only Python SDK.** beta.3 ships `AsyncLanceForgeClient`.

### Changes in detail

- **B1 worker-side traceparent re-emission.** `scatter_gather` gained
  a `trace_id: Option<String>` parameter. Each outbound
  `execute_local_search` Request carries a fresh `traceparent` with
  the same 32-hex trace_id. Worker logs `local_search trace_id=<hex>
  table=<name>` at info! level. `grep trace_id=<hex>` across coord +
  worker logs now returns a continuous request story.

- **B2 OBS-native audit log (put-per-batch).**
  `audit_log_path: s3://…` no longer fails startup. AuditSink
  batches up to 100 records or 30 s (whichever first) and PUTs one
  object `audit-{epoch_ms}-{rand_hex}.jsonl` under the configured
  prefix. Uses the `object_store` crate with env-based credentials,
  same pattern as Lance / S3MetaStore. PUT failures count every
  record in the batch as dropped (`lance_audit_dropped_total`).

- **B3 optional OTLP exporter.** New `otel` feature on the
  coordinator crate: `cargo build --features otel`. Operator sets
  `OTEL_EXPORTER_OTLP_ENDPOINT`; coord exports tracing spans to the
  collector at that endpoint. Default build pulls zero OTEL deps.
  See `docs/OBSERVABILITY.md` for the end-to-end pipeline.

- **B4 Grafana dashboard**: `deploy/grafana/lanceforge.json` +
  README + Prometheus alert snippets. Covers QPS / latency /
  error rate / worker health / audit-dropped (compliance) / per-
  table breakdowns.

- **B5 namespace filter pushed down to MetaShardState.**
  `all_tables_for_namespace(ns)` lists only keys under
  `{prefix}/tables/{ns}/` at the MetaStore layer. `list_tables`
  routes through it when the caller is namespace-bound. Four-case
  unit test covers own-ns, cross-ns, empty-ns, prefix-collision
  (tenant-a vs tenant-ab) edges.

- **B6 `AsyncLanceForgeClient`.** `grpc.aio`-based async client
  covering `search` / `insert` / `create_table` / `drop_table` /
  `list_tables` / `count_rows`. Proxy env guard (F8 parity).
  Upsert, batch_search, create_index, rebalance are 0.3 follow-ups.

- **`resolve_tables_inner` sanitisation** (the beta.2-missed bug):
  worker now applies the same `/` → `__` mapping when matching
  a user-facing `table_name` against stored shard names, so reads
  work on namespaced tables for the first time.

- **`docs/OBSERVABILITY.md`** (new): three-pillar overview (metrics
  / logs / traces) with the cross-process correlation story, the
  opt-in OTLP pipeline, and an honest §4 known-gaps list.

### Regression results

| Gate | beta.2 | **beta.3** |
|---|---:|---:|
| workspace lib + admin | 319 + 6 | **320 + 6** (+1 B5 meta test) |
| SDK unit | 30 | **38** (+8 async client) |
| E2E phases | 6 / 6 | 6 / 6 |
| Chaos worker_kill / stall | 10+10 | 10+10 |
| New: B1 e2e (trace_id in worker log) | — | PASS |
| New: namespace read path (tenant-a/orders AnnSearch) | broken | PASS |
| `cargo check --features otel` | n/a | clean |

### Deferred to 0.3

- Worker-side OTLP span emission (traceparent already flows; worker
  doesn't emit its own spans yet).
- REST `/v1/*` → in-process gRPC traceparent bridge.
- OTEL span attributes (`table`, `k`, `recall`) — depends on what
  operator investigations find most valuable in the wild.
- Coord-side sampling (everything is exported today; rely on a
  collector-side tail sampler at high QPS).
- Async SDK: upsert / batch_search / create_index / rebalance.
- Audit sink retry on PUT failure (0.3 exponential backoff).
- 4h soak characterization → merged as LIMITATIONS §13 update post-
  tag. beta.3 tags when the soak completes, with findings appended.

### Upgrade from 0.2.0-beta.2

- No wire break. Rolling restart of coordinators is sufficient.
- `audit_log_path: s3://…` now works. If you were using a
  workaround with an external shipper, you can simplify to native
  OBS destination (still fine to keep the shipper if it's doing
  enrichment).
- Build without `--features otel` gets you the stdout-only stack
  same as before. Enable OTEL only when you need distributed
  traces.
- Python SDK: `from lanceforge import AsyncLanceForgeClient` for
  async. Sync client surface unchanged.

Full change list: [`CHANGELOG.md`](./CHANGELOG.md).

---

## 0.2.0-beta.2 — 2026-04-20

**Posttag hardening for 0.2.0-beta.1.** The postmortem on the beta.1
tag identified six items where "the feature shipped" didn't mean
"the feature is actually wired through the request path." beta.2 is
the corresponding F-series (F1–F9) that closes those gaps, plus one
unplanned lancedb-panic fix that fell out of the new end-to-end
coverage. Backward compatible with 0.2.0-beta.1 on wire and on disk;
audit-log JSONL schema is additive (dedicated `trace_id` field) with
the old `details` substring preserved for one version.

### What was broken in beta.1

- **G5 namespace binding never worked for real writes.** lancedb 0.27
  `.unwrap()`s in its table-name validation when the name contains
  `/`. Every CreateTable under a `tenant-x/` prefix would panic the
  tokio task and return an opaque `h2 protocol error` to the client.
  The unit tests on `check_namespace()` passed, but no end-to-end
  gRPC test had ever fired a real CreateTable through the namespace
  path. **beta.2 ships the fix and the test that would have caught
  it.**
- **G7 audit sink silently dropped records on OBS URIs.** Operators
  who set `audit_log_path: s3://...` got a single `warn!` line at
  startup and then lost every subsequent record. beta.2 makes this
  a hard startup failure with a pointer at the real follow-up work
  in ROADMAP R3.
- **G7 audit sink channel-full was invisible to monitoring.** Drops
  emitted a `warn!` log, which is impossible to alert on reliably.
  beta.2 adds `lance_audit_dropped_total` as a Prometheus counter.
- **G9 `lance-admin meta restore --purge` had no safety gate.** A
  typo in `--meta` could wipe a live cluster. beta.2 adds
  `--dry-run` and `--yes`, requires confirmation on TTY when not
  supplied, and refuses to proceed on non-interactive stdin without
  `--yes`.
- **Admin CLI's S3 branch had zero automated coverage.** All three
  tests ran against FileMetaStore only. beta.2 exercises the
  S3MetaStore path end-to-end.
- **Python SDK silently routed localhost RPC through system proxy**
  on WSL2 / Codespaces / VPN machines — the exact H24 symptom that
  masqueraded as a LanceForge bug during alpha validation. beta.2
  strips `HTTP_PROXY` / `HTTPS_PROXY` / `GRPC_PROXY` / `ALL_PROXY`
  from the process env during `LanceForgeClient.__init__` (opt out
  with `respect_proxy_env=True`).
- **H25 had no regression test.** beta.1's soak caught an H21
  leftover that force-exited every coord after exactly 65 s. The
  fix landed but the next refactor could re-introduce it. beta.2
  ships an 80 s integration test that fails if coord dies before
  the 75 s mark.

### Changes in detail

- **F1 audit integrity**: `AuditSink::spawn` now takes a
  `metrics: Arc<Metrics>` handle and bumps `audit_dropped_count`
  on channel-full / sink-closed / file-write errors. A new
  `validate_audit_path()` rejects OBS schemes at config time with
  a diagnostic that points at the ROADMAP R3 item. Coordinator
  exits 2 on bad config.
- **F2 H25 regression test**:
  `lance-integration/tools/e2e_h25_coord_uptime_test.py`. 80 s
  wall-clock; nightly tier, not per-PR.
- **F3 namespace gRPC e2e**:
  `lance-integration/tools/e2e_beta2_ns_audit_test.py`. Exercises
  CreateTable / AnnSearch / ListTables through the full gRPC
  stack; 5 assertions spanning the allow path, the cross-ns reject
  path, and the list-filter path.
- **F4 audit-log e2e**: same file as F3 — asserts that real
  CreateTable + AddRows gRPC calls land JSONL records in the
  configured file, with the correct op / principal / target /
  trace_id.
- **F5 admin S3 round-trip**: `open_store_via_s3_branch_roundtrips`
  drives `cmd_dump` + `cmd_restore` through S3MetaStore (via
  `file://` URL, same pattern the meta crate's own tests use).
- **F6 admin safety flags**: `--dry-run` renders a create / update /
  delete plan and returns. `--yes` bypasses the TTY prompt for
  `--purge`. Non-interactive `--purge` without `--yes` refuses to
  proceed and leaves the target untouched. Four new tests.
- **F7 `SECURITY.md` §5 restructure**: storage-layer isolation
  caveat promoted to a boxed paragraph at the top of the section,
  ahead of the scope list.
- **F8 Python SDK proxy guard**: strips proxy env vars on
  `__init__`. Three new unit tests.
- **F9 `AuditRecord.trace_id` dedicated field**:
  `#[serde(default, skip_serializing_if = "Option::is_none")]`
  adds a top-level key alongside the in-details substring.
  Writers dual-write for one version; 0.3 drops the substring.
  Consumers should prefer the top-level field from beta.2 onward.
- **Lancedb panic fix (unblocks G5)**: coord sanitizes `/` → `__`
  in `shard_name` construction. Worker `create_local_shard` adds
  an input-validation guard that rejects names containing `/`,
  `..`, or a leading `.` — belt-and-braces against any other
  caller reaching the lancedb panic path. User-facing table_name
  keeps `/` everywhere else.

### Regression results

| Gate | beta.1 | **beta.2** |
|---|---:|---:|
| workspace lib + admin | 311 + 3 | **319 + 6** |
| SDK unit | 27 | **30** |
| E2E phases | 6 / 6 | 6 / 6 |
| Chaos worker_kill | 10 / 10 | 10 / 10 |
| Chaos worker_stall | 10 / 10 | 10 / 10 |
| New: F2 H25 uptime | — | PASS (75 s) |
| New: F3 + F4 ns/audit e2e | — | 7 / 7 |

### Deferred to 0.3

Same list as beta.1. The "worker RSS warmup step under sustained
read" question from LIMITATIONS §13 still needs a 4h+ soak to
characterise the step cadence — not done here, not a ship blocker
given the cache-warmup interpretation, but the commitment to
re-measure with a 0.3 harness change stands.

### Upgrade from 0.2.0-beta.1

- No wire or on-disk schema break. Rolling restart of coordinators
  is sufficient.
- If you set `audit_log_path: s3://...`, coordinator will now refuse
  to start; change it to a local path + an external log shipper,
  per `SECURITY.md` §7.
- SIEM parsers: start reading `trace_id` from the top-level JSONL
  field rather than parsing the `details` substring.
- Python SDK users on WSL2 / Codespaces previously had to manually
  `unset HTTP_PROXY`; that's now automatic. If you rely on the
  proxy for coord traffic (unusual), pass `respect_proxy_env=True`
  to `LanceForgeClient`.
- If you want to use G5 namespace binding with actual data writes,
  you can now: beta.1's silent lancedb panic is fixed. Nothing to
  change on your side.

Full change list: [`CHANGELOG.md`](./CHANGELOG.md).

---

## 0.2.0-beta.1 — 2026-04-19

**First beta on the SaaS-first track.** Closes the Severity-1 gaps
identified in `docs/COMPETITIVE_ANALYSIS.md` against Pinecone /
Weaviate / Qdrant / Milvus: multi-tenant namespace binding, per-key
QPS quota, persistent audit log, encryption-at-rest documentation,
operator CLI, and cross-process trace correlation. Backward
compatible with 0.2.0-alpha.1 on wire and on disk.

### Feature matrix (what's new in beta)

| Capability | 0.1.0 | 0.2.0-alpha.1 | **0.2.0-beta.1** |
|---|:-:|:-:|:-:|
| Three-role RBAC (Read/Write/Admin)           | ❌ | ✅ | ✅ |
| MetaStore-backed runtime key rotation        | ❌ | ✅ | ✅ |
| Per-key QPS quota (token bucket)             | ❌ | ❌ | ✅ **G6** |
| API-key ↔ namespace binding                  | ❌ | ❌ | ✅ **G5** |
| Persistent audit log (JSONL)                 | ❌ | ❌ | ✅ **G7** |
| Encryption-at-rest (SSE-KMS / SSE-S3 / SSE-C) | docs only | docs only | ✅ **G8** |
| `lance-admin` operator CLI                   | ❌ | ❌ | ✅ **G9** |
| W3C traceparent → audit log                  | ❌ | ❌ | ✅ **G10** |
| Mixed-RW read-QPS recovery (10 QPS W)        | 38% | 82.5% | 82.5% |
| Coverage on the 4 critical modules           | measured | ratcheted | ratcheted |
| Chaos 20-iter (worker kill + stall)          | — | 20/20 | 20/20 |
| Read-only 1h soak RSS/fd drift               | — | < 3% | **< 3%** (re-measured G4) |

### New capabilities in detail

- **G5 — Namespace multi-tenancy (minimal).** Each API key may carry
  a `namespace` field. Coordinator rejects any RPC whose `table_name`
  doesn't begin with `{ns}/` and filters `ListTables` to the caller's
  prefix. Admin keys without a namespace retain operator access. This
  is an API-layer boundary — storage-layer isolation (separate
  buckets / IAM roles per tenant) is still the operator's call. See
  `docs/SECURITY.md` §5 and `docs/LIMITATIONS.md` §9 for the explicit
  scope.
- **G6 — Per-key QPS quota.** `api_keys_rbac[].qps_limit` installs a
  token-bucket rate limit per key; exceeding the quota returns
  `ResourceExhausted`. Tokens are tracked in milli-units so
  sub-integer refill rates don't drift. (Already in alpha but
  production-ready in beta.)
- **G7 — Persistent audit log.** `security.audit_log_path` (local
  file or OBS URI) turns on an append-only JSONL sink. Every DDL and
  write RPC writes one record `{ts, op, principal, target, details}`;
  records carry the `trace_id` from the caller's W3C `traceparent`
  header when present. OBS paths currently drop-with-warn (no
  uniform append semantics across object stores) — point at a local
  path and ship with Vector / Fluent Bit until 0.3.
- **G8 — Encryption-at-rest.** `docs/SECURITY.md` §6 covers SSE-KMS,
  SSE-S3, and SSE-C configuration end-to-end, including IAM/KMS
  permission minimums and rotation paths. The object-store layer
  passes all encryption options through unchanged — LanceForge adds
  no second envelope.
- **G9 — `lance-admin` CLI.** Operator tool for MetaStore backup
  (`meta dump`), restore (`meta restore --purge`), listing, and
  shard-URI inspection. Copying the Lance data directories
  themselves is left to cloud-native tooling (`aws s3 sync`, etc.)
  — `lance-admin shards list` prints the URIs so ops can drive that
  with one command.
- **G10 — Traceparent pass-through (minimal).** Coordinator parses
  the W3C `traceparent` metadata and writes the 32-char trace_id into
  both the stdout `audit` log line and the JSONL record. Worker-side
  re-emission (so a distributed trace spans coord → worker) is
  scheduled for 0.3 — this minimal version still lets SREs correlate
  client logs ↔ coordinator audit line by `trace_id`.

### Build hygiene

- Dev profile now builds with `debug = "line-tables-only"` +
  `split-debuginfo = "unpacked"`. Integration-test binaries shrink
  from ~1.5 GB to ~300 MB; a full `target/` from an active dev cycle
  no longer blows past 400 GB. See `docs/BUILD_HYGIENE.md` for the
  command aliases (`cargo small`, `cargo prune`).
- Test-flake fix: `crates/meta/src/state.rs` tests migrated off
  hardcoded `/tmp/lanceforge_metastate_*.json` paths (nanosecond-
  clash races under parallel tokio-test) to `tempfile::tempdir()`.
  Same root cause as H23 (store.rs); state.rs was missed at the
  time.

### Not yet in beta (deferred to 0.3)

- OBS-native append for the audit log (OBS paths warn-and-drop
  today). Tracked as `ROADMAP_0.2.md` R3.
- Full OpenTelemetry exporter pipeline (OTLP collector integration).
  The traceparent header is parsed and persisted; emitting spans to
  Jaeger/Tempo is 0.3 work.
- Grafana dashboard template bundle.
- Async Python SDK (`grpc.aio`). Current SDK is sync.
- Namespace-level quota + ListTables enforcement via the MetaStore
  prefix (today the filter lives at the auth layer).
- Mixed-RW writes > 10 QPS — still 18.5% of baseline at 50 QPS on
  local fs. `PROFILE_MIXED_RW.md` documents the root cause
  (manifest-commit-storm + fsync barriers); the S3 path isn't
  impacted the same way but isn't bench-validated yet.
- `lance-admin` live-migration subcommand. Today's `meta
  dump`/`restore` is enough for disaster-recovery and cross-cluster
  seeding; doing online migration is 0.3.

### Upgrade notes (from 0.2.0-alpha.1)

- No wire or on-disk schema changes. A rolling restart of
  coordinators is sufficient.
- To use G5, add `namespace` to the relevant entries in
  `api_keys_rbac`. Keys without the field keep 0.1.x / alpha
  behaviour.
- To use G7, set `security.audit_log_path` to a writable local
  path. The directory is created if missing; records land one line
  per RPC.
- `Cargo.toml` dev-profile changes affect only local dev — release
  builds (`cargo build --release`) are unchanged.

### Known gotchas

- The `bench_sustained_load.py` and `bench_phase17_matrix.py`
  harnesses now clear `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` env
  vars at import — if you run them through a WSL2 shell that has
  a system proxy configured (`http://127.0.0.1:7897` is the common
  one), this is what was masquerading as "k=10000 60% error rate"
  in alpha's H24. The fix is unconditional; you don't need to do
  anything.

Full change list: [`CHANGELOG.md`](./CHANGELOG.md).

---

## 0.2.0-alpha.1 — 2026-04-18

**First alpha on the SaaS-first track.** Meets every gate in
`docs/ROADMAP_0.2.md` §3.3 (alpha exit criteria) at minimum-viable
depth. Backward compatible with 0.1.0 on wire and on disk.

### Highlights beyond 0.2.0-pre.1

- **B1.2.3 runtime API-key rotation** (closes `STATELESS_INVARIANTS`
  Gap A). When a MetaStore is configured, the coordinator bootstraps
  config keys into `auth/keys/` and a 60 s background task swaps the
  live registry atomically. Adding or revoking a key no longer
  requires a rolling restart.
- **B2.2 read/write role routing**. New `ExecutorRole::{Either,
  ReadPrimary, WritePrimary}` config knob; coordinator picks the
  best-matching worker per request. Combined with `read_consistency_
  secs=60` and `replica_factor=2`, the 10 QPS writes mixed workload
  recovers to **82.5% of read-only baseline** (up from 38% on
  0.1.0). Recommended production config is in `docs/LIMITATIONS.md`
  §11.
- **B3.1 / B3.2 version advertising** (already in pre.1): clients can
  discover server version via `HealthCheckResponse.server_version`;
  MetaStore JSON snapshots carry `schema_version = 1` with a
  forward-version reject gate and an auto-upgrade-on-write path for
  pre-0.2 snapshots.
- **B3.5 compatibility policy** — `docs/COMPAT_POLICY.md` formalises
  the three-dimensional contract (wire / persistence / config),
  deprecation windows, and break procedure.
- **B4-min coverage matrix** — `docs/COVERAGE_MATRIX.md` measures
  branch coverage on the four critical modules (scatter_gather /
  merge / metastore / connection_pool) and pins baselines that only
  ratchet up.
- **B5-min chaos framework** — `chaos/runner.py` + two scenarios
  (`worker_kill`, `worker_stall`), 10 iterations each, 100% pass
  rate required for alpha. Result archives in `chaos/results/`.
- **B6-min soak harness** — `soak/run.py` runs a light mixed
  workload and samples RSS / open-fd drift per minute. The alpha
  gate is 2 h with < 3 % drift; a 15-minute smoke run ships with
  this release.

### Not yet in alpha

- B2.2 does not yet close the 50 QPS writes scenario (still -81 %
  of baseline). That needs fragment-level cache invalidation and
  is a 0.2-beta item.
- B4's full ≥ 85 % branch coverage on the four modules — we ratchet
  per-release, the current numbers are the honest baseline.
- B5 full chaos fault library (net partition / OOM / S3 throttle /
  clock skew) and the nightly 100-iter CI job — beta.
- B6 nightly 24 h soak CI job — beta. The 2 h run is a release-
  time human-triggered gate for alpha.
- B3.3 `lance-admin migrate` CLI — deferred until we have a real
  schema migration to dispatch (probably 0.3).

### Upgrade notes (from 0.2.0-pre.1 or 0.1.0)

- Adding `deployment_profile: saas` to production configs will
  start rejecting local `metadata_path` values at boot. If your
  cluster is on `file://` MetaStore today, either keep
  `deployment_profile: self_hosted` (or unset — defaults to `dev`)
  or migrate the JSON to S3 first.
- Existing YAML with no `deployment_profile` continues to work.
- Legacy `api_keys` (non-RBAC format) still default to `Admin` in
  0.2.x. The flip to `Read` is scheduled for 0.3.0; see
  `COMPAT_POLICY.md` §9.

Full change list: [`CHANGELOG.md`](./CHANGELOG.md).

---

## 0.2.0-pre.1 — 2026-04-18

Opens the 0.2 / SaaS-first cycle. Backward compatible with 0.1.0 on
wire and on disk; pins the plumbing every later step needs.

**Highlights**:
- `deployment_profile` config field with startup-time guards
  (`saas` rejects local `metadata_path`; `self_hosted` requires
  explicit MetaStore).
- MetaStore JSON snapshot now version-stamped (`schema_version=1`);
  reads auto-upgrade from pre-0.2 snapshots, forward-version
  snapshots rejected.
- `HealthCheckResponse` advertises `server_version`.
- Connection-pool flake fixed: stale tonic Channel is now replaced
  whenever a worker transitions back from unhealthy.
- Ballista logically isolated from the default `cargo build` /
  `test` / `clippy` path via `default-members`; still buildable with
  explicit `--workspace`.

**Not yet**: the remaining 0.2 Blockers (B2 mixed-RW cache tiering,
B4 critical-path coverage matrix, B5 chaos framework, B6 24h soak)
are tracked in `docs/ROADMAP_0.2.md`. This is a **pre-alpha** — it's
safe to adopt on 0.1 workloads that already run here, not yet
recommended for net-new production deployments.

Full change list: [`CHANGELOG.md`](./CHANGELOG.md). Forward plan:
[`docs/ROADMAP_0.2.md`](./docs/ROADMAP_0.2.md). Honest limitation
inventory: [`docs/LIMITATIONS.md`](./docs/LIMITATIONS.md) +
[`docs/STATELESS_INVARIANTS.md`](./docs/STATELESS_INVARIANTS.md).

---

# LanceForge 1.0-rc Release Notes

**Release date**: 2026-04-15
**Tag**: `1.0-rc1`

## TL;DR

LanceForge is a distributed vector retrieval engine on top of Lance + object storage. After 17 phases of focused development, it now has:

- **正确性**：写入无重复、Saga 回滚、fsync、merge_insert hash-partition
- **安全**：API key + 三级 RBAC + TLS + 审计日志
- **运维**：MoveShard / Rebalance / Compaction / Orphan GC / 慢查询日志 / per-table metrics
- **弹性**：分页、响应大小上限、副本读 failover、saga 失败回滚
- **观测**：Prometheus 指标 + 慢查询日志 + 审计日志 + 健康检查
- **文档**：8 份完整文档 + 1 个真实 RAG 端到端示例
- **测试**：272 单测 + 6 套 E2E + 4 种规模 benchmark + RAG 质量验证

实测性能（128d，单机 WSL）：
- 100K × shards=1: **1927 QPS @ P99 6.5 ms** (read-only, conc=10)
- 1M × shards=2: **1774 QPS @ P99 7.0 ms** (read-only, conc=10)
- 1M × nprobes=80: **0.91 recall@10 @ 1889 QPS**
- BEIR SciFact RAG: nDCG@10 / Recall@100 与 numpy 暴力 baseline **完全一致**

## What's New since legacy releases

### Phase 11-16 — Core engine improvements (committed previously)

| Phase | 主题 |
|---|---|
| 11 | Auto-shard CreateTable + 写入无重复 + size-aware rebalance |
| 12 | MetaStore fsync + CreateTable saga 回滚 + RBAC 三级 |
| 13 | 分页 offset + 响应大小上限 + 审计日志 + 自动 Compaction |
| 14 | MoveShard 管理 RPC + 慢查询日志 + per-table 指标 |
| 15 | Delete/Upsert shard-level 路由 + DropTable 真删 OBS 文件 |
| 16 | Upsert hash-partition + 孤儿存储 GC |

### Phase 17 — Production validation (this release)

**5 个真实 bug 通过 benchmark + RAG E2E 暴露并修复**：
1. gRPC 默认 4 MiB decoding cap → 提升到 256 MiB+ 16 MiB
2. Client send 256 MiB cap → benchmark 改分块 ingest，coord default 升级
3. CreateTable 不自动建索引 → docs 强调，bench 显式建
4. conc=50 QPS 崩溃（误判服务端，实为客户端单 channel 序列化）→ docs 三处警告 + 修 bench
5. RAG nDCG 异常低（_distance sign 约定）→ rank-based scoring

**经验公式实测**：1M × 128d × 128 partitions 下达到 0.9 recall 需要 nprobes=80（之前文档预测的 30 偏低）

**8 份核心文档**：
- `docs/QUICKSTART.md` - 10 分钟首次跑通
- `docs/ARCHITECTURE.md` - 设计反思（为什么不做 2PC/quorum/resync）
- `docs/CONFIG.md` - 每个 YAML 字段
- `docs/DEPLOYMENT.md` - Helm chart + K8s
- `docs/OPERATIONS.md` - SRE runbook
- `docs/TROUBLESHOOTING.md` - 错误码 → 修复
- `docs/BENCHMARK.md` - 实测数据
- `docs/LIMITATIONS.md` - 11 条诚实边界

**第一个真场景**：`examples/rag_beir/` — BEIR SciFact RAG 端到端，质量与 numpy 暴力 baseline 完全一致

## Breaking Changes

无。Phase 11-17 全部向下兼容已部署的 YAML 配置和 gRPC proto。

## Default Changes（值得注意）

- `server.max_response_bytes`: 64 MiB → **256 MiB**（避免大 IPC 报错）
- gRPC 服务端 decoding/encoding limits 自动设为 `max_response_bytes + 16 MiB`
- gRPC 客户端连接池：256 MiB 收发上限

旧配置无需修改即可工作。

## Known Limitations

详见 [LIMITATIONS.md](docs/LIMITATIONS.md)，简要：

1. Upsert 与 Add 路由不一致（hash vs round-robin）→ 罕见场景下同 id 可能两份
2. `replica_factor` 是读并行度，不是数据副本
3. orphan_gc 当前仅本地 fs，对象存储 GC 待加
4. 多 coordinator 必须用 S3MetaStore；FileMetaStore 仅单机
5. **客户端必须每线程一个 gRPC channel**（Phase 17 实测教训）

## Performance Tips

```yaml
# 1M+ 行场景推荐起始配置
server:
  max_response_bytes: 268435456    # 256 MiB
  max_k: 10000
  slow_query_ms: 500

replica_factor: 2                   # 1M 行最优
```

```python
# 客户端：每线程一个 channel
def make_stub():
    return lance_service_pb2_grpc.LanceSchedulerServiceStub(
        grpc.insecure_channel("coord:9200",
            options=[("grpc.max_receive_message_length", 256*1024*1024),
                     ("grpc.max_send_message_length",   256*1024*1024)]))

# 高并发场景
with ThreadPoolExecutor(max_workers=50) as ex:
    futures = [ex.submit(lambda: make_stub().AnnSearch(...)) for _ in range(N)]
```

## Roadmap (driven by real user feedback)

不预先做的事，等触发条件：
- ⏳ Add hash-by-PK（等用户报 Upsert 重复 issue）
- ⏳ OpenTelemetry tracing（等首次跨节点排障失败）
- ⏳ 对象存储 GC（等 S3 客户账单异常）
- ⏳ Python SDK 异步 + 流式（等真实大批量场景）
- ⏳ Schema evolution（等真用户需求）

## Compatibility

- Rust: 1.70+
- Python: 3.10+
- Storage: S3 / GCS / Azure / 本地 fs
- K8s: 1.24+ (Helm 3)

## Acknowledgments

Built on [Lance](https://github.com/lancedb/lance) + [LanceDB](https://github.com/lancedb/lancedb) for storage and IVF indexing.

## How to Verify This Release

```bash
# 1. Unit tests
cargo test --release --workspace --lib

# 2. E2E suite (Phase 11-16) + smoke benchmark (~3 min)
bash lance-integration/tools/run_all_e2e.sh

# 3. Real-world RAG (~2 min)
python3 examples/rag_beir/run_beir_scifact.py
```

期望产出：272 单测全过；E2E 6/6 + smoke benchmark 通过；RAG nDCG@10 = baseline 一致。
