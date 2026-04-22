# LanceForge v3 Architecture — Single-Dataset Realignment

**Status**: **REVERSED in v0.3.0-alpha.3**. This doc describes the
single-dataset realignment that was landed in alpha.1 and alpha.2
and then reverted in alpha.3. Kept as design-record of the path we
chose not to take.

**Why reversed**: The single-dataset model collapses one query to
one worker. For LanceForge's target scale (100B rows × 1K dims), a
single query often needs CPU aggregation across many nodes — the
"scatter-gather" shape the multi-shard architecture provides. Until
Lance upstream ships fragment-level scheduling for cross-node
distributed execution, we keep the multi-shard model alpha.1
inherited.

**What survived the reversal**: AlterTable DROP / RENAME, Tags,
RestoreTable, CreateIndex full dispatch (including IVF_HNSW_SQ bug
fix), worker OpenTable/CloseTable, worker_select utility — all of
these are orthogonal to the shard count and remained in the tree
after alpha.3's revert, now working over the restored multi-shard
architecture.

**Original draft below, for historical reference.**

---

**Original status**: Design draft, Phase R0. Not yet implemented.
**Target release**: `v0.3.0-alpha.1`
**Supersedes**: `docs/ARCHITECTURE_V2.md` (multi-shard model)

---

## 1. Strategic framing

LanceForge v0.2 followed a model most open-source distributed vector
databases adopt: split one logical table into N physical datasets
(shards), each owned by a worker process. Writes hash-partition to a
shard; reads scatter to all shards and merge. This is how Milvus,
Weaviate, and Qdrant structure their clusters.

v3 rejects that model in favour of the lakehouse pattern that
Snowflake, BigQuery, Databricks SQL, and LanceDB Enterprise
converged on independently: **one logical table is one physical
dataset in object storage, and compute is a stateless pool that
reads it**. Data does not travel with workers. Workers are fungible
query executors that can be added, removed, or relocated without
moving bytes.

The reason the lakehouse pattern wins:

- **Storage and compute scale independently**. Peak ingest needs 8
  writers for an hour; steady-state query needs 2 readers. You don't
  pay for idle storage replicas on a compute node.
- **No data rebalance on cluster topology change**. Add a worker — it
  joins the read pool. Remove a worker — the others cover. No
  MoveShard RPC, no two-phase promotion, no "rebalance in progress"
  window.
- **Object storage is the durability contract**. Lance on S3/GCS/Azure
  is already 11-nines. Adding a second data-carrying layer (shards
  on local disk or NVMe) creates coherence problems without
  improving durability.
- **Lance's internal model is already fragment-based**. A Lance
  dataset consists of fragments; each fragment is a row-range chunk
  with its own files. Lance's scanner reads fragments in parallel
  (~256 concurrent by default). Our v2 "shard" is a layer that
  duplicates what Lance already provides via fragments.

v2 coupled compute to data because we treated each shard as a unit
of ownership. v3 decouples them. That is the single architectural
axis this realignment rotates.

## 2. Core primitives in the v3 model

### Table
A single Lance dataset at a URI. The URI is the only thing
MetaStore records about the table's physical state. Examples:
```
file:///var/lance/tenant_a/orders.lance
s3://lanceforge-prod/tenant_a/orders.lance
```

### Fragment (Lance-internal, not a LanceForge concept)
Row-range chunks inside a dataset. Lance creates them on ingest and
merges them on compaction. LanceForge does not name, track, or
route to fragments — that is entirely Lance's responsibility.

### Worker
A stateless query executor. Any worker can serve any table. Workers
hold an open `lancedb::Table` handle per active table in an LRU
cache; the underlying data is in object storage, not on the worker.
Killing a worker loses no data.

### Coordinator
Stateless RPC router. Picks a worker for each request using
consistent hashing on `(table_name, request_id)` over the healthy
worker pool. Coordinators share state via MetaStore.

### MetaStore
Cluster-level metadata: table registry (name → URI), API keys,
quotas, tags. Shrinks significantly vs v2 — no `shard_uris`, no
`ShardMapping`, no `TargetState` promotion.

### DDL lease
Retained for `CreateTable` and `DropTable` only (multi-coord
serialisation of top-level table operations). Removed for
`AlterTable` and `CreateIndex` — Lance's own manifest CAS handles
those atomically.

## 3. Request paths

### Read (AnnSearch / FtsSearch / HybridSearch / CountRows)

```
client
  └─> coord
        ├─ resolve table_name → uri (MetaStore, cached)
        ├─ pick worker via consistent-hash(table_name + request_id)
        └─> worker
              ├─ open/reuse cached lancedb::Table(uri)
              └─ delegate to Lance (parallel fragment scan, returns top-K)
        <─ result
  <─ result
```

One network hop to one worker. Lance handles fragment-level
parallelism inside the worker process. No scatter/gather.

### Write (AddRows / UpsertRows / DeleteRows)

```
client
  └─> coord
        ├─ resolve table_name → uri
        ├─ pick worker (same consistent-hash rule)
        └─> worker
              ├─ open cached lancedb::Table(uri)
              ├─ lance.add_rows(batch) -- OCC commit on manifest
              └─ return { commit_seq, table_version }
        <─ result
  <─ result
```

Multiple concurrent writers to the same table conflict on Lance's
manifest CAS and retry. Our v0.3.0-alpha.1 strategy: single-primary
writer per table at the coord (pick one worker deterministically
for writes to a given table). This avoids OCC retry storms. If
benchmarks show OCC handles N-writer contention acceptably, we
relax to multi-writer in a later release.

### DDL (CreateTable / DropTable / AlterTable / CreateIndex)

```
CreateTable:
  coord acquires DDL lease on (table_name)
  coord picks worker, issues CreateTable RPC
  worker creates lancedb::Table at chosen URI
  coord writes table_uris[name] = uri to MetaStore
  coord releases lease

AlterTable (ADD/DROP/RENAME/ALTER column):
  coord picks worker
  worker calls lance.add_columns / drop_columns / alter_columns
  Lance manifest CAS makes the change atomic
  no DDL lease required
```

### Tags and time travel (new in v3)

```
create_tag(name):
  worker.table.tags().create(name, current_version)  -- Lance native

checkout_tag(name) / checkout(version):
  read request carries tag or version
  worker opens Lance at that version:
    table.checkout(v).await  or  table.checkout_tag(name).await
  scan executes on the historical snapshot

restore_table(tag_or_version):
  AlterTable-grade op, goes through DDL lease
  worker calls table.restore()
```

No cluster-level tag aggregation needed — tags live in the Lance
dataset natively, single source of truth.

## 4. What changes from v2

### Removed proto messages
- `CreateLocalShardRequest`, `LoadShardRequest`, `UnloadShardRequest`
- `MoveShardRequest`
- `LocalAlterTableRequest::shard_name`, `LocalWriteRequest::target_shard`
- `HealthCheckResponse::shard_names`
- `ShardRoutingEntry`, `ShardUri`

### Removed MetaStore keys
- `shard_uris`
- `shard_mapping`
- `target_state` (shard assignment promotion)
- Per-shard replica metadata

### Removed code modules (~1,080 LOC net)
- `crates/common/src/shard_state.rs`
- `crates/meta/src/shard_manager.rs`
- `crates/common/src/shard_pruning.rs`
- `crates/coordinator/src/scatter_gather.rs` (collapses to trivial 1-of-N pick)
- `crates/common/src/partition.rs` (`partition_batch_by_hash`)

### Retained
- gRPC RPC names (AnnSearch, AddRows, AlterTable, etc.) — same outward API
- `commit_seq` for RYOW barrier (now equals `table.version()`)
- DDL lease for CreateTable/DropTable
- Auth + quota + audit (quota becomes per-table, not per-shard)
- IDX compaction loop (now one pass per table)
- Health-checked worker pool
- Python SDK interface (minor field removals, method names stable)

### New capabilities
- DROP / RENAME / ALTER COLUMN (pass-through to Lance)
- Tags (create/list/delete/get)
- Time travel (`checkout(version)` or `checkout_tag(name)`)
- Scalar indexes: BITMAP, LABEL_LIST (in addition to BTREE)
- Vector indexes: IVF_PQ, IVF_HNSW_SQ (proto already advertised, now wired)
- `restore_table(tag_or_version)`

## 5. Strategic tradeoffs we accept

### Cold worker start latency
A worker picking up a table it hasn't served before pays one
object-storage round trip to load the manifest plus potentially
fragment metadata. R0.5 benchmark B2 measures this; if >1s, R2
adds a warm-pool mechanism (workers pre-open tables flagged as
"hot" in MetaStore).

### Write concurrency ceiling on one table
Single Lance dataset + manifest CAS means write throughput on one
table is bounded by how fast Lance can serialise commits (typically
thousands per second on S3, higher on local disk). Users who need
higher per-table write rates split into multiple tables — same
strategy Snowflake / BigQuery push.

### No "data locality to worker"
With fungible workers, a query to `orders` might hit worker A at
10:00 and worker B at 10:05. Worker B pays a cold manifest fetch.
Mitigated by consistent-hash routing (same table tends to same
worker while the worker stays healthy) and LRU caching. Local
NVMe fragment cache (Enterprise's trick) is a 0.4 add-on.

### Breaking change for v0.2 users
Proto fields removed; multi-shard data layout does not work under
v3. `lance-admin migrate` (Phase R4) converts v2 → v3 offline.

## 6. Non-goals for v3

These are acknowledged gaps, not denied future work:

- NVMe fragment cache with consistent-hash locality (Enterprise parity) — v0.4
- Cross-region replication — independent effort, not coupled to realignment
- SQL interface — orthogonal
- Admin UI / operator dashboards — product layer, not data plane
- Embedding / reranker SDK-level integration — sits outside the server

## 7. Acceptance signals for completion

v3 is "done" (v0.3.0-alpha.1 cut) when:

1. All Phase R1–R6 exits green
2. Parity checklist 100% (v0.2 capabilities preserved or intentionally deprecated with note)
3. E2E suite 100% green in fungible-worker topology
4. Chaos suite passes 20/20 under "kill any worker"
5. Bench p50/p99 within +10% of v0.2 baseline for equivalent scale
6. Migration tool passes integration test: synthetic v2 data → migrate → v3 reads match
7. Design doc (this file) is updated to describe the as-built system

## 8. Phases (summary; see PLAN file for details)

| Phase | Gate |
|---|---|
| R0 | Design doc + parity checklist + CHANGELOG committed |
| R0.5 | Pre-flight benchmarks (OCC, cold start, scalar index) |
| R1a | Worker registry per-table; coord tolerates via compat shim |
| R1b | Coord single-worker dispatch; compat shim removed |
| R1c | MetaStore/proto slim; Python SDK updated |
| R2 | Fungible worker routing + cold-start handling |
| R3 | Lance-native pass-throughs (DDL extended, tags, time travel, index types) |
| R3.5 | Parity checklist verified end-to-end |
| R4 | Migration tool + integration test |
| R5 | Docs + OTEL tracing |
| R6 | Dead code purge + full regression + v0.3.0-alpha.1 tag |

Total: 6–7 weeks single-person.

## 9. Design decisions logged (ADRs)

ADRs live under `docs/adr/` and are written as the R0.5 benchmarks
resolve each open question:

- ADR-001 Single-primary writer per table vs open multi-writer OCC
- ADR-002 Cold-start mitigation strategy
- ADR-003 Scalar index as the partition-pruning replacement
- ADR-004 DDL lease scope in v3
- ADR-005 Migration tool guarantees (atomicity, reversibility)

Empty at R0 commit; populated as decisions are made.
