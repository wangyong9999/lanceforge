# ADR-002: Cold-start strategy — LRU cache, no warm-pool in v0.3.0-alpha.1

**Status**: Accepted (conservative default; validated in R6)
**Phase**: R0.5

## Decision

Workers keep an LRU cache of open `lancedb::Table` handles. First
access to a table pays one open cost (manifest fetch + metadata
parse). Subsequent accesses hit the cache.

No warm-pool preloading in v0.3.0-alpha.1. Tables are opened on
demand when routed to a worker.

## Why conservative

Lance opens a dataset by reading the latest manifest file from
object storage. Manifest is a single protobuf blob, typically
tens of KB to low MB. On S3 in-region, GET latency is single-to-
low-double-digit ms. Fragment file handles are opened lazily on
first scan.

If the cold-open cost is acceptable (p99 < 500ms is a reasonable
threshold for a write-once-ish table), no preloading is needed.

We haven't measured LanceForge's cold-open cost yet. Rather than
gate R1 on that measurement, we ship with no warm-pool, and fold
warm-pool in as a follow-up if R6 benchmark B2 shows unacceptable
cold-start latency.

## What this implies for R1a and R2

- `OpenTable(name, uri)` is a pure cache-insert; cheap and idempotent.
- Routing (R2) uses consistent-hash so the same table usually lands
  on the same worker. Cold-open happens once per (table, worker)
  pair per worker lifetime.
- Cache eviction is LRU with a size limit from config
  (`worker.open_table_cache_size`, default 1024). Evicting a table
  handle has no durability impact (data stays in object storage).

## Rejected alternatives

- **Preload all tables at worker startup**: wastes memory on tables
  this worker might never serve under consistent-hash routing.
- **Warm-pool via MetaStore "hot tables" hint**: coordination
  surface we don't need to design yet.
- **Cross-worker handle transfer**: adds complexity for an
  unclear win; worker restart cost is unchanged.

## Validation (deferred to R6)

Benchmark B2: fresh worker, issue first query, measure p50/p99
cold-start. Local disk and S3 (same region / cross region).

If p99 > 2s at any tier → implement warm-pool before GA.
If 500ms < p99 < 2s → warm-pool is optional polish for alpha.2.
If p99 < 500ms → no further action needed.

## Revisit triggers

- B2 measures cold-start above 2s in a realistic tier.
- User reports "first query after failover is slow" as a
  production issue.
- Lance changes open semantics (e.g., eager fragment index load).
