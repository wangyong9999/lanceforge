# ADR-003: Scalar BTREE index as the replacement for v0.2 hash partitioning

**Status**: Accepted (conservative default; validated in R6)
**Phase**: R0.5

## Decision

Equality-filter queries that v0.2 served by routing to one shard
(`hash_route_shard` in `shard_pruning.rs`) are served in v0.3 by:

1. A scalar BTREE or BITMAP index on the filter column, built via
   `CreateIndex(..., index_type="BTREE" or "BITMAP")`.
2. Lance's predicate pushdown narrowing the fragment scan.

Users who previously declared `on_columns: ["tenant_id"]` for
partitioning get the same pruning effect by creating a scalar index
on `tenant_id`. The `on_columns` field on write RPCs becomes a
hint for `merge_insert` key semantics only — no longer a
partitioning directive.

## Why conservative

Lance's scalar indexes are well-established (BTREE for high-
cardinality, BITMAP for low-cardinality, LABEL_LIST for list
columns). They sit on top of fragment metadata and deliver
predicate pushdown without needing a separate dataset per shard.

We believe scalar-index + fragment pruning will match or beat
hash-partition routing because:
- Fragment count scales with data volume, not with shard count.
- Fragment pruning supports multi-column predicates and range
  queries, which v0.2 hash partitioning did not.
- No cross-shard result merge.

Measuring this precisely needs benchmark B3. Rather than gate on
that, we proceed on the belief and validate in R6.

## What this implies for R1b, R3, and R4

- **R1b**: write path no longer partitions batches by hash. Whole
  batch goes to one worker.
- **R3**: BITMAP and LABEL_LIST scalar index types added to
  `CreateIndex` dispatch (previously only BTREE existed). Docs
  describe the equivalence: "v0.2 partitioning on column X is
  now achieved by creating a scalar index on X."
- **R4**: migration tool prompts the user — "detected on_columns
  partitioning on {cols}; v0.3 equivalent is a scalar index on
  those columns. Auto-create? [y/n]".
- **MIGRATION_V2_TO_V3.md** walks through this equivalence.

## Rejected alternatives

- **Lance partition columns**: Lance does support physical
  partitioning. Using it means the v2 hash-partitioning logic lives
  on, just inside Lance. But it's a heavier commitment (data
  physically reorganised), and scalar indexes give the same
  predicate-pushdown effect with a lighter footprint.
- **Keep hash-partition logic at coord**: hard to justify — we're
  realigning *away* from that coupling.

## Validation (deferred to R6)

Benchmark B3: 10M rows, column with 100 distinct values. Compare
v2 baseline (hash-partition + fan-out) vs v3 candidate (single
dataset + BTREE + predicate). Equality query at 50 QPS,
concurrency 10. Assert v3 within +30% p99 of v2.

If v3 is > 30% slower → consider Lance partition columns as a
backup structural replacement.

## Revisit triggers

- B3 shows unacceptable regression on equality-filter workloads.
- User reports scalar-index rebuild time is prohibitive at
  production scale.
- Lance upstream adds a new partition primitive we should adopt.
