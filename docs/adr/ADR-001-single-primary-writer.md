# ADR-001: Single-primary writer per table in v0.3.0-alpha.1

**Status**: Accepted (conservative default; validated in R6 benchmark)
**Phase**: R0.5
**Context**: single-dataset realignment

## Decision

Writes to a given table are routed to one deterministically-chosen
worker at the coord. We call that worker the "primary writer" for
the table. Selection is `consistent-hash(table_name)` over the
healthy worker pool — the same worker holds the write role for
the duration of its health.

Reads remain fungible: any healthy worker can serve reads.

## Why now, conservative

Lance's manifest commit uses optimistic concurrency (`ConditionalPut`).
Multiple writers on the same dataset will succeed as long as they
don't step on one another's manifest version. Under contention they
retry with backoff, up to a configured cap (default 20 retries).

We do not yet have evidence about how much contention LanceForge
workloads produce. Rather than gate R1 on benchmarks, we pick the
pessimistic setting:

- Single-primary-writer guarantees zero OCC conflicts on the hot path.
- Writes queue naturally inside one worker process — predictable
  latency, no wasted retries.
- If benchmark B1 (scheduled in R6) shows OCC handles N-writer
  contention cleanly, we relax to multi-writer in v0.3.0-alpha.2
  or GA without breaking the API.

## What this implies for R1b

- `pick_worker_for_write(table_name)` uses a different hash than
  `pick_worker_for_read(table_name, request_id)`.
- Write hash depends only on `table_name` so the same worker stays
  primary for the table's lifetime.
- On primary-writer failure, the health-checker drops it from the
  pool, and the hash re-resolves to a surviving worker. The new
  primary opens the table from object storage (cold-start cost
  applies — see ADR-002).

## Rejected alternatives

- **Open multi-writer OCC**: attractive but unvalidated under our
  workload. Risk: retry storms under high write QPS to a single
  table. We take the conservative path now, measure later.

- **Leader election via MetaStore**: adds coordination traffic and
  a lease timer separate from worker health. Deterministic
  consistent-hash achieves the same single-writer invariant with
  zero extra state.

## Validation (deferred to R6)

Benchmark B1: 8 workers concurrently calling `add_rows` on the
same dataset. Measure conflict rate vs QPS. If < 10% at target
load → propose relaxing to multi-writer in alpha.2.

## Revisit triggers

- B1 result makes the conservative choice look silly.
- Customer workload produces write fan-in that a single worker
  can't keep up with (workaround until then: user splits into
  multiple tables).
- Lance upstream changes OCC semantics.
