# ADR-005: v0.2 → v0.3 migration guarantees

**Status**: Accepted
**Phase**: R0.5 (binds R4 implementation)

## Decision

`lance-admin migrate` converts a v0.2 multi-shard deployment into
a v0.3 single-dataset layout with the following contract:

- **Read-only to source**: source shards are never modified or
  deleted during migration. A successful migration leaves both
  layouts intact until the user explicitly runs
  `lance-admin migrate --commit-cleanup`.
- **Checksummed**: row counts and per-row SHA256 digests are
  verified between source shards (combined) and the target
  dataset. Any mismatch fails the migration with no changes to
  the target URI.
- **Resumable**: progress tracked in `migration_state.json` at
  the target directory. If the tool crashes or is killed, a
  subsequent invocation resumes from the last checkpoint.
- **Dry-run supported**: `--dry-run` prints the plan (tables,
  source URIs, target URI, estimated row count per table) and
  exits without writing.
- **Schema-preserving**: target dataset has the same Arrow
  schema as the source tables. The `on_columns` hint from v0.2
  is captured in migration output so the user can decide whether
  to create a scalar index post-migration (ADR-003).

## What is NOT guaranteed

- **Zero downtime**: migration is offline. Writes to source
  during migration are NOT captured to target.
- **Index preservation**: v0.2 indexes are not copied. User
  creates v0.3 indexes on the target dataset after migration.
- **Reversibility of `--commit-cleanup`**: once source is
  deleted, recovery needs backup (outside migration tool).

## Algorithm outline

```
for each table in v0.2 config:
    target_uri = {table_root}/{table}.lance
    if migration_state.completed.contains(table):
        skip
    for each shard in table.shards:
        if migration_state.shards_done[table].contains(shard):
            skip
        batch_iter = open shard as Lance dataset, iterate RecordBatches
        for batch in batch_iter:
            append to target dataset
            update migration_state.shards_done[table]
    verify row_count(target) == sum(row_count(source_shards))
    verify sha256(target_rows) == sha256(union(source_rows))
    migration_state.completed.add(table)
```

SHA256 comparison uses a canonical per-row hash; order-independent
since the target may reshuffle during Lance ingest.

## Testing

- `e2e_migration_test.py`: synthetic multi-shard data (2 shards,
  10k rows total) → migrate → read from v3 → assert row count
  and vector-equality on all rows.
- Resumability test: run migration, kill at 50%, restart, assert
  final state identical to uninterrupted run.
- Schema preservation test: migrate a table with
  `on_columns=['tenant_id']`, verify Arrow schema round-trips and
  scalar-index recommendation appears in tool output.

## Failure modes and responses

| Failure | Response |
|---|---|
| Source shard URI unreachable | Fail, do not touch target |
| Target URI already exists (non-migration) | Fail, require explicit `--force` |
| SHA256 mismatch | Fail, leave partial target for inspection |
| Kill mid-migration | Resume on next invocation via migration_state.json |
| `--commit-cleanup` on partial migration | Refuse unless `completed` contains all tables |

## Rejected alternatives

- **In-place conversion (rewrite shards as fragments)**: Lance
  dataset format is not trivially convertible from N independent
  datasets; the safe path is read-and-rewrite.
- **Automatic index recreation**: scalar index choice is
  workload-specific (ADR-003 discussion); surface it as advice,
  not as automatic action.
- **Live migration with write-through to target**: large scope,
  deferred to v0.4+ if demand exists.
