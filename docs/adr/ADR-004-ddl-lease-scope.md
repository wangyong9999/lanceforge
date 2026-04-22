# ADR-004: DDL lease scope in v3 — CreateTable / DropTable only

**Status**: Accepted
**Phase**: R0.5

## Decision

The cross-coordinator DDL lease (MetaStore CAS lease in
`crates/meta/src/ddl_lease.rs`) wraps `CreateTable` and `DropTable`
only in v3. `AlterTable`, `CreateIndex`, `RestoreTable`, and all
other table-scoped mutations rely on Lance's native manifest CAS
for atomicity.

## Rationale

v2 used the DDL lease around every DDL operation because each was
a fan-out across N independent datasets; the lease was what made
"all shards' schemas advance together" atomic.

v3 has one Lance dataset per table. Lance's manifest commit is
atomic via `ConditionalPutCommitHandler`. Concurrent `add_columns`
calls from two coords either both succeed (landing at consecutive
manifest versions) or one retries until it does — Lance handles
this. No external lease needed.

`CreateTable` and `DropTable` are different: the dataset doesn't
exist yet (or is about to disappear), so there's no manifest to
CAS against. Two coords concurrently creating a table with the
same name is a race the table-uris MetaStore registry must
adjudicate. DDL lease gives the loser a clear "LeaseHeld" error
instead of silent data interleaving.

## Retention

- `CreateTable`: lease on table_name, write table_uris[name]=uri
  on success, release lease.
- `DropTable`: lease on table_name, delete dataset at URI, remove
  from table_uris, release lease.

## Removal

- `AlterTable (ADD/DROP/RENAME/ALTER)`: no lease; Lance handles.
- `CreateIndex`: no lease; Lance handles.
- `RestoreTable`: no lease; Lance handles.
- `CreateTag` / `DeleteTag`: no lease; Lance's `Tags` API is
  conflict-safe.

## Ordering invariant

If any DDL op talks to Lance, the RPC handler MUST complete the
Lance call before its return, regardless of whether a lease was
held. This prevents the "Lance ack'd but lease never released"
reorder. For lease-guarded ops (CreateTable/DropTable), release
happens after Lance commit — always.

## Rejected alternatives

- **Keep DDL lease everywhere**: redundant safety, extra latency,
  masks real Lance behaviour.
- **Remove lease entirely**: exposes CreateTable/DropTable races.

## Validation

Unit tests in `ddl_lease.rs` (already in place) cover lease
semantics. Integration test for CreateTable race under two coord
processes — covered by existing chaos suite after R2 reframing.
