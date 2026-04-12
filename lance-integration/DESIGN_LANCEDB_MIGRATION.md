# LanceDB Table API Migration Design

## Goal

Migrate worker query execution from `lance::Dataset::Scanner` to `lancedb::Table` API,
eliminating redundant custom implementations and gaining native hybrid search, reranking,
consistency, and write capabilities.

## Current State (Problems)

```
worker/executor.rs:
  lance::Dataset::scan() → manual Scanner setup → execute_on_dataset()
  ↓
  Bypasses lancedb entirely. Custom RefreshableDataset, no hybrid, no reranker.
```

## Target State

```
worker/executor.rs:
  lancedb::connect(parent_dir) → open_table(shard_name) → Table
  ↓
  ANN:    table.vector_search(query_vec)?.limit(k).nprobes(n).execute()
  FTS:    table.query().full_text_search(fts_query).limit(k).execute()
  Hybrid: table.query().full_text_search(fts).nearest_to(vec)?.limit(k).execute_hybrid()
```

## Key Design Decisions

### 1. Shard URI → lancedb connect + open_table

Given shard URI `s3://bucket/path/shard_00.lance`:
- `parent_dir` = `s3://bucket/path/`
- `table_name` = `shard_00`

```rust
let db = lancedb::connect(&parent_dir)
    .storage_options(opts.iter().map(|(k,v)| (k.clone(), v.clone())))
    .read_consistency_interval(Duration::from_secs(refresh_interval))
    .execute().await?;
let table = db.open_table(&table_name).execute().await?;
```

### 2. Query Execution Paths

**ANN:**
```rust
let results = table.vector_search(query_array)?
    .limit(k)
    .nprobes(nprobes)
    .filter(filter)  // if provided
    .execute().await?;
```

**FTS:**
```rust
let results = table.query()
    .full_text_search(FullTextSearchQuery::new(query_text))
    .limit(k)
    .filter(filter)
    .execute().await?;
```

**Hybrid (ANN + FTS → RRF):**
```rust
let results = table.query()
    .full_text_search(FullTextSearchQuery::new(query_text))
    .nearest_to(query_array)?
    .limit(k)
    .nprobes(nprobes)
    .execute_hybrid(QueryExecutionOptions::default())
    .await?;
```

### 3. What Gets Deleted

| Component | Reason |
|-----------|--------|
| `RefreshableDataset` | Replaced by `DatasetConsistencyWrapper` (built into lancedb Table) |
| `LanceTableOrDataset` enum | Only `Table` path remains |
| `execute_on_dataset()` | Replaced by lancedb query builder |
| `apply_vector_query()` | Replaced by `vector_search()` builder |
| `apply_fts_query()` | Replaced by `full_text_search()` builder |
| `decode_vector()` | Replaced by `Float32Array::from()` |
| Hybrid two-scan workaround | Replaced by native `execute_hybrid()` |

### 4. What Gets Kept

| Component | Reason |
|-----------|--------|
| `QueryCache` | lancedb doesn't have distributed query caching |
| `validate_descriptor()` | Input validation before lancedb call |
| `record_batch_to_ipc()` | IPC serialization for gRPC transport |
| `LanceTableRegistry` | Shard registration/routing (struct adapts, internals change) |

### 5. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| lancedb connect per-shard may open too many object store connections | Group shards by parent_dir, share one connection |
| VectorQuery returns stream not RecordBatch | `.try_collect::<Vec<_>>()` then concat |
| Schema differences between ANN (_distance) and FTS (_score) results | lancedb handles this in execute_hybrid |
| Existing tests use lance::Dataset directly | Integration tests go through gRPC, unaffected |

## Implementation Steps

1. Refactor `LanceTableRegistry::with_storage_options()` — open via lancedb connect
2. Implement `execute_query_on_table()` — ANN/FTS/Hybrid via lancedb Table API
3. Delete `RefreshableDataset`, `execute_on_dataset`, `apply_*` helpers
4. Update unit tests for new executor internals
5. Verify: cargo test --workspace
6. Rebuild binaries + run recall benchmarks
7. Run full E2E regression
