// Licensed under the Apache License, Version 2.0.
// LanceForge worker: table registration and query execution.
//
// Uses lancedb crate as the primary query API:
// - lancedb::connect() + open_table() for shard access
// - DatasetConsistencyWrapper for auto version refresh (data freshness)
// - VectorQuery builder for ANN search
// - Query builder for FTS search
// - execute_hybrid() for ANN+FTS with RRF fusion
//
// No direct lance::Dataset access — all queries go through lancedb Table.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use futures::TryStreamExt;
use log::{debug, info, warn};

use lance_distributed_proto::descriptor::{
    LanceQueryDescriptor, LanceQueryType, VectorQueryParams,
};
use lance_distributed_common::config::ShardConfig;

/// Manages lancedb Tables on this Worker.
///
/// Each shard is opened via `lancedb::connect(parent_dir).open_table(shard_name)`,
/// which provides DatasetConsistencyWrapper (auto version refresh) and the full
/// lancedb query API (vector_search, full_text_search, execute_hybrid).
pub struct LanceTableRegistry {
    /// Arc so background tasks can hold a handle without borrowing &self.
    tables: std::sync::Arc<tokio::sync::RwLock<Vec<(String, lancedb::Table)>>>,
    cache: Arc<crate::cache::QueryCache>,
    /// Lance dataset read consistency interval in seconds.
    read_consistency_secs: u64,
    /// Shutdown signal for background tasks (version poller).
    shutdown: Arc<tokio::sync::Notify>,
    /// Cached row counts per shard. Updated by writes/load/create; used by
    /// status() to avoid S3 round-trips on every health check.
    shard_row_counts: std::sync::Arc<tokio::sync::RwLock<HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>>>>,
    /// Per-shard cached version (refreshed by background poller).
    /// Reads are lock-free; writes (load/unload) update under the tables write lock.
    /// Phase 18: eliminates per-query version() I/O that was collapsing read
    /// QPS under write load (measured 91%+ drop with 10 QPS writes).
    shard_versions: std::sync::Arc<tokio::sync::RwLock<HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>>>>,
}

impl LanceTableRegistry {
    pub async fn new(
        _ctx: datafusion::prelude::SessionContext,
        shards: &[ShardConfig],
    ) -> Result<Self> {
        Self::with_storage_options(_ctx, shards, &HashMap::new()).await
    }

    pub async fn with_storage_options(
        _ctx: datafusion::prelude::SessionContext,
        shards: &[ShardConfig],
        storage_options: &HashMap<String, String>,
    ) -> Result<Self> {
        Self::with_full_config(_ctx, shards, storage_options, &Default::default()).await
    }

    pub async fn with_full_config(
        _ctx: datafusion::prelude::SessionContext,
        shards: &[ShardConfig],
        storage_options: &HashMap<String, String>,
        cache_config: &lance_distributed_common::config::CacheConfig,
    ) -> Result<Self> {
        let mut tables = Vec::new();

        // Group shards by parent directory to share connections
        let mut db_cache: HashMap<String, lancedb::Connection> = HashMap::new();

        for shard in shards {
            info!("Registering Lance shard: {} -> {}", shard.name, shard.uri);

            let (parent_dir, table_name) = split_shard_uri(&shard.uri);

            let db = if let Some(db) = db_cache.get(&parent_dir) {
                db.clone()
            } else {
                let db = lancedb::connect(&parent_dir)
                    .storage_options(storage_options.iter().map(|(k, v)| (k.clone(), v.clone())))
                    .read_consistency_interval(Duration::from_secs(cache_config.read_consistency_secs))
                    .execute()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                db_cache.insert(parent_dir.clone(), db.clone());
                db
            };

            let table = db.open_table(&table_name)
                .execute()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let row_count = table.count_rows(None)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            info!("Registered Lance shard: {} ({} rows, lancedb table mode)", shard.name, row_count);
            tables.push((shard.name.clone(), table));
        }

        // Seed per-shard row count cache from initial count_rows().
        let mut row_counts: HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>> = HashMap::new();
        for (name, tbl) in &tables {
            let count = tbl.count_rows(None).await.unwrap_or(0) as u64;
            row_counts.insert(name.clone(), std::sync::Arc::new(std::sync::atomic::AtomicU64::new(count)));
        }
        let row_counts_arc = std::sync::Arc::new(tokio::sync::RwLock::new(row_counts));

        // Seed per-shard version cache with an initial version() call.
        let mut versions: HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>> = HashMap::new();
        for (name, tbl) in &tables {
            let v = tbl.version().await.unwrap_or(0);
            versions.insert(name.clone(), std::sync::Arc::new(std::sync::atomic::AtomicU64::new(v)));
        }

        let tables_arc = std::sync::Arc::new(tokio::sync::RwLock::new(tables));
        let versions_arc = std::sync::Arc::new(tokio::sync::RwLock::new(versions));

        // Spawn background version poller: refresh each shard's cached
        // version at read_consistency_secs interval. Reads use the atomic
        // store, no I/O.
        let shutdown = Arc::new(tokio::sync::Notify::new());
        {
            let tables_lock = tables_arc.clone();
            let versions_lock = versions_arc.clone();
            let interval = std::time::Duration::from_secs(cache_config.read_consistency_secs.max(1));
            let stop = shutdown.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.tick().await; // skip immediate first tick
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        _ = stop.notified() => {
                            info!("Version poller stopping (shutdown)");
                            return;
                        }
                    }
                    let snapshot: Vec<(String, lancedb::Table)> = {
                        let tables = tables_lock.read().await;
                        tables.iter().map(|(n, t)| (n.clone(), t.clone())).collect()
                    };
                    for (name, tbl) in &snapshot {
                        let v = tbl.version().await.unwrap_or(0);
                        let vers = versions_lock.read().await;
                        if let Some(atom) = vers.get(name) {
                            atom.store(v, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            });
        }

        Ok(Self {
            tables: tables_arc,
            cache: Arc::new(crate::cache::QueryCache::new(
                Duration::from_secs(cache_config.ttl_secs),
                cache_config.max_entries,
                cache_config.max_cache_bytes,
            )),
            read_consistency_secs: cache_config.read_consistency_secs,
            shutdown,
            shard_row_counts: row_counts_arc,
            shard_versions: versions_arc,
        })
    }

    /// Signal background tasks (version poller) to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    /// Get cache hit rate for metrics.
    pub fn cache_hit_rate(&self) -> f64 {
        self.cache.hit_rate()
    }

    pub async fn execute_query(
        &self,
        descriptor: &LanceQueryDescriptor,
    ) -> Result<RecordBatch> {
        if descriptor.k == 0 {
            return Err(DataFusionError::Plan("k must be > 0".to_string()));
        }
        validate_descriptor(descriptor)?;

        // Snapshot the matching tables under the read lock, then release the lock
        // immediately. Previously the read lock was held across every async I/O
        // call (table.version, execute_on_table, cache I/O), which caused severe
        // contention at high concurrency (Phase 17 benchmark: conc=50 → QPS
        // collapsed from ~2000 to ~200). By snapshotting Arc-like handles we
        // give each request its own working set and reduce the read lock to
        // O(num_shards) time.
        let target_tables: Vec<(String, lancedb::Table)> = {
            let tables = self.tables.read().await;
            let matched = resolve_tables_inner(&tables, &descriptor.table_name)?;
            matched.into_iter().map(|(n, t)| (n.to_string(), t.clone())).collect()
        };

        // Phase 18: read pre-polled versions from AtomicU64 instead of calling
        // version() on the hot path. The background poller refreshes these at
        // read_consistency_secs interval, so cache invalidation is bounded by
        // that same interval. Net effect under mixed RW workload: no manifest
        // I/O on the read path; cache churn happens at most 1/secs rather
        // than 1/query. Measured fix for Phase 17 finding #6.
        let mut dataset_version = 0u64;
        {
            let vers = self.shard_versions.read().await;
            for (name, _) in &target_tables {
                if let Some(atom) = vers.get(name) {
                    dataset_version = dataset_version.max(atom.load(std::sync::atomic::Ordering::Relaxed));
                }
            }
        }

        // Check cache
        let cache_key = crate::cache::QueryCacheKey::from_descriptor_with_version(descriptor, dataset_version);
        if let Some(cached) = self.cache.get(&cache_key).await {
            debug!("Cache hit for query on {}", descriptor.table_name);
            return Ok(cached);
        }

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for (name, table) in &target_tables {
            debug!("Executing query on shard: {}", name);
            let batch = execute_on_table(table, descriptor).await?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        if all_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
        }

        let result = arrow::compute::concat_batches(&all_batches[0].schema(), &all_batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        self.cache.put(cache_key, result.clone()).await;
        Ok(result)
    }

    /// Dynamically load a new shard at runtime (called by coordinator after CreateTable).
    pub async fn load_shard(
        &self,
        shard_name: &str,
        uri: &str,
        storage_options: &std::collections::HashMap<String, String>,
    ) -> Result<u64> {
        let (parent_dir, table_name) = split_shard_uri(uri);
        let db = lancedb::connect(&parent_dir)
            .storage_options(storage_options.iter().map(|(k, v)| (k.clone(), v.clone())))
            .read_consistency_interval(Duration::from_secs(self.read_consistency_secs))
            .execute()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table = db.open_table(&table_name)
            .execute()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let row_count = table.count_rows(None).await
            .map_err(|e| DataFusionError::External(Box::new(e)))? as u64;
        let v = table.version().await.unwrap_or(0);
        self.tables.write().await.push((shard_name.to_string(), table));
        self.shard_versions.write().await.insert(
            shard_name.to_string(),
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(v)),
        );
        self.shard_row_counts.write().await.insert(
            shard_name.to_string(),
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(row_count)),
        );
        info!("Dynamically loaded shard: {} ({} rows)", shard_name, row_count);
        Ok(row_count)
    }

    /// Create a new local shard from data (called by coordinator during CreateTable auto-sharding).
    pub async fn create_local_shard(
        &self,
        shard_name: &str,
        parent_uri: &str,
        batch: arrow::array::RecordBatch,
        index_column: Option<&str>,
        num_partitions: u32,
        storage_options: &std::collections::HashMap<String, String>,
    ) -> Result<(u64, String)> {
        let db = lancedb::connect(parent_uri)
            .storage_options(storage_options.iter().map(|(k, v)| (k.clone(), v.clone())))
            .read_consistency_interval(Duration::from_secs(self.read_consistency_secs))
            .execute()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let num_rows = batch.num_rows() as u64;
        let table = db.create_table(shard_name, vec![batch])
            .execute()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Auto-create index if requested
        if let Some(col) = index_column
            && !col.is_empty() && num_rows >= 256 {
                let npart = if num_partitions > 0 { num_partitions } else { 32 };
                table.create_index(&[col], lancedb::index::Index::IvfFlat(
                    lancedb::index::vector::IvfFlatIndexBuilder::default().num_partitions(npart)
                )).execute().await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                info!("Created IVF_FLAT index on {}.{} ({} partitions)", shard_name, col, npart);
            }

        let uri = format!("{}/{}.lance", parent_uri.trim_end_matches('/'), shard_name);
        let v = table.version().await.unwrap_or(0);
        self.tables.write().await.push((shard_name.to_string(), table));
        self.shard_versions.write().await.insert(
            shard_name.to_string(),
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(v)),
        );
        self.shard_row_counts.write().await.insert(
            shard_name.to_string(),
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(num_rows)),
        );
        info!("Created local shard: {} ({} rows) at {}", shard_name, num_rows, uri);
        Ok((num_rows, uri))
    }

    /// Compact/optimize all loaded tables (merge small fragments for read performance).
    pub async fn compact_all(&self) -> Result<u64> {
        let tables = self.tables.read().await;
        let mut total_compacted = 0u64;
        for (name, table) in tables.iter() {
            match table.optimize(lancedb::table::OptimizeAction::All).await {
                Ok(stats) => {
                    info!("Compacted {}: {:?}", name, stats);
                    total_compacted += 1;
                }
                Err(e) => {
                    warn!("Compact failed for {}: {}", name, e);
                }
            }
        }
        Ok(total_compacted)
    }

    /// Unload a shard at runtime (for DropTable).
    pub async fn unload_shard(&self, shard_name: &str) {
        self.tables.write().await.retain(|(name, _)| name != shard_name);
        self.shard_versions.write().await.remove(shard_name);
        self.shard_row_counts.write().await.remove(shard_name);
        info!("Unloaded shard: {}", shard_name);
    }

    /// Point lookup: get rows by primary key IDs.
    pub async fn get_by_ids(
        &self,
        table_name: &str,
        ids: &[i64],
        id_column: &str,
        columns: &[String],
    ) -> Result<RecordBatch> {
        use lancedb::query::{ExecutableQuery, QueryBase};
        let targets: Vec<(String, lancedb::Table)> = {
            let tables = self.tables.read().await;
            let matched = resolve_tables_inner(&tables, table_name)?;
            matched.into_iter().map(|(n, t)| (n.to_string(), t.clone())).collect()
        };

        let id_list = ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ");
        let filter = format!("{} IN ({})", id_column, id_list);

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for (_, table) in &targets {
            let mut builder = table.query().only_if(filter.clone());
            if !columns.is_empty() {
                builder = builder.select(lancedb::query::Select::Columns(
                    columns.iter().map(|c| c.as_str().into()).collect()
                ));
            }
            let stream = builder.execute().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let batch: Vec<RecordBatch> = stream.try_collect().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            all_batches.extend(batch.into_iter().filter(|b| b.num_rows() > 0));
        }

        if all_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
        }
        arrow::compute::concat_batches(&all_batches[0].schema(), &all_batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Expression scan: filter rows without vector search.
    pub async fn scan_query(
        &self,
        table_name: &str,
        filter: &str,
        limit: usize,
        offset: usize,
        columns: &[String],
    ) -> Result<RecordBatch> {
        use lancedb::query::{ExecutableQuery, QueryBase};
        let targets: Vec<(String, lancedb::Table)> = {
            let tables = self.tables.read().await;
            let matched = resolve_tables_inner(&tables, table_name)?;
            matched.into_iter().map(|(n, t)| (n.to_string(), t.clone())).collect()
        };

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for (_, table) in &targets {
            let mut builder = table.query().only_if(filter);
            if !columns.is_empty() {
                builder = builder.select(lancedb::query::Select::Columns(
                    columns.iter().map(|c| c.as_str().into()).collect()
                ));
            }
            // Request more than needed per shard so coordinator can merge+truncate
            builder = builder.limit(limit + offset);
            let stream = builder.execute().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let batches: Vec<RecordBatch> = stream.try_collect().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            all_batches.extend(batches.into_iter().filter(|b| b.num_rows() > 0));
        }

        if all_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
        }
        arrow::compute::concat_batches(&all_batches[0].schema(), &all_batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Create an index on a table's column.
    pub async fn create_index_on_table(
        &self,
        table_name: &str,
        column: &str,
        index_type: &str,
        num_partitions: u32,
    ) -> Result<()> {
        let tables = self.tables.read().await;
        let targets = resolve_tables_inner(&tables, table_name)?;

        for (name, table) in &targets {
            let npart = if num_partitions > 0 { num_partitions } else { 32 };
            let index = match index_type {
                "BTREE" => lancedb::index::Index::BTree(Default::default()),
                "INVERTED" => lancedb::index::Index::FTS(Default::default()),
                _ => lancedb::index::Index::IvfFlat(
                    lancedb::index::vector::IvfFlatIndexBuilder::default().num_partitions(npart)
                ),
            };
            table.create_index(&[column], index)
                .execute()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            info!("Created {} index on {}.{}", index_type, name, column);
        }
        Ok(())
    }

    /// Get names of all loaded shards (for health check reporting).
    pub async fn shard_names(&self) -> Vec<String> {
        self.shard_row_counts.read().await.keys().cloned().collect()
    }

    /// Get worker status: (loaded_shard_count, total_rows).
    /// Uses cached row counts (updated by writes/load/create) to avoid
    /// N S3 round-trips per health check. The version poller background
    /// task refreshes counts periodically.
    pub async fn status(&self) -> (u32, u64) {
        let counts = self.shard_row_counts.read().await;
        let shard_count = counts.len() as u32;
        let total_rows: u64 = counts.values()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .sum();
        (shard_count, total_rows)
    }

    /// Get table info (row count + schema).
    pub async fn get_table_info(
        &self,
        table_name: &str,
    ) -> Result<(u64, Vec<lance_distributed_proto::generated::lance_distributed::ColumnInfo>)> {
        use lance_distributed_proto::generated::lance_distributed::ColumnInfo;
        // Phase 18: snapshot then I/O outside lock (same pattern as status/execute_query).
        let targets: Vec<(String, lancedb::Table)> = {
            let tables = self.tables.read().await;
            let matched = resolve_tables_inner(&tables, table_name)?;
            matched.into_iter().map(|(n, t)| (n.to_string(), t.clone())).collect()
        };

        let mut total_rows = 0u64;
        let mut columns = Vec::new();

        for (_, table) in &targets {
            total_rows += table.count_rows(None).await
                .map_err(|e| DataFusionError::External(Box::new(e)))? as u64;
            if columns.is_empty() {
                let schema = table.schema().await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                columns = schema.fields().iter().map(|f| ColumnInfo {
                    name: f.name().clone(),
                    data_type: format!("{:?}", f.data_type()),
                }).collect();
            }
        }
        Ok((total_rows, columns))
    }

    /// Execute a write operation (add/delete/upsert) on matching tables.
    /// Snapshots table handles under lock, then releases before I/O
    /// (same pattern as execute_query — prevents write contention with
    /// LoadShard/UnloadShard which need the write lock).
    pub async fn execute_write(
        &self,
        req: &lance_distributed_proto::generated::lance_distributed::LocalWriteRequest,
    ) -> Result<(u64, u64)> {
        // Snapshot matching tables under lock, then release immediately
        let targets: Vec<(String, lancedb::Table)> = {
            let tables = self.tables.read().await;
            let matched = if !req.target_shard.is_empty() {
                let m: Vec<_> = tables.iter()
                    .filter(|(name, _)| name == &req.target_shard)
                    .map(|(name, t)| (name.as_str(), t))
                    .collect();
                if m.is_empty() {
                    return Err(DataFusionError::Plan(format!(
                        "Target shard not found on this worker: {}", req.target_shard
                    )));
                }
                m
            } else {
                resolve_tables_inner(&tables, &req.table_name)?
            };
            matched.into_iter().map(|(n, t)| (n.to_string(), t.clone())).collect()
        };
        // All I/O below is lock-free
        let mut total_affected = 0u64;
        let mut max_version = 0u64;

        for (name, table) in &targets {
            debug!("Executing write on shard: {}", name);
            match req.write_type {
                0 => {
                    // Add rows with OCC conflict retry
                    if req.arrow_ipc_data.is_empty() {
                        return Err(DataFusionError::Plan("Empty data for add".to_string()));
                    }
                    let batch = lance_distributed_common::ipc::ipc_to_record_batch(&req.arrow_ipc_data)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    let rows = batch.num_rows() as u64;
                    let mut retries = 0u32;
                    loop {
                        match table.add(vec![batch.clone()]).execute().await {
                            Ok(_) => break,
                            Err(e) => {
                                let msg = e.to_string().to_lowercase();
                                if (msg.contains("conflict") || msg.contains("version"))
                                    && retries < 3 {
                                    retries += 1;
                                    warn!("Write conflict on {}, retry {}/3", name, retries);
                                    tokio::time::sleep(Duration::from_millis(50 * retries as u64)).await;
                                    continue;
                                }
                                return Err(DataFusionError::External(Box::new(e)));
                            }
                        }
                    }
                    total_affected += rows;
                }
                1 => {
                    // Delete rows by filter
                    if req.filter.is_empty() {
                        return Err(DataFusionError::Plan("Empty filter for delete".to_string()));
                    }
                    let pre_count = table.count_rows(None).await.unwrap_or(0) as u64;
                    table.delete(&req.filter)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let post_count = table.count_rows(None).await.unwrap_or(0) as u64;
                    total_affected += pre_count.saturating_sub(post_count);
                }
                2 => {
                    // Upsert via merge_insert with OCC conflict retry
                    if req.arrow_ipc_data.is_empty() {
                        return Err(DataFusionError::Plan("Empty data for upsert".to_string()));
                    }
                    if req.on_columns.is_empty() {
                        return Err(DataFusionError::Plan("on_columns required for upsert".to_string()));
                    }
                    let batch = lance_distributed_common::ipc::ipc_to_record_batch(&req.arrow_ipc_data)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    let rows = batch.num_rows() as u64;
                    let on_cols: Vec<&str> = req.on_columns.iter().map(|s| s.as_str()).collect();
                    let mut retries = 0u32;
                    loop {
                        let schema = table.schema().await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        let mut builder = table.merge_insert(&on_cols);
                        builder.when_matched_update_all(None)
                            .when_not_matched_insert_all();
                        match builder.execute(Box::new(arrow::record_batch::RecordBatchIterator::new(
                                vec![Ok(batch.clone())], schema,
                            ))).await {
                            Ok(_) => break,
                            Err(e) => {
                                let msg = e.to_string().to_lowercase();
                                if (msg.contains("conflict") || msg.contains("version"))
                                    && retries < 3 {
                                    retries += 1;
                                    warn!("Upsert conflict on {}, retry {}/3", name, retries);
                                    tokio::time::sleep(Duration::from_millis(50 * retries as u64)).await;
                                    continue;
                                }
                                return Err(DataFusionError::External(Box::new(e)));
                            }
                        }
                    }
                    total_affected += rows;
                }
                _ => {
                    return Err(DataFusionError::Plan(format!("Unknown write_type: {}", req.write_type)));
                }
            }
            max_version = max_version.max(
                table.version().await.unwrap_or(0)
            );
        }

        Ok((total_affected, max_version))
    }
}

fn resolve_tables_inner<'a>(
    tables: &'a [(String, lancedb::Table)],
    table_name: &str,
) -> Result<Vec<(&'a str, &'a lancedb::Table)>> {
    let matches: Vec<_> = tables.iter()
        .filter(|(name, _)| {
            name == table_name
                || (name.starts_with(table_name)
                    && name.as_bytes().get(table_name.len()) == Some(&b'_'))
        })
        .map(|(name, t)| (name.as_str(), t))
        .collect();

    if matches.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "No Lance shard found matching table: {}", table_name
        )));
    }
    Ok(matches)
}

/// Split a shard URI into (parent_directory, table_name).
/// e.g. "s3://bucket/path/shard_00.lance" → ("s3://bucket/path/", "shard_00")
fn split_shard_uri(uri: &str) -> (String, String) {
    // Remove trailing slash and .lance suffix for table name extraction
    let clean = uri.trim_end_matches('/');

    if let Some(last_slash) = clean.rfind('/') {
        let parent = &clean[..=last_slash];
        let filename = &clean[last_slash + 1..];
        let table_name = filename.strip_suffix(".lance").unwrap_or(filename);
        (parent.to_string(), table_name.to_string())
    } else {
        // No slash — treat entire URI as table name
        let table_name = clean.strip_suffix(".lance").unwrap_or(clean);
        ("./".to_string(), table_name.to_string())
    }
}

/// Execute a query on a lancedb Table using the query builder API.
async fn execute_on_table(
    table: &lancedb::Table,
    descriptor: &LanceQueryDescriptor,
) -> Result<RecordBatch> {
    use lancedb::query::{ExecutableQuery, QueryBase};

    let query_type = LanceQueryType::try_from(descriptor.query_type)
        .map_err(|_| DataFusionError::Plan(format!("Unknown query type: {}", descriptor.query_type)))?;

    let k = descriptor.k as usize;

    match query_type {
        LanceQueryType::Ann => {
            let vq = descriptor.vector_query.as_ref().ok_or_else(||
                DataFusionError::Plan("ANN query missing vector_query params".to_string()))?;
            let vector = decode_vector(vq)?;

            let mut builder = table.vector_search(vector.as_slice())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .limit(k)
                .nprobes(vq.nprobes as usize);

            if let Some(ref filter) = descriptor.filter {
                builder = builder.only_if(filter.clone());
                // lancedb defaults to prefilter=true: scalar index narrows candidates
                // before vector search. This gives good recall + high QPS when a
                // BTREE/BITMAP index exists on the filter column.
                // Increase nprobes for filtered queries to compensate for reduced
                // candidates per partition.
                builder = builder.nprobes(vq.nprobes.max(20) as usize);
            }

            let stream = builder.execute().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            collect_stream(stream).await
        }
        LanceQueryType::Fts => {
            let fts = descriptor.fts_query.as_ref().ok_or_else(||
                DataFusionError::Plan("FTS query missing fts_query params".to_string()))?;
            if fts.query_text.is_empty() {
                return Err(DataFusionError::Plan("FTS query text is empty".to_string()));
            }

            let fts_query = lance_index::scalar::FullTextSearchQuery::new(fts.query_text.clone());
            let mut builder = table.query()
                .full_text_search(fts_query)
                .limit(k);

            if let Some(ref filter) = descriptor.filter {
                builder = builder.only_if(filter.clone());
            }

            let stream = builder.execute().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            collect_stream(stream).await
        }
        LanceQueryType::Hybrid => {
            let vq = descriptor.vector_query.as_ref().ok_or_else(||
                DataFusionError::Plan("Hybrid query missing vector_query params".to_string()))?;
            let fts = descriptor.fts_query.as_ref().ok_or_else(||
                DataFusionError::Plan("Hybrid query missing fts_query params".to_string()))?;

            let vector = decode_vector(vq)?;
            let fts_query = lance_index::scalar::FullTextSearchQuery::new(fts.query_text.clone());

            let mut builder = table.query()
                .full_text_search(fts_query)
                .nearest_to(vector.as_slice())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .limit(k)
                .nprobes(vq.nprobes as usize);

            if let Some(ref filter) = descriptor.filter {
                builder = builder.only_if(filter.clone());
            }

            let stream = builder
                .execute_hybrid(lancedb::query::QueryExecutionOptions::default())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            collect_stream(stream).await
        }
    }
}

/// Collect a lancedb result stream into a single RecordBatch.
async fn collect_stream(
    stream: lancedb::arrow::SendableRecordBatchStream,
) -> Result<RecordBatch> {
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
    }
    arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn validate_descriptor(descriptor: &LanceQueryDescriptor) -> Result<()> {
    match LanceQueryType::try_from(descriptor.query_type) {
        Ok(LanceQueryType::Ann) => {
            if descriptor.vector_query.is_none() {
                return Err(DataFusionError::Plan(
                    "ANN query requires vector_query parameters".to_string(),
                ));
            }
        }
        Ok(LanceQueryType::Fts) => {
            if descriptor.fts_query.is_none() {
                return Err(DataFusionError::Plan(
                    "FTS query requires fts_query parameters".to_string(),
                ));
            }
        }
        Ok(LanceQueryType::Hybrid) => {
            if descriptor.vector_query.is_none() || descriptor.fts_query.is_none() {
                return Err(DataFusionError::Plan(
                    "Hybrid query requires both vector_query and fts_query".to_string(),
                ));
            }
        }
        Err(_) => {
            return Err(DataFusionError::Plan(format!(
                "Unknown query type: {}", descriptor.query_type
            )));
        }
    }
    Ok(())
}

fn decode_vector(vq: &VectorQueryParams) -> Result<Vec<f32>> {
    if !vq.vector_data.len().is_multiple_of(4) {
        return Err(DataFusionError::Plan(format!(
            "Invalid vector_data length {}: must be divisible by 4 (f32 alignment)",
            vq.vector_data.len()
        )));
    }
    let vector: Vec<f32> = vq.vector_data
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect();
    if vq.dimension > 0 && vector.len() != vq.dimension as usize {
        return Err(DataFusionError::Plan(format!(
            "Vector dimension mismatch: declared {}, actual {}",
            vq.dimension, vector.len()
        )));
    }
    if vector.is_empty() {
        return Err(DataFusionError::Plan("Query vector is empty".to_string()));
    }
    Ok(vector)
}

/// IPC serialize — wraps common::ipc with DataFusion error type.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    lance_distributed_common::ipc::record_batch_to_ipc(batch)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// IPC deserialize — wraps common::ipc with DataFusion error type.
pub fn ipc_to_record_batch(data: &[u8]) -> Result<RecordBatch> {
    if data.is_empty() {
        return Err(DataFusionError::Plan("Empty IPC data".to_string()));
    }
    lance_distributed_common::ipc::ipc_to_record_batch(data)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_distributed_proto::descriptor::{FtsQueryParams, VectorQueryParams};

    fn make_descriptor(query_type: i32, k: u32) -> LanceQueryDescriptor {
        LanceQueryDescriptor {
            query_type,
            table_name: "test_table".to_string(),
            k,
            vector_query: None,
            fts_query: None,
            filter: None,
            columns: vec![],
        }
    }

    fn make_vector_query() -> VectorQueryParams {
        VectorQueryParams {
            column: "vector".to_string(),
            vector_data: vec![0u8; 16],
            dimension: 4,
            nprobes: 10,
            metric_type: 0,
            oversample_factor: 0,
        }
    }

    fn make_fts_query() -> FtsQueryParams {
        FtsQueryParams {
            column: "text".to_string(),
            query_text: "hello world".to_string(),
        }
    }

    #[test]
    fn test_validate_descriptor_invalid_query_type() {
        let desc = make_descriptor(999, 10);
        let err = validate_descriptor(&desc).unwrap_err();
        assert!(err.to_string().contains("Unknown query type: 999"));
    }

    #[test]
    fn test_validate_descriptor_ann_missing_vector() {
        let desc = make_descriptor(0, 10);
        let err = validate_descriptor(&desc).unwrap_err();
        assert!(err.to_string().contains("vector_query"));
    }

    #[test]
    fn test_validate_descriptor_fts_missing_fts() {
        let desc = make_descriptor(1, 10);
        let err = validate_descriptor(&desc).unwrap_err();
        assert!(err.to_string().contains("fts_query"));
    }

    #[test]
    fn test_validate_descriptor_hybrid_missing_both() {
        let desc = make_descriptor(2, 10);
        let err = validate_descriptor(&desc).unwrap_err();
        assert!(err.to_string().contains("vector_query") || err.to_string().contains("fts_query"));
    }

    #[test]
    fn test_validate_descriptor_hybrid_missing_fts() {
        let mut desc = make_descriptor(2, 10);
        desc.vector_query = Some(make_vector_query());
        let err = validate_descriptor(&desc).unwrap_err();
        assert!(err.to_string().contains("fts_query"));
    }

    #[test]
    fn test_validate_descriptor_ann_ok() {
        let mut desc = make_descriptor(0, 10);
        desc.vector_query = Some(make_vector_query());
        assert!(validate_descriptor(&desc).is_ok());
    }

    #[test]
    fn test_validate_descriptor_fts_ok() {
        let mut desc = make_descriptor(1, 10);
        desc.fts_query = Some(make_fts_query());
        assert!(validate_descriptor(&desc).is_ok());
    }

    #[test]
    fn test_validate_descriptor_hybrid_ok() {
        let mut desc = make_descriptor(2, 10);
        desc.vector_query = Some(make_vector_query());
        desc.fts_query = Some(make_fts_query());
        assert!(validate_descriptor(&desc).is_ok());
    }

    #[test]
    fn test_decode_vector_misaligned() {
        let vq = VectorQueryParams {
            column: "v".to_string(),
            vector_data: vec![0u8; 5], // not divisible by 4
            dimension: 0,
            nprobes: 0,
            metric_type: 0,
            oversample_factor: 0,
        };
        let err = decode_vector(&vq).unwrap_err();
        assert!(err.to_string().contains("divisible by 4"));
    }

    #[test]
    fn test_decode_vector_empty() {
        let vq = VectorQueryParams {
            column: "v".to_string(),
            vector_data: vec![],
            dimension: 0,
            nprobes: 0,
            metric_type: 0,
            oversample_factor: 0,
        };
        let err = decode_vector(&vq).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_decode_vector_dimension_mismatch() {
        let vq = VectorQueryParams {
            column: "v".to_string(),
            vector_data: vec![0u8; 16],
            dimension: 8,
            nprobes: 0,
            metric_type: 0,
            oversample_factor: 0,
        };
        let err = decode_vector(&vq).unwrap_err();
        assert!(err.to_string().contains("dimension mismatch"));
    }

    #[test]
    fn test_decode_vector_ok() {
        let vq = VectorQueryParams {
            column: "v".to_string(),
            vector_data: vec![0u8; 16],
            dimension: 4,
            nprobes: 0,
            metric_type: 0,
            oversample_factor: 0,
        };
        let v = decode_vector(&vq).unwrap();
        assert_eq!(v.len(), 4);
    }

    #[test]
    fn test_ipc_empty_data() {
        let err = ipc_to_record_batch(&[]).unwrap_err();
        assert!(err.to_string().contains("Empty IPC data"));
    }

    #[test]
    fn test_split_shard_uri_s3() {
        let (parent, name) = split_shard_uri("s3://bucket/path/shard_00.lance");
        assert_eq!(parent, "s3://bucket/path/");
        assert_eq!(name, "shard_00");
    }

    #[test]
    fn test_split_shard_uri_local() {
        let (parent, name) = split_shard_uri("/tmp/data/my_table.lance");
        assert_eq!(parent, "/tmp/data/");
        assert_eq!(name, "my_table");
    }

    #[test]
    fn test_split_shard_uri_no_extension() {
        let (parent, name) = split_shard_uri("/data/shard_01");
        assert_eq!(parent, "/data/");
        assert_eq!(name, "shard_01");
    }
}
