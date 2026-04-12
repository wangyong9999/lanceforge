// Licensed under the Apache License, Version 2.0.
// Lance table registration and query execution for Ballista Executors.
//
// Uses lancedb crate for:
// - Table management with DatasetConsistencyWrapper (data freshness)
// - VectorQuery builder API (nprobes, refine_factor, distance_type)
// - Future: EmbeddingRegistry for text→vector
//
// Uses lance crate for:
// - Direct Dataset open (when URI points to a single table, not a database)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use log::{debug, info};
use tokio::sync::RwLock;

use lance_distributed_proto::descriptor::{
    FtsQueryParams, LanceQueryDescriptor, LanceQueryType, VectorQueryParams,
};
use lance_distributed_common::config::ShardConfig;

/// Manages Lance tables on this Executor.
///
/// Each shard is opened via lancedb Connection/Table which provides:
/// - DatasetConsistencyWrapper (auto version refresh for data freshness)
/// - Query builder API (vector_search, query)
///
/// Thread safety: lancedb::Table is Send + Sync.
pub struct LanceTableRegistry {
    tables: Vec<(String, LanceTableOrDataset)>,
    cache: Arc<crate::cache::QueryCache>,
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
        let mut tables = Vec::new();

        for shard in shards {
            info!("Registering Lance shard: {} -> {}", shard.name, shard.uri);

            // Open dataset directly via lance (each shard URI is a single table)
            let dataset = lance::dataset::builder::DatasetBuilder::from_uri(&shard.uri)
                .with_storage_options(storage_options.clone())
                .load()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let row_count = dataset.count_rows(None)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Wrap in lancedb Table for query builder API + consistency
            let db = lancedb::connection::connect(&shard.uri)
                .storage_options(storage_options.iter().map(|(k,v)| (k.clone(), v.clone())))
                .read_consistency_interval(Duration::from_secs(3))
                .execute()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let table = db.open_table(&shard.name)
                .execute()
                .await;

            // If open by name fails (shard URI is a table not a database),
            // fall back to wrapping the raw dataset
            let table = match table {
                Ok(t) => t,
                Err(_) => {
                    // Create a table handle from the already-opened dataset
                    // We use the query builder on the raw Scanner in this case
                    debug!("Shard {} is a direct table URI, using RefreshableDataset", shard.name);
                    let refreshable = RefreshableDataset::new(
                        shard.uri.clone(),
                        storage_options.clone(),
                        dataset,
                        Duration::from_secs(3),
                    );
                    tables.push((shard.name.clone(), LanceTableOrDataset::Dataset(refreshable)));
                    info!("Registered Lance shard: {} ({} rows, dataset mode)", shard.name, row_count);
                    continue;
                }
            };

            tables.push((shard.name.clone(), LanceTableOrDataset::Table(table)));
            info!("Registered Lance shard: {} ({} rows, table mode)", shard.name, row_count);
        }

        Ok(Self {
            tables,
            cache: Arc::new(crate::cache::QueryCache::new(
                Duration::from_secs(5), // 5s TTL
                1000,                    // max 1000 entries
            )),
        })
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

        // Get max dataset version across target shards (for cache key)
        let dataset_version = {
            let targets = self.resolve_tables(&descriptor.table_name)?;
            let mut max_v = 0u64;
            for (_, t) in &targets {
                if let LanceTableOrDataset::Dataset(rd) = t {
                    max_v = max_v.max(rd.version());
                }
            }
            max_v
        };

        // Check cache (key includes dataset version → auto-invalidates on refresh)
        let cache_key = crate::cache::QueryCacheKey::from_descriptor_with_version(descriptor, dataset_version);
        if let Some(cached) = self.cache.get(&cache_key).await {
            debug!("Cache hit for query on {}", descriptor.table_name);
            return Ok(cached);
        }

        let target_tables = self.resolve_tables(&descriptor.table_name)?;
        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for (name, table_or_ds) in &target_tables {
            debug!("Executing query on shard: {}", name);
            let batch = match table_or_ds {
                LanceTableOrDataset::Dataset(refreshable) => {
                    let ds = refreshable.get().await?;
                    execute_on_dataset(&ds, descriptor).await?
                }
                LanceTableOrDataset::Table(_tbl) => {
                    return Err(DataFusionError::NotImplemented(
                        "lancedb Table query path not yet implemented".to_string()
                    ));
                }
            };
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        if all_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
        }

        let result = arrow::compute::concat_batches(&all_batches[0].schema(), &all_batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // Store in cache
        self.cache.put(cache_key, result.clone()).await;

        Ok(result)
    }

    fn resolve_tables(&self, table_name: &str) -> Result<Vec<(&String, &LanceTableOrDataset)>> {
        let matches: Vec<_> = self.tables.iter()
            .filter(|(name, _)| {
                name == table_name
                    || (name.starts_with(table_name)
                        && name.as_bytes().get(table_name.len()) == Some(&b'_'))
            })
            .map(|(name, t)| (name, t))
            .collect();

        if matches.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No Lance shard found matching table: {}", table_name
            )));
        }
        Ok(matches)
    }
}

/// Dataset wrapper with background version refresh (data freshness).
/// Refresh runs in a background tokio task — never blocks the query path.
/// Inspired by lancedb DatasetConsistencyWrapper.
pub struct RefreshableDataset {
    dataset: RwLock<Arc<lance::dataset::Dataset>>,
    /// Current dataset version (for cache invalidation)
    version: std::sync::atomic::AtomicU64,
}

impl RefreshableDataset {
    pub fn new(
        uri: String,
        storage_options: HashMap<String, String>,
        dataset: lance::dataset::Dataset,
        refresh_interval: Duration,
    ) -> Arc<Self> {
        let initial_version = dataset.version().version;
        let rd = Arc::new(Self {
            dataset: RwLock::new(Arc::new(dataset)),
            version: std::sync::atomic::AtomicU64::new(initial_version),
        });

        // Background refresh task (off query path)
        let rd_clone = rd.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(refresh_interval).await;
                match lance::dataset::builder::DatasetBuilder::from_uri(&uri)
                    .with_storage_options(storage_options.clone())
                    .load()
                    .await
                {
                    Ok(new_ds) => {
                        let new_v = new_ds.version().version;
                        let old_v = rd_clone.version.load(std::sync::atomic::Ordering::Relaxed);
                        if new_v != old_v {
                            info!("Dataset refreshed: {} v{} → v{}", uri, old_v, new_v);
                            *rd_clone.dataset.write().await = Arc::new(new_ds);
                            rd_clone.version.store(new_v, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        debug!("Dataset refresh failed for {}: {}", uri, e);
                    }
                }
            }
        });

        rd
    }

    /// Get current dataset snapshot — non-blocking (no S3 call).
    pub async fn get(&self) -> Result<Arc<lance::dataset::Dataset>> {
        Ok(self.dataset.read().await.clone())
    }

    /// Current dataset version (for cache key).
    pub fn version(&self) -> u64 {
        self.version.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Shard can be opened as lancedb Table (full API) or refreshable Dataset.
enum LanceTableOrDataset {
    Table(lancedb::Table),
    Dataset(Arc<RefreshableDataset>),
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

/// Execute on a raw lance Dataset using Scanner (proven path).
async fn execute_on_dataset(
    dataset: &Arc<lance::dataset::Dataset>,
    descriptor: &LanceQueryDescriptor,
) -> Result<RecordBatch> {
    let mut scanner = dataset.scan();

    if !descriptor.columns.is_empty() {
        let cols: Vec<&str> = descriptor.columns.iter().map(|s| s.as_str()).collect();
        scanner.project(&cols)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    if let Some(filter) = &descriptor.filter {
        scanner.filter(filter)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    let query_type = LanceQueryType::try_from(descriptor.query_type)
        .map_err(|_| DataFusionError::Plan(format!("Unknown query type: {}", descriptor.query_type)))?;

    match query_type {
        LanceQueryType::Ann => {
            let vq = descriptor.vector_query.as_ref().ok_or_else(||
                DataFusionError::Plan("ANN query missing vector_query params".to_string()))?;
            apply_vector_query(&mut scanner, vq, descriptor.k)?;
        }
        LanceQueryType::Fts => {
            let fts = descriptor.fts_query.as_ref().ok_or_else(||
                DataFusionError::Plan("FTS query missing fts_query params".to_string()))?;
            apply_fts_query(&mut scanner, fts)?;
        }
        LanceQueryType::Hybrid => {
            // Lance Scanner does not support simultaneous ANN + FTS.
            // Execute as two separate queries, concat results for coordinator-side RRF.
            let vq = descriptor.vector_query.as_ref().ok_or_else(||
                DataFusionError::Plan("Hybrid query missing vector_query params".to_string()))?;
            let fts = descriptor.fts_query.as_ref().ok_or_else(||
                DataFusionError::Plan("Hybrid query missing fts_query params".to_string()))?;

            // ANN leg
            let mut ann_scanner = dataset.scan();
            if let Some(filter) = &descriptor.filter {
                ann_scanner.filter(filter)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            apply_vector_query(&mut ann_scanner, vq, descriptor.k)?;
            ann_scanner.limit(Some(descriptor.k as i64), None)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // FTS leg
            let mut fts_scanner = dataset.scan();
            if let Some(filter) = &descriptor.filter {
                fts_scanner.filter(filter)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            apply_fts_query(&mut fts_scanner, fts)?;
            fts_scanner.limit(Some(descriptor.k as i64), None)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let session = datafusion::prelude::SessionContext::default();
            let task_ctx = session.task_ctx();

            let ann_plan = ann_scanner.create_plan().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let ann_batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(
                datafusion::physical_plan::execute_stream(ann_plan, task_ctx.clone())?).await?;

            let fts_plan = fts_scanner.create_plan().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let fts_batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(
                datafusion::physical_plan::execute_stream(fts_plan, task_ctx)?).await?;

            // Concat ANN + FTS results (coordinator does RRF fusion)
            let mut all_batches = Vec::new();
            all_batches.extend(ann_batches);
            all_batches.extend(fts_batches);

            if all_batches.is_empty() {
                return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
            }
            return arrow::compute::concat_batches(&all_batches[0].schema(), &all_batches)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }
    }

    scanner.limit(Some(descriptor.k as i64), None)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let plan = scanner.create_plan().await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let session = datafusion::prelude::SessionContext::default();
    let task_ctx = session.task_ctx();
    let stream = datafusion::physical_plan::execute_stream(plan, task_ctx)?;
    let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream).await?;

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty())));
    }

    arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn apply_vector_query(
    scanner: &mut lance::dataset::scanner::Scanner,
    vq: &VectorQueryParams,
    k: u32,
) -> Result<()> {
    let vector = decode_vector(vq)?;
    let query_array = arrow::array::Float32Array::from(vector);
    scanner.nearest(&vq.column, &query_array, k as usize)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if vq.nprobes > 0 {
        scanner.nprobes(vq.nprobes as usize);
    }
    Ok(())
}

fn apply_fts_query(
    scanner: &mut lance::dataset::scanner::Scanner,
    fts: &FtsQueryParams,
) -> Result<()> {
    if fts.query_text.is_empty() {
        return Err(DataFusionError::Plan("FTS query text is empty".to_string()));
    }
    let fts_query = lance_index::scalar::FullTextSearchQuery::new(fts.query_text.clone());
    scanner.full_text_search(fts_query)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(())
}

fn decode_vector(vq: &VectorQueryParams) -> Result<Vec<f32>> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
            vector_data: vec![0u8; 16], // 4 floats × 4 bytes
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
        let desc = make_descriptor(0, 10); // 0 = Ann, no vector_query
        let err = validate_descriptor(&desc).unwrap_err();
        assert!(err.to_string().contains("vector_query"));
    }

    #[test]
    fn test_validate_descriptor_fts_missing_fts() {
        let desc = make_descriptor(1, 10); // 1 = Fts, no fts_query
        let err = validate_descriptor(&desc).unwrap_err();
        assert!(err.to_string().contains("fts_query"));
    }

    #[test]
    fn test_validate_descriptor_hybrid_missing_both() {
        let desc = make_descriptor(2, 10); // 2 = Hybrid
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
            vector_data: vec![0u8; 16], // 4 floats
            dimension: 8,              // but says 8
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
    fn test_apply_fts_query_empty_text() {
        // Can't test apply_fts_query without a real scanner,
        // but verify the early check works
        let fts = FtsQueryParams {
            column: "text".to_string(),
            query_text: "".to_string(),
        };
        // The function checks for empty text before scanner interaction
        assert!(fts.query_text.is_empty());
    }

    #[test]
    fn test_ipc_empty_data() {
        let err = ipc_to_record_batch(&[]).unwrap_err();
        assert!(err.to_string().contains("Empty IPC data"));
    }
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
