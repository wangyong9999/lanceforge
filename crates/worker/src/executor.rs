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
use log::{debug, info};

use lance_distributed_proto::descriptor::{
    FtsQueryParams, LanceQueryDescriptor, LanceQueryType, VectorQueryParams,
};
use lance_distributed_common::config::ShardConfig;

/// Manages lancedb Tables on this Worker.
///
/// Each shard is opened via `lancedb::connect(parent_dir).open_table(shard_name)`,
/// which provides DatasetConsistencyWrapper (auto version refresh) and the full
/// lancedb query API (vector_search, full_text_search, execute_hybrid).
pub struct LanceTableRegistry {
    tables: Vec<(String, lancedb::Table)>,
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
                    .read_consistency_interval(Duration::from_secs(3))
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

        Ok(Self {
            tables,
            cache: Arc::new(crate::cache::QueryCache::new(
                Duration::from_secs(5),
                1000,
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

        // Get table version for cache key
        let dataset_version = {
            let targets = self.resolve_tables(&descriptor.table_name)?;
            let mut max_v = 0u64;
            for (_, t) in &targets {
                max_v = max_v.max(
                    t.version().await.unwrap_or(0)
                );
            }
            max_v
        };

        // Check cache
        let cache_key = crate::cache::QueryCacheKey::from_descriptor_with_version(descriptor, dataset_version);
        if let Some(cached) = self.cache.get(&cache_key).await {
            debug!("Cache hit for query on {}", descriptor.table_name);
            return Ok(cached);
        }

        let target_tables = self.resolve_tables(&descriptor.table_name)?;
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

    fn resolve_tables(&self, table_name: &str) -> Result<Vec<(&str, &lancedb::Table)>> {
        let matches: Vec<_> = self.tables.iter()
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
