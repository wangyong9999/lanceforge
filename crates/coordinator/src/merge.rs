// Licensed under the Apache License, Version 2.0.
// Global result merge for distributed Lance queries.

use arrow::array::{Array, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

// Use lancedb's production-grade RRF Reranker
use lancedb::rerankers::Reranker;
use lancedb::rerankers::rrf::RRFReranker;

/// Merge multiple local TopK results into a single global TopK by distance (ascending).
pub fn global_top_k_by_distance(batches: Vec<RecordBatch>, k: usize) -> Result<RecordBatch> {
    if k == 0 {
        return Err(DataFusionError::Plan("k must be > 0".to_string()));
    }
    global_top_k(batches, k, "_distance", false)
}

/// Merge multiple local TopK results by score (descending).
pub fn global_top_k_by_score(batches: Vec<RecordBatch>, k: usize) -> Result<RecordBatch> {
    if k == 0 {
        return Err(DataFusionError::Plan("k must be > 0".to_string()));
    }
    global_top_k(batches, k, "_score", true)
}

/// Generic TopK merge with schema validation.
///
/// Memory optimization: each input batch (from a worker) is already sorted
/// by the lancedb query engine. We truncate each to K rows BEFORE concat,
/// reducing peak memory from N*oversample*K to N*K (where N = num workers).
/// For top-10000 with 5 workers: 500K rows → 50K rows before sort.
fn global_top_k(
    batches: Vec<RecordBatch>,
    k: usize,
    sort_column: &str,
    descending: bool,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    let schema = batches[0].schema();

    // Schema validation
    for (i, batch) in batches.iter().enumerate().skip(1) {
        if batch.schema() != schema {
            return Err(DataFusionError::Plan(format!(
                "Schema mismatch in batch {}: expected {:?}, got {:?}",
                i, schema, batch.schema()
            )));
        }
    }

    // Memory optimization: truncate each batch to k rows before concat.
    // Each worker returns results sorted by distance/score, so the first k
    // rows from each are the best candidates. This bounds concat memory to
    // num_workers * k instead of num_workers * oversample_k.
    let truncated: Vec<RecordBatch> = batches.into_iter()
        .map(|b| if b.num_rows() > k { b.slice(0, k) } else { b })
        .filter(|b| b.num_rows() > 0)
        .collect();

    if truncated.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let merged = arrow::compute::concat_batches(&schema, &truncated)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    if merged.num_rows() == 0 {
        return Ok(merged);
    }

    // Find sort column
    let sort_idx = schema.index_of(sort_column).map_err(|_| {
        DataFusionError::Plan(format!(
            "Sort column '{}' not found in schema: {:?}",
            sort_column,
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    })?;

    // Sort and take top-K
    let actual_k = k.min(merged.num_rows());
    let sort_indices = arrow::compute::sort_to_indices(
        merged.column(sort_idx),
        Some(SortOptions { descending, nulls_first: false }),
        Some(actual_k),
    ).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let columns: Vec<Arc<dyn Array>> = (0..merged.num_columns())
        .map(|i| arrow::compute::take(merged.column(i), &sort_indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    RecordBatch::try_new(schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Reciprocal Rank Fusion (RRF) for hybrid search.
/// Delegates to lancedb's production-grade RRFReranker (BTreeMap, dedup, rank).
pub async fn reciprocal_rank_fusion(
    ann_batch: RecordBatch,
    fts_batch: RecordBatch,
    k_param: f32,
    top_k: usize,
) -> Result<RecordBatch> {
    if top_k == 0 {
        return Err(DataFusionError::Plan("top_k must be > 0".to_string()));
    }
    if k_param <= 0.0 {
        return Err(DataFusionError::Plan("k_param must be > 0 for RRF".to_string()));
    }

    let reranker = RRFReranker::new(k_param);
    let result = reranker.rerank_hybrid("", ann_batch, fts_batch)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Take top_k if result has more rows
    if result.num_rows() > top_k {
        let indices = arrow::array::UInt32Array::from_iter_values(0..top_k as u32);
        let columns: Vec<Arc<dyn Array>> = (0..result.num_columns())
            .map(|i| arrow::compute::take(result.column(i), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        RecordBatch::try_new(result.schema(), columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    } else {
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Int32Array};
    use arrow::datatypes::{DataType, Field};

    fn make_distance_batch(ids: Vec<i32>, distances: Vec<f32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("_distance", DataType::Float32, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Float32Array::from(distances)),
        ]).unwrap()
    }

    #[test]
    fn test_global_top_k_by_distance() {
        let batch1 = make_distance_batch(vec![1, 2, 3], vec![0.1, 0.3, 0.5]);
        let batch2 = make_distance_batch(vec![4, 5, 6], vec![0.05, 0.2, 0.4]);

        let result = global_top_k_by_distance(vec![batch1, batch2], 3).unwrap();
        assert_eq!(result.num_rows(), 3);

        let distances = result.column(1).as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((distances.value(0) - 0.05).abs() < 1e-6);
        assert!((distances.value(1) - 0.1).abs() < 1e-6);
        assert!((distances.value(2) - 0.2).abs() < 1e-6);
    }

    #[test]
    fn test_global_top_k_empty() {
        let result = global_top_k_by_distance(vec![], 10).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_global_top_k_single_batch() {
        let batch = make_distance_batch(vec![1, 2, 3], vec![0.3, 0.1, 0.2]);
        let result = global_top_k_by_distance(vec![batch], 2).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_global_top_k_k_larger_than_data() {
        let batch = make_distance_batch(vec![1, 2], vec![0.1, 0.2]);
        let result = global_top_k_by_distance(vec![batch], 100).unwrap();
        assert_eq!(result.num_rows(), 2); // returns all available, not 100
    }

    #[test]
    fn test_global_top_k_k_zero_error() {
        let batch = make_distance_batch(vec![1], vec![0.1]);
        let result = global_top_k_by_distance(vec![batch], 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_global_top_k_nan_distances() {
        let batch = make_distance_batch(vec![1, 2, 3], vec![0.1, f32::NAN, 0.2]);
        let result = global_top_k_by_distance(vec![batch], 3).unwrap();
        // NaN should sort to end (nulls_first: false)
        assert_eq!(result.num_rows(), 3);
        let distances = result.column(1).as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((distances.value(0) - 0.1).abs() < 1e-6);
        assert!((distances.value(1) - 0.2).abs() < 1e-6);
        // third should be NaN
        assert!(distances.value(2).is_nan());
    }

    #[test]
    fn test_schema_mismatch_error() {
        let batch1 = make_distance_batch(vec![1], vec![0.1]);
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("different_id", DataType::Int32, false),
            Field::new("_distance", DataType::Float32, false),
        ]));
        let batch2 = RecordBatch::try_new(schema2, vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Float32Array::from(vec![0.2])),
        ]).unwrap();

        let result = global_top_k_by_distance(vec![batch1, batch2], 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Schema mismatch"));
    }

    #[tokio::test]
    async fn test_rrf_basic() {
        use arrow::array::UInt64Array;
        // lancedb RRFReranker expects _rowid column (lance::dataset::ROW_ID)
        let ann = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_rowid", DataType::UInt64, false),
                Field::new("_distance", DataType::Float32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![1, 2, 3])),
                Arc::new(Float32Array::from(vec![0.1, 0.2, 0.3])),
            ],
        ).unwrap();

        let fts = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_rowid", DataType::UInt64, false),
                Field::new("_score", DataType::Float32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![2, 3, 4])),
                Arc::new(Float32Array::from(vec![0.9, 0.8, 0.7])),
            ],
        ).unwrap();

        let result = reciprocal_rank_fusion(ann, fts, 60.0, 3).await.unwrap();
        assert!(result.num_rows() > 0);
        assert!(result.num_rows() <= 5); // 5 unique row_ids total
        // Result should have _relevance_score column
        assert!(result.schema().column_with_name("_relevance_score").is_some());
    }

    /// Performance regression test: merge 10 batches × 1000 rows in <50ms.
    #[test]
    fn test_merge_performance_regression() {
        let n = 1000;
        let num_batches = 10;
        let batches: Vec<RecordBatch> = (0..num_batches).map(|b| {
            let ids: Vec<i32> = (0..n).map(|i| b * n + i).collect();
            let distances: Vec<f32> = (0..n).map(|i| (i as f32) * 0.001 + (b as f32) * 0.01).collect();
            make_distance_batch(ids, distances)
        }).collect();

        let start = std::time::Instant::now();
        let result = global_top_k_by_distance(batches, 100).unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result.num_rows(), 100);
        // Should complete well under 50ms even on slow CI
        assert!(elapsed.as_millis() < 50, "Merge took {}ms (>50ms regression)", elapsed.as_millis());
    }
}
