// Licensed under the Apache License, Version 2.0.
// Edge case and robustness tests for LanceForge integration.

use std::sync::Arc;

use arrow::array::{Float32Array, Int32Array, RecordBatch, StringArray, FixedSizeListArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;

use lanceforge::codec::proto::{LanceQueryDescriptor, LanceQueryType, VectorQueryParams};
use lanceforge::config::ShardConfig;
use lanceforge::executor::LanceTableRegistry;
use lanceforge::merge::{global_top_k_by_distance, global_top_k_by_score, reciprocal_rank_fusion};

// ============================================================
// Helper: create lance shard
// ============================================================

async fn create_shard(path: &str, num_rows: usize, dim: usize, seed: u64, id_offset: i32) {
    let mut state = seed;
    let mut flat_values = Vec::with_capacity(num_rows * dim);
    for _ in 0..(num_rows * dim) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        flat_values.push(((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }

    let ids: Vec<i32> = (id_offset..id_offset + num_rows as i32).collect();
    let categories: Vec<String> = ids.iter().map(|id| format!("cat_{}", id % 5)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from(ids)),
        Arc::new(StringArray::from(categories)),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
            Arc::new(Float32Array::from(flat_values)),
            None,
        ).unwrap()),
    ]).unwrap();

    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, path, Some(params)).await.unwrap();
}

// ============================================================
// Executor validation tests
// ============================================================

#[tokio::test]
async fn test_executor_k_zero_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("shard.lance");
    create_shard(path.to_str().unwrap(), 10, 4, 1, 0).await;

    let shards = vec![ShardConfig {
        name: "shard".into(), uri: path.to_str().unwrap().into(), executors: vec![],
    }];
    let reg = LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap();

    let descriptor = LanceQueryDescriptor {
        table_name: "shard".into(),
        query_type: LanceQueryType::Ann as i32,
        vector_query: Some(VectorQueryParams {
            column: "vector".into(),
            vector_data: [0_f32; 4].iter().flat_map(|f| f.to_le_bytes()).collect(),
            dimension: 4,
            nprobes: 1,
            metric_type: 0,
            oversample_factor: 1,
        }),
        fts_query: None,
        filter: None,
        k: 0, // Invalid!
        columns: vec![],
    };

    let result = reg.execute_query(&descriptor).await;
    assert!(result.is_err(), "k=0 should be rejected");
    assert!(result.unwrap_err().to_string().contains("k must be > 0"));
}

#[tokio::test]
async fn test_executor_ann_without_vector_query_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("shard.lance");
    create_shard(path.to_str().unwrap(), 10, 4, 1, 0).await;

    let shards = vec![ShardConfig {
        name: "shard".into(), uri: path.to_str().unwrap().into(), executors: vec![],
    }];
    let reg = LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap();

    let descriptor = LanceQueryDescriptor {
        table_name: "shard".into(),
        query_type: LanceQueryType::Ann as i32,
        vector_query: None, // Missing for ANN query!
        fts_query: None,
        filter: None,
        k: 5,
        columns: vec![],
    };

    let result = reg.execute_query(&descriptor).await;
    assert!(result.is_err(), "ANN without vector_query should be rejected");
    assert!(result.unwrap_err().to_string().contains("vector_query"));
}

#[tokio::test]
async fn test_executor_empty_vector_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("shard.lance");
    create_shard(path.to_str().unwrap(), 10, 4, 1, 0).await;

    let shards = vec![ShardConfig {
        name: "shard".into(), uri: path.to_str().unwrap().into(), executors: vec![],
    }];
    let reg = LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap();

    let descriptor = LanceQueryDescriptor {
        table_name: "shard".into(),
        query_type: LanceQueryType::Ann as i32,
        vector_query: Some(VectorQueryParams {
            column: "vector".into(),
            vector_data: vec![], // Empty vector!
            dimension: 0,
            nprobes: 1,
            metric_type: 0,
            oversample_factor: 1,
        }),
        fts_query: None,
        filter: None,
        k: 5,
        columns: vec![],
    };

    let result = reg.execute_query(&descriptor).await;
    assert!(result.is_err(), "Empty vector should be rejected");
}

#[tokio::test]
async fn test_executor_dimension_mismatch_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("shard.lance");
    create_shard(path.to_str().unwrap(), 10, 4, 1, 0).await;

    let shards = vec![ShardConfig {
        name: "shard".into(), uri: path.to_str().unwrap().into(), executors: vec![],
    }];
    let reg = LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap();

    let descriptor = LanceQueryDescriptor {
        table_name: "shard".into(),
        query_type: LanceQueryType::Ann as i32,
        vector_query: Some(VectorQueryParams {
            column: "vector".into(),
            vector_data: [0_f32; 4].iter().flat_map(|f| f.to_le_bytes()).collect(),
            dimension: 8, // Claims 8 but data has 4
            nprobes: 1,
            metric_type: 0,
            oversample_factor: 1,
        }),
        fts_query: None,
        filter: None,
        k: 5,
        columns: vec![],
    };

    let result = reg.execute_query(&descriptor).await;
    assert!(result.is_err(), "Dimension mismatch should be rejected");
    assert!(result.unwrap_err().to_string().contains("dimension"));
}

#[tokio::test]
async fn test_executor_unknown_table_rejected() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("shard.lance");
    create_shard(path.to_str().unwrap(), 10, 4, 1, 0).await;

    let shards = vec![ShardConfig {
        name: "shard".into(), uri: path.to_str().unwrap().into(), executors: vec![],
    }];
    let reg = LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap();

    let descriptor = LanceQueryDescriptor {
        table_name: "nonexistent_table".into(),
        query_type: LanceQueryType::Ann as i32,
        vector_query: Some(VectorQueryParams {
            column: "vector".into(),
            vector_data: [0_f32; 4].iter().flat_map(|f| f.to_le_bytes()).collect(),
            dimension: 4,
            nprobes: 1,
            metric_type: 0,
            oversample_factor: 1,
        }),
        fts_query: None,
        filter: None,
        k: 5,
        columns: vec![],
    };

    let result = reg.execute_query(&descriptor).await;
    assert!(result.is_err(), "Unknown table should be rejected");
}

#[tokio::test]
async fn test_executor_table_name_prefix_safety() {
    let tmp = TempDir::new().unwrap();
    let p1 = tmp.path().join("prod.lance");
    let p2 = tmp.path().join("prod_shard_00.lance");
    let p3 = tmp.path().join("production.lance"); // should NOT match "prod"

    create_shard(p1.to_str().unwrap(), 10, 4, 1, 0).await;
    create_shard(p2.to_str().unwrap(), 10, 4, 2, 100).await;
    create_shard(p3.to_str().unwrap(), 10, 4, 3, 200).await;

    let shards = vec![
        ShardConfig { name: "prod".into(), uri: p1.to_str().unwrap().into(), executors: vec![] },
        ShardConfig { name: "prod_shard_00".into(), uri: p2.to_str().unwrap().into(), executors: vec![] },
        ShardConfig { name: "production".into(), uri: p3.to_str().unwrap().into(), executors: vec![] },
    ];
    let reg = LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap();

    let descriptor = LanceQueryDescriptor {
        table_name: "prod".into(),
        query_type: LanceQueryType::Ann as i32,
        vector_query: Some(VectorQueryParams {
            column: "vector".into(),
            vector_data: [0_f32; 4].iter().flat_map(|f| f.to_le_bytes()).collect(),
            dimension: 4,
            nprobes: 1,
            metric_type: 0,
            oversample_factor: 1,
        }),
        fts_query: None,
        filter: None,
        k: 50,
        columns: vec![],
    };

    let result = reg.execute_query(&descriptor).await.unwrap();
    // Should match "prod" and "prod_shard_00" but NOT "production"
    // Total: 20 rows (10 + 10), not 30
    let id_idx = result.schema().index_of("id").unwrap();
    let ids = result.column(id_idx).as_any().downcast_ref::<Int32Array>().unwrap();
    let max_id = (0..ids.len()).map(|i| ids.value(i)).max().unwrap_or(0);
    // "production" starts at id_offset=200, so if we see id>=200, the prefix matching is wrong
    assert!(max_id < 200, "Should NOT match 'production' table, but got id={}", max_id);
}

// ============================================================
// Concurrent query test
// ============================================================

#[tokio::test]
async fn test_executor_concurrent_queries() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("shard.lance");
    create_shard(path.to_str().unwrap(), 100, 8, 42, 0).await;

    let shards = vec![ShardConfig {
        name: "test".into(), uri: path.to_str().unwrap().into(), executors: vec![],
    }];
    let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap());

    // Fire 10 concurrent queries
    let mut handles = Vec::new();
    for i in 0..10 {
        let reg = reg.clone();
        handles.push(tokio::spawn(async move {
            let descriptor = LanceQueryDescriptor {
                table_name: "test".into(),
                query_type: LanceQueryType::Ann as i32,
                vector_query: Some(VectorQueryParams {
                    column: "vector".into(),
                    vector_data: [i as f32; 8].iter().flat_map(|f| f.to_le_bytes()).collect(),
                    dimension: 8,
                    nprobes: 1,
                    metric_type: 0,
                    oversample_factor: 1,
                }),
                fts_query: None,
                filter: None,
                k: 5,
                columns: vec![],
            };
            reg.execute_query(&descriptor).await
        }));
    }

    let mut success = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(batch) => {
                assert!(batch.num_rows() > 0);
                success += 1;
            }
            Err(e) => panic!("Concurrent query failed: {}", e),
        }
    }
    assert_eq!(success, 10, "All 10 concurrent queries should succeed");
}

// ============================================================
// IPC round-trip edge cases
// ============================================================

#[test]
fn test_ipc_empty_data_error() {
    let result = lanceforge::executor::ipc_to_record_batch(&[]);
    assert!(result.is_err());
}

#[test]
fn test_ipc_corrupt_data_error() {
    let result = lanceforge::executor::ipc_to_record_batch(&[0xFF, 0xFE, 0xFD]);
    assert!(result.is_err());
}

#[test]
fn test_ipc_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("_distance", DataType::Float32, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(Float32Array::from(vec![0.1, 0.2, 0.3])),
    ]).unwrap();

    let ipc = lanceforge::executor::record_batch_to_ipc(&batch).unwrap();
    let decoded = lanceforge::executor::ipc_to_record_batch(&ipc).unwrap();

    assert_eq!(decoded.num_rows(), 3);
    assert_eq!(decoded.schema(), batch.schema());
}

// ============================================================
// Merge edge cases
// ============================================================

#[test]
fn test_merge_score_descending() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("_score", DataType::Float32, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(Float32Array::from(vec![0.5, 0.9, 0.1])),
    ]).unwrap();

    let result = global_top_k_by_score(vec![batch], 2).unwrap();
    let scores = result.column(1).as_any().downcast_ref::<Float32Array>().unwrap();
    assert!((scores.value(0) - 0.9).abs() < 1e-6, "First should be highest score");
    assert!((scores.value(1) - 0.5).abs() < 1e-6, "Second should be second highest");
}

#[test]
fn test_merge_missing_sort_column_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(vec![1])),
    ]).unwrap();

    let result = global_top_k_by_distance(vec![batch], 1);
    assert!(result.is_err(), "Missing _distance column should error");
}

#[tokio::test]
async fn test_rrf_k_param_zero_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_rowid", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::new_empty(schema);
    let result = reciprocal_rank_fusion(batch.clone(), batch, 0.0, 5).await;
    assert!(result.is_err(), "k_param=0 should error");
}
