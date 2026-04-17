// Licensed under the Apache License, Version 2.0.
// Test: data freshness — new writes become visible to running Executor.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    FixedSizeListArray, Float32Array, Int32Array, RecordBatch,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;
use tokio::time::sleep;

use lanceforge::codec::proto::{LanceQueryDescriptor, LanceQueryType, VectorQueryParams};
use lanceforge::config::ShardConfig;
use lanceforge::executor::LanceTableRegistry;

async fn create_shard(path: &str, num_rows: usize, dim: usize, id_offset: i32) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from((id_offset..id_offset + num_rows as i32).collect::<Vec<_>>())),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
            Arc::new(Float32Array::from(vec![0.0_f32; num_rows * dim])),
            None,
        ).unwrap()),
    ]).unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, path, Some(p)).await.unwrap();
}

async fn append_rows(path: &str, num_rows: usize, dim: usize, id_offset: i32) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from((id_offset..id_offset + num_rows as i32).collect::<Vec<_>>())),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
            Arc::new(Float32Array::from(vec![1.0_f32; num_rows * dim])),
            None,
        ).unwrap()),
    ]).unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Append,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, path, Some(p)).await.unwrap();
}

/// Test: Executor sees new data after write (RefreshableDataset auto-refresh)
#[tokio::test]
async fn test_data_freshness_after_append() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 4;
    let path = tmp.path().join("fresh.lance");
    let path_str = path.to_str().unwrap();

    // Create initial data: 10 rows, ids 0-9
    create_shard(path_str, 10, dim, 0).await;

    // Open Executor with RefreshableDataset (3s refresh interval)
    let shards = vec![ShardConfig {
        name: "fresh".into(),
        uri: path_str.into(),
        executors: vec![],
    }];
    let reg = LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap();

    // Query 1: should see 10 rows
    let descriptor = LanceQueryDescriptor {
        table_name: "fresh".into(),
        query_type: LanceQueryType::Ann as i32,
        vector_query: Some(VectorQueryParams {
            column: "vector".into(),
            vector_data: vec![0_f32; dim].iter().flat_map(|f| f.to_le_bytes()).collect(),
            dimension: dim as u32,
            nprobes: 1,
            metric_type: 0,
            oversample_factor: 1,
        }),
        fts_query: None,
        filter: None,
        k: 100,
        columns: vec![],
    };

    let result1 = reg.execute_query(&descriptor).await.unwrap();
    let count1 = result1.num_rows();
    println!("Before append: {} rows", count1);
    assert_eq!(count1, 10, "Should see initial 10 rows");

    // Append 5 more rows, ids 100-104
    append_rows(path_str, 5, dim, 100).await;

    // Verify the data is actually written
    let ds = lance::dataset::Dataset::open(path_str).await.unwrap();
    let total = ds.count_rows(None).await.unwrap();
    assert_eq!(total, 15, "Dataset on disk should have 15 rows");

    // Wait for refresh interval (3s) + margin
    println!("Waiting for RefreshableDataset to pick up new version...");
    sleep(Duration::from_secs(5)).await;

    // Query 2: should see 15 rows (10 original + 5 appended)
    let result2 = reg.execute_query(&descriptor).await.unwrap();
    let count2 = result2.num_rows();
    println!("After append + refresh: {} rows", count2);
    assert!(count2 > count1, "Should see more rows after append. Before={}, After={}", count1, count2);
    assert_eq!(count2, 15, "Should see all 15 rows");

    println!("✓ Data freshness: {} → {} rows after append", count1, count2);
}
