// Licensed under the Apache License, Version 2.0.
// Test: one cluster serves multiple logical tables with different schemas.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;
use tokio::time::sleep;

use lanceforge::config::{ClusterConfig, ExecutorConfig, ShardConfig, TableConfig};
use lanceforge::executor::LanceTableRegistry;
use lanceforge::generated::lance_distributed::{
    lance_executor_service_server::LanceExecutorServiceServer,
    lance_scheduler_service_server::LanceSchedulerServiceServer,
    lance_scheduler_service_client::LanceSchedulerServiceClient,
    AnnSearchRequest,
};
use lanceforge::coordinator::CoordinatorService;
use lanceforge::worker::WorkerService;

async fn get_free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

async fn create_products_shard(path: &str, num_rows: usize, dim: usize, id_offset: i32) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from((id_offset..id_offset + num_rows as i32).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vec!["electronics"; num_rows])),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32, Arc::new(Float32Array::from(vec![0.5_f32; num_rows * dim])), None,
        ).unwrap()),
    ]).unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, path, Some(p)).await.unwrap();
}

async fn create_documents_shard(path: &str, num_rows: usize, dim: usize, id_offset: i32) {
    // Different schema: has "title" instead of "category", different dim
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("embedding", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from((id_offset..id_offset + num_rows as i32).collect::<Vec<_>>())),
        Arc::new(StringArray::from((0..num_rows).map(|i| format!("doc_{}", i)).collect::<Vec<_>>())),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32, Arc::new(Float32Array::from(vec![-0.5_f32; num_rows * dim])), None,
        ).unwrap()),
    ]).unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, path, Some(p)).await.unwrap();
}

/// Test: two tables with different schemas on the same cluster
#[tokio::test]
async fn test_multi_table_different_schemas() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let product_dim = 8;
    let doc_dim = 16; // different dimension!

    let prod_shard = tmp.path().join("products_shard_00.lance");
    let doc_shard = tmp.path().join("documents_shard_00.lance");

    create_products_shard(prod_shard.to_str().unwrap(), 50, product_dim, 0).await;
    create_documents_shard(doc_shard.to_str().unwrap(), 30, doc_dim, 1000).await;

    let ep = get_free_port().await;
    let sp = get_free_port().await;

    // One executor serves both tables
    let config = ClusterConfig {
        tables: vec![
            TableConfig {
                name: "products".into(),
                shards: vec![ShardConfig {
                    name: "products_shard_00".into(),
                    uri: prod_shard.to_str().unwrap().into(),
                    executors: vec!["e0".into()],
                }],
            },
            TableConfig {
                name: "documents".into(),
                shards: vec![ShardConfig {
                    name: "documents_shard_00".into(),
                    uri: doc_shard.to_str().unwrap().into(),
                    executors: vec!["e0".into()],
                }],
            },
        ],
        executors: vec![ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: ep , role: Default::default()}],
        ..Default::default()
    };

    // Executor loads both table shards
    let all_shards = config.shards_for_executor("e0");
    assert_eq!(all_shards.len(), 2, "Executor should have shards from both tables");

    let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &all_shards).await.unwrap());
    let svc = WorkerService::new(reg);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{ep}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc))
            .serve(addr).await.unwrap();
    });

    let sched = CoordinatorService::new(&config, Duration::from_secs(10));
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{sp}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched))
            .serve(addr).await.unwrap();
    });

    sleep(Duration::from_millis(3000)).await;

    let mut client = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{sp}")
    ).await.unwrap();

    // Query products table (dim=8)
    let resp1 = client.ann_search(AnnSearchRequest {
        table_name: "products".into(),
        vector_column: "vector".into(),
        query_vector: vec![0_f32; product_dim].iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: product_dim as u32,
        k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
        query_text: None,
        offset: 0,
    }).await.unwrap().into_inner();

    assert!(resp1.error.is_empty(), "Products query error: {}", resp1.error);
    assert!(resp1.num_rows > 0, "Products should return results");

    let batch1 = lanceforge::executor::ipc_to_record_batch(&resp1.arrow_ipc_data).unwrap();
    assert!(batch1.schema().column_with_name("category").is_some(), "Products should have 'category' column");
    let ids1: Vec<i32> = batch1.column_by_name("id").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap()
        .values().to_vec();
    assert!(ids1.iter().all(|&id| id < 1000), "Products IDs should be < 1000");
    println!("✓ Products: {} results, has 'category' column", batch1.num_rows());

    // Query documents table (dim=16, different vector column name)
    let resp2 = client.ann_search(AnnSearchRequest {
        table_name: "documents".into(),
        vector_column: "embedding".into(), // different column name!
        query_vector: vec![0_f32; doc_dim].iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: doc_dim as u32,
        k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
        query_text: None,
        offset: 0,
    }).await.unwrap().into_inner();

    assert!(resp2.error.is_empty(), "Documents query error: {}", resp2.error);
    assert!(resp2.num_rows > 0, "Documents should return results");

    let batch2 = lanceforge::executor::ipc_to_record_batch(&resp2.arrow_ipc_data).unwrap();
    assert!(batch2.schema().column_with_name("title").is_some(), "Documents should have 'title' column");
    let ids2: Vec<i32> = batch2.column_by_name("id").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap()
        .values().to_vec();
    assert!(ids2.iter().all(|&id| id >= 1000), "Document IDs should be >= 1000");
    println!("✓ Documents: {} results, has 'title' column", batch2.num_rows());

    // Query nonexistent table → NOT_FOUND
    let resp3 = client.ann_search(AnnSearchRequest {
        table_name: "nonexistent".into(),
        vector_column: "v".into(),
        query_vector: vec![0; 32], dimension: 8,
        k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
        query_text: None,
        offset: 0,
    }).await;
    assert!(resp3.is_err());
    println!("✓ Nonexistent table: NOT_FOUND");

    println!("\n=== Multi-table tests passed ===");
}
