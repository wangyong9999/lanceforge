// Licensed under the Apache License, Version 2.0.
// Test: text-based query with auto-embedding via mock provider.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;
use tokio::time::sleep;

use ballista_lance::config::{
    ClusterConfig, EmbeddingConfig, ExecutorConfig, ShardConfig, TableConfig,
};
use ballista_lance::executor::LanceTableRegistry;
use ballista_lance::generated::lance_distributed::{
    lance_executor_service_server::LanceExecutorServiceServer,
    lance_scheduler_service_server::LanceSchedulerServiceServer,
    lance_scheduler_service_client::LanceSchedulerServiceClient,
    AnnSearchRequest,
};
use ballista_lance::coordinator::CoordinatorService;
use ballista_lance::worker::WorkerService;

async fn create_shard(path: &str, num_rows: usize, dim: usize, seed: u64, id_offset: i32) {
    let mut state = seed;
    let mut flat = Vec::with_capacity(num_rows * dim);
    for _ in 0..(num_rows * dim) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        flat.push(((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from((id_offset..id_offset + num_rows as i32).collect::<Vec<_>>())),
        Arc::new(StringArray::from((0..num_rows).map(|i| format!("doc_{}", i + id_offset as usize)).collect::<Vec<_>>())),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32, Arc::new(Float32Array::from(flat)), None,
        ).unwrap()),
    ]).unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut p = lance::dataset::WriteParams::default();
    p.mode = lance::dataset::WriteMode::Create;
    lance::dataset::Dataset::write(reader, path, Some(p)).await.unwrap();
}

/// Test: text query with mock embedding — full gRPC flow
#[tokio::test]
async fn test_text_query_with_mock_embedding() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 32;

    let s0 = tmp.path().join("s0.lance");
    create_shard(s0.to_str().unwrap(), 100, dim, 42, 0).await;

    let ep = 51000_u16;
    let sp = 51001_u16;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "docs".into(),
            shards: vec![
                ShardConfig { name: "docs_s0".into(), uri: s0.to_str().unwrap().into(), executors: vec!["e0".into()] },
            ],
        }],
        executors: vec![
            ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: ep },
        ],
        embedding: Some(EmbeddingConfig {
            provider: "mock".into(),
            model: "test".into(),
            api_key: None,
            dimension: Some(dim as u32),
        }),
        ..Default::default()
    };

    // Start executor
    let shards = config.shards_for_executor("e0");
    let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap());
    let svc = WorkerService::new(reg);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{ep}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc))
            .serve(addr).await.unwrap();
    });

    // Start scheduler with embedding config
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

    // Test 1: Text query (no vector provided, text auto-embedded)
    let resp = client.ann_search(AnnSearchRequest {
        table_name: "docs".into(),
        vector_column: "vector".into(),
        query_vector: vec![],  // EMPTY — text will be embedded
        dimension: 0,           // inferred from embedding
        k: 5,
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
        query_text: Some("find similar documents".into()),
    }).await.unwrap().into_inner();

    assert!(resp.error.is_empty(), "Text query error: {}", resp.error);
    assert!(resp.num_rows > 0, "Text query should return results");
    assert!(resp.num_rows <= 5, "Should respect k=5");
    println!("✓ Text query: {} results", resp.num_rows);

    // Test 2: Vector query still works (backward compat)
    let query_vec: Vec<f32> = vec![0.0; dim];
    let resp2 = client.ann_search(AnnSearchRequest {
        table_name: "docs".into(),
        vector_column: "vector".into(),
        query_vector: query_vec.iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 5,
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
        query_text: None,
    }).await.unwrap().into_inner();

    assert!(resp2.error.is_empty(), "Vector query error: {}", resp2.error);
    assert!(resp2.num_rows > 0, "Vector query should still work");
    println!("✓ Vector query backward compat: {} results", resp2.num_rows);

    // Test 3: Both vector and text — vector takes priority
    let resp3 = client.ann_search(AnnSearchRequest {
        table_name: "docs".into(),
        vector_column: "vector".into(),
        query_vector: query_vec.iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 5,
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
        query_text: Some("ignored because vector is provided".into()),
    }).await.unwrap().into_inner();

    assert!(resp3.error.is_empty());
    assert!(resp3.num_rows > 0);
    println!("✓ Vector+text priority: vector used, {} results", resp3.num_rows);

    // Test 4: Neither vector nor text — error
    let resp4 = client.ann_search(AnnSearchRequest {
        table_name: "docs".into(),
        vector_column: "vector".into(),
        query_vector: vec![],
        dimension: 0,
        k: 5,
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
        query_text: None,  // both empty
    }).await;

    assert!(resp4.is_err(), "Should fail with neither vector nor text");
    println!("✓ Empty query rejected");

    println!("\n=== All text query tests passed ===");
}

/// Test: text query without embedding config — should fail gracefully
#[tokio::test]
async fn test_text_query_no_embedding_config() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 8;
    let s0 = tmp.path().join("s0.lance");
    create_shard(s0.to_str().unwrap(), 10, dim, 1, 0).await;

    let ep = 51010_u16;
    let sp = 51011_u16;

    // NO embedding config
    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "t".into(),
            shards: vec![ShardConfig { name: "t_s0".into(), uri: s0.to_str().unwrap().into(), executors: vec!["e0".into()] }],
        }],
        executors: vec![ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: ep }],
        ..Default::default() // embedding: None
    };

    let shards = config.shards_for_executor("e0");
    let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap());
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{ep}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(WorkerService::new(reg)))
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

    // Text query without embedding config → should fail with clear message
    let resp = client.ann_search(AnnSearchRequest {
        table_name: "t".into(),
        vector_column: "vector".into(),
        query_vector: vec![],
        dimension: 0,
        k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
        query_text: Some("hello".into()),
    }).await;

    assert!(resp.is_err(), "Should fail without embedding config");
    let err_msg = resp.unwrap_err().message().to_string();
    assert!(err_msg.contains("embedding"), "Error should mention embedding: {}", err_msg);
    println!("✓ Text query without config: clear error message");
}
