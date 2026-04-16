// Licensed under the Apache License, Version 2.0.
// Test: Coordinator HA — multiple active Coordinators, failover.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray};
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
    tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port()
}

async fn create_shard(path: &str, num_rows: usize, dim: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from((0..num_rows as i32).collect::<Vec<_>>())),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32, Arc::new(Float32Array::from(vec![0.5_f32; num_rows * dim])), None,
        ).unwrap()),
    ]).unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut p = lance::dataset::WriteParams::default();
    p.mode = lance::dataset::WriteMode::Create;
    lance::dataset::Dataset::write(reader, path, Some(p)).await.unwrap();
}

/// Test: two Coordinators serving same Workers, kill one, queries still work
#[tokio::test]
async fn test_multi_coordinator_failover() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 4;
    let s0 = tmp.path().join("ha_s0.lance");
    create_shard(s0.to_str().unwrap(), 50, dim).await;

    let worker_port = get_free_port().await;
    let coord1_port = get_free_port().await;
    let coord2_port = get_free_port().await;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "ha".into(),
            shards: vec![ShardConfig {
                name: "ha_s0".into(),
                uri: s0.to_str().unwrap().into(),
                executors: vec!["w0".into()],
            }],
        }],
        executors: vec![ExecutorConfig {
            id: "w0".into(), host: "127.0.0.1".into(), port: worker_port,
        }],
        ..Default::default()
    };

    // Start Worker
    let shards = config.shards_for_executor("w0");
    let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap());
    let w_svc = WorkerService::new(reg);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{worker_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(w_svc))
            .serve(addr).await.unwrap();
    });

    // Start Coordinator 1
    let c1_svc = CoordinatorService::new(&config, Duration::from_secs(10));
    let c1_handle = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{coord1_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(c1_svc))
            .serve(addr).await.unwrap();
    });

    // Start Coordinator 2
    let c2_svc = CoordinatorService::new(&config, Duration::from_secs(10));
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{coord2_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(c2_svc))
            .serve(addr).await.unwrap();
    });

    sleep(Duration::from_millis(3000)).await;

    let query_vec: Vec<u8> = vec![0_f32; dim].iter().flat_map(|f| f.to_le_bytes()).collect();

    // Query Coordinator 1
    let mut c1 = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{coord1_port}")
    ).await.unwrap();

    let resp1 = c1.ann_search(AnnSearchRequest {
        table_name: "ha".into(), vector_column: "vector".into(),
        query_vector: query_vec.clone(), dimension: dim as u32,
        k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
            offset: 0,
        query_text: None,
    }).await.unwrap().into_inner();
    assert!(resp1.error.is_empty());
    assert!(resp1.num_rows > 0);
    println!("✓ Coordinator 1: {} results", resp1.num_rows);

    // Query Coordinator 2
    let mut c2 = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{coord2_port}")
    ).await.unwrap();

    let resp2 = c2.ann_search(AnnSearchRequest {
        table_name: "ha".into(), vector_column: "vector".into(),
        query_vector: query_vec.clone(), dimension: dim as u32,
        k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
            offset: 0,
        query_text: None,
    }).await.unwrap().into_inner();
    assert!(resp2.error.is_empty());
    assert!(resp2.num_rows > 0);
    println!("✓ Coordinator 2: {} results", resp2.num_rows);

    // Kill Coordinator 1
    c1_handle.abort();
    sleep(Duration::from_millis(500)).await;

    // Coordinator 1 should be dead
    let c1_result = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{coord1_port}")
    ).await;
    // Connection may succeed (TCP) but query should fail or connect should fail
    let c1_dead = match c1_result {
        Err(_) => true,
        Ok(mut c) => c.ann_search(AnnSearchRequest {
            table_name: "ha".into(), vector_column: "vector".into(),
            query_vector: query_vec.clone(), dimension: dim as u32,
            k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
            offset: 0,
            query_text: None,
        }).await.is_err(),
    };
    assert!(c1_dead, "Coordinator 1 should be dead");
    println!("✓ Coordinator 1 is dead after abort");

    // Coordinator 2 still works!
    let resp3 = c2.ann_search(AnnSearchRequest {
        table_name: "ha".into(), vector_column: "vector".into(),
        query_vector: query_vec.clone(), dimension: dim as u32,
        k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
            offset: 0,
        query_text: None,
    }).await.unwrap().into_inner();
    assert!(resp3.error.is_empty());
    assert!(resp3.num_rows > 0);
    println!("✓ Coordinator 2 still works after Coordinator 1 death: {} results", resp3.num_rows);

    println!("\n=== HA failover test passed ===");
}
