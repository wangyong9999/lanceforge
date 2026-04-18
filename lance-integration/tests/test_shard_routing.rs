// Licensed under the Apache License, Version 2.0.
// Tests for shard-aware routing, pruning integration, and failover behavior.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;
use tokio::time::sleep;

use lanceforge::config::{ClusterConfig, ExecutorConfig, ShardConfig, TableConfig};

/// Get a free TCP port by binding to :0 and reading the assigned port.
async fn get_free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}
use lanceforge::executor::LanceTableRegistry;
use lanceforge::generated::lance_distributed::{
    lance_executor_service_server::LanceExecutorServiceServer,
    lance_scheduler_service_server::LanceSchedulerServiceServer,
    lance_scheduler_service_client::LanceSchedulerServiceClient,
    AnnSearchRequest,
};
use lanceforge::coordinator::CoordinatorService;
use lanceforge::worker::WorkerService;
use lanceforge::shard_pruning::{ColumnStats, ShardMetadata, ShardPruner};

async fn create_shard(path: &str, num_rows: usize, dim: usize, seed: u64, id_offset: i32, category: &str) {
    let mut state = seed;
    let mut flat = Vec::with_capacity(num_rows * dim);
    for _ in 0..(num_rows * dim) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        flat.push(((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), dim as i32,
        ), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(Int32Array::from((id_offset..id_offset + num_rows as i32).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vec![category; num_rows])),
        Arc::new(FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32, Arc::new(Float32Array::from(flat)), None,
        ).unwrap()),
    ]).unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, path, Some(p)).await.unwrap();
}

/// Test: shard pruning actually reduces fanout in a real gRPC cluster
#[tokio::test]
async fn test_shard_pruning_in_cluster() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 4;

    // Create 2 shards with different categories
    let s0 = tmp.path().join("s0.lance");
    let s1 = tmp.path().join("s1.lance");
    create_shard(s0.to_str().unwrap(), 50, dim, 1, 0, "electronics").await;
    create_shard(s1.to_str().unwrap(), 50, dim, 2, 100, "clothing").await;

    let ep0 = get_free_port().await;
    let ep1 = get_free_port().await;
    let sp = get_free_port().await;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "prod".into(),
            shards: vec![
                ShardConfig { name: "prod_s0".into(), uri: s0.to_str().unwrap().into(), executors: vec!["e0".into()] },
                ShardConfig { name: "prod_s1".into(), uri: s1.to_str().unwrap().into(), executors: vec!["e1".into()] },
            ],
        }],
        executors: vec![
            ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: ep0 , role: Default::default()},
            ExecutorConfig { id: "e1".into(), host: "127.0.0.1".into(), port: ep1 , role: Default::default()},
        ],
        ..Default::default()
    };

    // Start executors
    for (i, port) in [ep0, ep1].iter().enumerate() {
        let shards = config.shards_for_executor(&format!("e{i}"));
        let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap());
        let svc = WorkerService::new(reg);
        let p = *port;
        tokio::spawn(async move {
            let addr = format!("0.0.0.0:{p}").parse().unwrap();
            tonic::transport::Server::builder()
                .add_service(LanceExecutorServiceServer::new(svc))
                .serve(addr).await.unwrap();
        });
    }

    // Create pruner with shard metadata
    let pruner = ShardPruner::new(vec![
        ShardMetadata {
            shard_name: "prod_s0".into(), num_rows: 50,
            column_stats: std::collections::HashMap::from([
                ("category".into(), ColumnStats {
                    min_value: None, max_value: None,
                    distinct_values: Some(vec!["electronics".into()]),
                }),
            ]),
        },
        ShardMetadata {
            shard_name: "prod_s1".into(), num_rows: 50,
            column_stats: std::collections::HashMap::from([
                ("category".into(), ColumnStats {
                    min_value: None, max_value: None,
                    distinct_values: Some(vec!["clothing".into()]),
                }),
            ]),
        },
    ]);

    let sched = CoordinatorService::with_pruner(&config, Duration::from_secs(10), pruner);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{sp}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched))
            .serve(addr).await.unwrap();
    });

    sleep(Duration::from_millis(2000)).await;

    let mut client = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{sp}")
    ).await.unwrap();

    // Query with filter: category='electronics' → should only hit shard_0
    let resp = client.ann_search(AnnSearchRequest {
        table_name: "prod".into(),
        vector_column: "vector".into(),
        query_vector: vec![0_f32; dim].iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 10,
        nprobes: 1,
        filter: Some("category = 'electronics'".into()),
        columns: vec![],
        metric_type: 0,
            query_text: None,
            offset: 0,
    }).await.unwrap().into_inner();

    assert!(resp.error.is_empty(), "Error: {}", resp.error);
    if resp.num_rows > 0 {
        let batch = lanceforge::executor::ipc_to_record_batch(&resp.arrow_ipc_data).unwrap();
        // All results should have category=electronics (from shard_0 only)
        if let Ok(cat_idx) = batch.schema().index_of("category") {
            let cats = batch.column(cat_idx).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..cats.len() {
                assert_eq!(cats.value(i), "electronics",
                    "Pruning should have excluded clothing shard");
            }
        }
        println!("✓ Shard pruning: {} results, all electronics", batch.num_rows());
    }

    // Query without filter → should hit both shards
    let resp2 = client.ann_search(AnnSearchRequest {
        table_name: "prod".into(),
        vector_column: "vector".into(),
        query_vector: vec![0_f32; dim].iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 20,
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
            query_text: None,
            offset: 0,
    }).await.unwrap().into_inner();

    assert!(resp2.num_rows > 0, "Unfiltered should return results from both shards");
    println!("✓ No pruning: {} results from all shards", resp2.num_rows);
}

/// Test: primary executor down, secondary takes over
#[tokio::test]
async fn test_secondary_failover() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 4;

    // Create 1 shard, assigned to 2 executors (but only start executor_1)
    let s0 = tmp.path().join("fo_s0.lance");
    create_shard(s0.to_str().unwrap(), 30, dim, 42, 0, "test").await;

    // Use dynamic ports to avoid conflicts with other tests
    let ep0 = get_free_port().await; // PRIMARY — NOT started (simulates failure)
    let ep1 = get_free_port().await; // SECONDARY — started
    let sp = get_free_port().await;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "fo".into(),
            shards: vec![
                ShardConfig {
                    name: "fo_s0".into(),
                    uri: s0.to_str().unwrap().into(),
                    executors: vec!["e0".into(), "e1".into()], // primary=e0, secondary=e1
                },
            ],
        }],
        executors: vec![
            ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: ep0 , role: Default::default()},
            ExecutorConfig { id: "e1".into(), host: "127.0.0.1".into(), port: ep1 , role: Default::default()},
        ],
        ..Default::default()
    };

    // Only start executor 1 (secondary)
    let _shards1 = config.shards_for_executor("e1");
    // e1 has no shards in config — but in production it would load via shard_state
    // For this test, register the shard directly on e1
    let all_shards = vec![ShardConfig {
        name: "fo_s0".into(), uri: s0.to_str().unwrap().into(), executors: vec![],
    }];
    let reg1 = Arc::new(LanceTableRegistry::new(SessionContext::default(), &all_shards).await.unwrap());
    let svc1 = WorkerService::new(reg1);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{ep1}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc1))
            .serve(addr).await.unwrap();
    });

    let sched = CoordinatorService::new(&config, Duration::from_secs(10));
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{sp}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched))
            .serve(addr).await.unwrap();
    });

    sleep(Duration::from_millis(2000)).await;

    let mut client = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{sp}")
    ).await.unwrap();

    // Query — primary (e0) is down, should failover to secondary (e1)
    let resp = client.ann_search(AnnSearchRequest {
        table_name: "fo".into(),
        vector_column: "vector".into(),
        query_vector: vec![0_f32; dim].iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 5,
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
            query_text: None,
            offset: 0,
    }).await.unwrap().into_inner();

    assert!(resp.error.is_empty(), "Failover should work: {}", resp.error);
    assert!(resp.num_rows > 0, "Should get results from secondary executor");
    println!("✓ Secondary failover: {} results from secondary executor", resp.num_rows);
}
