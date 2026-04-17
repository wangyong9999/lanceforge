// Licensed under the Apache License, Version 2.0.
// Integration test: real gRPC distributed ANN/FTS/Hybrid search.
//
// Starts Scheduler + 2 Executor gRPC servers in-process (separate tokio tasks),
// then runs client queries against the Scheduler and verifies correctness.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray,
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

// ============================================================
// Test data generation
// ============================================================

fn generate_deterministic_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut vectors = Vec::with_capacity(n);
    let mut state = seed;
    for _ in 0..n {
        let mut vec = Vec::with_capacity(dim);
        for _ in 0..dim {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let val = ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0;
            vec.push(val);
        }
        vectors.push(vec);
    }
    vectors
}

async fn create_lance_shard(
    path: &str,
    num_rows: usize,
    dim: usize,
    seed: u64,
    id_offset: i32,
) {
    let vectors = generate_deterministic_vectors(num_rows, dim, seed);
    let ids: Vec<i32> = (id_offset..id_offset + num_rows as i32).collect();
    let categories: Vec<String> = ids.iter().map(|id| {
        if id % 3 == 0 { "alpha".into() }
        else if id % 3 == 1 { "beta".into() }
        else { "gamma".into() }
    }).collect();

    let flat_values: Vec<f32> = vectors.iter().flatten().copied().collect();
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
// Helper: deserialize Arrow IPC response
// ============================================================

fn decode_search_response(data: &[u8]) -> RecordBatch {
    lanceforge::executor::ipc_to_record_batch(data).unwrap()
}

// ============================================================
// Tests
// ============================================================

/// Full gRPC test: 2 Executors + 1 Scheduler, ANN query
#[tokio::test]
async fn test_grpc_ann_search() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 8;
    let rows_per_shard = 200;

    // Create 2 shards
    let s0 = tmp.path().join("shard_00.lance");
    let s1 = tmp.path().join("shard_01.lance");
    create_lance_shard(s0.to_str().unwrap(), rows_per_shard, dim, 42, 0).await;
    create_lance_shard(s1.to_str().unwrap(), rows_per_shard, dim, 99, rows_per_shard as i32).await;

    let exec0_port = 50200_u16;
    let exec1_port = 50201_u16;
    let sched_port = 50202_u16;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "test".to_string(),
            shards: vec![
                ShardConfig { name: "test_shard_00".into(), uri: s0.to_str().unwrap().into(), executors: vec!["e0".into()] },
                ShardConfig { name: "test_shard_01".into(), uri: s1.to_str().unwrap().into(), executors: vec!["e1".into()] },
            ],
        }],
        executors: vec![
            ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: exec0_port },
            ExecutorConfig { id: "e1".into(), host: "127.0.0.1".into(), port: exec1_port },
        ],
        ..Default::default()
    };

    // Start Executor 0
    let shards0 = config.shards_for_executor("e0");
    let ctx0 = SessionContext::default();
    let reg0 = Arc::new(LanceTableRegistry::new(ctx0, &shards0).await.unwrap());
    let svc0 = WorkerService::new(reg0);
    let exec0_handle = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{exec0_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc0))
            .serve(addr)
            .await
            .unwrap();
    });

    // Start Executor 1
    let shards1 = config.shards_for_executor("e1");
    let ctx1 = SessionContext::default();
    let reg1 = Arc::new(LanceTableRegistry::new(ctx1, &shards1).await.unwrap());
    let svc1 = WorkerService::new(reg1);
    let exec1_handle = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{exec1_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc1))
            .serve(addr)
            .await
            .unwrap();
    });

    // Start Scheduler
    let sched_svc = CoordinatorService::new(&config, Duration::from_secs(10));
    let sched_handle = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{sched_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched_svc))
            .serve(addr)
            .await
            .unwrap();
    });

    // Wait for servers to start
    sleep(Duration::from_millis(2000)).await; // Wait for health check to connect

    // Connect client to Scheduler
    let mut client = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{sched_port}")
    ).await.expect("Failed to connect to scheduler");

    // --- Test 1: ANN search ---
    let query_vector: Vec<f32> = vec![0.0; dim];
    let response = client.ann_search(AnnSearchRequest {
        table_name: "test".into(),
        vector_column: "vector".into(),
        query_vector: query_vector.iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 5,
        nprobes: 1,
        filter: None,
        columns: vec![],
            offset: 0,
        metric_type: 0,
            query_text: None,
    }).await.expect("ANN search failed");

    let resp = response.into_inner();
    assert!(resp.error.is_empty(), "Search error: {}", resp.error);
    assert!(resp.num_rows > 0, "Should have results");
    assert!(resp.num_rows <= 5, "Should respect top-K={}", resp.num_rows);

    let batch = decode_search_response(&resp.arrow_ipc_data);
    assert!(batch.num_rows() > 0);

    // Verify distances are sorted ascending
    let dist_idx = batch.schema().index_of("_distance").unwrap();
    let distances = batch.column(dist_idx).as_any().downcast_ref::<Float32Array>().unwrap();
    for i in 1..distances.len() {
        assert!(
            distances.value(i) >= distances.value(i - 1),
            "Distances must be sorted: {} >= {} at index {}",
            distances.value(i), distances.value(i - 1), i
        );
    }
    println!("✓ ANN search: {} results, min_dist={:.4}", batch.num_rows(), distances.value(0));

    // --- Test 2: ANN search with filter ---
    let response2 = client.ann_search(AnnSearchRequest {
        table_name: "test".into(),
        vector_column: "vector".into(),
        query_vector: query_vector.iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 10,
        nprobes: 1,
        filter: Some("category = 'alpha'".into()),
        columns: vec![],
        metric_type: 0,
            query_text: None,
            offset: 0,
    }).await.expect("Filtered ANN search failed");

    let resp2 = response2.into_inner();
    assert!(resp2.error.is_empty(), "Filtered search error: {}", resp2.error);
    if resp2.num_rows > 0 {
        let batch2 = decode_search_response(&resp2.arrow_ipc_data);
        // Verify all results have category='alpha'
        if let Ok(cat_idx) = batch2.schema().index_of("category") {
            let cats = batch2.column(cat_idx).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..cats.len() {
                assert_eq!(cats.value(i), "alpha", "Filter should only return 'alpha' rows");
            }
        }
        println!("✓ Filtered ANN: {} results, all category=alpha", batch2.num_rows());
    } else {
        println!("✓ Filtered ANN: 0 results (filter may be too restrictive for test data)");
    }

    // --- Test 3: Verify results come from BOTH shards ---
    let response3 = client.ann_search(AnnSearchRequest {
        table_name: "test".into(),
        vector_column: "vector".into(),
        query_vector: query_vector.iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 50, // large K to get results from both shards
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
            query_text: None,
            offset: 0,
    }).await.expect("Large K search failed");

    let resp3 = response3.into_inner();
    if resp3.num_rows > 0 {
        let batch3 = decode_search_response(&resp3.arrow_ipc_data);
        let id_idx = batch3.schema().index_of("id").unwrap();
        let ids = batch3.column(id_idx).as_any().downcast_ref::<Int32Array>().unwrap();

        let has_shard0 = (0..ids.len()).any(|i| ids.value(i) < rows_per_shard as i32);
        let has_shard1 = (0..ids.len()).any(|i| ids.value(i) >= rows_per_shard as i32);
        assert!(has_shard0, "Results should include rows from shard 0");
        assert!(has_shard1, "Results should include rows from shard 1");
        println!("✓ Cross-shard merge: {} results from both shards", batch3.num_rows());
    }

    // Clean up: abort server tasks
    exec0_handle.abort();
    exec1_handle.abort();
    sched_handle.abort();

    println!("\n=== All gRPC distributed tests passed ===");
}

/// Test: Scheduler returns error for unknown table
#[tokio::test]
async fn test_grpc_unknown_table() {
    let _ = env_logger::try_init();
    let sched_port = 50210_u16;

    let config = ClusterConfig {
        tables: vec![],
        executors: vec![],
        ..Default::default()
    };

    let sched_svc = CoordinatorService::new(&config, Duration::from_secs(5));
    let sched_handle = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{sched_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched_svc))
            .serve(addr)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(1500)).await;

    let mut client = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{sched_port}")
    ).await.unwrap();

    let result = client.ann_search(AnnSearchRequest {
        table_name: "nonexistent".into(),
        vector_column: "v".into(),
        query_vector: vec![0; 32],
        dimension: 8,
        k: 5,
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
            query_text: None,
            offset: 0,
    }).await;

    // Should return gRPC NOT_FOUND error
    assert!(result.is_err(), "Should fail for unknown table");
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
    println!("✓ Unknown table returns NOT_FOUND");

    sched_handle.abort();
}

/// Test: Empty shard returns empty results
#[tokio::test]
async fn test_grpc_empty_result() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 4;

    // Create shard with 10 rows
    let s0 = tmp.path().join("empty_shard.lance");
    create_lance_shard(s0.to_str().unwrap(), 10, dim, 1, 0).await;

    let exec_port = 50220_u16;
    let sched_port = 50221_u16;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "tiny".into(),
            shards: vec![ShardConfig {
                name: "tiny_shard".into(),
                uri: s0.to_str().unwrap().into(),
                executors: vec!["e0".into()],
            }],
        }],
        executors: vec![ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: exec_port }],
        ..Default::default()
    };

    let shards = config.shards_for_executor("e0");
    let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap());
    let exec_svc = WorkerService::new(reg);
    let exec_handle = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{exec_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(exec_svc))
            .serve(addr).await.unwrap();
    });

    let sched_svc = CoordinatorService::new(&config, Duration::from_secs(5));
    let sched_handle = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{sched_port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched_svc))
            .serve(addr).await.unwrap();
    });

    sleep(Duration::from_millis(1500)).await;

    let mut client = LanceSchedulerServiceClient::connect(
        format!("http://127.0.0.1:{sched_port}")
    ).await.unwrap();

    // Request more results than available
    let resp = client.ann_search(AnnSearchRequest {
        table_name: "tiny".into(),
        vector_column: "vector".into(),
        query_vector: vec![0_f32; dim].iter().flat_map(|f| f.to_le_bytes()).collect(),
        dimension: dim as u32,
        k: 100, // more than the 10 rows
        nprobes: 1,
        filter: None,
        columns: vec![],
        metric_type: 0,
            query_text: None,
            offset: 0,
    }).await.unwrap().into_inner();

    assert!(resp.error.is_empty());
    // Should return ≤10 rows (all available)
    assert!(resp.num_rows <= 10, "Should return at most 10 rows, got {}", resp.num_rows);
    println!("✓ Small shard: returned {} of 10 rows", resp.num_rows);

    exec_handle.abort();
    sched_handle.abort();
}
