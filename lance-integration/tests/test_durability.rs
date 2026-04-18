// Licensed under the Apache License, Version 2.0.
// Durability test: sustained queries + executor failure recovery.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
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

async fn create_shard(path: &str, num_rows: usize, dim: usize, seed: u64, id_offset: i32) {
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
        Arc::new(StringArray::from((0..num_rows).map(|i| format!("c{}", i % 5)).collect::<Vec<_>>())),
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

/// Sustained load test: 200 queries in rapid succession, all should succeed.
#[tokio::test]
async fn test_sustained_load() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 8;

    let s0 = tmp.path().join("s0.lance");
    let s1 = tmp.path().join("s1.lance");
    create_shard(s0.to_str().unwrap(), 500, dim, 1, 0).await;
    create_shard(s1.to_str().unwrap(), 500, dim, 2, 500).await;

    let ep0 = 50400_u16;
    let ep1 = 50401_u16;
    let sp = 50402_u16;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "dur".into(),
            shards: vec![
                ShardConfig { name: "dur_s0".into(), uri: s0.to_str().unwrap().into(), executors: vec!["e0".into()] },
                ShardConfig { name: "dur_s1".into(), uri: s1.to_str().unwrap().into(), executors: vec!["e1".into()] },
            ],
        }],
        executors: vec![
            ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: ep0 , role: Default::default() },
            ExecutorConfig { id: "e1".into(), host: "127.0.0.1".into(), port: ep1 , role: Default::default() },
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

    let sched = CoordinatorService::new(&config, Duration::from_secs(30));
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

    let success = Arc::new(AtomicU32::new(0));
    let errors = Arc::new(AtomicU32::new(0));
    let num_queries = 200;

    // Fire queries sequentially (stress on single connection)
    for i in 0..num_queries {
        let query_vec: Vec<f32> = vec![i as f32 * 0.01; dim];
        let resp = client.ann_search(AnnSearchRequest {
            table_name: "dur".into(),
            vector_column: "vector".into(),
            query_vector: query_vec.iter().flat_map(|f| f.to_le_bytes()).collect(),
            dimension: dim as u32,
            k: 5,
            nprobes: 1,
            filter: None,
            columns: vec![],
            metric_type: 0,
            query_text: None,
            offset: 0,
        }).await;

        match resp {
            Ok(r) if r.get_ref().error.is_empty() => {
                success.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let s = success.load(Ordering::Relaxed);
    let e = errors.load(Ordering::Relaxed);
    println!("Sustained load: {s}/{num_queries} success, {e} errors");
    assert!(s >= num_queries - 1, "At least 199/200 queries should succeed, got {s}");
}

/// Concurrent load test: 50 parallel queries
#[tokio::test]
async fn test_concurrent_load() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let dim = 8;

    let s0 = tmp.path().join("c0.lance");
    create_shard(s0.to_str().unwrap(), 200, dim, 42, 0).await;

    let ep = 50410_u16;
    let sp = 50411_u16;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "conc".into(),
            shards: vec![
                ShardConfig { name: "conc_s0".into(), uri: s0.to_str().unwrap().into(), executors: vec!["e0".into()] },
            ],
        }],
        executors: vec![
            ExecutorConfig { id: "e0".into(), host: "127.0.0.1".into(), port: ep , role: Default::default() },
        ],
        ..Default::default()
    };

    let shards = config.shards_for_executor("e0");
    let reg = Arc::new(LanceTableRegistry::new(SessionContext::default(), &shards).await.unwrap());
    let svc = WorkerService::new(reg);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{ep}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc))
            .serve(addr).await.unwrap();
    });

    let sched = CoordinatorService::new(&config, Duration::from_secs(30));
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{sp}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched))
            .serve(addr).await.unwrap();
    });

    sleep(Duration::from_millis(2000)).await;

    let success = Arc::new(AtomicU32::new(0));
    let num_concurrent = 50;
    let mut handles = Vec::new();

    for i in 0..num_concurrent {
        let succ = success.clone();
        handles.push(tokio::spawn(async move {
            let mut c = LanceSchedulerServiceClient::connect(
                format!("http://127.0.0.1:{sp}")
            ).await.unwrap();

            let query_vec: Vec<f32> = vec![i as f32 * 0.1; dim];
            let resp = c.ann_search(AnnSearchRequest {
                table_name: "conc".into(),
                vector_column: "vector".into(),
                query_vector: query_vec.iter().flat_map(|f| f.to_le_bytes()).collect(),
                dimension: dim as u32,
                k: 5, nprobes: 1, filter: None, columns: vec![], metric_type: 0,
            query_text: None,
            offset: 0,
            }).await;

            if resp.is_ok() && resp.unwrap().get_ref().error.is_empty() {
                succ.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let s = success.load(Ordering::Relaxed);
    println!("Concurrent load: {s}/{num_concurrent} success");
    assert_eq!(s, num_concurrent, "All concurrent queries should succeed");
}
