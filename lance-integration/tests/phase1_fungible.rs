// Licensed under the Apache License, Version 2.0.
//
// Phase 1 — worker-side fungibility. A worker starts with NO shards
// registered, receives a request carrying `shard_uri`, lazy-opens the
// table, and serves the query. No StatefulSet-style binding required.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;
use tokio::time::sleep;

use lanceforge::executor::LanceTableRegistry;
use lanceforge::generated::lance_distributed::{
    lance_executor_service_client::LanceExecutorServiceClient,
    lance_executor_service_server::LanceExecutorServiceServer,
    LocalSearchRequest,
};
use lanceforge::worker::WorkerService;

const DIM: usize = 32;
const ROWS: usize = 500;

fn rand_vecs(seed: u64, count: usize, dim: usize) -> Vec<f32> {
    let mut s = seed;
    let mut v = Vec::with_capacity(count * dim);
    for _ in 0..(count * dim) {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

async fn write_dataset(uri: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]));
    let flat = rand_vecs(7, ROWS, DIM);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from((0..ROWS as i32).collect::<Vec<_>>())),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    DIM as i32,
                    Arc::new(Float32Array::from(flat)),
                    None,
                )
                .unwrap(),
            ),
        ],
    )
    .unwrap();
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, uri, Some(p)).await.unwrap();
}

#[tokio::test]
async fn phase1_worker_lazy_opens_from_shard_uri() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().join("mydata.lance");
    let uri_s = uri.to_str().unwrap().to_string();
    write_dataset(&uri_s).await;

    // Worker starts with ZERO shards — fully empty.
    let reg = Arc::new(
        LanceTableRegistry::new(SessionContext::default(), &[])
            .await
            .unwrap(),
    );
    assert!(
        reg.open_table_names().await.is_empty(),
        "worker should start empty"
    );

    let svc = WorkerService::new(reg.clone());
    let port = 55999u16;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc))
            .serve(format!("0.0.0.0:{port}").parse().unwrap())
            .await
            .unwrap();
    });
    sleep(Duration::from_millis(400)).await;

    let mut client = LanceExecutorServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap();

    // Build a probe vector from the dataset so we know top-K exists.
    let probe = rand_vecs(7, ROWS, DIM);
    let q_bytes: Vec<u8> = probe[0..DIM].iter().flat_map(|f| f.to_le_bytes()).collect();

    // Send a request WITHOUT preloading the shard — coord-emulated.
    let resp = client
        .execute_local_search(LocalSearchRequest {
            query_type: 0,
            table_name: "mydata".into(),
            vector_column: Some("vector".into()),
            query_vector: Some(q_bytes),
            dimension: Some(DIM as u32),
            nprobes: Some(8),
            metric_type: Some(0),
            text_column: None,
            query_text: None,
            k: 10,
            filter: None,
            columns: vec!["id".into()],
            fragment_ids: vec![],
            shard_uri: Some(uri_s.clone()),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(resp.error.is_empty(), "expected success, got error: {}", resp.error);
    assert!(resp.num_rows > 0, "should return rows after lazy open");

    // Worker should now have "mydata" registered.
    let names = reg.open_table_names().await;
    assert!(
        names.iter().any(|n| n == "mydata"),
        "worker should have lazy-opened 'mydata' (got {names:?})"
    );
}

/// Verify behaviour when no shard_uri is provided AND the table isn't
/// pre-loaded — should fail with the normal "not found" error, not a
/// panic or hang.
#[tokio::test]
async fn phase1_no_uri_no_preload_yields_clean_error() {
    let _ = env_logger::try_init();
    let reg = Arc::new(
        LanceTableRegistry::new(SessionContext::default(), &[])
            .await
            .unwrap(),
    );
    let svc = WorkerService::new(reg);
    let port = 55998u16;
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc))
            .serve(format!("0.0.0.0:{port}").parse().unwrap())
            .await
            .unwrap();
    });
    sleep(Duration::from_millis(400)).await;
    let mut client = LanceExecutorServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap();

    let resp = client
        .execute_local_search(LocalSearchRequest {
            query_type: 0,
            table_name: "nobody".into(),
            vector_column: Some("vector".into()),
            query_vector: Some(vec![0u8; DIM * 4]),
            dimension: Some(DIM as u32),
            nprobes: Some(8),
            metric_type: Some(0),
            text_column: None,
            query_text: None,
            k: 10,
            filter: None,
            columns: vec![],
            fragment_ids: vec![],
            shard_uri: None,
        })
        .await
        .unwrap()
        .into_inner();

    assert!(
        !resp.error.is_empty(),
        "empty shard_uri + no preload should surface a clear error"
    );
    assert_eq!(resp.num_rows, 0);
}
