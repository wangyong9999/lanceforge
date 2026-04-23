// Licensed under the Apache License, Version 2.0.
//
// Phase 3 — single-shard tables route writes to an HRW-chosen primary
// worker, which lazy-opens the URI (phase-1 write path) and commits
// through Lance OCC. Avoids multi-writer manifest contention without
// requiring explicit leader election.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;
use tokio::time::sleep;

use lanceforge::config::{ClusterConfig, ExecutorConfig, ShardConfig, TableConfig};
use lanceforge::coordinator::CoordinatorService;
use lanceforge::executor::LanceTableRegistry;
use lanceforge::generated::lance_distributed::{
    lance_executor_service_server::LanceExecutorServiceServer,
    lance_scheduler_service_client::LanceSchedulerServiceClient,
    lance_scheduler_service_server::LanceSchedulerServiceServer,
    AddRowsRequest,
};
use lanceforge::worker::WorkerService;

const DIM: usize = 16;
const INITIAL_ROWS: usize = 100;
const ADDED_ROWS: usize = 50;

fn rand_vecs(seed: u64, count: usize, dim: usize) -> Vec<f32> {
    let mut s = seed;
    let mut v = Vec::with_capacity(count * dim);
    for _ in 0..(count * dim) {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn make_batch(id_start: i32, count: usize, seed: u64) -> RecordBatch {
    let sch = schema();
    let flat = rand_vecs(seed, count, DIM);
    RecordBatch::try_new(
        sch,
        vec![
            Arc::new(Int32Array::from(
                (id_start..id_start + count as i32).collect::<Vec<_>>(),
            )),
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
    .unwrap()
}

async fn create_seed_dataset(uri: &str) {
    let batch = make_batch(0, INITIAL_ROWS, 11);
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema());
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, uri, Some(p)).await.unwrap();
}

fn batch_to_ipc(batch: &RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn phase3_single_shard_add_rows_via_primary() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().join("t.lance");
    let uri_s = uri.to_str().unwrap().to_string();
    create_seed_dataset(&uri_s).await;

    let ep0 = 57020u16;
    let ep1 = 57021u16;
    let sp = 57022u16;

    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "t".into(),
            shards: vec![ShardConfig {
                name: "t_shard_00".into(),
                uri: uri_s.clone(),
                executors: vec!["w0".into(), "w1".into()],
            }],
        }],
        executors: vec![
            ExecutorConfig { id: "w0".into(), host: "127.0.0.1".into(), port: ep0, role: Default::default() },
            ExecutorConfig { id: "w1".into(), host: "127.0.0.1".into(), port: ep1, role: Default::default() },
        ],
        ..Default::default()
    };

    // Start workers fully empty (stateless).
    for p in [ep0, ep1] {
        let reg = Arc::new(
            LanceTableRegistry::new(SessionContext::default(), &[])
                .await
                .unwrap(),
        );
        let svc = WorkerService::new(reg);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(LanceExecutorServiceServer::new(svc))
                .serve(format!("0.0.0.0:{p}").parse().unwrap())
                .await
                .unwrap();
        });
    }
    sleep(Duration::from_millis(500)).await;

    let sched = CoordinatorService::new(&config, Duration::from_secs(30));
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched))
            .serve(format!("0.0.0.0:{sp}").parse().unwrap())
            .await
            .unwrap();
    });
    sleep(Duration::from_millis(2000)).await;

    let mut client = LanceSchedulerServiceClient::connect(format!("http://127.0.0.1:{sp}"))
        .await
        .unwrap();

    // Issue AddRows.
    let new_batch = make_batch(INITIAL_ROWS as i32, ADDED_ROWS, 99);
    let ipc = batch_to_ipc(&new_batch);
    let resp = client
        .add_rows(AddRowsRequest {
            table_name: "t".into(),
            arrow_ipc_data: ipc,
            on_columns: vec![],
            request_id: "phase3-1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.error.is_empty(), "AddRows error: {}", resp.error);
    assert_eq!(resp.affected_rows as usize, ADDED_ROWS);

    // Verify durability by opening a fresh Dataset handle (bypasses
    // coord's DatasetCache and worker's lancedb::Table cache).
    let ds = lance::dataset::Dataset::open(&uri_s).await.unwrap();
    let total = ds.count_rows(None).await.unwrap();
    assert_eq!(
        total,
        INITIAL_ROWS + ADDED_ROWS,
        "on-disk row count should reflect the committed AddRows"
    );

    // Second AddRows should succeed too (proves the primary-writer path
    // is stable under repeat use; a flaky OCC loop would surface here).
    let batch2 = make_batch((INITIAL_ROWS + ADDED_ROWS) as i32, ADDED_ROWS, 101);
    let resp2 = client
        .add_rows(AddRowsRequest {
            table_name: "t".into(),
            arrow_ipc_data: batch_to_ipc(&batch2),
            on_columns: vec![],
            request_id: "phase3-2".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp2.error.is_empty(), "second AddRows error: {}", resp2.error);

    let ds2 = lance::dataset::Dataset::open(&uri_s).await.unwrap();
    let total2 = ds2.count_rows(None).await.unwrap();
    assert_eq!(total2, INITIAL_ROWS + 2 * ADDED_ROWS);

    println!("phase3: 2 writes × {ADDED_ROWS} rows → {total2} rows on disk");
}
