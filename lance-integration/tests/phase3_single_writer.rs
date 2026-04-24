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
    AddRowsRequest, AnnSearchRequest,
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
    let pool = sched.pool_shutdown_handle();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched))
            .serve(format!("0.0.0.0:{sp}").parse().unwrap())
            .await
            .unwrap();
    });
    assert!(
        pool.wait_until_healthy(2, Duration::from_secs(5)).await,
        "both workers should become healthy within 5s"
    );

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

/// Regression guard: cache staleness bug. If the coord's DatasetCache
/// holds a stale handle after a write, the next ann_search would
/// enumerate the OLD fragment list and miss the newly committed rows.
/// With the invalidation hook wired into the write path, this test
/// should surface the added rows via the fragment-fanout path.
#[tokio::test]
async fn phase3_read_after_write_sees_new_rows() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().join("raw.lance");
    let uri_s = uri.to_str().unwrap().to_string();
    create_seed_dataset(&uri_s).await;

    let ep0 = 57030u16;
    let ep1 = 57031u16;
    let sp = 57032u16;
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
    for p in [ep0, ep1] {
        let reg = Arc::new(
            LanceTableRegistry::new(SessionContext::default(), &[])
                .await
                .unwrap(),
        );
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(LanceExecutorServiceServer::new(WorkerService::new(reg)))
                .serve(format!("0.0.0.0:{p}").parse().unwrap())
                .await
                .unwrap();
        });
    }
    sleep(Duration::from_millis(500)).await;
    let sched = CoordinatorService::new(&config, Duration::from_secs(30));
    let pool = sched.pool_shutdown_handle();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LanceSchedulerServiceServer::new(sched))
            .serve(format!("0.0.0.0:{sp}").parse().unwrap())
            .await
            .unwrap();
    });
    assert!(
        pool.wait_until_healthy(2, Duration::from_secs(5)).await,
        "both workers should become healthy within 5s"
    );
    let mut client = LanceSchedulerServiceClient::connect(format!("http://127.0.0.1:{sp}"))
        .await
        .unwrap();

    // Prime the coord's DatasetCache by doing one read first.
    let q0_bytes: Vec<u8> = vec![0f32; DIM].into_iter().flat_map(|f| f.to_le_bytes()).collect();
    let _warm = client
        .ann_search(AnnSearchRequest {
            table_name: "t".into(),
            vector_column: "vector".into(),
            query_vector: q0_bytes.clone(),
            dimension: DIM as u32,
            k: 5,
            nprobes: 1,
            filter: None,
            columns: vec!["id".into()],
            metric_type: 0,
            query_text: None,
            offset: 0,
            min_schema_version: 0,
            min_commit_seq: 0,
        })
        .await
        .unwrap();

    // Write NEW rows that don't exist yet. If cache isn't invalidated
    // these ids will be invisible to the next search.
    let added = make_batch(INITIAL_ROWS as i32, ADDED_ROWS, 99);
    let _ = client
        .add_rows(AddRowsRequest {
            table_name: "t".into(),
            arrow_ipc_data: batch_to_ipc(&added),
            on_columns: vec![],
            request_id: "raw".into(),
        })
        .await
        .unwrap()
        .into_inner();

    // Query targeting a vector from the new batch — top hit should be
    // id = INITIAL_ROWS (first added row, seed=99 row 0).
    let probe = rand_vecs(99, ADDED_ROWS, DIM);
    let q_bytes: Vec<u8> = probe[0..DIM].iter().flat_map(|f| f.to_le_bytes()).collect();
    let resp = client
        .ann_search(AnnSearchRequest {
            table_name: "t".into(),
            vector_column: "vector".into(),
            query_vector: q_bytes,
            dimension: DIM as u32,
            k: 3,
            nprobes: 1,
            filter: None,
            columns: vec!["id".into()],
            metric_type: 0,
            query_text: None,
            offset: 0,
            min_schema_version: 0,
            min_commit_seq: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.error.is_empty(), "search error: {}", resp.error);

    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(&resp.arrow_ipc_data[..], None).unwrap();
    let mut got = Vec::new();
    for b in reader {
        let b = b.unwrap();
        let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..ids.len() {
            got.push(ids.value(i));
        }
    }
    assert!(
        got.iter().any(|id| *id >= INITIAL_ROWS as i32),
        "read-after-write should return at least one newly added id; got {got:?}"
    );
    println!("phase3: read-after-write saw ids {got:?}");
}
