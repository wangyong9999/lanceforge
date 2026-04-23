// Licensed under the Apache License, Version 2.0.
//
// Phase 2 — coord routes single-shard tables via fragment fanout.
//
// Setup: one Lance dataset with four fragments at a single URI, one
// ShardConfig entry, two workers started with EMPTY shard lists.
// When ann_search arrives, coord detects the single-shard layout,
// enumerates fragments, HRW-routes subsets to the workers, and each
// worker lazy-opens the URI via the phase-1 shard_uri protocol.
//
// Gate: coord returns top-K rows, every top-K id is present in the
// dataset, no errors surfaced.

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
    AnnSearchRequest,
};
use lanceforge::worker::WorkerService;

const DIM: usize = 32;
const FRAGMENTS: usize = 4;
const ROWS_PER_FRAG: usize = 500; // 2k total

fn rand_vecs(seed: u64, count: usize, dim: usize) -> Vec<f32> {
    let mut s = seed;
    let mut v = Vec::with_capacity(count * dim);
    for _ in 0..(count * dim) {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

async fn make_dataset(uri: &str) {
    for i in 0..FRAGMENTS {
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
        let flat = rand_vecs(7u64.wrapping_add(i as u64 * 17), ROWS_PER_FRAG, DIM);
        let id_start = (i * ROWS_PER_FRAG) as i32;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(
                    (id_start..id_start + ROWS_PER_FRAG as i32).collect::<Vec<_>>(),
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
        .unwrap();
        let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
        let params = lance::dataset::WriteParams {
            mode: if i == 0 {
                lance::dataset::WriteMode::Create
            } else {
                lance::dataset::WriteMode::Append
            },
            max_rows_per_file: ROWS_PER_FRAG,
            max_rows_per_group: ROWS_PER_FRAG,
            ..Default::default()
        };
        lance::dataset::Dataset::write(reader, uri, Some(params))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn phase2_single_dataset_ann_search_through_coord() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().join("single.lance");
    let uri_s = uri.to_str().unwrap().to_string();
    make_dataset(&uri_s).await;

    let ep0 = 56010u16;
    let ep1 = 56011u16;
    let sp = 56012u16;

    // Single ShardConfig entry → coord takes fragment-fanout path.
    // Executors list is informational here: workers will lazy-open via
    // shard_uri from phase 1 regardless of this list.
    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "single".into(),
            shards: vec![ShardConfig {
                name: "single_shard_00".into(),
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

    // Workers start with zero preloaded shards — fully stateless.
    for (p, label) in [(ep0, "w0"), (ep1, "w1")] {
        let _ = label;
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

    // Query with a vector from fragment 2 so the top hit should be a
    // real row id in [1000, 1500).
    let probe = rand_vecs(7u64.wrapping_add(2 * 17), ROWS_PER_FRAG, DIM);
    let q_bytes: Vec<u8> = probe[0..DIM].iter().flat_map(|f| f.to_le_bytes()).collect();

    let resp = client
        .ann_search(AnnSearchRequest {
            table_name: "single".into(),
            vector_column: "vector".into(),
            query_vector: q_bytes,
            dimension: DIM as u32,
            k: 10,
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

    assert!(resp.error.is_empty(), "error: {}", resp.error);
    assert_eq!(resp.num_rows, 10, "expected top-10, got {}", resp.num_rows);

    // Decode ids and assert: all within the populated range, top hit
    // is id=1000 (the probe vector's source row).
    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(&resp.arrow_ipc_data[..], None).unwrap();
    let mut ids = Vec::new();
    for b in reader {
        let b = b.unwrap();
        let arr = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..arr.len() {
            ids.push(arr.value(i));
        }
    }
    let max_id = (FRAGMENTS * ROWS_PER_FRAG) as i32;
    for id in &ids {
        assert!(*id >= 0 && *id < max_id, "id {id} out of range");
    }
    assert_eq!(ids[0], 1000, "top hit should be the probe's source row (fragment 2 row 0), got {}", ids[0]);
    println!("phase2 top-10 via coord fragment fanout: {ids:?}");
}
