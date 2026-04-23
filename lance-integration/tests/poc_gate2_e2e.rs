// Licensed under the Apache License, Version 2.0.
//
// PoC Gate 2 end-to-end — single Lance dataset, two worker processes,
// coord-side HRW routes fragment subsets to workers, merged top-K
// matches single-worker baseline.
//
// Scope (deliberately narrow):
//   * Correctness only (benchmark is a separate step).
//   * Uses WorkerService gRPC directly; bypasses CoordinatorService
//     (coord still routes by shard; Gate 3 wires coord dispatch).
//   * 100k × 128d, 4 fragments, 2 workers. Large enough that IVF
//     matters, small enough to finish in seconds.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use tempfile::TempDir;
use tokio::time::sleep;

use lanceforge::config::ShardConfig;
use lanceforge::executor::LanceTableRegistry;
use lanceforge::generated::lance_distributed::{
    lance_executor_service_client::LanceExecutorServiceClient,
    lance_executor_service_server::LanceExecutorServiceServer,
    LocalSearchRequest,
};
use lanceforge::worker::WorkerService;
use lance_distributed_coordinator::fragment_router::assign_fragments;

const DIM: usize = 64;
const FRAGMENTS: usize = 4;
const ROWS_PER_FRAG: usize = 2_500; // 10k total; matches Gate 1 scale
const K: usize = 10;
const OVERSAMPLE: usize = 10;

fn rand_vecs(seed: u64, count: usize, dim: usize) -> Vec<f32> {
    let mut s = seed;
    let mut v = Vec::with_capacity(count * dim);
    for _ in 0..(count * dim) {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        v.push(((s >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

async fn create_dataset(uri: &str) {
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
        let flat = rand_vecs(42u64.wrapping_add(i as u64 * 1_000_003), ROWS_PER_FRAG, DIM);
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

async fn build_ivf_pq(parent: &str, table_name: &str) {
    use lancedb::index::Index;
    use lancedb::index::vector::IvfPqIndexBuilder;
    let db = lancedb::connect(parent).execute().await.unwrap();
    let table = db.open_table(table_name).execute().await.unwrap();
    table
        .create_index(
            &["vector"],
            Index::IvfPq(IvfPqIndexBuilder::default().num_partitions(4).num_sub_vectors(8)),
        )
        .execute()
        .await
        .unwrap();
}

/// Collect id+distance from an Arrow IPC response into a Vec<(id, dist)>.
fn decode_response(ipc: &[u8]) -> Vec<(i32, f32)> {
    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(ipc, None).unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let dists = batch
            .column_by_name("_distance")
            .expect("_distance column must be present")
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for i in 0..ids.len() {
            out.push((ids.value(i), dists.value(i)));
        }
    }
    out
}

async fn start_worker(
    port: u16,
    shards: Vec<ShardConfig>,
) -> tokio::task::JoinHandle<()> {
    let reg = Arc::new(
        LanceTableRegistry::new(SessionContext::default(), &shards)
            .await
            .unwrap(),
    );
    let svc = WorkerService::new(reg);
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{port}").parse().unwrap();
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(svc))
            .serve(addr)
            .await
            .unwrap();
    })
}

#[tokio::test]
async fn gate2_single_dataset_fragment_fanout_e2e() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().join("pocdata.lance");
    let uri_s = uri.to_str().unwrap().to_string();
    let parent_s = tmp.path().to_str().unwrap().to_string();

    println!("building {ROWS_PER_FRAG}×{FRAGMENTS} dataset at {uri_s}…");
    create_dataset(&uri_s).await;
    build_ivf_pq(&parent_s, "pocdata").await;

    // Discover fragment count from the dataset metadata.
    let ds = Arc::new(lance::dataset::Dataset::open(&uri_s).await.unwrap());
    let frag_ids: Vec<u32> = ds.get_fragments().iter().map(|f| f.id() as u32).collect();
    assert_eq!(frag_ids.len(), FRAGMENTS, "unexpected fragment count");
    println!("dataset fragments: {frag_ids:?}");

    // Baseline: single-process top-K over the whole dataset via Lance.
    let probe = rand_vecs(42u64.wrapping_add(2 * 1_000_003), ROWS_PER_FRAG, DIM);
    let q = Float32Array::from(probe[0..DIM].to_vec());
    let mut scanner = ds.scan();
    scanner.nearest("vector", &q, K).unwrap();
    scanner.nprobes(4);
    let baseline_stream = scanner.try_into_stream().await.unwrap();
    let baseline_batches: Vec<RecordBatch> =
        baseline_stream.try_collect().await.unwrap();
    let baseline_ids: Vec<i32> = baseline_batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(baseline_ids.len(), K);
    println!("baseline top-{K}: {baseline_ids:?}");

    // Start two workers, both open the same Lance URI under the same
    // table name. This is the *fungible worker* model — neither owns
    // data, both can serve any fragment.
    let shard = ShardConfig {
        name: "pocdata".into(),
        uri: uri_s.clone(),
        executors: vec!["w0".into(), "w1".into()],
    };
    let p0 = 53010u16;
    let p1 = 53011u16;
    let _h0 = start_worker(p0, vec![shard.clone()]).await;
    let _h1 = start_worker(p1, vec![shard.clone()]).await;
    sleep(Duration::from_millis(1500)).await;

    let mut c0 = LanceExecutorServiceClient::connect(format!("http://127.0.0.1:{p0}"))
        .await
        .unwrap();
    let mut c1 = LanceExecutorServiceClient::connect(format!("http://127.0.0.1:{p1}"))
        .await
        .unwrap();

    // Coord-side HRW assignment.
    let workers = vec!["w0".to_string(), "w1".to_string()];
    let assignment = assign_fragments(&frag_ids, &workers);
    println!("HRW assignment: {assignment:?}");
    assert!(
        assignment.values().all(|v| !v.is_empty()),
        "both workers should receive at least one fragment"
    );

    let q_bytes: Vec<u8> = probe[0..DIM].iter().flat_map(|f| f.to_le_bytes()).collect();

    let build_req = |frag_ids: Vec<u32>| LocalSearchRequest {
        query_type: 0, // ANN
        table_name: "pocdata".into(),
        vector_column: Some("vector".into()),
        query_vector: Some(q_bytes.clone()),
        dimension: Some(DIM as u32),
        nprobes: Some(4),
        metric_type: Some(0),
        text_column: None,
        query_text: None,
        k: (K * OVERSAMPLE) as u32,
        filter: None,
        columns: vec!["id".into()],
        fragment_ids: frag_ids,
    };

    let w0_req = build_req(assignment.get("w0").cloned().unwrap_or_default());
    let w1_req = build_req(assignment.get("w1").cloned().unwrap_or_default());

    let t0 = std::time::Instant::now();
    let (r0, r1) = tokio::join!(
        c0.execute_local_search(w0_req),
        c1.execute_local_search(w1_req)
    );
    let rpc_elapsed = t0.elapsed();
    let r0 = r0.unwrap().into_inner();
    let r1 = r1.unwrap().into_inner();
    println!(
        "RPC: w0 {}b {}rows, w1 {}b {}rows, total {:?}",
        r0.arrow_ipc_data.len(),
        r0.num_rows,
        r1.arrow_ipc_data.len(),
        r1.num_rows,
        rpc_elapsed
    );
    assert!(r0.error.is_empty(), "w0 error: {}", r0.error);
    assert!(r1.error.is_empty(), "w1 error: {}", r1.error);

    let mut cand0 = decode_response(&r0.arrow_ipc_data);
    let mut cand1 = decode_response(&r1.arrow_ipc_data);
    cand0.append(&mut cand1);
    // Global top-K by distance.
    cand0.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    cand0.truncate(K);
    let merged_ids: Vec<i32> = cand0.iter().map(|(id, _)| *id).collect();
    println!("merged top-{K}: {merged_ids:?}");

    let baseline_set: std::collections::HashSet<i32> = baseline_ids.iter().copied().collect();
    let hits = merged_ids
        .iter()
        .filter(|id| baseline_set.contains(id))
        .count();
    let recall = hits as f32 / K as f32;
    println!("recall = {hits}/{K} = {recall:.3}");

    assert!(
        recall >= 0.9,
        "Gate 2 recall {recall:.3} < 0.9 — fan-out correctness regressed"
    );

    // 5ms / shard target per-RPC — loose cap, not a benchmark.
    // At 100k rows × 2 shards this should be well under 500ms.
    assert!(
        rpc_elapsed < Duration::from_secs(5),
        "RPC took {rpc_elapsed:?} — suspiciously slow"
    );
    println!("\n=== Gate 2 E2E PASSED ===");
}
