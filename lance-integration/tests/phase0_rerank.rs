// Licensed under the Apache License, Version 2.0.
//
// Phase 0 — verify alpha.3 scatter-gather merges global top-K by
// EXACT distance, not PQ-approximated distance. Without refine_factor
// on the worker, each shard's IVF_PQ returns distances measured in
// its own (different) codebook, and the coord's global merge by
// distance is lossy by construction. PoC Gate 2 bench showed this as
// recall ~0.08 on 1M × 128d.
//
// Goal: recall vs brute-force ≥ 0.9 on a 500k × 128d × 2-shard setup.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use futures::TryStreamExt;
use tempfile::TempDir;
use tokio::time::sleep;

use lanceforge::config::{ClusterConfig, ExecutorConfig, ShardConfig, TableConfig};
use lanceforge::coordinator::CoordinatorService;
use lanceforge::generated::lance_distributed::{
    lance_executor_service_server::LanceExecutorServiceServer,
    lance_scheduler_service_client::LanceSchedulerServiceClient,
    lance_scheduler_service_server::LanceSchedulerServiceServer,
    AnnSearchRequest,
};
use lanceforge::worker::WorkerService;
use lanceforge::executor::LanceTableRegistry;
use datafusion::prelude::SessionContext;

const DIM: usize = 128;
const SHARD_ROWS: usize = 250_000; // 500k total × 2 shards
const K: usize = 10;

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

async fn write_shard(uri: &str, id_offset: i32, seed: u64) {
    let flat = rand_vecs(seed, SHARD_ROWS, DIM);
    let sch = schema();
    let batch = RecordBatch::try_new(
        sch.clone(),
        vec![
            Arc::new(Int32Array::from(
                (id_offset..id_offset + SHARD_ROWS as i32).collect::<Vec<_>>(),
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
    let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], sch);
    let p = lance::dataset::WriteParams {
        mode: lance::dataset::WriteMode::Create,
        ..Default::default()
    };
    lance::dataset::Dataset::write(reader, uri, Some(p)).await.unwrap();
}

async fn build_index(parent: &str, name: &str) {
    use lancedb::index::Index;
    use lancedb::index::vector::IvfPqIndexBuilder;
    let db = lancedb::connect(parent).execute().await.unwrap();
    let table = db.open_table(name).execute().await.unwrap();
    table
        .create_index(
            &["vector"],
            Index::IvfPq(
                IvfPqIndexBuilder::default().num_partitions(32).num_sub_vectors(16),
            ),
        )
        .execute()
        .await
        .unwrap();
}

/// Load every row (id + raw vector) from both shards into memory.
/// Used as the ground-truth vector pool for brute-force top-K.
///
/// Earlier version computed ground truth via `scan().nearest().nprobes(all)`
/// on each shard, but that still returns PQ-approximated distances and
/// merges across independent codebooks — the exact bug we're testing.
/// True brute force means decode original vectors and compute L2 in Rust.
async fn load_all_vectors(shard_uris: &[&str]) -> Vec<(i32, Vec<f32>)> {
    use arrow::array::ListArray;
    let mut out = Vec::new();
    for uri in shard_uris {
        let ds = Arc::new(lance::dataset::Dataset::open(uri).await.unwrap());
        let scanner = ds.scan();
        let stream = scanner.try_into_stream().await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        for b in batches {
            let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let vec_col = b.column_by_name("vector").unwrap();
            if let Some(fsl) = vec_col.as_any().downcast_ref::<FixedSizeListArray>() {
                let values = fsl.values().as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..ids.len() {
                    let off = i * DIM;
                    let v: Vec<f32> = (0..DIM).map(|j| values.value(off + j)).collect();
                    out.push((ids.value(i), v));
                }
            } else if let Some(list) = vec_col.as_any().downcast_ref::<ListArray>() {
                let _ = list;
                unreachable!("vector schema is FixedSizeList")
            }
        }
    }
    out
}

fn brute_force_top_k(pool: &[(i32, Vec<f32>)], query: &[f32]) -> std::collections::HashSet<i32> {
    let mut scored: Vec<(i32, f32)> = pool
        .iter()
        .map(|(id, v)| {
            let d: f32 = v.iter().zip(query.iter()).map(|(a, b)| (a - b) * (a - b)).sum();
            (*id, d)
        })
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    scored.truncate(K);
    scored.into_iter().map(|(id, _)| id).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // release-mode; run explicitly
async fn phase0_alpha3_recall_with_rerank() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let parent = tmp.path().to_str().unwrap();

    let s0 = format!("{parent}/t_shard_00.lance");
    let s1 = format!("{parent}/t_shard_01.lance");

    // Deterministic data; seeds differ per shard (realistic alpha.3).
    println!("writing shards…");
    write_shard(&s0, 0, 42).await;
    write_shard(&s1, SHARD_ROWS as i32, 42u64.wrapping_add(1_000_003)).await;
    println!("building per-shard IVF_PQ…");
    build_index(parent, "t_shard_00").await;
    build_index(parent, "t_shard_01").await;

    let ep0 = 55301u16;
    let ep1 = 55302u16;
    let sp = 55303u16;

    let mut server = lance_distributed_common::config::ServerConfig::default();
    // Bigger oversample so coord's request to worker has enough headroom
    // for PQ→refine to reach the true top-K, even in the worst-case
    // high-dim random distribution of the test.
    server.oversample_factor = 20;
    let config = ClusterConfig {
        server,
        tables: vec![TableConfig {
            name: "t".into(),
            shards: vec![
                ShardConfig {
                    name: "t_shard_00".into(),
                    uri: s0.clone(),
                    executors: vec!["w0".into()],
                },
                ShardConfig {
                    name: "t_shard_01".into(),
                    uri: s1.clone(),
                    executors: vec!["w1".into()],
                },
            ],
        }],
        executors: vec![
            ExecutorConfig { id: "w0".into(), host: "127.0.0.1".into(), port: ep0, role: Default::default() },
            ExecutorConfig { id: "w1".into(), host: "127.0.0.1".into(), port: ep1, role: Default::default() },
        ],
        ..Default::default()
    };

    let w0_shards = config.shards_for_executor("w0");
    let reg0 = Arc::new(LanceTableRegistry::new(SessionContext::default(), &w0_shards).await.unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(WorkerService::new(reg0)))
            .serve(format!("0.0.0.0:{ep0}").parse().unwrap())
            .await
            .unwrap();
    });
    let w1_shards = config.shards_for_executor("w1");
    let reg1 = Arc::new(LanceTableRegistry::new(SessionContext::default(), &w1_shards).await.unwrap());
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LanceExecutorServiceServer::new(WorkerService::new(reg1)))
            .serve(format!("0.0.0.0:{ep1}").parse().unwrap())
            .await
            .unwrap();
    });

    // Let workers bind before coord's ConnectionPool::connect_all runs.
    // Otherwise connect_all races; workers stay unhealthy for 10s (default
    // health_check interval) and the test hits "All workers unhealthy".
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

    // Load ground-truth vector pool once.
    println!("loading ground-truth pool…");
    let vec_pool = load_all_vectors(&[&s0, &s1]).await;
    assert_eq!(vec_pool.len(), 2 * SHARD_ROWS);

    // 40 random queries; compare scatter-gather vs brute-force.
    let n_queries = 40;
    let q_pool = rand_vecs(0xDEADBEEF, n_queries, DIM);
    let mut recalls: Vec<f32> = Vec::new();
    for i in 0..n_queries {
        let q = &q_pool[i * DIM..(i + 1) * DIM];
        let truth = brute_force_top_k(&vec_pool, q);

        let qv_bytes: Vec<u8> = q.iter().flat_map(|f| f.to_le_bytes()).collect();
        let resp = client
            .ann_search(AnnSearchRequest {
                table_name: "t".into(),
                vector_column: "vector".into(),
                query_vector: qv_bytes,
                dimension: DIM as u32,
                k: K as u32,
                // Full partition coverage: isolates merge correctness
                // from probe coverage. nprobes < all would mix "can't
                // reach this row" with "merge ranked this row wrong",
                // which is not what Phase 0 is testing.
                nprobes: 32,
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
        assert!(resp.error.is_empty(), "query {i}: {}", resp.error);

        use arrow::ipc::reader::StreamReader;
        let reader = StreamReader::try_new(&resp.arrow_ipc_data[..], None).unwrap();
        let mut got: Vec<(i32, f32)> = Vec::new();
        for b in reader {
            let b = b.unwrap();
            let ids = b.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
            let ds = b.column_by_name("_distance").map(|c| c.as_any().downcast_ref::<Float32Array>().unwrap().clone());
            for j in 0..ids.len() {
                let d = ds.as_ref().map(|a| a.value(j)).unwrap_or(0.0);
                got.push((ids.value(j), d));
            }
        }
        let got_ids: std::collections::HashSet<i32> = got.iter().map(|(id, _)| *id).collect();
        let hits = got_ids.intersection(&truth).count();
        let r = hits as f32 / K as f32;
        if i < 3 {
            let truth_sorted: Vec<i32> = truth.iter().copied().collect();
            println!("q{i}: got {} rows, hits {}/{}, recall {r:.2}", got.len(), hits, K);
            println!("   truth: {truth_sorted:?}");
            println!("   got:   {got:?}");
        }
        recalls.push(r);
    }

    let mean = recalls.iter().sum::<f32>() / recalls.len() as f32;
    let min_v = recalls.iter().cloned().fold(1.0f32, f32::min);
    println!("recall mean={mean:.3} min={min_v:.3} across {n_queries} queries");

    assert!(
        mean >= 0.9,
        "recall {mean:.3} < 0.9 — scatter-gather top-K merge needs refine_factor at worker"
    );
}
