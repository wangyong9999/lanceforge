// Licensed under the Apache License, Version 2.0.
//
// PoC Gate 1 — verify Lance `with_fragments` + `.nearest()` + `prefilter(true)`
// is a viable primitive for distributed scatter-gather on a SINGLE Lance
// dataset. If this gate passes, fan-out by fragment-id is the replacement
// for multi-shard scatter-gather in the single-dataset architecture path.
//
// Scope:
//   * Pure Lance/Arrow APIs. No LanceForge gRPC, no multi-shard code paths.
//   * One dataset, four fragments, split [0,1] vs [2,3] across two "workers".
//   * Compare merged fan-out top-K against a full-scan baseline top-K.
//
// Pass: recall(fanout_topK, baseline_topK) ≥ 0.99 at oversample=10.
// Kill: any of those API calls errors, or recall is materially below baseline.

use std::sync::Arc;

use arrow::array::{Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use futures::TryStreamExt;
use tempfile::TempDir;

const DIM: usize = 64;
const FRAGMENTS: usize = 4;
const ROWS_PER_FRAG: usize = 2_500; // 10k total
const K: usize = 10;
const OVERSAMPLE: usize = 10; // each worker pulls K*OVERSAMPLE candidates

fn deterministic_vecs(seed: u64, count: usize, dim: usize) -> Vec<f32> {
    let mut state = seed;
    let mut v = Vec::with_capacity(count * dim);
    for _ in 0..(count * dim) {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        v.push(((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0);
    }
    v
}

fn make_batch(id_start: i32, seed: u64) -> (Arc<Schema>, RecordBatch) {
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
    let flat = deterministic_vecs(seed, ROWS_PER_FRAG, DIM);
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
    (schema, batch)
}

async fn create_dataset_4_fragments(uri: &str) {
    for i in 0..FRAGMENTS {
        let (schema, batch) = make_batch(
            (i * ROWS_PER_FRAG) as i32,
            42u64.wrapping_add(i as u64 * 1_000_003),
        );
        let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
        let params = lance::dataset::WriteParams {
            mode: if i == 0 {
                lance::dataset::WriteMode::Create
            } else {
                lance::dataset::WriteMode::Append
            },
            // Force 1 batch → 1 fragment per write call.
            max_rows_per_file: ROWS_PER_FRAG,
            max_rows_per_group: ROWS_PER_FRAG,
            ..Default::default()
        };
        lance::dataset::Dataset::write(reader, uri, Some(params))
            .await
            .unwrap();
    }
}

async fn collect_ids(
    stream: lance::dataset::scanner::DatasetRecordBatchStream,
) -> Vec<i32> {
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..ids.len() {
            out.push(ids.value(i));
        }
    }
    out
}

#[tokio::test]
async fn gate1_with_fragments_prefilter_nearest() {
    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().join("ds.lance");
    let uri_s = uri.to_str().unwrap();

    create_dataset_4_fragments(uri_s).await;

    let dataset = Arc::new(lance::dataset::Dataset::open(uri_s).await.unwrap());
    let all_frags = dataset.get_fragments();
    assert_eq!(
        all_frags.len(),
        FRAGMENTS,
        "expected {FRAGMENTS} fragments, got {}",
        all_frags.len()
    );
    println!(
        "✓ dataset has {} fragments (ids {:?})",
        all_frags.len(),
        all_frags.iter().map(|f| f.id()).collect::<Vec<_>>()
    );

    // Query vec: deterministic pick from fragment 0's row 7.
    let probe_row_seed = 42u64;
    let probe = deterministic_vecs(probe_row_seed, ROWS_PER_FRAG, DIM);
    // Take row index 7 in that fragment (offset 7 * DIM).
    let q_slice = &probe[7 * DIM..8 * DIM];
    let q = Arc::new(Float32Array::from(q_slice.to_vec()));

    // === Baseline: full-scan nearest on the whole dataset ===
    let mut scanner = dataset.scan();
    scanner
        .nearest("vector", q.as_ref(), K)
        .expect("baseline nearest() should work without fragments");
    // No .limit() needed — nearest already caps at K.
    let stream = scanner.try_into_stream().await.unwrap();
    let baseline_ids = collect_ids(stream).await;
    assert_eq!(
        baseline_ids.len(),
        K,
        "baseline should return exactly K={K} rows, got {}",
        baseline_ids.len()
    );
    println!("✓ baseline top-{K} ids: {baseline_ids:?}");

    // === Fan-out: split fragments [0,1] vs [2,3] ===
    let pick_fragments = |ids: &[usize]| -> Vec<lance_table::format::Fragment> {
        ids.iter()
            .map(|&i| dataset.get_fragment(i).unwrap().metadata().clone())
            .collect()
    };

    let run_worker = |frag_meta: Vec<lance_table::format::Fragment>| {
        let ds = dataset.clone();
        let q = q.clone();
        async move {
            let mut scan = ds.scan();
            scan.with_fragments(frag_meta);
            scan.prefilter(true); // required — else nearest() errors on fragment scan
            scan.nearest("vector", q.as_ref(), K * OVERSAMPLE)
                .expect("with_fragments + prefilter + nearest should succeed");
            let stream = scan.try_into_stream().await.unwrap();
            collect_ids(stream).await
        }
    };

    let w0 = run_worker(pick_fragments(&[0, 1]));
    let w1 = run_worker(pick_fragments(&[2, 3]));
    let (w0_ids, w1_ids) = tokio::join!(w0, w1);
    println!(
        "✓ worker0 returned {} candidates, worker1 returned {}",
        w0_ids.len(),
        w1_ids.len()
    );
    assert!(
        !w0_ids.is_empty() && !w1_ids.is_empty(),
        "each worker must return non-zero candidates"
    );

    // === Merge: union candidates, then recall-check vs baseline ===
    // (Real scatter-gather would re-rank by distance; here we just check
    //  whether baseline top-K is reachable via the fan-out candidate pool.)
    let mut merged: std::collections::HashSet<i32> = std::collections::HashSet::new();
    merged.extend(w0_ids.iter().copied());
    merged.extend(w1_ids.iter().copied());

    let baseline_set: std::collections::HashSet<i32> = baseline_ids.iter().copied().collect();
    let hits = baseline_set.intersection(&merged).count();
    let recall = hits as f32 / baseline_set.len() as f32;
    println!(
        "✓ recall(fanout candidates ⊇ baseline top-{K}) = {hits}/{K} = {recall:.3}"
    );

    assert!(
        recall >= 0.99,
        "Gate 1 recall < 0.99 — with_fragments+nearest may not be scanning correctly"
    );
    println!("\n=== Gate 1a (exhaustive) PASSED ===");
}

/// Same as gate1, but builds an IVF_PQ index first. Verifies fragment
/// scan interacts correctly with a vector index (the production case).
#[tokio::test]
async fn gate1_with_fragments_nearest_ivf_indexed() {
    use lancedb::index::Index;
    use lancedb::index::vector::IvfPqIndexBuilder;

    let _ = env_logger::try_init();
    let tmp = TempDir::new().unwrap();
    let uri = tmp.path().join("ds.lance");
    let uri_s = uri.to_str().unwrap();
    create_dataset_4_fragments(uri_s).await;

    // Open via lancedb to get create_index, then re-open via lance for scan.
    let parent = tmp.path().to_str().unwrap();
    let db = lancedb::connect(parent).execute().await.unwrap();
    let table = db.open_table("ds").execute().await.unwrap();
    table
        .create_index(
            &["vector"],
            Index::IvfPq(IvfPqIndexBuilder::default().num_partitions(4).num_sub_vectors(8)),
        )
        .execute()
        .await
        .expect("create_index IVF_PQ");
    drop(table);
    drop(db);

    let dataset = Arc::new(lance::dataset::Dataset::open(uri_s).await.unwrap());
    assert_eq!(dataset.get_fragments().len(), FRAGMENTS);

    // Use a real row from the dataset as the query vector so we know it's
    // findable (first row of fragment 2).
    let probe = deterministic_vecs(
        42u64.wrapping_add(2 * 1_000_003),
        ROWS_PER_FRAG,
        DIM,
    );
    let q = Arc::new(Float32Array::from(probe[0..DIM].to_vec()));

    // Baseline with IVF index.
    let mut scanner = dataset.scan();
    scanner.nearest("vector", q.as_ref(), K).unwrap();
    scanner.nprobes(4); // scan all partitions → deterministic recall
    let stream = scanner.try_into_stream().await.unwrap();
    let baseline_ids = collect_ids(stream).await;
    assert_eq!(baseline_ids.len(), K);
    println!("✓ IVF baseline top-{K}: {baseline_ids:?}");

    let pick = |ids: &[usize]| -> Vec<lance_table::format::Fragment> {
        ids.iter()
            .map(|&i| dataset.get_fragment(i).unwrap().metadata().clone())
            .collect()
    };
    let run_worker = |frag_meta: Vec<lance_table::format::Fragment>| {
        let ds = dataset.clone();
        let q = q.clone();
        async move {
            let mut scan = ds.scan();
            scan.with_fragments(frag_meta);
            scan.prefilter(true);
            scan.nearest("vector", q.as_ref(), K * OVERSAMPLE).unwrap();
            scan.nprobes(4);
            let s = scan.try_into_stream().await.unwrap();
            collect_ids(s).await
        }
    };
    let (w0, w1) = tokio::join!(
        run_worker(pick(&[0, 1])),
        run_worker(pick(&[2, 3]))
    );
    println!("✓ IVF worker0={} worker1={} candidates", w0.len(), w1.len());

    let merged: std::collections::HashSet<i32> =
        w0.iter().chain(w1.iter()).copied().collect();
    let baseline_set: std::collections::HashSet<i32> = baseline_ids.iter().copied().collect();
    let hits = baseline_set.intersection(&merged).count();
    let recall = hits as f32 / baseline_set.len() as f32;
    println!("✓ IVF recall = {hits}/{K} = {recall:.3}");

    // IVF is approximate; tolerate one miss but demand at least 0.9.
    assert!(
        recall >= 0.9,
        "Gate 1b IVF recall {recall:.3} < 0.9 — fragment scan may not honor IVF index correctly"
    );
    println!("\n=== Gate 1b (IVF_PQ indexed) PASSED ===");
}
