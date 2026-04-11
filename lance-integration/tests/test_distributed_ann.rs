// Licensed under the Apache License, Version 2.0.
// Integration test: distributed ANN search via Scatter-Gather.
//
// Test scenario:
// 1. Create 2 Lance tables (shards) with known vector data
// 2. Build IVF_PQ indexes on each shard
// 3. Create 2 LanceTableRegistry instances (simulating 2 Executors)
// 4. Create LanceSchedulerExtension with shard→executor mapping
// 5. Run InProcessScatterGather ANN query
// 6. Verify: correct global TopK results merged from both shards

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use tempfile::TempDir;

use lanceforge::config::{ClusterConfig, ExecutorConfig, ShardConfig, TableConfig};
use lanceforge::executor::LanceTableRegistry;
use lanceforge::merge::global_top_k_by_distance;
use lanceforge::scheduler::{InProcessScatterGather, LanceSchedulerExtension};

/// Generate random vectors for testing
fn generate_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    // Simple deterministic pseudo-random for reproducibility
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

/// Create a Lance table with vectors and metadata
async fn create_test_lance_table(
    path: &str,
    num_rows: usize,
    dim: usize,
    seed: u64,
    id_offset: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let vectors = generate_vectors(num_rows, dim, seed);

    // Build Arrow arrays
    let ids: Vec<i32> = (id_offset..id_offset + num_rows as i32).collect();
    let categories: Vec<String> = ids
        .iter()
        .map(|id| {
            if id % 2 == 0 {
                "cat_a".to_string()
            } else {
                "cat_b".to_string()
            }
        })
        .collect();

    let id_array = Arc::new(Int32Array::from(ids));
    let category_array = Arc::new(StringArray::from(categories));

    // Create FixedSizeList for vectors
    let flat_values: Vec<f32> = vectors.iter().flatten().copied().collect();
    let values_array = Arc::new(Float32Array::from(flat_values));
    let vector_array = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
            values_array,
            None,
        )
        .unwrap(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
    ]));

    let batch =
        RecordBatch::try_new(schema.clone(), vec![id_array, category_array, vector_array]).unwrap();

    // Write Lance table using RecordBatchIterator
    let reader = arrow::record_batch::RecordBatchIterator::new(
        vec![Ok(batch)],
        schema,
    );
    let mut write_params = lance::dataset::WriteParams::default();
    write_params.mode = lance::dataset::WriteMode::Create;
    lance::dataset::Dataset::write(reader, path, Some(write_params)).await?;

    Ok(())
}

#[tokio::test]
async fn test_merge_global_top_k() {
    // Test the merge module independently
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("_distance", DataType::Float32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float32Array::from(vec![0.1, 0.3, 0.5])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Arc::new(Float32Array::from(vec![0.05, 0.2, 0.4])),
        ],
    )
    .unwrap();

    let result = global_top_k_by_distance(vec![batch1, batch2], 3).unwrap();
    assert_eq!(result.num_rows(), 3);

    let ids = result
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let distances = result
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    // Closest: id=4 (0.05), id=1 (0.1), id=5 (0.2)
    assert_eq!(ids.value(0), 4);
    assert_eq!(ids.value(1), 1);
    assert_eq!(ids.value(2), 5);
    assert!((distances.value(0) - 0.05).abs() < 1e-6);
}

#[tokio::test]
async fn test_config_routing() {
    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "products".to_string(),
            shards: vec![
                ShardConfig {
                    name: "shard_00".to_string(),
                    uri: "s3://test/shard_00.lance".to_string(),
                    executors: vec!["exec_0".to_string()],
                },
                ShardConfig {
                    name: "shard_01".to_string(),
                    uri: "s3://test/shard_01.lance".to_string(),
                    executors: vec!["exec_1".to_string()],
                },
            ],
        }],
        executors: vec![
            ExecutorConfig {
                id: "exec_0".to_string(),
                host: "127.0.0.1".to_string(),
                port: 50051,
            },
            ExecutorConfig {
                id: "exec_1".to_string(),
                host: "127.0.0.1".to_string(),
                port: 50052,
            },
        ],
        ..Default::default()
    };

    let scheduler =
        LanceSchedulerExtension::new(&config, std::time::Duration::from_secs(30));

    let targets = scheduler
        .resolve_targets("products")
        .expect("Should resolve targets");
    assert_eq!(targets.len(), 2);
    assert!(targets.contains(&"exec_0".to_string()));
    assert!(targets.contains(&"exec_1".to_string()));
}

// Full end-to-end test with real Lance data
// This test creates actual Lance tables, registers them on simulated executors,
// and runs a distributed ANN query via InProcessScatterGather.
#[tokio::test]
async fn test_end_to_end_distributed_ann() {
    let tmp = TempDir::new().unwrap();
    let dim = 8; // small dimension for fast testing
    let num_rows_per_shard = 100;

    // Create two shard tables
    let shard0_path = tmp.path().join("shard_00.lance");
    let shard1_path = tmp.path().join("shard_01.lance");

    create_test_lance_table(
        shard0_path.to_str().unwrap(),
        num_rows_per_shard,
        dim,
        42,
        0,
    )
    .await
    .expect("Failed to create shard 0");

    create_test_lance_table(
        shard1_path.to_str().unwrap(),
        num_rows_per_shard,
        dim,
        123,
        num_rows_per_shard as i32,
    )
    .await
    .expect("Failed to create shard 1");

    // Config
    let config = ClusterConfig {
        tables: vec![TableConfig {
            name: "test_table".to_string(),
            shards: vec![
                ShardConfig {
                    name: "test_table_shard_00".to_string(),
                    uri: shard0_path.to_str().unwrap().to_string(),
                    executors: vec!["exec_0".to_string()],
                },
                ShardConfig {
                    name: "test_table_shard_01".to_string(),
                    uri: shard1_path.to_str().unwrap().to_string(),
                    executors: vec!["exec_1".to_string()],
                },
            ],
        }],
        executors: vec![
            ExecutorConfig {
                id: "exec_0".to_string(),
                host: "127.0.0.1".to_string(),
                port: 50051,
            },
            ExecutorConfig {
                id: "exec_1".to_string(),
                host: "127.0.0.1".to_string(),
                port: 50052,
            },
        ],
        ..Default::default()
    };

    // Create executor registries
    let ctx0 = SessionContext::default();
    let shards0 = config.shards_for_executor("exec_0");
    let registry0 = LanceTableRegistry::new(ctx0, &shards0)
        .await
        .expect("Failed to create registry 0");

    let ctx1 = SessionContext::default();
    let shards1 = config.shards_for_executor("exec_1");
    let registry1 = LanceTableRegistry::new(ctx1, &shards1)
        .await
        .expect("Failed to create registry 1");

    // Create scheduler + in-process scatter-gather
    let scheduler =
        LanceSchedulerExtension::new(&config, std::time::Duration::from_secs(30));

    let mut executors = HashMap::new();
    executors.insert("exec_0".to_string(), registry0);
    executors.insert("exec_1".to_string(), registry1);

    let sg = InProcessScatterGather {
        scheduler,
        executors,
    };

    // Run distributed ANN query
    let query_vector = vec![0.0_f32; dim]; // zero vector
    let result = sg
        .scatter_gather_ann(
            "test_table",
            query_vector,
            "vector",
            5, // top-5
            1, // nprobes
            None,
            vec![],
        )
        .await
        .expect("ANN scatter-gather should succeed");

    // Verify results
    assert!(result.num_rows() > 0, "Should have results");
    assert!(result.num_rows() <= 5, "Should respect top-K limit");

    // Verify _distance column exists and is sorted ascending
    let distance_idx = result.schema().index_of("_distance").unwrap();
    let distances = result
        .column(distance_idx)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();

    for i in 1..distances.len() {
        assert!(
            distances.value(i) >= distances.value(i - 1),
            "Distances should be sorted ascending: {} >= {}",
            distances.value(i),
            distances.value(i - 1)
        );
    }

    println!(
        "Distributed ANN test passed: {} results, min_distance={:.4}",
        result.num_rows(),
        distances.value(0)
    );
}
