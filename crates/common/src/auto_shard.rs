// Licensed under the Apache License, Version 2.0.
// Auto-sharding: discover Lance fragments and assign them to executors.
//
// Replaces manual YAML shard configuration with automatic fragment-based
// routing. Each Lance table internally consists of Fragments (atomic data
// units). This module maps fragments to executors via greedy bin-packing.

use std::collections::HashMap;

use log::info;

/// Split a URI into (parent_directory, name).
/// e.g. "s3://bucket/path/table.lance" → ("s3://bucket/path/", "table")
pub fn split_parent_uri(uri: &str) -> (String, String) {
    let clean = uri.trim_end_matches('/');
    if let Some(last_slash) = clean.rfind('/') {
        let parent = &clean[..=last_slash];
        let filename = &clean[last_slash + 1..];
        let name = filename.strip_suffix(".lance").unwrap_or(filename);
        (parent.to_string(), name.to_string())
    } else {
        let name = clean.strip_suffix(".lance").unwrap_or(clean);
        ("./".to_string(), name.to_string())
    }
}

use crate::config::{ClusterConfig, ExecutorConfig, ShardConfig, TableConfig};

/// Metadata about a single Lance fragment.
#[derive(Debug, Clone)]
pub struct FragmentInfo {
    pub fragment_id: u64,
    pub physical_rows: usize,
}

/// Discover fragments in a Lance dataset.
pub async fn discover_fragments(
    table_uri: &str,
    storage_options: &HashMap<String, String>,
) -> Result<Vec<FragmentInfo>, Box<dyn std::error::Error + Send + Sync>> {
    let builder = lance::dataset::builder::DatasetBuilder::from_uri(table_uri)
        .with_storage_options(storage_options.clone());
    let dataset = builder.load().await?;

    let mut fragments = Vec::new();
    for f in dataset.get_fragments() {
        let rows = f.count_rows(None).await.unwrap_or(0);
        fragments.push(FragmentInfo {
            fragment_id: f.id() as u64,
            physical_rows: rows,
        });
    }

    info!(
        "Discovered {} fragments in {} ({} total rows)",
        fragments.len(),
        table_uri,
        fragments.iter().map(|f| f.physical_rows).sum::<usize>()
    );

    Ok(fragments)
}

/// Assign fragments to executors using greedy bin-packing (least-loaded first).
/// Groups adjacent fragments into shards to reduce the number of shards per executor.
pub fn assign_fragments_to_executors(
    table_name: &str,
    table_uri: &str,
    fragments: &[FragmentInfo],
    executor_ids: &[String],
    max_fragments_per_shard: usize,
) -> Vec<ShardConfig> {
    if fragments.is_empty() || executor_ids.is_empty() {
        return vec![];
    }

    // Group fragments into chunks (shards)
    let chunk_size = max_fragments_per_shard.max(1);
    let chunks: Vec<&[FragmentInfo]> = fragments.chunks(chunk_size).collect();

    // Assign chunks to executors via round-robin (simple, balanced)
    let mut shards = Vec::new();
    for (i, chunk) in chunks.iter().enumerate() {
        let executor_id = &executor_ids[i % executor_ids.len()];
        let first_frag = chunk.first().map(|f| f.fragment_id).unwrap_or(0);
        let last_frag = chunk.last().map(|f| f.fragment_id).unwrap_or(0);
        let total_rows: usize = chunk.iter().map(|f| f.physical_rows).sum();

        shards.push(ShardConfig {
            name: format!("{}_frag_{}_{}", table_name, first_frag, last_frag),
            uri: table_uri.to_string(),
            executors: vec![executor_id.clone()],
        });

        info!(
            "Shard {}: fragments {}-{} ({} rows) → {}",
            shards.len() - 1,
            first_frag,
            last_frag,
            total_rows,
            executor_id
        );
    }

    shards
}

/// Generate a complete ClusterConfig from auto-discovered fragments.
pub async fn generate_auto_config(
    table_name: &str,
    table_uri: &str,
    executors: &[ExecutorConfig],
    storage_options: &HashMap<String, String>,
) -> Result<ClusterConfig, Box<dyn std::error::Error + Send + Sync>> {
    let fragments = discover_fragments(table_uri, storage_options).await?;
    let executor_ids: Vec<String> = executors.iter().map(|e| e.id.clone()).collect();

    // Aim for ~1 shard per executor; group fragments accordingly
    let fragments_per_shard = (fragments.len() / executor_ids.len().max(1)).max(1);
    let shards = assign_fragments_to_executors(
        table_name, table_uri, &fragments, &executor_ids, fragments_per_shard,
    );

    Ok(ClusterConfig {
        tables: vec![TableConfig {
            name: table_name.to_string(),
            shards,
        }],
        executors: executors.to_vec(),
        storage_options: storage_options.clone(),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fragments(count: usize, rows_each: usize) -> Vec<FragmentInfo> {
        (0..count)
            .map(|i| FragmentInfo {
                fragment_id: i as u64,
                physical_rows: rows_each,
            })
            .collect()
    }

    #[test]
    fn test_assign_empty() {
        let shards = assign_fragments_to_executors("t", "uri", &[], &["w0".into()], 1);
        assert!(shards.is_empty());
    }

    #[test]
    fn test_assign_single_executor() {
        let frags = make_fragments(5, 1000);
        let shards = assign_fragments_to_executors("t", "uri", &frags, &["w0".into()], 5);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].executors, vec!["w0"]);
    }

    #[test]
    fn test_assign_round_robin() {
        let frags = make_fragments(6, 1000);
        let executors = vec!["w0".into(), "w1".into(), "w2".into()];
        let shards = assign_fragments_to_executors("t", "uri", &frags, &executors, 2);
        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].executors[0], "w0");
        assert_eq!(shards[1].executors[0], "w1");
        assert_eq!(shards[2].executors[0], "w2");
    }

    #[test]
    fn test_assign_more_shards_than_executors() {
        let frags = make_fragments(10, 100);
        let executors = vec!["w0".into(), "w1".into()];
        let shards = assign_fragments_to_executors("t", "uri", &frags, &executors, 2);
        assert_eq!(shards.len(), 5);
        // Round-robin: w0, w1, w0, w1, w0
        assert_eq!(shards[0].executors[0], "w0");
        assert_eq!(shards[1].executors[0], "w1");
        assert_eq!(shards[2].executors[0], "w0");
    }

    #[test]
    fn test_shard_naming() {
        let frags = make_fragments(4, 500);
        let shards = assign_fragments_to_executors("products", "s3://b/p.lance", &frags, &["w0".into()], 2);
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].name, "products_frag_0_1");
        assert_eq!(shards[1].name, "products_frag_2_3");
        assert_eq!(shards[0].uri, "s3://b/p.lance");
    }

    #[test]
    fn test_split_parent_uri_s3() {
        let (parent, name) = split_parent_uri("s3://bucket/path/table.lance");
        assert_eq!(parent, "s3://bucket/path/");
        assert_eq!(name, "table");
    }

    #[test]
    fn test_split_parent_uri_local_absolute() {
        let (parent, name) = split_parent_uri("/data/warehouse/products.lance");
        assert_eq!(parent, "/data/warehouse/");
        assert_eq!(name, "products");
    }

    #[test]
    fn test_split_parent_uri_no_lance_suffix() {
        // Not all Lance datasets have the .lance suffix on disk.
        let (parent, name) = split_parent_uri("/tmp/foo_dataset");
        assert_eq!(parent, "/tmp/");
        assert_eq!(name, "foo_dataset");
    }

    #[test]
    fn test_split_parent_uri_trailing_slash() {
        // Trailing slash on Lance directories is common; shouldn't confuse the split.
        let (parent, name) = split_parent_uri("s3://bucket/table.lance/");
        assert_eq!(parent, "s3://bucket/");
        assert_eq!(name, "table");
    }

    #[test]
    fn test_split_parent_uri_no_directory() {
        // Edge case: no slash at all — the dataset is "relative cwd".
        let (parent, name) = split_parent_uri("table.lance");
        assert_eq!(parent, "./");
        assert_eq!(name, "table");
    }

    #[test]
    fn test_assign_empty_executors() {
        let frags = make_fragments(3, 100);
        let shards = assign_fragments_to_executors("t", "uri", &frags, &[], 1);
        assert!(shards.is_empty(),
            "no executors must return empty — otherwise caller can't route anywhere");
    }

    #[test]
    fn test_assign_zero_chunk_size_defaults_to_one() {
        // Caller mistake: max_fragments_per_shard=0 would cause a div-by-zero
        // or panic if we didn't clamp. The .max(1) guard prevents that.
        let frags = make_fragments(3, 100);
        let shards = assign_fragments_to_executors("t", "uri", &frags, &["w0".into()], 0);
        assert_eq!(shards.len(), 3, "chunk size 0 should behave like 1 (one shard per fragment)");
    }

    #[test]
    fn test_assign_row_aggregation_in_shard_name() {
        // Each shard name should span the first and last fragment IDs in the chunk.
        let frags = make_fragments(7, 100);
        let shards = assign_fragments_to_executors("t", "uri", &frags, &["w0".into()], 3);
        // 7 frags / chunks of 3 → [0-2] [3-5] [6-6]
        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].name, "t_frag_0_2");
        assert_eq!(shards[1].name, "t_frag_3_5");
        assert_eq!(shards[2].name, "t_frag_6_6");
    }
}
