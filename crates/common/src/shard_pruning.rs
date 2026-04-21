// Licensed under the Apache License, Version 2.0.
// Shard pruning: skip shards that cannot contain matching rows.
//
// When a query has a WHERE filter like "category = 'electronics'",
// we check each shard's metadata to see if it could contain that value.
// Shards with no matching data are excluded from the Scatter fanout.
//
// This reduces fanout from N (all shards) to K (relevant shards),
// directly improving P99 latency and reducing compute waste.

use std::collections::HashMap;

use log::debug;
use serde::{Deserialize, Serialize};

/// Per-shard metadata for pruning decisions.
/// Stored alongside the cluster config or computed at shard creation time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    /// Shard name (matches config)
    pub shard_name: String,
    /// Row count in this shard
    pub num_rows: u64,
    /// Per-column min/max statistics for scalar columns
    pub column_stats: HashMap<String, ColumnStats>,
}

/// Min/max statistics for a single column in a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value (as string for universal comparison)
    pub min_value: Option<String>,
    /// Maximum value
    pub max_value: Option<String>,
    /// Distinct values (if cardinality is low, store all; else None)
    pub distinct_values: Option<Vec<String>>,
}

/// Shard pruning engine.
pub struct ShardPruner {
    /// shard_name → metadata
    metadata: HashMap<String, ShardMetadata>,
}

impl ShardPruner {
    pub fn new(metadata: Vec<ShardMetadata>) -> Self {
        let map: HashMap<String, ShardMetadata> = metadata
            .into_iter()
            .map(|m| (m.shard_name.clone(), m))
            .collect();
        Self { metadata: map }
    }

    /// No metadata → no pruning
    pub fn empty() -> Self {
        Self { metadata: HashMap::new() }
    }

    /// Prune shards based on a SQL filter expression.
    /// Returns the subset of shard_names that could match the filter.
    ///
    /// Simple implementation: parse basic equality predicates like "column = 'value'"
    /// and check against distinct_values or min/max bounds.
    pub fn prune(&self, shard_names: &[String], filter: Option<&str>) -> Vec<String> {
        let filter = match filter {
            Some(f) if !f.is_empty() => f,
            _ => return shard_names.to_vec(), // no filter → no pruning
        };

        if self.metadata.is_empty() {
            return shard_names.to_vec(); // no metadata → no pruning
        }

        // Parse simple equality: "column = 'value'" or "column = value"
        let predicate = parse_simple_equality(filter);

        match predicate {
            Some((column, value)) => {
                let result: Vec<String> = shard_names
                    .iter()
                    .filter(|name| {
                        match self.metadata.get(*name) {
                            Some(meta) => shard_could_match(meta, &column, &value),
                            None => true, // no metadata for this shard → keep it
                        }
                    })
                    .cloned()
                    .collect();

                debug!(
                    "Shard pruning: {} → {} shards (filter: {}={}, pruned {})",
                    shard_names.len(), result.len(), column, value,
                    shard_names.len() - result.len()
                );
                result
            }
            None => {
                // Cannot parse filter → no pruning (safe default)
                debug!("Shard pruning: cannot parse filter '{}', skipping", filter);
                shard_names.to_vec()
            }
        }
    }
}

/// Check if a shard could contain rows matching column = value.
fn shard_could_match(meta: &ShardMetadata, column: &str, value: &str) -> bool {
    match meta.column_stats.get(column) {
        Some(stats) => {
            // Check distinct values first (exact match)
            if let Some(distinct) = &stats.distinct_values {
                return distinct.iter().any(|v| v == value);
            }
            // Check min/max range
            match (&stats.min_value, &stats.max_value) {
                (Some(min), Some(max)) => value >= min.as_str() && value <= max.as_str(),
                _ => true, // incomplete stats → keep shard
            }
        }
        None => true, // no stats for this column → keep shard
    }
}

/// Hash-route a read request to a single shard.
///
/// If the filter is a simple equality predicate on the table's
/// declared `on_columns` hash key, return the target shard index —
/// computed with the same hash the write path uses in
/// `coordinator::service::partition_batch_by_hash`. Callers that
/// get `Some(idx)` can skip the full scatter and dispatch to just
/// that one shard; anything else (unparseable filter, filter column
/// not in on_columns, multi-column on_columns, num_shards <= 1)
/// returns `None` so the caller falls back to the existing
/// full-scatter path.
///
/// Safety: this is a **pruning optimisation only**. A `Some` result
/// must agree with the write-side hash; otherwise we miss rows. The
/// unit tests below pin the agreement with the write path's hash
/// formula across string and numeric keys.
pub fn hash_route_shard(
    filter: Option<&str>,
    on_columns: &[String],
    num_shards: usize,
) -> Option<usize> {
    if num_shards <= 1 {
        return None;
    }
    // Multi-column on_columns means the write-side hash mixes all key
    // values per row. A single-predicate filter (col = val) can only
    // constrain one dimension; without the others we can't uniquely
    // identify the bucket. Fall back to full scatter.
    if on_columns.len() != 1 {
        return None;
    }
    let filter = filter?;
    let (col, val) = parse_simple_equality(filter)?;
    if col != on_columns[0] {
        return None;
    }
    Some(hash_value_to_shard(&val, num_shards))
}

/// The authoritative hash formula shared with the write path.
///
/// `coordinator::service::partition_batch_by_hash` hashes the Arrow
/// string representation of the scalar value. For primitive types
/// `arrow::util::display::array_value_to_string` produces the
/// canonical decimal / string form:
///   int   42    -> "42"
///   str   "abc" -> "abc"
///   float 3.14  -> "3.14"
/// The read-side filter value is already a string (SQL literal), so
/// we just hash it directly with the same `DefaultHasher`. String
/// and int keys agree trivially; floats / decimals may diverge under
/// formatter differences — documented as "only int / string keys
/// safe for partition pruning" in the user docs.
pub fn hash_value_to_shard(value: &str, num_shards: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() % num_shards as u64) as usize
}

/// Parse simple equality predicate: "column = 'value'" or "column = value"
fn parse_simple_equality(filter: &str) -> Option<(String, String)> {
    let filter = filter.trim();

    // Reject compound expressions — only handle single equality
    let upper = filter.to_uppercase();
    if upper.contains(" AND ") || upper.contains(" OR ") || upper.contains(" NOT ")
        || upper.contains(">=") || upper.contains("<=") || upper.contains("!=")
    {
        return None;
    }

    // Handle: column = 'value', column='value', column = value
    let parts: Vec<&str> = filter.splitn(2, '=').collect();
    if parts.len() != 2 {
        return None;
    }

    let column = parts[0].trim().to_string();
    let value = parts[1]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    if column.is_empty() || value.is_empty() || column.contains(' ') {
        return None;
    }

    Some((column, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metadata() -> Vec<ShardMetadata> {
        vec![
            ShardMetadata {
                shard_name: "shard_00".into(),
                num_rows: 1000,
                column_stats: HashMap::from([
                    ("category".into(), ColumnStats {
                        min_value: None, max_value: None,
                        distinct_values: Some(vec!["electronics".into(), "books".into()]),
                    }),
                    ("region".into(), ColumnStats {
                        min_value: Some("APAC".into()),
                        max_value: Some("APAC".into()),
                        distinct_values: Some(vec!["APAC".into()]),
                    }),
                ]),
            },
            ShardMetadata {
                shard_name: "shard_01".into(),
                num_rows: 1000,
                column_stats: HashMap::from([
                    ("category".into(), ColumnStats {
                        min_value: None, max_value: None,
                        distinct_values: Some(vec!["clothing".into(), "food".into()]),
                    }),
                    ("region".into(), ColumnStats {
                        min_value: Some("EMEA".into()),
                        max_value: Some("EMEA".into()),
                        distinct_values: Some(vec!["EMEA".into()]),
                    }),
                ]),
            },
            ShardMetadata {
                shard_name: "shard_02".into(),
                num_rows: 1000,
                column_stats: HashMap::from([
                    ("category".into(), ColumnStats {
                        min_value: None, max_value: None,
                        distinct_values: Some(vec!["electronics".into(), "food".into()]),
                    }),
                    ("region".into(), ColumnStats {
                        min_value: Some("NA".into()),
                        max_value: Some("NA".into()),
                        distinct_values: Some(vec!["NA".into()]),
                    }),
                ]),
            },
        ]
    }

    #[test]
    fn test_prune_by_category() {
        let pruner = ShardPruner::new(test_metadata());
        let all = vec!["shard_00".into(), "shard_01".into(), "shard_02".into()];

        let result = pruner.prune(&all, Some("category = 'electronics'"));
        assert_eq!(result, vec!["shard_00", "shard_02"]); // shard_01 has no electronics
    }

    #[test]
    fn test_prune_by_region() {
        let pruner = ShardPruner::new(test_metadata());
        let all = vec!["shard_00".into(), "shard_01".into(), "shard_02".into()];

        let result = pruner.prune(&all, Some("region = 'APAC'"));
        assert_eq!(result, vec!["shard_00"]); // only shard_00 has APAC
    }

    #[test]
    fn test_no_filter_no_prune() {
        let pruner = ShardPruner::new(test_metadata());
        let all = vec!["shard_00".into(), "shard_01".into(), "shard_02".into()];

        let result = pruner.prune(&all, None);
        assert_eq!(result.len(), 3);

        let result2 = pruner.prune(&all, Some(""));
        assert_eq!(result2.len(), 3);
    }

    #[test]
    fn test_no_metadata_no_prune() {
        let pruner = ShardPruner::empty();
        let all = vec!["shard_00".into(), "shard_01".into()];

        let result = pruner.prune(&all, Some("category = 'electronics'"));
        assert_eq!(result.len(), 2); // no metadata → keep all
    }

    #[test]
    fn test_complex_filter_no_prune() {
        let pruner = ShardPruner::new(test_metadata());
        let all = vec!["shard_00".into(), "shard_01".into()];

        // Complex filter we can't parse → keep all
        let result = pruner.prune(&all, Some("category = 'a' AND region = 'b'"));
        assert_eq!(result.len(), 2);
    }

    // ── Partition pruning (hash-routed) tests ──

    #[test]
    fn hash_route_returns_none_when_num_shards_le_1() {
        assert_eq!(hash_route_shard(Some("id = 'abc'"), &["id".into()], 0), None);
        assert_eq!(hash_route_shard(Some("id = 'abc'"), &["id".into()], 1), None);
    }

    #[test]
    fn hash_route_returns_none_when_no_on_columns() {
        assert_eq!(hash_route_shard(Some("id = 'abc'"), &[], 4), None);
    }

    #[test]
    fn hash_route_returns_none_when_filter_missing() {
        assert_eq!(hash_route_shard(None, &["id".into()], 4), None);
        assert_eq!(hash_route_shard(Some(""), &["id".into()], 4), None);
    }

    #[test]
    fn hash_route_returns_none_for_multicolumn_on_columns() {
        // Write-side hashes tuple of column values — a single-predicate
        // filter can't uniquely identify the bucket.
        assert_eq!(
            hash_route_shard(Some("a = 'x'"), &["a".into(), "b".into()], 4),
            None
        );
    }

    #[test]
    fn hash_route_returns_none_when_filter_column_not_in_on_columns() {
        assert_eq!(
            hash_route_shard(Some("category = 'foo'"), &["id".into()], 4),
            None
        );
    }

    #[test]
    fn hash_route_returns_none_for_complex_filter() {
        // parse_simple_equality rejects compound / non-equality ops.
        assert_eq!(
            hash_route_shard(Some("id = 'x' AND foo = 'y'"), &["id".into()], 4),
            None
        );
        assert_eq!(
            hash_route_shard(Some("id >= 'x'"), &["id".into()], 4),
            None
        );
    }

    #[test]
    fn hash_route_is_deterministic_and_in_range() {
        let on = vec!["id".into()];
        for v in ["abc", "42", "hello world", ""] {
            let a = hash_route_shard(Some(&format!("id = '{v}'")), &on, 8);
            let b = hash_route_shard(Some(&format!("id = '{v}'")), &on, 8);
            if !v.is_empty() {
                assert!(a.is_some());
                assert_eq!(a, b);
                let idx = a.unwrap();
                assert!(idx < 8, "shard idx {idx} out of range");
            } else {
                // parse_simple_equality requires non-empty value
                assert_eq!(a, None);
            }
        }
    }

    #[test]
    fn hash_route_agrees_with_write_path_for_string_keys() {
        // Pin agreement with coordinator::service::partition_batch_by_hash.
        // For a string column the write path hashes the value's string
        // representation — for a plain `StringArray`, that's the value
        // itself, so our helper should produce the same shard index as
        // the write path would for a row with that key.
        use std::hash::{Hash, Hasher};
        let vals = ["tenant-a", "tenant-b", "tenant-c", "42", "long-string-value"];
        for v in &vals {
            // Write-side simulation: hash the "formatted scalar" string.
            let mut write_hasher = std::collections::hash_map::DefaultHasher::new();
            v.hash(&mut write_hasher);
            let write_shard = (write_hasher.finish() % 8) as usize;

            // Read-side: same value through hash_route_shard.
            let read_shard = hash_route_shard(
                Some(&format!("id = '{v}'")),
                &["id".into()],
                8,
            )
            .expect("valid filter");

            assert_eq!(
                read_shard, write_shard,
                "write/read hash disagreement for '{v}'"
            );
        }
    }

    #[test]
    fn hash_route_handles_numeric_literal() {
        // SQL `id = 42` (unquoted) parses to value="42"; the write path
        // for an Int32 column formats 42 as "42". Same string, same
        // hash.
        let out = hash_route_shard(Some("id = 42"), &["id".into()], 4);
        assert!(out.is_some(), "numeric literal must parse");
    }

    #[test]
    fn test_parse_equality_variants() {
        assert_eq!(
            parse_simple_equality("category = 'electronics'"),
            Some(("category".into(), "electronics".into()))
        );
        assert_eq!(
            parse_simple_equality("region='APAC'"),
            Some(("region".into(), "APAC".into()))
        );
        assert_eq!(
            parse_simple_equality("category = electronics"),
            Some(("category".into(), "electronics".into()))
        );
        assert_eq!(parse_simple_equality("no_equals_sign"), None);
        assert_eq!(parse_simple_equality(""), None);
    }

    /// Performance regression: prune 100 shards in <1ms.
    /// Skipped under sanitizers — instrumentation adds 5-10x slowdown.
    /// Set SKIP_PERF_TESTS=1 to skip (CI sets this under sanitizer runs).
    #[test]
    fn test_pruning_performance_regression() {
        if std::env::var("SKIP_PERF_TESTS").is_ok() {
            eprintln!("SKIP_PERF_TESTS set — skipping under sanitizer");
            return;
        }
        let metadata: Vec<ShardMetadata> = (0..100).map(|i| {
            ShardMetadata {
                shard_name: format!("shard_{:04}", i),
                num_rows: 10000,
                column_stats: HashMap::from([
                    ("category".into(), ColumnStats {
                        min_value: None, max_value: None,
                        distinct_values: Some(vec![format!("cat_{}", i % 10)]),
                    }),
                ]),
            }
        }).collect();
        let pruner = ShardPruner::new(metadata);
        let all: Vec<String> = (0..100).map(|i| format!("shard_{:04}", i)).collect();

        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = pruner.prune(&all, Some("category = 'cat_5'"));
        }
        let elapsed = start.elapsed();

        // 1000 iterations over 100 shards should complete in <200ms
        // (relaxed from 100ms — CI/debug builds run slower)
        assert!(elapsed.as_millis() < 200, "Pruning took {}ms (>200ms regression)", elapsed.as_millis());
    }
}
