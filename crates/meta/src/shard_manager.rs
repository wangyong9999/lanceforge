// Licensed under the Apache License, Version 2.0.
// ShardManager: replica-aware shard assignment with soft affinity.
//
// Responsibilities:
// 1. Assign each shard to `replica_factor` workers
// 2. Spread replicas across different workers (anti-affinity)
// 3. Balance shard count per worker (greedy least-loaded)
// 4. Soft affinity: prefer keeping existing assignments (minimize data movement)

use std::collections::{HashMap, HashSet};

use log::info;

/// Shard assignment: shard_name → list of executor_ids (ordered: primary first).
pub type ShardAssignment = HashMap<String, Vec<String>>;

/// Configuration for shard assignment policy.
///
/// NOTE ON SEMANTICS: LanceForge stores data on object storage (S3/GCS/Azure),
/// which already provides durability. `replica_factor` here is NOT data
/// replication — it controls **read fan-out**: how many workers are assigned
/// to hold (open a handle on) the same Lance URI. The extra workers serve
/// reads for QPS scaling and act as read-failover targets. Writes still go
/// to a single primary per shard (coordinator's per-shard routing), because
/// Lance's manifest-CAS already makes a single writer atomic.
#[derive(Debug, Clone)]
pub struct ShardPolicy {
    /// Read-parallelism per shard: 1 = single owner, 2 = primary + read-replica.
    /// Not data replication (OBS handles durability).
    pub replica_factor: usize,
    /// Maximum shards per worker (0 = unlimited).
    pub max_shards_per_worker: usize,
}

impl Default for ShardPolicy {
    fn default() -> Self {
        Self {
            replica_factor: 2,
            max_shards_per_worker: 0,
        }
    }
}

/// Compute a balanced shard assignment with replica spreading.
///
/// Algorithm (greedy least-loaded with anti-affinity):
/// 1. For each shard, assign `replica_factor` workers
/// 2. Each replica goes to the worker with the fewest assigned shards
/// 3. No two replicas of the same shard go to the same worker
///
/// `current` is the existing assignment (for soft affinity — prefer not moving).
/// Compute a balanced shard assignment.
///
/// If `shard_sizes` is provided, balances by total data size (rows) per worker.
/// Otherwise falls back to balancing by shard count.
pub fn assign_shards(
    shard_names: &[String],
    executor_ids: &[String],
    policy: &ShardPolicy,
    current: Option<&ShardAssignment>,
) -> ShardAssignment {
    assign_shards_with_sizes(shard_names, executor_ids, policy, current, None)
}

pub fn assign_shards_with_sizes(
    shard_names: &[String],
    executor_ids: &[String],
    policy: &ShardPolicy,
    current: Option<&ShardAssignment>,
    shard_sizes: Option<&HashMap<String, u64>>,
) -> ShardAssignment {
    if shard_names.is_empty() || executor_ids.is_empty() {
        return HashMap::new();
    }

    let replica_factor = policy.replica_factor.min(executor_ids.len()).max(1);
    let mut assignment: ShardAssignment = HashMap::new();
    // Track load by data size (rows) if available, otherwise by shard count
    let mut worker_load: HashMap<String, u64> = executor_ids.iter()
        .map(|id| (id.clone(), 0u64))
        .collect();

    // Sort shards by size descending (largest first) for better bin-packing
    let mut sorted_shards: Vec<&String> = shard_names.iter().collect();
    if let Some(sizes) = shard_sizes {
        sorted_shards.sort_by(|a, b| {
            let sa = sizes.get(*a).unwrap_or(&1);
            let sb = sizes.get(*b).unwrap_or(&1);
            sb.cmp(sa) // descending
        });
    }

    for shard in &sorted_shards {
        let shard_weight = shard_sizes
            .and_then(|s| s.get(*shard).copied())
            .unwrap_or(1); // fallback: count-based (each shard = 1)

        // Soft affinity: if shard already assigned and workers are still available, keep it
        if let Some(existing) = current.and_then(|c| c.get(*shard)) {
            let still_valid: Vec<String> = existing.iter()
                .filter(|w| executor_ids.contains(w))
                .cloned()
                .collect();
            if still_valid.len() >= replica_factor {
                let kept: Vec<String> = still_valid.into_iter().take(replica_factor).collect();
                for w in &kept {
                    *worker_load.entry(w.clone()).or_default() += shard_weight;
                }
                assignment.insert((*shard).clone(), kept);
                continue;
            }
        }

        // Assign replica_factor workers, preferring least-loaded by data size
        let mut assigned: Vec<String> = Vec::new();
        let mut used: HashSet<String> = HashSet::new();

        for _ in 0..replica_factor {
            if let Some(best) = executor_ids.iter()
                .filter(|w| !used.contains(*w))
                .min_by_key(|w| worker_load.get(*w).unwrap_or(&0))
            {
                assigned.push(best.clone());
                used.insert(best.clone());
                *worker_load.entry(best.clone()).or_default() += shard_weight;
            }
        }

        assignment.insert((*shard).clone(), assigned);
    }

    info!("Assigned {} shards across {} workers (replica_factor={}, size_aware={})",
        shard_names.len(), executor_ids.len(), replica_factor, shard_sizes.is_some());

    assignment
}

/// Compute the diff between current and desired assignments.
/// Returns: (to_load, to_unload) — shards that need to be loaded/unloaded on each worker.
pub fn compute_diff(
    current: &ShardAssignment,
    desired: &ShardAssignment,
) -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>) {
    // to_load[worker] = shards to load on that worker
    // to_unload[worker] = shards to unload from that worker
    let mut to_load: HashMap<String, Vec<String>> = HashMap::new();
    let mut to_unload: HashMap<String, Vec<String>> = HashMap::new();

    // Find new assignments
    for (shard, desired_workers) in desired {
        let current_workers: HashSet<&String> = current.get(shard)
            .map(|v| v.iter().collect())
            .unwrap_or_default();

        for w in desired_workers {
            if !current_workers.contains(w) {
                to_load.entry(w.clone()).or_default().push(shard.clone());
            }
        }
    }

    // Find removed assignments
    for (shard, current_workers) in current {
        let desired_workers: HashSet<&String> = desired.get(shard)
            .map(|v| v.iter().collect())
            .unwrap_or_default();

        for w in current_workers {
            if !desired_workers.contains(w) {
                to_unload.entry(w.clone()).or_default().push(shard.clone());
            }
        }
    }

    (to_load, to_unload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_basic() {
        let shards = vec!["s0".into(), "s1".into(), "s2".into()];
        let executors = vec!["w0".into(), "w1".into(), "w2".into()];
        let policy = ShardPolicy { replica_factor: 1, max_shards_per_worker: 0 };

        let result = assign_shards(&shards, &executors, &policy, None);
        assert_eq!(result.len(), 3);
        // Each shard has exactly 1 executor
        for workers in result.values() {
            assert_eq!(workers.len(), 1);
        }
    }

    #[test]
    fn test_assign_with_replicas() {
        let shards = vec!["s0".into(), "s1".into()];
        let executors = vec!["w0".into(), "w1".into(), "w2".into()];
        let policy = ShardPolicy { replica_factor: 2, max_shards_per_worker: 0 };

        let result = assign_shards(&shards, &executors, &policy, None);
        // Each shard has 2 executors (replicated)
        for workers in result.values() {
            assert_eq!(workers.len(), 2);
            // No duplicate workers for same shard
            let unique: HashSet<&String> = workers.iter().collect();
            assert_eq!(unique.len(), 2);
        }
    }

    #[test]
    fn test_assign_balanced() {
        let shards: Vec<String> = (0..6).map(|i| format!("s{}", i)).collect();
        let executors = vec!["w0".into(), "w1".into(), "w2".into()];
        let policy = ShardPolicy { replica_factor: 1, max_shards_per_worker: 0 };

        let result = assign_shards(&shards, &executors, &policy, None);

        // Count shards per worker
        let mut load: HashMap<String, usize> = HashMap::new();
        for workers in result.values() {
            for w in workers {
                *load.entry(w.clone()).or_default() += 1;
            }
        }
        // Each worker should have exactly 2 shards (6 shards / 3 workers)
        for count in load.values() {
            assert_eq!(*count, 2, "Load should be balanced: {:?}", load);
        }
    }

    #[test]
    fn test_assign_soft_affinity() {
        let shards = vec!["s0".into(), "s1".into()];
        let executors = vec!["w0".into(), "w1".into()];
        let policy = ShardPolicy { replica_factor: 1, max_shards_per_worker: 0 };

        let mut current = HashMap::new();
        current.insert("s0".to_string(), vec!["w0".to_string()]);
        current.insert("s1".to_string(), vec!["w1".to_string()]);

        let result = assign_shards(&shards, &executors, &policy, Some(&current));

        // Should keep existing assignment (soft affinity)
        assert_eq!(result["s0"], vec!["w0"]);
        assert_eq!(result["s1"], vec!["w1"]);
    }

    #[test]
    fn test_assign_worker_removed() {
        let shards = vec!["s0".into(), "s1".into()];
        let executors = vec!["w0".into()]; // w1 removed
        let policy = ShardPolicy { replica_factor: 1, max_shards_per_worker: 0 };

        let mut current = HashMap::new();
        current.insert("s0".to_string(), vec!["w0".to_string()]);
        current.insert("s1".to_string(), vec!["w1".to_string()]); // w1 gone

        let result = assign_shards(&shards, &executors, &policy, Some(&current));

        // s0 stays on w0 (affinity), s1 moves to w0 (only worker left)
        assert_eq!(result["s0"], vec!["w0"]);
        assert_eq!(result["s1"], vec!["w0"]);
    }

    #[test]
    fn test_compute_diff() {
        let mut current = HashMap::new();
        current.insert("s0".to_string(), vec!["w0".to_string()]);
        current.insert("s1".to_string(), vec!["w1".to_string()]);

        let mut desired = HashMap::new();
        desired.insert("s0".to_string(), vec!["w0".to_string()]); // unchanged
        desired.insert("s1".to_string(), vec!["w0".to_string()]); // moved from w1→w0

        let (to_load, to_unload) = compute_diff(&current, &desired);

        assert_eq!(to_load.get("w0").map(|v| v.len()).unwrap_or(0), 1); // s1 → w0
        assert_eq!(to_unload.get("w1").map(|v| v.len()).unwrap_or(0), 1); // s1 removed from w1
    }

    #[test]
    fn test_replica_factor_exceeds_workers() {
        let shards = vec!["s0".into()];
        let executors = vec!["w0".into()]; // only 1 worker
        let policy = ShardPolicy { replica_factor: 3, max_shards_per_worker: 0 }; // wants 3 replicas

        let result = assign_shards(&shards, &executors, &policy, None);
        // Should cap at 1 replica (can't exceed worker count)
        assert_eq!(result["s0"].len(), 1);
    }

    #[test]
    fn test_empty_inputs() {
        let policy = ShardPolicy::default();
        assert!(assign_shards(&[], &["w0".into()], &policy, None).is_empty());
        assert!(assign_shards(&["s0".into()], &[], &policy, None).is_empty());
    }

    #[test]
    fn test_size_aware_balancing() {
        // 3 shards with very different sizes: 10M, 100, 5M
        let shards = vec!["big".into(), "tiny".into(), "medium".into()];
        let executors = vec!["w0".into(), "w1".into()];
        let policy = ShardPolicy { replica_factor: 1, max_shards_per_worker: 0 };
        let mut sizes = HashMap::new();
        sizes.insert("big".to_string(), 10_000_000u64);
        sizes.insert("tiny".to_string(), 100u64);
        sizes.insert("medium".to_string(), 5_000_000u64);

        let result = assign_shards_with_sizes(
            &shards, &executors, &policy, None, Some(&sizes));

        // "big" (10M) should be alone on one worker
        // "medium" (5M) + "tiny" (100) should share the other worker
        let big_worker = &result["big"][0];
        let medium_worker = &result["medium"][0];
        let tiny_worker = &result["tiny"][0];

        // big and medium should be on DIFFERENT workers (size-aware separation)
        assert_ne!(big_worker, medium_worker,
            "10M and 5M shards should be on different workers");
        // tiny should be co-located with the bigger one to balance load
        // (5M+100 ≈ 5M vs 10M — best possible split)
        assert_eq!(tiny_worker, medium_worker,
            "tiny shard should be with medium, not with big: {:?}", result);
    }

    #[test]
    fn test_size_aware_fallback_to_count() {
        // Without sizes, should still work (fallback to count-based)
        let shards = vec!["s0".into(), "s1".into(), "s2".into()];
        let executors = vec!["w0".into(), "w1".into()];
        let policy = ShardPolicy { replica_factor: 1, max_shards_per_worker: 0 };

        let result = assign_shards_with_sizes(
            &shards, &executors, &policy, None, None);
        assert_eq!(result.len(), 3);
    }
}
