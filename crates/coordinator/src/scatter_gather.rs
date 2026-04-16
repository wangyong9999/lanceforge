// Licensed under the Apache License, Version 2.0.
// Coordinator Scatter-Gather: parallel dispatch to Workers + result merge.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};
use tonic::Request;

use lance_distributed_common::ipc::ipc_to_record_batch;
use lance_distributed_proto::generated::lance_distributed as pb;
use crate::merge::{global_top_k_by_distance, global_top_k_by_score};
use lance_distributed_common::shard_state::ShardState;
use lance_distributed_common::shard_pruning::ShardPruner;

use super::connection_pool::ConnectionPool;

/// Extract shard-to-worker grouping logic for testability.
/// Load-balances across all healthy executors per shard.
#[allow(dead_code)]
pub(crate) fn group_shards_by_worker(
    shard_routing: &[(String, String, Option<String>)],
    healthy_workers: &std::collections::HashSet<String>,
) -> Result<(HashMap<String, Vec<String>>, usize), String> {
    static TEST_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    let mut worker_shards: HashMap<String, Vec<String>> = HashMap::new();
    let mut skipped = 0;

    for (shard, primary, secondary) in shard_routing {
        let mut healthy = Vec::new();
        if healthy_workers.contains(primary) {
            healthy.push(primary.clone());
        }
        if let Some(sec) = secondary
            && healthy_workers.contains(sec) {
                healthy.push(sec.clone());
            }

        if healthy.is_empty() {
            skipped += 1;
            continue;
        }

        let idx = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % healthy.len();
        worker_shards.entry(healthy[idx].clone()).or_default().push(shard.clone());
    }

    if worker_shards.is_empty() && !shard_routing.is_empty() {
        return Err("All workers unhealthy".to_string());
    }
    Ok((worker_shards, skipped))
}

/// Check failure threshold: >50% failures = error.
#[allow(dead_code)]
pub(crate) fn check_failure_threshold(failures: usize, total: usize) -> Result<(), String> {
    if failures == total && total > 0 {
        return Err(format!("All {} workers failed", total));
    }
    if failures > 0 && failures * 2 > total {
        return Err(format!("{}/{} workers failed (>50%)", failures, total));
    }
    Ok(())
}

/// Execute a Scatter-Gather query with shard-level routing and failover.
pub async fn scatter_gather(
    pool: &Arc<ConnectionPool>,
    shard_state: &Arc<dyn ShardState>,
    pruner: &Arc<ShardPruner>,
    table_name: &str,
    local_req: pb::LocalSearchRequest,
    k: u32,
    merge_by_distance: bool,
    query_timeout: Duration,
) -> Result<pb::SearchResponse, tonic::Status> {
    // Enforce global timeout on entire scatter-gather (routing + dispatch + merge)
    match tokio::time::timeout(query_timeout, scatter_gather_inner(
        pool, shard_state, pruner, table_name, local_req, k, merge_by_distance, query_timeout,
    )).await {
        Ok(result) => result,
        Err(_) => Err(tonic::Status::deadline_exceeded(
            format!("scatter_gather timed out after {}s", query_timeout.as_secs())
        )),
    }
}

async fn scatter_gather_inner(
    pool: &Arc<ConnectionPool>,
    shard_state: &Arc<dyn ShardState>,
    pruner: &Arc<ShardPruner>,
    table_name: &str,
    local_req: pb::LocalSearchRequest,
    k: u32,
    merge_by_distance: bool,
    query_timeout: Duration,
) -> Result<pb::SearchResponse, tonic::Status> {
    let filter = local_req.filter.as_deref();

    // Get shard routing with pruning
    let shard_routing = {
        let routing = shard_state.get_shard_routing(table_name).await;
        if routing.is_empty() {
            return Err(tonic::Status::not_found(format!("Table not found: {table_name}")));
        }
        let all_shard_names: Vec<String> = routing.iter().map(|(s, _, _)| s.clone()).collect();
        let eligible = pruner.prune(&all_shard_names, filter);
        routing.into_iter().filter(|(s, _, _)| eligible.contains(s)).collect::<Vec<_>>()
    };

    if shard_routing.is_empty() {
        return Ok(pb::SearchResponse { arrow_ipc_data: vec![], num_rows: 0, error: String::new(), truncated: false, next_offset: 0, latency_ms: 0 });
    }

    // Snapshot healthy worker set ONCE (avoids 2N serial RwLock acquisitions
    // in the routing loop — was the biggest hot-path bottleneck).
    let healthy_set: std::collections::HashSet<String> = {
        let statuses = pool.worker_statuses().await;
        statuses.into_iter()
            .filter(|(_, _, _, healthy, _, _, _)| *healthy)
            .map(|(id, _, _, _, _, _, _)| id)
            .collect()
    };

    static READ_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    let mut worker_shards: HashMap<String, Vec<String>> = HashMap::new();
    let mut skipped = 0;

    for (shard, primary, secondary) in &shard_routing {
        let mut healthy_executors = Vec::new();
        if healthy_set.contains(primary) {
            healthy_executors.push(primary.clone());
        }
        if let Some(sec) = secondary
            && healthy_set.contains(sec) {
                healthy_executors.push(sec.clone());
            }

        if healthy_executors.is_empty() {
            skipped += 1;
            warn!("Shard {} has no healthy worker", shard);
            continue;
        }

        let idx = READ_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % healthy_executors.len();
        worker_shards.entry(healthy_executors[idx].clone()).or_default().push(shard.clone());
    }

    if worker_shards.is_empty() {
        return Err(tonic::Status::unavailable(format!("All workers unhealthy for {}", table_name)));
    }

    debug!("Scatter: {} shards → {} workers ({} skipped)", shard_routing.len() - skipped, worker_shards.len(), skipped);

    // Scatter: parallel gRPC. Wrap request in Arc to avoid cloning the
    // query vector (up to 6KB for 1536d) for each worker.
    let shared_req = Arc::new(local_req);
    let mut handles = Vec::new();
    for wid in worker_shards.keys() {
        if let Ok(mut client) = pool.get_healthy_client(wid).await {
            let req = (*shared_req).clone();
            let worker_id = wid.clone();
            let timeout = query_timeout;
            handles.push(tokio::spawn(async move {
                let result = tokio::time::timeout(timeout, client.execute_local_search(Request::new(req))).await;
                (worker_id, result)
            }));
        }
    }

    // Gather + failure tracking
    let mut batches = Vec::new();
    let total = handles.len();
    let mut failures = 0;
    let mut failure_details = Vec::new();

    for handle in handles {
        match handle.await {
            Ok((wid, Ok(Ok(response)))) => {
                let resp = response.into_inner();
                if !resp.error.is_empty() {
                    failures += 1; failure_details.push(format!("{}: {}", wid, resp.error));
                } else if resp.num_rows > 0 {
                    match ipc_to_record_batch(&resp.arrow_ipc_data) {
                        Ok(batch) => batches.push(batch),
                        Err(_e) => { failures += 1; failure_details.push(format!("{}: IPC err", wid)); }
                    }
                }
            }
            Ok((wid, Ok(Err(status)))) => { failures += 1; failure_details.push(format!("{}: {}", wid, status.code())); }
            Ok((wid, Err(_))) => { failures += 1; failure_details.push(format!("{}: timeout", wid)); }
            Err(_e) => { failures += 1; }
        }
    }

    if failures == total && total > 0 {
        return Err(tonic::Status::internal(format!("All {} workers failed: {}", total, failure_details.join("; "))));
    }

    // Partial failure: return results from healthy workers with warning.
    // Previous behavior: >50% failures = error. New: always return what we have,
    // set error field as warning so client knows results may be incomplete.
    let partial_warning = if failures > 0 {
        let msg = format!("partial: {}/{} workers failed ({})", failures, total, failure_details.join("; "));
        warn!("{}", msg);
        msg
    } else {
        String::new()
    };

    if batches.is_empty() {
        return Ok(pb::SearchResponse { arrow_ipc_data: vec![], num_rows: 0, error: partial_warning, truncated: false, next_offset: 0, latency_ms: 0 });
    }

    // Merge
    let merged = if merge_by_distance {
        global_top_k_by_distance(batches, k as usize)
    } else {
        global_top_k_by_score(batches, k as usize)
    };

    match merged {
        Ok(batch) => {
            let ipc = lance_distributed_common::ipc::record_batch_to_ipc(&batch)
                .map_err(|e| tonic::Status::internal(format!("IPC error: {e}")))?;
            Ok(pb::SearchResponse { arrow_ipc_data: ipc, num_rows: batch.num_rows() as u32, error: partial_warning, truncated: false, next_offset: 0, latency_ms: 0 })
        }
        Err(e) => Ok(pb::SearchResponse { arrow_ipc_data: vec![], num_rows: 0, error: e.to_string(), truncated: false, next_offset: 0, latency_ms: 0 }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn routing_3shards() -> Vec<(String, String, Option<String>)> {
        vec![
            ("s0".into(), "w0".into(), Some("w1".into())),
            ("s1".into(), "w1".into(), Some("w2".into())),
            ("s2".into(), "w2".into(), None),
        ]
    }

    #[test]
    fn test_group_all_healthy() {
        let healthy: HashSet<String> = ["w0", "w1", "w2"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing_3shards(), &healthy).unwrap();
        assert_eq!(skipped, 0);
        // All 3 shards should be assigned (may be load-balanced across replicas)
        let total_shards: usize = groups.values().map(|v| v.len()).sum();
        assert_eq!(total_shards, 3, "All 3 shards should be assigned");
    }

    #[test]
    fn test_group_primary_down_failover() {
        // w0 is down — s0 should failover to w1 (only healthy replica)
        let healthy: HashSet<String> = ["w1", "w2"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing_3shards(), &healthy).unwrap();
        assert_eq!(skipped, 0);
        // s0: primary w0 is down, failover to secondary w1
        assert!(groups["w1"].contains(&"s0".to_string()), "s0 should failover to w1");
        // s1: primary w1, secondary w2 — load-balanced to either
        let s1_assigned = groups.values().any(|v| v.contains(&"s1".to_string()));
        assert!(s1_assigned, "s1 should be assigned to w1 or w2");
    }

    #[test]
    fn test_group_no_secondary_skipped() {
        // w2 is down, s2 has no secondary → skipped
        let healthy: HashSet<String> = ["w0", "w1"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing_3shards(), &healthy).unwrap();
        assert_eq!(skipped, 1); // s2 skipped
        assert!(!groups.contains_key("w2"));
    }

    #[test]
    fn test_group_all_down() {
        let healthy: HashSet<String> = HashSet::new();
        let result = group_shards_by_worker(&routing_3shards(), &healthy);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unhealthy"));
    }

    #[test]
    fn test_group_empty_routing() {
        let healthy: HashSet<String> = ["w0"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&[], &healthy).unwrap();
        assert_eq!(groups.len(), 0);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_failure_threshold_none() {
        assert!(check_failure_threshold(0, 3).is_ok());
    }

    #[test]
    fn test_failure_threshold_minority() {
        // 1/3 failed → ok (<=50%)
        assert!(check_failure_threshold(1, 3).is_ok());
    }

    #[test]
    fn test_failure_threshold_majority() {
        // 2/3 failed → error (>50%)
        let err = check_failure_threshold(2, 3).unwrap_err();
        assert!(err.contains("2/3"));
    }

    #[test]
    fn test_failure_threshold_all() {
        let err = check_failure_threshold(3, 3).unwrap_err();
        assert!(err.contains("All 3"));
    }

    #[test]
    fn test_failure_threshold_half_ok() {
        // 1/2 = 50% exactly, but >50% check means this is ok
        assert!(check_failure_threshold(1, 2).is_ok());
    }

    #[test]
    fn test_failure_threshold_zero_total() {
        assert!(check_failure_threshold(0, 0).is_ok());
    }

    #[test]
    fn test_load_balance_across_replicas() {
        // Both w0 and w1 are healthy, s0 has both as executors
        let routing = vec![
            ("s0".into(), "w0".into(), Some("w1".into())),
        ];
        let healthy: HashSet<String> = ["w0", "w1"].iter().map(|s| s.to_string()).collect();

        // Run multiple times — should distribute across w0 and w1
        let mut w0_count = 0;
        let mut w1_count = 0;
        for _ in 0..100 {
            let (groups, _) = group_shards_by_worker(&routing, &healthy).unwrap();
            if groups.contains_key("w0") { w0_count += 1; }
            if groups.contains_key("w1") { w1_count += 1; }
        }
        // Both should get some traffic (load-balanced)
        assert!(w0_count > 20, "w0 should get >20% traffic, got {}", w0_count);
        assert!(w1_count > 20, "w1 should get >20% traffic, got {}", w1_count);
    }

    #[test]
    fn test_replica_failover_when_one_down() {
        // s0 has w0 (primary) and w1 (secondary), but w0 is down
        let routing = vec![
            ("s0".into(), "w0".into(), Some("w1".into())),
        ];
        let healthy: HashSet<String> = ["w1"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing, &healthy).unwrap();
        assert_eq!(skipped, 0);
        assert!(groups.contains_key("w1"), "Should failover to w1");
        assert!(!groups.contains_key("w0"), "w0 is down");
    }
}
