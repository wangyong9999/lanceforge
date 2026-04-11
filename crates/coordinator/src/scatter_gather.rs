// Licensed under the Apache License, Version 2.0.
// Coordinator Scatter-Gather: parallel dispatch to Workers + result merge.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, warn};
use tonic::Request;

use lance_distributed_common::ipc::ipc_to_record_batch;
use lance_distributed_proto::generated::lance_distributed as pb;
use crate::merge::{global_top_k_by_distance, global_top_k_by_score};
use lance_distributed_common::shard_state::ShardState;
use lance_distributed_common::shard_pruning::ShardPruner;

use super::connection_pool::ConnectionPool;

/// Extract shard-to-worker grouping logic for testability.
/// Returns Ok(worker_shards) or Err if too many shards have no healthy worker.
pub(crate) fn group_shards_by_worker(
    shard_routing: &[(String, String, Option<String>)],
    healthy_workers: &std::collections::HashSet<String>,
) -> Result<(HashMap<String, Vec<String>>, usize), String> {
    let mut worker_shards: HashMap<String, Vec<String>> = HashMap::new();
    let mut skipped = 0;

    for (shard, primary, secondary) in shard_routing {
        let chosen = if healthy_workers.contains(primary) {
            Some(primary.clone())
        } else if let Some(sec) = secondary {
            if healthy_workers.contains(sec) {
                Some(sec.clone())
            } else { None }
        } else { None };

        match chosen {
            Some(wid) => { worker_shards.entry(wid).or_default().push(shard.clone()); }
            None => { skipped += 1; }
        }
    }

    if worker_shards.is_empty() && !shard_routing.is_empty() {
        return Err("All workers unhealthy".to_string());
    }
    Ok((worker_shards, skipped))
}

/// Check failure threshold: >50% failures = error.
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
        return Ok(pb::SearchResponse { arrow_ipc_data: vec![], num_rows: 0, error: String::new() });
    }

    // Group shards by target worker (prefer primary, fallback to secondary)
    let mut worker_shards: HashMap<String, Vec<String>> = HashMap::new();
    let mut skipped = 0;

    for (shard, primary, secondary) in &shard_routing {
        let chosen = if pool.get_healthy_client(primary).await.is_ok() {
            Some(primary.clone())
        } else if let Some(sec) = secondary {
            if pool.get_healthy_client(sec).await.is_ok() {
                info!("Failover: shard {} → secondary {}", shard, sec);
                Some(sec.clone())
            } else { None }
        } else { None };

        match chosen {
            Some(wid) => { worker_shards.entry(wid).or_default().push(shard.clone()); }
            None => { skipped += 1; warn!("Shard {} has no healthy worker", shard); }
        }
    }

    if worker_shards.is_empty() {
        return Err(tonic::Status::unavailable(format!("All workers unhealthy for {}", table_name)));
    }

    debug!("Scatter: {} shards → {} workers ({} skipped)", shard_routing.len() - skipped, worker_shards.len(), skipped);

    // Scatter: parallel gRPC
    let mut handles = Vec::new();
    for (wid, _) in &worker_shards {
        if let Ok(mut client) = pool.get_healthy_client(wid).await {
            let req = local_req.clone();
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
                        Err(e) => { failures += 1; failure_details.push(format!("{}: IPC err", wid)); }
                    }
                }
            }
            Ok((wid, Ok(Err(status)))) => { failures += 1; failure_details.push(format!("{}: {}", wid, status.code())); }
            Ok((wid, Err(_))) => { failures += 1; failure_details.push(format!("{}: timeout", wid)); }
            Err(e) => { failures += 1; }
        }
    }

    if failures == total {
        return Err(tonic::Status::internal(format!("All {} workers failed: {}", total, failure_details.join("; "))));
    }
    if failures > 0 && failures * 2 > total {
        return Err(tonic::Status::internal(format!("{}/{} workers failed: {}", failures, total, failure_details.join("; "))));
    }

    if batches.is_empty() {
        return Ok(pb::SearchResponse { arrow_ipc_data: vec![], num_rows: 0, error: String::new() });
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
            Ok(pb::SearchResponse { arrow_ipc_data: ipc, num_rows: batch.num_rows() as u32, error: String::new() })
        }
        Err(e) => Ok(pb::SearchResponse { arrow_ipc_data: vec![], num_rows: 0, error: e.to_string() }),
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
        assert_eq!(groups.len(), 3);
        assert_eq!(groups["w0"], vec!["s0"]);
        assert_eq!(groups["w1"], vec!["s1"]);
        assert_eq!(groups["w2"], vec!["s2"]);
    }

    #[test]
    fn test_group_primary_down_failover() {
        // w0 is down — s0 should failover to w1
        let healthy: HashSet<String> = ["w1", "w2"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing_3shards(), &healthy).unwrap();
        assert_eq!(skipped, 0);
        assert!(groups["w1"].contains(&"s0".to_string())); // failover
        assert!(groups["w1"].contains(&"s1".to_string())); // primary
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
}
