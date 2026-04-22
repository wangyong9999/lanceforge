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
///
/// `trace_id` — optional W3C trace_id extracted from the client's
/// `traceparent` metadata. When set, every outbound worker RPC is
/// re-decorated with `traceparent: 00-<trace_id>-<new-span>-01` so
/// worker logs carry the same trace_id operators saw in the coord
/// audit line (B1). No-op when None.
pub async fn scatter_gather(
    pool: &Arc<ConnectionPool>,
    shard_state: &Arc<dyn ShardState>,
    pruner: &Arc<ShardPruner>,
    table_name: &str,
    local_req: pb::LocalSearchRequest,
    k: u32,
    merge_by_distance: bool,
    query_timeout: Duration,
    trace_id: Option<String>,
    on_columns: &[String],
) -> Result<pb::SearchResponse, tonic::Status> {
    // Enforce global timeout on entire scatter-gather (routing + dispatch + merge)
    match tokio::time::timeout(query_timeout, scatter_gather_inner(
        pool, shard_state, pruner, table_name, local_req, k, merge_by_distance, query_timeout, trace_id, on_columns,
    )).await {
        Ok(result) => result,
        Err(_) => Err(tonic::Status::deadline_exceeded(
            format!("scatter_gather timed out after {}s", query_timeout.as_secs())
        )),
    }
}

/// Attach a `traceparent` metadata header to an outbound Request. Caller
/// supplies the 32-hex trace_id (from the incoming traceparent); we
/// mint a fresh 16-hex span_id per outbound RPC so Jaeger/Tempo can
/// thread spans even though we're not yet emitting spans ourselves
/// (0.3 OTLP work — for now the value is cross-process log grep).
pub fn inject_traceparent<T>(mut req: Request<T>, trace_id: &str) -> Request<T> {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Cheap 64-bit randomish span_id from nanos; uniqueness across
    // fan-out isn't cryptographic, only enough that log grep can
    // distinguish the N per-shard calls of a single scatter.
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let span_id = format!("{:016x}", nanos);
    let tp = format!("00-{trace_id}-{span_id}-01");
    if let Ok(val) = tonic::metadata::MetadataValue::try_from(tp) {
        req.metadata_mut().insert("traceparent", val);
    }
    req
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
    trace_id: Option<String>,
    on_columns: &[String],
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
        let mut pruned: Vec<_> = routing.into_iter()
            .filter(|(s, _, _)| eligible.contains(s))
            .collect();

        // #5.1 Partition pruning: if on_columns is declared AND the
        // filter is a single-column equality on a hash key, route to
        // exactly one shard. The shard index convention matches
        // `partition_batch_by_hash` in service.rs — shards are named
        // `{safe_stem}_shard_{idx:02}` (CreateTable) or listed in
        // config order for static tables; we match by the `_shard_NN`
        // suffix. Unrecognised names → fall through to full scatter.
        let num_shards = pruned.len();
        if let Some(target_idx) = lance_distributed_common::shard_pruning::hash_route_shard(
            filter, on_columns, num_shards,
        ) {
            let suffix = format!("_shard_{:02}", target_idx);
            let before = pruned.len();
            let retained: Vec<_> = pruned
                .iter()
                .filter(|(name, _, _)| name.ends_with(&suffix))
                .cloned()
                .collect();
            if !retained.is_empty() {
                debug!(
                    "Partition pruning: {} → 1 shard (idx {}, suffix {}) for {}",
                    before, target_idx, suffix, table_name
                );
                pruned = retained;
            } else {
                // Shard naming didn't match the expected convention —
                // fall back to full scatter rather than miss data.
                log::warn!(
                    "Partition pruning: target idx {} (suffix {}) not found \
                     in routing for {}, falling back to full scatter",
                    target_idx, suffix, table_name
                );
            }
        }
        pruned
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

        // B2.2: role-aware selection — reads prefer ReadPrimary > Either >
        // WritePrimary, so a mixed-RW workload can isolate a write-primary
        // worker and keep query traffic off of it. Falls back to round-
        // robin when all candidates tie (the 0.1.x default where every
        // worker is Either).
        let idx = match pool.pick_by_role(&healthy_executors, /*for_reads=*/ true).await {
            Some(i) => {
                // If best role ties across multiple candidates, round-robin
                // amongst them to preserve load balance. We detect a tie by
                // checking whether another candidate shares the winner's role.
                let winner_role = pool.role_of(&healthy_executors[i]).await;
                let mut tied: Vec<usize> = Vec::new();
                for (j, wid) in healthy_executors.iter().enumerate() {
                    if pool.role_of(wid).await == winner_role {
                        tied.push(j);
                    }
                }
                if tied.len() > 1 {
                    let rr = READ_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % tied.len();
                    tied[rr]
                } else {
                    i
                }
            }
            None => 0,
        };
        worker_shards.entry(healthy_executors[idx].clone()).or_default().push(shard.clone());
    }

    if worker_shards.is_empty() {
        return Err(tonic::Status::unavailable(format!("All workers unhealthy for {}", table_name)));
    }

    debug!("Scatter: {} shards → {} workers ({} skipped)", shard_routing.len() - skipped, worker_shards.len(), skipped);

    // Scatter: parallel gRPC via JoinSet (automatically aborts tasks when
    // dropped — e.g., on client disconnect or timeout, preventing wasted compute).
    let shared_req = Arc::new(local_req);
    let mut tasks = tokio::task::JoinSet::new();
    for wid in worker_shards.keys() {
        if let Ok(mut client) = pool.get_healthy_client(wid).await {
            let req = (*shared_req).clone();
            let worker_id = wid.clone();
            let timeout = query_timeout;
            // B1: propagate traceparent so worker-side logs carry the
            // same trace_id as the coord audit line. Zero-cost when the
            // client didn't send a traceparent.
            let tid = trace_id.clone();
            // H13: per-shard latency timing. Each task reports its own wall
            // clock so operators debugging a slow query can see which
            // worker is the outlier instead of only seeing the aggregate.
            tasks.spawn(async move {
                let start = std::time::Instant::now();
                let mut outbound = Request::new(req);
                if let Some(tid) = tid.as_deref() {
                    outbound = inject_traceparent(outbound, tid);
                }
                let result = tokio::time::timeout(timeout, client.execute_local_search(outbound)).await;
                let per_worker_ms = start.elapsed().as_millis() as u64;
                (worker_id, result, per_worker_ms)
            });
        }
    }

    // Gather + failure tracking
    let mut batches = Vec::new();
    let total = tasks.len();
    let mut failures = 0;
    let mut failure_details = Vec::new();

    // H13: collect per-shard latencies so slow-query logs can pinpoint
    // the outlier worker. Populated in the gather loop below.
    let mut per_worker_latencies: Vec<(String, u64)> = Vec::with_capacity(total);

    while let Some(handle) = tasks.join_next().await {
        match handle {
            Ok((wid, Ok(Ok(response)), per_ms)) => {
                per_worker_latencies.push((wid.clone(), per_ms));
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
            Ok((wid, Ok(Err(status)), per_ms)) => {
                per_worker_latencies.push((wid.clone(), per_ms));
                failures += 1;
                failure_details.push(format!("{}: {}", wid, status.code()));
            }
            Ok((wid, Err(_), per_ms)) => {
                per_worker_latencies.push((wid.clone(), per_ms));
                failures += 1;
                failure_details.push(format!("{}: timeout", wid));
            }
            Err(_e) => { failures += 1; }
        }
    }

    // H13: emit per-shard breakdown at debug level on every query, and
    // warn level whenever the max worker latency exceeds 2× the median
    // (the usual "straggler" pattern). debug! keeps the default-logging
    // production build quiet; operators bump RUST_LOG when they're
    // debugging a slow-query incident.
    if !per_worker_latencies.is_empty() {
        debug!(
            "Scatter {} per-shard latencies (ms): {:?}",
            table_name, per_worker_latencies
        );
        let max_ms = per_worker_latencies.iter().map(|(_, m)| *m).max().unwrap_or(0);
        let mut sorted: Vec<u64> = per_worker_latencies.iter().map(|(_, m)| *m).collect();
        sorted.sort();
        let median = sorted[sorted.len() / 2];
        if median > 0 && max_ms > median.saturating_mul(2) {
            warn!(
                "Scatter {} straggler: max={}ms median={}ms detail={:?}",
                table_name, max_ms, median, per_worker_latencies
            );
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

    // ── H6: scatter_gather error-branch lib coverage (no live workers) ──

    #[test]
    fn test_group_mixed_healthy_unhealthy() {
        // w0 down, w1 up (secondary for s0, primary for s1);
        // w2 up (only secondary for s1, primary for s2).
        // Expected: s0 → w1, s1 → w1 or w2, s2 → w2.
        let routing = routing_3shards();
        let healthy: HashSet<String> = ["w1", "w2"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing, &healthy).unwrap();
        assert_eq!(skipped, 0, "every shard has at least one healthy candidate");
        let total: usize = groups.values().map(|v| v.len()).sum();
        assert_eq!(total, 3);
        assert!(!groups.contains_key("w0"), "w0 is unhealthy, must not receive shards");
    }

    #[test]
    fn test_group_empty_routing_returns_ok_zero_skipped() {
        // Zero-shard routing (e.g. after full pruning): we return Ok with
        // empty groups and 0 skipped, not an error.
        let routing: Vec<(String, String, Option<String>)> = vec![];
        let healthy: HashSet<String> = ["w0"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing, &healthy).unwrap();
        assert!(groups.is_empty());
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_group_single_shard_single_replica_missing() {
        // One shard, primary down, no secondary → skipped.
        let routing = vec![("s0".into(), "w0".into(), None)];
        let healthy: HashSet<String> = HashSet::new();
        let err = group_shards_by_worker(&routing, &healthy).unwrap_err();
        assert!(err.contains("unhealthy"), "got: {err}");
    }

    #[test]
    fn test_group_secondary_only_healthy() {
        // Primary w0 down, secondary w1 healthy — s0 must go to w1.
        let routing = vec![("s0".into(), "w0".into(), Some("w1".into()))];
        let healthy: HashSet<String> = ["w1"].iter().map(|s| s.to_string()).collect();
        let (groups, skipped) = group_shards_by_worker(&routing, &healthy).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(groups.get("w1").map(|v| v.len()), Some(1));
        assert!(!groups.contains_key("w0"));
    }

    #[test]
    fn test_failure_threshold_single_failure_half_rounded() {
        // 1/2 failures — "failures*2 > total" condition is > not >=
        // so 1/2 is NOT a failure (tie goes to success).
        assert!(check_failure_threshold(1, 2).is_ok(),
                "exactly half failed should not error (strict >50% rule)");
    }

    #[test]
    fn test_failure_threshold_large_numbers() {
        // Guard the arithmetic for realistic cluster sizes.
        assert!(check_failure_threshold(5, 100).is_ok());
        assert!(check_failure_threshold(50, 100).is_ok(), "exactly 50% OK");
        assert!(check_failure_threshold(51, 100).is_err(), "51% fails");
    }

    #[test]
    fn test_group_three_way_replica() {
        // Some shards have extended replica (replica_factor=3 would surface
        // that via secondary + another spot — but our Option<String> model
        // only captures 2. This test pins the 2-replica ceiling behaviour.)
        let routing = vec![("s0".into(), "w0".into(), Some("w1".into()))];
        let healthy: HashSet<String> = ["w0", "w1", "w2"].iter().map(|s| s.to_string()).collect();
        let (groups, _) = group_shards_by_worker(&routing, &healthy).unwrap();
        let total: usize = groups.values().map(|v| v.len()).sum();
        assert_eq!(total, 1, "single shard routes to exactly one worker");
        assert!(!groups.contains_key("w2"),
                "w2 is healthy but not in this shard's replica set, must not receive");
    }

    // ── beta.5: end-to-end scatter_gather coverage with mock workers ──
    //
    // These tests exercise the async / gRPC paths that the helper-function
    // tests above cannot reach: the timeout wrapper, the healthy-pool
    // snapshot, task fan-out + gather, IPC decode, per-shard latency
    // logging, merge-by-distance vs merge-by-score branches, and the
    // partial-failure warning path.

    use crate::test_support::{
        spawn_mock_worker, HealthBehavior, MockState, MockWorkerHandle, SearchBehavior,
    };
    use lance_distributed_common::config::{
        ClusterConfig, ExecutorConfig, ShardConfig, TableConfig,
    };
    use lance_distributed_common::shard_pruning::ShardPruner;
    use lance_distributed_common::shard_state::StaticShardState;
    use super::super::connection_pool::ConnectionPool;
    use std::sync::Arc;
    use std::time::Duration;

    /// Build a running mock worker with the given behavior on a free port.
    async fn spawn_worker(
        search: SearchBehavior,
        health: HealthBehavior,
    ) -> MockWorkerHandle {
        let state = MockState::new(search, health);
        spawn_mock_worker(state).await
    }

    /// Build a ConnectionPool pointed at the given worker ports and wait
    /// until `connect_all` has populated the map. Uses fast health-check
    /// config so tests run quickly.
    async fn build_pool(
        workers: &[(&str, u16)],
    ) -> Arc<ConnectionPool> {
        let mut endpoints = HashMap::new();
        for (id, port) in workers {
            endpoints.insert(id.to_string(), ("127.0.0.1".to_string(), *port));
        }
        use lance_distributed_common::config::{HealthCheckConfig, ServerConfig};
        let hc = HealthCheckConfig {
            interval_secs: 1,
            unhealthy_threshold: 2,
            remove_threshold: 5,
            refresh_interval_secs: 1,
            reconciliation_interval_secs: 1,
            connect_timeout_secs: 1,
            ping_timeout_secs: 1,
        };
        let sv = ServerConfig::default();
        let pool = Arc::new(ConnectionPool::new(
            endpoints,
            Duration::from_secs(5),
            &hc,
            &sv,
        ));
        // Drive an immediate connect via start_background. The loop also
        // begins issuing health checks, which is what we want for the
        // connection_pool tests; scatter_gather tests just need the
        // initial connect.
        pool.start_background(None);
        // Poll briefly until every configured worker shows healthy.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let statuses = pool.worker_statuses().await;
            if !statuses.is_empty() && statuses.iter().all(|s| s.3) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        pool
    }

    /// Build a StaticShardState with single-table, N-shard, 1-primary-each
    /// routing. `workers` is the ordered list of (id, port); shard i is
    /// assigned to worker i.
    fn build_shard_state(
        table: &str,
        workers: &[(&str, u16)],
    ) -> Arc<dyn lance_distributed_common::shard_state::ShardState> {
        let mut shards = Vec::new();
        for (i, (wid, _)) in workers.iter().enumerate() {
            shards.push(ShardConfig {
                name: format!("s{}", i),
                uri: format!("/tmp/mock-s{}", i),
                executors: vec![wid.to_string()],
            });
        }
        let executors = workers.iter().map(|(id, port)| ExecutorConfig {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port: *port,
            role: Default::default(),
        }).collect();
        let cfg = ClusterConfig {
            tables: vec![TableConfig { name: table.to_string(), shards }],
            executors,
            ..Default::default()
        };
        Arc::new(StaticShardState::from_config(&cfg))
    }

    fn empty_request() -> pb::LocalSearchRequest {
        pb::LocalSearchRequest {
            query_type: 0,
            table_name: "t".to_string(),
            k: 10,
            ..Default::default()
        }
    }

    /// Build a ShardState whose shard names follow the CreateTable
    /// convention `{table}_shard_{idx:02}`, required by partition
    /// pruning to match shard_index → shard_name. Regular tests use
    /// `build_shard_state` which produces s0/s1/... and therefore
    /// exercises the fall-back full-scatter path.
    fn build_pruning_shard_state(
        table: &str,
        workers: &[(&str, u16)],
    ) -> Arc<dyn lance_distributed_common::shard_state::ShardState> {
        use lance_distributed_common::config::{
            ClusterConfig, ExecutorConfig, ShardConfig, TableConfig,
        };
        let mut shards = Vec::new();
        for (i, (wid, _)) in workers.iter().enumerate() {
            shards.push(ShardConfig {
                name: format!("{}_shard_{:02}", table, i),
                uri: format!("/tmp/prune-shard-{}", i),
                executors: vec![wid.to_string()],
            });
        }
        let executors = workers.iter().map(|(id, port)| ExecutorConfig {
            id: id.to_string(),
            host: "127.0.0.1".to_string(),
            port: *port,
            role: Default::default(),
        }).collect();
        let cfg = ClusterConfig {
            tables: vec![TableConfig { name: table.to_string(), shards }],
            executors,
            ..Default::default()
        };
        Arc::new(StaticShardState::from_config(&cfg))
    }

    #[tokio::test]
    async fn partition_pruning_targets_single_shard() {
        // Four workers, table with on_columns=["id"], filter `id = 'abc'`.
        // Exactly one worker should receive the search RPC; the other
        // three should see zero calls.
        let _ = env_logger::builder().is_test(true).try_init();
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let w1 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let w2 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let w3 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let workers = [
            ("w0", w0.port), ("w1", w1.port), ("w2", w2.port), ("w3", w3.port),
        ];
        let pool = build_pool(&workers).await;
        let state = build_pruning_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let mut req = empty_request();
        req.filter = Some("id = 'abc'".to_string());
        let resp = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            req, 5, true, Duration::from_secs(2), None,
            &["id".to_string()],
        ).await.unwrap();

        assert!(resp.error.is_empty(), "unexpected partial warning: {}", resp.error);

        // Authoritative target index from the shared helper.
        let target_idx = lance_distributed_common::shard_pruning::hash_route_shard(
            Some("id = 'abc'"), &["id".to_string()], 4,
        ).expect("hash route should resolve");
        let handles = [&w0, &w1, &w2, &w3];
        let total_calls: u64 = handles
            .iter()
            .map(|h| h.state.search_calls.load(std::sync::atomic::Ordering::SeqCst))
            .sum();
        assert_eq!(total_calls, 1, "only the target shard should have been called");
        assert_eq!(
            handles[target_idx]
                .state
                .search_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "target shard (idx {target_idx}) must receive the one call"
        );
    }

    #[tokio::test]
    async fn partition_pruning_falls_back_when_filter_missing() {
        // Same setup as the pruning test, but no filter — all shards
        // should receive a call (full scatter, zero regression).
        let _ = env_logger::builder().is_test(true).try_init();
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let w1 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let workers = [("w0", w0.port), ("w1", w1.port)];
        let pool = build_pool(&workers).await;
        let state = build_pruning_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        // No filter → on_columns doesn't help, full scatter.
        let _ = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true, Duration::from_secs(2), None,
            &["id".to_string()],
        ).await.unwrap();

        assert_eq!(w0.state.search_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(w1.state.search_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn partition_pruning_falls_back_when_on_columns_empty() {
        // Filter present but no on_columns declared — this is the pre-
        // 5.1 behaviour and must be preserved exactly.
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let w1 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec![] },
        ).await;
        let workers = [("w0", w0.port), ("w1", w1.port)];
        let pool = build_pool(&workers).await;
        let state = build_pruning_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let mut req = empty_request();
        req.filter = Some("id = 'abc'".to_string());
        let _ = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            req, 5, true, Duration::from_secs(2), None,
            &[], // empty on_columns — no pruning
        ).await.unwrap();

        assert_eq!(w0.state.search_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(w1.state.search_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_scatter_gather_happy_path_merge_by_distance() {
        let _ = env_logger::builder().is_test(true).try_init();
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 3 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 3, shard_names: vec!["s0".into()] },
        ).await;
        let w1 = spawn_worker(
            SearchBehavior::Ok { rows: 3 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 3, shard_names: vec!["s1".into()] },
        ).await;
        let workers = [("w0", w0.port), ("w1", w1.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let resp = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5,
            /*merge_by_distance=*/ true,
            Duration::from_secs(5),
            None,
            &[],
        ).await.unwrap();

        assert!(resp.error.is_empty(), "no partial warning expected: {}", resp.error);
        assert!(resp.num_rows > 0);
    }

    #[tokio::test]
    async fn test_scatter_gather_happy_path_merge_by_score() {
        let _ = env_logger::builder().is_test(true).try_init();
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 3 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 3, shard_names: vec!["s0".into()] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let resp = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5,
            /*merge_by_distance=*/ false,
            Duration::from_secs(5),
            None,
            &[],
        ).await.unwrap();

        assert!(resp.error.is_empty());
        assert!(resp.num_rows > 0);
    }

    #[tokio::test]
    async fn test_scatter_gather_table_not_found() {
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 0, total_rows: 0, shard_names: vec![] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let err = scatter_gather_inner(
            &pool, &state, &pruner, "does-not-exist",
            empty_request(), 5, true,
            Duration::from_secs(2), None,
            &[],
        ).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_scatter_gather_all_workers_failed_returns_internal() {
        let w0 = spawn_worker(
            SearchBehavior::GrpcError("boom".into()),
            HealthBehavior::Ok { loaded_shards: 0, total_rows: 0, shard_names: vec![] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let err = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(2), None,
            &[],
        ).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("workers failed"));
    }

    #[tokio::test]
    async fn test_scatter_gather_partial_failure_warns_keeps_data() {
        let _ = env_logger::builder().is_test(true).try_init();
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 2 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 2, shard_names: vec!["s0".into()] },
        ).await;
        let w1 = spawn_worker(
            SearchBehavior::GrpcError("kaboom".into()),
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 0, shard_names: vec!["s1".into()] },
        ).await;
        let workers = [("w0", w0.port), ("w1", w1.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let resp = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(2), None,
            &[],
        ).await.unwrap();

        assert!(!resp.error.is_empty(), "partial warning expected");
        assert!(resp.error.contains("partial"));
        assert!(resp.num_rows > 0, "should still return data from w0");
    }

    #[tokio::test]
    async fn test_scatter_gather_app_error_counts_as_failure() {
        let w0 = spawn_worker(
            SearchBehavior::AppError("ann unavailable".into()),
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 0, shard_names: vec!["s0".into()] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let err = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(2), None,
            &[],
        ).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn test_scatter_gather_ipc_decode_error_counts_as_failure() {
        let w0 = spawn_worker(
            SearchBehavior::GarbageIpc,
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 0, shard_names: vec!["s0".into()] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let err = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(2), None,
            &[],
        ).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("IPC err"));
    }

    #[tokio::test]
    async fn test_scatter_gather_empty_responses_return_empty_ok() {
        let w0 = spawn_worker(
            SearchBehavior::Empty,
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 0, shard_names: vec!["s0".into()] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let resp = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(2), None,
            &[],
        ).await.unwrap();

        assert_eq!(resp.num_rows, 0);
        assert!(resp.error.is_empty(), "no failures, no warning");
    }

    #[tokio::test]
    async fn test_scatter_gather_outer_timeout_wraps_inner() {
        // The outer `scatter_gather` wraps a 0-timeout around a slow worker.
        // Slow worker delay > timeout → deadline_exceeded even though the
        // inner task would eventually succeed.
        let w0 = spawn_worker(
            SearchBehavior::Slow(Duration::from_secs(5)),
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 0, shard_names: vec!["s0".into()] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let err = scatter_gather(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_millis(100),
            None,
            &[],
        ).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    }

    #[tokio::test]
    async fn test_scatter_gather_trace_id_propagates_via_metadata() {
        // Mock doesn't introspect metadata, but the inject_traceparent
        // branch is still exercised. This test pins the happy path when
        // trace_id is present.
        let w0 = spawn_worker(
            SearchBehavior::Ok { rows: 2 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 2, shard_names: vec!["s0".into()] },
        ).await;
        let workers = [("w0", w0.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let resp = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(2),
            Some("0123456789abcdef0123456789abcdef".to_string()),
            &[],
        ).await.unwrap();

        assert!(resp.num_rows > 0);
        assert!(w0.state.search_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn test_scatter_gather_all_workers_unhealthy_returns_unavailable() {
        // Build a shard state referencing workers that were never spawned:
        // the pool never succeeds in connecting, so they stay unhealthy;
        // every shard gets skipped and worker_shards ends empty.
        let endpoints: HashMap<String, (String, u16)> = vec![
            ("w0".to_string(), ("127.0.0.1".to_string(), 1u16)),
            ("w1".to_string(), ("127.0.0.1".to_string(), 2u16)),
        ].into_iter().collect();
        use lance_distributed_common::config::{HealthCheckConfig, ServerConfig};
        let hc = HealthCheckConfig { interval_secs: 60, ping_timeout_secs: 1, connect_timeout_secs: 1, ..Default::default() };
        let pool = Arc::new(ConnectionPool::new(
            endpoints, Duration::from_secs(2), &hc, &ServerConfig::default(),
        ));
        // No start_background — workers never become healthy.
        let workers = [("w0", 1u16), ("w1", 2u16)];
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let err = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(2), None,
            &[],
        ).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn test_scatter_gather_straggler_triggers_warn_branch() {
        // Two workers: one fast, one slow (but within budget). The max
        // latency exceeds 2× median → straggler warn path hit.
        let w_fast = spawn_worker(
            SearchBehavior::Ok { rows: 2 },
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 2, shard_names: vec!["s0".into()] },
        ).await;
        let w_slow = spawn_worker(
            SearchBehavior::Slow(Duration::from_millis(250)),
            HealthBehavior::Ok { loaded_shards: 1, total_rows: 1, shard_names: vec!["s1".into()] },
        ).await;
        let workers = [("w0", w_fast.port), ("w1", w_slow.port)];
        let pool = build_pool(&workers).await;
        let state = build_shard_state("t", &workers);
        let pruner = Arc::new(ShardPruner::empty());

        let resp = scatter_gather_inner(
            &pool, &state, &pruner, "t",
            empty_request(), 5, true,
            Duration::from_secs(3), None,
            &[],
        ).await.unwrap();

        assert!(resp.num_rows > 0);
    }
}
