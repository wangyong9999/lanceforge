// Licensed under the Apache License, Version 2.0.
//
// Phase 2: coord-side fragment fanout for single-dataset tables.
//
// When a table maps to exactly one Lance dataset (one ShardConfig
// entry in the routing), the coord:
//   1. enumerates the dataset's fragments
//   2. HRW-routes fragment subsets to healthy workers
//   3. fan-out each subset via LocalSearchRequest with
//      fragment_ids + shard_uri (phase 1 fungible path)
//   4. merges global top-K via the existing merge utility
//
// Multi-shard tables keep the legacy scatter_gather path. This file
// is net-additive — no existing behaviour changes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use log::{debug, warn};
use tokio::sync::RwLock;
use tonic::{Request, Status};

use crate::connection_pool::ConnectionPool;
use crate::fragment_router::assign_fragments;
use crate::merge::global_top_k_by_distance;
use crate::scatter_gather::inject_traceparent;
use lance_distributed_common::ipc::{ipc_to_record_batch, record_batch_to_ipc};
use lance_distributed_proto::generated::lance_distributed as pb;

/// Tiny per-table Dataset cache so we don't pay a manifest read per
/// query. Stable while the underlying manifest doesn't change; MVP
/// does not refresh (caller must restart coord after write). Adequate
/// for read-heavy scenarios and for the Phase 2 verification tests.
pub struct DatasetCache {
    inner: RwLock<HashMap<String, Arc<lance::dataset::Dataset>>>,
}

impl Default for DatasetCache {
    fn default() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }
}

impl DatasetCache {
    pub async fn get_or_open(&self, uri: &str) -> Result<Arc<lance::dataset::Dataset>, Status> {
        {
            let r = self.inner.read().await;
            if let Some(d) = r.get(uri) {
                return Ok(d.clone());
            }
        }
        let ds = lance::dataset::Dataset::open(uri)
            .await
            .map_err(|e| Status::internal(format!("open dataset {uri}: {e}")))?;
        let ds = Arc::new(ds);
        self.inner.write().await.insert(uri.to_string(), ds.clone());
        Ok(ds)
    }

    /// Drop the cached handle for `uri`. Next `get_or_open` will re-read
    /// the current manifest. Called by the write path after a successful
    /// commit so subsequent reads see the new fragment set.
    pub async fn invalidate(&self, uri: &str) {
        self.inner.write().await.remove(uri);
    }
}

/// Dispatch a fragment fanout over `uri`. Returns the merged
/// top-`merge_limit` response.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_fragment_fanout(
    pool: &Arc<ConnectionPool>,
    dataset_cache: &Arc<DatasetCache>,
    uri: &str,
    table_name: &str,
    local_req: pb::LocalSearchRequest,
    merge_limit: u32,
    query_timeout: Duration,
    trace_id: Option<String>,
) -> Result<pb::SearchResponse, Status> {
    // 1. Resolve fragments from the dataset manifest (cached).
    let dataset = dataset_cache.get_or_open(uri).await?;
    let fragment_ids: Vec<u32> = dataset
        .get_fragments()
        .iter()
        .map(|f| f.id() as u32)
        .collect();
    if fragment_ids.is_empty() {
        return Ok(pb::SearchResponse {
            arrow_ipc_data: vec![],
            num_rows: 0,
            error: format!("{table_name}: dataset at {uri} has no fragments"),
            truncated: false,
            next_offset: 0,
            latency_ms: 0,
        });
    }

    // 2. Healthy worker snapshot.
    let healthy: Vec<String> = pool
        .worker_statuses()
        .await
        .into_iter()
        .filter(|(_, _, _, healthy, _, _, _)| *healthy)
        .map(|(id, _, _, _, _, _, _)| id)
        .collect();
    if healthy.is_empty() {
        return Err(Status::unavailable(format!(
            "no healthy workers for {table_name}"
        )));
    }

    // 3. HRW assignment.
    let assignment = assign_fragments(&fragment_ids, &healthy);
    debug!(
        "fragment fanout {table_name}: {} fragments → {} workers",
        fragment_ids.len(),
        assignment.len()
    );

    // 4. Fan out. One worker per assigned subset; shard_uri + fragment_ids
    //    carry the lazy-open directive from phase 1.
    let shared_req = Arc::new(local_req);
    let uri_owned = uri.to_string();
    let mut tasks = tokio::task::JoinSet::new();
    for (worker_id, fids) in assignment {
        let Ok(mut client) = pool.get_healthy_client(&worker_id).await else {
            warn!("fanout {table_name}: no client for worker {worker_id}");
            continue;
        };
        let mut req = (*shared_req).clone();
        req.fragment_ids = fids;
        req.shard_uri = Some(uri_owned.clone());
        let timeout = query_timeout;
        let tid = trace_id.clone();
        tasks.spawn(async move {
            let mut outbound = Request::new(req);
            if let Some(tid) = tid.as_deref() {
                outbound = inject_traceparent(outbound, tid);
            }
            let result = tokio::time::timeout(timeout, client.execute_local_search(outbound)).await;
            (worker_id, result)
        });
    }

    // 5. Gather IPC batches.
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut failures = 0usize;
    let mut failure_detail = Vec::new();
    let total = tasks.len();
    while let Some(handle) = tasks.join_next().await {
        match handle {
            Ok((wid, Ok(Ok(response)))) => {
                let resp = response.into_inner();
                if !resp.error.is_empty() {
                    failures += 1;
                    failure_detail.push(format!("{wid}: {}", resp.error));
                } else if resp.num_rows > 0 {
                    match ipc_to_record_batch(&resp.arrow_ipc_data) {
                        Ok(b) => batches.push(b),
                        Err(_e) => {
                            failures += 1;
                            failure_detail.push(format!("{wid}: IPC decode failure"));
                        }
                    }
                }
            }
            Ok((wid, Ok(Err(status)))) => {
                failures += 1;
                failure_detail.push(format!("{wid}: {}", status.code()));
            }
            Ok((wid, Err(_timeout))) => {
                failures += 1;
                failure_detail.push(format!("{wid}: timeout"));
            }
            Err(_) => failures += 1,
        }
    }
    if total > 0 && failures == total {
        return Err(Status::internal(format!(
            "all {total} workers failed: {}",
            failure_detail.join("; ")
        )));
    }
    let partial = if failures > 0 {
        format!(
            "partial: {failures}/{total} workers failed ({})",
            failure_detail.join("; ")
        )
    } else {
        String::new()
    };
    if batches.is_empty() {
        return Ok(pb::SearchResponse {
            arrow_ipc_data: vec![],
            num_rows: 0,
            error: partial,
            truncated: false,
            next_offset: 0,
            latency_ms: 0,
        });
    }

    // 6. Merge — same comparator as scatter_gather (ascending distance,
    //    which is the post-Phase-0 true-L2 after worker refine).
    let merged = global_top_k_by_distance(batches, merge_limit as usize)
        .map_err(|e| Status::internal(format!("merge: {e}")))?;
    let ipc = record_batch_to_ipc(&merged)
        .map_err(|e| Status::internal(format!("IPC encode: {e}")))?;
    Ok(pb::SearchResponse {
        arrow_ipc_data: ipc,
        num_rows: merged.num_rows() as u32,
        error: partial,
        truncated: false,
        next_offset: 0,
        latency_ms: 0,
    })
}
