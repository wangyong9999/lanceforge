// Licensed under the Apache License, Version 2.0.
// Scheduler-side Lance query routing and Scatter-Gather execution.
//
// Extends Ballista Scheduler with a fast path for ANN/FTS/hybrid queries
// that bypasses the standard Stage/Shuffle pipeline.
// Instead: Scatter query descriptor → Executors execute locally → Gather + merge.

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use log::{debug, info, warn};

use crate::codec::proto::{
    FtsQueryParams, LanceQueryDescriptor, LanceQueryType, VectorQueryParams,
};
use crate::config::ClusterConfig;
use crate::merge::{global_top_k_by_distance, global_top_k_by_score};

/// Scheduler extension for Lance distributed retrieval.
/// Manages shard routing and Scatter-Gather execution.
#[allow(dead_code)] // InProcess Scatter-Gather, used in tests
pub struct LanceSchedulerExtension {
    routing_table: HashMap<String, Vec<(String, Vec<String>)>>,
    executor_connections: HashMap<String, ExecutorConnection>,
    query_timeout: Duration,
    /// Default oversampling factor for distributed recall compensation
    default_oversample_factor: u32,
}

/// Connection to a remote Executor for sending queries.
/// For MVP: simple gRPC channel. Production: connection pool.
pub struct ExecutorConnection {
    pub id: String,
    pub host: String,
    pub port: u16,
    // In MVP, we'll use direct function calls for in-process testing
    // and gRPC for real distributed execution
}

impl LanceSchedulerExtension {
    pub fn new(config: &ClusterConfig, query_timeout: Duration) -> Self {
        let routing_table = config.build_routing_table();

        let executor_connections: HashMap<String, ExecutorConnection> = config
            .executors
            .iter()
            .map(|e| {
                (
                    e.id.clone(),
                    ExecutorConnection {
                        id: e.id.clone(),
                        host: e.host.clone(),
                        port: e.port,
                    },
                )
            })
            .collect();

        info!(
            "LanceSchedulerExtension initialized: {} tables, {} executors",
            routing_table.len(),
            executor_connections.len()
        );

        Self {
            routing_table,
            executor_connections,
            query_timeout,
            default_oversample_factor: 2,
        }
    }

    /// Handle an ANN search request via Scatter-Gather fast path.
    pub async fn handle_ann_search(
        &self,
        table_name: &str,
        vector: Vec<f32>,
        column: &str,
        k: u32,
        nprobes: u32,
        filter: Option<String>,
        columns: Vec<String>,
    ) -> Result<RecordBatch> {
        let descriptor = LanceQueryDescriptor {
            table_name: table_name.to_string(),
            query_type: LanceQueryType::Ann as i32,
            vector_query: Some(VectorQueryParams {
                column: column.to_string(),
                vector_data: vector.iter().flat_map(|f| f.to_le_bytes()).collect(),
                dimension: vector.len() as u32,
                nprobes,
                metric_type: 0, // L2
                oversample_factor: self.default_oversample_factor,
            }),
            fts_query: None,
            filter,
            k: k * self.default_oversample_factor, // oversample for recall
            columns,
            fragment_ids: vec![],
        };

        let results = self.scatter_gather(&descriptor).await?;
        global_top_k_by_distance(results, k as usize)
    }

    /// Handle a full-text search request via Scatter-Gather fast path.
    pub async fn handle_fts_search(
        &self,
        table_name: &str,
        query_text: &str,
        column: &str,
        k: u32,
        filter: Option<String>,
        columns: Vec<String>,
    ) -> Result<RecordBatch> {
        let descriptor = LanceQueryDescriptor {
            table_name: table_name.to_string(),
            query_type: LanceQueryType::Fts as i32,
            vector_query: None,
            fts_query: Some(FtsQueryParams {
                query_text: query_text.to_string(),
                column: column.to_string(),
            }),
            filter,
            k: k * self.default_oversample_factor,
            columns,
            fragment_ids: vec![],
        };

        let results = self.scatter_gather(&descriptor).await?;
        global_top_k_by_score(results, k as usize)
    }

    /// Handle a hybrid search request (ANN + FTS → RRF).
    pub async fn handle_hybrid_search(
        &self,
        table_name: &str,
        vector: Vec<f32>,
        vector_column: &str,
        query_text: &str,
        text_column: &str,
        k: u32,
        nprobes: u32,
        filter: Option<String>,
        columns: Vec<String>,
    ) -> Result<RecordBatch> {
        let descriptor = LanceQueryDescriptor {
            table_name: table_name.to_string(),
            query_type: LanceQueryType::Hybrid as i32,
            vector_query: Some(VectorQueryParams {
                column: vector_column.to_string(),
                vector_data: vector.iter().flat_map(|f| f.to_le_bytes()).collect(),
                dimension: vector.len() as u32,
                nprobes,
                metric_type: 0,
                oversample_factor: self.default_oversample_factor,
            }),
            fts_query: Some(FtsQueryParams {
                query_text: query_text.to_string(),
                column: text_column.to_string(),
            }),
            filter,
            k: k * self.default_oversample_factor,
            columns,
            fragment_ids: vec![],
        };

        let results = self.scatter_gather(&descriptor).await?;

        // For hybrid, each Executor's result already has combined scores
        // We do global TopK merge on the combined _relevance_score
        global_top_k_by_score(results, k as usize)
    }

    /// Core Scatter-Gather implementation.
    /// Sends query descriptor to all relevant Executors in parallel,
    /// collects results, handles timeouts and partial failures.
    async fn scatter_gather(
        &self,
        descriptor: &LanceQueryDescriptor,
    ) -> Result<Vec<RecordBatch>> {
        // 1. Resolve target executors
        let target_executor_ids = self.resolve_targets(&descriptor.table_name)?;

        debug!(
            "Scatter-Gather: table={}, targets={:?}, query_type={}",
            descriptor.table_name, target_executor_ids, descriptor.query_type
        );

        if target_executor_ids.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No executors found for table: {}",
                descriptor.table_name
            )));
        }

        // 2. Scatter: send to all target executors in parallel
        // For MVP with in-process testing, this will be direct function calls.
        // For real distributed execution, this will be gRPC calls.
        //
        // The actual gRPC transport integration will be added when we
        // extend Ballista's SchedulerGrpc service with the ANN endpoint.
        //
        // For now, return a placeholder to validate the flow.
        info!(
            "Scatter to {} executors for table {}",
            target_executor_ids.len(),
            descriptor.table_name
        );

        // TODO: Replace with actual gRPC Scatter when integrating with Ballista Scheduler
        // For MVP unit testing, use the InProcessScatterGather (see tests)
        Err(DataFusionError::NotImplemented(
            "gRPC Scatter not yet implemented - use InProcessScatterGather for testing".to_string(),
        ))
    }

    /// Resolve which executor IDs should receive this query.
    pub fn resolve_targets(&self, table_name: &str) -> Result<Vec<String>> {
        let routes = self.routing_table.get(table_name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Table not found in routing table: {}",
                table_name
            ))
        })?;

        let mut executor_ids: Vec<String> = Vec::new();
        for (_shard_name, shard_executors) in routes {
            for eid in shard_executors {
                if !executor_ids.contains(eid) {
                    executor_ids.push(eid.clone());
                }
            }
        }

        Ok(executor_ids)
    }
}

/// In-process Scatter-Gather for testing without real gRPC.
/// Simulates distributed execution by running queries on local registries.
pub struct InProcessScatterGather {
    pub scheduler: LanceSchedulerExtension,
    pub executors: HashMap<String, crate::executor::LanceTableRegistry>,
}

impl InProcessScatterGather {
    pub async fn scatter_gather_ann(
        &self,
        table_name: &str,
        vector: Vec<f32>,
        column: &str,
        k: u32,
        nprobes: u32,
        filter: Option<String>,
        columns: Vec<String>,
    ) -> Result<RecordBatch> {
        let descriptor = LanceQueryDescriptor {
            table_name: table_name.to_string(),
            query_type: LanceQueryType::Ann as i32,
            vector_query: Some(VectorQueryParams {
                column: column.to_string(),
                vector_data: vector.iter().flat_map(|f| f.to_le_bytes()).collect(),
                dimension: vector.len() as u32,
                nprobes,
                metric_type: 0,
                oversample_factor: 2,
            }),
            fts_query: None,
            filter,
            k: k * 2, // oversample
            columns,
            fragment_ids: vec![],
        };

        let target_ids = self.scheduler.resolve_targets(table_name)?;

        // Execute on each target executor in parallel (simulated)
        let mut results = Vec::new();
        for eid in &target_ids {
            if let Some(registry) = self.executors.get(eid) {
                let batch = registry.execute_query(&descriptor).await?;
                if batch.num_rows() > 0 {
                    results.push(batch);
                }
            } else {
                warn!("Executor {} not found in InProcessScatterGather", eid);
            }
        }

        global_top_k_by_distance(results, k as usize)
    }
}
