// Licensed under the Apache License, Version 2.0.
// Coordinator gRPC service: receives client queries, delegates to scatter_gather.

use std::sync::Arc;
use std::time::Duration;

use log::info;
use tonic::{Request, Response, Status};

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_proto::generated::lance_distributed as pb;
use pb::{
    lance_scheduler_service_server::LanceSchedulerService,
    AnnSearchRequest, FtsSearchRequest, HybridSearchRequest, SearchResponse,
};
use lance_distributed_common::shard_state::{ShardState, StaticShardState, reconciliation_loop};
use lance_distributed_common::shard_pruning::ShardPruner;
use lance_distributed_proto::descriptor::LanceQueryType;

use super::connection_pool::ConnectionPool;

/// Coordinator gRPC service — query entry point.
pub struct CoordinatorService {
    pool: Arc<ConnectionPool>,
    shard_state: Arc<dyn ShardState>,
    pruner: Arc<ShardPruner>,
    oversample_factor: u32,
    query_timeout: Duration,
    embedding_config: Option<lance_distributed_common::config::EmbeddingConfig>,
    metrics: Arc<lance_distributed_common::metrics::Metrics>,
}

impl CoordinatorService {
    pub fn new(config: &ClusterConfig, query_timeout: Duration) -> Self {
        Self::with_pruner(config, query_timeout, ShardPruner::empty())
    }

    /// Get metrics handle for the metrics HTTP server.
    pub fn metrics(&self) -> Arc<lance_distributed_common::metrics::Metrics> {
        self.metrics.clone()
    }

    pub fn with_pruner(config: &ClusterConfig, query_timeout: Duration, pruner: ShardPruner) -> Self {
        let shard_state: Arc<dyn ShardState> = Arc::new(StaticShardState::from_config(config));

        let mut endpoints = std::collections::HashMap::new();
        for e in &config.executors {
            endpoints.insert(e.id.clone(), (e.host.clone(), e.port));
        }

        let pool = Arc::new(ConnectionPool::new(endpoints, query_timeout));

        info!("CoordinatorService: {} executors", config.executors.len());

        // Start connection pool with ShardState integration
        pool.start_background(Some(shard_state.clone()));

        // Start reconciliation loop
        let recon_state = shard_state.clone();
        tokio::spawn(async move {
            reconciliation_loop(recon_state, Duration::from_secs(5)).await;
        });

        let metrics = lance_distributed_common::metrics::Metrics::new();

        Self {
            pool,
            shard_state,
            pruner: Arc::new(pruner),
            oversample_factor: 2,
            query_timeout,
            embedding_config: config.embedding.clone(),
            metrics,
        }
    }
}

#[tonic::async_trait]
impl LanceSchedulerService for CoordinatorService {
    async fn ann_search(
        &self,
        request: Request<AnnSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let k = validate_k(req.k, self.oversample_factor)?;
        let oversample_k = k * self.oversample_factor;

        if req.table_name.is_empty() || req.table_name.len() > 256 {
            return Err(Status::invalid_argument("table_name must be 1-256 characters"));
        }

        // Resolve query vector: provided vector or embed text
        let query_vector = if !req.query_vector.is_empty() {
            req.query_vector
        } else if let Some(ref text) = req.query_text {
            let config = self.embedding_config.as_ref().ok_or_else(|| {
                Status::failed_precondition("Text query requires embedding config".to_string())
            })?;
            let vec = crate::embedding::embed_query(text, config).await
                .map_err(|e| Status::internal(format!("Embedding failed: {e}")))?;
            vec.iter().flat_map(|f| f.to_le_bytes()).collect()
        } else {
            return Err(Status::invalid_argument("Either query_vector or query_text required".to_string()));
        };

        let dimension = if req.dimension > 0 { req.dimension } else { (query_vector.len() / 4) as u32 };

        let local_req = pb::LocalSearchRequest {
            query_type: LanceQueryType::Ann as i32,
            table_name: req.table_name.clone(),
            vector_column: Some(req.vector_column),
            query_vector: Some(query_vector),
            dimension: Some(dimension),
            nprobes: Some(req.nprobes),
            metric_type: Some(req.metric_type),
            text_column: None, query_text: None,
            k: oversample_k, filter: req.filter, columns: req.columns,
        };

        let t0 = std::time::Instant::now();
        let result = super::scatter_gather::scatter_gather(
            &self.pool, &self.shard_state, &self.pruner,
            &req.table_name, local_req, k, true, self.query_timeout,
        ).await;
        let latency_us = t0.elapsed().as_micros() as u64;
        self.metrics.record_query(latency_us, result.is_ok());
        Ok(Response::new(result?))
    }

    async fn fts_search(
        &self,
        request: Request<FtsSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let k = validate_k(req.k, self.oversample_factor)?;
        let oversample_k = k * self.oversample_factor;

        let local_req = pb::LocalSearchRequest {
            query_type: LanceQueryType::Fts as i32,
            table_name: req.table_name.clone(),
            vector_column: None, query_vector: None, dimension: None,
            nprobes: None, metric_type: None,
            text_column: Some(req.text_column), query_text: Some(req.query_text),
            k: oversample_k, filter: req.filter, columns: req.columns,
        };

        let t0 = std::time::Instant::now();
        let result = super::scatter_gather::scatter_gather(
            &self.pool, &self.shard_state, &self.pruner,
            &req.table_name, local_req, k, false, self.query_timeout,
        ).await;
        self.metrics.record_query(t0.elapsed().as_micros() as u64, result.is_ok());
        Ok(Response::new(result?))
    }

    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let k = validate_k(req.k, self.oversample_factor)?;
        let oversample_k = k * self.oversample_factor;

        let local_req = pb::LocalSearchRequest {
            query_type: LanceQueryType::Hybrid as i32,
            table_name: req.table_name.clone(),
            vector_column: Some(req.vector_column),
            query_vector: Some(req.query_vector),
            dimension: Some(req.dimension),
            nprobes: Some(req.nprobes),
            metric_type: Some(req.metric_type),
            text_column: Some(req.text_column),
            query_text: Some(req.query_text),
            k: oversample_k, filter: req.filter, columns: req.columns,
        };

        let t0 = std::time::Instant::now();
        let result = super::scatter_gather::scatter_gather(
            &self.pool, &self.shard_state, &self.pruner,
            &req.table_name, local_req, k, false, self.query_timeout,
        ).await;
        self.metrics.record_query(t0.elapsed().as_micros() as u64, result.is_ok());
        Ok(Response::new(result?))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<pb::ClusterStatusRequest>,
    ) -> Result<Response<pb::ClusterStatusResponse>, Status> {
        let statuses = self.pool.worker_statuses().await;
        let executors: Vec<pb::ExecutorStatus> = statuses.iter().map(|(id, host, port, healthy, last_check)| {
            pb::ExecutorStatus {
                executor_id: id.clone(),
                host: host.clone(),
                port: *port as u32,
                healthy: *healthy,
                loaded_shards: 0,
                last_health_check_ms: last_check.elapsed().as_millis() as i64,
            }
        }).collect();

        Ok(Response::new(pb::ClusterStatusResponse {
            executors,
            total_shards: 0,
            total_rows: 0,
        }))
    }

    async fn add_rows(
        &self,
        request: Request<pb::AddRowsRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() || req.table_name.len() > 256 {
            return Err(Status::invalid_argument("table_name must be 1-256 characters"));
        }
        if req.arrow_ipc_data.is_empty() {
            return Err(Status::invalid_argument("arrow_ipc_data is empty"));
        }

        // Route Add to first healthy worker for this table (MVP: round-robin later)
        let executors = self.shard_state.executors_for_table(&req.table_name).await;
        if executors.is_empty() {
            return Err(Status::not_found(format!("No executors for table: {}", req.table_name)));
        }

        let worker_id = &executors[0];
        let mut client = self.pool.get_healthy_client(worker_id).await?;

        let local_req = pb::LocalWriteRequest {
            write_type: 0, // Add
            table_name: req.table_name,
            arrow_ipc_data: req.arrow_ipc_data,
            filter: String::new(),
        };

        let result = tokio::time::timeout(
            self.query_timeout,
            client.execute_local_write(Request::new(local_req)),
        ).await
            .map_err(|_| Status::deadline_exceeded("Write timed out"))?
            .map_err(|e| Status::internal(format!("Worker error: {}", e)))?;

        let resp = result.into_inner();
        Ok(Response::new(pb::WriteResponse {
            affected_rows: resp.affected_rows,
            new_version: resp.new_version,
            error: resp.error,
        }))
    }

    async fn delete_rows(
        &self,
        request: Request<pb::DeleteRowsRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() || req.table_name.len() > 256 {
            return Err(Status::invalid_argument("table_name must be 1-256 characters"));
        }
        if req.filter.is_empty() {
            return Err(Status::invalid_argument("filter is required for delete"));
        }

        // Delete fans out to ALL workers (don't know which shard has matching rows)
        let executors = self.shard_state.executors_for_table(&req.table_name).await;
        if executors.is_empty() {
            return Err(Status::not_found(format!("No executors for table: {}", req.table_name)));
        }

        let mut total_affected = 0u64;
        let mut max_version = 0u64;
        let mut errors = Vec::new();

        for worker_id in &executors {
            if let Ok(mut client) = self.pool.get_healthy_client(worker_id).await {
                let local_req = pb::LocalWriteRequest {
                    write_type: 1, // Delete
                    table_name: req.table_name.clone(),
                    arrow_ipc_data: vec![],
                    filter: req.filter.clone(),
                };

                match tokio::time::timeout(
                    self.query_timeout,
                    client.execute_local_write(Request::new(local_req)),
                ).await {
                    Ok(Ok(resp)) => {
                        let r = resp.into_inner();
                        if r.error.is_empty() {
                            total_affected += r.affected_rows;
                            max_version = max_version.max(r.new_version);
                        } else {
                            errors.push(format!("{}: {}", worker_id, r.error));
                        }
                    }
                    Ok(Err(e)) => errors.push(format!("{}: {}", worker_id, e)),
                    Err(_) => errors.push(format!("{}: timeout", worker_id)),
                }
            }
        }

        let error = if errors.is_empty() { String::new() } else { errors.join("; ") };
        Ok(Response::new(pb::WriteResponse {
            affected_rows: total_affected,
            new_version: max_version,
            error,
        }))
    }
}

const MAX_K: u32 = 100_000;

fn validate_k(k: u32, oversample_factor: u32) -> std::result::Result<u32, Status> {
    if k == 0 {
        return Err(Status::invalid_argument("k must be > 0"));
    }
    if k > MAX_K {
        return Err(Status::invalid_argument(format!("k={k} exceeds maximum {MAX_K}")));
    }
    k.checked_mul(oversample_factor).ok_or_else(|| {
        Status::invalid_argument(format!("k={k} * oversample={oversample_factor} overflows u32"))
    })?;
    Ok(k)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_distributed_common::config::{ClusterConfig, TableConfig, ShardConfig, ExecutorConfig, EmbeddingConfig};

    fn make_config() -> ClusterConfig {
        ClusterConfig {
            tables: vec![TableConfig {
                name: "products".to_string(),
                shards: vec![ShardConfig {
                    name: "shard_0".to_string(),
                    uri: "/tmp/test.lance".to_string(),
                    executors: vec!["w0".to_string()],
                }],
            }],
            executors: vec![ExecutorConfig {
                id: "w0".to_string(),
                host: "127.0.0.1".to_string(),
                port: 59999,
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_coordinator_service_creation() {
        let config = make_config();
        let _svc = CoordinatorService::new(&config, Duration::from_secs(5));
        // Service creation should not panic even if workers are unreachable
    }

    #[tokio::test]
    async fn test_coordinator_with_embedding_config() {
        let mut config = make_config();
        config.embedding = Some(EmbeddingConfig {
            provider: "mock".to_string(),
            model: "test".to_string(),
            api_key: None,
            dimension: Some(64),
        });
        let _svc = CoordinatorService::new(&config, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_coordinator_empty_executors() {
        let config = ClusterConfig::default();
        let _svc = CoordinatorService::new(&config, Duration::from_secs(5));
    }
}
