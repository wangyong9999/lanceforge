// Licensed under the Apache License, Version 2.0.
// Coordinator gRPC service: receives client queries, delegates to scatter_gather.

use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
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

/// Default maximum concurrent queries (overridden by ServerConfig.max_concurrent_queries).
/// Default max concurrent queries (overridden by ServerConfig).
const _DEFAULT_MAX_CONCURRENT_QUERIES: usize = 200;

/// Coordinator gRPC service — query entry point.
pub struct CoordinatorService {
    pool: Arc<ConnectionPool>,
    shard_state: Arc<dyn ShardState>,
    pruner: Arc<ShardPruner>,
    oversample_factor: u32,
    query_timeout: Duration,
    embedding_config: Option<lance_distributed_common::config::EmbeddingConfig>,
    metrics: Arc<lance_distributed_common::metrics::Metrics>,
    query_semaphore: Arc<tokio::sync::Semaphore>,
    storage_options: std::collections::HashMap<String, String>,
    /// Default storage path for runtime-created tables.
    default_table_path: Option<String>,
    /// Replication factor for shard assignment during rebalance.
    replica_factor: usize,
    /// Round-robin counter for write distribution.
    write_counter: std::sync::atomic::AtomicUsize,
    /// For error messages.
    max_concurrent_queries: usize,
    /// Shard name → URI registry for LoadShard RPCs during rebalance.
    shard_uris: Arc<tokio::sync::RwLock<std::collections::HashMap<String, String>>>,
}

impl CoordinatorService {
    pub fn new(config: &ClusterConfig, query_timeout: Duration) -> Self {
        Self::with_pruner(config, query_timeout, ShardPruner::empty())
    }

    /// Get metrics handle for the metrics HTTP server.
    pub fn metrics(&self) -> Arc<lance_distributed_common::metrics::Metrics> {
        self.metrics.clone()
    }

    /// Get a handle that can be used to trigger graceful shutdown.
    /// Call this before moving `service` into the gRPC server.
    pub fn pool_shutdown_handle(&self) -> Arc<ConnectionPool> {
        self.pool.clone()
    }

    /// Create with MetaStore-backed metadata (survives restarts, CAS-safe).
    /// Supports both file paths and S3 URIs (s3://bucket/path/metadata.json).
    pub async fn with_meta_state(
        config: &ClusterConfig,
        query_timeout: Duration,
        metadata_path: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let store: Arc<dyn lance_distributed_meta::store::MetaStore> = if metadata_path.starts_with("s3://")
            || metadata_path.starts_with("gs://")
            || metadata_path.starts_with("az://")
        {
            info!("Initializing S3MetaStore at {}", metadata_path);
            Arc::new(
                lance_distributed_meta::store::S3MetaStore::new(
                    metadata_path,
                    config.storage_options.iter().map(|(k, v)| (k.clone(), v.clone())),
                ).await.map_err(|e| format!("S3MetaStore init failed: {e}"))?
            )
        } else {
            Arc::new(
                lance_distributed_meta::store::FileMetaStore::new(metadata_path).await
                    .map_err(|e| format!("FileMetaStore init failed: {e}"))?
            )
        };
        let meta_state = lance_distributed_meta::state::MetaShardState::new(store.clone(), "lanceforge");

        // Initialize from config (merges existing persisted state with config)
        for e in &config.executors {
            meta_state.register_executor(&e.id, &e.host, e.port).await;
        }
        let routing = config.build_routing_table();
        for (table_name, shard_routes) in &routing {
            if meta_state.get_current_target(table_name).await.is_none() {
                let mapping: lance_distributed_common::shard_state::ShardMapping = shard_routes.iter()
                    .map(|(sname, execs)| (sname.clone(), execs.clone()))
                    .collect();
                meta_state.register_table(table_name, mapping).await;
            }
        }

        info!("Using MetaStore at {} ({} tables)", metadata_path,
            meta_state.all_tables().await.len());
        Ok(Self::with_shard_state(config, query_timeout, Arc::new(meta_state)))
    }

    pub fn with_pruner(config: &ClusterConfig, query_timeout: Duration, pruner: ShardPruner) -> Self {
        let shard_state: Arc<dyn ShardState> = Arc::new(StaticShardState::from_config(config));
        Self::with_shard_state_and_pruner(config, query_timeout, shard_state, pruner)
    }

    fn with_shard_state(config: &ClusterConfig, query_timeout: Duration, shard_state: Arc<dyn ShardState>) -> Self {
        Self::with_shard_state_and_pruner(config, query_timeout, shard_state, ShardPruner::empty())
    }

    fn with_shard_state_and_pruner(config: &ClusterConfig, query_timeout: Duration, shard_state: Arc<dyn ShardState>, pruner: ShardPruner) -> Self {

        let mut endpoints = std::collections::HashMap::new();
        for e in &config.executors {
            endpoints.insert(e.id.clone(), (e.host.clone(), e.port));
        }

        // Build shard URI registry from config
        let mut shard_uris = std::collections::HashMap::new();
        for table in &config.tables {
            for shard in &table.shards {
                shard_uris.insert(shard.name.clone(), shard.uri.clone());
            }
        }

        let mut pool = ConnectionPool::new(endpoints, query_timeout, &config.health_check, &config.server);

        // Configure client-side TLS for coordinator→worker connections
        if let Some(ref ca_path) = config.security.tls_ca_cert {
            match std::fs::read(ca_path) {
                Ok(ca_pem) => {
                    pool = pool.with_tls(ca_pem);
                    info!("TLS enabled for coordinator→worker connections (CA: {})", ca_path);
                }
                Err(e) => warn!("Failed to read TLS CA cert {}: {}", ca_path, e),
            }
        }
        let pool = Arc::new(pool);

        info!("CoordinatorService: {} executors, {} shard URIs", config.executors.len(), shard_uris.len());

        // Start connection pool with ShardState integration
        pool.start_background(Some(shard_state.clone()));

        // Start reconciliation loop
        let recon_state = shard_state.clone();
        let recon_interval = Duration::from_secs(config.health_check.reconciliation_interval_secs);
        tokio::spawn(async move {
            reconciliation_loop(recon_state, recon_interval).await;
        });

        let metrics = lance_distributed_common::metrics::Metrics::new();
        let max_queries = config.server.max_concurrent_queries;
        let query_semaphore = Arc::new(tokio::sync::Semaphore::new(max_queries));

        Self {
            pool,
            shard_state,
            pruner: Arc::new(pruner),
            oversample_factor: config.server.oversample_factor,
            query_timeout,
            embedding_config: config.embedding.clone(),
            metrics,
            query_semaphore,
            storage_options: config.storage_options.clone(),
            default_table_path: config.default_table_path.clone(),
            replica_factor: config.replica_factor,
            write_counter: std::sync::atomic::AtomicUsize::new(0),
            max_concurrent_queries: max_queries,
            shard_uris: Arc::new(tokio::sync::RwLock::new(shard_uris)),
        }
    }
}

#[tonic::async_trait]
impl LanceSchedulerService for CoordinatorService {
    async fn ann_search(
        &self,
        request: Request<AnnSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!(
                "Too many concurrent queries (max {})", self.max_concurrent_queries
            ))
        })?;
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
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
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
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
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
        let mut total_shards = 0u32;
        let mut total_rows = 0u64;
        let executors: Vec<pb::ExecutorStatus> = statuses.iter()
            .map(|(id, host, port, healthy, last_check, loaded_shards, rows)| {
                total_shards += loaded_shards;
                total_rows += rows;
                pb::ExecutorStatus {
                    executor_id: id.clone(),
                    host: host.clone(),
                    port: *port as u32,
                    healthy: *healthy,
                    loaded_shards: *loaded_shards,
                    last_health_check_ms: last_check.elapsed().as_millis() as i64,
                }
            }).collect();

        Ok(Response::new(pb::ClusterStatusResponse {
            executors,
            total_shards,
            total_rows,
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

        // Route Add via round-robin across SHARDS (not executors) to prevent
        // duplication when a worker owns multiple shards of the same table.
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("No shards for table: {}", req.table_name)));
        }

        let idx = self.write_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % routing.len();
        let (shard_name, primary, secondary) = &routing[idx];
        // Try primary, fall back to secondary
        let worker_id = if self.pool.get_healthy_client(primary).await.is_ok() {
            primary
        } else if let Some(sec) = secondary {
            sec
        } else {
            return Err(Status::unavailable(format!("No healthy worker for shard {}", shard_name)));
        };
        let mut client = self.pool.get_healthy_client(worker_id).await?;

        let local_req = pb::LocalWriteRequest {
            write_type: 0, // Add
            table_name: req.table_name,
            arrow_ipc_data: req.arrow_ipc_data,
            filter: String::new(),
            on_columns: vec![],
            target_shard: shard_name.clone(),
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
                    on_columns: vec![],
                    target_shard: String::new(), // fan-out to all local shards
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

    async fn upsert_rows(
        &self,
        request: Request<pb::UpsertRowsRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() || req.table_name.len() > 256 {
            return Err(Status::invalid_argument("table_name must be 1-256 characters"));
        }
        if req.arrow_ipc_data.is_empty() {
            return Err(Status::invalid_argument("arrow_ipc_data is empty"));
        }
        if req.on_columns.is_empty() {
            return Err(Status::invalid_argument("on_columns required for upsert"));
        }

        // Upsert fans out to ALL workers (each applies merge_insert on local shard)
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
                    write_type: 2, // Upsert
                    table_name: req.table_name.clone(),
                    arrow_ipc_data: req.arrow_ipc_data.clone(),
                    filter: String::new(),
                    on_columns: req.on_columns.clone(),
                    target_shard: String::new(), // fan-out to all local shards
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

    async fn create_table(
        &self,
        request: Request<pb::CreateTableRequest>,
    ) -> Result<Response<pb::CreateTableResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        if req.arrow_ipc_data.is_empty() {
            return Err(Status::invalid_argument("arrow_ipc_data required (initial data defines schema)"));
        }

        let batch = lance_distributed_common::ipc::ipc_to_record_batch(&req.arrow_ipc_data)
            .map_err(|e| Status::internal(format!("IPC decode failed: {e}")))?;
        let num_rows = batch.num_rows() as u64;

        let base = if req.uri.is_empty() {
            self.default_table_path.as_deref().unwrap_or("/tmp/lanceforge_tables").to_string()
        } else {
            // Extract parent dir from user-provided URI
            let (parent, _) = lance_distributed_common::auto_shard::split_parent_uri(&req.uri);
            parent
        };

        // Get healthy workers for auto-sharding
        let statuses = self.pool.worker_statuses().await;
        let healthy: Vec<_> = statuses.iter()
            .filter(|(_, _, _, h, _, _, _)| *h)
            .collect();

        if healthy.is_empty() {
            return Err(Status::unavailable("No healthy workers available"));
        }

        // Determine shard count: min(healthy workers, data size threshold)
        let n_shards = if num_rows < 1000 { 1 } else { healthy.len() };

        // Split batch into N partitions
        let partitions = split_batch(&batch, n_shards);
        let mut mapping = std::collections::HashMap::new();
        let mut shard_uris_new = Vec::new();
        let mut total_created = 0u64;
        let index_col = if req.index_column.is_empty() { String::new() } else { req.index_column.clone() };

        // Send CreateLocalShard to each worker in parallel
        let mut handles = Vec::new();
        for (i, partition) in partitions.into_iter().enumerate() {
            let shard_name = format!("{}_shard_{:02}", req.table_name, i);
            let worker_id = healthy[i % healthy.len()].0.clone();

            if let Ok(mut client) = self.pool.get_healthy_client(&worker_id).await {
                let ipc_data = lance_distributed_common::ipc::record_batch_to_ipc(&partition)
                    .map_err(|e| Status::internal(format!("IPC encode: {e}")))?;
                let create_req = pb::CreateLocalShardRequest {
                    shard_name: shard_name.clone(),
                    parent_uri: base.clone(),
                    arrow_ipc_data: ipc_data,
                    index_column: index_col.clone(),
                    index_num_partitions: req.index_num_partitions,
                    storage_options: self.storage_options.clone(),
                };
                let sn = shard_name.clone();
                let wid = worker_id.clone();
                handles.push(tokio::spawn(async move {
                    let result = client.create_local_shard(Request::new(create_req)).await;
                    (sn, wid, result)
                }));
            }

            mapping.insert(shard_name.clone(), vec![worker_id.clone()]);
        }

        // Gather results
        for handle in handles {
            match handle.await {
                Ok((shard_name, _wid, Ok(resp))) => {
                    let r = resp.into_inner();
                    if r.error.is_empty() {
                        total_created += r.num_rows;
                        shard_uris_new.push((shard_name, r.uri));
                    } else {
                        return Err(Status::internal(format!("CreateLocalShard failed: {}", r.error)));
                    }
                }
                Ok((_, _, Err(e))) => return Err(Status::internal(format!("CreateLocalShard RPC: {e}"))),
                Err(e) => return Err(Status::internal(format!("Spawn error: {e}"))),
            }
        }

        // Register all shards in shard state
        self.shard_state.register_table(&req.table_name, mapping).await;

        // Register shard URIs for rebalance
        let mut uris = self.shard_uris.write().await;
        for (shard_name, uri) in &shard_uris_new {
            uris.insert(shard_name.clone(), uri.clone());
        }

        info!("Created table '{}' ({} rows across {} shards on {} workers)",
            req.table_name, total_created, n_shards, healthy.len());

        Ok(Response::new(pb::CreateTableResponse {
            table_name: req.table_name.clone(),
            num_rows,
            error: String::new(),
        }))
    }

    async fn list_tables(
        &self,
        _request: Request<pb::ListTablesRequest>,
    ) -> Result<Response<pb::ListTablesResponse>, Status> {
        let tables = self.shard_state.all_tables().await;
        Ok(Response::new(pb::ListTablesResponse {
            table_names: tables,
            error: String::new(),
        }))
    }

    async fn drop_table(
        &self,
        request: Request<pb::DropTableRequest>,
    ) -> Result<Response<pb::DropTableResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        // Tell workers to unload shards for this table
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        for (shard_name, primary, _secondary) in &routing {
            if let Ok(mut client) = self.pool.get_healthy_client(primary).await {
                let _ = client.unload_shard(Request::new(pb::UnloadShardRequest {
                    shard_name: shard_name.clone(),
                })).await;
            }
        }
        // Remove from shard state (set empty mapping)
        let empty: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        self.shard_state.register_table(&req.table_name, empty).await;
        info!("Dropped table '{}'", req.table_name);
        Ok(Response::new(pb::DropTableResponse { error: String::new() }))
    }

    async fn create_index(
        &self,
        request: Request<pb::CreateIndexRequest>,
    ) -> Result<Response<pb::CreateIndexResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() || req.column.is_empty() {
            return Err(Status::invalid_argument("table_name and column required"));
        }

        // Fan out to all workers to create index on their local shards
        let executors = self.shard_state.executors_for_table(&req.table_name).await;
        if executors.is_empty() {
            return Err(Status::not_found(format!("No executors for table: {}", req.table_name)));
        }

        // Fan out CreateIndex to all workers
        let mut errors = Vec::new();
        for worker_id in &executors {
            if let Ok(mut client) = self.pool.get_healthy_client(worker_id).await {
                match client.execute_create_index(Request::new(pb::CreateIndexRequest {
                    table_name: req.table_name.clone(),
                    column: req.column.clone(),
                    index_type: req.index_type.clone(),
                    num_partitions: req.num_partitions,
                })).await {
                    Ok(resp) => {
                        let r = resp.into_inner();
                        if !r.error.is_empty() {
                            errors.push(format!("{}: {}", worker_id, r.error));
                        }
                    }
                    Err(e) => errors.push(format!("{}: {}", worker_id, e)),
                }
            }
        }
        let error = if errors.is_empty() { String::new() } else { errors.join("; ") };
        info!("CreateIndex on '{}' column '{}': {}", req.table_name, req.column,
            if error.is_empty() { "success" } else { &error });
        Ok(Response::new(pb::CreateIndexResponse { error }))
    }

    async fn get_schema(
        &self,
        request: Request<pb::GetSchemaRequest>,
    ) -> Result<Response<pb::GetSchemaResponse>, Status> {
        let req = request.into_inner();
        let executors = self.shard_state.executors_for_table(&req.table_name).await;
        if executors.is_empty() {
            return Err(Status::not_found(format!("Table not found: {}", req.table_name)));
        }
        // Ask first worker for table info
        if let Ok(mut client) = self.pool.get_healthy_client(&executors[0]).await {
            if let Ok(resp) = client.get_table_info(Request::new(pb::GetTableInfoRequest {
                table_name: req.table_name.clone(),
            })).await {
                let r = resp.into_inner();
                return Ok(Response::new(pb::GetSchemaResponse {
                    columns: r.columns,
                    error: r.error,
                }));
            }
        }
        Ok(Response::new(pb::GetSchemaResponse {
            columns: vec![],
            error: "Failed to get schema from workers".to_string(),
        }))
    }

    async fn count_rows(
        &self,
        request: Request<pb::CountRowsRequest>,
    ) -> Result<Response<pb::CountRowsResponse>, Status> {
        let req = request.into_inner();
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("Table not found: {}", req.table_name)));
        }
        // Count per-shard via primary executor. Query by shard name (not table name)
        // to avoid double-counting when a worker hosts multiple shards or replicas.
        let mut total = 0u64;
        for (shard_name, primary, secondary) in &routing {
            // Try primary, fall back to secondary
            let worker = if self.pool.get_healthy_client(primary).await.is_ok() {
                primary
            } else if let Some(sec) = secondary {
                sec
            } else {
                continue;
            };
            if let Ok(mut client) = self.pool.get_healthy_client(worker).await {
                if let Ok(resp) = client.get_table_info(Request::new(pb::GetTableInfoRequest {
                    table_name: shard_name.clone(), // query by shard name for exact count
                })).await {
                    total += resp.into_inner().num_rows;
                }
            }
        }
        Ok(Response::new(pb::CountRowsResponse {
            count: total,
            error: String::new(),
        }))
    }

    async fn rebalance(
        &self,
        _request: Request<pb::RebalanceRequest>,
    ) -> Result<Response<pb::RebalanceResponse>, Status> {
        use lance_distributed_meta::shard_manager::{assign_shards, compute_diff, ShardPolicy};

        // Only consider healthy executors for rebalance (skip dead workers)
        let statuses = self.pool.worker_statuses().await;
        let executor_ids: Vec<String> = statuses.iter()
            .filter(|(_, _, _, healthy, _, _, _)| *healthy)
            .map(|(id, _, _, _, _, _, _)| id.clone())
            .collect();
        let all_tables = self.shard_state.all_tables().await;

        if executor_ids.is_empty() {
            return Ok(Response::new(pb::RebalanceResponse {
                shards_moved: 0,
                error: "No executors available".to_string(),
            }));
        }

        let policy = ShardPolicy {
            replica_factor: self.replica_factor.min(executor_ids.len()),
            max_shards_per_worker: 0,
        };

        let mut total_moved = 0u32;
        let mut errors = Vec::new();
        let shard_uris = self.shard_uris.read().await;

        for table_name in &all_tables {
            let current_routing = self.shard_state.get_shard_routing(table_name).await;
            if current_routing.is_empty() { continue; }

            // Build current assignment map
            let shard_names: Vec<String> = current_routing.iter().map(|(s, _, _)| s.clone()).collect();
            let mut current_assignment = std::collections::HashMap::new();
            for (shard, primary, secondary) in &current_routing {
                let mut execs = vec![primary.clone()];
                if let Some(sec) = secondary {
                    execs.push(sec.clone());
                }
                current_assignment.insert(shard.clone(), execs);
            }

            // Collect per-shard row counts for size-aware balancing.
            // Query each shard's primary for its row count.
            let mut shard_sizes: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            for (shard_name, primary, secondary) in &current_routing {
                let worker = if self.pool.get_healthy_client(primary).await.is_ok() {
                    Some(primary.as_str())
                } else {
                    secondary.as_ref().map(|s| s.as_str())
                };
                if let Some(wid) = worker {
                    if let Ok(mut client) = self.pool.get_healthy_client(wid).await {
                        if let Ok(resp) = client.get_table_info(Request::new(pb::GetTableInfoRequest {
                            table_name: shard_name.clone(),
                        })).await {
                            shard_sizes.insert(shard_name.clone(), resp.into_inner().num_rows);
                        }
                    }
                }
            }

            // Compute new assignment with size-aware ShardManager
            let new_assignment = lance_distributed_meta::shard_manager::assign_shards_with_sizes(
                &shard_names, &executor_ids, &policy, Some(&current_assignment),
                if shard_sizes.is_empty() { None } else { Some(&shard_sizes) },
            );

            // Compute diff (minimal movement)
            let (to_load, to_unload) = compute_diff(&current_assignment, &new_assignment);

            // Phase 1: Send LoadShard RPCs to workers receiving new shards
            for (worker_id, shards_to_load) in &to_load {
                if let Ok(mut client) = self.pool.get_healthy_client(worker_id).await {
                    for shard_name in shards_to_load {
                        if let Some(uri) = shard_uris.get(shard_name) {
                            let load_req = pb::LoadShardRequest {
                                shard_name: shard_name.clone(),
                                uri: uri.clone(),
                                storage_options: self.storage_options.clone(),
                            };
                            match tokio::time::timeout(
                                Duration::from_secs(30),
                                client.load_shard(Request::new(load_req)),
                            ).await {
                                Ok(Ok(_)) => {
                                    info!("Rebalance: loaded {} on {}", shard_name, worker_id);
                                }
                                Ok(Err(e)) => {
                                    errors.push(format!("LoadShard {}->{}: {}", shard_name, worker_id, e));
                                }
                                Err(_) => {
                                    errors.push(format!("LoadShard {}->{}: timeout", shard_name, worker_id));
                                }
                            }
                        } else {
                            errors.push(format!("No URI for shard {}", shard_name));
                        }
                    }
                }
            }

            // Phase 2: Send UnloadShard RPCs to workers losing shards
            for (worker_id, shards_to_unload) in &to_unload {
                if let Ok(mut client) = self.pool.get_healthy_client(worker_id).await {
                    for shard_name in shards_to_unload {
                        let unload_req = pb::UnloadShardRequest {
                            shard_name: shard_name.clone(),
                        };
                        match tokio::time::timeout(
                            Duration::from_secs(10),
                            client.unload_shard(Request::new(unload_req)),
                        ).await {
                            Ok(Ok(_)) => {
                                info!("Rebalance: unloaded {} from {}", shard_name, worker_id);
                            }
                            Ok(Err(e)) => {
                                // Non-fatal: worker may already have lost the shard
                                warn!("Rebalance: unload {} from {} failed: {}", shard_name, worker_id, e);
                            }
                            Err(_) => {
                                warn!("Rebalance: unload {} from {} timed out", shard_name, worker_id);
                            }
                        }
                    }
                }
            }

            let moved: usize = to_load.values().map(|v| v.len()).sum();
            total_moved += moved as u32;

            // Phase 3: Update shard_state metadata (after RPCs succeed)
            self.shard_state.register_table(table_name, new_assignment).await;
        }

        let error = if errors.is_empty() { String::new() } else { errors.join("; ") };
        if !error.is_empty() {
            warn!("Rebalance completed with errors: {}", error);
        }
        info!("Rebalance: {} shards moved across {} executors", total_moved, executor_ids.len());
        Ok(Response::new(pb::RebalanceResponse {
            shards_moved: total_moved,
            error,
        }))
    }

    async fn register_worker(
        &self,
        request: Request<pb::RegisterWorkerRequest>,
    ) -> Result<Response<pb::RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        if req.worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id required"));
        }
        info!("Worker '{}' registering at {}:{}", req.worker_id, req.host, req.port);
        self.shard_state.register_executor(&req.worker_id, &req.host, req.port as u16).await;
        Ok(Response::new(pb::RegisterWorkerResponse {
            assigned_shards: 0,
            error: String::new(),
        }))
    }
}

const MAX_K: u32 = 100_000;

/// Split a RecordBatch into N roughly-equal partitions for auto-sharding.
fn split_batch(batch: &arrow::array::RecordBatch, n: usize) -> Vec<arrow::array::RecordBatch> {
    if n <= 1 {
        return vec![batch.clone()];
    }
    let total = batch.num_rows();
    let chunk_size = (total + n - 1) / n;
    (0..n)
        .map(|i| {
            let start = i * chunk_size;
            let len = chunk_size.min(total.saturating_sub(start));
            if len == 0 {
                batch.slice(0, 0) // empty batch with same schema
            } else {
                batch.slice(start, len)
            }
        })
        .filter(|b| b.num_rows() > 0)
        .collect()
}

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
