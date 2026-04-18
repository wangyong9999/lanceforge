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
    /// Auth + RBAC interceptor (None = no auth).
    auth: Option<Arc<super::auth::ApiKeyInterceptor>>,
    /// Max bytes per SearchResponse (0 = unlimited).
    max_response_bytes: usize,
    /// Hard upper limit on k (and offset+k) per call.
    max_k: u32,
    /// Slow-query threshold (ms). 0 disables slow-query logging.
    slow_query_ms: u64,
    /// Shutdown signal for background tasks (reconciliation loop, etc.).
    bg_shutdown: Arc<tokio::sync::Notify>,
    /// MetaStore-backed state for shard URI persistence (None = static/no persistence).
    meta_state: Option<Arc<lance_distributed_meta::state::MetaShardState>>,
    /// Per-table DDL mutex: prevents concurrent CreateTable/DropTable on same table name.
    ddl_locks: Arc<tokio::sync::RwLock<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
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

    /// Get the background task shutdown signal.
    /// Call `notify_waiters()` to stop reconciliation loop, orphan GC, etc.
    pub fn bg_shutdown(&self) -> Arc<tokio::sync::Notify> {
        self.bg_shutdown.clone()
    }

    /// Expose the underlying MetaStore if one is configured. Returns None
    /// when the service runs in Static (no-persistence) mode. Used by the
    /// auth hot-reload task (B1.2.3) to share the same backend.
    pub fn meta_store(&self) -> Option<Arc<dyn lance_distributed_meta::store::MetaStore>> {
        self.meta_state.as_ref().map(|ms| ms.store())
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

        // Recover persisted shard URIs from MetaStore (survives restarts)
        let persisted_uris = meta_state.get_shard_uris().await;
        let recovered_count = persisted_uris.len();

        let meta_arc = Arc::new(meta_state);
        info!("Using MetaStore at {} ({} tables, {} shard URIs recovered)",
            metadata_path, meta_arc.all_tables().await.len(), recovered_count);

        let mut svc = Self::with_shard_state(config, query_timeout, meta_arc.clone() as Arc<dyn ShardState>);
        // Merge persisted URIs into the in-memory map (YAML URIs already loaded)
        {
            let mut uris = svc.shard_uris.write().await;
            for (k, v) in persisted_uris {
                uris.entry(k).or_insert(v);
            }
        }
        svc.meta_state = Some(meta_arc);
        Ok(svc)
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
        let shard_uris_arc = Arc::new(tokio::sync::RwLock::new(shard_uris));
        let pool = pool.with_shard_uris(shard_uris_arc.clone());
        let pool = Arc::new(pool);

        info!("CoordinatorService: {} executors", config.executors.len());

        // Start connection pool with ShardState integration
        pool.start_background(Some(shard_state.clone()));

        // Start reconciliation loop with shutdown signal
        let bg_shutdown = Arc::new(tokio::sync::Notify::new());
        let recon_state = shard_state.clone();
        let recon_interval = Duration::from_secs(config.health_check.reconciliation_interval_secs);
        let recon_stop = bg_shutdown.clone();
        tokio::spawn(async move {
            reconciliation_loop(recon_state, recon_interval, recon_stop).await;
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
            shard_uris: shard_uris_arc,
            auth: None,
            max_response_bytes: config.server.max_response_bytes,
            max_k: config.server.max_k,
            slow_query_ms: config.server.slow_query_ms,
            bg_shutdown,
            meta_state: None,
            ddl_locks: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Attach an auth interceptor for RBAC enforcement at method level.
    pub fn with_auth(mut self, auth: Arc<super::auth::ApiKeyInterceptor>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Build a handle that background tasks (orphan GC, etc.) can hold
    /// without owning the CoordinatorService.
    pub fn orphan_gc_handle(&self) -> OrphanGcHandle {
        OrphanGcHandle {
            shard_uris: self.shard_uris.clone(),
            root: self.default_table_path.clone(),
        }
    }

    /// Authorize a request against a required permission.
    fn check_perm<T>(&self, req: &Request<T>, perm: super::auth::Permission) -> Result<(), Status> {
        if let Some(a) = &self.auth {
            a.authorize(req, perm)?;
        }
        Ok(())
    }

    /// Emit an audit log line for a sensitive action (write/admin). Level=info.
    /// Format: `audit op=<op> principal=<short_key> target=<table> details=<...>`
    fn audit<T>(&self, req: &Request<T>, op: &str, target: &str, details: &str) {
        let principal = self.auth.as_ref()
            .map(|a| a.principal(req))
            .unwrap_or_else(|| "no-auth".to_string());
        info!("audit op={} principal={} target={} details={}", op, principal, target, details);
    }

    /// Acquire a per-table DDL mutex. Prevents concurrent CreateTable/DropTable
    /// on the same table name from racing and creating duplicate shards.
    async fn ddl_lock(&self, table_name: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.ddl_locks.write().await;
            locks.entry(table_name.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    /// Emit a structured slow-query warn log if elapsed exceeds threshold.
    fn maybe_slow(&self, op: &str, table: &str, k: u32, elapsed_us: u64, num_rows: u32) {
        if self.slow_query_ms == 0 { return; }
        if elapsed_us / 1000 >= self.slow_query_ms {
            warn!("slow_query op={} table={} k={} rows={} elapsed_ms={}",
                op, table, k, num_rows, elapsed_us / 1000);
            self.metrics.record_slow_query();
        }
    }

    /// Hash-partition AddRows path: mirrors upsert_rows so that mixed
    /// Add+Upsert workloads with the same key always target the same shard.
    /// LocalWriteRequest.on_columns stays empty — on_columns here only drives
    /// partitioning, not Lance's merge semantics (which only Upsert uses).
    async fn add_rows_hash_partitioned(
        &self,
        req: pb::AddRowsRequest,
        routing: Vec<(String, String, Option<String>)>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        let input_batch = lance_distributed_common::ipc::ipc_to_record_batch(&req.arrow_ipc_data)
            .map_err(|e| Status::invalid_argument(format!("IPC decode: {e}")))?;
        let partitions = partition_batch_by_hash(&input_batch, &req.on_columns, routing.len())
            .map_err(|e| Status::invalid_argument(format!("partition: {e}")))?;

        let mut total_affected = 0u64;
        let mut max_version = 0u64;
        let mut errors = Vec::new();

        for (shard_idx, partition_batch) in partitions.iter().enumerate() {
            if partition_batch.num_rows() == 0 { continue; }
            let (shard_name, primary, secondary) = &routing[shard_idx];
            let chosen = if self.pool.get_healthy_client(primary).await.is_ok() {
                Some(primary.clone())
            } else if let Some(s) = secondary {
                if self.pool.get_healthy_client(s).await.is_ok() { Some(s.clone()) } else { None }
            } else { None };

            let worker_id = match chosen {
                Some(w) => w,
                None => { errors.push(format!("{}: no healthy worker", shard_name)); continue; }
            };
            let partition_ipc = lance_distributed_common::ipc::record_batch_to_ipc(partition_batch)
                .map_err(|e| Status::internal(format!("IPC encode: {e}")))?;
            if let Ok(mut client) = self.pool.get_healthy_client(&worker_id).await {
                let local_req = pb::LocalWriteRequest {
                    write_type: 0, // Add — Lance does a plain append, ignoring on_columns
                    table_name: req.table_name.clone(),
                    arrow_ipc_data: partition_ipc,
                    filter: String::new(),
                    on_columns: vec![],
                    target_shard: shard_name.clone(),
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
                            errors.push(format!("{}: {}", shard_name, r.error));
                        }
                    }
                    Ok(Err(e)) => errors.push(format!("{}: {}", shard_name, e)),
                    Err(_) => errors.push(format!("{}: timeout", shard_name)),
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

#[tonic::async_trait]
impl LanceSchedulerService for CoordinatorService {
    async fn ann_search(
        &self,
        request: Request<AnnSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!(
                "Too many concurrent queries (max {})", self.max_concurrent_queries
            ))
        })?;
        let req = request.into_inner();
        let (merge_limit, oversample_k) = validate_offset_k(req.offset, req.k, self.max_k, self.oversample_factor)?;
        let _ = validate_k(req.k, self.oversample_factor)?;

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
            &req.table_name, local_req, merge_limit, true, self.query_timeout,
        ).await;
        let latency_us = t0.elapsed().as_micros() as u64;
        self.metrics.record_query(latency_us, result.is_ok());
        let resp = result?;
        let mut paged = apply_pagination_and_cap(resp, req.offset, req.k, self.max_response_bytes);
        paged.latency_ms = latency_us / 1000;
        self.metrics.record_table_query(&req.table_name, paged.num_rows as u64, paged.error.is_empty());
        self.maybe_slow("ann_search", &req.table_name, req.k, latency_us, paged.num_rows);
        Ok(Response::new(paged))
    }

    async fn fts_search(
        &self,
        request: Request<FtsSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
        let req = request.into_inner();
        let (merge_limit, oversample_k) = validate_offset_k(req.offset, req.k, self.max_k, self.oversample_factor)?;

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
            &req.table_name, local_req, merge_limit, false, self.query_timeout,
        ).await;
        let latency_us = t0.elapsed().as_micros() as u64;
        self.metrics.record_query(latency_us, result.is_ok());
        let resp = result?;
        let mut paged = apply_pagination_and_cap(resp, req.offset, req.k, self.max_response_bytes);
        paged.latency_ms = latency_us / 1000;
        self.metrics.record_table_query(&req.table_name, paged.num_rows as u64, paged.error.is_empty());
        self.maybe_slow("fts_search", &req.table_name, req.k, latency_us, paged.num_rows);
        Ok(Response::new(paged))
    }

    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
        let req = request.into_inner();
        let (merge_limit, oversample_k) = validate_offset_k(req.offset, req.k, self.max_k, self.oversample_factor)?;

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
            &req.table_name, local_req, merge_limit, false, self.query_timeout,
        ).await;
        let latency_us = t0.elapsed().as_micros() as u64;
        self.metrics.record_query(latency_us, result.is_ok());
        let resp = result?;
        let mut paged = apply_pagination_and_cap(resp, req.offset, req.k, self.max_response_bytes);
        paged.latency_ms = latency_us / 1000;
        self.metrics.record_table_query(&req.table_name, paged.num_rows as u64, paged.error.is_empty());
        self.maybe_slow("hybrid_search", &req.table_name, req.k, latency_us, paged.num_rows);
        Ok(Response::new(paged))
    }

    async fn get_cluster_status(
        &self,
        request: Request<pb::ClusterStatusRequest>,
    ) -> Result<Response<pb::ClusterStatusResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
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

        // Update Prometheus executor health counters
        let healthy_count = executors.iter().filter(|e| e.healthy).count() as u64;
        self.metrics.set_executor_health(healthy_count, executors.len() as u64);

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
        self.check_perm(&request, super::auth::Permission::Write)?;
        let req = request.into_inner();
        if req.table_name.is_empty() || req.table_name.len() > 256 {
            return Err(Status::invalid_argument("table_name must be 1-256 characters"));
        }
        if req.arrow_ipc_data.is_empty() {
            return Err(Status::invalid_argument("arrow_ipc_data is empty"));
        }

        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("No shards for table: {}", req.table_name)));
        }

        // Two routing modes:
        //   on_columns set  → hash-partition by key, fan out to matching shards.
        //                     Same hash UpsertRows uses, so mixed Add/Upsert with
        //                     the same key land on the same shard (no
        //                     duplication). See LIMITATIONS.md §1.
        //   on_columns empty → historical round-robin over shards (one call,
        //                      one shard). Kept for backward compat.
        if !req.on_columns.is_empty() {
            return self.add_rows_hash_partitioned(req, routing).await;
        }

        let idx = self.write_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % routing.len();
        let (shard_name, primary, secondary) = &routing[idx];
        // Try primary, fall back to secondary
        let worker_id = if self.pool.get_healthy_client(primary).await.is_ok() {
            primary
        } else if let Some(sec) = secondary {
            if self.pool.get_healthy_client(sec).await.is_ok() { sec }
            else { return Err(Status::unavailable(format!("No healthy worker for shard {}", shard_name))); }
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
        self.check_perm(&request, super::auth::Permission::Write)?;
        let req = request.into_inner();
        if req.table_name.is_empty() || req.table_name.len() > 256 {
            return Err(Status::invalid_argument("table_name must be 1-256 characters"));
        }
        if req.filter.is_empty() {
            return Err(Status::invalid_argument("filter is required for delete"));
        }

        // Fan-out ONCE PER SHARD (not per worker): the data is a single OBS-backed
        // Lance table per shard URI; a Lance delete on the same URI from multiple
        // workers is redundant work. Route each shard's delete to a single healthy
        // owner (primary, or secondary if primary is down).
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("No shards for table: {}", req.table_name)));
        }

        let mut total_affected = 0u64;
        let mut max_version = 0u64;
        let mut errors = Vec::new();

        for (shard_name, primary, secondary) in &routing {
            let chosen = if self.pool.get_healthy_client(primary).await.is_ok() {
                Some(primary.clone())
            } else if let Some(s) = secondary {
                if self.pool.get_healthy_client(s).await.is_ok() { Some(s.clone()) } else { None }
            } else { None };

            let worker_id = match chosen {
                Some(w) => w,
                None => { errors.push(format!("{}: no healthy worker", shard_name)); continue; }
            };
            if let Ok(mut client) = self.pool.get_healthy_client(&worker_id).await {
                let local_req = pb::LocalWriteRequest {
                    write_type: 1, // Delete
                    table_name: req.table_name.clone(),
                    arrow_ipc_data: vec![],
                    filter: req.filter.clone(),
                    on_columns: vec![],
                    target_shard: shard_name.clone(),
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
                            errors.push(format!("{}: {}", shard_name, r.error));
                        }
                    }
                    Ok(Err(e)) => errors.push(format!("{}: {}", shard_name, e)),
                    Err(_) => errors.push(format!("{}: timeout", shard_name)),
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
        self.check_perm(&request, super::auth::Permission::Write)?;
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

        // Hash-partition the incoming rows by `on_columns` so each row lands
        // on exactly ONE shard. Before this, every shard ran merge_insert over
        // the whole batch; non-matching rows got inserted once per shard
        // (N× duplication). Post-partition, each row's upsert targets only
        // its owning shard — within that shard, Lance's merge_insert gives
        // the usual update-or-insert semantics.
        //
        // Consistency caveat: Add currently routes round-robin (not by PK),
        // so an existing row may live on a different shard than the hash
        // would assign. Fixing that requires hash-based Add routing (future).
        // For now, Upsert is internally consistent (no duplication) but may
        // create a second copy on a new shard if the original Add landed
        // elsewhere. Documented as a known limitation.
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("No shards for table: {}", req.table_name)));
        }
        let input_batch = lance_distributed_common::ipc::ipc_to_record_batch(&req.arrow_ipc_data)
            .map_err(|e| Status::invalid_argument(format!("IPC decode: {e}")))?;
        let partitions = partition_batch_by_hash(&input_batch, &req.on_columns, routing.len())
            .map_err(|e| Status::invalid_argument(format!("partition: {e}")))?;

        let mut total_affected = 0u64;
        let mut max_version = 0u64;
        let mut errors = Vec::new();

        for (shard_idx, partition_batch) in partitions.iter().enumerate() {
            if partition_batch.num_rows() == 0 { continue; }
            let (shard_name, primary, secondary) = &routing[shard_idx];
            let chosen = if self.pool.get_healthy_client(primary).await.is_ok() {
                Some(primary.clone())
            } else if let Some(s) = secondary {
                if self.pool.get_healthy_client(s).await.is_ok() { Some(s.clone()) } else { None }
            } else { None };

            let worker_id = match chosen {
                Some(w) => w,
                None => { errors.push(format!("{}: no healthy worker", shard_name)); continue; }
            };
            let partition_ipc = lance_distributed_common::ipc::record_batch_to_ipc(partition_batch)
                .map_err(|e| Status::internal(format!("IPC encode: {e}")))?;
            if let Ok(mut client) = self.pool.get_healthy_client(&worker_id).await {
                let local_req = pb::LocalWriteRequest {
                    write_type: 2, // Upsert
                    table_name: req.table_name.clone(),
                    arrow_ipc_data: partition_ipc,
                    filter: String::new(),
                    on_columns: req.on_columns.clone(),
                    target_shard: shard_name.clone(),
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
                            errors.push(format!("{}: {}", shard_name, r.error));
                        }
                    }
                    Ok(Err(e)) => errors.push(format!("{}: {}", shard_name, e)),
                    Err(_) => errors.push(format!("{}: timeout", shard_name)),
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
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.audit(&request, "CreateTable", "<inline>", "");
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        // DDL serialization: prevent concurrent CreateTable on same name
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        // Check if table already exists
        if !self.shard_state.get_shard_routing(&req.table_name).await.is_empty() {
            return Err(Status::already_exists(format!("Table '{}' already exists", req.table_name)));
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
                // 120s budget covers IPC decode + dataset write + optional index build.
                // Without this, a stalled worker hangs the entire CreateTable indefinitely.
                handles.push(tokio::spawn(async move {
                    let result = tokio::time::timeout(
                        Duration::from_secs(120),
                        client.create_local_shard(Request::new(create_req)),
                    ).await;
                    (sn, wid, result)
                }));
            }

            mapping.insert(shard_name.clone(), vec![worker_id.clone()]);
        }

        // Gather results; on ANY failure, saga-rollback all successfully created shards.
        let mut created_ok: Vec<(String, String)> = Vec::new(); // (shard_name, worker_id)
        let mut first_err: Option<String> = None;
        for handle in handles {
            match handle.await {
                Ok((shard_name, wid, Ok(Ok(resp)))) => {
                    let r = resp.into_inner();
                    if r.error.is_empty() {
                        total_created += r.num_rows;
                        shard_uris_new.push((shard_name.clone(), r.uri));
                        created_ok.push((shard_name, wid));
                    } else if first_err.is_none() {
                        first_err = Some(format!("CreateLocalShard failed: {}", r.error));
                    }
                }
                Ok((_, _, Ok(Err(e)))) => {
                    if first_err.is_none() {
                        first_err = Some(format!("CreateLocalShard RPC: {e}"));
                    }
                }
                Ok((sn, wid, Err(_elapsed))) => {
                    if first_err.is_none() {
                        first_err = Some(format!(
                            "CreateLocalShard timeout on worker {wid} for shard {sn} (>120s)"
                        ));
                    }
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(format!("Spawn error: {e}"));
                    }
                }
            }
        }
        if let Some(err) = first_err {
            // Saga compensate: unload every successfully created shard
            warn!("CreateTable '{}' failed ({}), rolling back {} shard(s)",
                req.table_name, err, created_ok.len());
            for (shard_name, wid) in &created_ok {
                if let Ok(mut client) = self.pool.get_healthy_client(wid).await {
                    let _ = client.unload_shard(Request::new(pb::UnloadShardRequest {
                        shard_name: shard_name.clone(),
                    })).await;
                }
            }
            return Err(Status::internal(err));
        }

        // Register all shards in shard state
        self.shard_state.register_table(&req.table_name, mapping).await;

        // Register shard URIs for rebalance (in-memory)
        {
            let mut uris = self.shard_uris.write().await;
            for (shard_name, uri) in &shard_uris_new {
                uris.insert(shard_name.clone(), uri.clone());
            }
        }
        // Persist shard URIs to MetaStore (survives coordinator restart)
        if let Some(ref ms) = self.meta_state {
            let uri_map: std::collections::HashMap<String, String> =
                shard_uris_new.iter().cloned().collect();
            ms.put_shard_uris(&uri_map).await;
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
        request: Request<pb::ListTablesRequest>,
    ) -> Result<Response<pb::ListTablesResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
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
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.audit(&request, "DropTable", &request.get_ref().table_name, "");
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        // Tell all workers (primary + secondary) to unload shards for this table
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        for (shard_name, primary, secondary) in &routing {
            for wid in std::iter::once(primary).chain(secondary.iter()) {
                if let Ok(mut client) = self.pool.get_healthy_client(wid).await {
                    let _ = client.unload_shard(Request::new(pb::UnloadShardRequest {
                        shard_name: shard_name.clone(),
                    })).await;
                }
            }
        }

        // Delete the backing Lance datasets on object storage to prevent
        // cost leak (previously DropTable only removed metadata; .lance
        // directories on S3/MinIO stayed forever).
        let shard_uris: Vec<(String, String)> = {
            let uris = self.shard_uris.read().await;
            routing.iter()
                .filter_map(|(sn, _, _)| uris.get(sn).map(|u| (sn.clone(), u.clone())))
                .collect()
        };
        for (shard_name, uri) in &shard_uris {
            match delete_lance_dataset(uri, &self.storage_options).await {
                Ok(()) => info!("Deleted shard storage: {} ({})", shard_name, uri),
                Err(e) => warn!("Failed to delete shard storage {} ({}): {}", shard_name, uri, e),
            }
        }
        // Drop from URI registry (in-memory + MetaStore)
        let dropped_names: Vec<String> = routing.iter().map(|(sn, _, _)| sn.clone()).collect();
        {
            let mut uris = self.shard_uris.write().await;
            for sn in &dropped_names { uris.remove(sn); }
        }
        if let Some(ref ms) = self.meta_state {
            ms.remove_shard_uris(&dropped_names).await;
        }
        // Remove from shard state (set empty mapping)
        let empty: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        self.shard_state.register_table(&req.table_name, empty).await;
        self.metrics.remove_table(&req.table_name);
        // Clean up DDL lock to prevent leak
        self.ddl_locks.write().await.remove(&req.table_name);
        info!("Dropped table '{}' (purged {} shard(s) from storage)", req.table_name, shard_uris.len());
        Ok(Response::new(pb::DropTableResponse { error: String::new() }))
    }

    async fn create_index(
        &self,
        request: Request<pb::CreateIndexRequest>,
    ) -> Result<Response<pb::CreateIndexResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.audit(&request, "CreateIndex",
            &request.get_ref().table_name,
            &format!("column={} type={}", request.get_ref().column, request.get_ref().index_type));
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
        self.check_perm(&request, super::auth::Permission::Read)?;
        let req = request.into_inner();
        let executors = self.shard_state.executors_for_table(&req.table_name).await;
        if executors.is_empty() {
            return Err(Status::not_found(format!("Table not found: {}", req.table_name)));
        }
        // Ask first worker for table info
        if let Ok(mut client) = self.pool.get_healthy_client(&executors[0]).await
            && let Ok(resp) = client.get_table_info(Request::new(pb::GetTableInfoRequest {
                table_name: req.table_name.clone(),
            })).await {
                let r = resp.into_inner();
                return Ok(Response::new(pb::GetSchemaResponse {
                    columns: r.columns,
                    error: r.error,
                }));
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
        self.check_perm(&request, super::auth::Permission::Read)?;
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
            if let Ok(mut client) = self.pool.get_healthy_client(worker).await
                && let Ok(resp) = client.get_table_info(Request::new(pb::GetTableInfoRequest {
                    table_name: shard_name.clone(), // query by shard name for exact count
                })).await {
                    total += resp.into_inner().num_rows;
                }
        }
        Ok(Response::new(pb::CountRowsResponse {
            count: total,
            error: String::new(),
        }))
    }

    async fn rebalance(
        &self,
        request: Request<pb::RebalanceRequest>,
    ) -> Result<Response<pb::RebalanceResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.audit(&request, "Rebalance", "*", "");
        use lance_distributed_meta::shard_manager::{compute_diff, ShardPolicy};

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
                if let Some(wid) = worker
                    && let Ok(mut client) = self.pool.get_healthy_client(wid).await
                        && let Ok(resp) = client.get_table_info(Request::new(pb::GetTableInfoRequest {
                            table_name: shard_name.clone(),
                        })).await {
                            shard_sizes.insert(shard_name.clone(), resp.into_inner().num_rows);
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

            let moved: usize = to_load.values().map(|v| v.len()).sum();
            total_moved += moved as u32;

            // Phase 2: Update routing metadata BEFORE unloading old shards.
            // This ensures queries always have a valid target during the transition:
            // - New workers already loaded the shards (Phase 1)
            // - Routing now points to new workers
            // - Even if crash happens here, both old and new workers have the data
            self.shard_state.register_table(table_name, new_assignment).await;

            // Phase 3: Unload from old workers (best-effort, after routing updated)
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
                                warn!("Rebalance: unload {} from {} failed: {}", shard_name, worker_id, e);
                            }
                            Err(_) => {
                                warn!("Rebalance: unload {} from {} timed out", shard_name, worker_id);
                            }
                        }
                    }
                }
            }
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

    async fn move_shard(
        &self,
        request: Request<pb::MoveShardRequest>,
    ) -> Result<Response<pb::MoveShardResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.audit(&request, "MoveShard",
            &request.get_ref().shard_name,
            &format!("to={} force={}",
                request.get_ref().to_worker, request.get_ref().force));
        let req = request.into_inner();
        if req.shard_name.is_empty() || req.to_worker.is_empty() {
            return Err(Status::invalid_argument("shard_name and to_worker required"));
        }

        // Find the table that owns this shard and its current owner.
        let mut found: Option<(String, String)> = None; // (table_name, from_worker)
        for tbl in self.shard_state.all_tables().await {
            let routing = self.shard_state.get_shard_routing(&tbl).await;
            for (sname, primary, _sec) in routing {
                if sname == req.shard_name {
                    found = Some((tbl.clone(), primary));
                    break;
                }
            }
            if found.is_some() { break; }
        }
        let (table_name, from_worker) = found.ok_or_else(||
            Status::not_found(format!("Shard not found: {}", req.shard_name)))?;

        if from_worker == req.to_worker {
            return Ok(Response::new(pb::MoveShardResponse {
                from_worker, to_worker: req.to_worker, error: String::new(),
            }));
        }

        // Phase 1: LoadShard on destination worker.
        let shard_uri = {
            let uris = self.shard_uris.read().await;
            uris.get(&req.shard_name).cloned()
                .ok_or_else(|| Status::failed_precondition(
                    format!("Shard URI unknown for {}", req.shard_name)))?
        };
        let mut to_client = self.pool.get_healthy_client(&req.to_worker).await
            .map_err(|e| Status::unavailable(format!("to_worker {} unavailable: {e}", req.to_worker)))?;
        let load_resp = tokio::time::timeout(
            Duration::from_secs(60),
            to_client.load_shard(Request::new(pb::LoadShardRequest {
                shard_name: req.shard_name.clone(),
                uri: shard_uri,
                storage_options: self.storage_options.clone(),
            })),
        ).await
            .map_err(|_| Status::deadline_exceeded("LoadShard timeout"))?
            .map_err(|e| Status::internal(format!("LoadShard RPC: {e}")))?
            .into_inner();
        if !load_resp.error.is_empty() {
            return Err(Status::internal(format!("LoadShard failed: {}", load_resp.error)));
        }

        // Phase 2: update routing to the new owner (before unloading old, so reads
        // have a valid target during the switch).
        let current_routing = self.shard_state.get_shard_routing(&table_name).await;
        let new_mapping: std::collections::HashMap<String, Vec<String>> = current_routing.into_iter()
            .map(|(sname, primary, sec)| {
                if sname == req.shard_name {
                    let mut new_list = vec![req.to_worker.clone()];
                    if let Some(s) = sec && s != req.to_worker { new_list.push(s); }
                    (sname, new_list)
                } else {
                    let mut v = vec![primary];
                    if let Some(s) = sec { v.push(s); }
                    (sname, v)
                }
            })
            .collect();
        self.shard_state.register_table(&table_name, new_mapping).await;

        // Phase 3: UnloadShard on old worker (best-effort; ignore failures if force=true).
        if let Ok(mut from_client) = self.pool.get_healthy_client(&from_worker).await {
            let _ = from_client.unload_shard(Request::new(pb::UnloadShardRequest {
                shard_name: req.shard_name.clone(),
            })).await;
        } else if !req.force {
            warn!("MoveShard: old worker {} unreachable (shard routed to {} but not unloaded)",
                from_worker, req.to_worker);
        }

        info!("MoveShard: {} moved from {} to {}", req.shard_name, from_worker, req.to_worker);
        Ok(Response::new(pb::MoveShardResponse {
            from_worker, to_worker: req.to_worker, error: String::new(),
        }))
    }

    async fn register_worker(
        &self,
        request: Request<pb::RegisterWorkerRequest>,
    ) -> Result<Response<pb::RegisterWorkerResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        let req = request.into_inner();
        if req.worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id required"));
        }
        info!("Worker '{}' registering at {}:{}", req.worker_id, req.host, req.port);
        self.shard_state.register_executor(&req.worker_id, &req.host, req.port as u16).await;
        // Add to connection pool so health check will discover and connect to it
        self.pool.add_endpoint(&req.worker_id, &req.host, req.port as u16).await;
        Ok(Response::new(pb::RegisterWorkerResponse {
            assigned_shards: 0,
            error: String::new(),
        }))
    }

    async fn get_by_ids(
        &self,
        request: Request<pb::GetByIdsRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        if req.ids.is_empty() {
            return Ok(Response::new(pb::SearchResponse {
                arrow_ipc_data: vec![], num_rows: 0, error: String::new(),
                truncated: false, next_offset: 0, latency_ms: 0,
            }));
        }

        let t0 = std::time::Instant::now();
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("Table not found: {}", req.table_name)));
        }

        // Fan out GetByIds to all workers (IDs may be on any shard)
        let mut handles = Vec::new();
        let statuses = self.pool.worker_statuses().await;
        let healthy: std::collections::HashSet<String> = statuses.iter()
            .filter(|(_, _, _, h, _, _, _)| *h)
            .map(|(id, _, _, _, _, _, _)| id.clone())
            .collect();

        // Deduplicate worker IDs
        let mut worker_ids = std::collections::HashSet::new();
        for (_, primary, secondary) in &routing {
            if healthy.contains(primary) {
                worker_ids.insert(primary.clone());
            } else if let Some(sec) = secondary
                && healthy.contains(sec) {
                    worker_ids.insert(sec.clone());
                }
        }

        let shared_req = std::sync::Arc::new(req);
        for wid in &worker_ids {
            if let Ok(mut client) = self.pool.get_healthy_client(wid).await {
                let r = pb::GetByIdsRequest {
                    table_name: shared_req.table_name.clone(),
                    ids: shared_req.ids.clone(),
                    id_column: shared_req.id_column.clone(),
                    columns: shared_req.columns.clone(),
                };
                let worker_id = wid.clone();
                let timeout = self.query_timeout;
                handles.push(tokio::spawn(async move {
                    let result = tokio::time::timeout(timeout,
                        client.local_get_by_ids(Request::new(r))).await;
                    (worker_id, result)
                }));
            }
        }

        // Gather results
        let mut batches = Vec::new();
        for handle in handles {
            if let Ok((_, Ok(Ok(resp)))) = handle.await {
                let r = resp.into_inner();
                if r.num_rows > 0 && r.error.is_empty()
                    && let Ok(batch) = lance_distributed_common::ipc::ipc_to_record_batch(&r.arrow_ipc_data) {
                        batches.push(batch);
                    }
            }
        }

        let latency_ms = t0.elapsed().as_millis() as u64;

        if batches.is_empty() {
            return Ok(Response::new(pb::SearchResponse {
                arrow_ipc_data: vec![], num_rows: 0, error: String::new(),
                truncated: false, next_offset: 0, latency_ms,
            }));
        }

        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| Status::internal(format!("merge: {e}")))?;
        let ipc = lance_distributed_common::ipc::record_batch_to_ipc(&merged)
            .map_err(|e| Status::internal(format!("IPC: {e}")))?;

        Ok(Response::new(pb::SearchResponse {
            num_rows: merged.num_rows() as u32,
            arrow_ipc_data: ipc,
            error: String::new(),
            truncated: false, next_offset: 0, latency_ms,
        }))
    }

    async fn query(
        &self,
        request: Request<pb::QueryRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        if req.filter.is_empty() {
            return Err(Status::invalid_argument("filter required for query scan"));
        }
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let t0 = std::time::Instant::now();

        // Fan out to all workers
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("Table not found: {}", req.table_name)));
        }
        let statuses = self.pool.worker_statuses().await;
        let healthy: std::collections::HashSet<String> = statuses.iter()
            .filter(|(_, _, _, h, _, _, _)| *h)
            .map(|(id, _, _, _, _, _, _)| id.clone())
            .collect();

        let mut worker_ids = std::collections::HashSet::new();
        for (_, primary, secondary) in &routing {
            if healthy.contains(primary) { worker_ids.insert(primary.clone()); }
            else if let Some(sec) = secondary
                && healthy.contains(sec) { worker_ids.insert(sec.clone()); }
        }

        let mut handles = Vec::new();
        for wid in &worker_ids {
            if let Ok(mut client) = self.pool.get_healthy_client(wid).await {
                let r = pb::QueryRequest {
                    table_name: req.table_name.clone(),
                    filter: req.filter.clone(),
                    limit: (limit + req.offset as usize) as u32,
                    offset: 0, // workers return all, coordinator truncates
                    columns: req.columns.clone(),
                };
                let timeout = self.query_timeout;
                handles.push(tokio::spawn(async move {
                    tokio::time::timeout(timeout, client.local_query(Request::new(r))).await
                }));
            }
        }

        let mut batches = Vec::new();
        for handle in handles {
            if let Ok(Ok(Ok(resp))) = handle.await {
                let r = resp.into_inner();
                if r.num_rows > 0 && r.error.is_empty()
                    && let Ok(batch) = lance_distributed_common::ipc::ipc_to_record_batch(&r.arrow_ipc_data) {
                        batches.push(batch);
                    }
            }
        }

        let latency_ms = t0.elapsed().as_millis() as u64;
        self.metrics.record_query(t0.elapsed().as_micros() as u64, true);

        if batches.is_empty() {
            return Ok(Response::new(pb::SearchResponse {
                arrow_ipc_data: vec![], num_rows: 0, error: String::new(),
                truncated: false, next_offset: 0, latency_ms,
            }));
        }

        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| Status::internal(format!("merge: {e}")))?;

        // Apply offset + limit
        let off = req.offset as usize;
        let total = merged.num_rows();
        let sliced = if off >= total {
            merged.slice(0, 0)
        } else {
            let len = limit.min(total - off);
            merged.slice(off, len)
        };

        let ipc = lance_distributed_common::ipc::record_batch_to_ipc(&sliced)
            .map_err(|e| Status::internal(format!("IPC: {e}")))?;
        let next_off = if off + sliced.num_rows() < total { (off + sliced.num_rows()) as u32 } else { 0 };

        Ok(Response::new(pb::SearchResponse {
            num_rows: sliced.num_rows() as u32,
            arrow_ipc_data: ipc,
            error: String::new(),
            truncated: false, next_offset: next_off, latency_ms,
        }))
    }

    async fn batch_search(
        &self,
        request: Request<pb::BatchSearchRequest>,
    ) -> Result<Response<pb::BatchSearchResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        if req.query_vectors.is_empty() {
            return Ok(Response::new(pb::BatchSearchResponse { results: vec![], error: String::new() }));
        }

        let k = if req.k == 0 { 10 } else { req.k };
        let oversample_k = k * self.oversample_factor;

        // Execute each query in parallel
        let mut handles = Vec::new();
        for qv in &req.query_vectors {
            let local_req = pb::LocalSearchRequest {
                query_type: 0, // ANN
                table_name: req.table_name.clone(),
                vector_column: Some(if req.vector_column.is_empty() { "vector".to_string() } else { req.vector_column.clone() }),
                query_vector: Some(qv.clone()),
                dimension: Some(req.dimension),
                nprobes: Some(if req.nprobes == 0 { 20 } else { req.nprobes }),
                metric_type: Some(req.metric_type),
                text_column: None, query_text: None,
                k: oversample_k, filter: req.filter.clone(), columns: req.columns.clone(),
            };

            let pool = self.pool.clone();
            let shard_state = self.shard_state.clone();
            let pruner = self.pruner.clone();
            let table = req.table_name.clone();
            let timeout = self.query_timeout;
            let merge_k = k;

            handles.push(tokio::spawn(async move {
                super::scatter_gather::scatter_gather(
                    &pool, &shard_state, &pruner, &table, local_req, merge_k, true, timeout,
                ).await
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(resp)) => results.push(resp),
                Ok(Err(e)) => results.push(pb::SearchResponse {
                    arrow_ipc_data: vec![], num_rows: 0,
                    error: e.message().to_string(),
                    truncated: false, next_offset: 0, latency_ms: 0,
                }),
                Err(e) => results.push(pb::SearchResponse {
                    arrow_ipc_data: vec![], num_rows: 0,
                    error: format!("spawn: {e}"),
                    truncated: false, next_offset: 0, latency_ms: 0,
                }),
            }
        }

        Ok(Response::new(pb::BatchSearchResponse { results, error: String::new() }))
    }
}

const MAX_K: u32 = 100_000;

/// Handles needed by the orphan GC loop. Exposed as a separate struct so
/// the loop does not hold a CoordinatorService reference (avoids ownership
/// conflicts with the gRPC server taking `service` by value).
#[derive(Clone)]
pub struct OrphanGcHandle {
    pub shard_uris: Arc<tokio::sync::RwLock<std::collections::HashMap<String, String>>>,
    pub root: Option<String>,
}

/// Orphan-dataset GC loop.
///
/// Periodically lists `.lance` directories under `default_table_path` and
/// deletes those not referenced by any registered shard URI AND older than
/// `min_age_secs`. Local filesystem only for now; object-store GC would
/// need a list operation we don't wire up here.
pub async fn orphan_gc_loop(
    handle: OrphanGcHandle,
    cfg: lance_distributed_common::config::OrphanGcConfig,
    shutdown: Arc<tokio::sync::Notify>,
) {
    let interval = std::time::Duration::from_secs(cfg.interval_secs);
    let mut tick = tokio::time::interval(interval);
    tick.tick().await; // skip immediate first tick
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown.notified() => {
                log::info!("orphan_gc loop stopping (shutdown)");
                return;
            }
        }
        let root = match handle.root.as_ref() {
            Some(r) => r.clone(),
            None => continue,
        };
        // Only support local fs (object-store listing is a follow-up).
        if root.starts_with("s3://") || root.starts_with("gs://") || root.starts_with("az://") {
            log::debug!("orphan_gc: skipping object-store root {} (not yet supported)", root);
            continue;
        }
        let entries = match tokio::fs::read_dir(&root).await {
            Ok(e) => e,
            Err(e) => { log::debug!("orphan_gc: read_dir {}: {}", root, e); continue; }
        };
        let registered: std::collections::HashSet<String> =
            handle.shard_uris.read().await.values().cloned().collect();
        let now = std::time::SystemTime::now();
        let min_age = std::time::Duration::from_secs(cfg.min_age_secs);
        let mut purged = 0u64;
        let mut entries = entries;
        loop {
            let ent = match entries.next_entry().await {
                Ok(Some(e)) => e,
                _ => break,
            };
            let path = ent.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if !name.ends_with(".lance") { continue; }
            let full = path.to_string_lossy().to_string();
            // Registered URIs may or may not end with ".lance" — compare stems.
            let stem = name.strip_suffix(".lance").unwrap_or(&name);
            let referenced = registered.iter().any(|u|
                u == &full
                || u.ends_with(&format!("/{}", name))
                || u.ends_with(&format!("/{}", stem)));
            if referenced { continue; }

            // Age check
            let meta = match tokio::fs::metadata(&path).await { Ok(m) => m, Err(_) => continue };
            let modified = meta.modified().ok();
            if let Some(mt) = modified
                && let Ok(age) = now.duration_since(mt)
                    && age < min_age { continue; }
            match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => { log::info!("orphan_gc: purged {}", full); purged += 1; }
                Err(e) => log::warn!("orphan_gc: failed to purge {}: {}", full, e),
            }
        }
        if purged > 0 {
            log::info!("orphan_gc: cycle complete, purged {} dataset(s)", purged);
        }
    }
}

/// Delete a Lance dataset (the .lance directory) from local fs or object store.
/// Missing datasets are a no-op. Used by DropTable to prevent storage leak.
async fn delete_lance_dataset(
    uri: &str,
    storage_options: &std::collections::HashMap<String, String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let is_remote = uri.starts_with("s3://") || uri.starts_with("gs://") || uri.starts_with("az://");
    if !is_remote {
        let path = uri.strip_prefix("file://").unwrap_or(uri);
        if tokio::fs::metadata(path).await.is_ok() {
            tokio::fs::remove_dir_all(path).await?;
        }
        return Ok(());
    }
    let (parent, name) = lance_distributed_common::auto_shard::split_parent_uri(uri);
    let db = lancedb::connect(&parent)
        .storage_options(storage_options.iter().map(|(k, v)| (k.clone(), v.clone())))
        .execute()
        .await?;
    if let Err(e) = db.drop_table(&name, &[]).await {
        let msg = e.to_string().to_lowercase();
        if msg.contains("not found") || msg.contains("does not exist") || msg.contains("no such") {
            return Ok(());
        }
        return Err(Box::new(e));
    }
    Ok(())
}

/// Hash-partition a RecordBatch into N sub-batches by the values of `on_columns`.
/// Returns a Vec of length N (may contain empty batches for shards with no rows).
/// Used by upsert_rows so each row is sent to exactly one shard.
fn partition_batch_by_hash(
    batch: &arrow::array::RecordBatch,
    on_columns: &[String],
    num_shards: usize,
) -> Result<Vec<arrow::array::RecordBatch>, String> {
    use arrow::array::{Array, UInt64Array};
    use arrow::compute::take;
    use std::hash::{Hash, Hasher};

    if num_shards == 0 { return Err("num_shards=0".into()); }
    if num_shards == 1 { return Ok(vec![batch.clone()]); }

    // Resolve key column indices
    let schema = batch.schema();
    let key_indices: Vec<usize> = on_columns.iter()
        .map(|c| schema.index_of(c).map_err(|e| format!("column '{c}': {e}")))
        .collect::<Result<_, _>>()?;

    // Compute shard index for each row via a stable hash of the key values.
    let nrows = batch.num_rows();
    let mut buckets: Vec<Vec<u64>> = (0..num_shards).map(|_| Vec::new()).collect();
    for row in 0..nrows {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for &ci in &key_indices {
            let col = batch.column(ci);
            // Hash via string representation of scalar for type-agnostic stability.
            // (Not the fastest path, but correct for any primitive/string/binary.)
            let formatted = arrow::util::display::array_value_to_string(col, row)
                .map_err(|e| format!("hash row {row}: {e}"))?;
            formatted.hash(&mut hasher);
        }
        let shard = (hasher.finish() % num_shards as u64) as usize;
        buckets[shard].push(row as u64);
    }

    // Materialize each bucket as a RecordBatch via `take`.
    let mut out = Vec::with_capacity(num_shards);
    for indices in buckets {
        if indices.is_empty() {
            out.push(batch.slice(0, 0));
            continue;
        }
        let idx_arr = UInt64Array::from(indices);
        let cols: Vec<std::sync::Arc<dyn Array>> = batch.columns().iter()
            .map(|c| take(c.as_ref(), &idx_arr, None).map_err(|e| format!("take: {e}")))
            .collect::<Result<_, _>>()?;
        let rb = arrow::array::RecordBatch::try_new(schema.clone(), cols)
            .map_err(|e| format!("RecordBatch::try_new: {e}"))?;
        out.push(rb);
    }
    Ok(out)
}

/// Split a RecordBatch into N roughly-equal partitions for auto-sharding.
fn split_batch(batch: &arrow::array::RecordBatch, n: usize) -> Vec<arrow::array::RecordBatch> {
    if n <= 1 {
        return vec![batch.clone()];
    }
    let total = batch.num_rows();
    let chunk_size = total.div_ceil(n);
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

/// Validate offset+k against per-call limit and oversample headroom.
fn validate_offset_k(offset: u32, k: u32, max_k: u32, oversample_factor: u32) -> std::result::Result<(u32, u32), Status> {
    if k == 0 { return Err(Status::invalid_argument("k must be > 0")); }
    let total = offset.checked_add(k).ok_or_else(||
        Status::invalid_argument(format!("offset({offset}) + k({k}) overflow")))?;
    if total > max_k {
        return Err(Status::invalid_argument(format!(
            "offset+k = {} exceeds max_k {}", total, max_k
        )));
    }
    total.checked_mul(oversample_factor).ok_or_else(||
        Status::invalid_argument("(offset+k)*oversample overflow".to_string()))?;
    Ok((total, total * oversample_factor))
}

/// Apply offset slicing and response-size cap to a merged SearchResponse.
/// - Decodes IPC, slices [offset .. offset+k] (or fewer if results limited).
/// - If serialized payload exceeds `max_bytes`, drops tail rows until under cap
///   and sets `truncated=true`. `next_offset` set when more rows likely remain.
fn apply_pagination_and_cap(
    resp: pb::SearchResponse,
    offset: u32,
    k: u32,
    max_bytes: usize,
) -> pb::SearchResponse {
    if !resp.error.is_empty() || resp.arrow_ipc_data.is_empty() {
        return resp;
    }
    let batch = match lance_distributed_common::ipc::ipc_to_record_batch(&resp.arrow_ipc_data) {
        Ok(b) => b,
        Err(_) => return resp,
    };
    let total_rows = batch.num_rows();
    let off = offset as usize;
    if off >= total_rows {
        return pb::SearchResponse {
            arrow_ipc_data: vec![], num_rows: 0, error: String::new(),
            truncated: false, next_offset: 0, latency_ms: 0,
        };
    }
    let want = (k as usize).min(total_rows - off);
    let mut sliced = batch.slice(off, want);
    let next_offset = if off + want < total_rows { (off + want) as u32 } else { 0 };

    // Encode and cap by bytes
    let mut ipc = match lance_distributed_common::ipc::record_batch_to_ipc(&sliced) {
        Ok(b) => b,
        Err(_) => return resp,
    };
    let mut truncated = false;
    if max_bytes > 0 && ipc.len() > max_bytes && sliced.num_rows() > 1 {
        // Halve until under cap (binary shrink)
        let mut rows = sliced.num_rows();
        while rows > 1 {
            rows = rows.div_ceil(2);
            let trial = sliced.slice(0, rows);
            if let Ok(buf) = lance_distributed_common::ipc::record_batch_to_ipc(&trial)
                && buf.len() <= max_bytes {
                    sliced = trial; ipc = buf; truncated = true; break;
                }
        }
    }
    pb::SearchResponse {
        num_rows: sliced.num_rows() as u32,
        arrow_ipc_data: ipc,
        error: String::new(),
        truncated,
        next_offset: if truncated { (off + sliced.num_rows()) as u32 } else { next_offset },
        latency_ms: 0, // populated by caller after apply_pagination_and_cap
    }
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

    // ---------- Pure helper function tests ----------
    //
    // These are the request-validation and batch-partitioning helpers that
    // back every Search / Insert RPC. Getting the k/offset math wrong is
    // how you leak memory, crash on overflow, or silently serve wrong
    // pagination windows. Unit tests here catch those bugs before they
    // reach the gRPC layer.

    #[test]
    fn test_validate_k_rejects_zero() {
        assert!(validate_k(0, 1).is_err());
    }

    #[test]
    fn test_validate_k_rejects_over_max() {
        assert!(validate_k(MAX_K + 1, 1).is_err());
        // Exactly at MAX_K must succeed.
        assert_eq!(validate_k(MAX_K, 1).unwrap(), MAX_K);
    }

    #[test]
    fn test_validate_k_detects_oversample_overflow() {
        // 1 * u32::MAX overflows when multiplied by oversample=2.
        let err = validate_k(u32::MAX / 2 + 1, 2).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_validate_offset_k_rejects_zero_k() {
        assert!(validate_offset_k(5, 0, 100, 1).is_err());
    }

    #[test]
    fn test_validate_offset_k_rejects_overflow() {
        // offset + k overflows u32.
        assert!(validate_offset_k(u32::MAX, 2, 1_000_000, 1).is_err());
    }

    #[test]
    fn test_validate_offset_k_rejects_exceeds_max() {
        // offset+k > max_k.
        assert!(validate_offset_k(90, 20, 100, 1).is_err());
    }

    #[test]
    fn test_validate_offset_k_success() {
        let (total, fetch) = validate_offset_k(10, 20, 1000, 2).unwrap();
        assert_eq!(total, 30);
        assert_eq!(fetch, 60, "with oversample=2, fetch is (offset+k)*2");
    }

    #[test]
    fn test_split_batch_n_le_one() {
        let batch = make_i32_batch(vec![1, 2, 3, 4, 5]);
        let parts = split_batch(&batch, 1);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].num_rows(), 5);

        // n=0 is treated the same as n=1 (the caller already validated upstream).
        let parts = split_batch(&batch, 0);
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn test_split_batch_even_chunks() {
        let batch = make_i32_batch((0..10).collect());
        let parts = split_batch(&batch, 2);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].num_rows(), 5);
        assert_eq!(parts[1].num_rows(), 5);
    }

    #[test]
    fn test_split_batch_uneven_chunks() {
        // 7 rows / 3 shards → [3, 3, 1] (ceil-div chunk size).
        let batch = make_i32_batch((0..7).collect());
        let parts = split_batch(&batch, 3);
        assert_eq!(parts.len(), 3);
        let sizes: Vec<_> = parts.iter().map(|b| b.num_rows()).collect();
        assert_eq!(sizes.iter().sum::<usize>(), 7);
        // All-non-empty — we filter out empty partitions.
        assert!(sizes.iter().all(|&s| s > 0));
    }

    #[test]
    fn test_split_batch_more_shards_than_rows() {
        // 3 rows / 5 shards → only 3 non-empty partitions (empties filtered).
        let batch = make_i32_batch(vec![1, 2, 3]);
        let parts = split_batch(&batch, 5);
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_partition_batch_by_hash_single_shard() {
        let batch = make_i32_batch(vec![1, 2, 3]);
        let out = partition_batch_by_hash(&batch, &["id".into()], 1).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[test]
    fn test_partition_batch_by_hash_zero_is_error() {
        let batch = make_i32_batch(vec![1]);
        assert!(partition_batch_by_hash(&batch, &["id".into()], 0).is_err());
    }

    #[test]
    fn test_partition_batch_by_hash_unknown_column_errors() {
        let batch = make_i32_batch(vec![1, 2, 3]);
        let err = partition_batch_by_hash(&batch, &["nope".into()], 4).unwrap_err();
        assert!(err.contains("column 'nope'"), "error should name the bad column");
    }

    #[test]
    fn test_partition_batch_by_hash_partitions_all_rows() {
        // All rows must end up in some partition — no drops.
        let batch = make_i32_batch((0..100).collect());
        let out = partition_batch_by_hash(&batch, &["id".into()], 4).unwrap();
        assert_eq!(out.len(), 4);
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_partition_batch_by_hash_deterministic() {
        // Same input must land in the same partition layout every call —
        // otherwise upsert would silently send the same row to two shards.
        let batch = make_i32_batch((0..50).collect());
        let a = partition_batch_by_hash(&batch, &["id".into()], 3).unwrap();
        let b = partition_batch_by_hash(&batch, &["id".into()], 3).unwrap();
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.num_rows(), y.num_rows(),
                "hash partitioning must be stable across calls");
        }
    }

    /// Core invariant that makes the Add/Upsert unification safe: for a given
    /// (id, on_columns, num_shards), the row always lands in the same bucket
    /// index regardless of what OTHER rows are in the batch. Without this,
    /// an initial Add of 1000 rows and a later Upsert of 1 row for the same
    /// id could still disagree on which shard owns that id.
    #[test]
    fn test_partition_batch_by_hash_single_key_stable_across_batch_sizes() {
        // Bucket index for id=42 in the isolation batch.
        let solo = make_i32_batch(vec![42]);
        let solo_parts = partition_batch_by_hash(&solo, &["id".into()], 4).unwrap();
        let id_42_shard = solo_parts.iter().position(|b| b.num_rows() == 1).unwrap();

        // Now id=42 embedded in a larger batch — the shard index of id=42
        // must match what we computed in isolation.
        let mixed_ids: Vec<i32> = (0..200).map(|i| if i == 137 { 42 } else { i }).collect();
        let mixed = make_i32_batch(mixed_ids);
        let mixed_parts = partition_batch_by_hash(&mixed, &["id".into()], 4).unwrap();

        use arrow::array::Int32Array;
        let found_in = mixed_parts.iter().position(|p| {
            let col = p.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..col.len()).any(|i| col.value(i) == 42)
        }).unwrap();
        assert_eq!(found_in, id_42_shard,
            "id=42 must land in the same shard whether batch has 1 row or 200");
    }

    #[test]
    fn test_apply_pagination_passthrough_on_error_response() {
        // Error responses must not be decoded/re-encoded — preserve as-is.
        let resp = pb::SearchResponse {
            arrow_ipc_data: vec![],
            num_rows: 0,
            error: "upstream failure".to_string(),
            truncated: false, next_offset: 0, latency_ms: 5,
        };
        let out = apply_pagination_and_cap(resp.clone(), 0, 10, 0);
        assert_eq!(out.error, "upstream failure");
        assert_eq!(out.latency_ms, 5);
    }

    #[test]
    fn test_apply_pagination_offset_past_total() {
        // offset beyond total_rows must return an empty response (not panic,
        // not a negative slice). next_offset is 0 because nothing more to fetch.
        let batch = make_i32_batch((0..5).collect());
        let ipc = lance_distributed_common::ipc::record_batch_to_ipc(&batch).unwrap();
        let resp = pb::SearchResponse {
            arrow_ipc_data: ipc,
            num_rows: 5, error: String::new(),
            truncated: false, next_offset: 0, latency_ms: 0,
        };
        let out = apply_pagination_and_cap(resp, 100, 10, 0);
        assert_eq!(out.num_rows, 0);
        assert_eq!(out.next_offset, 0);
    }

    #[test]
    fn test_apply_pagination_sets_next_offset_when_more_rows_remain() {
        // 100 rows, offset=10, k=20 → slice [10..30], next_offset=30 (still 70 to go).
        let batch = make_i32_batch((0..100).collect());
        let ipc = lance_distributed_common::ipc::record_batch_to_ipc(&batch).unwrap();
        let resp = pb::SearchResponse {
            arrow_ipc_data: ipc, num_rows: 100, error: String::new(),
            truncated: false, next_offset: 0, latency_ms: 0,
        };
        let out = apply_pagination_and_cap(resp, 10, 20, 0);
        assert_eq!(out.num_rows, 20);
        assert_eq!(out.next_offset, 30);
        assert!(!out.truncated);
    }

    #[test]
    fn test_apply_pagination_last_page_next_offset_zero() {
        // Fetching the final window → no next_offset.
        let batch = make_i32_batch((0..100).collect());
        let ipc = lance_distributed_common::ipc::record_batch_to_ipc(&batch).unwrap();
        let resp = pb::SearchResponse {
            arrow_ipc_data: ipc, num_rows: 100, error: String::new(),
            truncated: false, next_offset: 0, latency_ms: 0,
        };
        let out = apply_pagination_and_cap(resp, 90, 20, 0);
        assert_eq!(out.num_rows, 10, "requested 20 but only 10 remain");
        assert_eq!(out.next_offset, 0);
    }

    fn make_i32_batch(values: Vec<i32>) -> arrow::array::RecordBatch {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let arr: ArrayRef = Arc::new(Int32Array::from(values));
        arrow::array::RecordBatch::try_new(schema, vec![arr]).unwrap()
    }
}
