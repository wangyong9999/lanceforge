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
    /// #5.1 Partition pruning: per-table hash-partition columns
    /// declared at CreateTable. Read-side lookup on AnnSearch/
    /// FtsSearch/Hybrid with a `col = val` filter routes to a single
    /// shard when the column is in on_columns. Populated from
    /// MetaStore at startup + on each CreateTable.
    table_on_columns: Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<String>>>>,
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
    /// Write-idempotency dedup cache (H3). Maps client-supplied request_id
    /// to (inserted_at, cached WriteResponse). Entries older than
    /// `dedup_ttl` are purged by a background reaper; new requests with a
    /// known id replay the cached response without re-executing the write.
    /// Empty request_id skips the cache entirely — 0.1 behaviour.
    write_dedup: Arc<tokio::sync::RwLock<std::collections::HashMap<String, (std::time::Instant, pb::WriteResponse)>>>,
    /// Optional persistent audit sink (G7). When set, every `audit()` call
    /// also appends a JSONL record to the configured path (local file or
    /// OBS URI).
    audit_sink: Option<lance_distributed_common::audit::AuditSink>,
    /// #5.2 DDL lease holder identifier for multi-coordinator
    /// deployments. Defaults to `coord-{pid}`. qn_cp::run overrides
    /// with the operator-provided instance_id so lease logs identify
    /// the owning pod cleanly. Used as the lease `holder` field in
    /// `ddl_lease/{table}`.
    instance_id: String,
    /// Phase 2 single-dataset path: cached Lance Dataset handles for
    /// manifest reads during fragment-fanout dispatch. Opened lazily
    /// on first query per table; not refreshed for MVP (users restart
    /// coord after writes if they need the new fragment set).
    dataset_cache: Arc<super::fragment_dispatch::DatasetCache>,
}

/// How long a completed write's request_id stays deduplicatable.
/// Clients that retry within this window get the cached result; retries
/// outside it re-execute the write. 5 minutes covers typical network /
/// coordinator-restart retry loops without bloating memory on idle keys.
const WRITE_DEDUP_TTL: Duration = Duration::from_secs(300);

/// Background reaper interval — scanned every 60 s; pays ~O(N) per sweep
/// over the current in-memory dedup map.
const WRITE_DEDUP_REAP_INTERVAL: Duration = Duration::from_secs(60);

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

    /// Install per-worker role preferences from the cluster config (B2.2).
    /// Call once at startup after the service is built and before the
    /// gRPC server starts serving.
    pub async fn install_executor_roles(
        &self,
        executors: &[lance_distributed_common::config::ExecutorConfig],
    ) {
        let map: std::collections::HashMap<String, lance_distributed_common::config::ExecutorRole> =
            executors.iter().map(|e| (e.id.clone(), e.role)).collect();
        self.pool.set_roles(map).await;
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
        // #5.1: also recover per-table on_columns for read-side pruning.
        let persisted_on_cols = meta_state.get_table_on_columns().await;
        let on_cols_count = persisted_on_cols.len();

        let meta_arc = Arc::new(meta_state);
        info!("Using MetaStore at {} ({} tables, {} shard URIs recovered, {} on_columns entries)",
            metadata_path, meta_arc.all_tables().await.len(), recovered_count, on_cols_count);

        let mut svc = Self::with_shard_state(config, query_timeout, meta_arc.clone() as Arc<dyn ShardState>);
        // Merge persisted URIs into the in-memory map (YAML URIs already loaded)
        {
            let mut uris = svc.shard_uris.write().await;
            for (k, v) in persisted_uris {
                uris.entry(k).or_insert(v);
            }
        }
        // Merge persisted on_columns into the in-memory cache.
        {
            let mut on_cols = svc.table_on_columns.write().await;
            for (t, cols) in persisted_on_cols {
                on_cols.entry(t).or_insert(cols);
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
            table_on_columns: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            auth: None,
            max_response_bytes: config.server.max_response_bytes,
            max_k: config.server.max_k,
            slow_query_ms: config.server.slow_query_ms,
            bg_shutdown,
            meta_state: None,
            ddl_locks: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            write_dedup: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            audit_sink: None,
            instance_id: format!("coord-{}", std::process::id()),
            dataset_cache: Arc::new(super::fragment_dispatch::DatasetCache::default()),
        }
    }

    /// Set the DDL lease holder id for multi-coord deployments. qn_cp
    /// calls this with the operator-provided instance_id so lease
    /// held-by diagnostics carry the pod name / cluster id.
    pub fn with_instance_id(mut self, id: String) -> Self {
        self.instance_id = id;
        self
    }

    /// Install a persistent audit sink (G7). Every subsequent `audit()`
    /// call writes a JSONL record to the sink in addition to the usual
    /// info! log line. Call once at startup before the gRPC server takes
    /// ownership.
    pub fn with_audit_sink(mut self, sink: lance_distributed_common::audit::AuditSink) -> Self {
        self.audit_sink = Some(sink);
        self
    }

    /// Attach an auth interceptor for RBAC enforcement at method level.
    pub fn with_auth(mut self, auth: Arc<super::auth::ApiKeyInterceptor>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// #5.1 Look up the declared hash-partition columns for a table.
    /// Empty = no pruning. Populated at CreateTable + restored from
    /// MetaStore on startup (see `with_meta_state`).
    async fn on_columns_for(&self, table: &str) -> Vec<String> {
        self.table_on_columns
            .read()
            .await
            .get(table)
            .cloned()
            .unwrap_or_default()
    }

    /// #5.5 Bump the global commit sequence via MetaStore-backed
    /// atomic increment. Returns 0 when no MetaStore is configured
    /// (static deployments have no durable counter). Called at the
    /// tail of every successful write path so clients can thread
    /// the returned seq into subsequent read requests for read-
    /// your-own-writes.
    async fn bump_commit_seq(&self) -> u64 {
        match &self.meta_state {
            Some(ms) => ms.incr_commit_seq().await,
            None => 0,
        }
    }

    /// #5.5 Current global commit_seq without mutating it.
    async fn current_commit_seq(&self) -> u64 {
        match &self.meta_state {
            Some(ms) => ms.get_commit_seq().await,
            None => 0,
        }
    }

    /// #5.5 min_commit_seq guard for reads.
    async fn check_min_commit_seq(&self, min: u64) -> Result<(), Status> {
        if min == 0 {
            return Ok(());
        }
        let current = self.current_commit_seq().await;
        if current < min {
            return Err(Status::failed_precondition(format!(
                "min_commit_seq {} not satisfied; current = {}",
                min, current
            )));
        }
        Ok(())
    }

    /// #5.4 Schema-version consistency guard.
    ///
    /// If `min > 0`, verify `current_schema_version(table) >= min`.
    /// Fails with FailedPrecondition when the coord's view is
    /// behind. Used by read RPCs for read-your-DDL-writes after a
    /// client AlterTable returned `new_schema_version = N`.
    ///
    /// No-op when `min == 0` (default) or no MetaStore is configured
    /// (static-config deployments don't track schema versions).
    async fn check_min_schema_version(&self, table: &str, min: u64) -> Result<(), Status> {
        if min == 0 {
            return Ok(());
        }
        let Some(ref ms) = self.meta_state else {
            return Err(Status::failed_precondition(
                "min_schema_version requires a configured metadata_path",
            ));
        };
        let schema_store =
            lance_distributed_meta::schema_store::SchemaStore::new(ms.store());
        let current = schema_store
            .latest_version(table)
            .await
            .map_err(|e| Status::internal(format!("schema_store: {e}")))?
            .unwrap_or(0);
        if current < min {
            return Err(Status::failed_precondition(format!(
                "min_schema_version {} not satisfied; current = {}",
                min, current
            )));
        }
        Ok(())
    }

    /// #5.2 Acquire a cross-coordinator DDL lease for `table`. Only
    /// active when MetaStore is configured — standalone / static
    /// deployments fall back to the per-coord `ddl_locks`. Translates
    /// `AcquireError::Held` into `Status::failed_precondition` with
    /// the current holder id so clients get an actionable retry
    /// signal. `None` return means no MetaStore; caller proceeds
    /// with only the per-coord lock.
    async fn acquire_ddl_lease(
        &self,
        table: &str,
    ) -> Result<Option<lance_distributed_meta::ddl_lease::DdlLease>, Status> {
        let Some(ref ms) = self.meta_state else {
            return Ok(None);
        };
        match lance_distributed_meta::ddl_lease::acquire(
            ms.store(),
            table,
            &self.instance_id,
            None,
        )
        .await
        {
            Ok(lease) => Ok(Some(lease)),
            Err(lance_distributed_meta::ddl_lease::AcquireError::Held { holder, age_ns }) => {
                Err(Status::failed_precondition(format!(
                    "DDL in progress on '{table}' (held by '{holder}', {}ms old). Retry shortly.",
                    age_ns / 1_000_000
                )))
            }
            Err(e) => Err(Status::internal(format!("ddl lease: {e}"))),
        }
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

    /// Enforce API-key ↔ namespace binding (G5). Called after `check_perm`
    /// on every RPC that names a table. Callers whose key has no namespace
    /// binding (or no auth configured) pass through; callers bound to a
    /// namespace get `PermissionDenied` unless the table name begins with
    /// `{namespace}/`. This is a name-prefix boundary, not a storage-layer
    /// one — see `docs/SECURITY.md` §5 for scope.
    fn check_ns<T>(&self, req: &Request<T>, table: &str) -> Result<(), Status> {
        if let Some(a) = &self.auth {
            a.check_namespace(req, table)?;
        }
        Ok(())
    }

    /// Emit an audit log line for a sensitive action (write/admin). Level=info.
    /// Format: `audit op=<op> principal=<short_key> target=<table> details=<...>`.
    /// Also submits a structured JSONL record to the configured audit sink
    /// (G7) when one is installed, so DDL + write events are durably
    /// archived for SIEM ingestion.
    ///
    /// If the caller passed a W3C `traceparent` metadata header (G10) the
    /// extracted trace_id is added to both the stdout line and the JSONL
    /// record so cross-process log correlation works without a full OTEL
    /// exporter pipeline.
    fn audit<T>(&self, req: &Request<T>, op: &str, target: &str, details: &str) {
        let principal = self.auth.as_ref()
            .map(|a| a.principal(req))
            .unwrap_or_else(|| "no-auth".to_string());
        let trace_id = extract_trace_id(req);
        let trace_fragment = match &trace_id {
            Some(t) => format!(" trace_id={t}"),
            None => String::new(),
        };
        info!("audit op={} principal={} target={} details={}{}",
              op, principal, target, details, trace_fragment);
        if let Some(ref sink) = self.audit_sink {
            // F9: dual-write the trace_id — both as the dedicated JSONL
            // field (SIEM-friendly, the 0.3 authoritative source) and
            // embedded in `details` as `trace_id=<hex>` (back-compat for
            // SIEM parsers built against 0.2.0-beta.1). One release
            // window, then 0.3 drops the details substring.
            let details_with_trace = match &trace_id {
                Some(t) => format!("{details} trace_id={t}"),
                None => details.to_string(),
            };
            let record = lance_distributed_common::audit::AuditRecord::new(
                op, &principal, target, &details_with_trace,
            ).with_trace_id(trace_id);
            sink.submit(record);
        }
    }
}

/// Parse the W3C `traceparent` metadata header and return the middle
/// `trace_id` segment (32 hex chars). Returns None when the header is
/// missing or doesn't match the shape `version-trace-span-flags`.
/// Version byte is not validated — every caller that emits a traceparent
/// uses `00` today, but a future `01` should still be parseable here.
/// Corrupt shapes return None instead of panicking — we treat bad
/// traceparent as "no tracing" rather than failing the RPC.
fn extract_trace_id<T>(req: &Request<T>) -> Option<String> {
    let raw = req.metadata().get("traceparent")?.to_str().ok()?;
    let mut parts = raw.split('-');
    parts.next()?;                  // version
    let trace = parts.next()?;      // 32 hex chars
    if trace.len() != 32 || !trace.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(trace.to_string())
}

/// Keep audit-log line bounded when the payload contains a long filter
/// expression or similar. Chars, not bytes — we don't want to split UTF-8.
fn truncate_for_audit(s: &str) -> String {
    const MAX_LEN: usize = 200;
    if s.chars().count() <= MAX_LEN {
        return s.to_string();
    }
    let truncated: String = s.chars().take(MAX_LEN).collect();
    format!("{truncated}…(+{} chars)", s.chars().count() - MAX_LEN)
}

#[cfg(test)]
mod audit_util_tests {
    use super::truncate_for_audit;

    #[test]
    fn passthrough_under_limit() {
        assert_eq!(truncate_for_audit("short"), "short");
    }

    #[test]
    fn truncates_long_ascii() {
        let s = "a".repeat(250);
        let out = truncate_for_audit(&s);
        assert!(out.starts_with(&"a".repeat(200)));
        assert!(out.contains("+50 chars"));
    }

    #[test]
    fn counts_chars_not_bytes_on_unicode() {
        // 200 '中' characters (3 bytes each in UTF-8) must pass through
        // without truncation — chars not bytes.
        let s = "中".repeat(200);
        assert_eq!(truncate_for_audit(&s), s);
        let s2 = "中".repeat(201);
        assert!(truncate_for_audit(&s2).contains("+1 chars"));
    }
}

impl CoordinatorService {

    /// Check the dedup cache for a prior response to `request_id`. Empty
    /// ids skip the cache. Expired entries are not returned; they'll be
    /// cleaned up by the reaper (or lazily by the next write with the
    /// same id — see `record_dedup`). Returns the cached response if
    /// still live within TTL, None otherwise.
    async fn check_dedup(&self, request_id: &str) -> Option<pb::WriteResponse> {
        if request_id.is_empty() {
            return None;
        }
        let cache = self.write_dedup.read().await;
        let (inserted_at, resp) = cache.get(request_id)?;
        if inserted_at.elapsed() <= WRITE_DEDUP_TTL {
            Some(resp.clone())
        } else {
            None
        }
    }

    /// Record a freshly-produced response under `request_id`. No-op on
    /// empty id. Cached responses are kept for `WRITE_DEDUP_TTL` and
    /// returned verbatim on retry.
    async fn record_dedup(&self, request_id: &str, resp: &pb::WriteResponse) {
        if request_id.is_empty() {
            return;
        }
        self.write_dedup.write().await.insert(
            request_id.to_string(),
            (std::time::Instant::now(), resp.clone()),
        );
    }

    /// Spawn the dedup-cache reaper. Runs until `bg_shutdown` fires.
    /// Takes `&self` (not `Arc<Self>`) so callers don't have to Arc the
    /// whole service — the cache and the shutdown Notify are already Arc
    /// fields and cloned into the background task directly.
    pub fn spawn_dedup_reaper(&self) {
        let cache = self.write_dedup.clone();
        let stop = self.bg_shutdown.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WRITE_DEDUP_REAP_INTERVAL);
            tick.tick().await; // skip immediate first tick
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = stop.notified() => {
                        info!("write dedup reaper stopping (shutdown)");
                        return;
                    }
                }
                let mut guard = cache.write().await;
                let before = guard.len();
                guard.retain(|_, (inserted, _)| inserted.elapsed() <= WRITE_DEDUP_TTL);
                let after = guard.len();
                if before != after {
                    log::debug!("write dedup: reaped {} expired entries, {} live", before - after, after);
                }
            }
        });
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
                // D8: per-shard write latency, mirrors H13 on the read
                // path. Lets operators see which shard is the outlier
                // when a write spikes — the usual culprit is the shard
                // holding the largest manifest.
                let w_start = std::time::Instant::now();
                let result = tokio::time::timeout(
                    self.query_timeout,
                    client.execute_local_write(Request::new(local_req)),
                ).await;
                let w_ms = w_start.elapsed().as_millis() as u64;
                if self.slow_query_ms > 0 && w_ms >= self.slow_query_ms {
                    warn!("slow_write shard={} worker={} elapsed_ms={}",
                          shard_name, worker_id, w_ms);
                }
                match result {
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
                    Err(_) => errors.push(format!("{}: timeout ({}ms)", shard_name, w_ms)),
                }
            }
        }

        let error = if errors.is_empty() { String::new() } else { errors.join("; ") };
        // #5.5 Bump global commit_seq on successful write + surface
        // to client. No MetaStore → seq stays 0.
        let commit_seq = if error.is_empty() {
            self.bump_commit_seq().await
        } else {
            0
        };
        Ok(Response::new(pb::WriteResponse {
            affected_rows: total_affected,
            new_version: max_version,
            error,
            commit_seq,
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
        self.check_ns(&request, &request.get_ref().table_name)?;
        // #5.4 Schema-version guard.
        self.check_min_schema_version(
            &request.get_ref().table_name,
            request.get_ref().min_schema_version,
        )
        .await?;
        self.check_min_commit_seq(request.get_ref().min_commit_seq).await?;
        // B1: snapshot the incoming trace_id before into_inner() consumes
        // the Request. None when the client didn't send a traceparent.
        let trace_id = extract_trace_id(&request);
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

        let dimension = validate_vector_dimension(&query_vector, req.dimension)?;

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
            fragment_ids: vec![],
            shard_uri: None,
        };

        let t0 = std::time::Instant::now();
        let on_cols = self.on_columns_for(&req.table_name).await;

        // Phase 2: single-dataset tables (one ShardConfig entry) take
        // the fragment-fanout path — coord enumerates Lance fragments,
        // HRW-routes subsets to healthy workers, each worker lazy-opens
        // via the phase-1 shard_uri protocol. Multi-shard tables keep
        // scatter_gather. The routing lookup is identical to what
        // scatter_gather does first thing, so the branch cost is a
        // single HashMap read.
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        let single_shard_uri: Option<String> = if routing.len() == 1 {
            self.shard_uris.read().await.get(&routing[0].0).cloned()
        } else {
            None
        };

        let result = if let Some(uri) = single_shard_uri {
            super::fragment_dispatch::dispatch_fragment_fanout(
                &self.pool,
                &self.dataset_cache,
                &uri,
                &req.table_name,
                local_req,
                merge_limit,
                self.query_timeout,
                trace_id,
            )
            .await
        } else {
            super::scatter_gather::scatter_gather(
                &self.pool, &self.shard_state, &self.pruner,
                &req.table_name, local_req, merge_limit, true, self.query_timeout, trace_id,
                &on_cols,
            ).await
        };
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
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.check_min_schema_version(
            &request.get_ref().table_name,
            request.get_ref().min_schema_version,
        )
        .await?;
        self.check_min_commit_seq(request.get_ref().min_commit_seq).await?;
        let trace_id = extract_trace_id(&request);
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
            fragment_ids: vec![],
            shard_uri: None,
        };

        let t0 = std::time::Instant::now();
        let on_cols = self.on_columns_for(&req.table_name).await;
        let result = super::scatter_gather::scatter_gather(
            &self.pool, &self.shard_state, &self.pruner,
            &req.table_name, local_req, merge_limit, false, self.query_timeout, trace_id,
            &on_cols,
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
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.check_min_schema_version(
            &request.get_ref().table_name,
            request.get_ref().min_schema_version,
        )
        .await?;
        self.check_min_commit_seq(request.get_ref().min_commit_seq).await?;
        let trace_id = extract_trace_id(&request);
        let _permit = self.query_semaphore.try_acquire().map_err(|_| {
            Status::resource_exhausted(format!("Too many concurrent queries (max {})", self.max_concurrent_queries))
        })?;
        let req = request.into_inner();
        let (merge_limit, oversample_k) = validate_offset_k(req.offset, req.k, self.max_k, self.oversample_factor)?;
        let dimension = validate_vector_dimension(&req.query_vector, req.dimension)?;

        let local_req = pb::LocalSearchRequest {
            query_type: LanceQueryType::Hybrid as i32,
            table_name: req.table_name.clone(),
            vector_column: Some(req.vector_column),
            query_vector: Some(req.query_vector),
            dimension: Some(dimension),
            nprobes: Some(req.nprobes),
            metric_type: Some(req.metric_type),
            text_column: Some(req.text_column),
            query_text: Some(req.query_text),
            k: oversample_k, filter: req.filter, columns: req.columns,
            fragment_ids: vec![],
            shard_uri: None,
        };

        let t0 = std::time::Instant::now();
        let on_cols = self.on_columns_for(&req.table_name).await;
        let result = super::scatter_gather::scatter_gather(
            &self.pool, &self.shard_state, &self.pruner,
            &req.table_name, local_req, merge_limit, false, self.query_timeout, trace_id,
            &on_cols,
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
        self.check_ns(&request, &request.get_ref().table_name)?;
        let req_ref = request.get_ref();
        self.audit(&request, "AddRows", &req_ref.table_name,
                   &format!("bytes={} on_columns={} request_id={}",
                            req_ref.arrow_ipc_data.len(),
                            req_ref.on_columns.join(","),
                            if req_ref.request_id.is_empty() { "<none>" } else { &req_ref.request_id }));
        // H3: dedup before any real work. Client retries within the 5-min
        // window get the original response without a second write.
        if let Some(cached) = self.check_dedup(&req_ref.request_id).await {
            log::debug!("add_rows dedup hit for request_id={}", req_ref.request_id);
            return Ok(Response::new(cached));
        }
        let request_id = req_ref.request_id.clone();
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
            let resp = self.add_rows_hash_partitioned(req, routing).await?;
            self.record_dedup(&request_id, resp.get_ref()).await;
            return Ok(resp);
        }

        let idx = self.write_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % routing.len();
        let (shard_name, primary, secondary) = &routing[idx];

        // B2.2: write-path role preference. Build the healthy candidate list
        // (primary + optional secondary) and pick the one most suited to
        // writes: WritePrimary > Either > ReadPrimary. Defaults behave
        // exactly like the old primary-first code when every worker is
        // Either — no behavioural change for 0.1.x configs.
        let mut candidates: Vec<String> = Vec::new();
        if self.pool.get_healthy_client(primary).await.is_ok() {
            candidates.push(primary.clone());
        }
        if let Some(sec) = secondary
            && self.pool.get_healthy_client(sec).await.is_ok()
        {
            candidates.push(sec.clone());
        }
        if candidates.is_empty() {
            return Err(Status::unavailable(format!("No healthy worker for shard {}", shard_name)));
        }
        let pick = self.pool.pick_by_role(&candidates, /*for_reads=*/ false).await
            .expect("candidates non-empty above");
        let worker_id = candidates[pick].clone();
        let worker_id = &worker_id;
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
        let commit_seq = if resp.error.is_empty() {
            self.bump_commit_seq().await
        } else {
            0
        };
        let write_resp = pb::WriteResponse {
            affected_rows: resp.affected_rows,
            new_version: resp.new_version,
            error: resp.error,
            commit_seq,
        };
        self.record_dedup(&request_id, &write_resp).await;
        Ok(Response::new(write_resp))
    }

    async fn delete_rows(
        &self,
        request: Request<pb::DeleteRowsRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Write)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        let req_ref = request.get_ref();
        self.audit(&request, "DeleteRows", &req_ref.table_name,
                   &format!("filter={}", truncate_for_audit(&req_ref.filter)));
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
        // #5.5 Bump global commit_seq on successful write + surface
        // to client. No MetaStore → seq stays 0.
        let commit_seq = if error.is_empty() {
            self.bump_commit_seq().await
        } else {
            0
        };
        Ok(Response::new(pb::WriteResponse {
            affected_rows: total_affected,
            new_version: max_version,
            error,
            commit_seq,
        }))
    }

    async fn upsert_rows(
        &self,
        request: Request<pb::UpsertRowsRequest>,
    ) -> Result<Response<pb::WriteResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Write)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        let req_ref = request.get_ref();
        self.audit(&request, "UpsertRows", &req_ref.table_name,
                   &format!("bytes={} on_columns={} request_id={}",
                            req_ref.arrow_ipc_data.len(),
                            req_ref.on_columns.join(","),
                            if req_ref.request_id.is_empty() { "<none>" } else { &req_ref.request_id }));
        // H3: dedup (merge_insert is already semantically idempotent for the
        // same payload, but this spares the worker the recompute).
        if let Some(cached) = self.check_dedup(&req_ref.request_id).await {
            log::debug!("upsert_rows dedup hit for request_id={}", req_ref.request_id);
            return Ok(Response::new(cached));
        }
        let request_id = req_ref.request_id.clone();
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
        let commit_seq = if error.is_empty() {
            self.bump_commit_seq().await
        } else {
            0
        };
        let write_resp = pb::WriteResponse {
            affected_rows: total_affected,
            new_version: max_version,
            error,
            commit_seq,
        };
        self.record_dedup(&request_id, &write_resp).await;
        Ok(Response::new(write_resp))
    }

    async fn create_table(
        &self,
        request: Request<pb::CreateTableRequest>,
    ) -> Result<Response<pb::CreateTableResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.audit(&request, "CreateTable", &request.get_ref().table_name, "");
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        // DDL serialization: prevent concurrent CreateTable on same name.
        // Local mutex first (fast path within a single coord); then the
        // distributed lease for cross-coord correctness (#5.2). When no
        // MetaStore is configured the lease is a no-op and we fall back
        // to per-coord semantics.
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        let ddl_lease = self.acquire_ddl_lease(&req.table_name).await?;
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

        // Send CreateLocalShard to each worker in parallel.
        //
        // H4 note — this fan-out deliberately uses Vec<JoinHandle>, NOT
        // JoinSet. Client-side cancellation in the middle of a CreateTable
        // is a data-integrity problem either way:
        //   - JoinSet: drops + aborts in-flight lance writes mid-manifest
        //     → half-written fragments + no rollback → worse orphan state
        //   - Vec: tasks keep running to completion without rollback →
        //     orphan shard files but lance state is consistent
        // The right long-term fix is H3 (request_id idempotency window +
        // saga rollback on detected abandonment); until that lands, keep
        // the current pattern so at least the workers reach a consistent
        // on-disk state. Orphan_gc_loop can clean up the unreferenced
        // shards if the operator has it enabled.
        let mut handles = Vec::new();
        for (i, partition) in partitions.into_iter().enumerate() {
            // Sanitize the table_name portion of shard_name: lancedb 0.27
            // rejects dataset names containing `/` (and its current
            // validation path `.unwrap()`s on rejection instead of
            // returning an error, which would panic the worker if we
            // passed `/` through — see F3 regression). G5 requires
            // `tenant-a/table`-style namespace prefixes at the API,
            // which means every shard_name would carry a `/`. Map `/`
            // to `__` when constructing the shard name that lives on
            // disk and in lancedb; the user-facing table_name keeps
            // the `/` separator everywhere else (routing, audit,
            // ListTables filtering).
            let safe_stem = req.table_name.replace('/', "__");
            let shard_name = format!("{}_shard_{:02}", safe_stem, i);
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

        // #5.1 Partition pruning: stash on_columns for read-side routing.
        // No-op when caller didn't declare hash keys. Written to both the
        // in-memory cache (hot read path) and MetaStore (survives restart
        // + visible to other coord instances via auth-reload-style sync
        // if Phase C.4's cache is ever extended to poll this key).
        if !req.on_columns.is_empty() {
            self.table_on_columns
                .write()
                .await
                .insert(req.table_name.clone(), req.on_columns.clone());
            if let Some(ref ms) = self.meta_state {
                ms.put_table_on_columns(&req.table_name, &req.on_columns).await;
            }
        }

        // #5.3 Schema evolution basement: persist the CreateTable
        // schema at version=1 so ALTER TABLE has a monotonic
        // baseline to bump from. The arrow_ipc_data payload already
        // carries both schema and initial rows; we strip it down to
        // just the schema for storage.
        if let Some(ref ms) = self.meta_state {
            // Schema-only IPC = a zero-row slice of the CreateTable
            // batch. Worker decoder reads `reader.schema()` and
            // ignores row content.
            let empty = batch.slice(0, 0);
            let schema_bytes = lance_distributed_common::ipc::record_batch_to_ipc(&empty)
                .map_err(|e| Status::internal(format!("schema IPC: {e}")))?;
            let schema_store = lance_distributed_meta::schema_store::SchemaStore::new(
                ms.store(),
            );
            // Create-only write at v=1. If a peer coord already
            // wrote v=1 for this name, ignore — we're past the
            // shard-state duplicate-name check above, so they did
            // the work first.
            match schema_store.put_schema(&req.table_name, 1, &schema_bytes).await {
                Ok(()) => {}
                Err(lance_distributed_meta::store::MetaError::VersionConflict { .. }) => {
                    log::debug!(
                        "CreateTable '{}': schema v=1 already stored by another coord",
                        req.table_name
                    );
                }
                Err(e) => log::warn!(
                    "CreateTable '{}': schema persist failed: {} (continuing)",
                    req.table_name, e
                ),
            }
        }

        info!("Created table '{}' ({} rows across {} shards on {} workers{})",
            req.table_name, total_created, n_shards, healthy.len(),
            if !req.on_columns.is_empty() { format!(", on_columns={:?}", req.on_columns) } else { String::new() }
        );

        // Release the DDL lease promptly on success so the next DDL
        // doesn't wait for TTL. Failures drop the handle and fall
        // back to TTL expiry — cheaper than threading release through
        // every error path.
        if let Some(lease) = ddl_lease {
            lease.release().await;
        }

        Ok(Response::new(pb::CreateTableResponse {
            table_name: req.table_name.clone(),
            num_rows,
            error: String::new(),
        }))
    }

    /// #5.3 ADD COLUMN NULLABLE across all shards atomically.
    ///
    /// Orchestration:
    ///   1. Acquire DDL lease (cross-coord serialisation).
    ///   2. Read current schema from SchemaStore; enforce
    ///      `expected_schema_version` CAS guard if non-zero.
    ///   3. Fan out LocalAlterTable to every shard. All must
    ///      succeed — first failure aborts without a rollback
    ///      attempt, because Lance's add_columns is idempotent (re-
    ///      running with the same new-column schema is a no-op
    ///      if already applied). Operator retries via a repeat
    ///      AlterTable; expected_schema_version keeps double-applies
    ///      safe.
    ///   4. Compute merged schema = old ++ new columns, write at
    ///      version+1 in SchemaStore.
    ///   5. Release lease.
    async fn alter_table(
        &self,
        request: Request<pb::AlterTableRequest>,
    ) -> Result<Response<pb::AlterTableResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.audit(
            &request,
            "AlterTable",
            &request.get_ref().table_name,
            &format!(
                "expected_v={} new_schema_bytes={}",
                request.get_ref().expected_schema_version,
                request.get_ref().add_columns_arrow_ipc.len()
            ),
        );
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        if req.add_columns_arrow_ipc.is_empty()
            && req.drop_columns.is_empty()
            && req.rename_columns.is_empty()
        {
            return Err(Status::invalid_argument(
                "AlterTable requires at least one of: add_columns_arrow_ipc, drop_columns, rename_columns"
            ));
        }
        let _ddl_guard = self.ddl_lock(&req.table_name).await;

        // Precondition checks BEFORE acquiring the cross-coord DDL
        // lease so that failed validations (routing missing, no
        // MetaStore, schema_version mismatch) don't leave a stale
        // lease hanging for TTL. The local `_ddl_guard` still
        // serialises within this process.
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("table {}", req.table_name)));
        }
        let Some(ref ms) = self.meta_state else {
            return Err(Status::failed_precondition(
                "AlterTable requires a configured metadata_path (MetaStore)",
            ));
        };
        let schema_store =
            lance_distributed_meta::schema_store::SchemaStore::new(ms.store());
        let (current_version, current_schema_bytes) = schema_store
            .get_latest(&req.table_name)
            .await
            .map_err(|e| Status::internal(format!("schema_store: {e}")))?
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "no schema recorded for table '{}' — CreateTable predates v2 schema storage?",
                    req.table_name
                ))
            })?;
        if req.expected_schema_version > 0 && req.expected_schema_version != current_version {
            return Err(Status::failed_precondition(format!(
                "expected_schema_version {} does not match current {}",
                req.expected_schema_version, current_version
            )));
        }

        // Preconditions clean — now take the distributed lease.
        let ddl_lease = self.acquire_ddl_lease(&req.table_name).await?;

        // Parse both schemas so we can merge post-success.
        let parse_schema = |bytes: &[u8]| -> Result<arrow::datatypes::Schema, Status> {
            use arrow::ipc::reader::StreamReader;
            let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
                .map_err(|e| Status::internal(format!("parse schema IPC: {e}")))?;
            Ok((*reader.schema()).clone())
        };
        let old_schema = parse_schema(&current_schema_bytes)?;
        let new_cols_schema = if req.add_columns_arrow_ipc.is_empty() {
            arrow::datatypes::Schema::empty()
        } else {
            parse_schema(&req.add_columns_arrow_ipc)?
        };

        // Fan out LocalAlterTable to every shard (primary only —
        // secondary replicas converge via Lance manifest refresh).
        let mut errors: Vec<String> = Vec::new();
        for (shard_name, primary, _secondary) in &routing {
            let Ok(mut client) = self.pool.get_healthy_client(primary).await else {
                errors.push(format!("shard {}: worker {} unreachable", shard_name, primary));
                continue;
            };
            match client
                .execute_alter_table(Request::new(pb::LocalAlterTableRequest {
                    shard_name: shard_name.clone(),
                    add_columns_arrow_ipc: req.add_columns_arrow_ipc.clone(),
                    drop_columns: req.drop_columns.clone(),
                    rename_columns: req.rename_columns.clone(),
                }))
                .await
            {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() {
                        errors.push(format!("shard {}: {}", shard_name, r.error));
                    }
                }
                Err(e) => errors.push(format!("shard {}: {}", shard_name, e)),
            }
        }
        if !errors.is_empty() {
            return Err(Status::internal(format!(
                "AlterTable fan-out failed: {}",
                errors.join("; ")
            )));
        }

        // Compute merged schema and persist at version+1. Apply ops
        // in the same order the worker does: ADD, DROP, RENAME.
        let mut merged_fields: Vec<arrow::datatypes::Field> =
            old_schema.fields().iter().map(|f| (**f).clone()).collect();
        for f in new_cols_schema.fields() {
            // Force nullable — the worker's NewColumnTransform::AllNulls
            // semantics require it.
            let mut nullable_field = (**f).clone();
            nullable_field = nullable_field.with_nullable(true);
            merged_fields.push(nullable_field);
        }
        if !req.drop_columns.is_empty() {
            let drop_set: std::collections::HashSet<&str> = req.drop_columns.iter()
                .map(|s| s.as_str()).collect();
            merged_fields.retain(|f| !drop_set.contains(f.name().as_str()));
        }
        if !req.rename_columns.is_empty() {
            for f in &mut merged_fields {
                if let Some(new_name) = req.rename_columns.get(f.name()) {
                    *f = f.clone().with_name(new_name.clone());
                }
            }
        }
        let merged_schema = arrow::datatypes::Schema::new(merged_fields);
        let merged_bytes = {
            let empty = arrow::array::RecordBatch::new_empty(std::sync::Arc::new(merged_schema));
            lance_distributed_common::ipc::record_batch_to_ipc(&empty)
                .map_err(|e| Status::internal(format!("merged schema IPC: {e}")))?
        };
        let new_version = current_version + 1;
        schema_store
            .put_schema(&req.table_name, new_version, &merged_bytes)
            .await
            .map_err(|e| Status::internal(format!("schema_store put v{}: {}", new_version, e)))?;

        info!(
            "AlterTable '{}': ADD +{} DROP {} RENAME {} → schema v={}→{}",
            req.table_name,
            new_cols_schema.fields().len(),
            req.drop_columns.len(),
            req.rename_columns.len(),
            current_version,
            new_version
        );
        if let Some(lease) = ddl_lease {
            lease.release().await;
        }
        Ok(Response::new(pb::AlterTableResponse {
            new_schema_version: new_version,
            error: String::new(),
        }))
    }

    /// alpha.3: Create a tag on every shard of the table. Each shard
    /// tags the version `req.version` (or its current version if 0) on
    /// its own Lance dataset. Fan-out with DDL lease, matching the
    /// AlterTable pattern.
    ///
    /// Bounded-staleness note: if writes are landing concurrently, the
    /// "current version" captured on shard_0 vs shard_N may differ by
    /// one or two writes. The DDL lease prevents concurrent tag
    /// creations across coords, but does not freeze the write path.
    /// Users who need strict point-in-time consistency should quiesce
    /// writes before tagging (application-level concern).
    async fn create_tag(
        &self,
        request: Request<pb::CreateTagRequest>,
    ) -> Result<Response<pb::CreateTagResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.audit(
            &request, "CreateTag",
            &request.get_ref().table_name,
            &format!("tag={} v={}", request.get_ref().tag_name, request.get_ref().version),
        );
        let req = request.into_inner();
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        let ddl_lease = self.acquire_ddl_lease(&req.table_name).await?;
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("table '{}'", req.table_name)));
        }
        let mut errors: Vec<String> = Vec::new();
        let mut max_tagged_version: u64 = 0;
        for (shard_name, primary, _secondary) in &routing {
            let Ok(mut client) = self.pool.get_healthy_client(primary).await else {
                errors.push(format!("{}: worker {} unreachable", shard_name, primary));
                continue;
            };
            match client.execute_create_tag(Request::new(pb::LocalCreateTagRequest {
                shard_name: shard_name.clone(),
                tag_name: req.tag_name.clone(),
                version: req.version,
            })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() {
                        errors.push(format!("{}: {}", shard_name, r.error));
                    } else {
                        max_tagged_version = max_tagged_version.max(r.tagged_version);
                    }
                }
                Err(e) => errors.push(format!("{}: {}", shard_name, e)),
            }
        }
        if let Some(lease) = ddl_lease {
            lease.release().await;
        }
        if !errors.is_empty() {
            return Ok(Response::new(pb::CreateTagResponse {
                tagged_version: 0,
                error: format!("CreateTag fan-out failed on {} shard(s): {}",
                    errors.len(), errors.join("; ")),
            }));
        }
        Ok(Response::new(pb::CreateTagResponse {
            tagged_version: max_tagged_version,
            error: String::new(),
        }))
    }

    /// alpha.3: List tags. Because CreateTag fans out to every shard,
    /// the tag set is (eventually) identical across shards. We query
    /// any one healthy shard — intersection semantics are unnecessary
    /// as long as CreateTag doesn't silently succeed on a subset.
    async fn list_tags(
        &self,
        request: Request<pb::ListTagsRequest>,
    ) -> Result<Response<pb::ListTagsResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        let req = request.into_inner();
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("table '{}'", req.table_name)));
        }
        // Iterate shards, return the first one that responds. Preserves
        // availability when shard 0's worker is down.
        let mut last_err = String::from("no healthy worker");
        for (shard_name, primary, secondary) in &routing {
            let owner = if self.pool.get_healthy_client(primary).await.is_ok() {
                primary.clone()
            } else if let Some(s) = secondary {
                if self.pool.get_healthy_client(s).await.is_ok() { s.clone() } else { continue }
            } else { continue };
            let Ok(mut client) = self.pool.get_healthy_client(&owner).await else { continue };
            match client.execute_list_tags(Request::new(pb::LocalListTagsRequest {
                shard_name: shard_name.clone(),
            })).await {
                Ok(resp) => return Ok(Response::new(resp.into_inner())),
                Err(e) => last_err = format!("{}: {}", shard_name, e),
            }
        }
        Err(Status::unavailable(last_err))
    }

    /// alpha.3: Delete a tag on every shard. Fan-out under DDL lease;
    /// individual shard failures are surfaced but don't halt the pass
    /// (tag semantics are already idempotent per shard).
    async fn delete_tag(
        &self,
        request: Request<pb::DeleteTagRequest>,
    ) -> Result<Response<pb::DeleteTagResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.audit(
            &request, "DeleteTag",
            &request.get_ref().table_name,
            &request.get_ref().tag_name.clone(),
        );
        let req = request.into_inner();
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        let ddl_lease = self.acquire_ddl_lease(&req.table_name).await?;
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("table '{}'", req.table_name)));
        }
        let mut errors: Vec<String> = Vec::new();
        for (shard_name, primary, _secondary) in &routing {
            let Ok(mut client) = self.pool.get_healthy_client(primary).await else {
                errors.push(format!("{}: worker {} unreachable", shard_name, primary));
                continue;
            };
            match client.execute_delete_tag(Request::new(pb::LocalDeleteTagRequest {
                shard_name: shard_name.clone(),
                tag_name: req.tag_name.clone(),
            })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() {
                        errors.push(format!("{}: {}", shard_name, r.error));
                    }
                }
                Err(e) => errors.push(format!("{}: {}", shard_name, e)),
            }
        }
        if let Some(lease) = ddl_lease {
            lease.release().await;
        }
        if !errors.is_empty() {
            return Ok(Response::new(pb::DeleteTagResponse {
                error: format!("DeleteTag fan-out partial: {}", errors.join("; ")),
            }));
        }
        Ok(Response::new(pb::DeleteTagResponse { error: String::new() }))
    }

    /// alpha.3: Restore every shard to the given version (on each
    /// shard's native Lance version counter) or tag name. Fan-out
    /// under DDL lease. All shards must succeed or the caller gets an
    /// error listing the failures.
    ///
    /// Caveat: Lance restore creates a new manifest pointing at old
    /// data. If shard A restores successfully and shard B fails, the
    /// cluster is temporarily in a split-version state. Retrying the
    /// RestoreTable call brings both shards in line (Lance restore is
    /// idempotent over a given target version).
    async fn restore_table(
        &self,
        request: Request<pb::RestoreTableRequest>,
    ) -> Result<Response<pb::RestoreTableResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.audit(
            &request, "RestoreTable",
            &request.get_ref().table_name,
            &format!("version={} tag={}", request.get_ref().version, request.get_ref().tag),
        );
        let req = request.into_inner();
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        let ddl_lease = self.acquire_ddl_lease(&req.table_name).await?;
        let routing = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing.is_empty() {
            return Err(Status::not_found(format!("table '{}'", req.table_name)));
        }
        let mut errors: Vec<String> = Vec::new();
        let mut max_new_version: u64 = 0;
        for (shard_name, primary, _secondary) in &routing {
            let Ok(mut client) = self.pool.get_healthy_client(primary).await else {
                errors.push(format!("{}: worker {} unreachable", shard_name, primary));
                continue;
            };
            match client.execute_restore_table(Request::new(pb::LocalRestoreTableRequest {
                shard_name: shard_name.clone(),
                version: req.version,
                tag: req.tag.clone(),
            })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() {
                        errors.push(format!("{}: {}", shard_name, r.error));
                    } else {
                        max_new_version = max_new_version.max(r.new_version);
                    }
                }
                Err(e) => errors.push(format!("{}: {}", shard_name, e)),
            }
        }
        if let Some(lease) = ddl_lease {
            lease.release().await;
        }
        if !errors.is_empty() {
            return Ok(Response::new(pb::RestoreTableResponse {
                new_version: 0,
                error: format!("RestoreTable fan-out failed on {} shard(s); retry is safe: {}",
                    errors.len(), errors.join("; ")),
            }));
        }
        Ok(Response::new(pb::RestoreTableResponse {
            new_version: max_new_version,
            error: String::new(),
        }))
    }

    async fn list_tables(
        &self,
        request: Request<pb::ListTablesRequest>,
    ) -> Result<Response<pb::ListTablesResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        // B5: when the caller is bound to a namespace AND a MetaStore is
        // configured, push the filter down one layer so the key list
        // never crosses the wire unfiltered. Fall back to the
        // auth-layer filter (F3) for Static shard state or unbound
        // operator keys. Same end-result for correctness; B5 cuts
        // listing cost on a 10k-table / 1000-namespace cluster.
        let caller_ns = self.auth.as_ref().and_then(|a| a.namespace_for(&request));
        let tables = match (&caller_ns, &self.meta_state) {
            (Some(ns), Some(ms)) => ms.all_tables_for_namespace(ns).await,
            _ => {
                let all = self.shard_state.all_tables().await;
                match &self.auth {
                    Some(a) => a.filter_namespace(&request, all),
                    None => all,
                }
            }
        };
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
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.audit(&request, "DropTable", &request.get_ref().table_name, "");
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name required"));
        }
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        let ddl_lease = self.acquire_ddl_lease(&req.table_name).await?;
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
        // #5.1: drop on_columns cache + MetaStore entry.
        self.table_on_columns.write().await.remove(&req.table_name);
        if let Some(ref ms) = self.meta_state {
            ms.remove_table_on_columns(&req.table_name).await;
        }
        // Remove from shard state (set empty mapping)
        let empty: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        self.shard_state.register_table(&req.table_name, empty).await;
        self.metrics.remove_table(&req.table_name);
        // Clean up DDL lock to prevent leak
        self.ddl_locks.write().await.remove(&req.table_name);
        info!("Dropped table '{}' (purged {} shard(s) from storage)", req.table_name, shard_uris.len());
        if let Some(lease) = ddl_lease {
            lease.release().await;
        }
        Ok(Response::new(pb::DropTableResponse { error: String::new() }))
    }

    async fn create_index(
        &self,
        request: Request<pb::CreateIndexRequest>,
    ) -> Result<Response<pb::CreateIndexResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Admin)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.audit(&request, "CreateIndex",
            &request.get_ref().table_name,
            &format!("column={} type={}", request.get_ref().column, request.get_ref().index_type));
        let req = request.into_inner();
        if req.table_name.is_empty() || req.column.is_empty() {
            return Err(Status::invalid_argument("table_name and column required"));
        }

        // #5.2 DDL lease — prevents concurrent CreateIndex on the
        // same table from a peer coord (would otherwise race on
        // Lance manifest CAS, wasting compute).
        let _ddl_guard = self.ddl_lock(&req.table_name).await;
        let ddl_lease = self.acquire_ddl_lease(&req.table_name).await?;

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
        if let Some(lease) = ddl_lease {
            lease.release().await;
        }
        Ok(Response::new(pb::CreateIndexResponse { error }))
    }

    async fn get_schema(
        &self,
        request: Request<pb::GetSchemaRequest>,
    ) -> Result<Response<pb::GetSchemaResponse>, Status> {
        self.check_perm(&request, super::auth::Permission::Read)?;
        self.check_ns(&request, &request.get_ref().table_name)?;
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
        self.check_ns(&request, &request.get_ref().table_name)?;
        self.check_min_schema_version(
            &request.get_ref().table_name,
            request.get_ref().min_schema_version,
        )
        .await?;
        self.check_min_commit_seq(request.get_ref().min_commit_seq).await?;
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
        self.check_ns(&request, &request.get_ref().table_name)?;
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

        // Fan out GetByIds to all workers (IDs may be on any shard).
        // JoinSet so a client-side cancel drops the fan-out cleanly (H4).
        let mut tasks = tokio::task::JoinSet::new();
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
                tasks.spawn(async move {
                    let result = tokio::time::timeout(timeout,
                        client.local_get_by_ids(Request::new(r))).await;
                    (worker_id, result)
                });
            }
        }

        // Gather results
        let mut batches = Vec::new();
        while let Some(res) = tasks.join_next().await {
            if let Ok((_, Ok(Ok(resp)))) = res {
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
        self.check_ns(&request, &request.get_ref().table_name)?;
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

        // JoinSet fan-out: cancellation-safe (H4).
        let mut tasks = tokio::task::JoinSet::new();
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
                tasks.spawn(async move {
                    tokio::time::timeout(timeout, client.local_query(Request::new(r))).await
                });
            }
        }

        let mut batches = Vec::new();
        while let Some(res) = tasks.join_next().await {
            if let Ok(Ok(Ok(resp))) = res {
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
        self.check_ns(&request, &request.get_ref().table_name)?;
        let trace_id = extract_trace_id(&request);
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

        // Every query_vector in the batch must agree with the declared
        // dimension; validate once up front so a bad entry doesn't spawn
        // the scatter-gather just to fail downstream.
        for (i, qv) in req.query_vectors.iter().enumerate() {
            validate_vector_dimension(qv, req.dimension).map_err(|e|
                Status::invalid_argument(format!("query_vectors[{i}]: {}", e.message()))
            )?;
        }

        // Execute each query in parallel. H4: JoinSet aborts every
        // child task on drop, so a client cancelling the batch_search
        // RPC stops the N scatter-gathers immediately instead of
        // letting them finish burning worker CPU for queries nobody
        // is waiting on.
        let mut tasks = tokio::task::JoinSet::new();
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
                fragment_ids: vec![],
            shard_uri: None,
        };

            let pool = self.pool.clone();
            let shard_state = self.shard_state.clone();
            let pruner = self.pruner.clone();
            let table = req.table_name.clone();
            let timeout = self.query_timeout;
            let merge_k = k;

            let tid = trace_id.clone();
            // #5.1 batch_search: capture on_columns once up-front, reuse
            // across every sub-query task spawned for this batch.
            let on_cols = self.on_columns_for(&table).await;
            tasks.spawn(async move {
                super::scatter_gather::scatter_gather(
                    &pool, &shard_state, &pruner, &table, local_req, merge_k, true, timeout, tid,
                    &on_cols,
                ).await
            });
        }

        let mut results = Vec::new();
        while let Some(res) = tasks.join_next().await {
            match res {
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

/// Validate that a query_vector byte payload is internally consistent with the
/// declared `dimension`. The wire format is packed little-endian f32, so the
/// byte length must be a positive multiple of 4 and — if a dimension is
/// declared — must equal `dimension * 4`.
///
/// Returns the agreed dimension (declared wins when non-zero, otherwise derived).
/// The empty-vector case (len == 0) is a different code path — ann_search uses
/// it for text-embedding fallback, so we pass through with the declared value.
///
/// H10 hardening: previously, `ann_search` silently took `req.dimension` on
/// faith when > 0, so a client sending `dimension=5` with a 16-byte
/// `query_vector` (4 f32s) would pass the coordinator and then produce
/// nonsense or a crash inside the worker's lance query path.
fn validate_vector_dimension(query_vector: &[u8], declared_dim: u32) -> std::result::Result<u32, Status> {
    if query_vector.is_empty() {
        return Ok(declared_dim);
    }
    if !query_vector.len().is_multiple_of(4) {
        return Err(Status::invalid_argument(format!(
            "query_vector length {} is not a multiple of 4 bytes (little-endian f32 expected)",
            query_vector.len()
        )));
    }
    let derived = (query_vector.len() / 4) as u32;
    if declared_dim > 0 && declared_dim != derived {
        return Err(Status::invalid_argument(format!(
            "dimension mismatch: query_vector carries {} f32 values (len={} bytes) but dimension={} was declared",
            derived, query_vector.len(), declared_dim
        )));
    }
    Ok(if declared_dim > 0 { declared_dim } else { derived })
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
                role: Default::default(),
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

    // ── H19: explicit boundary coverage around max_k ──

    #[test]
    fn test_validate_offset_k_exact_max_k() {
        // offset + k == max_k must succeed (upper bound is inclusive).
        let (total, _) = validate_offset_k(50, 50, 100, 1).unwrap();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_validate_offset_k_one_past_max_k() {
        // offset + k == max_k + 1 must fail.
        assert!(validate_offset_k(50, 51, 100, 1).is_err());
        assert!(validate_offset_k(51, 50, 100, 1).is_err());
    }

    #[test]
    fn test_validate_offset_k_zero_offset_full_k() {
        // offset=0, k=max_k is the "first page to the limit" scenario.
        let (total, _) = validate_offset_k(0, 100, 100, 1).unwrap();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_validate_offset_k_max_offset_k_one() {
        // offset = max_k - 1, k = 1 — last single row at the limit.
        let (total, _) = validate_offset_k(99, 1, 100, 1).unwrap();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_validate_offset_k_oversample_at_boundary() {
        // At the exact max_k boundary with oversample > 1, the fetch size
        // grows but the total (offset + k) is what's capped.
        let (total, fetch) = validate_offset_k(40, 60, 100, 3).unwrap();
        assert_eq!(total, 100);
        assert_eq!(fetch, 300, "oversample should multiply fetch, not cap");
    }

    #[test]
    fn test_validate_offset_k_k_only_at_max() {
        // No offset pagination, k = max_k is legal.
        let (total, _) = validate_offset_k(0, 10_000, 10_000, 1).unwrap();
        assert_eq!(total, 10_000);
    }

    #[test]
    fn test_validate_offset_k_offset_at_max_without_k_fails() {
        // offset = max_k with k = 1 would need row max_k+1 — reject.
        assert!(validate_offset_k(100, 1, 100, 1).is_err());
    }

    // ── H10: dimension / query_vector consistency ──

    #[test]
    fn test_validate_vector_dim_derives_from_bytes_when_dim_zero() {
        // 16 bytes = 4 f32 values. Declared_dim=0 means "derive".
        let vec_bytes = vec![0u8; 16];
        assert_eq!(validate_vector_dimension(&vec_bytes, 0).unwrap(), 4);
    }

    #[test]
    fn test_validate_vector_dim_agrees_with_declared() {
        let vec_bytes = vec![0u8; 16]; // 4 f32s
        // declared=4 agrees → accept and return 4.
        assert_eq!(validate_vector_dimension(&vec_bytes, 4).unwrap(), 4);
    }

    #[test]
    fn test_validate_vector_dim_rejects_mismatch() {
        let vec_bytes = vec![0u8; 16]; // 4 f32s
        // declared=5 disagrees with derived=4 → reject.
        let err = validate_vector_dimension(&vec_bytes, 5).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("dimension mismatch"), "got: {}", err.message());
        assert!(err.message().contains("declared"), "error must spell out the conflict");
    }

    #[test]
    fn test_validate_vector_dim_rejects_non_f32_aligned_bytes() {
        // 13 bytes is not a valid f32 little-endian sequence.
        let vec_bytes = vec![0u8; 13];
        let err = validate_vector_dimension(&vec_bytes, 0).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("not a multiple of 4"), "got: {}", err.message());
    }

    #[test]
    fn test_validate_vector_dim_empty_passes_through() {
        // Empty query_vector is the text-embed fallback path; caller
        // resolves it elsewhere. Validate passes the declared value
        // through unchanged.
        assert_eq!(validate_vector_dimension(&[], 0).unwrap(), 0);
        assert_eq!(validate_vector_dimension(&[], 128).unwrap(), 128);
    }

    #[test]
    fn test_validate_vector_dim_large_vector_declared() {
        // 4096 bytes = 1024 f32s. Declared agrees.
        let vec_bytes = vec![0u8; 4096];
        assert_eq!(validate_vector_dimension(&vec_bytes, 1024).unwrap(), 1024);
    }

    #[test]
    fn test_validate_vector_dim_off_by_one_caught() {
        // Most realistic bug: client hardcodes dimension from one model
        // but sends vector from another. declared=768 (BERT-base),
        // derived=384 (MiniLM) — must reject.
        let vec_bytes = vec![0u8; 384 * 4];
        let err = validate_vector_dimension(&vec_bytes, 768).unwrap_err();
        assert!(err.message().contains("384"), "error must include the derived value");
        assert!(err.message().contains("768"), "error must include the declared value");
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

    // ── H11: DDL boundary cases ──

    #[test]
    fn test_split_batch_zero_rows_n_one_preserves_schema() {
        // n<=1 short-circuit: caller sees a single empty batch that still
        // carries the schema. This path is what CreateTable hits when
        // num_rows=0 and n_shards=1 (< 1000 rows heuristic).
        let batch = make_i32_batch(vec![]);
        let parts = split_batch(&batch, 1);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].num_rows(), 0);
        assert_eq!(parts[0].schema().fields().len(), batch.schema().fields().len());
    }

    #[test]
    fn test_split_batch_zero_rows_n_gt_one_returns_empty() {
        // Asymmetric: with n > 1 and zero rows, every partition would be
        // empty and the filter drops them all. Callers must guard against
        // an empty Vec, not assume `n` partitions are returned.
        let batch = make_i32_batch(vec![]);
        let parts = split_batch(&batch, 4);
        assert!(parts.is_empty(),
                "0-row batch with n>1 returns Vec::new() after the empty-batch filter");
    }

    #[test]
    fn test_split_batch_single_row_multi_shards() {
        // 1 row / 4 shards → only the first partition carries the row;
        // the other 3 are dropped by the filter. Confirms no panic on
        // `chunk_size=1` + `n=4`.
        let batch = make_i32_batch(vec![42]);
        let parts = split_batch(&batch, 4);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_partition_batch_by_hash_zero_rows() {
        // Hash-partition of an empty batch with n=3 should return an empty
        // Vec (callers rely on this to short-circuit AddRows on 0 rows
        // without creating empty shards).
        let batch = make_i32_batch(vec![]);
        let out = partition_batch_by_hash(&batch, &["id".into()], 3).unwrap();
        assert!(out.iter().all(|b| b.num_rows() == 0),
                "each partition is empty or dropped");
    }

    #[test]
    fn test_split_batch_exactly_1000_rows_boundary() {
        // CreateTable's n_shards heuristic is `if num_rows < 1000 { 1 }`.
        // Test the boundary: 1000 rows (not <1000) would get the workers-
        // count path. split_batch directly with n=3: even 334+333+333.
        let rows: Vec<i32> = (0..1000).collect();
        let batch = make_i32_batch(rows);
        let parts = split_batch(&batch, 3);
        assert_eq!(parts.len(), 3);
        let total: usize = parts.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_split_batch_n_zero_treated_as_one() {
        // Defensive: n=0 must not divide-by-zero. Current code short-
        // circuits at n<=1 → single-batch clone.
        let batch = make_i32_batch(vec![1, 2, 3]);
        let parts = split_batch(&batch, 0);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].num_rows(), 3);
    }

    #[test]
    fn test_partition_batch_by_hash_single_shard() {
        let batch = make_i32_batch(vec![1, 2, 3]);
        let out = partition_batch_by_hash(&batch, &["id".into()], 1).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    // ── H3: write-idempotency dedup cache ──

    fn make_dedup_service() -> CoordinatorService {
        let config = ClusterConfig {
            tables: vec![],
            executors: vec![ExecutorConfig {
                id: "w0".into(), host: "127.0.0.1".into(), port: 1,
                role: Default::default(),
            }],
            ..Default::default()
        };
        CoordinatorService::new(&config, Duration::from_secs(30))
    }

    #[tokio::test]
    async fn test_dedup_empty_id_never_caches() {
        let svc = make_dedup_service();
        // Empty id: record is a no-op, check returns None.
        let resp = pb::WriteResponse { affected_rows: 100, new_version: 1, error: String::new() , commit_seq: 0 };
        svc.record_dedup("", &resp).await;
        assert!(svc.check_dedup("").await.is_none(),
                "empty request_id must never be cached (0.1 behaviour)");
    }

    #[tokio::test]
    async fn test_dedup_roundtrip() {
        let svc = make_dedup_service();
        let resp = pb::WriteResponse { affected_rows: 42, new_version: 7, error: String::new() , commit_seq: 0 };
        svc.record_dedup("req-abc", &resp).await;
        let got = svc.check_dedup("req-abc").await.expect("cache hit expected");
        assert_eq!(got.affected_rows, 42);
        assert_eq!(got.new_version, 7);
    }

    #[tokio::test]
    async fn test_dedup_miss_on_unknown_id() {
        let svc = make_dedup_service();
        let resp = pb::WriteResponse { affected_rows: 1, new_version: 1, error: String::new() , commit_seq: 0 };
        svc.record_dedup("known", &resp).await;
        assert!(svc.check_dedup("unknown").await.is_none());
    }

    #[tokio::test]
    async fn test_dedup_expired_entry_ignored() {
        // Inject an expired entry directly into the cache and confirm
        // check_dedup returns None (the reaper clean-up is best-effort,
        // so `check_dedup` must independently honour the TTL).
        let svc = make_dedup_service();
        let past = std::time::Instant::now()
            .checked_sub(WRITE_DEDUP_TTL + Duration::from_secs(10))
            .expect("Instant subtraction did not underflow in test env");
        let resp = pb::WriteResponse { affected_rows: 1, new_version: 1, error: String::new() , commit_seq: 0 };
        svc.write_dedup.write().await.insert("stale".into(), (past, resp));
        assert!(svc.check_dedup("stale").await.is_none(),
                "TTL-expired entries must not be served even if reaper hasn't run");
    }

    #[tokio::test]
    async fn test_dedup_overwrites_prior_entry_for_same_id() {
        // Same request_id written twice: the second record_dedup wins.
        // Not strictly required by the protocol (clients shouldn't
        // reuse ids across different payloads) but documents the
        // last-writer-wins semantics for anyone reading the code.
        let svc = make_dedup_service();
        let a = pb::WriteResponse { affected_rows: 1, new_version: 1, error: String::new() , commit_seq: 0 };
        let b = pb::WriteResponse { affected_rows: 99, new_version: 2, error: String::new() , commit_seq: 0 };
        svc.record_dedup("same", &a).await;
        svc.record_dedup("same", &b).await;
        let got = svc.check_dedup("same").await.unwrap();
        assert_eq!(got.affected_rows, 99);
        assert_eq!(got.new_version, 2);
    }

    #[tokio::test]
    async fn test_dedup_preserves_error_field() {
        // A failed write should be cached too — re-executing after a
        // logical error gives the same outcome, so clients get the
        // same error back on retry rather than a surprise success.
        let svc = make_dedup_service();
        let failed = pb::WriteResponse { affected_rows: 0, new_version: 0, error: "table not found".into(), commit_seq: 0 };
        svc.record_dedup("failed-req", &failed).await;
        let got = svc.check_dedup("failed-req").await.unwrap();
        assert_eq!(got.error, "table not found");
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

    // ── G10: traceparent parsing ──

    #[test]
    fn test_extract_trace_id_w3c_shape() {
        use tonic::metadata::MetadataValue;
        let mut req = Request::new(());
        req.metadata_mut().insert(
            "traceparent",
            MetadataValue::try_from("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap(),
        );
        let tid = extract_trace_id(&req).expect("well-formed traceparent must parse");
        assert_eq!(tid, "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    #[test]
    fn test_extract_trace_id_missing_header_returns_none() {
        let req = Request::new(());
        assert!(extract_trace_id(&req).is_none());
    }

    #[test]
    fn test_extract_trace_id_rejects_bad_shape() {
        use tonic::metadata::MetadataValue;
        let mut req = Request::new(());
        // Too short — not 32 hex chars.
        req.metadata_mut().insert(
            "traceparent",
            MetadataValue::try_from("00-deadbeef-00f067aa0ba902b7-01").unwrap(),
        );
        assert!(extract_trace_id(&req).is_none(),
                "malformed traceparent must return None, not panic");
    }

    // ── G7: persistent audit sink end-to-end ──
    //
    // Verifies that `CoordinatorService::audit()` actually writes a JSONL
    // record to the configured sink. The audit.rs unit tests already cover
    // the sink in isolation; this test covers the wiring between the
    // service layer and the sink, which is what would break silently if a
    // future refactor drops the `with_audit_sink` call site.

    #[tokio::test]
    async fn test_audit_sink_records_write_op() {
        use lance_distributed_common::audit::AuditSink;
        use tokio::sync::Notify;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log").to_string_lossy().to_string();
        let shutdown = Arc::new(Notify::new());

        let svc = CoordinatorService::new(&make_config(), Duration::from_secs(5));
        let sink = AuditSink::spawn(path.clone(), shutdown.clone(), svc.metrics());
        let svc = svc.with_audit_sink(sink);

        // Fake a request from a known principal (no auth = principal
        // becomes "no-auth") and submit an audit line through the private
        // audit() path. We only expose this via the pb::AddRowsRequest
        // shape to exercise the same code path a real RPC would.
        let req = Request::new(pb::AddRowsRequest {
            table_name: "t1".to_string(),
            arrow_ipc_data: vec![0; 5],
            on_columns: vec![],
            request_id: String::new(),
        });
        svc.audit(&req, "AddRows", "t1", "bytes=5");

        // Allow the background sink task to drain.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.notify_waiters();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("\"op\":\"AddRows\""),
                "audit file must contain the op we submitted: {content}");
        assert!(content.contains("\"target\":\"t1\""));
    }
}
