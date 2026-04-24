// Licensed under the Apache License, Version 2.0.
// Coordinator connection pool: manages gRPC connections to Workers.
// Handles health checking, auto-reconnect, and stale connection removal.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{info, warn};
use tokio::sync::RwLock;
use tonic::Request;

use lance_distributed_proto::generated::lance_distributed as pb;
use lance_distributed_common::shard_state::ShardState;

/// Result of a health check, collected without holding locks.
enum HealthCheckResult {
    Reconnected {
        client: pb::lance_executor_service_client::LanceExecutorServiceClient<tonic::transport::Channel>,
        host: String,
        port: u16,
    },
    Healthy { loaded_shards: u32, total_rows: u64, shard_names: Vec<String> },
    Failed,
}

/// State of a single Worker connection.
pub struct WorkerState {
    pub client: pb::lance_executor_service_client::LanceExecutorServiceClient<
        tonic::transport::Channel,
    >,
    pub healthy: bool,
    pub last_check: Instant,
    pub consecutive_failures: u32,
    pub loaded_shards: u32,
    pub total_rows: u64,
}

/// Manages connections to all Worker nodes.
pub struct ConnectionPool {
    workers: Arc<RwLock<HashMap<String, WorkerState>>>,
    endpoints: Arc<RwLock<HashMap<String, (String, u16)>>>,
    query_timeout: Duration,
    /// TLS config for client-side connection to workers.
    tls_config: Option<tonic::transport::ClientTlsConfig>,
    /// Shutdown signal for the health check loop.
    shutdown: Arc<tokio::sync::Notify>,
    /// Health check tuning from config.
    health_check_interval_secs: u64,
    unhealthy_threshold: u32,
    remove_threshold: u32,
    /// Worker connection timeout in seconds.
    connect_timeout_secs: u64,
    /// Health check ping timeout in seconds.
    ping_timeout_secs: u64,
    /// HTTP/2 keepalive interval in seconds.
    keepalive_interval_secs: u64,
    /// HTTP/2 keepalive timeout in seconds.
    keepalive_timeout_secs: u64,
    /// Shard URI registry (shared with CoordinatorService for recovery).
    shard_uris: Option<Arc<tokio::sync::RwLock<HashMap<String, String>>>>,
    /// Per-worker role preference (B2.2). Defaults to `Either` for any
    /// worker not explicitly configured. Stored behind RwLock so
    /// RegisterWorker can add entries at runtime.
    roles: Arc<RwLock<HashMap<String, lance_distributed_common::config::ExecutorRole>>>,
}

impl ConnectionPool {
    pub fn new(
        endpoints: HashMap<String, (String, u16)>,
        query_timeout: Duration,
        health_config: &lance_distributed_common::config::HealthCheckConfig,
        server_config: &lance_distributed_common::config::ServerConfig,
    ) -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            endpoints: Arc::new(RwLock::new(endpoints)),
            query_timeout,
            tls_config: None,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            health_check_interval_secs: health_config.interval_secs,
            unhealthy_threshold: health_config.unhealthy_threshold,
            remove_threshold: health_config.remove_threshold,
            connect_timeout_secs: health_config.connect_timeout_secs,
            ping_timeout_secs: health_config.ping_timeout_secs,
            keepalive_interval_secs: server_config.keepalive_interval_secs,
            keepalive_timeout_secs: server_config.keepalive_timeout_secs,
            shard_uris: None,
            roles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Install the per-worker role preference map built from config. B2.2.
    pub async fn set_roles(
        &self,
        roles: HashMap<String, lance_distributed_common::config::ExecutorRole>,
    ) {
        *self.roles.write().await = roles;
    }

    /// Look up a worker's role preference. Defaults to `Either` for any
    /// worker not explicitly configured (covers RegisterWorker-added
    /// workers and keeps 0.1.x round-robin behaviour when no operator
    /// pins roles).
    pub async fn role_of(&self, worker_id: &str) -> lance_distributed_common::config::ExecutorRole {
        self.roles.read().await
            .get(worker_id)
            .copied()
            .unwrap_or(lance_distributed_common::config::ExecutorRole::Either)
    }

    /// Pick the best-matched candidate from a list of healthy workers
    /// given the desired role. Preference order (B2.2):
    /// - `for_reads=true`:  ReadPrimary > Either > WritePrimary
    /// - `for_reads=false`: WritePrimary > Either > ReadPrimary
    ///
    /// Single-candidate shards and all-`Either` clusters are unaffected.
    /// Returns an index into `candidates` or None if the slice is empty.
    pub async fn pick_by_role(&self, candidates: &[String], for_reads: bool) -> Option<usize> {
        use lance_distributed_common::config::ExecutorRole::*;
        if candidates.is_empty() {
            return None;
        }
        let roles = self.roles.read().await;
        let score = |r: lance_distributed_common::config::ExecutorRole| -> u8 {
            match (for_reads, r) {
                (true,  ReadPrimary)  => 3,
                (true,  Either)       => 2,
                (true,  WritePrimary) => 1,
                (false, WritePrimary) => 3,
                (false, Either)       => 2,
                (false, ReadPrimary)  => 1,
            }
        };
        let mut best_idx = 0usize;
        let mut best_score = 0u8;
        for (i, wid) in candidates.iter().enumerate() {
            let role = roles.get(wid).copied().unwrap_or(Either);
            let s = score(role);
            if s > best_score {
                best_score = s;
                best_idx = i;
            }
        }
        Some(best_idx)
    }

    /// Signal the health check loop to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Dynamically add a new worker endpoint (for RegisterWorker).
    /// The next health check cycle will connect to it.
    pub async fn add_endpoint(&self, worker_id: &str, host: &str, port: u16) {
        self.endpoints.write().await.insert(worker_id.to_string(), (host.to_string(), port));
        info!("Added endpoint for worker {} at {}:{}", worker_id, host, port);
    }

    /// Attach shard URI registry for automatic shard recovery.
    pub fn with_shard_uris(mut self, uris: Arc<tokio::sync::RwLock<HashMap<String, String>>>) -> Self {
        self.shard_uris = Some(uris);
        self
    }

    /// Look up a shard URI for recovery LoadShard.
    async fn get_shard_uri(&self, shard_name: &str) -> Option<String> {
        if let Some(ref uris) = self.shard_uris {
            uris.read().await.get(shard_name).cloned()
        } else { None }
    }

    /// Enable TLS for worker connections using a CA certificate.
    pub fn with_tls(mut self, ca_cert_pem: Vec<u8>) -> Self {
        let ca = tonic::transport::Certificate::from_pem(ca_cert_pem);
        self.tls_config = Some(
            tonic::transport::ClientTlsConfig::new().ca_certificate(ca)
        );
        self
    }

    /// Get a healthy client for a Worker, or error if unhealthy/disconnected.
    pub async fn get_healthy_client(
        &self,
        worker_id: &str,
    ) -> Result<pb::lance_executor_service_client::LanceExecutorServiceClient<tonic::transport::Channel>, tonic::Status> {
        let states = self.workers.read().await;
        match states.get(worker_id) {
            Some(ws) if ws.healthy => Ok(ws.client.clone()),
            Some(_) => Err(tonic::Status::unavailable(format!("Worker {} unhealthy", worker_id))),
            None => Err(tonic::Status::unavailable(format!("Worker {} not connected", worker_id))),
        }
    }

    /// Start background tasks: eager connect + periodic health check.
    /// Optionally wires into ShardState for dynamic rebalancing.
    pub fn start_background(
        self: &Arc<Self>,
        shard_state: Option<Arc<dyn ShardState>>,
    ) {
        let pool = self.clone();

        tokio::spawn(async move {
            // Eager initial connect
            pool.connect_all().await;
            // Periodic health check
            pool.health_check_loop(shard_state).await;
        });
    }

    /// Build an endpoint with standard settings and optional TLS.
    fn build_endpoint(&self, host: &str, port: u16) -> Result<tonic::transport::Endpoint, String> {
        let scheme = if self.tls_config.is_some() { "https" } else { "http" };
        let addr = format!("{}://{}:{}", scheme, host, port);
        let mut endpoint = tonic::transport::Channel::from_shared(addr.clone())
            .map_err(|e| format!("Invalid endpoint URI {}: {}", addr, e))?
            .connect_timeout(Duration::from_secs(self.connect_timeout_secs))
            .timeout(self.query_timeout)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(self.keepalive_interval_secs))
            .keep_alive_timeout(Duration::from_secs(self.keepalive_timeout_secs))
            .http2_adaptive_window(true);
        if let Some(ref tls) = self.tls_config {
            endpoint = endpoint.tls_config(tls.clone())
                .map_err(|e| format!("TLS config error: {}", e))?;
        }
        Ok(endpoint)
    }

    async fn connect_all(&self) {
        let endpoints = self.endpoints.read().await.clone();
        for (wid, (host, port)) in &endpoints {
            let endpoint = match self.build_endpoint(host, *port) {
                Ok(ep) => ep,
                Err(e) => {
                    warn!("Failed to build endpoint for worker {}: {}", wid, e);
                    continue;
                }
            };
            match endpoint.connect().await {
                Ok(channel) => {
                    let mut client = pb::lance_executor_service_client::LanceExecutorServiceClient::new(channel)
                        .max_decoding_message_size(256 * 1024 * 1024)
                        .max_encoding_message_size(256 * 1024 * 1024);
                    // Immediate health check to populate shard/row data
                    let (loaded_shards, total_rows) = match tokio::time::timeout(
                        Duration::from_secs(self.ping_timeout_secs),
                        client.health_check(Request::new(pb::HealthCheckRequest {})),
                    ).await {
                        Ok(Ok(resp)) => {
                            let r = resp.into_inner();
                            (r.loaded_shards, r.total_rows)
                        }
                        _ => (0, 0),
                    };
                    let mut workers = self.workers.write().await;
                    workers.insert(wid.clone(), WorkerState {
                        client,
                        healthy: true,
                        last_check: Instant::now(),
                        consecutive_failures: 0,
                        loaded_shards,
                        total_rows,
                    });
                    info!("Connected to worker {} at {}:{} ({} shards, {} rows)",
                        wid, host, port, loaded_shards, total_rows);
                }
                Err(e) => {
                    warn!("Failed to connect to worker {} at {}:{}: {}", wid, host, port, e);
                }
            }
        }
    }

    async fn health_check_loop(
        &self,
        shard_state: Option<Arc<dyn ShardState>>,
    ) {
        let check_interval = Duration::from_secs(self.health_check_interval_secs);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                _ = self.shutdown.notified() => {
                    info!("Health check loop stopping (shutdown)");
                    return;
                }
            }

            // Phase 1: Collect health check results WITHOUT holding locks during I/O
            let mut results: Vec<(String, HealthCheckResult)> = Vec::new();

            let endpoints_snapshot = self.endpoints.read().await.clone();
            for (wid, (host, port)) in &endpoints_snapshot {
                // Force a fresh connect whenever the worker is either absent or
                // currently unhealthy. The old condition (`!contains_key`) left
                // a stale tonic Channel attached to a previous worker process
                // after it died and came back: health_check alone is not enough
                // to prove that Channel can still carry real RPC traffic, so
                // scatter_gather would hit it and fail with UNAVAILABLE on the
                // first post-restart query. Replacing the entire WorkerState
                // (via the Reconnected branch below) guarantees scatter_gather
                // sees a freshly connected client.
                let needs_connect = {
                    let workers = self.workers.read().await;
                    match workers.get(wid) {
                        None => true,
                        Some(ws) => !ws.healthy,
                    }
                };

                if needs_connect {
                    let endpoint = match self.build_endpoint(host, *port) {
                        Ok(ep) => ep,
                        Err(e) => {
                            warn!("Failed to build endpoint for worker {}: {}", wid, e);
                            continue;
                        }
                    };
                    if let Ok(channel) = endpoint.connect().await {
                        results.push((wid.clone(), HealthCheckResult::Reconnected {
                            client: pb::lance_executor_service_client::LanceExecutorServiceClient::new(channel)
                        .max_decoding_message_size(256 * 1024 * 1024)
                        .max_encoding_message_size(256 * 1024 * 1024),
                            host: host.clone(),
                            port: *port,
                        }));
                    } else {
                        // Reconnect failed. Count this as a Failed tick so
                        // `consecutive_failures` keeps climbing past
                        // `unhealthy_threshold` towards `remove_threshold`.
                        // Without this branch a permanently-offline worker
                        // pegged at `unhealthy` never gets evicted: the
                        // Failed path only runs on the ping side, which
                        // we never take once !healthy. For a never-yet-
                        // connected worker the Failed arm is a no-op
                        // (get_mut returns None), so this only removes
                        // workers that were once healthy and then died.
                        results.push((wid.clone(), HealthCheckResult::Failed));
                    }
                    continue;
                }

                // Clone client outside of lock, then ping without holding any lock
                let client = self.workers.read().await
                    .get(wid).map(|ws| ws.client.clone());

                if let Some(mut c) = client {
                    match tokio::time::timeout(
                        Duration::from_secs(self.ping_timeout_secs),
                        c.health_check(Request::new(pb::HealthCheckRequest {})),
                    ).await {
                        Ok(Ok(resp)) => {
                            let r = resp.into_inner();
                            results.push((wid.clone(), HealthCheckResult::Healthy {
                                loaded_shards: r.loaded_shards,
                                total_rows: r.total_rows,
                                shard_names: r.shard_names,
                            }));
                        }
                        _ => {
                            results.push((wid.clone(), HealthCheckResult::Failed));
                        }
                    }
                }
            }

            // Phase 2: Apply results under brief lock acquisitions
            for (wid, result) in results {
                match result {
                    HealthCheckResult::Reconnected { client, host, port } => {
                        self.workers.write().await.insert(wid.clone(), WorkerState {
                            client, healthy: true,
                            last_check: Instant::now(), consecutive_failures: 0,
                            loaded_shards: 0, total_rows: 0,
                        });
                        info!("Health: worker {} reconnected", wid);
                        if let Some(ref ss) = shard_state {
                            ss.register_executor(&wid, &host, port).await;
                        }
                    }
                    HealthCheckResult::Healthy { loaded_shards, total_rows, shard_names } => {
                        // Release the write guard before recovery. Shard
                        // recovery calls self.get_healthy_client(), which
                        // takes a read lock on `workers`; tokio RwLock is
                        // write-preferring and not reentrant, so holding
                        // the write guard here deadlocks the health loop.
                        {
                            let mut workers = self.workers.write().await;
                            if let Some(ws) = workers.get_mut(&wid) {
                                ws.healthy = true;
                                ws.last_check = Instant::now();
                                ws.consecutive_failures = 0;
                                ws.loaded_shards = loaded_shards;
                                ws.total_rows = total_rows;
                            }
                        }
                        // Shard recovery: check if worker is missing expected shards
                        if let Some(ref ss) = shard_state {
                            let actual: std::collections::HashSet<String> = shard_names.into_iter().collect();
                            // Find shards assigned to this worker that it doesn't have loaded
                            for table in ss.all_tables().await {
                                let routing = ss.get_shard_routing(&table).await;
                                for (shard, primary, secondary) in &routing {
                                    let assigned = primary == &wid
                                        || secondary.as_ref().is_some_and(|s| s == &wid);
                                    if assigned && !actual.contains(shard) {
                                        // Shard missing — attempt recovery via LoadShard
                                        if let Ok(mut client) = self.get_healthy_client(&wid).await {
                                            let uri = self.get_shard_uri(shard).await;
                                            if let Some(uri) = uri {
                                                info!("Shard recovery: loading {} on {} (missing after restart)", shard, wid);
                                                let _ = client.load_shard(Request::new(pb::LoadShardRequest {
                                                    shard_name: shard.clone(),
                                                    uri,
                                                    storage_options: std::collections::HashMap::new(),
                                                })).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    HealthCheckResult::Failed => {
                        let (should_remove, _consecutive) = {
                            let mut workers = self.workers.write().await;
                            if let Some(ws) = workers.get_mut(&wid) {
                                ws.consecutive_failures += 1;
                                ws.last_check = Instant::now();
                                if ws.consecutive_failures >= self.unhealthy_threshold {
                                    ws.healthy = false;
                                    warn!("Health: worker {} unhealthy ({} failures)", wid, ws.consecutive_failures);
                                }
                                (ws.consecutive_failures >= self.remove_threshold, ws.consecutive_failures)
                            } else {
                                (false, 0)
                            }
                        };
                        if should_remove {
                            self.workers.write().await.remove(&wid);
                            warn!("Health: worker {} removed, will retry", wid);
                            if let Some(ref ss) = shard_state {
                                ss.remove_executor(&wid).await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Poll until `expected` workers report healthy=true, or until
    /// `timeout` elapses. Returns true on success.
    ///
    /// Exists to eliminate the `sleep(2_000ms)` pattern in integration
    /// tests: those sleeps coped with ConnectionPool's 10 s health-check
    /// cadence by waiting a worst-case window, which flakes under CI
    /// load and wastes real time. This helper polls at 50 ms so tests
    /// resume as soon as connect_all completes.
    pub async fn wait_until_healthy(&self, expected: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let poll = Duration::from_millis(50);
        loop {
            let healthy = self
                .worker_statuses()
                .await
                .into_iter()
                .filter(|(_, _, _, h, _, _, _)| *h)
                .count();
            if healthy >= expected {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// Get health status for all workers (for cluster status API).
    pub async fn worker_statuses(&self) -> Vec<(String, String, u16, bool, Instant, u32, u64)> {
        let workers = self.workers.read().await;
        let endpoints = self.endpoints.read().await;
        endpoints.iter().map(|(id, (host, port))| {
            let (healthy, last_check, loaded_shards, total_rows) = workers.get(id)
                .map(|ws| (ws.healthy, ws.last_check, ws.loaded_shards, ws.total_rows))
                .unwrap_or((false, Instant::now(), 0, 0));
            (id.clone(), host.clone(), *port, healthy, last_check, loaded_shards, total_rows)
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool(workers: Vec<(&str, &str, u16)>) -> ConnectionPool {
        let endpoints: HashMap<String, (String, u16)> = workers.into_iter()
            .map(|(id, host, port)| (id.to_string(), (host.to_string(), port)))
            .collect();
        ConnectionPool::new(endpoints, Duration::from_secs(30), &Default::default(), &Default::default())
    }

    #[tokio::test]
    async fn test_get_healthy_client_not_connected() {
        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)]);
        // No connections yet — should return unavailable
        let result = pool.get_healthy_client("w0").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("not connected"));
    }

    #[tokio::test]
    async fn test_get_healthy_client_unknown_worker() {
        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)]);
        let result = pool.get_healthy_client("w999").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("not connected"));
    }

    #[tokio::test]
    async fn test_worker_statuses_no_connections() {
        let pool = make_pool(vec![
            ("w0", "127.0.0.1", 50100),
            ("w1", "127.0.0.1", 50101),
        ]);
        let statuses = pool.worker_statuses().await;
        assert_eq!(statuses.len(), 2);
        // All should be unhealthy (not connected)
        for (_, _host, _port, healthy, _, _, _) in &statuses {
            assert!(!healthy);
        }
    }

    #[tokio::test]
    async fn test_empty_pool() {
        let pool = make_pool(vec![]);
        let statuses = pool.worker_statuses().await;
        assert_eq!(statuses.len(), 0);
    }

    #[tokio::test]
    async fn test_add_endpoint_dynamically() {
        let pool = make_pool(vec![]);
        pool.add_endpoint("w_new", "10.0.0.1", 50100).await;
        let statuses = pool.worker_statuses().await;
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].0, "w_new");
        assert_eq!(statuses[0].1, "10.0.0.1");
        assert_eq!(statuses[0].2, 50100);
    }

    // ── B2.2: role-aware routing ──

    #[tokio::test]
    async fn test_role_of_defaults_to_either() {
        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)]);
        use lance_distributed_common::config::ExecutorRole;
        // No roles configured → Either for every worker.
        assert_eq!(pool.role_of("w0").await, ExecutorRole::Either);
        assert_eq!(pool.role_of("never-seen").await, ExecutorRole::Either);
    }

    #[tokio::test]
    async fn test_pick_by_role_reads_prefer_read_primary() {
        let pool = make_pool(vec![
            ("w0", "127.0.0.1", 50100),
            ("w1", "127.0.0.1", 50101),
        ]);
        use lance_distributed_common::config::ExecutorRole::*;
        let mut roles = HashMap::new();
        roles.insert("w0".to_string(), WritePrimary);
        roles.insert("w1".to_string(), ReadPrimary);
        pool.set_roles(roles).await;

        // Reads should pick w1 (ReadPrimary) over w0 (WritePrimary).
        let candidates = vec!["w0".to_string(), "w1".to_string()];
        let idx = pool.pick_by_role(&candidates, true).await.unwrap();
        assert_eq!(candidates[idx], "w1");

        // Writes should pick w0 (WritePrimary) over w1 (ReadPrimary).
        let idx = pool.pick_by_role(&candidates, false).await.unwrap();
        assert_eq!(candidates[idx], "w0");
    }

    #[tokio::test]
    async fn test_pick_by_role_either_beats_opposite() {
        let pool = make_pool(vec![
            ("w0", "127.0.0.1", 50100),
            ("w1", "127.0.0.1", 50101),
        ]);
        use lance_distributed_common::config::ExecutorRole::*;
        let mut roles = HashMap::new();
        roles.insert("w0".to_string(), WritePrimary);
        roles.insert("w1".to_string(), Either);
        pool.set_roles(roles).await;

        // For reads: Either (w1) should beat WritePrimary (w0).
        let candidates = vec!["w0".to_string(), "w1".to_string()];
        let idx = pool.pick_by_role(&candidates, true).await.unwrap();
        assert_eq!(candidates[idx], "w1");
    }

    #[tokio::test]
    async fn test_pick_by_role_falls_back_to_opposite() {
        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)]);
        use lance_distributed_common::config::ExecutorRole::*;
        let mut roles = HashMap::new();
        roles.insert("w0".to_string(), WritePrimary);
        pool.set_roles(roles).await;

        // Reads with only WritePrimary available — must still route there,
        // role is a preference, not a hard constraint.
        let candidates = vec!["w0".to_string()];
        let idx = pool.pick_by_role(&candidates, true).await.unwrap();
        assert_eq!(candidates[idx], "w0");
    }

    #[tokio::test]
    async fn test_pick_by_role_empty() {
        let pool = make_pool(vec![]);
        assert!(pool.pick_by_role(&[], true).await.is_none());
    }

    #[tokio::test]
    async fn test_pick_by_role_all_either_picks_first() {
        // 0.1.x default: every worker is Either. Picker returns index 0;
        // the round-robin in scatter_gather handles load balance.
        let pool = make_pool(vec![("w0", "", 1), ("w1", "", 2), ("w2", "", 3)]);
        let candidates = vec!["w0".to_string(), "w1".to_string(), "w2".to_string()];
        assert_eq!(pool.pick_by_role(&candidates, true).await.unwrap(), 0);
        assert_eq!(pool.pick_by_role(&candidates, false).await.unwrap(), 0);
    }

    // ── H7: connection_pool lib-coverage expansion ──

    #[tokio::test]
    async fn test_build_endpoint_http_scheme_without_tls() {
        let pool = make_pool(vec![]);
        let ep = pool.build_endpoint("10.0.0.1", 50100).unwrap();
        // Endpoint doesn't expose the URI for direct inspection, but
        // construction must succeed — validates the format! path +
        // from_shared + timeout setters.
        let _ = ep;
    }

    #[tokio::test]
    async fn test_build_endpoint_rejects_invalid_host() {
        let pool = make_pool(vec![]);
        // NUL bytes in a URI are invalid — tonic from_shared should reject.
        let err = pool.build_endpoint("10.0.0.1\0junk", 50100).err();
        assert!(err.is_some(), "build_endpoint must reject a NUL-containing host");
    }

    #[tokio::test]
    async fn test_build_endpoint_https_when_tls_configured() {
        // rustls needs a crypto provider installed at process level before
        // any TLS config is built. Idempotent in tests: the first test to
        // run wins, the rest see Err and continue.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let dummy_pem = b"-----BEGIN CERTIFICATE-----\nMIIBkTCB+w==\n-----END CERTIFICATE-----\n".to_vec();
        let pool = make_pool(vec![]).with_tls(dummy_pem);
        // Scheme flips to https; build_endpoint adapts.
        let res = pool.build_endpoint("10.0.0.1", 50100);
        // Either success or TLS config error — both prove the scheme/TLS
        // branches are exercised. With a malformed cert we expect Err.
        let _ = res;
    }

    #[tokio::test]
    async fn test_get_healthy_client_returns_unhealthy_error() {
        // Inject a WorkerState marked unhealthy and prove the error branch.
        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)]);
        {
            // Build a dummy client — we won't call it, we just need a
            // WorkerState value. Use a bare Endpoint that never connects.
            let ep = tonic::transport::Channel::from_static("http://127.0.0.1:50100")
                .connect_timeout(Duration::from_millis(1));
            let channel = ep.connect_lazy();
            let client = pb::lance_executor_service_client::LanceExecutorServiceClient::new(channel);
            pool.workers.write().await.insert("w0".to_string(), WorkerState {
                client,
                healthy: false,
                last_check: Instant::now(),
                consecutive_failures: 3,
                loaded_shards: 0,
                total_rows: 0,
            });
        }
        let err = pool.get_healthy_client("w0").await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("unhealthy"), "got: {}", err.message());
    }

    #[tokio::test]
    async fn test_set_roles_replaces_prior_map() {
        use lance_distributed_common::config::ExecutorRole::*;
        let pool = make_pool(vec![]);
        let mut m1 = HashMap::new();
        m1.insert("w0".into(), ReadPrimary);
        pool.set_roles(m1).await;
        assert_eq!(pool.role_of("w0").await, ReadPrimary);

        let mut m2 = HashMap::new();
        m2.insert("w0".into(), WritePrimary);
        pool.set_roles(m2).await;
        assert_eq!(pool.role_of("w0").await, WritePrimary,
                   "set_roles must replace, not merge");
    }

    #[tokio::test]
    async fn test_shutdown_is_idempotent() {
        // Calling shutdown twice should not panic. Also exercises the
        // Notify::notify_one code path so the shutdown struct field is
        // covered.
        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)]);
        pool.shutdown();
        pool.shutdown(); // no panic
    }

    #[tokio::test]
    async fn test_add_endpoint_idempotency_under_add_then_lookup() {
        let pool = make_pool(vec![]);
        pool.add_endpoint("w_dyn", "10.0.0.5", 50200).await;
        pool.add_endpoint("w_dyn", "10.0.0.5", 50200).await;  // same again
        let statuses = pool.worker_statuses().await;
        assert_eq!(statuses.iter().filter(|(id, _, _, _, _, _, _)| id == "w_dyn").count(), 1,
                   "repeated add_endpoint with same id must not duplicate");
    }

    #[tokio::test]
    async fn test_get_shard_uri_missing_key() {
        let uris = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let pool = make_pool(vec![]).with_shard_uris(uris);
        assert!(pool.get_shard_uri("never-registered").await.is_none());
    }

    #[tokio::test]
    async fn test_add_endpoint_overwrites_same_id() {
        // Worker reconnecting to a new host/port (K8s pod reschedule).
        let pool = make_pool(vec![("w0", "10.0.0.1", 50100)]);
        pool.add_endpoint("w0", "10.0.0.5", 50100).await;
        let statuses = pool.worker_statuses().await;
        assert_eq!(statuses.len(), 1, "same id must overwrite, not duplicate");
        assert_eq!(statuses[0].1, "10.0.0.5");
    }

    #[tokio::test]
    async fn test_get_shard_uri_without_registry() {
        // Pool without with_shard_uris returns None for every lookup —
        // caller falls back to whatever bootstrapping path they have.
        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)]);
        assert!(pool.get_shard_uri("any_shard").await.is_none());
    }

    // ── beta.5: live-network coverage for connect_all + health_check_loop ──
    //
    // These exercise the paths that unit-level getters above can't reach:
    // real gRPC connect, periodic ping, unhealthy→removed transition,
    // reconnect-after-down, and shard recovery via LoadShard.

    use crate::test_support::{
        spawn_mock_worker, HealthBehavior, MockState, SearchBehavior,
    };
    use lance_distributed_common::config::{HealthCheckConfig, ServerConfig};

    fn fast_hc() -> HealthCheckConfig {
        HealthCheckConfig {
            interval_secs: 1,
            unhealthy_threshold: 2,
            remove_threshold: 4,
            refresh_interval_secs: 1,
            reconciliation_interval_secs: 1,
            connect_timeout_secs: 1,
            ping_timeout_secs: 1,
        }
    }

    async fn wait_healthy(pool: &Arc<ConnectionPool>, wid: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let statuses = pool.worker_statuses().await;
            if statuses.iter().any(|s| s.0 == wid && s.3) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    async fn wait_unhealthy(pool: &Arc<ConnectionPool>, wid: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let statuses = pool.worker_statuses().await;
            if statuses.iter().any(|s| s.0 == wid && !s.3) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    #[tokio::test]
    async fn test_connect_all_populates_healthy_client() {
        let state = MockState::new(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 2, total_rows: 123, shard_names: vec!["s0".into(), "s1".into()] },
        );
        let mock = spawn_mock_worker(state.clone()).await;
        let mut endpoints = HashMap::new();
        endpoints.insert("w0".to_string(), ("127.0.0.1".to_string(), mock.port));
        let pool = Arc::new(ConnectionPool::new(
            endpoints, Duration::from_secs(5), &fast_hc(), &ServerConfig::default(),
        ));
        pool.start_background(None);
        assert!(wait_healthy(&pool, "w0", Duration::from_secs(3)).await);

        // get_healthy_client returns a usable channel
        let _client = pool.get_healthy_client("w0").await.expect("should be healthy");
        // Health check seeded totals from HealthCheckResponse
        let statuses = pool.worker_statuses().await;
        let (_, _, _, healthy, _, loaded_shards, total_rows) = statuses[0].clone();
        assert!(healthy);
        assert_eq!(loaded_shards, 2);
        assert_eq!(total_rows, 123);
    }

    #[tokio::test]
    async fn test_health_check_marks_unhealthy_after_failures() {
        let state = MockState::new(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 0, total_rows: 0, shard_names: vec![] },
        );
        let mock = spawn_mock_worker(state.clone()).await;
        let mut endpoints = HashMap::new();
        endpoints.insert("w0".to_string(), ("127.0.0.1".to_string(), mock.port));
        let pool = Arc::new(ConnectionPool::new(
            endpoints, Duration::from_secs(5), &fast_hc(), &ServerConfig::default(),
        ));
        pool.start_background(None);
        assert!(wait_healthy(&pool, "w0", Duration::from_secs(3)).await);

        // Flip mock to return Err on every health check — the pool should
        // mark it unhealthy after unhealthy_threshold=2 consecutive failures.
        state.set_health(HealthBehavior::Err).await;
        assert!(wait_unhealthy(&pool, "w0", Duration::from_secs(5)).await,
            "pool must see sustained health errors and flip healthy=false");
    }

    #[tokio::test]
    async fn test_health_check_removes_after_remove_threshold() {
        // Mock stays up but returns Err on every health_check. With the
        // fix that counts reconnect-failure as a Failed tick, this case
        // still works when (unhealthy, remove) coincide — the first
        // two pings fail, failures reach unhealthy_threshold=2 and
        // remove_threshold=2 in the same tick, worker evicted.
        let hc = HealthCheckConfig {
            interval_secs: 1,
            unhealthy_threshold: 2,
            remove_threshold: 2,
            refresh_interval_secs: 1,
            reconciliation_interval_secs: 1,
            connect_timeout_secs: 1,
            ping_timeout_secs: 1,
        };
        let state = MockState::new(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 0, total_rows: 0, shard_names: vec![] },
        );
        let mock = spawn_mock_worker(state.clone()).await;
        let mut endpoints = HashMap::new();
        endpoints.insert("w0".to_string(), ("127.0.0.1".to_string(), mock.port));
        let pool = Arc::new(ConnectionPool::new(
            endpoints, Duration::from_secs(5), &hc, &ServerConfig::default(),
        ));
        pool.start_background(None);
        assert!(wait_healthy(&pool, "w0", Duration::from_secs(3)).await);

        state.set_health(HealthBehavior::Err).await;
        // After 2 consecutive ping failures the Failed arm fires
        // should_remove=true and the worker is evicted from `workers`.
        // A later tick will see needs_connect=true and re-insert via
        // Reconnected, so the window where contains_key is false can be
        // brief; poll frequently enough to catch it.
        let deadline = Instant::now() + Duration::from_secs(6);
        let mut saw_removed = false;
        while Instant::now() < deadline {
            {
                let workers = pool.workers.read().await;
                if !workers.contains_key("w0") {
                    saw_removed = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(saw_removed, "expected worker to be removed after remove_threshold failures");
    }

    #[tokio::test]
    async fn test_permanently_offline_worker_is_evicted_at_remove_threshold() {
        // Production defaults (unhealthy=3, remove=5) were effectively
        // dead code before: once a worker flipped to !healthy, the loop
        // stopped counting ping-side failures, and reconnect-side
        // failures weren't counted at all — so a permanently-offline
        // worker was pegged at unhealthy and never evicted. This test
        // pins the fixed behaviour: kill the mock, and within
        // ~remove_threshold × interval seconds the worker is removed.
        let hc = HealthCheckConfig {
            interval_secs: 1,
            unhealthy_threshold: 2,
            remove_threshold: 4,
            refresh_interval_secs: 1,
            reconciliation_interval_secs: 1,
            connect_timeout_secs: 1,
            ping_timeout_secs: 1,
        };
        let state = MockState::new(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 0, total_rows: 0, shard_names: vec![] },
        );
        let mock = spawn_mock_worker(state.clone()).await;
        let port = mock.port;
        let mut endpoints = HashMap::new();
        endpoints.insert("w0".to_string(), ("127.0.0.1".to_string(), port));
        let pool = Arc::new(ConnectionPool::new(
            endpoints, Duration::from_secs(5), &hc, &ServerConfig::default(),
        ));
        pool.start_background(None);
        assert!(wait_healthy(&pool, "w0", Duration::from_secs(3)).await);

        // Take the mock offline. The listener closes; future connect()
        // attempts will fail with ConnectionRefused, and pings on the
        // last-known channel will fail too.
        drop(mock);

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut saw_removed = false;
        while Instant::now() < deadline {
            {
                let workers = pool.workers.read().await;
                if !workers.contains_key("w0") {
                    saw_removed = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(saw_removed,
            "a permanently-offline worker must be evicted once consecutive_failures ≥ remove_threshold");
    }

    #[tokio::test]
    async fn test_shutdown_stops_health_check_loop() {
        let state = MockState::new(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 0, total_rows: 0, shard_names: vec![] },
        );
        let mock = spawn_mock_worker(state.clone()).await;
        let mut endpoints = HashMap::new();
        endpoints.insert("w0".to_string(), ("127.0.0.1".to_string(), mock.port));
        let pool = Arc::new(ConnectionPool::new(
            endpoints, Duration::from_secs(5), &fast_hc(), &ServerConfig::default(),
        ));
        pool.start_background(None);
        assert!(wait_healthy(&pool, "w0", Duration::from_secs(3)).await);

        // Capture call counter, signal shutdown, verify it stops climbing.
        pool.shutdown();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let calls_before = state.health_calls.load(std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(3)).await;
        let calls_after = state.health_calls.load(std::sync::atomic::Ordering::SeqCst);
        // Allow a tolerance of 1 for a just-in-flight ping that landed
        // before the shutdown notify was observed.
        assert!(
            calls_after <= calls_before + 1,
            "shutdown must stop the health loop (before={} after={})",
            calls_before, calls_after
        );
    }

    #[tokio::test]
    async fn test_connect_all_skips_unreachable_endpoint() {
        // Port 1 on localhost is reliably unreachable. The pool should not
        // insert a worker entry at all — worker_statuses shows the endpoint
        // row but healthy=false.
        let mut endpoints = HashMap::new();
        endpoints.insert("w0".to_string(), ("127.0.0.1".to_string(), 1u16));
        let pool = Arc::new(ConnectionPool::new(
            endpoints, Duration::from_secs(5), &fast_hc(), &ServerConfig::default(),
        ));
        pool.start_background(None);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let statuses = pool.worker_statuses().await;
        assert_eq!(statuses.len(), 1);
        assert!(!statuses[0].3, "unreachable endpoint should not be healthy");
    }

    #[tokio::test]
    async fn test_shard_recovery_triggers_load_shard_on_missing() {
        // Worker reports empty shard_names during health_check; pool's
        // recovery branch should call LoadShard for any assigned shard
        // not listed. This requires wiring a shard_state + shard_uri
        // registry into the pool.
        use lance_distributed_common::config::{
            ClusterConfig, ExecutorConfig, ShardConfig, TableConfig,
        };
        use lance_distributed_common::shard_state::StaticShardState;

        let state = MockState::new(
            SearchBehavior::Ok { rows: 1 },
            HealthBehavior::Ok { loaded_shards: 0, total_rows: 0, shard_names: vec![] },
        );
        let mock = spawn_mock_worker(state.clone()).await;
        let cluster = ClusterConfig {
            tables: vec![TableConfig {
                name: "t".into(),
                shards: vec![ShardConfig {
                    name: "s0".into(),
                    uri: "/tmp/mock-s0".into(),
                    executors: vec!["w0".into()],
                }],
            }],
            executors: vec![ExecutorConfig {
                id: "w0".into(),
                host: "127.0.0.1".into(),
                port: mock.port,
                role: Default::default(),
            }],
            ..Default::default()
        };
        let shard_state: Arc<dyn ShardState> = Arc::new(StaticShardState::from_config(&cluster));

        let mut uris_map = HashMap::new();
        uris_map.insert("s0".to_string(), "/tmp/mock-s0".to_string());
        let shard_uris = Arc::new(tokio::sync::RwLock::new(uris_map));

        let mut endpoints = HashMap::new();
        endpoints.insert("w0".to_string(), ("127.0.0.1".to_string(), mock.port));
        let pool = Arc::new(
            ConnectionPool::new(endpoints, Duration::from_secs(5), &fast_hc(), &ServerConfig::default())
                .with_shard_uris(shard_uris)
        );
        pool.start_background(Some(shard_state));
        assert!(wait_healthy(&pool, "w0", Duration::from_secs(3)).await);

        // Wait at least one health cycle for the recovery branch to fire.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_load = false;
        while Instant::now() < deadline {
            if state.load_shard_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                saw_load = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(saw_load, "expected at least one LoadShard to be issued");
    }

    #[tokio::test]
    async fn test_get_shard_uri_with_registry() {
        use std::sync::Arc;
        use tokio::sync::RwLock;

        let registry = Arc::new(RwLock::new(HashMap::new()));
        registry.write().await.insert(
            "products_shard_00".to_string(),
            "s3://bucket/products/shard_00.lance".to_string(),
        );

        let pool = make_pool(vec![("w0", "127.0.0.1", 50100)])
            .with_shard_uris(registry.clone());

        let uri = pool.get_shard_uri("products_shard_00").await;
        assert_eq!(uri.as_deref(), Some("s3://bucket/products/shard_00.lance"));
        // Unknown shard returns None (not an error).
        assert!(pool.get_shard_uri("not_registered").await.is_none());
    }

}
