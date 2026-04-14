// Licensed under the Apache License, Version 2.0.
// Coordinator connection pool: manages gRPC connections to Workers.
// Handles health checking, auto-reconnect, and stale connection removal.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info, warn};
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
    Healthy,
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
}

/// Manages connections to all Worker nodes.
pub struct ConnectionPool {
    workers: Arc<RwLock<HashMap<String, WorkerState>>>,
    endpoints: HashMap<String, (String, u16)>,
    query_timeout: Duration,
    /// TLS config for client-side connection to workers.
    tls_config: Option<tonic::transport::ClientTlsConfig>,
}

impl ConnectionPool {
    pub fn new(endpoints: HashMap<String, (String, u16)>, query_timeout: Duration) -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            endpoints,
            query_timeout,
            tls_config: None,
        }
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
            .connect_timeout(Duration::from_secs(3))
            .timeout(self.query_timeout)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(20))
            .http2_adaptive_window(true);
        if let Some(ref tls) = self.tls_config {
            endpoint = endpoint.tls_config(tls.clone())
                .map_err(|e| format!("TLS config error: {}", e))?;
        }
        Ok(endpoint)
    }

    async fn connect_all(&self) {
        for (wid, (host, port)) in &self.endpoints {
            let endpoint = match self.build_endpoint(host, *port) {
                Ok(ep) => ep,
                Err(e) => {
                    warn!("Failed to build endpoint for worker {}: {}", wid, e);
                    continue;
                }
            };
            match endpoint.connect().await {
                Ok(channel) => {
                    let client = pb::lance_executor_service_client::LanceExecutorServiceClient::new(channel);
                    let mut workers = self.workers.write().await;
                    workers.insert(wid.clone(), WorkerState {
                        client,
                        healthy: true,
                        last_check: Instant::now(),
                        consecutive_failures: 0,
                    });
                    info!("Connected to worker {} at {}:{}", wid, host, port);
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
        let check_interval = Duration::from_secs(10);
        loop {
            tokio::time::sleep(check_interval).await;

            // Phase 1: Collect health check results WITHOUT holding locks during I/O
            let mut results: Vec<(String, HealthCheckResult)> = Vec::new();

            for (wid, (host, port)) in &self.endpoints {
                let needs_connect = !self.workers.read().await.contains_key(wid);

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
                            client: pb::lance_executor_service_client::LanceExecutorServiceClient::new(channel),
                            host: host.clone(),
                            port: *port,
                        }));
                    }
                    continue;
                }

                // Clone client outside of lock, then ping without holding any lock
                let client = self.workers.read().await
                    .get(wid).map(|ws| ws.client.clone());

                if let Some(mut c) = client {
                    let ok = matches!(
                        tokio::time::timeout(
                            Duration::from_secs(3),
                            c.health_check(Request::new(pb::HealthCheckRequest {})),
                        ).await,
                        Ok(Ok(_))
                    );
                    results.push((wid.clone(), if ok {
                        HealthCheckResult::Healthy
                    } else {
                        HealthCheckResult::Failed
                    }));
                }
            }

            // Phase 2: Apply results under brief lock acquisitions
            for (wid, result) in results {
                match result {
                    HealthCheckResult::Reconnected { client, host, port } => {
                        self.workers.write().await.insert(wid.clone(), WorkerState {
                            client, healthy: true,
                            last_check: Instant::now(), consecutive_failures: 0,
                        });
                        info!("Health: worker {} reconnected", wid);
                        if let Some(ref ss) = shard_state {
                            ss.register_executor(&wid, &host, port).await;
                        }
                    }
                    HealthCheckResult::Healthy => {
                        let mut workers = self.workers.write().await;
                        if let Some(ws) = workers.get_mut(&wid) {
                            ws.healthy = true;
                            ws.last_check = Instant::now();
                            ws.consecutive_failures = 0;
                        }
                    }
                    HealthCheckResult::Failed => {
                        let (should_remove, consecutive) = {
                            let mut workers = self.workers.write().await;
                            if let Some(ws) = workers.get_mut(&wid) {
                                ws.consecutive_failures += 1;
                                ws.last_check = Instant::now();
                                if ws.consecutive_failures >= 2 {
                                    ws.healthy = false;
                                    warn!("Health: worker {} unhealthy ({} failures)", wid, ws.consecutive_failures);
                                }
                                (ws.consecutive_failures >= 5, ws.consecutive_failures)
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

    /// Get health status for all workers (for cluster status API).
    pub async fn worker_statuses(&self) -> Vec<(String, String, u16, bool, Instant)> {
        let workers = self.workers.read().await;
        self.endpoints.iter().map(|(id, (host, port))| {
            let (healthy, last_check) = workers.get(id)
                .map(|ws| (ws.healthy, ws.last_check))
                .unwrap_or((false, Instant::now()));
            (id.clone(), host.clone(), *port, healthy, last_check)
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
        ConnectionPool::new(endpoints, Duration::from_secs(30))
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
        for (_, _host, _port, healthy, _) in &statuses {
            assert!(!healthy);
        }
    }

    #[tokio::test]
    async fn test_empty_pool() {
        let pool = make_pool(vec![]);
        let statuses = pool.worker_statuses().await;
        assert_eq!(statuses.len(), 0);
    }
}
