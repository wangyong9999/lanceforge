// Query Node role — CP client + routing cache.
//
// Phase C.4 of docs/ARCHITECTURE_V2_PLAN.md. Exposes `RoutingClient`,
// a thin library that QN (or a test harness) uses to ask CP for
// table routing, with a TTL cache and graceful degradation when CP
// is unreachable (decision D1).
//
// Phase C.4 is intentionally additive: the existing
// CoordinatorService keeps its in-process ShardState and doesn't
// call RoutingClient. Phase E wires RoutingClient into the
// standalone lance-qn bin once the four-role split lands. Until
// then this module is exercised by unit tests only.
//
// Cache state machine (stale-while-error):
//
//   age(entry) < TTL          → serve from cache, no CP call
//   TTL <= age < STALE_LIMIT  → call CP; on success refresh, on
//                               failure serve stale with warn
//   age >= STALE_LIMIT        → call CP; on failure return
//                               ResolveError::CpUnreachable
//
// Background subscribe task: reconnects on stream drop and holds
// the stream open so Phase D's push-invalidation has a client.
// Cancelled cleanly on ShutdownSignal.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lance_distributed_proto::cluster_control_client::ClusterControlClient;
use lance_distributed_proto::{ResolveTableRequest, SubscribeRoutingRequest, TableRouting};
use log::{debug, warn};
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::common::ShutdownSignal;

const DEFAULT_TTL: Duration = Duration::from_secs(30);
const DEFAULT_STALE_LIMIT: Duration = Duration::from_secs(60);

/// Error surface visible to QN callers.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// CP explicitly returned NotFound for the table.
    #[error("table not found: {0}")]
    NotFound(String),
    /// CP is unreachable AND the local cache has nothing fresh
    /// enough to serve. Distinct from NotFound so callers can
    /// return UNAVAILABLE vs NOT_FOUND correctly.
    #[error("control plane unreachable: {0}")]
    CpUnreachable(String),
    /// Any other error from CP (malformed response, precondition
    /// failed on min_schema_version, etc).
    #[error("control plane error: {0}")]
    Other(String),
}

/// Options for a `RoutingClient`. All overridable for testing.
#[derive(Clone, Debug)]
pub struct RoutingClientOptions {
    pub ttl: Duration,
    pub stale_limit: Duration,
    /// Delay between resubscribe attempts when the SubscribeRouting
    /// stream errors or closes.
    pub resubscribe_backoff: Duration,
}

impl Default for RoutingClientOptions {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            stale_limit: DEFAULT_STALE_LIMIT,
            resubscribe_backoff: Duration::from_secs(1),
        }
    }
}

struct CacheEntry {
    routing: TableRouting,
    inserted_at: Instant,
}

/// QN-side client for the ClusterControl service. Holds a CP
/// channel, a per-table routing cache, and (optionally) a
/// background resubscribe task.
#[derive(Clone)]
pub struct RoutingClient {
    inner: Arc<Inner>,
}

struct Inner {
    client: RwLock<ClusterControlClient<Channel>>,
    cache: RwLock<HashMap<String, CacheEntry>>,
    opts: RoutingClientOptions,
}

impl RoutingClient {
    /// Connect to the CP at the given URL (e.g. `http://127.0.0.1:50052`).
    /// The connection is lazy — the first RPC is what actually dials.
    pub async fn connect(cp_url: String) -> Result<Self, ResolveError> {
        Self::connect_with(cp_url, RoutingClientOptions::default()).await
    }

    pub async fn connect_with(
        cp_url: String,
        opts: RoutingClientOptions,
    ) -> Result<Self, ResolveError> {
        let client = ClusterControlClient::connect(cp_url)
            .await
            .map_err(|e| ResolveError::CpUnreachable(format!("connect: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                client: RwLock::new(client),
                cache: RwLock::new(HashMap::new()),
                opts,
            }),
        })
    }

    /// Resolve table routing with cache + stale-while-error fallback.
    pub async fn resolve(&self, table: &str) -> Result<TableRouting, ResolveError> {
        let now = Instant::now();
        // Fast path: fresh cache.
        if let Some(fresh) = self.cached_if(|age| age < self.inner.opts.ttl, table, now).await {
            return Ok(fresh);
        }
        // Try CP.
        match self.fetch_from_cp(table).await {
            Ok(fresh) => Ok(fresh),
            Err(ResolveError::NotFound(t)) => Err(ResolveError::NotFound(t)),
            Err(ResolveError::CpUnreachable(msg))
            | Err(ResolveError::Other(msg)) => {
                // CP failed — can we serve stale?
                if let Some(stale) = self
                    .cached_if(|age| age < self.inner.opts.stale_limit, table, now)
                    .await
                {
                    warn!(
                        "CP unreachable, serving stale routing for '{}' ({}s old): {}",
                        table,
                        now.elapsed().as_secs(),
                        msg
                    );
                    Ok(stale)
                } else {
                    Err(ResolveError::CpUnreachable(msg))
                }
            }
        }
    }

    async fn cached_if(
        &self,
        fresh_enough: impl Fn(Duration) -> bool,
        table: &str,
        now: Instant,
    ) -> Option<TableRouting> {
        let cache = self.inner.cache.read().await;
        let entry = cache.get(table)?;
        if fresh_enough(now.saturating_duration_since(entry.inserted_at)) {
            Some(entry.routing.clone())
        } else {
            None
        }
    }

    async fn fetch_from_cp(&self, table: &str) -> Result<TableRouting, ResolveError> {
        // Tonic clients are cheap to clone (Channel is Arc-backed), so
        // take a short read lock, clone out, release. The clone is an
        // independent handle — subsequent RPCs don't need to hold the
        // lock and can run concurrently with the subscribe task.
        let mut client = self.inner.client.read().await.clone();
        let resp = client
            .resolve_table(ResolveTableRequest {
                table_name: table.to_string(),
                min_schema_version: 0,
            })
            .await
            .map_err(|e| match e.code() {
                tonic::Code::NotFound => ResolveError::NotFound(table.to_string()),
                tonic::Code::Unavailable => ResolveError::CpUnreachable(e.message().to_string()),
                _ => ResolveError::Other(e.message().to_string()),
            })?;
        let routing = resp.into_inner();
        // Update cache.
        let mut cache = self.inner.cache.write().await;
        cache.insert(
            table.to_string(),
            CacheEntry {
                routing: routing.clone(),
                inserted_at: Instant::now(),
            },
        );
        Ok(routing)
    }

    /// Spawn a background task that keeps a SubscribeRouting stream
    /// open, reconnecting on drop with `resubscribe_backoff`. Runs
    /// until `shutdown` fires. Phase C.4 stub: the stream currently
    /// carries no updates (CP won't push until Phase D), so this
    /// just proves the reconnect loop terminates on shutdown.
    pub fn spawn_subscribe(&self, shutdown: ShutdownSignal) -> tokio::task::JoinHandle<()> {
        let inner = self.inner.clone();
        let backoff = self.inner.opts.resubscribe_backoff;
        tokio::spawn(async move {
            // Register a single Notified future and keep it across
            // the whole task. Per-iteration .notified() futures
            // would miss notify_waiters() fired between iterations
            // (Notify documents notify_waiters as waking only
            // currently-registered waiters, with no stored permit).
            let notified = shutdown.notified();
            tokio::pin!(notified);
            let main = async {
                loop {
                    let snapshot = inner.client.read().await.clone();
                    subscribe_once(snapshot).await;
                    tokio::time::sleep(backoff).await;
                }
            };
            tokio::pin!(main);
            tokio::select! {
                _ = &mut notified => {}
                _ = &mut main => {} // main is infinite, unreachable
            }
        })
    }
}

async fn subscribe_once(mut client: ClusterControlClient<Channel>) {
    let stream = client
        .subscribe_routing(SubscribeRoutingRequest {
            table_names: vec![],
        })
        .await;
    let mut stream = match stream {
        Ok(s) => s.into_inner(),
        Err(e) => {
            debug!("subscribe_routing failed: {}", e);
            return;
        }
    };
    // Loop until the server closes / errors. Shutdown is handled by
    // the caller's outer select on &mut notified, which cancels this
    // future mid-await.
    while let Ok(Some(_update)) = stream.message().await {
        // Phase D wires cache invalidation here.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cp;
    use lance_distributed_common::config::{
        ClusterConfig, ExecutorConfig, ShardConfig, TableConfig,
    };
    use std::time::Duration;
    use tempfile::TempDir;

    fn free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    fn minimal_config(tmp: &TempDir) -> ClusterConfig {
        ClusterConfig {
            metadata_path: Some(
                tmp.path().join("meta.json").to_str().unwrap().to_string(),
            ),
            tables: vec![TableConfig {
                name: "t".into(),
                shards: vec![ShardConfig {
                    name: "s0".into(),
                    uri: "/tmp/qn-test-s0".into(),
                    executors: vec!["w0".into()],
                }],
            }],
            executors: vec![ExecutorConfig {
                id: "w0".into(),
                host: "127.0.0.1".into(),
                port: 9999,
                role: Default::default(),
            }],
            ..Default::default()
        }
    }

    async fn spawn_cp_and_client(
        config: ClusterConfig,
        ttl: Duration,
        stale: Duration,
    ) -> (
        RoutingClient,
        ShutdownSignal,
        tokio::task::JoinHandle<()>,
        u16,
    ) {
        let port = free_port();
        let shutdown = crate::common::new_shutdown();
        let sb = shutdown.clone();
        let handle = tokio::spawn(async move {
            let _ = cp::run(config, port, sb).await;
        });
        // Wait for CP to accept.
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let opts = RoutingClientOptions {
            ttl,
            stale_limit: stale,
            resubscribe_backoff: Duration::from_millis(100),
        };
        let client = RoutingClient::connect_with(format!("http://127.0.0.1:{port}"), opts)
            .await
            .unwrap();
        (client, shutdown, handle, port)
    }

    #[tokio::test]
    async fn first_resolve_hits_cp_then_caches() {
        let tmp = TempDir::new().unwrap();
        let (client, shutdown, handle, _port) =
            spawn_cp_and_client(minimal_config(&tmp), Duration::from_secs(30), Duration::from_secs(60)).await;

        let r1 = client.resolve("t").await.unwrap();
        assert_eq!(r1.table_name, "t");
        assert_eq!(r1.shards.len(), 1);

        // Second call should be a cache hit — even if CP dies next
        // it should still succeed.
        let r2 = client.resolve("t").await.unwrap();
        assert_eq!(r2.shards[0].primary_executor, "w0");

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn missing_table_surfaces_not_found() {
        let tmp = TempDir::new().unwrap();
        let (client, shutdown, handle, _port) =
            spawn_cp_and_client(minimal_config(&tmp), Duration::from_secs(30), Duration::from_secs(60)).await;

        let err = client.resolve("does-not-exist").await.unwrap_err();
        assert!(matches!(err, ResolveError::NotFound(_)), "got: {err:?}");

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn cp_death_within_stale_window_serves_cache() {
        let tmp = TempDir::new().unwrap();
        // Very short TTL / stale so the test doesn't drag.
        let (client, shutdown, handle, _port) = spawn_cp_and_client(
            minimal_config(&tmp),
            Duration::from_millis(100),
            Duration::from_secs(5),
        )
        .await;

        // Populate cache.
        let _ = client.resolve("t").await.unwrap();

        // Kill CP.
        shutdown.notify_waiters();
        let _ = handle.await;

        // Wait out the fresh window.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Still within stale_limit — expect a warn + stale data back.
        let r = client.resolve("t").await.unwrap();
        assert_eq!(r.table_name, "t");
        assert_eq!(r.shards[0].primary_executor, "w0");
    }

    #[tokio::test]
    async fn cp_death_past_stale_window_returns_unreachable() {
        let tmp = TempDir::new().unwrap();
        let (client, shutdown, handle, _port) = spawn_cp_and_client(
            minimal_config(&tmp),
            Duration::from_millis(50),
            Duration::from_millis(150),
        )
        .await;

        let _ = client.resolve("t").await.unwrap();

        shutdown.notify_waiters();
        let _ = handle.await;

        // Past stale_limit.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let err = client.resolve("t").await.unwrap_err();
        assert!(
            matches!(err, ResolveError::CpUnreachable(_)),
            "expected CpUnreachable, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn subscribe_task_exits_on_shutdown() {
        let tmp = TempDir::new().unwrap();
        let (client, shutdown, cp_handle, _port) =
            spawn_cp_and_client(minimal_config(&tmp), Duration::from_secs(30), Duration::from_secs(60)).await;

        let sub_shutdown = crate::common::new_shutdown();
        let sub_handle = client.spawn_subscribe(sub_shutdown.clone());

        // Let the subscribe loop run briefly.
        tokio::time::sleep(Duration::from_millis(100)).await;

        sub_shutdown.notify_waiters();
        // Background task should wind up within the 100ms backoff
        // plus one stream.message() yield. Give it a second's headroom.
        tokio::time::timeout(Duration::from_secs(2), sub_handle)
            .await
            .expect("subscribe task did not exit within 2s")
            .ok();

        shutdown.notify_waiters();
        let _ = cp_handle.await;
    }

    #[tokio::test]
    async fn concurrent_resolves_during_outage_are_consistent() {
        // Chaos E2E (Phase C.5 scope): N concurrent resolves while CP
        // transitions between fresh / stale / expired. Every call
        // should see a coherent state — never partially-updated cache,
        // never a deadlock, and the stale-to-expired transition
        // applies uniformly.
        let tmp = TempDir::new().unwrap();
        let (client, shutdown, handle, _port) = spawn_cp_and_client(
            minimal_config(&tmp),
            Duration::from_millis(50),
            Duration::from_millis(200),
        )
        .await;

        // Prime the cache.
        let _ = client.resolve("t").await.unwrap();

        // Kill CP.
        shutdown.notify_waiters();
        let _ = handle.await;

        // Fire 16 concurrent resolves during the stale window. All
        // should succeed from cache without any deadlock.
        tokio::time::sleep(Duration::from_millis(80)).await; // past TTL, within stale
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let c = client.clone();
            tasks.push(tokio::spawn(async move { c.resolve("t").await }));
        }
        for task in tasks {
            let r = task.await.unwrap().unwrap();
            assert_eq!(r.table_name, "t");
        }

        // Past stale_limit, all 16 concurrent calls should fail with
        // the same error class.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let c = client.clone();
            tasks.push(tokio::spawn(async move { c.resolve("t").await }));
        }
        for task in tasks {
            let e = task.await.unwrap().unwrap_err();
            assert!(
                matches!(e, ResolveError::CpUnreachable(_)),
                "got: {e:?}"
            );
        }
    }

    #[tokio::test]
    async fn fresh_cache_does_not_hit_cp() {
        let tmp = TempDir::new().unwrap();
        // Long TTL so second call stays fresh.
        let (client, shutdown, handle, _port) = spawn_cp_and_client(
            minimal_config(&tmp),
            Duration::from_secs(10),
            Duration::from_secs(20),
        )
        .await;

        // First call — populates cache.
        let _ = client.resolve("t").await.unwrap();

        // Kill CP; a fresh hit should still succeed because we never
        // even contact CP for a fresh cache.
        shutdown.notify_waiters();
        let _ = handle.await;

        let r = client.resolve("t").await.unwrap();
        assert_eq!(r.table_name, "t");
    }
}
