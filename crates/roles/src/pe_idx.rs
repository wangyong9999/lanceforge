// Composite PE + IDX runner.
//
// Phase A: the worker binary's body extracted behind a
// `pub async fn run(...)`. The PE-shaped parts (gRPC Executor
// server, LanceTableRegistry) and the IDX-shaped parts
// (auto-compaction loop) are annotated so Phase D can cleanly
// extract compaction into a standalone IDX runner backed by the
// CP-served JobQueue.
//
// Role breakdown within this function (Phase A — in-process only):
//
//   PE responsibilities:
//     - tonic Executor gRPC server
//     - LanceTableRegistry (per-shard dataset cache + version poller)
//     - Per-request read/write/load_shard/create_local_shard handling
//
//   IDX responsibilities:
//     - Periodic auto-compaction loop (when enabled)
//     - compact_all RPC handler (stays on PE for now; Phase D moves
//       to CP-dispatched jobs)

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use lance_distributed_common::config::ClusterConfig;
use lance_distributed_proto::lance_executor_service_server::LanceExecutorServiceServer;
use lance_distributed_worker::executor::LanceTableRegistry;
use lance_distributed_worker::service::WorkerService;
use log::info;
use tokio::signal;

/// Run the composite PE + IDX lifecycle inside the current tokio
/// runtime. Returns when the server has drained after SIGTERM /
/// ctrl+c.
///
/// Called by `lance-worker` (Phase A) and `lance-monolith` (Phase B).
pub async fn run(
    config: ClusterConfig,
    worker_id: String,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let shards = config.shards_for_executor(&worker_id);
    if shards.is_empty() {
        info!(
            "No initial shards for worker '{}' — waiting for LoadShard RPCs",
            worker_id
        );
    }

    let server_cfg = &config.server;
    let keepalive_interval = Duration::from_secs(server_cfg.keepalive_interval_secs);
    let keepalive_timeout = Duration::from_secs(server_cfg.keepalive_timeout_secs);

    info!(
        "Starting Lance Worker '{}' with {} shards on port {}",
        worker_id,
        shards.len(),
        port
    );

    // ── PE: per-shard registry + cache ─────────────────────────────
    let ctx = SessionContext::default();
    let registry = Arc::new(
        LanceTableRegistry::with_full_config(
            ctx,
            &shards,
            &config.storage_options,
            &config.cache,
        )
        .await?,
    );

    // ── IDX: periodic auto-compaction loop (Phase 13) ──────────────
    // Phase D moves this behind a CP-dispatched job so a separate
    // IDX fleet can scale without dragging PE read capacity.
    let compact_shutdown = Arc::new(tokio::sync::Notify::new());
    if config.compaction.enabled {
        let reg = registry.clone();
        let interval = Duration::from_secs(config.compaction.interval_secs);
        let stop = compact_shutdown.clone();
        info!(
            "Auto-compaction enabled (interval={}s)",
            config.compaction.interval_secs
        );
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // skip immediate first tick
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = stop.notified() => {
                        log::info!("Auto-compaction loop stopping (shutdown)");
                        return;
                    }
                }
                match reg.compact_all().await {
                    Ok(n) => info!("Auto-compaction: {} table(s) optimized", n),
                    Err(e) => log::warn!("Auto-compaction failed: {}", e),
                }
            }
        });
    }

    // Worker cap is a pathological-single-shard defense (not coord's per-response
    // cap). Keep it generous: 4× coord's cap. Worker truncates only when a
    // single shard's output alone would dwarf the intended global response.
    let worker_cap = config
        .server
        .max_response_bytes
        .saturating_mul(4)
        .max(64 * 1024 * 1024);
    let registry_shutdown = registry.clone();
    let service = WorkerService::with_max_response_bytes(registry, worker_cap);

    // ── PE: gRPC Executor server ───────────────────────────────────
    let addr = format!("0.0.0.0:{}", port).parse()?;
    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(keepalive_interval))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .concurrency_limit_per_connection(server_cfg.concurrency_limit);

    if config.security.tls_enabled() {
        let cert_path = config
            .security
            .tls_cert
            .as_ref()
            .ok_or("TLS enabled but tls_cert path missing")?;
        let key_path = config
            .security
            .tls_key
            .as_ref()
            .ok_or("TLS enabled but tls_key path missing")?;
        let tls_config =
            lance_distributed_common::tls::load_server_tls_config(cert_path, key_path).await?;
        info!("TLS enabled");
        server = server.tls_config(tls_config)?;
    }

    server
        .add_service(
            LanceExecutorServiceServer::new(service)
                .max_decoding_message_size(worker_cap.saturating_add(16 * 1024 * 1024))
                .max_encoding_message_size(worker_cap.saturating_add(16 * 1024 * 1024)),
        )
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.ok();
            info!("Shutdown signal received — draining in-flight requests...");
        })
        .await?;

    // Stop registry background (version poller) and the compaction loop.
    registry_shutdown.shutdown();
    compact_shutdown.notify_waiters();
    info!("Worker stopped — all background tasks signalled, in-flight requests completed");
    Ok(())
}
