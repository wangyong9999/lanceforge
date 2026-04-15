// Licensed under the Apache License, Version 2.0.
// Lance Worker binary.

use std::sync::Arc;
use std::time::Duration;

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_proto::lance_executor_service_server::LanceExecutorServiceServer;
use lance_distributed_worker::executor::LanceTableRegistry;
use lance_distributed_worker::service::WorkerService;
use datafusion::prelude::SessionContext;
use log::info;
use tokio::signal;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json().with_target(true))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_target(true))
            .init();
    }

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: lance-worker <config.yaml> <worker_id> [port]");
        std::process::exit(1);
    }

    let config = ClusterConfig::from_file(&args[1])?;
    let worker_id = &args[2];
    let port: u16 = args.get(3).and_then(|p| p.parse().ok()).unwrap_or(50100);

    let shards = config.shards_for_executor(worker_id);
    if shards.is_empty() {
        info!("No initial shards for worker '{}' — waiting for LoadShard RPCs", worker_id);
    }

    let server_cfg = &config.server;
    let keepalive_interval = Duration::from_secs(server_cfg.keepalive_interval_secs);
    let keepalive_timeout = Duration::from_secs(server_cfg.keepalive_timeout_secs);

    info!("Starting Lance Worker '{}' with {} shards on port {}", worker_id, shards.len(), port);

    let ctx = SessionContext::default();
    let registry = Arc::new(
        LanceTableRegistry::with_full_config(ctx, &shards, &config.storage_options, &config.cache).await?
    );

    // Spawn periodic auto-compaction loop (Phase 13: prevents fragment accumulation).
    if config.compaction.enabled {
        let reg = registry.clone();
        let interval = Duration::from_secs(config.compaction.interval_secs);
        info!("Auto-compaction enabled (interval={}s)", config.compaction.interval_secs);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // skip immediate first tick
            loop {
                tick.tick().await;
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
    let worker_cap = config.server.max_response_bytes.saturating_mul(4).max(64 * 1024 * 1024);
    let service = WorkerService::with_max_response_bytes(registry, worker_cap);

    let addr = format!("0.0.0.0:{}", port).parse()?;
    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(keepalive_interval))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .concurrency_limit_per_connection(server_cfg.concurrency_limit);

    if config.security.tls_enabled() {
        let cert_path = config.security.tls_cert.as_ref()
            .ok_or("TLS enabled but tls_cert path missing")?;
        let key_path = config.security.tls_key.as_ref()
            .ok_or("TLS enabled but tls_key path missing")?;
        let cert = tokio::fs::read(cert_path).await?;
        let key = tokio::fs::read(key_path).await?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
        info!("TLS enabled");
        server = server.tls_config(tls_config)?;
    }

    server
        .add_service(LanceExecutorServiceServer::new(service))
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.ok();
            info!("Shutdown signal received — draining in-flight requests...");
        })
        .await?;

    info!("Worker stopped — all in-flight requests completed");
    Ok(())
}
