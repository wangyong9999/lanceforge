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
    let registry = LanceTableRegistry::with_storage_options(ctx, &shards, &config.storage_options).await?;
    let service = WorkerService::new(Arc::new(registry));

    let addr = format!("0.0.0.0:{}", port).parse()?;
    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(keepalive_interval))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .concurrency_limit_per_connection(server_cfg.concurrency_limit);

    if config.security.tls_enabled() {
        let cert = tokio::fs::read(config.security.tls_cert.as_ref().unwrap()).await?;
        let key = tokio::fs::read(config.security.tls_key.as_ref().unwrap()).await?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
        info!("TLS enabled");
        server = server.tls_config(tls_config)?;
    }

    server
        .add_service(LanceExecutorServiceServer::new(service))
        .serve_with_shutdown(addr, async { signal::ctrl_c().await.ok(); })
        .await?;

    Ok(())
}
