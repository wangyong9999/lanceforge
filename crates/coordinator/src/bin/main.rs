// Licensed under the Apache License, Version 2.0.
// Lance Coordinator binary.

use std::time::Duration;

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_coordinator::auth::ApiKeyInterceptor;
use lance_distributed_coordinator::ha::HaConfig;
use lance_distributed_coordinator::service::CoordinatorService;
use lance_distributed_proto::lance_scheduler_service_server::LanceSchedulerServiceServer;
use log::info;
use tokio::signal;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Structured logging: JSON in production (LOG_FORMAT=json), pretty otherwise.
    // The tracing-log bridge captures all log:: macros automatically.
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
    if args.len() < 2 {
        eprintln!("Usage: lance-coordinator <config.yaml> [port] [instance-id]");
        std::process::exit(1);
    }

    let config = ClusterConfig::from_file(&args[1])?;
    let port: u16 = args.get(2).and_then(|p| p.parse().ok()).unwrap_or(50050);
    let instance_id = args.get(3).cloned()
        .unwrap_or_else(|| format!("coordinator-{}", std::process::id()));

    HaConfig { instance_id: instance_id.clone(), total_instances: 1 }.log_status();

    let server_cfg = &config.server;
    let query_timeout = Duration::from_secs(server_cfg.query_timeout_secs);
    let keepalive_interval = Duration::from_secs(server_cfg.keepalive_interval_secs);
    let keepalive_timeout = Duration::from_secs(server_cfg.keepalive_timeout_secs);

    info!("Starting Lance Coordinator '{}' on port {}", instance_id, port);
    if config.security.auth_enabled() {
        info!("API key authentication enabled ({} keys configured)", config.security.api_keys.len());
    }

    let service = CoordinatorService::new(&config, query_timeout);
    let addr = format!("0.0.0.0:{}", port).parse()?;

    // Wrap service with API key interceptor if configured
    let auth = ApiKeyInterceptor::new(config.security.api_keys.clone());
    let svc = LanceSchedulerServiceServer::with_interceptor(service, move |req| auth.check(req));

    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(keepalive_interval))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .concurrency_limit_per_connection(server_cfg.concurrency_limit)
        .add_service(svc)
        .serve_with_shutdown(addr, async { signal::ctrl_c().await.ok(); })
        .await?;

    Ok(())
}
