// Licensed under the Apache License, Version 2.0.
// Lance Control Plane binary.
//
// Phase E wrapper around `roles::cp::run`. A standalone CP process
// that serves the ClusterControl + NodeLifecycle gRPC protocols
// introduced in Phase C.1. Consumed by:
//   - Operators running a split deployment (CP separate from coord)
//   - QN-side `RoutingClient` (Phase C.4)
//
// The monolith binary (Phase B) embeds `cp::run` directly so this
// standalone bin and monolith-embedded CP share one implementation.

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_roles::common::new_shutdown;
use log::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

const DEFAULT_PORT: u16 = 50052;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    init_tracing(filter);

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: lance-cp <config.yaml> [port]");
        eprintln!("       port defaults to {DEFAULT_PORT}");
        std::process::exit(1);
    }

    let config = ClusterConfig::from_file(&args[1])?;
    config.validate().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    let port: u16 = args
        .get(2)
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let shutdown = new_shutdown();
    let shutdown_for_signal = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => info!("SIGINT received — draining CP..."),
                    _ = sigterm.recv() => info!("SIGTERM received — draining CP..."),
                }
            } else {
                let _ = tokio::signal::ctrl_c().await;
                info!("shutdown signal received — draining CP...");
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            info!("shutdown signal received — draining CP...");
        }
        shutdown_for_signal.notify_waiters();
    });

    info!("lance-cp starting on port {port}");
    lance_distributed_roles::cp::run(config, port, shutdown).await
}

fn init_tracing(filter: EnvFilter) {
    let fmt_layer = if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        fmt::layer().json().with_target(true).boxed()
    } else {
        fmt::layer().with_target(true).boxed()
    };
    tracing_subscriber::registry().with(filter).with(fmt_layer).init();
}
