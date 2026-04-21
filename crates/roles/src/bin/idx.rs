// Licensed under the Apache License, Version 2.0.
// Lance Indexer binary.
//
// Phase E wrapper around `roles::idx::run`. Standalone compaction
// process for split-deployment operators who want real CPU isolation
// from PE (query) workers.
//
// To split: set `compaction.enabled: false` on worker(s) so
// `pe_idx::run` skips its inline compaction loop, and run this bin
// against the same cluster config.

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_roles::common::new_shutdown;
use log::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    init_tracing(filter);

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: lance-idx <config.yaml>");
        std::process::exit(1);
    }

    let config = ClusterConfig::from_file(&args[1])?;
    config.validate().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let shutdown = new_shutdown();
    let shutdown_for_signal = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => info!("SIGINT received — stopping idx..."),
                    _ = sigterm.recv() => info!("SIGTERM received — stopping idx..."),
                }
            } else {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        shutdown_for_signal.notify_waiters();
    });

    info!("lance-idx starting");
    lance_distributed_roles::idx::run(config, shutdown).await
}

fn init_tracing(filter: EnvFilter) {
    let fmt_layer = if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        fmt::layer().json().with_target(true).boxed()
    } else {
        fmt::layer().with_target(true).boxed()
    };
    tracing_subscriber::registry().with(filter).with(fmt_layer).init();
}
