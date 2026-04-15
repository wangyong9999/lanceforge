// Licensed under the Apache License, Version 2.0.
// Lance Coordinator binary.

use std::time::Duration;

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_coordinator::auth::ApiKeyInterceptor;
use lance_distributed_coordinator::ha::HaConfig;
use lance_distributed_coordinator::service::CoordinatorService;
use lance_distributed_proto::lance_scheduler_service_server::LanceSchedulerServiceServer;
use log::{info, warn};
use tokio::signal;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install default rustls crypto provider (required for TLS)
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
    if args.len() < 2 {
        eprintln!("Usage: lance-coordinator <config.yaml> [port] [instance-id]");
        eprintln!("       lance-coordinator --auto-shard <table_name> <table_uri> <executor_config.yaml> [port]");
        std::process::exit(1);
    }

    let config = if args[1] == "--auto-shard" {
        // Auto-shard mode: discover fragments and generate config automatically
        if args.len() < 5 {
            eprintln!("Usage: lance-coordinator --auto-shard <table_name> <table_uri> <executor_config.yaml> [port]");
            std::process::exit(1);
        }
        let table_name = &args[2];
        let table_uri = &args[3];
        let executor_config = ClusterConfig::from_file(&args[4])?;

        info!("Auto-shard mode: discovering fragments in {}", table_uri);
        let auto_config = lance_distributed_common::auto_shard::generate_auto_config(
            table_name,
            table_uri,
            &executor_config.executors,
            &executor_config.storage_options,
        ).await.map_err(|e| -> Box<dyn std::error::Error> { e })?;

        // Write generated config for workers to use
        let auto_config_path = format!("/tmp/lanceforge_autoshard_{}.yaml", std::process::id());
        let yaml = serde_yaml::to_string(&auto_config)?;
        tokio::fs::write(&auto_config_path, &yaml).await?;
        info!("Auto-shard config written to {} — start workers with this config", auto_config_path);

        auto_config
    } else {
        // Standard mode: load config from YAML
        ClusterConfig::from_file(&args[1])?
    };

    let port: u16 = args.iter()
        .skip(2)
        .find_map(|a| a.parse::<u16>().ok())
        .unwrap_or(50050);
    let instance_id = args.iter()
        .skip(2)
        .find(|a| a.parse::<u16>().is_err() && !a.starts_with("--") && !a.ends_with(".yaml") && !a.ends_with(".lance"))
        .cloned()
        .unwrap_or_else(|| format!("coordinator-{}", std::process::id()));

    HaConfig { instance_id: instance_id.clone(), total_instances: 1 }.log_status();

    let server_cfg = &config.server;
    let query_timeout = Duration::from_secs(server_cfg.query_timeout_secs);
    let keepalive_interval = Duration::from_secs(server_cfg.keepalive_interval_secs);
    let keepalive_timeout = Duration::from_secs(server_cfg.keepalive_timeout_secs);

    info!("Starting Lance Coordinator '{}' on port {} ({} tables, {} executors)",
        instance_id, port, config.tables.len(), config.executors.len());
    if config.security.auth_enabled() {
        info!("API key authentication enabled ({} keys configured)", config.security.api_keys.len());
    }

    let mut service = if let Some(ref metadata_path) = config.metadata_path {
        CoordinatorService::with_meta_state(&config, query_timeout, metadata_path).await?
    } else {
        CoordinatorService::new(&config, query_timeout)
    };

    // Build RBAC interceptor: prefer `api_keys_rbac`, fall back to legacy `api_keys`.
    use lance_distributed_coordinator::auth::Role;
    use std::collections::HashMap as StdMap;
    let mut role_map: StdMap<String, Role> = StdMap::new();
    for e in &config.security.api_keys_rbac {
        if let Some(r) = Role::parse(&e.role) {
            role_map.insert(e.key.clone(), r);
        } else {
            warn!("Ignoring api_keys_rbac entry with invalid role: {}", e.role);
        }
    }
    for k in &config.security.api_keys {
        role_map.entry(k.clone()).or_insert(Role::Admin);
    }
    let auth_arc = std::sync::Arc::new(
        if role_map.is_empty() {
            ApiKeyInterceptor::new(vec![])
        } else {
            ApiKeyInterceptor::with_roles(role_map.clone())
        }
    );
    service = service.with_auth(auth_arc.clone());
    let metrics = service.metrics();
    // Keep a handle for graceful shutdown
    let shutdown_handle = service.pool_shutdown_handle();
    let addr = format!("0.0.0.0:{}", port).parse()?;

    // Start orphan GC loop (opt-in, disabled by default — destructive).
    if config.orphan_gc.enabled {
        info!("Orphan GC enabled (interval={}s, min_age={}s)",
            config.orphan_gc.interval_secs, config.orphan_gc.min_age_secs);
        let gc_handle = service.orphan_gc_handle();
        let gc_cfg = config.orphan_gc.clone();
        tokio::spawn(async move {
            lance_distributed_coordinator::service::orphan_gc_loop(gc_handle, gc_cfg).await;
        });
    }

    // Start REST/metrics HTTP server on port+1
    let rest_port = port + 1;
    tokio::spawn(async move {
        lance_distributed_coordinator::rest::start_rest_server(metrics, rest_port).await;
    });

    // Raise gRPC message limits: default tonic caps are 4 MiB, which is hit
    // by even moderate CreateTable/AddRows payloads (e.g., 10K × 128d = 5 MB).
    // Cap at max_response_bytes + 16 MiB headroom for metadata/overhead.
    // NOTE: authentication is now enforced PER METHOD inside the service
    // (see CoordinatorService::check_perm). The outer tonic interceptor was
    // redundant and prevented setting message-size limits, so it was removed.
    let msg_limit = config.server.max_response_bytes.saturating_add(16 * 1024 * 1024);
    let _ = auth_arc; // kept in scope for Arc lifetime clarity; actually held by service
    let svc = LanceSchedulerServiceServer::new(service)
        .max_decoding_message_size(msg_limit)
        .max_encoding_message_size(msg_limit);

    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(keepalive_interval))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .concurrency_limit_per_connection(server_cfg.concurrency_limit);

    // TLS: load server certificate and key if configured
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
        .add_service(svc)
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.ok();
            info!("Shutdown signal received — draining in-flight requests...");
        })
        .await?;

    // Stop background tasks (health check loop)
    shutdown_handle.shutdown();
    info!("Server stopped — all in-flight requests completed");
    Ok(())
}
