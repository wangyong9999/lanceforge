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

    // C3: optional OTLP tracing layer, same feature gate and env var
    // convention as the coordinator (B3). Same `OTEL_EXPORTER_OTLP_ENDPOINT`
    // env picks up the collector URL; spans from this worker show up
    // alongside the coord's under the matching trace_id (B1 re-emits
    // `traceparent` on outbound coord→worker RPCs, so trace context
    // threads through).
    init_tracing_with_optional_otel(filter);

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: lance-worker <config.yaml> <worker_id> [port]");
        std::process::exit(1);
    }

    let config = ClusterConfig::from_file(&args[1])?;
    config.validate().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
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
    let compact_shutdown = Arc::new(tokio::sync::Notify::new());
    if config.compaction.enabled {
        let reg = registry.clone();
        let interval = Duration::from_secs(config.compaction.interval_secs);
        let stop = compact_shutdown.clone();
        info!("Auto-compaction enabled (interval={}s)", config.compaction.interval_secs);
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
    let worker_cap = config.server.max_response_bytes.saturating_mul(4).max(64 * 1024 * 1024);
    let registry_shutdown = registry.clone(); // keep handle for graceful shutdown
    let service = WorkerService::with_max_response_bytes(registry, worker_cap);

    let addr = format!("0.0.0.0:{}", port).parse()?;
    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(keepalive_interval))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .concurrency_limit_per_connection(server_cfg.concurrency_limit);

    // TLS: delegate cert/key loading to the shared helper (H17) so
    // coordinator and worker share the same error wording and empty-
    // file rejection.
    if config.security.tls_enabled() {
        let cert_path = config.security.tls_cert.as_ref()
            .ok_or("TLS enabled but tls_cert path missing")?;
        let key_path = config.security.tls_key.as_ref()
            .ok_or("TLS enabled but tls_key path missing")?;
        let tls_config = lance_distributed_common::tls::load_server_tls_config(cert_path, key_path).await?;
        info!("TLS enabled");
        server = server.tls_config(tls_config)?;
    }

    server
        .add_service(
            LanceExecutorServiceServer::new(service)
                .max_decoding_message_size(worker_cap.saturating_add(16 * 1024 * 1024))
                .max_encoding_message_size(worker_cap.saturating_add(16 * 1024 * 1024))
        )
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.ok();
            info!("Shutdown signal received — draining in-flight requests...");
        })
        .await?;

    // Stop all background tasks (version poller, compaction loop)
    registry_shutdown.shutdown();
    compact_shutdown.notify_waiters();
    info!("Worker stopped — all background tasks signalled, in-flight requests completed");
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════
// C3: OTLP exporter for the worker process. Mirrors the coord's B3
// shape so the two processes' spans line up under the same trace
// ID in Jaeger / Tempo without any per-attribute glue code. The
// `service.name` resource attr differs (`lance-worker`) so collector-
// side filters can still split by role.
// ═══════════════════════════════════════════════════════════════════

#[cfg(feature = "otel")]
fn init_tracing_with_optional_otel(filter: EnvFilter) {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;

    let otel_layer = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .and_then(|endpoint| {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .map_err(|e| eprintln!("otel: exporter build failed: {e}"))
                .ok()?;
            let provider = opentelemetry_sdk::trace::TracerProvider::builder()
                .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
                .with_resource(opentelemetry_sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", "lance-worker"),
                ]))
                .build();
            let tracer = provider.tracer("lance-worker");
            opentelemetry::global::set_tracer_provider(provider);
            Some(tracing_opentelemetry::layer().with_tracer(tracer))
        });

    let fmt_layer = if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        fmt::layer().json().with_target(true).boxed()
    } else {
        fmt::layer().with_target(true).boxed()
    };
    let reg = tracing_subscriber::registry().with(filter).with(fmt_layer);
    match otel_layer {
        Some(otel) => reg.with(otel).init(),
        None => reg.init(),
    }
}

#[cfg(not(feature = "otel"))]
fn init_tracing_with_optional_otel(filter: EnvFilter) {
    let fmt_layer = if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        fmt::layer().json().with_target(true).boxed()
    } else {
        fmt::layer().with_target(true).boxed()
    };
    tracing_subscriber::registry().with(filter).with(fmt_layer).init();
}
