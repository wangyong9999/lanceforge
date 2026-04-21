// Licensed under the Apache License, Version 2.0.
// Lance Worker binary.
//
// Phase A (v2 architecture): thin wrapper around
// `lance_distributed_roles::pe_idx::run`. Same pattern as the
// coordinator bin — lifecycle lives in the roles crate so future
// `lance-monolith` (Phase B) and `lance-pe` / `lance-idx` (Phase E)
// bins reuse it.

use lance_distributed_common::config::ClusterConfig;
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
    let worker_id = args[2].clone();
    let port: u16 = args.get(3).and_then(|p| p.parse().ok()).unwrap_or(50100);

    lance_distributed_roles::pe_idx::run(config, worker_id, port).await
}

// ═══════════════════════════════════════════════════════════════════
// C3: OTLP exporter for the worker process. Mirrors the coord's B3
// shape so the two processes' spans line up under the same trace ID
// in Jaeger / Tempo without any per-attribute glue code. The
// `service.name` resource attr differs (`lance-worker`) so collector
// filters can still split by role.
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
