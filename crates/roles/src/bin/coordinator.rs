// Licensed under the Apache License, Version 2.0.
// Lance Coordinator binary.
//
// Phase A (v2 architecture): this bin is now a thin wrapper around
// `lance_distributed_roles::qn_cp::run`. All coord lifecycle logic
// lives in the roles crate so the forthcoming `lance-monolith` bin
// (Phase B) and the standalone `lance-qn` / `lance-cp` bins
// (Phase E) can reuse it. Behaviour is identical to pre-v2.

use lance_distributed_common::config::ClusterConfig;
use log::warn;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install default rustls crypto provider (required for TLS).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // B3: optional OTLP tracing layer, same gating pattern as before.
    // Stays in the binary because tracing is process-global state.
    init_tracing_with_optional_otel(filter);

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: lance-coordinator <config.yaml> [port] [instance-id]");
        eprintln!("       lance-coordinator --auto-shard <table_name> <table_uri> <executor_config.yaml> [port]");
        std::process::exit(1);
    }

    let config = if args[1] == "--auto-shard" {
        // Auto-shard mode: discover fragments and generate config automatically.
        //
        // Allowed only under deployment_profile=dev. The generated
        // config is written to /tmp/lanceforge_autoshard_{pid}.yaml and
        // consumed by workers on the same machine — neither survives a
        // pod replacement. ROADMAP_0.2 §B1 Gap B tracks the full
        // MetaStore-backed bootstrap (B1.2.2b, deferred post-alpha).
        if args.len() < 5 {
            eprintln!("Usage: lance-coordinator --auto-shard <table_name> <table_uri> <executor_config.yaml> [port]");
            std::process::exit(1);
        }
        let table_name = &args[2];
        let table_uri = &args[3];
        let executor_config = ClusterConfig::from_file(&args[4])?;

        use lance_distributed_common::config::DeploymentProfile;
        if executor_config.deployment_profile != DeploymentProfile::Dev {
            eprintln!(
                "--auto-shard is only permitted under deployment_profile=dev.\n\
                 Current profile: {:?}. The generated config is written to /tmp and \
                 consumed by workers on the same machine — it does not survive pod \
                 replacement. Use an explicit executor config (or a SaaS bootstrap \
                 tool) instead.",
                executor_config.deployment_profile
            );
            std::process::exit(2);
        }

        log::info!("Auto-shard mode: discovering fragments in {}", table_uri);
        let auto_config = lance_distributed_common::auto_shard::generate_auto_config(
            table_name,
            table_uri,
            &executor_config.executors,
            &executor_config.storage_options,
        ).await.map_err(|e| -> Box<dyn std::error::Error> { e })?;

        let auto_config_path = format!("/tmp/lanceforge_autoshard_{}.yaml", std::process::id());
        let yaml = serde_yaml::to_string(&auto_config)?;
        tokio::fs::write(&auto_config_path, &yaml).await?;
        warn!(
            "Auto-shard config written to {} — dev-only ephemeral file, will not \
             survive pod/process restart. Start workers with: <same_config_path>",
            auto_config_path
        );

        auto_config
    } else {
        ClusterConfig::from_file(&args[1])?
    };
    config.validate().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let port: u16 = args
        .iter()
        .skip(2)
        .find_map(|a| a.parse::<u16>().ok())
        .unwrap_or(50050);
    let instance_id = args
        .iter()
        .skip(2)
        .find(|a| {
            a.parse::<u16>().is_err()
                && !a.starts_with("--")
                && !a.ends_with(".yaml")
                && !a.ends_with(".lance")
        })
        .cloned()
        .unwrap_or_else(|| format!("coordinator-{}", std::process::id()));

    lance_distributed_roles::qn_cp::run(config, port, instance_id).await
}

// ═══════════════════════════════════════════════════════════════════
// B3: OpenTelemetry OTLP exporter (feature-gated). Kept here because
// tracing is process-global state — binaries init it once, role
// runners only use `tracing::` macros that land on whatever layer
// the bin installed. The monolith binary (Phase B) will have an
// identical init; duplication is intentional to keep the roles
// crate free of tracing-subscriber side effects.
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
                    opentelemetry::KeyValue::new("service.name", "lance-coordinator"),
                ]))
                .build();
            let tracer = provider.tracer("lance-coordinator");
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
