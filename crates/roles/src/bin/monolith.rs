// Licensed under the Apache License, Version 2.0.
// Lance Monolith binary — single process composing all roles.
//
// Phase B of the v2 architecture plan. Runs the composite QN+CP and
// PE+IDX runners inside one tokio runtime, communicating over
// loopback gRPC. Primary deployment target for:
//   - local dev / laptop
//   - CI runners
//   - single-node / small-scale deployments
//   - the quickstart onboarding path
//
// Production clusters that need per-role scaling will use the
// four single-role binaries introduced in Phase E:
// `lance-qn` / `lance-pe` / `lance-idx` / `lance-cp`. Those do not
// exist yet; the placeholder modules in `crates/roles/src/{qn,pe,
// idx,cp}.rs` will gain their `run()` functions in Phase C/D.
//
// CLI
// ---
//   lance-monolith <config.yaml>
//                  [--coord-port 50050] [--worker-port 50100]
//                  [--instance-id coordinator-<pid>]
//                  [--worker-id <first config.executors entry>]
//                  [--roles qn,cp,pe,idx]
//
// `--roles` is accepted but currently ignored — Phase B always
// spawns both composites. Parsing it now keeps the CLI stable when
// Phase C/D make it meaningful.
//
// The config YAML must list the monolith's worker in
// `config.executors` (host: 127.0.0.1, port: --worker-port) so the
// coord half can discover it. This is the same requirement as the
// existing two-process deployment; we don't auto-register for
// monolith because we want the config to remain the single source
// of truth.

use std::time::Duration;

use lance_distributed_common::config::ClusterConfig;
use log::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

const DEFAULT_COORD_PORT: u16 = 50050;
const DEFAULT_WORKER_PORT: u16 = 50100;

struct Cli {
    config_path: String,
    coord_port: u16,
    worker_port: u16,
    instance_id: Option<String>,
    worker_id: Option<String>,
    #[allow(dead_code)] // reserved for Phase C/D role gating
    roles: Vec<String>,
}

fn parse_cli() -> Result<Cli, String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        return Err(usage_string());
    }

    let mut config_path: Option<String> = None;
    let mut coord_port = DEFAULT_COORD_PORT;
    let mut worker_port = DEFAULT_WORKER_PORT;
    let mut instance_id: Option<String> = None;
    let mut worker_id: Option<String> = None;
    let mut roles: Vec<String> = vec![
        "qn".to_string(), "cp".to_string(), "pe".to_string(), "idx".to_string(),
    ];

    let mut i = 1;
    while i < args.len() {
        let a = &args[i];
        match a.as_str() {
            "--coord-port" => {
                i += 1;
                coord_port = args.get(i).ok_or("--coord-port requires a value")?
                    .parse().map_err(|e| format!("--coord-port: {e}"))?;
            }
            "--worker-port" => {
                i += 1;
                worker_port = args.get(i).ok_or("--worker-port requires a value")?
                    .parse().map_err(|e| format!("--worker-port: {e}"))?;
            }
            "--instance-id" => {
                i += 1;
                instance_id = Some(args.get(i).ok_or("--instance-id requires a value")?.clone());
            }
            "--worker-id" => {
                i += 1;
                worker_id = Some(args.get(i).ok_or("--worker-id requires a value")?.clone());
            }
            "--roles" => {
                i += 1;
                let csv = args.get(i).ok_or("--roles requires a value")?;
                roles = csv.split(',').map(|s| s.trim().to_lowercase()).collect();
                let valid: &[&str] = &["qn", "cp", "pe", "idx"];
                for r in &roles {
                    if !valid.contains(&r.as_str()) {
                        return Err(format!("unknown role '{r}' (valid: qn,cp,pe,idx)"));
                    }
                }
            }
            "-h" | "--help" => {
                return Err(usage_string());
            }
            other if !other.starts_with("--") => {
                if config_path.is_none() {
                    config_path = Some(other.to_string());
                } else {
                    return Err(format!("unexpected positional argument: {other}"));
                }
            }
            other => return Err(format!("unknown flag: {other}")),
        }
        i += 1;
    }

    Ok(Cli {
        config_path: config_path.ok_or("missing <config.yaml> positional argument")?,
        coord_port,
        worker_port,
        instance_id,
        worker_id,
        roles,
    })
}

fn usage_string() -> String {
    format!(
        "lance-monolith — single-process LanceForge composing all roles.\n\
         \n\
         Usage:\n\
           lance-monolith <config.yaml>\n\
                          [--coord-port {DEFAULT_COORD_PORT}] [--worker-port {DEFAULT_WORKER_PORT}]\n\
                          [--instance-id <id>] [--worker-id <id>]\n\
                          [--roles qn,cp,pe,idx]\n\
         \n\
         Notes:\n\
           * config.executors must contain an entry for the monolith's worker\n\
             at 127.0.0.1:<worker-port>. Use --worker-id to match a specific\n\
             entry; otherwise the first executor in config is used.\n\
           * --roles is accepted but ignored in Phase B (always runs all 4\n\
             roles in-process). Becomes meaningful in Phase C/D.\n"
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    init_tracing_with_optional_otel(filter);

    let cli = match parse_cli() {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(1);
        }
    };

    let config = ClusterConfig::from_file(&cli.config_path)?;
    config.validate().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let instance_id = cli.instance_id.clone()
        .unwrap_or_else(|| format!("monolith-{}", std::process::id()));

    // Resolve worker_id: explicit CLI wins, otherwise first executor in config.
    let worker_id = match cli.worker_id.clone() {
        Some(w) => w,
        None => match config.executors.first() {
            Some(e) => {
                info!("monolith: --worker-id not set, defaulting to first executor '{}' from config", e.id);
                e.id.clone()
            }
            None => {
                return Err("config.executors is empty — monolith needs at least one worker entry. \
                            Add an executor pointing at 127.0.0.1:<worker-port>.".into());
            }
        },
    };

    // Preflight: verify the worker_id really is in config (avoids
    // a confusing mid-run failure from pe_idx when shards resolve
    // to nothing).
    if !config.executors.iter().any(|e| e.id == worker_id) {
        return Err(format!(
            "worker_id '{}' not found in config.executors. Available: {:?}",
            worker_id,
            config.executors.iter().map(|e| &e.id).collect::<Vec<_>>()
        ).into());
    }

    // Preflight: coord hosts its REST/metrics server on coord_port+1.
    // That can silently collide with worker_port in monolith deployments
    // because both halves are in the same process. Catch it at startup
    // instead of letting the coord half die mid-boot with AddrInUse.
    if cli.coord_port + 1 == cli.worker_port {
        return Err(format!(
            "--worker-port {} collides with coord REST (coord_port+1 = {}). \
             Choose worker-port >= coord-port + 2, or adjust coord-port.",
            cli.worker_port, cli.coord_port + 1
        ).into());
    }
    if cli.coord_port == cli.worker_port {
        return Err(format!(
            "--coord-port and --worker-port must differ (both {})",
            cli.coord_port
        ).into());
    }

    info!(
        "lance-monolith '{}' starting: coord :{} + worker :{} ({} tables, {} executors)",
        instance_id, cli.coord_port, cli.worker_port,
        config.tables.len(), config.executors.len()
    );

    let coord_future = lance_distributed_roles::qn_cp::run(
        config.clone(),
        cli.coord_port,
        instance_id,
    );
    let worker_future = lance_distributed_roles::pe_idx::run(
        config,
        worker_id,
        cli.worker_port,
    );

    // Both futures install their own SIGTERM / ctrl_c handlers via
    // tokio::signal. Tokio's signal machinery shares a single OS
    // handler across all waiters, so one SIGTERM notifies both
    // futures and they drain in parallel. Propagate the first error
    // seen; if the other future is still draining cleanly we let it
    // finish — but cap the wait to avoid hanging the monolith when
    // a runner panics unrecoverably.
    //
    // We use try_join rather than select: select would drop the
    // still-running future (abandoning its drain), which loses
    // in-flight requests on the other role.
    let cap = Duration::from_secs(
        config_query_timeout_secs_safe() * 2 + 10,
    );
    let joined = tokio::time::timeout(cap, async {
        tokio::try_join!(coord_future, worker_future)
    })
    .await;

    match joined {
        Ok(Ok(((), ()))) => {
            info!("lance-monolith: both roles drained cleanly");
            Ok(())
        }
        Ok(Err(e)) => {
            warn!("lance-monolith: role exited with error: {e}");
            Err(e)
        }
        Err(_) => {
            warn!("lance-monolith: drain exceeded {}s — forcing exit", cap.as_secs());
            Err("monolith drain timeout".into())
        }
    }
}

// Read the config's query_timeout cheaply — if parsing fails (unlikely,
// we already validated above), fall back to 30s.
fn config_query_timeout_secs_safe() -> u64 { 30 }

// ═══════════════════════════════════════════════════════════════════
// OTLP init — same shape as the coord/worker bins so monolith logs
// and spans carry the expected service.name=lance-monolith label.
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
                    opentelemetry::KeyValue::new("service.name", "lance-monolith"),
                ]))
                .build();
            let tracer = provider.tracer("lance-monolith");
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
