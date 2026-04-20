// Licensed under the Apache License, Version 2.0.
// Lance Coordinator binary.

use std::time::Duration;

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_coordinator::auth::ApiKeyInterceptor;
use lance_distributed_coordinator::ha::HaConfig;
use lance_distributed_coordinator::service::CoordinatorService;
use lance_distributed_proto::lance_scheduler_service_server::LanceSchedulerServiceServer;
use log::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install default rustls crypto provider (required for TLS)
    let _ = rustls::crypto::ring::default_provider().install_default();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // B3: optional OTLP tracing layer. Feature-gated (`--features otel`).
    // When the binary is built with OTEL and the operator sets
    // `OTEL_EXPORTER_OTLP_ENDPOINT`, spans produced by the `tracing`
    // macros in the codebase are exported via gRPC to an OTLP
    // collector (Jaeger, Tempo, otel-collector, Datadog agent, …).
    // When the feature is off or the env var is missing, the registry
    // is identical to the default stdout stack — no behavior change.
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
        // Allowed only under deployment_profile=dev. The generated config is
        // written to /tmp/lanceforge_autoshard_{pid}.yaml and consumed by
        // workers on the same machine — neither survives a pod replacement,
        // which is fine for laptop/dev use but fatal for SaaS / self-hosted
        // production. ROADMAP_0.2 §B1 Gap B tracks the full MetaStore-backed
        // auto-shard bootstrap (B1.2.2b, deferred post-alpha).
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

        info!("Auto-shard mode: discovering fragments in {}", table_uri);
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
        // Standard mode: load config from YAML
        ClusterConfig::from_file(&args[1])?
    };
    config.validate().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

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
    // G6: build per-key QPS limits in the same pass so they stay in sync
    // with the role map.
    let mut rate_map: StdMap<String, u32> = StdMap::new();
    // G5: per-key namespace bindings. Collected in the same pass so the
    // auth interceptor has one consistent view of every key's role, quota,
    // and namespace when it hot-reloads.
    let mut ns_map: StdMap<String, String> = StdMap::new();
    for e in &config.security.api_keys_rbac {
        if let Some(r) = Role::parse(&e.role) {
            role_map.insert(e.key.clone(), r);
            if e.qps_limit > 0 {
                rate_map.insert(e.key.clone(), e.qps_limit);
            }
            if let Some(ns) = e.namespace.as_ref().filter(|s| !s.is_empty()) {
                ns_map.insert(e.key.clone(), ns.clone());
            }
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
    if !rate_map.is_empty() {
        info!("auth: enforcing per-key QPS limits on {} key(s)", rate_map.len());
        auth_arc.set_rate_limits(rate_map);
    }
    if !ns_map.is_empty() {
        info!("auth: binding {} key(s) to namespaces (G5)", ns_map.len());
        auth_arc.set_namespaces(ns_map);
    }
    service = service.with_auth(auth_arc.clone());

    // Grab the background-shutdown notify handle before any subsystem that
    // needs it is wired up. The audit sink (G7) and the metastore / meta
    // refresh tasks below all hang off this same signal.
    let bg_shutdown = service.bg_shutdown();

    // G7: persistent audit sink. Spawned before the service moves into
    // the gRPC server so the sender is in place when the first request
    // arrives. Skip entirely when the operator hasn't configured a path
    // — keeps 0.1 stdout-only behaviour for dev clusters.
    //
    // F1 hardening: reject OBS URIs at config time instead of silently
    // dropping records. Operators who want OBS audit today must ship
    // via an external log collector (Vector / Fluent Bit) pointed at a
    // local file. 0.3 will land real OBS-append support (ROADMAP R3).
    let metrics = service.metrics();
    if let Some(ref audit_path) = config.security.audit_log_path {
        if let Err(e) = lance_distributed_common::audit::validate_audit_path(audit_path) {
            log::error!("invalid audit_log_path: {e}");
            std::process::exit(2);
        }
        let audit_sink = lance_distributed_common::audit::AuditSink::spawn(
            audit_path.clone(),
            bg_shutdown.clone(),
            metrics.clone(),
        );
        service = service.with_audit_sink(audit_sink);
    }

    // B2.2: install per-worker role preferences so the scatter-gather and
    // write paths can split read vs write traffic across replicas. No-op
    // when every executor uses the default `Either` role.
    service.install_executor_roles(&config.executors).await;
    // Keep handles for graceful shutdown
    let shutdown_handle = service.pool_shutdown_handle();
    let addr = format!("0.0.0.0:{}", port).parse()?;

    // B1.2.3: MetaStore-backed key registry.
    //
    // If a MetaStore is configured:
    //   (a) Bootstrap — on first run, copy config-provided keys into the
    //       store so multi-coordinator deployments don't need synchronized
    //       YAML. Idempotent: subsequent calls see a populated prefix.
    //   (b) Hot reload — every 60s pull the registry back out and swap it
    //       into the interceptor atomically. New keys added via direct
    //       MetaStore write (or a future admin RPC) become live within the
    //       reload window; revoked keys stop authenticating within the same
    //       window. Request handling keeps running against the last
    //       successful snapshot if the MetaStore is transiently unreachable.
    //
    // No MetaStore → keys stay config-frozen (0.1.x behaviour).
    if let Some(store) = service.meta_store() {
        match lance_distributed_coordinator::auth::bootstrap_metastore_if_empty(
            store.as_ref(), &role_map
        ).await {
            Ok(n) if n > 0 => info!("auth: bootstrapped {} key(s) into MetaStore", n),
            Ok(_) => info!("auth: MetaStore auth/keys/ already populated; skipping bootstrap"),
            Err(e) => warn!("auth: MetaStore bootstrap failed: {} (continuing with config keys)", e),
        }

        let reload_store = store.clone();
        let reload_auth = auth_arc.clone();
        let reload_stop = bg_shutdown.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(60);
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // skip immediate tick; bootstrap already seeded
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = reload_stop.notified() => {
                        info!("auth reload task stopping (shutdown)");
                        return;
                    }
                }
                match lance_distributed_coordinator::auth::load_from_metastore(reload_store.as_ref()).await {
                    Ok(keys) => {
                        let n = keys.len();
                        reload_auth.reload(keys);
                        log::debug!("auth: reloaded {} key(s) from MetaStore", n);
                    }
                    Err(e) => log::warn!(
                        "auth: reload from MetaStore failed, keeping last snapshot: {}", e
                    ),
                }
            }
        });
        info!("auth: hot-reload task running (60s interval)");
    } else if !role_map.is_empty() {
        warn!(
            "auth: MetaStore not configured — api_keys are frozen at startup. \
             Set `metadata_path` to enable runtime key rotation."
        );
    }

    // Start orphan GC loop (opt-in, disabled by default — destructive).
    if config.orphan_gc.enabled {
        info!("Orphan GC enabled (interval={}s, min_age={}s)",
            config.orphan_gc.interval_secs, config.orphan_gc.min_age_secs);
        let gc_handle = service.orphan_gc_handle();
        let gc_cfg = config.orphan_gc.clone();
        let gc_stop = bg_shutdown.clone();
        tokio::spawn(async move {
            lance_distributed_coordinator::service::orphan_gc_loop(gc_handle, gc_cfg, gc_stop).await;
        });
    }

    // H3: spawn the write-dedup cache reaper before the gRPC server takes
    // ownership of `service`. The reaper scans the in-memory map every
    // 60s and drops entries older than the 5-minute TTL.
    service.spawn_dedup_reaper();

    // Start REST/metrics HTTP server on port+1. Shares bg_shutdown so a
    // SIGTERM to the coordinator drains REST in lockstep with gRPC instead
    // of leaving a stale `/healthz` answering 200 OK after the rest of
    // the stack has already exited (H15 hardening).
    let rest_port = port + 1;
    let rest_shutdown = bg_shutdown.clone();
    tokio::spawn(async move {
        lance_distributed_coordinator::rest::start_rest_server(
            metrics, rest_port, port, rest_shutdown,
        ).await;
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

    // TLS: delegate cert/key loading to the shared helper so both
    // coordinator and worker report errors with the same wording and
    // reject empty files before tonic gets a chance to fail obscurely
    // during the first handshake (H17).
    if config.security.tls_enabled() {
        let cert_path = config.security.tls_cert.as_ref()
            .ok_or("TLS enabled but tls_cert path missing")?;
        let key_path = config.security.tls_key.as_ref()
            .ok_or("TLS enabled but tls_key path missing")?;
        let tls_config = lance_distributed_common::tls::load_server_tls_config(cert_path, key_path).await?;
        info!("TLS enabled");
        server = server.tls_config(tls_config)?;
    }

    // Graceful shutdown (H21, fixed H25). Listen for both SIGINT
    // (Ctrl+C, local dev) and SIGTERM (K8s pod termination). Fire
    // `bg_shutdown` as soon as the signal arrives so background tasks
    // (REST, auth reload, orphan GC, health-check loop) begin draining
    // *in parallel* with the gRPC in-flight drain — otherwise they hog
    // the tokio runtime while the gRPC server is waiting on them.
    //
    // A separate seatbelt task caps the drain window at
    // `2 × query_timeout + 5s`: once `bg_shutdown` fires the seatbelt
    // waits that long and then `std::process::exit(1)`. This keeps a
    // stuck in-flight RPC from trapping the pod longer than K8s's
    // `terminationGracePeriodSeconds` budget. The per-query
    // `query_timeout` inside scatter_gather ensures no single RPC takes
    // longer than that anyway; this is belt-and-braces.
    //
    // **Why the seatbelt is a separate task, not
    // `tokio::time::timeout(max_shutdown, serve_fut)`**:
    // wrapping `serve_fut` in a timeout makes the *entire* server
    // lifetime budget-bound, not just the drain — without a shutdown
    // signal the coord would die at `max_shutdown` seconds flat. H21
    // originally shipped that shape and it silently limited every
    // coordinator pod to 65 s of uptime; the 60 min read-only soak in
    // G4 re-surfaced it. The fix is to bind the budget only to the
    // drain window, which starts when `bg_shutdown` is notified.
    let bg_shutdown_for_signal = bg_shutdown.clone();
    let bg_shutdown_for_seatbelt = bg_shutdown.clone();
    let query_timeout = Duration::from_secs(server_cfg.query_timeout_secs);
    let max_shutdown = query_timeout.saturating_mul(2).saturating_add(Duration::from_secs(5));
    tokio::spawn(async move {
        bg_shutdown_for_seatbelt.notified().await;
        tokio::time::sleep(max_shutdown).await;
        warn!(
            "Graceful shutdown exceeded {}s budget; forcing exit. \
             In-flight queries may have been abandoned — check metrics / logs.",
            max_shutdown.as_secs()
        );
        std::process::exit(1);
    });

    let shutdown_signal = async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => info!("SIGINT received — draining in-flight requests..."),
                _ = sigterm.recv() => info!("SIGTERM received — draining in-flight requests..."),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutdown signal received — draining in-flight requests...");
        }
        // Fire bg_shutdown the moment we know we're shutting down so
        // background tasks start exiting in parallel with the gRPC drain
        // AND the seatbelt starts its force-exit countdown.
        bg_shutdown_for_signal.notify_waiters();
    };

    server.add_service(svc).serve_with_shutdown(addr, shutdown_signal).await?;

    // Stop remaining handles. bg_shutdown was fired above on signal, but
    // re-notify to cover the rare case where serve completed without ever
    // firing the signal path (e.g. test harness using a mock future).
    shutdown_handle.shutdown();
    bg_shutdown.notify_waiters();
    info!("Server stopped — all background tasks signalled");
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════
// B3: OpenTelemetry OTLP exporter (feature-gated).
//
// The default build keeps zero OTEL deps. `cargo build --features otel`
// compiles in `opentelemetry-otlp` + `tracing-opentelemetry` and
// wires a second tracing layer that ships spans over gRPC to the
// OTLP collector at `OTEL_EXPORTER_OTLP_ENDPOINT`.
//
// Spans come from the existing `#[tracing::instrument]` / `info!`
// call sites in the codebase — no new code per RPC. Coord audit
// lines already carry `trace_id=<hex>` (F9 + G10), so the same
// trace threads through stdout logs *and* OTLP spans once enabled.
// ═══════════════════════════════════════════════════════════════════

#[cfg(feature = "otel")]
fn init_tracing_with_optional_otel(filter: EnvFilter) {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;

    // Build an OpenTelemetry layer only when the operator opted in via
    // the OTLP spec env var. No env var ⇒ default stdout stack.
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
