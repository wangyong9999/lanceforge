// Composite QN + CP runner.
//
// Phase A: this is the coord binary's body, extracted behind a
// `pub async fn run(...)`. Two goals:
//
//   1. The current `lance-coordinator` binary (and any future
//      `lance-monolith`) can invoke the coord lifecycle without
//      duplicating 300+ lines of main().
//   2. The QN-shaped and CP-shaped responsibilities inside this
//      function are annotated so Phase C can extract the CP half
//      into its own standalone runner without a code search.
//
// Role breakdown within this function (Phase A — in-process only):
//
//   QN responsibilities:
//     - tonic Scheduler gRPC server (request entry point)
//     - Scatter-gather + merge (via CoordinatorService)
//
//   CP responsibilities:
//     - ApiKeyInterceptor construction + RBAC/QPS/namespace maps
//     - Audit sink spawn
//     - MetaStore-backed key registry bootstrap + 60s hot-reload
//     - Orphan GC background loop
//     - Write-dedup cache reaper (really coord-internal but lives CP-side)
//     - REST + /metrics HTTP server
//     - ConnectionPool health check loop
//
// The function preserves every invariant the original main() relied
// on, including the H21/H25 seatbelt shape (drain window bounded
// separately from server lifetime).

use std::sync::Arc;
use std::time::Duration;

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_coordinator::auth::{ApiKeyInterceptor, Role};
use lance_distributed_coordinator::ha::HaConfig;
use lance_distributed_coordinator::service::CoordinatorService;
use lance_distributed_proto::lance_scheduler_service_server::LanceSchedulerServiceServer;
use log::{info, warn};

/// Run the composite QN + CP lifecycle inside the current tokio
/// runtime. Returns when the server has finished draining after
/// SIGTERM / ctrl+c (or the shutdown seatbelt fires).
///
/// Called by `lance-coordinator` (Phase A) and `lance-monolith` (Phase B).
pub async fn run(
    config: ClusterConfig,
    port: u16,
    instance_id: String,
) -> Result<(), Box<dyn std::error::Error>> {
    HaConfig { instance_id: instance_id.clone(), total_instances: 1 }.log_status();

    let server_cfg = &config.server;
    let query_timeout = Duration::from_secs(server_cfg.query_timeout_secs);
    let keepalive_interval = Duration::from_secs(server_cfg.keepalive_interval_secs);
    let keepalive_timeout = Duration::from_secs(server_cfg.keepalive_timeout_secs);

    info!(
        "Starting Lance Coordinator '{}' on port {} ({} tables, {} executors)",
        instance_id, port, config.tables.len(), config.executors.len()
    );
    if config.security.auth_enabled() {
        info!(
            "API key authentication enabled ({} keys configured)",
            config.security.api_keys.len()
        );
    }

    let mut service = if let Some(ref metadata_path) = config.metadata_path {
        CoordinatorService::with_meta_state(&config, query_timeout, metadata_path).await?
    } else {
        CoordinatorService::new(&config, query_timeout)
    };
    // #5.2 DDL lease holder id. Identifies this coord in lease
    // records so peers see a readable "held by <instance>" message
    // instead of a pid.
    service = service.with_instance_id(instance_id.clone());

    // ── CP: build auth registry ────────────────────────────────────
    // Build RBAC interceptor: prefer `api_keys_rbac`, fall back to legacy `api_keys`.
    use std::collections::HashMap as StdMap;
    let mut role_map: StdMap<String, Role> = StdMap::new();
    // G6: per-key QPS limits in the same pass so they stay in sync with role map.
    let mut rate_map: StdMap<String, u32> = StdMap::new();
    // G5: per-key namespace bindings, also in the same pass so auth
    // interceptor has one consistent view on every hot-reload.
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
    let auth_arc = Arc::new(if role_map.is_empty() {
        ApiKeyInterceptor::new(vec![])
    } else {
        ApiKeyInterceptor::with_roles(role_map.clone())
    });
    if !rate_map.is_empty() {
        info!("auth: enforcing per-key QPS limits on {} key(s)", rate_map.len());
        auth_arc.set_rate_limits(rate_map);
    }
    if !ns_map.is_empty() {
        info!("auth: binding {} key(s) to namespaces (G5)", ns_map.len());
        auth_arc.set_namespaces(ns_map);
    }
    service = service.with_auth(auth_arc.clone());

    // Grab the shared background-shutdown notify before any CP
    // subsystem is wired up. The audit sink, MetaStore hot-reload,
    // orphan GC, and the H25 seatbelt all hang off this signal.
    let bg_shutdown = service.bg_shutdown();

    // ── CP: audit sink ─────────────────────────────────────────────
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

    // ── CP: role preferences (B2.2) ────────────────────────────────
    service.install_executor_roles(&config.executors).await;
    let shutdown_handle = service.pool_shutdown_handle();
    let addr = format!("0.0.0.0:{}", port).parse()?;

    // ── CP: MetaStore-backed key registry hot-reload (B1.2.3) ──────
    if let Some(store) = service.meta_store() {
        match lance_distributed_coordinator::auth::bootstrap_metastore_if_empty(
            store.as_ref(),
            &role_map,
        )
        .await
        {
            Ok(n) if n > 0 => info!("auth: bootstrapped {} key(s) into MetaStore", n),
            Ok(_) => info!(
                "auth: MetaStore auth/keys/ already populated; skipping bootstrap"
            ),
            Err(e) => warn!(
                "auth: MetaStore bootstrap failed: {} (continuing with config keys)",
                e
            ),
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
                match lance_distributed_coordinator::auth::load_from_metastore(
                    reload_store.as_ref(),
                )
                .await
                {
                    Ok(keys) => {
                        let n = keys.len();
                        reload_auth.reload(keys);
                        log::debug!("auth: reloaded {} key(s) from MetaStore", n);
                    }
                    Err(e) => log::warn!(
                        "auth: reload from MetaStore failed, keeping last snapshot: {}",
                        e
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

    // ── CP: orphan GC (opt-in) ─────────────────────────────────────
    if config.orphan_gc.enabled {
        info!(
            "Orphan GC enabled (interval={}s, min_age={}s)",
            config.orphan_gc.interval_secs, config.orphan_gc.min_age_secs
        );
        let gc_handle = service.orphan_gc_handle();
        let gc_cfg = config.orphan_gc.clone();
        let gc_stop = bg_shutdown.clone();
        tokio::spawn(async move {
            lance_distributed_coordinator::service::orphan_gc_loop(
                gc_handle, gc_cfg, gc_stop,
            )
            .await;
        });
    }

    // ── CP-adjacent: write-dedup cache reaper (H3) ─────────────────
    service.spawn_dedup_reaper();

    // ── CP: REST + /metrics HTTP server ────────────────────────────
    let rest_port = port + 1;
    let rest_shutdown = bg_shutdown.clone();
    tokio::spawn(async move {
        lance_distributed_coordinator::rest::start_rest_server(
            metrics,
            rest_port,
            port,
            rest_shutdown,
        )
        .await;
    });

    // ── QN: gRPC Scheduler service ─────────────────────────────────
    let msg_limit = config.server.max_response_bytes.saturating_add(16 * 1024 * 1024);
    let _ = auth_arc; // kept in scope for Arc lifetime clarity; actually held by service
    let svc = LanceSchedulerServiceServer::new(service)
        .max_decoding_message_size(msg_limit)
        .max_encoding_message_size(msg_limit);

    let mut server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(keepalive_interval))
        .http2_keepalive_timeout(Some(keepalive_timeout))
        .concurrency_limit_per_connection(server_cfg.concurrency_limit);

    if config.security.tls_enabled() {
        let cert_path = config
            .security
            .tls_cert
            .as_ref()
            .ok_or("TLS enabled but tls_cert path missing")?;
        let key_path = config
            .security
            .tls_key
            .as_ref()
            .ok_or("TLS enabled but tls_key path missing")?;
        let tls_config =
            lance_distributed_common::tls::load_server_tls_config(cert_path, key_path).await?;
        info!("TLS enabled");
        server = server.tls_config(tls_config)?;
    }

    // ── Graceful shutdown + H25 seatbelt ───────────────────────────
    // Fire `bg_shutdown` on SIGTERM so all CP background tasks drain
    // in parallel with the gRPC drain. A separate task caps the
    // overall drain window at `2 × query_timeout + 5s`.
    //
    // The seatbelt is a **separate** task, not a timeout wrapping
    // `serve_fut`: wrapping makes the whole server budget-bound from
    // startup. H21 originally had that shape and silently capped the
    // coordinator at 65s of uptime — see coordinator/src/bin/main.rs
    // commit history for the H25 fix.
    let bg_shutdown_for_signal = bg_shutdown.clone();
    let bg_shutdown_for_seatbelt = bg_shutdown.clone();
    let max_shutdown = query_timeout
        .saturating_mul(2)
        .saturating_add(Duration::from_secs(5));
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
        bg_shutdown_for_signal.notify_waiters();
    };

    server
        .add_service(svc)
        .serve_with_shutdown(addr, shutdown_signal)
        .await?;

    // Post-serve cleanup: signal any task that hasn't seen the drain yet.
    shutdown_handle.shutdown();
    bg_shutdown.notify_waiters();
    info!("Server stopped — all background tasks signalled");
    Ok(())
}
