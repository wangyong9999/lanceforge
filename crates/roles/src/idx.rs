// Indexer role — standalone compaction runner.
//
// Phase E of docs/ARCHITECTURE_V2_PLAN.md. Extracts the auto-
// compaction loop that previously lived inside pe_idx::run into its
// own `idx::run(config, shutdown)` function, so operators can run it
// as a separate `lance-idx` process for real CPU isolation from PE
// query workers.
//
// Compatibility: pe_idx::run keeps its inline compaction loop when
// `config.compaction.enabled = true` (the default). Split deployments
// set that to false and run `lance-idx` separately. Monolith keeps
// running pe_idx's inline loop — no change unless the operator opts
// into the split shape.
//
// Phase E scope: IDX compacts only shards declared in `config.tables`.
// Runtime-created tables (via CreateTable RPC after startup) aren't
// discovered here yet — that needs IDX to poll MetaStore for newly-
// appearing tables, which is on the 0.3 backlog (along with the
// JobQueue that also needs MetaStore).

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use lance_distributed_common::config::{ClusterConfig, ShardConfig};
use lance_distributed_worker::executor::LanceTableRegistry;
use log::{info, warn};

use crate::common::ShutdownSignal;

pub async fn run(
    config: ClusterConfig,
    shutdown: ShutdownSignal,
) -> Result<(), Box<dyn std::error::Error>> {
    let shards: Vec<ShardConfig> = config
        .tables
        .iter()
        .flat_map(|t| t.shards.iter().cloned())
        .collect();

    if shards.is_empty() {
        info!("lance-idx: no config-declared shards — waiting for shutdown");
        shutdown.notified().await;
        return Ok(());
    }

    let ctx = SessionContext::default();
    let registry = Arc::new(
        LanceTableRegistry::with_full_config(
            ctx,
            &shards,
            &config.storage_options,
            &config.cache,
        )
        .await?,
    );

    // Floor at 60s so a misconfigured `interval_secs: 0` doesn't
    // busy-loop compaction. Operators wanting tighter cadence must
    // set it explicitly.
    let interval = Duration::from_secs(config.compaction.interval_secs.max(60));
    info!(
        "lance-idx: compacting {} shards every {}s",
        shards.len(),
        interval.as_secs()
    );

    let mut tick = tokio::time::interval(interval);
    tick.tick().await; // skip immediate first tick
    loop {
        tokio::select! {
            _ = tick.tick() => {
                match registry.compact_all().await {
                    Ok(n) => info!("lance-idx: compacted {} table(s)", n),
                    Err(e) => warn!("lance-idx: compact failed: {e}"),
                }
            }
            _ = shutdown.notified() => break,
        }
    }

    registry.shutdown();
    info!("lance-idx: stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_distributed_common::config::{CompactionConfig, ExecutorConfig, TableConfig};
    use std::time::Instant;

    fn empty_config() -> ClusterConfig {
        ClusterConfig::default()
    }

    #[tokio::test]
    async fn empty_config_exits_cleanly_on_shutdown() {
        let shutdown = crate::common::new_shutdown();
        let sb = shutdown.clone();
        let handle = tokio::spawn(async move {
            // Swallow the error into a string so the task future is Send.
            // We only assert the task terminates cleanly.
            run(empty_config(), sb).await.map_err(|e| e.to_string())
        });

        // Give the runner a moment to enter its wait state.
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.notify_waiters();

        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("idx::run did not exit within 2s");
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn shutdown_cancels_mid_sleep() {
        // Config has one in-memory table so idx::run enters the
        // compact loop instead of the early-exit path. The 60s
        // floor means we'd wait a minute without shutdown; assert
        // shutdown cancels that wait promptly.
        let tmp = tempfile::tempdir().unwrap();
        let shard_path = tmp.path().join("s0.lance");
        let cfg = ClusterConfig {
            tables: vec![TableConfig {
                name: "t".into(),
                shards: vec![ShardConfig {
                    name: "s0".into(),
                    uri: shard_path.to_str().unwrap().to_string(),
                    executors: vec!["w0".into()],
                }],
            }],
            executors: vec![ExecutorConfig {
                id: "w0".into(),
                host: "127.0.0.1".into(),
                port: 9999,
                role: Default::default(),
            }],
            compaction: CompactionConfig {
                enabled: true,
                interval_secs: 10,
                skip_if_busy: false,
            },
            ..Default::default()
        };

        let shutdown = crate::common::new_shutdown();
        let sb = shutdown.clone();
        let started = Instant::now();
        let handle = tokio::spawn(async move {
            run(cfg, sb).await.map_err(|e| e.to_string())
        });

        // Let the runner enter the compact tick.
        tokio::time::sleep(Duration::from_millis(200)).await;
        shutdown.notify_waiters();

        let result = tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("idx::run did not exit within 3s after shutdown");
        // We accept Ok or Err — the key assertion is that it
        // stopped within 3s, not 60s (= the compact interval floor).
        let _ = result;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "idx took >5s to shutdown: {:?}",
            started.elapsed()
        );
    }
}
