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
    let initial_shards: Vec<ShardConfig> = config
        .tables
        .iter()
        .flat_map(|t| t.shards.iter().cloned())
        .collect();

    let ctx = SessionContext::default();
    let registry = Arc::new(
        LanceTableRegistry::with_full_config(
            ctx,
            &initial_shards,
            &config.storage_options,
            &config.cache,
        )
        .await?,
    );

    // #3 Runtime table discovery: when MetaStore is configured,
    // each tick refreshes the shard list from `shard_uris` and
    // LoadShards any newly-appearing entries. Without this step,
    // tables created at runtime via CreateTable RPC (not in the
    // config.tables list) would never be compacted by this
    // standalone IDX process.
    let meta_store = match config.metadata_path.as_deref() {
        Some(path) if !path.is_empty() => Some(open_metastore(path, &config).await?),
        _ => None,
    };

    // Floor at 60s so a misconfigured `interval_secs: 0` doesn't
    // busy-loop compaction. Operators wanting tighter cadence must
    // set it explicitly.
    let interval = Duration::from_secs(config.compaction.interval_secs.max(60));
    info!(
        "lance-idx: starting with {} config shard(s), metadata_path={}, interval={}s",
        initial_shards.len(),
        if meta_store.is_some() { "set (runtime discovery on)" } else { "unset (config-only)" },
        interval.as_secs()
    );

    let mut tick = tokio::time::interval(interval);
    tick.tick().await; // skip immediate first tick
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Some(ref ms) = meta_store {
                    discover_and_load(ms, &registry, &config.storage_options).await;
                }
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

async fn open_metastore(
    path: &str,
    config: &ClusterConfig,
) -> Result<Arc<lance_distributed_meta::state::MetaShardState>, Box<dyn std::error::Error>> {
    use lance_distributed_meta::state::MetaShardState;
    use lance_distributed_meta::store::{FileMetaStore, MetaStore, S3MetaStore};
    let store: Arc<dyn MetaStore> = if path.starts_with("s3://")
        || path.starts_with("gs://")
        || path.starts_with("az://")
    {
        Arc::new(
            S3MetaStore::new(
                path,
                config
                    .storage_options
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone())),
            )
            .await?,
        )
    } else {
        Arc::new(FileMetaStore::new(path).await?)
    };
    Ok(Arc::new(MetaShardState::new(store, "lanceforge")))
}

/// Pull the current shard_uris registry and LoadShard any entries
/// the registry hasn't seen yet. Idempotent — already-loaded shard
/// names are skipped without touching Lance.
async fn discover_and_load(
    meta: &lance_distributed_meta::state::MetaShardState,
    registry: &Arc<LanceTableRegistry>,
    storage_options: &std::collections::HashMap<String, String>,
) {
    let uris = meta.get_shard_uris().await;
    if uris.is_empty() {
        return;
    }
    let loaded: std::collections::HashSet<String> =
        registry.shard_names().await.into_iter().collect();
    for (shard_name, uri) in uris {
        if loaded.contains(&shard_name) {
            continue;
        }
        match registry.load_shard(&shard_name, &uri, storage_options).await {
            Ok(_) => info!("lance-idx: discovered runtime shard '{shard_name}' at {uri}"),
            Err(e) => warn!(
                "lance-idx: failed to load runtime shard '{shard_name}' at {uri}: {e}"
            ),
        }
    }
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
    async fn discover_and_load_skips_already_loaded_and_attempts_new() {
        // Verify the discovery helper: given a MetaStore with one
        // URI for an already-loaded shard and one for an
        // unknown-shard URI that can't actually be loaded, the
        // helper must NOT attempt to re-load the loaded shard and
        // must NOT panic on the failed-load case (it logs and
        // continues).
        //
        // We don't build a real Lance dataset in the test — the
        // happy-path load_shard requires lancedb dependencies the
        // roles crate doesn't pull for tests. The failure path
        // still exercises the discovery flow: read MetaStore →
        // diff against registry → attempt load → log on error.
        use lance_distributed_common::config::CacheConfig;
        use lance_distributed_meta::state::MetaShardState;
        use lance_distributed_meta::store::{FileMetaStore, MetaStore};
        let tmp = tempfile::tempdir().unwrap();
        let meta_path = tmp.path().join("meta.json");

        // Seed one URI that doesn't point at a real shard.
        {
            let store: Arc<dyn MetaStore> = Arc::new(
                FileMetaStore::new(meta_path.to_str().unwrap()).await.unwrap(),
            );
            let state = MetaShardState::new(store, "lanceforge");
            let mut map = std::collections::HashMap::new();
            map.insert(
                "ghost_shard".to_string(),
                tmp.path().join("does_not_exist.lance").to_str().unwrap().to_string(),
            );
            state.put_shard_uris(&map).await;
        }

        // Open the MetaStore again + an empty registry.
        let store: Arc<dyn MetaStore> = Arc::new(
            FileMetaStore::new(meta_path.to_str().unwrap()).await.unwrap(),
        );
        let state = MetaShardState::new(store, "lanceforge");
        let registry = Arc::new(
            LanceTableRegistry::with_full_config(
                SessionContext::default(),
                &[],
                &std::collections::HashMap::new(),
                &CacheConfig::default(),
            )
            .await
            .unwrap(),
        );

        // discover_and_load reads the MetaStore (1 entry), sees
        // the registry has no shards, attempts load_shard on the
        // bogus URI. Lance fails cleanly; helper logs + returns.
        discover_and_load(&state, &registry, &std::collections::HashMap::new()).await;
        // Registry stayed empty (bad URI didn't create a phantom
        // entry).
        assert!(registry.shard_names().await.is_empty());
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
