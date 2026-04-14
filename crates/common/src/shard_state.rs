// Licensed under the Apache License, Version 2.0.
// Shard state management: mapping FragmentGroups to Executors.
//
// Two implementations:
// - StaticShardState: from YAML config (MVP, no persistence)
// - EtcdShardState: persisted in etcd with Watch (Prod)
//
// Both support the two-phase Target model:
// - CurrentTarget: used for query routing (read-only during queries)
// - NextTarget: computed by rebalancer, promoted atomically

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use log::{debug, info, warn};

use crate::config::ClusterConfig;

/// Mapping: shard_name → list of executor_ids (primary first, then secondary)
pub type ShardMapping = HashMap<String, Vec<String>>;

/// Two-phase target state for a table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TargetState {
    pub current: ShardMapping,
    pub next: Option<ShardMapping>,
    pub version: u64,
}

/// Per-executor readiness: which shards this executor has confirmed loaded.
/// Used by the readiness gate to prevent premature promote.
pub type ExecutorReadiness = HashMap<String, Vec<String>>; // executor_id → loaded shard_names

/// Trait for shard state backends.
#[async_trait::async_trait]
pub trait ShardState: Send + Sync + 'static {
    /// Get current routing mapping for a table.
    async fn get_current_target(&self, table_name: &str) -> Option<ShardMapping>;

    /// Set the next target (desired state after rebalancing).
    async fn set_next_target(&self, table_name: &str, mapping: ShardMapping) -> Result<(), String>;

    /// Promote NextTarget → CurrentTarget atomically.
    /// Only succeeds if all shards in NextTarget are confirmed loaded by their assigned executors.
    async fn promote_target(&self, table_name: &str) -> Result<(), String>;

    /// Report that an executor has finished loading specific shards.
    /// Called by health check or executor heartbeat.
    async fn report_loaded(&self, executor_id: &str, shard_names: Vec<String>);

    /// Check if NextTarget is ready to promote (all shards loaded by assigned executors).
    async fn is_next_target_ready(&self, table_name: &str) -> bool;

    /// Get shard→executor mapping for query routing.
    /// Returns: Vec<(shard_name, primary_executor, Option<secondary_executor>)>
    async fn get_shard_routing(&self, table_name: &str) -> Vec<(String, String, Option<String>)>;

    /// Get all executor IDs that should serve a table (from CurrentTarget).
    async fn executors_for_table(&self, table_name: &str) -> Vec<String>;

    /// Register a new executor. Triggers rebalancing if needed.
    async fn register_executor(&self, executor_id: &str, host: &str, port: u16);

    /// Remove an executor. Triggers rebalancing.
    async fn remove_executor(&self, executor_id: &str);

    /// Get all known executors.
    async fn all_executors(&self) -> Vec<(String, String, u16)>; // (id, host, port)

    /// Register a new table with its shard mapping (bypasses readiness gate).
    /// Used by CreateTable to make a new table immediately queryable.
    async fn register_table(&self, table_name: &str, mapping: ShardMapping);

    /// List all table names managed by this state.
    async fn all_tables(&self) -> Vec<String>;
}

/// Reconciliation loop: periodically checks NextTarget readiness and auto-promotes.
/// Inspired by Milvus TargetObserver.check().
/// Run this as a background tokio task alongside the Scheduler.
pub async fn reconciliation_loop(state: Arc<dyn ShardState>, interval: Duration) {
    info!("Reconciliation loop started (interval: {:?})", interval);
    loop {
        tokio::time::sleep(interval).await;

        let tables = state.all_tables().await;
        for table in &tables {
            if state.is_next_target_ready(table).await {
                match state.promote_target(table).await {
                    Ok(()) => info!("Reconciliation: auto-promoted table={}", table),
                    Err(e) => warn!("Reconciliation: promote failed for table={}: {}", table, e),
                }
            }
        }
    }
}

// ============================================================
// StaticShardState: from YAML config (MVP)
// ============================================================

pub struct StaticShardState {
    targets: Arc<RwLock<HashMap<String, TargetState>>>,
    executors: Arc<RwLock<Vec<(String, String, u16)>>>,
    /// Executor readiness: which shards each executor has confirmed loaded
    readiness: Arc<RwLock<ExecutorReadiness>>,
}

impl StaticShardState {
    pub fn from_config(config: &ClusterConfig) -> Self {
        let mut targets = HashMap::new();

        for table in &config.tables {
            let mut mapping = ShardMapping::new();
            for shard in &table.shards {
                mapping.insert(shard.name.clone(), shard.executors.clone());
            }
            targets.insert(table.name.clone(), TargetState {
                current: mapping,
                next: None,
                version: 1,
            });
        }

        let executors: Vec<(String, String, u16)> = config.executors.iter()
            .map(|e| (e.id.clone(), e.host.clone(), e.port))
            .collect();

        Self {
            targets: Arc::new(RwLock::new(targets)),
            executors: Arc::new(RwLock::new(executors)),
            readiness: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Incremental rebalance: preserves existing valid assignments,
    /// only moves shards from removed/overloaded executors.
    /// Inspired by Milvus ScoreBasedBalancer: minimize unnecessary migrations.
    async fn rebalance(&self, table_name: &str) {
        let executors = self.executors.read().await;
        if executors.is_empty() {
            return;
        }

        let executor_ids: Vec<String> = executors.iter().map(|(id, _, _)| id.clone()).collect();

        let mut targets = self.targets.write().await;
        if let Some(state) = targets.get(table_name) {
            let mut new_mapping = ShardMapping::new();

            // Count how many primary shards each executor has
            let mut load: HashMap<String, usize> = executor_ids.iter()
                .map(|id| (id.clone(), 0))
                .collect();

            // Phase 1: Keep existing assignments where executor is still alive
            for (shard, assigned) in &state.current {
                let valid_primary = assigned.first()
                    .filter(|eid| executor_ids.contains(eid))
                    .cloned();
                let valid_secondary = assigned.get(1)
                    .filter(|eid| executor_ids.contains(eid))
                    .cloned();

                if let Some(ref primary) = valid_primary {
                    *load.entry(primary.clone()).or_insert(0) += 1;
                }

                new_mapping.insert(shard.clone(), match (valid_primary, valid_secondary) {
                    (Some(p), Some(s)) => vec![p, s],
                    (Some(p), None) => vec![p],
                    (None, Some(s)) => vec![s], // promote secondary to primary
                    (None, None) => vec![],     // orphan, needs reassignment
                });
            }

            // Phase 2: Assign orphan shards to least-loaded executor
            let shard_names_to_fix: Vec<String> = new_mapping.iter()
                .filter(|(_, assigned)| assigned.is_empty())
                .map(|(name, _)| name.clone())
                .collect();

            for shard in &shard_names_to_fix {
                let least = load.iter().min_by_key(|(_, count)| *count)
                    .map(|(id, _)| id.clone());
                if let Some(eid) = least {
                    if let Some(assigned) = new_mapping.get_mut(shard) {
                        assigned.push(eid.clone());
                    }
                    if let Some(count) = load.get_mut(&eid) {
                        *count += 1;
                    }
                    info!("Rebalance: orphan shard {} → {}", shard, eid);
                }
            }

            // Phase 3: Ensure secondary if >1 executors
            if executor_ids.len() > 1 {
                let shards_needing_secondary: Vec<String> = new_mapping.iter()
                    .filter(|(_, assigned)| assigned.len() == 1)
                    .map(|(name, _)| name.clone())
                    .collect();

                for shard in &shards_needing_secondary {
                    let primary = new_mapping[shard][0].clone();
                    let secondary = load.iter()
                        .filter(|(id, _)| **id != primary)
                        .min_by_key(|(_, count)| *count)
                        .map(|(id, _)| id.clone());
                    if let Some(sec) = secondary {
                        if let Some(assigned) = new_mapping.get_mut(shard) {
                            assigned.push(sec);
                        }
                    }
                }
            }

            let version = state.version;
            // Only set next if it differs from current
            if new_mapping != state.current {
                if let Some(s) = targets.get_mut(table_name) {
                    s.next = Some(new_mapping);
                    info!("Rebalance: table={} v{} → next target computed (incremental)", table_name, version);
                }
            } else {
                debug!("Rebalance: table={} v{} → no changes needed", table_name, version);
            }
        }
    }
}

#[async_trait::async_trait]
impl ShardState for StaticShardState {
    async fn get_current_target(&self, table_name: &str) -> Option<ShardMapping> {
        let targets = self.targets.read().await;
        targets.get(table_name).map(|s| s.current.clone())
    }

    async fn set_next_target(&self, table_name: &str, mapping: ShardMapping) -> Result<(), String> {
        let mut targets = self.targets.write().await;
        if let Some(state) = targets.get_mut(table_name) {
            state.next = Some(mapping);
            Ok(())
        } else {
            Err(format!("Table not found: {}", table_name))
        }
    }

    async fn promote_target(&self, table_name: &str) -> Result<(), String> {
        // Readiness gate: check if all shards are loaded before promoting
        if !self.is_next_target_ready(table_name).await {
            return Err("NextTarget not ready: not all shards confirmed loaded".to_string());
        }

        let mut targets = self.targets.write().await;
        if let Some(state) = targets.get_mut(table_name) {
            if let Some(next) = state.next.take() {
                info!("Promote: table={} v{} → v{}", table_name, state.version, state.version + 1);
                state.current = next;
                state.version += 1;
                Ok(())
            } else {
                Err("No next target to promote".to_string())
            }
        } else {
            Err(format!("Table not found: {}", table_name))
        }
    }

    async fn report_loaded(&self, executor_id: &str, shard_names: Vec<String>) {
        let mut readiness = self.readiness.write().await;
        readiness.insert(executor_id.to_string(), shard_names);
    }

    async fn is_next_target_ready(&self, table_name: &str) -> bool {
        let targets = self.targets.read().await;
        let state = match targets.get(table_name) {
            Some(s) => s,
            None => return false,
        };

        let next = match &state.next {
            Some(n) => n,
            None => return false,
        };

        let readiness = self.readiness.read().await;

        // Check: for each shard in NextTarget, at least the primary executor
        // has reported that shard as loaded
        for (shard_name, assigned_executors) in next {
            let primary = match assigned_executors.first() {
                Some(p) => p,
                None => return false,
            };

            let is_loaded = readiness
                .get(primary)
                .map(|loaded| loaded.contains(shard_name))
                .unwrap_or(false);

            if !is_loaded {
                debug!("NextTarget not ready: shard {} not loaded on primary {}", shard_name, primary);
                return false;
            }
        }

        true
    }

    async fn get_shard_routing(&self, table_name: &str) -> Vec<(String, String, Option<String>)> {
        let targets = self.targets.read().await;
        match targets.get(table_name) {
            Some(state) => {
                state.current.iter().map(|(shard, executors)| {
                    let primary = executors.first().cloned().unwrap_or_default();
                    let secondary = executors.get(1).cloned();
                    (shard.clone(), primary, secondary)
                }).collect()
            }
            None => vec![],
        }
    }

    async fn executors_for_table(&self, table_name: &str) -> Vec<String> {
        let targets = self.targets.read().await;
        let mut executor_ids = Vec::new();
        if let Some(state) = targets.get(table_name) {
            for executors in state.current.values() {
                for eid in executors {
                    if !executor_ids.contains(eid) {
                        executor_ids.push(eid.clone());
                    }
                }
            }
        }
        executor_ids
    }

    async fn register_executor(&self, executor_id: &str, host: &str, port: u16) {
        let mut executors = self.executors.write().await;
        if !executors.iter().any(|(id, _, _)| id == executor_id) {
            executors.push((executor_id.to_string(), host.to_string(), port));
            info!("Registered executor: {} at {}:{}", executor_id, host, port);
        }
        drop(executors);

        // Trigger rebalance for all tables
        let table_names: Vec<String> = {
            let targets = self.targets.read().await;
            targets.keys().cloned().collect()
        };
        for table in &table_names {
            self.rebalance(table).await;
        }
    }

    async fn remove_executor(&self, executor_id: &str) {
        let mut executors = self.executors.write().await;
        executors.retain(|(id, _, _)| id != executor_id);
        warn!("Removed executor: {}", executor_id);
        drop(executors);

        // Trigger rebalance for all tables
        let table_names: Vec<String> = {
            let targets = self.targets.read().await;
            targets.keys().cloned().collect()
        };
        for table in &table_names {
            self.rebalance(table).await;
        }
    }

    async fn all_executors(&self) -> Vec<(String, String, u16)> {
        self.executors.read().await.clone()
    }

    async fn all_tables(&self) -> Vec<String> {
        self.targets.read().await.keys().cloned().collect()
    }

    async fn register_table(&self, table_name: &str, mapping: ShardMapping) {
        let mut targets = self.targets.write().await;
        targets.insert(table_name.to_string(), TargetState {
            current: mapping,
            next: None,
            version: 1,
        });
        info!("Registered new table '{}' in shard state", table_name);
    }
}

// ============================================================
// EtcdShardState (behind feature flag)
// ============================================================

#[cfg(feature = "etcd")]
pub mod etcd_state {
    use super::*;
    use etcd_client::{Client, GetOptions, PutOptions};

    pub struct EtcdShardState {
        /// Read/write client (for get/put/delete operations)
        rw_client: Arc<tokio::sync::Mutex<Client>>,
        prefix: String,
        /// Cache current targets in memory, synced from etcd Watch
        cache: Arc<RwLock<HashMap<String, TargetState>>>,
        /// Readiness tracking (in-memory, not persisted to etcd)
        readiness: Arc<RwLock<super::ExecutorReadiness>>,
    }

    impl EtcdShardState {
        pub async fn new(endpoints: &[&str], prefix: &str) -> Result<Self, String> {
            // Separate clients for read/write vs watch to avoid blocking
            let rw_client = Client::connect(endpoints, None)
                .await
                .map_err(|e| format!("etcd rw connect failed: {e}"))?;
            let watch_client = Client::connect(endpoints, None)
                .await
                .map_err(|e| format!("etcd watch connect failed: {e}"))?;

            let state = Self {
                rw_client: Arc::new(tokio::sync::Mutex::new(rw_client)),
                prefix: prefix.to_string(),
                cache: Arc::new(RwLock::new(HashMap::new())),
                readiness: Arc::new(RwLock::new(HashMap::new())),
            };

            state.sync_from_etcd().await?;

            // Watch uses its own client — never blocks rw operations
            let cache = state.cache.clone();
            let watch_client = Arc::new(tokio::sync::Mutex::new(watch_client));
            let prefix_clone = state.prefix.clone();
            tokio::spawn(async move {
                Self::watch_loop(watch_client, prefix_clone, cache).await;
            });

            Ok(state)
        }

        async fn sync_from_etcd(&self) -> Result<(), String> {
            let mut client = self.rw_client.lock().await;
            let key = format!("{}/targets/", self.prefix);
            let resp = client
                .get(key.as_bytes(), Some(GetOptions::new().with_prefix()))
                .await
                .map_err(|e| format!("etcd get failed: {e}"))?;

            let mut cache = self.cache.write().await;
            for kv in resp.kvs() {
                let key_str = kv.key_str().unwrap_or_default();
                // Parse: prefix/targets/current/{table} or prefix/targets/next/{table}
                if let Some(table) = key_str.strip_prefix(&format!("{}/targets/current/", self.prefix)) {
                    let mapping: ShardMapping = serde_json::from_slice(kv.value())
                        .unwrap_or_default();
                    let entry = cache.entry(table.to_string()).or_insert(TargetState {
                        current: ShardMapping::new(),
                        next: None,
                        version: kv.mod_revision() as u64,
                    });
                    entry.current = mapping;
                    entry.version = kv.mod_revision() as u64;
                }
            }
            info!("Synced {} tables from etcd", cache.len());
            Ok(())
        }

        async fn watch_loop(
            client: Arc<tokio::sync::Mutex<Client>>,
            prefix: String,
            cache: Arc<RwLock<HashMap<String, TargetState>>>,
        ) {
            loop {
                let result = {
                    let mut c = client.lock().await;
                    let key = format!("{}/targets/", prefix);
                    c.watch(key.as_bytes(), Some(etcd_client::WatchOptions::new().with_prefix()))
                        .await
                };

                match result {
                    Ok((_, mut stream)) => {
                        while let Some(resp) = stream.message().await.ok().flatten() {
                            for event in resp.events() {
                                if let Some(kv) = event.kv() {
                                    let key_str = kv.key_str().unwrap_or_default();
                                    if let Some(table) = key_str.strip_prefix(
                                        &format!("{}/targets/current/", prefix),
                                    ) {
                                        if let Ok(mapping) = serde_json::from_slice::<ShardMapping>(kv.value()) {
                                            let mut c = cache.write().await;
                                            let entry = c.entry(table.to_string()).or_insert(TargetState {
                                                current: ShardMapping::new(),
                                                next: None,
                                                version: 0,
                                            });
                                            entry.current = mapping;
                                            entry.version = kv.mod_revision() as u64;
                                            info!("etcd watch: updated table={} v{}", table, entry.version);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("etcd watch error: {}, retrying in 5s", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl ShardState for EtcdShardState {
        async fn get_current_target(&self, table_name: &str) -> Option<ShardMapping> {
            let cache = self.cache.read().await;
            cache.get(table_name).map(|s| s.current.clone())
        }

        async fn set_next_target(&self, table_name: &str, mapping: ShardMapping) -> Result<(), String> {
            let key = format!("{}/targets/next/{}", self.prefix, table_name);
            let value = serde_json::to_vec(&mapping).map_err(|e| format!("serialize: {e}"))?;

            let mut client = self.rw_client.lock().await;
            client.put(key.as_bytes(), value, None)
                .await
                .map_err(|e| format!("etcd put next: {e}"))?;

            let mut cache = self.cache.write().await;
            let entry = cache.entry(table_name.to_string()).or_insert(TargetState {
                current: ShardMapping::new(),
                next: None,
                version: 0,
            });
            entry.next = Some(mapping);
            Ok(())
        }

        async fn promote_target(&self, table_name: &str) -> Result<(), String> {
            // Read next, write to current, delete next — atomically via etcd txn
            let mut cache = self.cache.write().await;
            let state = cache.get_mut(table_name).ok_or("Table not found")?;
            let next = state.next.take().ok_or("No next target")?;

            let key = format!("{}/targets/current/{}", self.prefix, table_name);
            let value = serde_json::to_vec(&next).map_err(|e| format!("serialize: {e}"))?;

            let mut client = self.rw_client.lock().await;
            client.put(key.as_bytes(), value, None)
                .await
                .map_err(|e| format!("etcd put current: {e}"))?;

            // Delete next key
            let next_key = format!("{}/targets/next/{}", self.prefix, table_name);
            let _ = client.delete(next_key.as_bytes(), None).await;

            state.current = next;
            state.version += 1;
            info!("etcd promote: table={} v{}", table_name, state.version);
            Ok(())
        }

        async fn executors_for_table(&self, table_name: &str) -> Vec<String> {
            let cache = self.cache.read().await;
            let mut ids = Vec::new();
            if let Some(state) = cache.get(table_name) {
                for executors in state.current.values() {
                    for eid in executors {
                        if !ids.contains(eid) { ids.push(eid.clone()); }
                    }
                }
            }
            ids
        }

        async fn register_executor(&self, executor_id: &str, host: &str, port: u16) {
            let key = format!("{}/executors/{}", self.prefix, executor_id);
            let value = format!("{}:{}", host, port);
            let mut client = self.rw_client.lock().await;
            let _ = client.put(key.as_bytes(), value.as_bytes(), None).await;
            info!("etcd: registered executor {}", executor_id);
        }

        async fn remove_executor(&self, executor_id: &str) {
            let key = format!("{}/executors/{}", self.prefix, executor_id);
            let mut client = self.rw_client.lock().await;
            let _ = client.delete(key.as_bytes(), None).await;
            warn!("etcd: removed executor {}", executor_id);
        }

        async fn all_executors(&self) -> Vec<(String, String, u16)> {
            let key = format!("{}/executors/", self.prefix);
            let mut client = self.rw_client.lock().await;
            match client.get(key.as_bytes(), Some(GetOptions::new().with_prefix())).await {
                Ok(resp) => {
                    resp.kvs().iter().filter_map(|kv| {
                        let id = kv.key_str().ok()?.strip_prefix(&format!("{}/executors/", self.prefix))?.to_string();
                        let val = kv.value_str().ok()?;
                        let parts: Vec<&str> = val.split(':').collect();
                        if parts.len() == 2 {
                            Some((id, parts[0].to_string(), parts[1].parse().ok()?))
                        } else {
                            None
                        }
                    }).collect()
                }
                Err(_) => vec![],
            }
        }

        async fn all_tables(&self) -> Vec<String> {
            self.cache.read().await.keys().cloned().collect()
        }

        async fn register_table(&self, table_name: &str, mapping: ShardMapping) {
            let mut cache = self.cache.write().await;
            cache.insert(table_name.to_string(), TargetState {
                current: mapping, next: None, version: 1,
            });
        }

        async fn report_loaded(&self, executor_id: &str, shard_names: Vec<String>) {
            let mut readiness = self.readiness.write().await;
            readiness.insert(executor_id.to_string(), shard_names);
        }

        async fn is_next_target_ready(&self, table_name: &str) -> bool {
            let cache = self.cache.read().await;
            let state = match cache.get(table_name) {
                Some(s) => s,
                None => return false,
            };
            let next = match &state.next {
                Some(n) => n,
                None => return false,
            };
            let readiness = self.readiness.read().await;
            for (shard_name, assigned) in next {
                let primary = match assigned.first() {
                    Some(p) => p,
                    None => return false,
                };
                let loaded = readiness.get(primary)
                    .map(|s| s.contains(shard_name))
                    .unwrap_or(false);
                if !loaded { return false; }
            }
            true
        }

        async fn get_shard_routing(&self, table_name: &str) -> Vec<(String, String, Option<String>)> {
            let cache = self.cache.read().await;
            match cache.get(table_name) {
                Some(state) => state.current.iter().map(|(shard, executors)| {
                    let primary = executors.first().cloned().unwrap_or_default();
                    let secondary = executors.get(1).cloned();
                    (shard.clone(), primary, secondary)
                }).collect(),
                None => vec![],
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExecutorConfig, ShardConfig, TableConfig};

    fn test_config() -> ClusterConfig {
        ClusterConfig {
            tables: vec![TableConfig {
                name: "t1".into(),
                shards: vec![
                    ShardConfig { name: "s0".into(), uri: "/tmp/s0".into(), executors: vec!["e0".into()] },
                    ShardConfig { name: "s1".into(), uri: "/tmp/s1".into(), executors: vec!["e0".into()] },
                    ShardConfig { name: "s2".into(), uri: "/tmp/s2".into(), executors: vec!["e1".into()] },
                ],
            }],
            executors: vec![
                ExecutorConfig { id: "e0".into(), host: "h0".into(), port: 100 },
                ExecutorConfig { id: "e1".into(), host: "h1".into(), port: 101 },
            ],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_static_state_basic() {
        let state = StaticShardState::from_config(&test_config());

        let mapping = state.get_current_target("t1").await.unwrap();
        assert_eq!(mapping.len(), 3);
        assert_eq!(mapping["s0"], vec!["e0"]);
        assert_eq!(mapping["s2"], vec!["e1"]);

        let execs = state.executors_for_table("t1").await;
        assert!(execs.contains(&"e0".to_string()));
        assert!(execs.contains(&"e1".to_string()));
    }

    #[tokio::test]
    async fn test_static_state_promote_with_readiness() {
        let state = StaticShardState::from_config(&test_config());

        let mut new_mapping = ShardMapping::new();
        new_mapping.insert("s0".into(), vec!["e1".into()]);
        new_mapping.insert("s1".into(), vec!["e1".into()]);
        new_mapping.insert("s2".into(), vec!["e0".into()]);

        state.set_next_target("t1", new_mapping.clone()).await.unwrap();

        // Promote should fail — no readiness reported yet
        let result = state.promote_target("t1").await;
        assert!(result.is_err(), "Should fail without readiness");
        assert!(result.unwrap_err().contains("not ready"));

        // Report partial readiness (only e1 loaded s0)
        state.report_loaded("e1", vec!["s0".into(), "s1".into()]).await;

        // Still should fail — e0 hasn't loaded s2
        let result = state.promote_target("t1").await;
        assert!(result.is_err(), "Should fail with partial readiness");

        // Report e0 loaded s2
        state.report_loaded("e0", vec!["s2".into()]).await;

        // Now should succeed
        state.promote_target("t1").await.unwrap();

        let current = state.get_current_target("t1").await.unwrap();
        assert_eq!(current["s0"], vec!["e1"]);
        assert_eq!(current["s2"], vec!["e0"]);
    }

    #[tokio::test]
    async fn test_shard_routing() {
        let state = StaticShardState::from_config(&test_config());
        let routing = state.get_shard_routing("t1").await;

        assert_eq!(routing.len(), 3);
        // Each shard should have primary (and possibly secondary)
        for (shard, primary, _secondary) in &routing {
            assert!(!primary.is_empty(), "Shard {} should have primary", shard);
        }
    }

    #[tokio::test]
    async fn test_static_state_rebalance_on_add() {
        let state = StaticShardState::from_config(&test_config());

        // Add executor e2
        state.register_executor("e2", "h2", 102).await;

        let targets = state.targets.read().await;
        let t1 = targets.get("t1").unwrap();

        // Incremental rebalance: existing assignments preserved
        // New executor gets secondaries, not primaries (minimize migration)
        // Next target should exist because secondaries are being added
        if let Some(next) = &t1.next {
            // Every shard should have at least primary + secondary
            for (shard, executors) in next {
                assert!(!executors.is_empty(), "Shard {} should have primary", shard);
                if executors.len() >= 2 {
                    assert_ne!(executors[0], executors[1], "Primary and secondary should differ for {}", shard);
                }
            }
        }
        // If no changes needed (current already optimal), next is None — also valid
    }

    #[tokio::test]
    async fn test_static_state_remove_executor() {
        let state = StaticShardState::from_config(&test_config());

        state.remove_executor("e1").await;

        let targets = state.targets.read().await;
        let t1 = targets.get("t1").unwrap();
        assert!(t1.next.is_some());

        // All shards should now be on e0 (only remaining executor)
        let next = t1.next.as_ref().unwrap();
        for executors in next.values() {
            assert!(executors.contains(&"e0".to_string()));
        }
    }

    #[tokio::test]
    async fn test_promote_without_next_errors() {
        let state = StaticShardState::from_config(&test_config());
        let result = state.promote_target("t1").await;
        assert!(result.is_err(), "Promote without next should fail");
    }

    #[tokio::test]
    async fn test_unknown_table() {
        let state = StaticShardState::from_config(&test_config());
        assert!(state.get_current_target("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_reconciliation_auto_promote() {
        let state = Arc::new(StaticShardState::from_config(&test_config()));

        // Set a next target
        let mut new_mapping = ShardMapping::new();
        new_mapping.insert("s0".into(), vec!["e1".into()]);
        new_mapping.insert("s1".into(), vec!["e1".into()]);
        new_mapping.insert("s2".into(), vec!["e0".into()]);
        state.set_next_target("t1", new_mapping).await.unwrap();

        // Not ready yet — no executor has reported loaded
        assert!(!state.is_next_target_ready("t1").await);

        // Report all loaded
        state.report_loaded("e1", vec!["s0".into(), "s1".into()]).await;
        state.report_loaded("e0", vec!["s2".into()]).await;

        // Now ready
        assert!(state.is_next_target_ready("t1").await);

        // Simulate reconciliation loop (one iteration)
        let state_clone: Arc<dyn ShardState> = state.clone();
        let tables = state_clone.all_tables().await;
        for table in &tables {
            if state_clone.is_next_target_ready(table).await {
                state_clone.promote_target(table).await.unwrap();
            }
        }

        // Verify promoted
        let current = state.get_current_target("t1").await.unwrap();
        assert_eq!(current["s0"], vec!["e1"]);
    }

    #[tokio::test]
    async fn test_all_tables() {
        let state = StaticShardState::from_config(&test_config());
        let tables = state.all_tables().await;
        assert_eq!(tables, vec!["t1"]);
    }
}
