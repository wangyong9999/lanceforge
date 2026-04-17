// Licensed under the Apache License, Version 2.0.
// PersistentShardState: shared metadata stored on S3/local filesystem.
//
// Uses the same storage backend as Lance tables (S3, MinIO, local FS).
// All coordinators read/write the same metadata file, ensuring consistency
// across restarts and multi-coordinator deployments.
//
// Key design: metadata is a small JSON file (<1MB even for thousands of tables).
// Write frequency is low (DDL operations only), so S3 PUT cost is negligible.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{info, debug};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::shard_state::{
    ExecutorReadiness, ShardMapping, ShardState, TargetState,
};

/// Persistent metadata stored as JSON on shared storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MetadataSnapshot {
    tables: HashMap<String, TargetState>,
    executors: Vec<(String, String, u16)>, // (id, host, port)
    version: u64,
}

/// ShardState implementation backed by a shared storage file (S3 or local).
/// Persists across coordinator restarts. Multiple coordinators share the same file.
pub struct PersistentShardState {
    /// In-memory cache of the current state.
    state: RwLock<MetadataSnapshot>,
    /// Readiness tracking (in-memory, not persisted).
    readiness: RwLock<ExecutorReadiness>,
    /// Path to the metadata file (e.g., "s3://bucket/lanceforge/metadata.json").
    metadata_path: String,
    /// Storage options for S3 access.
    storage_options: HashMap<String, String>,
}

impl PersistentShardState {
    /// Create from config, loading any existing persisted state.
    pub async fn new(
        metadata_path: &str,
        storage_options: &HashMap<String, String>,
        config: &super::config::ClusterConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let state = Self {
            state: RwLock::new(MetadataSnapshot::default()),
            readiness: RwLock::new(HashMap::new()),
            metadata_path: metadata_path.to_string(),
            storage_options: storage_options.clone(),
        };

        // Try to load existing persisted state
        if let Err(e) = state.load().await {
            info!("No existing metadata at {} ({}), initializing from config", metadata_path, e);
        }

        // Merge config into loaded state (config defines executors + initial tables)
        {
            let mut s = state.state.write().await;
            // Executors from config always override (they define the current cluster)
            s.executors = config.executors.iter()
                .map(|e| (e.id.clone(), e.host.clone(), e.port))
                .collect();

            // Tables from config: only add if not already in persisted state
            let routing = config.build_routing_table();
            for (table_name, shard_routes) in &routing {
                if !s.tables.contains_key(table_name) {
                    let mapping: ShardMapping = shard_routes.iter()
                        .map(|(sname, execs)| (sname.clone(), execs.clone()))
                        .collect();
                    s.tables.insert(table_name.clone(), TargetState {
                        current: mapping,
                        next: None,
                        version: 1,
                    });
                }
            }
        }

        // Persist merged state
        state.save().await?;

        // Background sync is started by the caller (coordinator binary)
        // to avoid Arc ownership issues in constructors.
        Ok(state)
    }

    /// Create without background sync (for simpler usage / testing).
    pub async fn from_config(
        metadata_path: &str,
        config: &super::config::ClusterConfig,
    ) -> Self {
        let mut tables = HashMap::new();
        let routing = config.build_routing_table();
        for (table_name, shard_routes) in &routing {
            let mapping: ShardMapping = shard_routes.iter()
                .map(|(sname, execs)| (sname.clone(), execs.clone()))
                .collect();
            tables.insert(table_name.clone(), TargetState {
                current: mapping,
                next: None,
                version: 1,
            });
        }

        let executors = config.executors.iter()
            .map(|e| (e.id.clone(), e.host.clone(), e.port))
            .collect();

        Self {
            state: RwLock::new(MetadataSnapshot { tables, executors, version: 1 }),
            readiness: RwLock::new(HashMap::new()),
            metadata_path: metadata_path.to_string(),
            storage_options: HashMap::new(),
        }
    }

    pub async fn load(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let data = if self.metadata_path.starts_with("s3://") || self.metadata_path.starts_with("gs://") {
            // S3/GCS: use lance object_store
            let store = lance_io_object_store(&self.metadata_path, &self.storage_options)?;
            store.get(&self.metadata_path).await?
        } else {
            // Local filesystem
            tokio::fs::read_to_string(&self.metadata_path).await?
        };
        let snapshot: MetadataSnapshot = serde_json::from_str(&data)?;
        *self.state.write().await = snapshot;
        debug!("Loaded metadata from {}", self.metadata_path);
        Ok(())
    }

    pub async fn save(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snapshot = self.state.read().await.clone();
        let json = serde_json::to_string_pretty(&snapshot)?;

        if self.metadata_path.starts_with("s3://") || self.metadata_path.starts_with("gs://") {
            let store = lance_io_object_store(&self.metadata_path, &self.storage_options)?;
            store.put(&self.metadata_path, &json).await?;
        } else {
            // Local filesystem
            if let Some(parent) = std::path::Path::new(&self.metadata_path).parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
            tokio::fs::write(&self.metadata_path, &json).await?;
        }
        debug!("Saved metadata to {}", self.metadata_path);
        Ok(())
    }

    /// Start a background sync loop. Call from coordinator binary.
    pub fn start_sync(self: &Arc<Self>, interval: Duration) {
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = state.load().await {
                    debug!("Metadata sync failed: {}", e);
                }
            }
        });
    }
}

// Simple file-based storage (no object_store dependency for MVP)
fn lance_io_object_store(
    _path: &str,
    _opts: &HashMap<String, String>,
) -> Result<SimpleStore, Box<dyn std::error::Error + Send + Sync>> {
    // For S3, we'd use lance::io::ObjectStore. For MVP, support local FS only.
    // S3 support is a straightforward addition via object_store crate.
    Ok(SimpleStore)
}

struct SimpleStore;
impl SimpleStore {
    async fn get(&self, path: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("S3 metadata store not yet implemented for {}", path).into())
    }
    async fn put(&self, _path: &str, _data: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("S3 metadata store not yet implemented".into())
    }
}

#[async_trait::async_trait]
impl ShardState for PersistentShardState {
    async fn get_current_target(&self, table_name: &str) -> Option<ShardMapping> {
        self.state.read().await.tables.get(table_name).map(|t| t.current.clone())
    }

    async fn set_next_target(&self, table_name: &str, mapping: ShardMapping) -> Result<(), String> {
        let mut s = self.state.write().await;
        if let Some(target) = s.tables.get_mut(table_name) {
            target.next = Some(mapping);
        } else {
            s.tables.insert(table_name.to_string(), TargetState {
                current: HashMap::new(),
                next: Some(mapping),
                version: 1,
            });
        }
        drop(s);
        self.save().await.map_err(|e| format!("persist failed: {e}"))?;
        Ok(())
    }

    async fn promote_target(&self, table_name: &str) -> Result<(), String> {
        if !self.is_next_target_ready(table_name).await {
            return Err("Next target not ready".to_string());
        }
        let mut s = self.state.write().await;
        if let Some(target) = s.tables.get_mut(table_name)
            && let Some(next) = target.next.take() {
                target.current = next;
                target.version += 1;
            }
        drop(s);
        self.save().await.map_err(|e| format!("persist failed: {e}"))?;
        Ok(())
    }

    async fn report_loaded(&self, executor_id: &str, shard_names: Vec<String>) {
        let mut readiness = self.readiness.write().await;
        readiness.insert(executor_id.to_string(), shard_names);
    }

    async fn is_next_target_ready(&self, table_name: &str) -> bool {
        let s = self.state.read().await;
        let next = match s.tables.get(table_name).and_then(|t| t.next.as_ref()) {
            Some(n) => n.clone(),
            None => return false,
        };
        drop(s);
        let readiness = self.readiness.read().await;
        for (shard_name, assigned_executors) in &next {
            let primary = match assigned_executors.first() {
                Some(p) => p,
                None => return false,
            };
            let is_loaded = readiness.get(primary)
                .map(|loaded| loaded.contains(shard_name))
                .unwrap_or(false);
            if !is_loaded {
                return false;
            }
        }
        true
    }

    async fn get_shard_routing(&self, table_name: &str) -> Vec<(String, String, Option<String>)> {
        let s = self.state.read().await;
        let mapping = match s.tables.get(table_name) {
            Some(t) => &t.current,
            None => return vec![],
        };
        mapping.iter().map(|(shard, executors)| {
            let primary = executors.first().cloned().unwrap_or_default();
            let secondary = executors.get(1).cloned();
            (shard.clone(), primary, secondary)
        }).collect()
    }

    async fn executors_for_table(&self, table_name: &str) -> Vec<String> {
        let s = self.state.read().await;
        let mapping = match s.tables.get(table_name) {
            Some(t) => &t.current,
            None => return vec![],
        };
        let mut executor_ids = Vec::new();
        for executors in mapping.values() {
            for eid in executors {
                if !executor_ids.contains(eid) {
                    executor_ids.push(eid.clone());
                }
            }
        }
        executor_ids
    }

    async fn register_executor(&self, executor_id: &str, host: &str, port: u16) {
        let mut s = self.state.write().await;
        let entry = (executor_id.to_string(), host.to_string(), port);
        if !s.executors.iter().any(|(id, _, _)| id == executor_id) {
            s.executors.push(entry);
        }
        drop(s);
        let _ = self.save().await;
    }

    async fn remove_executor(&self, executor_id: &str) {
        let mut s = self.state.write().await;
        s.executors.retain(|(id, _, _)| id != executor_id);
        drop(s);
        let _ = self.save().await;
    }

    async fn all_executors(&self) -> Vec<(String, String, u16)> {
        self.state.read().await.executors.clone()
    }

    async fn register_table(&self, table_name: &str, mapping: ShardMapping) {
        let mut s = self.state.write().await;
        s.tables.insert(table_name.to_string(), TargetState {
            current: mapping,
            next: None,
            version: 1,
        });
        s.version += 1;
        drop(s);
        let _ = self.save().await;
        info!("Registered table '{}' (persisted)", table_name);
    }

    async fn all_tables(&self) -> Vec<String> {
        self.state.read().await.tables.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn test_config() -> ClusterConfig {
        ClusterConfig {
            tables: vec![TableConfig {
                name: "products".to_string(),
                shards: vec![ShardConfig {
                    name: "products_shard_00".to_string(),
                    uri: "/tmp/test.lance".to_string(),
                    executors: vec!["w0".to_string()],
                }],
            }],
            executors: vec![ExecutorConfig {
                id: "w0".to_string(),
                host: "127.0.0.1".to_string(),
                port: 50100,
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_persistent_state_from_config() {
        let state = PersistentShardState::from_config("/tmp/nonexistent.json", &test_config()).await;
        let tables = state.all_tables().await;
        assert_eq!(tables, vec!["products"]);
        let routing = state.get_shard_routing("products").await;
        assert_eq!(routing.len(), 1);
        assert_eq!(routing[0].1, "w0");
    }

    #[tokio::test]
    async fn test_persistent_state_register_table() {
        let state = PersistentShardState::from_config("/tmp/nonexistent.json", &test_config()).await;
        let mut mapping = HashMap::new();
        mapping.insert("new_shard".to_string(), vec!["w0".to_string()]);
        state.register_table("new_table", mapping).await;
        let tables = state.all_tables().await;
        assert!(tables.contains(&"new_table".to_string()));
    }

    #[tokio::test]
    async fn test_persistent_state_save_load_local() {
        let path = "/tmp/lanceforge_test_metadata.json";
        let _ = tokio::fs::remove_file(path).await;

        let state = PersistentShardState::from_config(path, &test_config()).await;
        state.save().await.unwrap();

        // Verify file exists and is valid JSON
        let data = tokio::fs::read_to_string(path).await.unwrap();
        let snapshot: MetadataSnapshot = serde_json::from_str(&data).unwrap();
        assert!(snapshot.tables.contains_key("products"));
        assert_eq!(snapshot.executors.len(), 1);

        // Load into new state
        let state2 = PersistentShardState::from_config(path, &ClusterConfig::default()).await;
        state2.load().await.unwrap();
        let tables = state2.all_tables().await;
        assert!(tables.contains(&"products".to_string()));

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_persistent_state_survives_restart() {
        let path = "/tmp/lanceforge_restart_test.json";
        let _ = tokio::fs::remove_file(path).await;

        // Simulate first coordinator session
        {
            let state = PersistentShardState::from_config(path, &test_config()).await;
            let mut mapping = HashMap::new();
            mapping.insert("runtime_shard".to_string(), vec!["w0".to_string()]);
            state.register_table("runtime_table", mapping).await;
        }

        // Simulate restart with empty config — should recover from persisted file
        {
            let state = PersistentShardState::from_config(path, &ClusterConfig::default()).await;
            state.load().await.unwrap();
            let tables = state.all_tables().await;
            assert!(tables.contains(&"runtime_table".to_string()),
                "Runtime-created table should survive restart: {:?}", tables);
        }

        let _ = tokio::fs::remove_file(path).await;
    }

    // Two-phase rebalance: set_next_target → report_loaded → is_next_target_ready
    // → promote_target. This covers the critical shard-transition state machine.
    #[tokio::test]
    async fn test_next_target_lifecycle() {
        let path = format!("/tmp/lanceforge_next_target_{}.json", std::process::id());
        let _ = tokio::fs::remove_file(&path).await;

        let state = PersistentShardState::from_config(&path, &test_config()).await;

        // Stage 1: set a new target with a shard pinned to w1 (not loaded yet).
        let mut next = HashMap::new();
        next.insert("new_shard".to_string(), vec!["w1".to_string()]);
        state.set_next_target("products", next.clone()).await.unwrap();

        // Not ready yet — w1 hasn't reported the shard as loaded.
        assert!(!state.is_next_target_ready("products").await,
            "target must not be ready before report_loaded");

        // Reporting the wrong shard name keeps readiness false.
        state.report_loaded("w1", vec!["wrong_shard".to_string()]).await;
        assert!(!state.is_next_target_ready("products").await,
            "reporting a different shard name shouldn't flip readiness");

        // Stage 2: the correct report flips readiness.
        state.report_loaded("w1", vec!["new_shard".to_string()]).await;
        assert!(state.is_next_target_ready("products").await);

        // Stage 3: promote — current becomes next, version bumps.
        state.promote_target("products").await.unwrap();
        let routing = state.get_shard_routing("products").await;
        assert_eq!(routing.len(), 1);
        assert_eq!(routing[0].0, "new_shard");
        assert_eq!(routing[0].1, "w1");

        // Promoting again without a pending next is a no-op success path
        // (target.next was taken in the previous promote).
        assert!(!state.is_next_target_ready("products").await,
            "after promotion there is no next target → not ready");

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_promote_without_ready_next_fails() {
        let path = format!("/tmp/lanceforge_promote_fail_{}.json", std::process::id());
        let _ = tokio::fs::remove_file(&path).await;

        let state = PersistentShardState::from_config(&path, &test_config()).await;
        // No set_next_target → no pending next → promote must fail loudly
        // rather than silently "succeed" and leave stale routing.
        let err = state.promote_target("products").await;
        assert!(err.is_err(), "promote with no pending target must return Err");

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_set_next_target_on_unknown_table() {
        // Setting a next target for a table the state has never seen should
        // bootstrap that table with empty current + the next mapping, so a
        // subsequent promote creates the table cleanly.
        let path = format!("/tmp/lanceforge_new_table_next_{}.json", std::process::id());
        let _ = tokio::fs::remove_file(&path).await;

        let state = PersistentShardState::from_config(&path, &ClusterConfig::default()).await;
        let mut next = HashMap::new();
        next.insert("s0".to_string(), vec!["w0".to_string()]);
        state.set_next_target("brand_new", next).await.unwrap();

        state.report_loaded("w0", vec!["s0".to_string()]).await;
        assert!(state.is_next_target_ready("brand_new").await);
        state.promote_target("brand_new").await.unwrap();

        let tables = state.all_tables().await;
        assert!(tables.contains(&"brand_new".to_string()));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_executor_registration() {
        let path = format!("/tmp/lanceforge_exec_reg_{}.json", std::process::id());
        let _ = tokio::fs::remove_file(&path).await;

        let state = PersistentShardState::from_config(&path, &test_config()).await;

        // Pre-existing executor from config.
        let before = state.all_executors().await;
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].0, "w0");

        // Register new → list grows; register same id again → idempotent.
        state.register_executor("w1", "10.0.0.2", 50101).await;
        state.register_executor("w1", "10.0.0.2", 50101).await;
        let after_add = state.all_executors().await;
        assert_eq!(after_add.len(), 2, "duplicate register must be idempotent");

        // Remove one → list shrinks; removing unknown id is a no-op.
        state.remove_executor("w0").await;
        state.remove_executor("does_not_exist").await;
        let after_remove = state.all_executors().await;
        assert_eq!(after_remove.len(), 1);
        assert_eq!(after_remove[0].0, "w1");

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_executors_for_table_dedup() {
        // Two shards on the same executor should only list it once.
        let path = format!("/tmp/lanceforge_exec_dedup_{}.json", std::process::id());
        let _ = tokio::fs::remove_file(&path).await;

        let mut mapping = HashMap::new();
        mapping.insert("s0".to_string(), vec!["w0".to_string(), "w1".to_string()]);
        mapping.insert("s1".to_string(), vec!["w0".to_string()]);

        let state = PersistentShardState::from_config(&path, &ClusterConfig::default()).await;
        state.register_table("t", mapping).await;

        let execs = state.executors_for_table("t").await;
        assert_eq!(execs.len(), 2);
        assert!(execs.contains(&"w0".to_string()));
        assert!(execs.contains(&"w1".to_string()));

        // Unknown table returns empty (not a panic).
        assert!(state.executors_for_table("missing").await.is_empty());

        let _ = tokio::fs::remove_file(&path).await;
    }

    // S3 is documented as "not yet implemented" in the SimpleStore stub.
    // If someone ever flips the behavior, this test will fail loudly so they
    // also remember to update the documentation and error message.
    #[tokio::test]
    async fn test_s3_metadata_path_returns_clear_error() {
        let state = PersistentShardState::from_config("s3://bucket/meta.json",
            &ClusterConfig::default()).await;
        let err = state.load().await;
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("not yet implemented"),
            "S3 load path must surface the 'not yet implemented' marker, got: {msg}");
    }
}
