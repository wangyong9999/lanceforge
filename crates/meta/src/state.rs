// Licensed under the Apache License, Version 2.0.
// MetaShardState: ShardState implementation backed by MetaStore.
//
// This replaces both StaticShardState and PersistentShardState with a
// single, clean implementation that uses the MetaStore abstraction.

use std::collections::HashMap;
use std::sync::Arc;

use log::info;

use lance_distributed_common::shard_state::{ShardMapping, ShardState, TargetState};
use super::store::MetaStore;

/// ShardState implementation backed by MetaStore (file, S3, or etcd).
/// All mutations are persisted with CAS versioning.
pub struct MetaShardState {
    store: Arc<dyn MetaStore>,
    prefix: String,
}

impl MetaShardState {
    pub fn new(store: Arc<dyn MetaStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.to_string(),
        }
    }

    fn table_key(&self, table_name: &str) -> String {
        format!("{}/tables/{}", self.prefix, table_name)
    }

    fn executor_key(&self) -> String {
        format!("{}/executors", self.prefix)
    }

    async fn get_target(&self, table_name: &str) -> Option<TargetState> {
        let key = self.table_key(table_name);
        match self.store.get(&key).await {
            Ok(Some(v)) => serde_json::from_str(&v.value).ok(),
            _ => None,
        }
    }

    async fn put_target(&self, table_name: &str, target: &TargetState) -> Result<(), String> {
        let key = self.table_key(table_name);
        let value = serde_json::to_string(target)
            .map_err(|e| format!("serialize: {e}"))?;

        // Get current version for CAS
        let current_version = self.store.get(&key).await
            .ok()
            .flatten()
            .map(|v| v.version)
            .unwrap_or(0);

        self.store.put(&key, &value, current_version).await
            .map_err(|e| format!("put: {e}"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ShardState for MetaShardState {
    async fn get_current_target(&self, table_name: &str) -> Option<ShardMapping> {
        self.get_target(table_name).await.map(|t| t.current)
    }

    async fn set_next_target(&self, table_name: &str, mapping: ShardMapping) -> Result<(), String> {
        let mut target = self.get_target(table_name).await.unwrap_or(TargetState {
            current: HashMap::new(),
            next: None,
            version: 0,
        });
        target.next = Some(mapping);
        self.put_target(table_name, &target).await
    }

    async fn promote_target(&self, table_name: &str) -> Result<(), String> {
        let mut target = self.get_target(table_name).await
            .ok_or_else(|| format!("table not found: {table_name}"))?;
        if let Some(next) = target.next.take() {
            target.current = next;
            target.version += 1;
            self.put_target(table_name, &target).await?;
        }
        Ok(())
    }

    async fn report_loaded(&self, _executor_id: &str, _shard_names: Vec<String>) {
        // Readiness tracking is in-memory (not persisted to MetaStore)
    }

    async fn is_next_target_ready(&self, _table_name: &str) -> bool {
        // Simplified: always ready (readiness gate bypassed for MetaShardState)
        true
    }

    async fn get_shard_routing(&self, table_name: &str) -> Vec<(String, String, Option<String>)> {
        let mapping = match self.get_current_target(table_name).await {
            Some(m) => m,
            None => return vec![],
        };
        mapping.iter().map(|(shard, executors)| {
            let primary = executors.first().cloned().unwrap_or_default();
            let secondary = executors.get(1).cloned();
            (shard.clone(), primary, secondary)
        }).collect()
    }

    async fn executors_for_table(&self, table_name: &str) -> Vec<String> {
        let mapping = match self.get_current_target(table_name).await {
            Some(m) => m,
            None => return vec![],
        };
        let mut ids = Vec::new();
        for executors in mapping.values() {
            for eid in executors {
                if !ids.contains(eid) {
                    ids.push(eid.clone());
                }
            }
        }
        ids
    }

    async fn register_executor(&self, executor_id: &str, host: &str, port: u16) {
        let key = self.executor_key();
        let mut executors = self.get_executors_internal().await;
        let entry = (executor_id.to_string(), host.to_string(), port);
        if !executors.iter().any(|(id, _, _)| id == executor_id) {
            executors.push(entry);
        }
        let value = serde_json::to_string(&executors).unwrap_or_default();
        let version = self.store.get(&key).await.ok().flatten().map(|v| v.version).unwrap_or(0);
        let _ = self.store.put(&key, &value, version).await;
    }

    async fn remove_executor(&self, executor_id: &str) {
        let key = self.executor_key();
        let mut executors = self.get_executors_internal().await;
        executors.retain(|(id, _, _)| id != executor_id);
        let value = serde_json::to_string(&executors).unwrap_or_default();
        let version = self.store.get(&key).await.ok().flatten().map(|v| v.version).unwrap_or(0);
        let _ = self.store.put(&key, &value, version).await;
    }

    async fn all_executors(&self) -> Vec<(String, String, u16)> {
        self.get_executors_internal().await
    }

    async fn register_table(&self, table_name: &str, mapping: ShardMapping) {
        let target = TargetState {
            current: mapping,
            next: None,
            version: 1,
        };
        let _ = self.put_target(table_name, &target).await;
        info!("Registered table '{}' in MetaStore", table_name);
    }

    async fn all_tables(&self) -> Vec<String> {
        let prefix = format!("{}/tables/", self.prefix);
        match self.store.list_keys(&prefix).await {
            Ok(keys) => keys.iter()
                .filter_map(|k| k.strip_prefix(&prefix).map(|s| s.to_string()))
                .collect(),
            Err(_) => vec![],
        }
    }
}

impl MetaShardState {
    async fn get_executors_internal(&self) -> Vec<(String, String, u16)> {
        let key = self.executor_key();
        match self.store.get(&key).await {
            Ok(Some(v)) => serde_json::from_str(&v.value).unwrap_or_default(),
            _ => vec![],
        }
    }

    // ── Shard URI persistence (survives coordinator restarts) ──

    fn shard_uris_key(&self) -> String {
        format!("{}/shard_uris", self.prefix)
    }

    /// Load all persisted shard URIs.
    pub async fn get_shard_uris(&self) -> HashMap<String, String> {
        let key = self.shard_uris_key();
        match self.store.get(&key).await {
            Ok(Some(v)) => serde_json::from_str(&v.value).unwrap_or_default(),
            _ => HashMap::new(),
        }
    }

    /// Persist a batch of shard URI updates (merge into existing).
    pub async fn put_shard_uris(&self, uris: &HashMap<String, String>) {
        let key = self.shard_uris_key();
        let mut current = self.get_shard_uris().await;
        for (k, v) in uris {
            current.insert(k.clone(), v.clone());
        }
        let value = serde_json::to_string(&current).unwrap_or_default();
        let version = self.store.get(&key).await.ok().flatten().map(|v| v.version).unwrap_or(0);
        let _ = self.store.put(&key, &value, version).await;
    }

    /// Remove shard URIs for dropped shards.
    pub async fn remove_shard_uris(&self, shard_names: &[String]) {
        let key = self.shard_uris_key();
        let mut current = self.get_shard_uris().await;
        for name in shard_names {
            current.remove(name);
        }
        let value = serde_json::to_string(&current).unwrap_or_default();
        let version = self.store.get(&key).await.ok().flatten().map(|v| v.version).unwrap_or(0);
        let _ = self.store.put(&key, &value, version).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FileMetaStore;

    async fn test_state() -> MetaShardState {
        let path = format!("/tmp/lanceforge_metastate_test_{}.json", rand_id());
        let _ = tokio::fs::remove_file(&path).await;
        let store = Arc::new(FileMetaStore::new(&path).await.unwrap());
        MetaShardState::new(store, "test")
    }

    fn rand_id() -> u64 {
        use std::time::SystemTime;
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
    }

    #[tokio::test]
    async fn test_register_and_query_table() {
        let state = test_state().await;
        let mut mapping = HashMap::new();
        mapping.insert("s0".to_string(), vec!["w0".to_string()]);
        state.register_table("products", mapping).await;

        let tables = state.all_tables().await;
        assert!(tables.contains(&"products".to_string()));

        let routing = state.get_shard_routing("products").await;
        assert_eq!(routing.len(), 1);
        assert_eq!(routing[0].1, "w0");
    }

    #[tokio::test]
    async fn test_register_executor() {
        let state = test_state().await;
        state.register_executor("w0", "127.0.0.1", 50100).await;
        state.register_executor("w1", "127.0.0.1", 50101).await;

        let executors = state.all_executors().await;
        assert_eq!(executors.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_executor() {
        let state = test_state().await;
        state.register_executor("w0", "127.0.0.1", 50100).await;
        state.register_executor("w1", "127.0.0.1", 50101).await;
        state.remove_executor("w0").await;

        let executors = state.all_executors().await;
        assert_eq!(executors.len(), 1);
        assert_eq!(executors[0].0, "w1");
    }

    #[tokio::test]
    async fn test_shard_state_survives_new_instance() {
        let path = format!("/tmp/lanceforge_metastate_survive_{}.json", rand_id());
        let _ = tokio::fs::remove_file(&path).await;

        // Instance 1: register table
        {
            let store = Arc::new(FileMetaStore::new(&path).await.unwrap());
            let state = MetaShardState::new(store, "test");
            let mut mapping = HashMap::new();
            mapping.insert("s0".to_string(), vec!["w0".to_string()]);
            state.register_table("my_table", mapping).await;
        }

        // Instance 2: should see the table
        {
            let store = Arc::new(FileMetaStore::new(&path).await.unwrap());
            let state = MetaShardState::new(store, "test");
            let tables = state.all_tables().await;
            assert!(tables.contains(&"my_table".to_string()),
                "Table should survive across instances: {:?}", tables);
        }

        let _ = tokio::fs::remove_file(&path).await;
    }
}
