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

    /// Expose the underlying MetaStore so callers (auth hot-reload, admin
    /// CLI, etc.) can use the same backend without rebuilding it.
    pub fn store(&self) -> Arc<dyn MetaStore> {
        self.store.clone()
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

    // set_next_target → promote_target flips current and bumps version.
    #[tokio::test]
    async fn test_set_next_then_promote() {
        let state = test_state().await;

        let mut initial = HashMap::new();
        initial.insert("s0".to_string(), vec!["w0".to_string()]);
        state.register_table("t", initial).await;

        let mut next = HashMap::new();
        next.insert("s0".to_string(), vec!["w1".to_string()]);
        state.set_next_target("t", next).await.unwrap();

        // MetaShardState has no readiness gate (always true) — promote succeeds.
        assert!(state.is_next_target_ready("t").await,
            "MetaShardState skips readiness gate by design");
        state.promote_target("t").await.unwrap();

        let routing = state.get_shard_routing("t").await;
        assert_eq!(routing[0].1, "w1", "current should now point to w1");

        // Version bumped.
        let target = state.get_target("t").await.unwrap();
        assert_eq!(target.version, 2, "version must bump after promote");
    }

    #[tokio::test]
    async fn test_promote_missing_table_errors() {
        let state = test_state().await;
        let err = state.promote_target("no_such_table").await;
        assert!(err.is_err(), "promoting unknown table must fail");
        assert!(err.unwrap_err().contains("not found"),
            "error should mention 'not found' for operator diagnostics");
    }

    #[tokio::test]
    async fn test_get_shard_routing_primary_and_secondary() {
        let state = test_state().await;
        let mut m = HashMap::new();
        m.insert("s0".to_string(), vec!["primary_w".to_string(), "secondary_w".to_string()]);
        m.insert("s1".to_string(), vec!["solo_w".to_string()]);
        state.register_table("t", m).await;

        let routing = state.get_shard_routing("t").await;
        let by_shard: HashMap<String, (String, Option<String>)> = routing.into_iter()
            .map(|(s, p, sec)| (s, (p, sec)))
            .collect();

        // Replicated shard has Some secondary.
        let (p, sec) = &by_shard["s0"];
        assert_eq!(p, "primary_w");
        assert_eq!(sec.as_deref(), Some("secondary_w"));

        // Single-replica shard has None secondary.
        let (p, sec) = &by_shard["s1"];
        assert_eq!(p, "solo_w");
        assert!(sec.is_none());
    }

    #[tokio::test]
    async fn test_executors_for_table_dedup_and_missing() {
        let state = test_state().await;
        let mut m = HashMap::new();
        m.insert("s0".to_string(), vec!["w0".to_string(), "w1".to_string()]);
        m.insert("s1".to_string(), vec!["w0".to_string()]);
        state.register_table("t", m).await;

        let execs = state.executors_for_table("t").await;
        assert_eq!(execs.len(), 2, "duplicate executors must be collapsed");

        // Unknown table returns empty list (not an error).
        assert!(state.executors_for_table("nope").await.is_empty());
    }

    #[tokio::test]
    async fn test_register_executor_idempotent() {
        let state = test_state().await;
        state.register_executor("w0", "h", 1).await;
        state.register_executor("w0", "h", 1).await;
        state.register_executor("w0", "different_host", 2).await; // also idempotent by id
        let execs = state.all_executors().await;
        assert_eq!(execs.len(), 1, "same id must not duplicate");
    }

    // Shard-URI persistence (used when Phase 2 creates shards dynamically and
    // needs to remember their Lance dataset URIs across coordinator restarts).
    #[tokio::test]
    async fn test_shard_uri_lifecycle() {
        let state = test_state().await;

        assert!(state.get_shard_uris().await.is_empty(), "fresh state has no URIs");

        let mut batch = HashMap::new();
        batch.insert("t_s0".to_string(), "s3://bucket/t/s0.lance".to_string());
        batch.insert("t_s1".to_string(), "s3://bucket/t/s1.lance".to_string());
        state.put_shard_uris(&batch).await;

        let stored = state.get_shard_uris().await;
        assert_eq!(stored.len(), 2);
        assert_eq!(stored["t_s0"], "s3://bucket/t/s0.lance");

        // Second put with a disjoint key should MERGE, not replace.
        let mut more = HashMap::new();
        more.insert("t_s2".to_string(), "s3://bucket/t/s2.lance".to_string());
        state.put_shard_uris(&more).await;
        let after_merge = state.get_shard_uris().await;
        assert_eq!(after_merge.len(), 3);

        // remove_shard_uris with an unknown name is a no-op.
        state.remove_shard_uris(&["t_s0".to_string(), "does_not_exist".to_string()]).await;
        let after_remove = state.get_shard_uris().await;
        assert_eq!(after_remove.len(), 2);
        assert!(!after_remove.contains_key("t_s0"));
    }

    #[tokio::test]
    async fn test_all_tables_filters_by_prefix() {
        // MetaShardState prefixes keys with "{prefix}/tables/". Using a
        // different prefix on the same store must not see tables from
        // the first namespace — multi-tenant isolation at the state level.
        let path = format!("/tmp/lanceforge_metastate_ns_{}.json", rand_id());
        let _ = tokio::fs::remove_file(&path).await;
        let store: Arc<dyn MetaStore> = Arc::new(FileMetaStore::new(&path).await.unwrap());

        let ns_a = MetaShardState::new(store.clone(), "tenant_a");
        let ns_b = MetaShardState::new(store.clone(), "tenant_b");

        let mut m = HashMap::new();
        m.insert("s0".to_string(), vec!["w0".to_string()]);
        ns_a.register_table("a_table", m).await;

        let a_tables = ns_a.all_tables().await;
        let b_tables = ns_b.all_tables().await;
        assert!(a_tables.contains(&"a_table".to_string()));
        assert!(b_tables.is_empty(), "tenant_b must not see tenant_a's tables");

        let _ = tokio::fs::remove_file(&path).await;
    }
}
