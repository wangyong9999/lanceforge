// Licensed under the Apache License, Version 2.0.
// MetaStore: versioned key-value store for cluster metadata.
//
// All writes use Compare-And-Swap (CAS) via version numbers to prevent
// concurrent coordinator conflicts. Inspired by Milvus etcd metastore
// and Qdrant Persistent state.

use std::collections::HashMap;
use std::path::Path;

use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

/// Versioned value stored in MetaStore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Versioned<T> {
    pub value: T,
    pub version: u64,
}

/// Error type for MetaStore operations.
#[derive(Debug)]
pub enum MetaError {
    /// CAS conflict: expected version N but found M.
    VersionConflict { expected: u64, actual: u64 },
    /// Key not found.
    NotFound(String),
    /// Storage I/O error.
    StorageError(String),
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaError::VersionConflict { expected, actual } =>
                write!(f, "version conflict: expected {expected}, actual {actual}"),
            MetaError::NotFound(key) => write!(f, "key not found: {key}"),
            MetaError::StorageError(msg) => write!(f, "storage error: {msg}"),
        }
    }
}

impl std::error::Error for MetaError {}

/// MetaStore trait: versioned KV with CAS semantics.
///
/// All implementations must guarantee:
/// 1. `put` only succeeds if current version matches `expected_version`
/// 2. `version` is monotonically increasing per key
/// 3. Reads are consistent (may be stale by sync_interval for cached impls)
#[async_trait::async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Get value by key. Returns None if not found.
    async fn get(&self, key: &str) -> Result<Option<Versioned<String>>, MetaError>;

    /// Put value with CAS. `expected_version` = 0 means create-if-not-exists.
    /// Returns the new version on success.
    async fn put(&self, key: &str, value: &str, expected_version: u64) -> Result<u64, MetaError>;

    /// Delete key. Returns Ok(()) even if key doesn't exist.
    async fn delete(&self, key: &str) -> Result<(), MetaError>;

    /// List all keys with a given prefix.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, MetaError>;

    /// Get all key-value pairs with a given prefix.
    async fn get_prefix(&self, prefix: &str) -> Result<HashMap<String, Versioned<String>>, MetaError>;
}

// ════════════════════════════════════════════
// FileMetaStore: local filesystem implementation
// ════════════════════════════════════════════

/// MetaStore backed by a single JSON file with in-memory cache.
/// Suitable for single-coordinator or development deployments.
/// For multi-coordinator, use S3MetaStore or etcd.
pub struct FileMetaStore {
    path: String,
    data: tokio::sync::RwLock<StoreData>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoreData {
    entries: HashMap<String, Versioned<String>>,
    global_version: u64,
}

impl FileMetaStore {
    pub async fn new(path: &str) -> Result<Self, MetaError> {
        let data = if Path::new(path).exists() {
            let content = tokio::fs::read_to_string(path).await
                .map_err(|e| MetaError::StorageError(format!("read {path}: {e}")))?;
            serde_json::from_str(&content)
                .map_err(|e| MetaError::StorageError(format!("parse {path}: {e}")))?
        } else {
            StoreData::default()
        };
        info!("FileMetaStore: {} ({} keys)", path, data.entries.len());
        Ok(Self {
            path: path.to_string(),
            data: tokio::sync::RwLock::new(data),
        })
    }

    async fn persist(&self) -> Result<(), MetaError> {
        let data = self.data.read().await;
        let json = serde_json::to_string_pretty(&*data)
            .map_err(|e| MetaError::StorageError(format!("serialize: {e}")))?;
        drop(data);

        if let Some(parent) = Path::new(&self.path).parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        // Atomic write: write to temp file then rename
        let tmp = format!("{}.tmp", self.path);
        tokio::fs::write(&tmp, &json).await
            .map_err(|e| MetaError::StorageError(format!("write {tmp}: {e}")))?;
        tokio::fs::rename(&tmp, &self.path).await
            .map_err(|e| MetaError::StorageError(format!("rename: {e}")))?;
        debug!("FileMetaStore: persisted to {}", self.path);
        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaStore for FileMetaStore {
    async fn get(&self, key: &str) -> Result<Option<Versioned<String>>, MetaError> {
        let data = self.data.read().await;
        Ok(data.entries.get(key).cloned())
    }

    async fn put(&self, key: &str, value: &str, expected_version: u64) -> Result<u64, MetaError> {
        let mut data = self.data.write().await;
        let current_version = data.entries.get(key).map(|v| v.version).unwrap_or(0);

        if expected_version != current_version {
            return Err(MetaError::VersionConflict {
                expected: expected_version,
                actual: current_version,
            });
        }

        data.global_version += 1;
        let new_version = data.global_version;
        data.entries.insert(key.to_string(), Versioned {
            value: value.to_string(),
            version: new_version,
        });
        drop(data);

        self.persist().await?;
        Ok(new_version)
    }

    async fn delete(&self, key: &str) -> Result<(), MetaError> {
        let mut data = self.data.write().await;
        data.entries.remove(key);
        drop(data);
        self.persist().await?;
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, MetaError> {
        let data = self.data.read().await;
        Ok(data.entries.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn get_prefix(&self, prefix: &str) -> Result<HashMap<String, Versioned<String>>, MetaError> {
        let data = self.data.read().await;
        Ok(data.entries.iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_store_crud() {
        let path = "/tmp/lanceforge_meta_test_crud.json";
        let _ = tokio::fs::remove_file(path).await;

        let store = FileMetaStore::new(path).await.unwrap();

        // Create
        let v1 = store.put("tables/products", r#"{"shards":["s0"]}"#, 0).await.unwrap();
        assert!(v1 > 0);

        // Read
        let entry = store.get("tables/products").await.unwrap().unwrap();
        assert_eq!(entry.version, v1);
        assert!(entry.value.contains("s0"));

        // Update with CAS
        let v2 = store.put("tables/products", r#"{"shards":["s0","s1"]}"#, v1).await.unwrap();
        assert!(v2 > v1);

        // CAS conflict
        let err = store.put("tables/products", "bad", v1).await;
        assert!(matches!(err, Err(MetaError::VersionConflict { .. })));

        // Delete
        store.delete("tables/products").await.unwrap();
        assert!(store.get("tables/products").await.unwrap().is_none());

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_file_store_prefix() {
        let path = "/tmp/lanceforge_meta_test_prefix.json";
        let _ = tokio::fs::remove_file(path).await;

        let store = FileMetaStore::new(path).await.unwrap();
        store.put("tables/a", "1", 0).await.unwrap();
        store.put("tables/b", "2", 0).await.unwrap();
        store.put("sessions/w0", "alive", 0).await.unwrap();

        let keys = store.list_keys("tables/").await.unwrap();
        assert_eq!(keys.len(), 2);

        let entries = store.get_prefix("tables/").await.unwrap();
        assert_eq!(entries.len(), 2);

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_file_store_persistence() {
        let path = "/tmp/lanceforge_meta_test_persist.json";
        let _ = tokio::fs::remove_file(path).await;

        // Write
        {
            let store = FileMetaStore::new(path).await.unwrap();
            store.put("key", "value", 0).await.unwrap();
        }

        // Read after "restart"
        {
            let store = FileMetaStore::new(path).await.unwrap();
            let entry = store.get("key").await.unwrap().unwrap();
            assert_eq!(entry.value, "value");
        }

        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_cas_create_if_not_exists() {
        let path = "/tmp/lanceforge_meta_test_cas.json";
        let _ = tokio::fs::remove_file(path).await;

        let store = FileMetaStore::new(path).await.unwrap();

        // First create: expected_version=0 → success
        store.put("new_key", "v1", 0).await.unwrap();

        // Second create: expected_version=0 → conflict (already exists)
        let err = store.put("new_key", "v2", 0).await;
        assert!(matches!(err, Err(MetaError::VersionConflict { .. })));

        let _ = tokio::fs::remove_file(path).await;
    }
}
