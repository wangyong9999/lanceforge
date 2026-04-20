// Licensed under the Apache License, Version 2.0.
// MetaStore: versioned key-value store for cluster metadata.
//
// All writes use Compare-And-Swap (CAS) via version numbers to prevent
// concurrent coordinator conflicts. Inspired by Milvus etcd metastore
// and Qdrant Persistent state.

use std::collections::HashMap;
use std::path::Path;

use log::{debug, info};
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

/// Current on-disk / on-OBS schema version for the MetaStore snapshot.
///
/// ROADMAP_0.2 §B3.2. Versioning rules:
///   - 0 = implicit pre-0.2 snapshot (no `schema_version` field in JSON).
///         Deserializes to `StoreData::default()` schema_version via serde
///         default, then gets stamped to `CURRENT_SCHEMA_VERSION` on the
///         next successful write. Reads keep working.
///   - 1 = 0.2-alpha baseline. Same shape as 0 but explicit.
///   - Future bumps MUST land with a migration in `migrate_snapshot` and
///     be documented in `docs/COMPAT_POLICY.md` (B3.5).
///
/// A snapshot with a version strictly greater than `CURRENT_SCHEMA_VERSION`
/// is rejected at load time — we cannot safely read a format authored by a
/// newer server without forward-compat proof.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

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
///
/// At construction, an exclusive advisory lock is taken on `<path>.lock`.
/// This fails fast if another process already holds the lock, preventing
/// split-brain when operators accidentally share a filesystem backend
/// (e.g. via NFS) across multiple coordinator instances.
pub struct FileMetaStore {
    path: String,
    data: tokio::sync::RwLock<StoreData>,
    /// Lock file handle. Kept alive for process lifetime; dropping releases the lock.
    #[allow(dead_code)]
    lock_file: std::fs::File,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoreData {
    entries: HashMap<String, Versioned<String>>,
    global_version: u64,
    /// On-disk schema version. Missing in pre-0.2 snapshots → deserializes
    /// to 0 via serde default; gets stamped on the next write.
    #[serde(default)]
    schema_version: u32,
}

impl StoreData {
    /// Validate a freshly-loaded snapshot's schema_version. Called after
    /// JSON deserialization but before the store becomes available for
    /// reads. Returns Err for forward-incompatible snapshots, Ok for
    /// current or upgrade-on-next-write-compatible snapshots.
    fn validate_schema_version(&self, source: &str) -> Result<(), MetaError> {
        if self.schema_version > CURRENT_SCHEMA_VERSION {
            return Err(MetaError::StorageError(format!(
                "{source}: snapshot schema_version {} is newer than this server \
                 supports ({}). Upgrade the server or use `lance-admin migrate` \
                 to author a downgraded snapshot.",
                self.schema_version, CURRENT_SCHEMA_VERSION
            )));
        }
        if self.schema_version < CURRENT_SCHEMA_VERSION {
            log::info!(
                "{source}: loaded schema_version {} (< current {}), will be \
                 upgraded in place on next write",
                self.schema_version, CURRENT_SCHEMA_VERSION
            );
        }
        Ok(())
    }
}

impl FileMetaStore {
    pub async fn new(path: &str) -> Result<Self, MetaError> {
        // Acquire cross-process advisory lock BEFORE reading the data file,
        // so two coordinators cannot race on the initial load.
        if let Some(parent) = Path::new(path).parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        let lock_path = format!("{path}.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| MetaError::StorageError(format!("open lock {lock_path}: {e}")))?;
        use fs4::FileExt;
        lock_file.try_lock_exclusive()
            .map_err(|e| MetaError::StorageError(format!(
                "FileMetaStore at {path} is already locked by another process — \
                 use S3MetaStore or etcd for multi-coordinator deployments ({e})"
            )))?;

        let data: StoreData = if Path::new(path).exists() {
            let content = tokio::fs::read_to_string(path).await
                .map_err(|e| MetaError::StorageError(format!("read {path}: {e}")))?;
            let parsed: StoreData = serde_json::from_str(&content)
                .map_err(|e| MetaError::StorageError(format!("parse {path}: {e}")))?;
            parsed.validate_schema_version(&format!("FileMetaStore({path})"))?;
            parsed
        } else {
            StoreData { schema_version: CURRENT_SCHEMA_VERSION, ..Default::default() }
        };
        info!("FileMetaStore: {} ({} keys, schema_v{}) [locked {lock_path}]",
            path, data.entries.len(), data.schema_version);
        Ok(Self {
            path: path.to_string(),
            data: tokio::sync::RwLock::new(data),
            lock_file,
        })
    }

    async fn persist(&self) -> Result<(), MetaError> {
        // Stamp current schema_version on every persist so pre-0.2 snapshots
        // get upgraded in place on the next write. Cheap — it's one u32 field.
        {
            let mut data = self.data.write().await;
            if data.schema_version != CURRENT_SCHEMA_VERSION {
                data.schema_version = CURRENT_SCHEMA_VERSION;
            }
        }
        let data = self.data.read().await;
        let json = serde_json::to_string_pretty(&*data)
            .map_err(|e| MetaError::StorageError(format!("serialize: {e}")))?;
        drop(data);

        if let Some(parent) = Path::new(&self.path).parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        // Durable atomic write: write temp → fsync file → rename → fsync parent dir
        let tmp = format!("{}.tmp", self.path);
        {
            let mut f = tokio::fs::File::create(&tmp).await
                .map_err(|e| MetaError::StorageError(format!("create {tmp}: {e}")))?;
            use tokio::io::AsyncWriteExt;
            f.write_all(json.as_bytes()).await
                .map_err(|e| MetaError::StorageError(format!("write {tmp}: {e}")))?;
            f.sync_all().await
                .map_err(|e| MetaError::StorageError(format!("fsync {tmp}: {e}")))?;
        }
        tokio::fs::rename(&tmp, &self.path).await
            .map_err(|e| MetaError::StorageError(format!("rename: {e}")))?;
        // fsync parent dir to persist the rename itself
        if let Some(parent) = Path::new(&self.path).parent()
            && let Ok(dir) = tokio::fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        debug!("FileMetaStore: persisted to {} (fsync'd)", self.path);
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

// ════════════════════════════════════════════
// S3MetaStore: S3-compatible object storage backend
// ════════════════════════════════════════════

/// MetaStore backed by S3-compatible storage (S3, MinIO, GCS, Azure).
///
/// Stores all metadata as a single JSON object. CAS is implemented via
/// `object_store::PutMode::Update(UpdateVersion)`, which maps to
/// S3 conditional PutObject (If-Match ETag) for conflict detection.
///
/// Suitable for multi-coordinator deployments:
/// - All coordinators read/write the same S3 object
/// - CAS prevents concurrent writes from clobbering each other
/// - Reads may be slightly stale (S3 eventual consistency)
pub struct S3MetaStore {
    store: Box<dyn object_store::ObjectStore>,
    path: object_store::path::Path,
    data: tokio::sync::RwLock<StoreData>,
    /// Last known ETag for conditional PUT.
    etag: tokio::sync::RwLock<Option<object_store::UpdateVersion>>,
}

impl S3MetaStore {
    /// Create a new S3MetaStore at the given URI.
    ///
    /// URI format: `s3://bucket/path/metadata.json`
    /// Options: AWS credentials, endpoint, region, etc.
    pub async fn new(
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, MetaError> {
        let parsed = url::Url::parse(uri)
            .map_err(|e| MetaError::StorageError(format!("invalid URI {uri}: {e}")))?;

        let (store, path) = object_store::parse_url_opts(&parsed, options)
            .map_err(|e| MetaError::StorageError(format!("create store for {uri}: {e}")))?;

        // Try to load existing data
        let (data, etag) = match store.get(&path).await {
            Ok(result) => {
                let update_version = result.meta.e_tag.clone()
                    .map(|t| object_store::UpdateVersion { e_tag: Some(t), version: None });
                let bytes = result.bytes().await
                    .map_err(|e| MetaError::StorageError(format!("read {uri}: {e}")))?;
                let store_data: StoreData = serde_json::from_slice(&bytes)
                    .map_err(|e| MetaError::StorageError(format!("parse {uri}: {e}")))?;
                store_data.validate_schema_version(&format!("S3MetaStore({uri})"))?;
                info!("S3MetaStore: {} ({} keys, schema_v{})",
                    uri, store_data.entries.len(), store_data.schema_version);
                (store_data, update_version)
            }
            Err(object_store::Error::NotFound { .. }) => {
                info!("S3MetaStore: {} (new, 0 keys, schema_v{})", uri, CURRENT_SCHEMA_VERSION);
                (StoreData { schema_version: CURRENT_SCHEMA_VERSION, ..Default::default() }, None)
            }
            Err(e) => return Err(MetaError::StorageError(format!("get {uri}: {e}"))),
        };

        Ok(Self {
            store: Box::new(store),
            path,
            data: tokio::sync::RwLock::new(data),
            etag: tokio::sync::RwLock::new(etag),
        })
    }

    /// Persist current state to S3 with conditional PUT.
    async fn persist(&self) -> Result<(), MetaError> {
        // Stamp current schema_version on every persist so pre-0.2 snapshots
        // get upgraded in place on the next write. Cheap — it's one u32 field.
        {
            let mut data = self.data.write().await;
            if data.schema_version != CURRENT_SCHEMA_VERSION {
                data.schema_version = CURRENT_SCHEMA_VERSION;
            }
        }
        let data = self.data.read().await;
        let json = serde_json::to_string_pretty(&*data)
            .map_err(|e| MetaError::StorageError(format!("serialize: {e}")))?;
        drop(data);

        let bytes = object_store::PutPayload::from(json.into_bytes());
        let etag = self.etag.read().await;

        let put_opts = if let Some(ref uv) = *etag {
            // Conditional put: only succeed if ETag matches (CAS at S3 level)
            object_store::PutOptions {
                mode: object_store::PutMode::Update(uv.clone()),
                ..Default::default()
            }
        } else {
            // First write: create-if-not-exists
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                ..Default::default()
            }
        };
        drop(etag);

        // Try conditional PUT. If the backend doesn't support it (e.g., local FS),
        // fall back to unconditional overwrite. CAS is still enforced at the
        // application level via version checks in put().
        let result = match self.store.put_opts(&self.path, bytes.clone(), put_opts).await {
            Ok(r) => r,
            Err(object_store::Error::NotImplemented) |
            Err(object_store::Error::NotSupported { .. }) => {
                // Fallback: unconditional overwrite
                self.store.put(&self.path, bytes).await
                    .map_err(|e| MetaError::StorageError(format!("S3 put: {e}")))?
            }
            Err(object_store::Error::AlreadyExists { .. }) |
            Err(object_store::Error::Precondition { .. }) => {
                return Err(MetaError::StorageError("S3 CAS conflict — retry after refresh".to_string()));
            }
            Err(e) => return Err(MetaError::StorageError(format!("S3 put: {e}"))),
        };

        // Update our ETag for next conditional PUT
        let new_etag = result.e_tag.map(|t| object_store::UpdateVersion { e_tag: Some(t), version: None });
        *self.etag.write().await = new_etag;

        debug!("S3MetaStore: persisted to {}", self.path);
        Ok(())
    }

    /// Refresh local cache from S3 (call before reads in multi-coordinator setup).
    pub async fn refresh(&self) -> Result<(), MetaError> {
        match self.store.get(&self.path).await {
            Ok(result) => {
                let update_version = result.meta.e_tag.clone()
                    .map(|t| object_store::UpdateVersion { e_tag: Some(t), version: None });
                let bytes = result.bytes().await
                    .map_err(|e| MetaError::StorageError(format!("read: {e}")))?;
                let store_data: StoreData = serde_json::from_slice(&bytes)
                    .map_err(|e| MetaError::StorageError(format!("parse: {e}")))?;
                store_data.validate_schema_version("S3MetaStore::refresh")?;
                *self.data.write().await = store_data;
                *self.etag.write().await = update_version;
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(MetaError::StorageError(format!("refresh: {e}"))),
        }
    }
}

#[async_trait::async_trait]
impl MetaStore for S3MetaStore {
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

    /// Shared helper (H23): every FileMetaStore test gets its own tempdir,
    /// wiped on Drop. No more hard-coded `/tmp/lanceforge_meta_test_*.json`
    /// colliding across parallel cargo test runs, retries, or crash leftovers.
    /// Returns (tempdir_guard, absolute_path_to_file).
    fn unique_meta_path() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("meta.json").to_string_lossy().to_string();
        (dir, path)
    }

    // ── S3MetaStore tests (using local filesystem via file:// URLs) ──

    #[tokio::test]
    async fn test_s3_store_crud() {
        let dir = format!("/tmp/lanceforge_s3meta_test_{}", std::process::id());
        let _ = tokio::fs::create_dir_all(&dir).await;
        let uri = format!("file://{}/metadata.json", dir);

        let store = S3MetaStore::new(&uri, std::iter::empty::<(String, String)>()).await.unwrap();

        // Create
        let v1 = store.put("tables/products", r#"{"shards":["s0"]}"#, 0).await.unwrap();
        assert!(v1 > 0);

        // Read
        let entry = store.get("tables/products").await.unwrap().unwrap();
        assert_eq!(entry.version, v1);

        // Update with CAS
        let v2 = store.put("tables/products", r#"{"shards":["s0","s1"]}"#, v1).await.unwrap();
        assert!(v2 > v1);

        // CAS conflict
        let err = store.put("tables/products", "bad", v1).await;
        assert!(matches!(err, Err(MetaError::VersionConflict { .. })));

        // Delete
        store.delete("tables/products").await.unwrap();
        assert!(store.get("tables/products").await.unwrap().is_none());

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_s3_store_persistence() {
        let dir = format!("/tmp/lanceforge_s3meta_persist_{}", std::process::id());
        let _ = tokio::fs::create_dir_all(&dir).await;
        let uri = format!("file://{}/metadata.json", dir);

        // Write
        {
            let store = S3MetaStore::new(&uri, std::iter::empty::<(String, String)>()).await.unwrap();
            store.put("key", "value", 0).await.unwrap();
        }

        // Read after "restart"
        {
            let store = S3MetaStore::new(&uri, std::iter::empty::<(String, String)>()).await.unwrap();
            let entry = store.get("key").await.unwrap().unwrap();
            assert_eq!(entry.value, "value");
        }

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_s3_store_refresh() {
        let dir = format!("/tmp/lanceforge_s3meta_refresh_{}", std::process::id());
        let _ = tokio::fs::create_dir_all(&dir).await;
        let uri = format!("file://{}/metadata.json", dir);

        let store1 = S3MetaStore::new(&uri, std::iter::empty::<(String, String)>()).await.unwrap();
        store1.put("key", "v1", 0).await.unwrap();

        // Simulate second coordinator reading same data
        let store2 = S3MetaStore::new(&uri, std::iter::empty::<(String, String)>()).await.unwrap();
        let entry = store2.get("key").await.unwrap().unwrap();
        assert_eq!(entry.value, "v1");

        // Store1 updates
        let v1 = store1.get("key").await.unwrap().unwrap().version;
        store1.put("key", "v2", v1).await.unwrap();

        // Store2 refreshes and sees the update
        store2.refresh().await.unwrap();
        let entry = store2.get("key").await.unwrap().unwrap();
        assert_eq!(entry.value, "v2");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    // ── H8: MetaStore error-path coverage ──

    #[tokio::test]
    async fn test_s3_store_invalid_uri_fails_fast() {
        // Unparseable URI ("foo" with no scheme) must surface a storage
        // error at construction, not panic and not proceed silently.
        let err = S3MetaStore::new("not-a-valid-uri-at-all", std::iter::empty::<(String, String)>()).await.err();
        assert!(
            matches!(&err, Some(MetaError::StorageError(_))),
            "expected StorageError for bad URI, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_cas_reject_stale_expected_version() {
        // Arrange: key at v1. Attempt update at v1-1 (stale) must error
        // with VersionConflict carrying the true {expected, actual}.
        let (_dir, path) = unique_meta_path();

        let store = FileMetaStore::new(&path).await.unwrap();
        let v1 = store.put("k", "v1", 0).await.unwrap();
        // Client holds a stale version (pretend it's 0)
        let err = store.put("k", "v2", 0).await;
        match err {
            Err(MetaError::VersionConflict { expected, actual }) => {
                assert_eq!(expected, 0);
                assert_eq!(actual, v1, "conflict must report the actual version so clients can re-sync");
            }
            other => panic!("expected VersionConflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key_is_ok() {
        // Per trait contract (store.rs:60), delete of an absent key
        // returns Ok(()). This covers the NotFound handling branch.
        let (_dir, path) = unique_meta_path();

        let store = FileMetaStore::new(&path).await.unwrap();
        assert!(store.delete("never-existed").await.is_ok(),
                "delete of absent key must be a no-op success");
    }

    #[tokio::test]
    async fn test_file_store_corrupted_snapshot_rejected() {
        // A half-written JSON blob in the MetaStore file must surface
        // as a parse error at load, not deserialize to partial state.
        let (_dir, path) = unique_meta_path();
        tokio::fs::write(&path, b"{this is not valid json at all").await.unwrap();

        let err = FileMetaStore::new(&path).await.err();
        assert!(
            matches!(&err, Some(MetaError::StorageError(m)) if m.contains("parse")),
            "corrupted snapshot must be rejected with a parse error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_s3_store_bootstraps_at_nonexistent_path() {
        // S3MetaStore at a path that doesn't exist yet must create a
        // fresh empty store, not error out.
        let dir = format!("/tmp/lanceforge_s3_fresh_{}", std::process::id());
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let uri = format!("file://{}/metadata.json", dir);

        let store = S3MetaStore::new(&uri, std::iter::empty::<(String, String)>()).await.unwrap();
        assert!(store.get("no-such-key").await.unwrap().is_none());
        // Confirm we can successfully write to it after a fresh-start load.
        store.put("first", "value", 0).await.unwrap();

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_list_keys_empty_and_populated() {
        let (_dir, path) = unique_meta_path();

        let store = FileMetaStore::new(&path).await.unwrap();
        assert!(store.list_keys("").await.unwrap().is_empty(),
                "empty store must return empty key list");

        store.put("ns/a", "x", 0).await.unwrap();
        store.put("ns/b", "y", 0).await.unwrap();
        store.put("other", "z", 0).await.unwrap();

        let mut ns_keys = store.list_keys("ns/").await.unwrap();
        ns_keys.sort();
        assert_eq!(ns_keys, vec!["ns/a".to_string(), "ns/b".to_string()]);
    }

    // ── FileMetaStore tests ──

    #[tokio::test]
    async fn test_file_store_crud() {
        let (_dir, path) = unique_meta_path();

        let store = FileMetaStore::new(&path).await.unwrap();

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
    }

    #[tokio::test]
    async fn test_file_store_prefix() {
        let (_dir, path) = unique_meta_path();

        let store = FileMetaStore::new(&path).await.unwrap();
        store.put("tables/a", "1", 0).await.unwrap();
        store.put("tables/b", "2", 0).await.unwrap();
        store.put("sessions/w0", "alive", 0).await.unwrap();

        let keys = store.list_keys("tables/").await.unwrap();
        assert_eq!(keys.len(), 2);

        let entries = store.get_prefix("tables/").await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_file_store_persistence() {
        let (_dir, path) = unique_meta_path();

        // Write
        {
            let store = FileMetaStore::new(&path).await.unwrap();
            store.put("key", "value", 0).await.unwrap();
        }

        // Read after "restart"
        {
            let store = FileMetaStore::new(&path).await.unwrap();
            let entry = store.get("key").await.unwrap().unwrap();
            assert_eq!(entry.value, "value");
        }
    }

    #[tokio::test]
    async fn test_cas_create_if_not_exists() {
        let (_dir, path) = unique_meta_path();

        let store = FileMetaStore::new(&path).await.unwrap();

        // First create: expected_version=0 → success
        store.put("new_key", "v1", 0).await.unwrap();

        // Second create: expected_version=0 → conflict (already exists)
        let err = store.put("new_key", "v2", 0).await;
        assert!(matches!(err, Err(MetaError::VersionConflict { .. })));
    }

    // ── Schema-version tests (B3.2) ──

    #[tokio::test]
    async fn test_schema_version_stamped_on_new_store() {
        let (_dir, path) = unique_meta_path();

        // Fresh store → put one key so persist runs → reopen and inspect JSON.
        {
            let store = FileMetaStore::new(&path).await.unwrap();
            store.put("k", "v", 0).await.unwrap();
        }
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["schema_version"].as_u64().unwrap() as u32,
                   CURRENT_SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn test_schema_version_upgraded_from_pre_0_2_snapshot() {
        // Simulate a 0.1-era snapshot with no schema_version field.
        let (_dir, path) = unique_meta_path();
        let legacy = r#"{"entries":{"k":{"value":"v","version":1}},"global_version":1}"#;
        tokio::fs::write(&path, legacy).await.unwrap();

        // Loading must succeed (pre-0.2 compatible).
        let store = FileMetaStore::new(&path).await.unwrap();
        let entry = store.get("k").await.unwrap().unwrap();
        assert_eq!(entry.value, "v");
        // Writing any key stamps the new schema_version.
        store.put("k2", "v2", 0).await.unwrap();
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["schema_version"].as_u64().unwrap() as u32,
                   CURRENT_SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn test_schema_version_future_rejected() {
        // A snapshot authored by a newer server (schema_v999) must be rejected
        // so old servers don't silently corrupt forward-version state.
        let (_dir, path) = unique_meta_path();
        let future_snapshot = format!(
            r#"{{"entries":{{}},"global_version":0,"schema_version":{}}}"#,
            CURRENT_SCHEMA_VERSION + 999
        );
        tokio::fs::write(&path, future_snapshot).await.unwrap();

        let err = FileMetaStore::new(&path).await.err();
        assert!(
            matches!(&err, Some(MetaError::StorageError(m)) if m.contains("newer than this server supports")),
            "expected fwd-compat error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_file_store_advisory_lock_prevents_concurrent_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.json").to_string_lossy().to_string();

        // First instance takes the lock
        let store1 = FileMetaStore::new(&path).await.unwrap();

        // Second instance must fail fast — prevents multi-process split-brain
        let err = FileMetaStore::new(&path).await.err();
        assert!(
            matches!(&err, Some(MetaError::StorageError(m)) if m.contains("locked")),
            "expected lock error, got: {err:?}"
        );

        // After first instance drops, a new one can open
        drop(store1);
        let _store2 = FileMetaStore::new(&path).await
            .expect("lock should be released after drop");
    }

    // ── D5: S3MetaStore against a real S3-compatible server (MinIO) ──
    //
    // Everything else in this module exercises the `file://` path
    // through S3MetaStore's object_store dispatch, which catches bugs
    // in the URI parsing + version bookkeeping but not in the actual
    // S3 HTTP round-trip. These tests plug into a local MinIO when
    // the operator opts in.
    //
    // Activation: set MINIO_E2E_TEST=1 (plus MINIO_* credentials if
    // they differ from the defaults the other SSE tests use). Without
    // the env var the test returns early so cargo test still runs in
    // a sealed box. CI nightly-soak-chaos.yml `minio-sse` job
    // satisfies the env for its coverage run.

    fn minio_uri_from_env() -> Option<(String, Vec<(String, String)>)> {
        if std::env::var("MINIO_E2E_TEST").ok().as_deref() != Some("1") {
            return None;
        }
        let endpoint = std::env::var("MINIO_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        let key = std::env::var("MINIO_ROOT_USER")
            .unwrap_or_else(|_| "lanceadmin".to_string());
        let secret = std::env::var("MINIO_ROOT_PASSWORD")
            .unwrap_or_else(|_| "lanceadmin123".to_string());
        let bucket = std::env::var("MINIO_MSTORE_BUCKET")
            .unwrap_or_else(|_| "lance-meta-test".to_string());
        // Unique key per run so concurrent CI jobs don't collide.
        let rand = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let uri = format!("s3://{bucket}/meta-{rand}.json");
        let opts = vec![
            ("aws_access_key_id".to_string(), key),
            ("aws_secret_access_key".to_string(), secret),
            ("aws_endpoint".to_string(), endpoint),
            ("aws_region".to_string(), "us-east-1".to_string()),
            ("allow_http".to_string(), "true".to_string()),
        ];
        Some((uri, opts))
    }

    /// Create the MinIO bucket if missing. `object_store` fails with
    /// 404 when the bucket doesn't exist, but we can't create buckets
    /// via that API (S3 CreateBucket isn't in the ObjectStore trait).
    /// Drop down to the raw `reqwest::put` style via a shell curl
    /// invocation — ugly but avoids pulling boto3 / awssdk deps just
    /// for this test bootstrap.
    async fn ensure_bucket(uri: &str, opts: &[(String, String)]) {
        let key = opts.iter().find(|(k, _)| k == "aws_access_key_id")
            .map(|(_, v)| v.as_str()).unwrap_or("");
        let secret = opts.iter().find(|(k, _)| k == "aws_secret_access_key")
            .map(|(_, v)| v.as_str()).unwrap_or("");
        let endpoint = opts.iter().find(|(k, _)| k == "aws_endpoint")
            .map(|(_, v)| v.as_str()).unwrap_or("http://127.0.0.1:9000");
        let bucket = uri.strip_prefix("s3://").unwrap_or(uri)
            .split('/').next().unwrap_or("");
        // Use aws sigv4 via mc or skip if we can't. Try python3+boto3.
        let py = format!(
            "import boto3\n\
             s = boto3.client('s3', endpoint_url='{endpoint}',\n\
                              aws_access_key_id='{key}',\n\
                              aws_secret_access_key='{secret}',\n\
                              region_name='us-east-1')\n\
             try: s.create_bucket(Bucket='{bucket}')\n\
             except Exception: pass\n"
        );
        let _ = tokio::process::Command::new("python3")
            .arg("-c").arg(&py)
            .output().await;
    }

    #[tokio::test]
    async fn test_s3_store_real_minio_cas_and_persistence() {
        let Some((uri, opts)) = minio_uri_from_env() else {
            eprintln!("skipping: set MINIO_E2E_TEST=1 to run against MinIO");
            return;
        };
        ensure_bucket(&uri, &opts).await;
        let store1 = S3MetaStore::new(&uri, opts.clone()).await.unwrap();

        // Basic CRUD + CAS over real HTTP.
        let v1 = store1.put("tables/t1", "{\"s0\":[\"w0\"]}", 0).await.unwrap();
        assert!(v1 > 0);
        let entry = store1.get("tables/t1").await.unwrap().unwrap();
        assert_eq!(entry.version, v1);

        // CAS conflict on stale version.
        let err = store1.put("tables/t1", "{\"wrong\":true}", 0).await;
        assert!(matches!(err, Err(MetaError::VersionConflict { .. })),
                "expected CAS conflict, got {err:?}");

        // Persistence: new instance reopens and reads committed state.
        drop(store1);
        let store2 = S3MetaStore::new(&uri, opts.clone()).await.unwrap();
        let entry = store2.get("tables/t1").await.unwrap().unwrap();
        assert_eq!(entry.version, v1,
                   "persistence round-trip lost the version");

        // list_keys filters by prefix.
        store2.put("tables/t2", "{}", 0).await.unwrap();
        store2.put("shards/s0", "file:///ignored", 0).await.unwrap();
        let tables = store2.list_keys("tables/").await.unwrap();
        assert_eq!(tables.len(), 2,
                   "list_keys must filter by prefix; got {tables:?}");

        // Cleanup: delete the test object so repeated runs don't
        // accumulate MinIO litter.
        store2.delete("tables/t1").await.ok();
        store2.delete("tables/t2").await.ok();
        store2.delete("shards/s0").await.ok();
    }
}
