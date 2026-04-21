// Centralised Arrow schema storage, versioned per table.
//
// Phase C.2 of docs/ARCHITECTURE_V2_PLAN.md. Enables the downstream
// capabilities that can't land without a schema truth-source outside
// the per-shard Lance manifests: atomic multi-shard DDL, schema
// evolution, time-travel, and read-your-own-writes on DDL.
//
// Storage shape:
//     schemas/{table_name}/{version:020}  →  base64(Arrow IPC schema)
//
// The 20-digit zero-padded version segment makes lexicographic order
// identical to numeric order — `list_keys(prefix)` returns versions
// in ascending order, so `.last()` is the current head.
//
// This is a thin convenience layer over the existing `MetaStore`
// trait. No new trait methods, no impact on FileMetaStore /
// S3MetaStore implementations — they keep serving opaque strings,
// we base64-encode the bytes ourselves.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

use crate::store::{MetaError, MetaStore};

/// Prefix for per-table schema keys. Kept distinct from the
/// `tables/` prefix that holds routing state (`TargetState`).
const SCHEMA_PREFIX: &str = "schemas/";

/// Width of the zero-padded version segment. 20 digits covers the
/// u64 max, so lex order == numeric order for any version we can
/// legally produce.
const VERSION_WIDTH: usize = 20;

/// Schema storage layer. Owns nothing itself; delegates to an
/// Arc<dyn MetaStore>. Cloning is cheap — Arc bump only.
#[derive(Clone)]
pub struct SchemaStore {
    meta: Arc<dyn MetaStore>,
}

impl SchemaStore {
    pub fn new(meta: Arc<dyn MetaStore>) -> Self {
        Self { meta }
    }

    /// Create-only write. Returns `VersionConflict` if `(table, version)`
    /// already exists. Caller is expected to first read `latest_version`
    /// and retry on conflict — two writers racing to add the same
    /// version will see exactly one winner.
    pub async fn put_schema(
        &self,
        table: &str,
        version: u64,
        arrow_ipc: &[u8],
    ) -> Result<(), MetaError> {
        let key = schema_key(table, version);
        let encoded = B64.encode(arrow_ipc);
        // expected_version = 0 means "create-if-not-exists"
        self.meta.put(&key, &encoded, 0).await.map(|_| ())
    }

    /// Read a specific version. `None` if table or version doesn't exist.
    pub async fn get_schema(
        &self,
        table: &str,
        version: u64,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        let key = schema_key(table, version);
        match self.meta.get(&key).await? {
            Some(v) => Ok(Some(decode(&v.value)?)),
            None => Ok(None),
        }
    }

    /// Read the latest version + its bytes. `None` if the table has
    /// no schemas stored. Does two round-trips (list + get); if that
    /// becomes a hotspot, switch to `get_prefix` + max-version scan.
    pub async fn get_latest(
        &self,
        table: &str,
    ) -> Result<Option<(u64, Vec<u8>)>, MetaError> {
        let prefix = table_prefix(table);
        let mut keys = self.meta.list_keys(&prefix).await?;
        // list_keys order is impl-defined; zero-padding means we can
        // sort and take last() cheaply. N is at most a handful per
        // table for the lifetime of this feature.
        keys.sort();
        let Some(key) = keys.last() else {
            return Ok(None);
        };
        let version = parse_version(key, &prefix)?;
        let bytes = self
            .get_schema(table, version)
            .await?
            .ok_or_else(|| MetaError::StorageError(format!("schema key {key} vanished mid-read")))?;
        Ok(Some((version, bytes)))
    }

    /// Just the latest version number. Used by writers to compute
    /// `next = latest + 1` before calling `put_schema`.
    pub async fn latest_version(&self, table: &str) -> Result<Option<u64>, MetaError> {
        let prefix = table_prefix(table);
        let mut keys = self.meta.list_keys(&prefix).await?;
        keys.sort();
        match keys.last() {
            Some(k) => Ok(Some(parse_version(k, &prefix)?)),
            None => Ok(None),
        }
    }
}

fn schema_key(table: &str, version: u64) -> String {
    format!("{SCHEMA_PREFIX}{table}/{:0width$}", version, width = VERSION_WIDTH)
}

fn table_prefix(table: &str) -> String {
    format!("{SCHEMA_PREFIX}{table}/")
}

fn parse_version(full_key: &str, table_prefix: &str) -> Result<u64, MetaError> {
    full_key
        .strip_prefix(table_prefix)
        .and_then(|tail| tail.parse::<u64>().ok())
        .ok_or_else(|| MetaError::StorageError(format!("malformed schema key: {full_key}")))
}

fn decode(s: &str) -> Result<Vec<u8>, MetaError> {
    B64.decode(s)
        .map_err(|e| MetaError::StorageError(format!("base64 decode failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FileMetaStore;
    use tempfile::TempDir;

    async fn new_store() -> (SchemaStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("meta.json");
        let meta: Arc<dyn MetaStore> =
            Arc::new(FileMetaStore::new(path.to_str().unwrap()).await.unwrap());
        (SchemaStore::new(meta), tmp)
    }

    #[tokio::test]
    async fn put_then_get_roundtrips_bytes() {
        let (store, _tmp) = new_store().await;
        let bytes = vec![0xde, 0xad, 0xbe, 0xef, 0x00, 0xff];
        store.put_schema("orders", 1, &bytes).await.unwrap();
        let back = store.get_schema("orders", 1).await.unwrap();
        assert_eq!(back.as_deref(), Some(bytes.as_slice()));
    }

    #[tokio::test]
    async fn get_missing_table_returns_none() {
        let (store, _tmp) = new_store().await;
        assert!(store.get_schema("never-created", 1).await.unwrap().is_none());
        assert!(store.get_latest("never-created").await.unwrap().is_none());
        assert!(store.latest_version("never-created").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn same_table_version_is_create_only() {
        let (store, _tmp) = new_store().await;
        store.put_schema("t", 1, b"v1").await.unwrap();
        // Writing v=1 again must fail (create-only semantics).
        let second = store.put_schema("t", 1, b"v1-replay").await;
        assert!(matches!(second, Err(MetaError::VersionConflict { .. })));
        // Original bytes unchanged.
        assert_eq!(
            store.get_schema("t", 1).await.unwrap().as_deref(),
            Some(b"v1" as &[u8])
        );
    }

    #[tokio::test]
    async fn get_latest_returns_highest_version() {
        let (store, _tmp) = new_store().await;
        store.put_schema("t", 1, b"first").await.unwrap();
        store.put_schema("t", 2, b"second").await.unwrap();
        store.put_schema("t", 3, b"third").await.unwrap();
        let (v, bytes) = store.get_latest("t").await.unwrap().unwrap();
        assert_eq!(v, 3);
        assert_eq!(bytes, b"third");
    }

    #[tokio::test]
    async fn lex_order_matches_numeric_across_magnitude_boundaries() {
        // Naively formatting u64 as decimal WITHOUT zero-padding
        // would sort "10" before "9". The 20-digit pad protects
        // against that — verify by straddling a magnitude boundary.
        let (store, _tmp) = new_store().await;
        store.put_schema("t", 9, b"nine").await.unwrap();
        store.put_schema("t", 10, b"ten").await.unwrap();
        let (v, bytes) = store.get_latest("t").await.unwrap().unwrap();
        assert_eq!(v, 10);
        assert_eq!(bytes, b"ten");
    }

    #[tokio::test]
    async fn different_tables_dont_interfere() {
        let (store, _tmp) = new_store().await;
        store.put_schema("a", 1, b"a1").await.unwrap();
        store.put_schema("b", 1, b"b1").await.unwrap();
        store.put_schema("b", 2, b"b2").await.unwrap();
        assert_eq!(store.latest_version("a").await.unwrap(), Some(1));
        assert_eq!(store.latest_version("b").await.unwrap(), Some(2));
        assert_eq!(
            store.get_schema("a", 1).await.unwrap().as_deref(),
            Some(b"a1" as &[u8])
        );
    }

    #[tokio::test]
    async fn write_read_version_with_table_name_containing_slash() {
        // Namespaced table names use "/" as separator (G5). The key
        // structure encodes tables verbatim so the slash is allowed.
        // Verify nothing below the SchemaStore layer chokes on it.
        let (store, _tmp) = new_store().await;
        store.put_schema("tenant-a/orders", 1, b"ns1").await.unwrap();
        assert_eq!(
            store.get_schema("tenant-a/orders", 1).await.unwrap().as_deref(),
            Some(b"ns1" as &[u8])
        );
    }

    #[tokio::test]
    async fn large_schema_bytes_roundtrip() {
        // Arrow IPC schemas are small in practice but nothing in the
        // API caps size. Verify base64 encoding survives a kilobyte.
        let (store, _tmp) = new_store().await;
        let bytes: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        store.put_schema("t", 1, &bytes).await.unwrap();
        let back = store.get_schema("t", 1).await.unwrap().unwrap();
        assert_eq!(back, bytes);
    }
}
