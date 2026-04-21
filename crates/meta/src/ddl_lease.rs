// Distributed DDL lease — coordinates schema mutations across
// multiple coordinator instances via MetaStore CAS.
//
// #5.2 of docs/ARCHITECTURE_V2.md. Solves the split-brain risk
// where two coords both process CreateTable/AlterTable/DropTable
// on the same table concurrently and produce inconsistent on-disk
// state. Per-coord `ddl_locks` (service.rs) serialises DDL within
// one process; this layer serialises DDL across processes.
//
// Protocol
// --------
//   * Acquire: read the current lease record at `ddl_lease/{name}`.
//     If it doesn't exist or its TTL has expired, CAS-write a fresh
//     record. Lose the CAS → another coord raced; caller gets
//     LeaseHeld.
//   * Release: delete the key. Best-effort — a crashed / killed
//     coord still loses the lease at TTL expiry.
//   * Steal-on-expire: a caller may acquire over an expired lease
//     as long as it's older than the written TTL. Fresh leases are
//     not stealable even if their holder is unreachable.
//
// The TTL must be >= the DDL operation's upper bound. Default is
// 30s which covers typical CreateTable fan-out on 4-8 workers.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::store::{MetaError, MetaStore};

const DEFAULT_TTL_NS: u128 = 30 * 1_000_000_000;

fn lease_key(table: &str) -> String {
    format!("ddl_lease/{table}")
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LeaseRecord {
    holder: String,
    acquired_ns: u128,
    ttl_ns: u128,
}

/// Outcome of a lease acquire attempt. The caller consumes this
/// handle to release the lease; dropping without release falls back
/// to TTL expiry.
pub struct DdlLease {
    store: std::sync::Arc<dyn MetaStore>,
    table: String,
    /// Snapshot of the CAS version we wrote at, so release can CAS-
    /// delete precisely our own lease and not another holder's (in
    /// the pathological case where our TTL already expired between
    /// acquire and release).
    written_version: u64,
}

// `dyn MetaStore` isn't Debug; provide a minimal hand-rolled impl so
// tests can `.unwrap_err()` on Results carrying this type.
impl std::fmt::Debug for DdlLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdlLease")
            .field("table", &self.table)
            .field("written_version", &self.written_version)
            .finish()
    }
}

#[derive(Debug)]
pub enum AcquireError {
    /// Another coord holds a fresh (non-expired) lease. Message
    /// carries the holder id for diagnostics; caller should not
    /// retry without backoff.
    Held { holder: String, age_ns: u128 },
    /// MetaStore I/O / serialisation failure.
    Storage(MetaError),
}

impl std::fmt::Display for AcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcquireError::Held { holder, age_ns } => write!(
                f,
                "DDL lease held by '{holder}' (age {}ms)",
                age_ns / 1_000_000
            ),
            AcquireError::Storage(e) => write!(f, "ddl_lease storage: {e}"),
        }
    }
}

impl std::error::Error for AcquireError {}

impl From<MetaError> for AcquireError {
    fn from(e: MetaError) -> Self {
        AcquireError::Storage(e)
    }
}

/// Try to acquire a DDL lease for `table`.
///
/// `holder` should be a cluster-unique id for the caller, e.g. the
/// coordinator's `instance_id`. `ttl` caps how long we hold the
/// lease; choose > the DDL op's worst-case latency. Pass None for
/// the default 30s.
pub async fn acquire(
    store: std::sync::Arc<dyn MetaStore>,
    table: &str,
    holder: &str,
    ttl_ns: Option<u128>,
) -> Result<DdlLease, AcquireError> {
    let ttl_ns = ttl_ns.unwrap_or(DEFAULT_TTL_NS);
    let key = lease_key(table);
    let now = now_ns();

    // Read current.
    let existing = store.get(&key).await?;
    let expected_version = match existing {
        Some(v) => {
            let rec: LeaseRecord = serde_json::from_str(&v.value).map_err(|e| {
                MetaError::StorageError(format!("ddl_lease parse: {e}"))
            })?;
            let age = now.saturating_sub(rec.acquired_ns);
            if age < rec.ttl_ns {
                return Err(AcquireError::Held {
                    holder: rec.holder,
                    age_ns: age,
                });
            }
            // Expired — steal via CAS at the current version.
            v.version
        }
        None => 0, // create-only
    };

    let rec = LeaseRecord {
        holder: holder.to_string(),
        acquired_ns: now,
        ttl_ns,
    };
    let value = serde_json::to_string(&rec)
        .map_err(|e| MetaError::StorageError(format!("ddl_lease encode: {e}")))?;
    match store.put(&key, &value, expected_version).await {
        Ok(new_version) => Ok(DdlLease {
            store,
            table: table.to_string(),
            written_version: new_version,
        }),
        Err(MetaError::VersionConflict { .. }) => {
            // Someone raced us — re-read to report who holds it.
            let raced_holder = store
                .get(&key)
                .await
                .ok()
                .flatten()
                .and_then(|v| serde_json::from_str::<LeaseRecord>(&v.value).ok())
                .map(|r| r.holder)
                .unwrap_or_else(|| "unknown".to_string());
            Err(AcquireError::Held {
                holder: raced_holder,
                age_ns: 0,
            })
        }
        Err(e) => Err(AcquireError::Storage(e)),
    }
}

impl DdlLease {
    /// Release the lease. CAS-delete on our own written version so
    /// we don't clobber a successor that acquired after our TTL
    /// expired. Best-effort: logs on failure instead of returning
    /// Err — the caller is usually mid-DDL and shouldn't branch on
    /// release outcome.
    pub async fn release(self) {
        let key = lease_key(&self.table);
        // `put` with the expected version to a tombstone value, or
        // better: check version matches then delete. The existing
        // MetaStore trait has `delete` but no version-guarded
        // delete. Use the safer "put a tombstone" pattern — the key
        // stays with a near-zero TTL so subsequent acquires
        // immediately see it as expired.
        //
        // (A version-guarded `delete_at` on the trait would be
        // cleaner, but adding trait methods ripples through File /
        // S3 / Etcd impls. Tombstone-via-put matches the existing
        // pattern used by shard_uris removal.)
        let tombstone = LeaseRecord {
            holder: String::new(),
            acquired_ns: 0,
            ttl_ns: 0,
        };
        let value = serde_json::to_string(&tombstone).unwrap_or_default();
        if let Err(e) = self.store.put(&key, &value, self.written_version).await {
            log::debug!(
                "ddl_lease release for '{}' at v={}: {} (falling back to TTL)",
                self.table, self.written_version, e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FileMetaStore;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn new_store() -> (Arc<dyn MetaStore>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("meta.json");
        let store: Arc<dyn MetaStore> =
            Arc::new(FileMetaStore::new(path.to_str().unwrap()).await.unwrap());
        (store, tmp)
    }

    #[tokio::test]
    async fn acquire_succeeds_on_empty_key() {
        let (store, _tmp) = new_store().await;
        let lease = acquire(store, "t", "coord-a", None).await.unwrap();
        lease.release().await;
    }

    #[tokio::test]
    async fn second_acquire_from_different_holder_fails() {
        let (store, _tmp) = new_store().await;
        let _a = acquire(store.clone(), "t", "coord-a", None).await.unwrap();
        let err = acquire(store, "t", "coord-b", None).await.unwrap_err();
        match err {
            AcquireError::Held { holder, .. } => assert_eq!(holder, "coord-a"),
            _ => panic!("expected Held, got {err:?}"),
        }
    }

    #[tokio::test]
    async fn release_allows_subsequent_acquire() {
        let (store, _tmp) = new_store().await;
        let first = acquire(store.clone(), "t", "coord-a", None).await.unwrap();
        first.release().await;
        let second = acquire(store, "t", "coord-b", None).await.unwrap();
        second.release().await;
    }

    #[tokio::test]
    async fn expired_lease_can_be_stolen() {
        let (store, _tmp) = new_store().await;
        // Acquire with a 10ms TTL then wait past it.
        let _ = acquire(store.clone(), "t", "coord-a", Some(10_000_000))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        let stolen = acquire(store, "t", "coord-b", None).await.unwrap();
        stolen.release().await;
    }

    #[tokio::test]
    async fn two_tables_are_independent() {
        let (store, _tmp) = new_store().await;
        let a = acquire(store.clone(), "table-a", "coord-a", None)
            .await
            .unwrap();
        let b = acquire(store.clone(), "table-b", "coord-a", None)
            .await
            .unwrap();
        a.release().await;
        b.release().await;
    }

    #[tokio::test]
    async fn concurrent_acquire_one_winner() {
        // Race: 8 tasks acquire the same table lease. Exactly one
        // returns Ok; the rest see Held.
        let (store, _tmp) = new_store().await;
        let mut tasks = Vec::new();
        for i in 0..8 {
            let s = store.clone();
            tasks.push(tokio::spawn(async move {
                acquire(s, "t", &format!("coord-{i}"), None).await
            }));
        }
        let mut winners = 0;
        let mut losers = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(l) => {
                    winners += 1;
                    l.release().await;
                }
                Err(AcquireError::Held { .. }) => losers += 1,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert_eq!(winners, 1, "exactly one acquire should win");
        assert_eq!(losers, 7);
    }

    #[tokio::test]
    async fn steal_on_expire_reports_new_holder() {
        let (store, _tmp) = new_store().await;
        let _ = acquire(store.clone(), "t", "old-holder", Some(5_000_000))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        // New holder steals and releases.
        let new_lease = acquire(store.clone(), "t", "new-holder", None)
            .await
            .unwrap();
        // While new holder holds, yet another contender should see "new-holder".
        let err = acquire(store.clone(), "t", "third", None).await.unwrap_err();
        match err {
            AcquireError::Held { holder, .. } => assert_eq!(holder, "new-holder"),
            _ => panic!("expected Held"),
        }
        new_lease.release().await;
    }
}
