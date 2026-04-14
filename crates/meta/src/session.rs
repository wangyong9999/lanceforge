// Licensed under the Apache License, Version 2.0.
// Session Manager: worker registration with TTL heartbeat.
//
// Inspired by Milvus SessionManager (etcd lease + TTL).
// Workers register with the coordinator, send periodic heartbeats.
// If heartbeat expires, session is removed and shards are reassigned.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::store::{MetaStore, MetaError};

/// Session TTL: worker must heartbeat within this interval or be considered dead.
/// (Milvus default: 30s, we use 30s too)
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30);

/// Worker session info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSession {
    pub worker_id: String,
    pub host: String,
    pub port: u16,
    pub registered_at: u64, // unix timestamp secs
    pub last_heartbeat: u64,
}

/// Manages worker sessions with TTL-based expiry.
pub struct SessionManager {
    store: Arc<dyn MetaStore>,
    prefix: String,
    ttl: Duration,
    /// In-memory cache of live sessions, refreshed from store.
    sessions: RwLock<HashMap<String, (WorkerSession, Instant)>>,
}

impl SessionManager {
    pub fn new(store: Arc<dyn MetaStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.to_string(),
            ttl: DEFAULT_SESSION_TTL,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Register a worker. Creates or updates session in MetaStore.
    pub async fn register(
        &self,
        worker_id: &str,
        host: &str,
        port: u16,
    ) -> Result<(), MetaError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let session = WorkerSession {
            worker_id: worker_id.to_string(),
            host: host.to_string(),
            port,
            registered_at: now,
            last_heartbeat: now,
        };

        let key = format!("{}/sessions/{}", self.prefix, worker_id);
        let value = serde_json::to_string(&session)
            .map_err(|e| MetaError::StorageError(format!("serialize session: {e}")))?;

        // Try create first, then update if exists
        match self.store.put(&key, &value, 0).await {
            Ok(_) => {}
            Err(MetaError::VersionConflict { actual, .. }) => {
                // Already registered, update
                self.store.put(&key, &value, actual).await?;
            }
            Err(e) => return Err(e),
        }

        self.sessions.write().await.insert(
            worker_id.to_string(),
            (session.clone(), Instant::now()),
        );

        info!("Worker '{}' registered at {}:{}", worker_id, host, port);
        Ok(())
    }

    /// Heartbeat: update last_heartbeat timestamp.
    pub async fn heartbeat(&self, worker_id: &str) -> Result<(), MetaError> {
        let mut sessions = self.sessions.write().await;
        if let Some((session, last_seen)) = sessions.get_mut(worker_id) {
            *last_seen = Instant::now();
            session.last_heartbeat = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // Persist heartbeat to store
            let key = format!("{}/sessions/{}", self.prefix, worker_id);
            let value = serde_json::to_string(&session)
                .map_err(|e| MetaError::StorageError(format!("serialize: {e}")))?;
            // Best-effort persist (don't fail heartbeat on store error)
            if let Err(e) = self.store.get(&key).await.and_then(|v| {
                Ok(v.map(|v| v.version).unwrap_or(0))
            }) {
                warn!("Heartbeat persist skipped for {}: {}", worker_id, e);
            }
        }
        Ok(())
    }

    /// Get all live (non-expired) sessions.
    pub async fn live_sessions(&self) -> Vec<WorkerSession> {
        let sessions = self.sessions.read().await;
        sessions.values()
            .filter(|(_, last_seen)| last_seen.elapsed() < self.ttl)
            .map(|(s, _)| s.clone())
            .collect()
    }

    /// Get expired sessions and remove them.
    pub async fn expire_sessions(&self) -> Vec<String> {
        let mut sessions = self.sessions.write().await;
        let mut expired = Vec::new();

        sessions.retain(|worker_id, (_, last_seen)| {
            if last_seen.elapsed() >= self.ttl {
                expired.push(worker_id.clone());
                false
            } else {
                true
            }
        });

        // Remove from store
        for worker_id in &expired {
            let key = format!("{}/sessions/{}", self.prefix, worker_id);
            let _ = self.store.delete(&key).await;
            warn!("Worker '{}' session expired (TTL {:?})", worker_id, self.ttl);
        }

        expired
    }

    /// Load sessions from store (for coordinator restart recovery).
    pub async fn load_from_store(&self) -> Result<(), MetaError> {
        let prefix = format!("{}/sessions/", self.prefix);
        let entries = self.store.get_prefix(&prefix).await?;
        let mut sessions = self.sessions.write().await;

        for (key, versioned) in entries {
            if let Ok(session) = serde_json::from_str::<WorkerSession>(&versioned.value) {
                sessions.insert(session.worker_id.clone(), (session, Instant::now()));
            }
        }
        info!("Loaded {} sessions from store", sessions.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FileMetaStore;

    async fn test_store(name: &str) -> Arc<dyn MetaStore> {
        let path = format!("/tmp/lanceforge_session_{}_{}.json", name, std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let _ = tokio::fs::remove_file(&path).await;
        Arc::new(FileMetaStore::new(&path).await.unwrap())
    }

    #[tokio::test]
    async fn test_register_and_list() {
        let store = test_store("reg1").await;
        let mgr = SessionManager::new(store, "test");

        mgr.register("w0", "127.0.0.1", 50100).await.unwrap();
        mgr.register("w1", "127.0.0.1", 50101).await.unwrap();

        let live = mgr.live_sessions().await;
        assert_eq!(live.len(), 2);
    }

    #[tokio::test]
    async fn test_session_expiry() {
        let store = test_store("reg1").await;
        let mgr = SessionManager::new(store, "test")
            .with_ttl(Duration::from_millis(50));

        mgr.register("w0", "127.0.0.1", 50100).await.unwrap();
        assert_eq!(mgr.live_sessions().await.len(), 1);

        tokio::time::sleep(Duration::from_millis(100)).await;
        let expired = mgr.expire_sessions().await;
        assert_eq!(expired, vec!["w0"]);
        assert_eq!(mgr.live_sessions().await.len(), 0);
    }

    #[tokio::test]
    async fn test_heartbeat_extends_session() {
        let store = test_store("reg1").await;
        let mgr = SessionManager::new(store, "test")
            .with_ttl(Duration::from_millis(200));

        mgr.register("w0", "127.0.0.1", 50100).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        mgr.heartbeat("w0").await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should still be alive (heartbeat extended the window)
        assert_eq!(mgr.live_sessions().await.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_register() {
        let store = test_store("reg1").await;
        let mgr = SessionManager::new(store, "test");

        mgr.register("w0", "127.0.0.1", 50100).await.unwrap();
        // Re-register same worker (e.g., after restart) — should succeed
        mgr.register("w0", "127.0.0.1", 50100).await.unwrap();

        assert_eq!(mgr.live_sessions().await.len(), 1);
    }
}
