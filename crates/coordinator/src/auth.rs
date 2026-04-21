// Licensed under the Apache License, Version 2.0.
// API key authentication + role-based authorization interceptor for gRPC.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Status};

/// Access role for an API key. Higher roles include lower roles' permissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Read-only: search, count, list, get_schema, get_status.
    Read,
    /// Write: Read + add/delete/upsert rows.
    Write,
    /// Admin: Write + create_table, drop_table, create_index, rebalance.
    Admin,
}

impl Role {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "read" => Some(Role::Read),
            "write" => Some(Role::Write),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }
}

/// Permission level required by an RPC method.
#[derive(Clone, Copy, Debug)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl Permission {
    fn satisfied_by(&self, role: Role) -> bool {
        match self {
            Permission::Read => role >= Role::Read,
            Permission::Write => role >= Role::Write,
            Permission::Admin => role >= Role::Admin,
        }
    }
}

/// Per-key QPS rate-limit token bucket (G6). One bucket per API key that
/// declares a `qps_limit`. Refill at `qps_limit` tokens per second, capacity
/// = `qps_limit` (burst allowance of one second). Keys with limit=0 or
/// absent skip this path entirely.
#[derive(Debug)]
struct RateBucket {
    qps_limit: u32,
    /// Number of tokens left × 1000 (milli-tokens) so fractional refill
    /// between requests doesn't get lost to integer truncation.
    tokens_milli: std::sync::Mutex<(u64, std::time::Instant)>,
}

impl RateBucket {
    fn new(qps_limit: u32) -> Self {
        let cap = (qps_limit as u64) * 1000;
        Self {
            qps_limit,
            tokens_milli: std::sync::Mutex::new((cap, std::time::Instant::now())),
        }
    }

    /// Try to consume one token. Returns true if the request is allowed,
    /// false if the bucket is empty. Thread-safe; bucket is refilled
    /// in place proportional to elapsed time since the last call.
    fn try_consume(&self) -> bool {
        let cap = (self.qps_limit as u64) * 1000;
        if cap == 0 { return true; }
        let mut guard = self.tokens_milli.lock().expect("rate bucket lock poisoned");
        let (ref mut tokens, ref mut last) = *guard;
        let now = std::time::Instant::now();
        let elapsed_ms = now.duration_since(*last).as_millis() as u64;
        // Refill: qps_limit tokens/s × elapsed_ms/1000 × 1000 milli-tokens
        //       = qps_limit × elapsed_ms milli-tokens.
        let refill = (self.qps_limit as u64).saturating_mul(elapsed_ms);
        *tokens = (*tokens + refill).min(cap);
        *last = now;
        if *tokens >= 1000 {
            *tokens -= 1000;
            true
        } else {
            false
        }
    }
}

/// API key validator + role lookup. Checks "authorization" metadata header.
///
/// Keys live behind an `Arc<RwLock<Arc<HashMap>>>` so `reload()` can swap the
/// entire registry atomically — the sync-path `authenticate()` snapshots an
/// `Arc` under a read lock and releases the lock immediately, so a concurrent
/// reload never blocks request handling. ROADMAP_0.2 §B1.2.3.
///
/// Per-key QPS quotas (G6) and namespace bindings (G5) live in parallel
/// maps keyed by the same API key. Updated by `reload()` alongside the
/// role map.
#[derive(Clone)]
#[allow(clippy::type_complexity)] // double Arc<RwLock<Arc<_>>> is intentional: outer Arc for sharing across threads, inner Arc for lock-free reload swap
pub struct ApiKeyInterceptor {
    /// Map from key → role. Empty map = no auth required (all allowed as Admin).
    keys: Arc<std::sync::RwLock<Arc<HashMap<String, Role>>>>,
    /// Map from key → rate bucket. Only keys with qps_limit > 0 appear here;
    /// unrated keys are allowed through without a bucket lookup cost.
    buckets: Arc<std::sync::RwLock<Arc<HashMap<String, Arc<RateBucket>>>>>,
    /// Map from key → namespace prefix (G5). Only keys that declared a
    /// non-empty namespace appear here. A missing entry means "no
    /// namespace binding" — caller can reference any table.
    namespaces: Arc<std::sync::RwLock<Arc<HashMap<String, String>>>>,
}

impl ApiKeyInterceptor {
    /// Legacy constructor: all keys granted Admin role (backward compatible).
    ///
    /// **Security note (will flip in 0.3.0)**: legacy `api_keys` without an
    /// explicit role are granted `Admin` today. This is retained for 0.1 →
    /// 0.2 compatibility; 0.3.0 will default to `Read`. See
    /// `docs/COMPAT_POLICY.md` §9.
    pub fn new(keys: Vec<String>) -> Self {
        let map: HashMap<String, Role> =
            keys.into_iter().map(|k| (k, Role::Admin)).collect();
        Self {
            keys: Arc::new(std::sync::RwLock::new(Arc::new(map))),
            buckets: Arc::new(std::sync::RwLock::new(Arc::new(HashMap::new()))),
            namespaces: Arc::new(std::sync::RwLock::new(Arc::new(HashMap::new()))),
        }
    }

    /// New constructor with explicit role assignment.
    pub fn with_roles(keys: HashMap<String, Role>) -> Self {
        Self {
            keys: Arc::new(std::sync::RwLock::new(Arc::new(keys))),
            buckets: Arc::new(std::sync::RwLock::new(Arc::new(HashMap::new()))),
            namespaces: Arc::new(std::sync::RwLock::new(Arc::new(HashMap::new()))),
        }
    }

    /// Install per-key QPS quotas (G6). Keys with qps_limit=0 are omitted.
    /// Called once at startup from `coordinator/bin/main.rs` after the
    /// interceptor is constructed from config.
    pub fn set_rate_limits(&self, limits: HashMap<String, u32>) {
        let mut buckets = HashMap::new();
        for (key, qps) in limits {
            if qps > 0 {
                buckets.insert(key, Arc::new(RateBucket::new(qps)));
            }
        }
        *self.buckets.write().expect("auth buckets lock poisoned") = Arc::new(buckets);
    }

    /// Install per-key namespace bindings (G5). Keys not present in the
    /// map have no namespace restriction. Called once at startup from
    /// `coordinator/bin/main.rs`.
    pub fn set_namespaces(&self, bindings: HashMap<String, String>) {
        let filtered: HashMap<String, String> = bindings
            .into_iter()
            .filter(|(_, ns)| !ns.is_empty())
            .collect();
        *self.namespaces.write().expect("auth namespaces lock poisoned") = Arc::new(filtered);
    }

    /// Snapshot the current namespace map. Cheap (Arc clone).
    fn namespaces_snapshot(&self) -> Arc<HashMap<String, String>> {
        self.namespaces.read().expect("auth namespaces lock poisoned").clone()
    }

    /// Resolve the caller's namespace binding, if any. Used by the
    /// coordinator service to enforce `{ns}/` prefix on every table
    /// reference. Returns `None` when the caller is unauthenticated, has
    /// no key header, or the key is not bound to a namespace.
    pub fn namespace_for<T>(&self, req: &Request<T>) -> Option<String> {
        let key = req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s))?;
        self.namespaces_snapshot().get(key).cloned()
    }

    /// Enforce namespace prefix on a table name for the caller. Returns
    /// `PermissionDenied` when the caller's key is bound to a namespace
    /// and `table` does not start with `{namespace}/`. Callers without a
    /// namespace (or unauthenticated setups) pass through.
    pub fn check_namespace<T>(&self, req: &Request<T>, table: &str) -> Result<(), Status> {
        if let Some(ns) = self.namespace_for(req) {
            let required = format!("{}/", ns);
            if !table.starts_with(&required) {
                return Err(Status::permission_denied(format!(
                    "table '{table}' not in caller's namespace '{ns}/'"
                )));
            }
        }
        Ok(())
    }

    /// Filter a list of table names by the caller's namespace binding.
    /// Callers without a namespace see the full list (keeps operator
    /// `ListTables` usable for break-glass admins).
    pub fn filter_namespace<T>(&self, req: &Request<T>, tables: Vec<String>) -> Vec<String> {
        match self.namespace_for(req) {
            Some(ns) => {
                let prefix = format!("{}/", ns);
                tables.into_iter().filter(|t| t.starts_with(&prefix)).collect()
            }
            None => tables,
        }
    }

    /// Replace the live key registry atomically. Callers: the hot-reload
    /// task in `coordinator/bin/main.rs`. Zero-downtime swap — in-flight
    /// requests holding an `Arc` snapshot complete against the old map.
    pub fn reload(&self, new_keys: HashMap<String, Role>) {
        *self.keys.write().expect("auth registry lock poisoned") = Arc::new(new_keys);
    }

    /// Snapshot the current registry. Cheap (Arc clone).
    fn snapshot(&self) -> Arc<HashMap<String, Role>> {
        self.keys.read().expect("auth registry lock poisoned").clone()
    }

    /// Snapshot the current rate-limit map. Cheap (Arc clone).
    fn buckets_snapshot(&self) -> Arc<HashMap<String, Arc<RateBucket>>> {
        self.buckets.read().expect("auth buckets lock poisoned").clone()
    }

    /// Check rate limit for a key. Returns Ok(()) if the request is
    /// allowed, Err(ResourceExhausted) if the bucket is empty.
    /// Keys without a configured rate limit always pass.
    pub fn check_rate_limit(&self, api_key: &str) -> Result<(), Status> {
        let buckets = self.buckets_snapshot();
        match buckets.get(api_key) {
            Some(b) if !b.try_consume() => Err(Status::resource_exhausted(
                format!("API key quota exceeded: {} QPS limit", b.qps_limit)
            )),
            _ => Ok(()),
        }
    }

    /// Current key count, for metrics / logs.
    pub fn key_count(&self) -> usize {
        self.snapshot().len()
    }

    /// Returns true if authentication is required (non-empty key list).
    pub fn auth_required(&self) -> bool {
        !self.snapshot().is_empty()
    }

    /// Validate key only (no permission check). Returns the role if valid.
    pub fn authenticate<T>(&self, req: &Request<T>) -> Result<Role, Status> {
        let keys = self.snapshot();
        if keys.is_empty() {
            return Ok(Role::Admin); // No auth configured — allow everything
        }
        let key = req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s));
        match key {
            Some(k) => keys.get(k).copied()
                .ok_or_else(|| Status::unauthenticated("Invalid API key")),
            None => Err(Status::unauthenticated("Missing authorization header")),
        }
    }

    /// Legacy API: authenticate only, return request unchanged.
    pub fn check<T>(&self, req: Request<T>) -> Result<Request<T>, Status> {
        self.authenticate(&req)?;
        Ok(req)
    }

    /// Authenticate and authorize for a required permission level. Also
    /// charges the per-key rate limit bucket (G6) after successful auth.
    /// Rate-limit failure returns `ResourceExhausted`; auth failures keep
    /// returning `Unauthenticated`/`PermissionDenied` so operators can
    /// diagnose the difference from error codes alone.
    pub fn authorize<T>(&self, req: &Request<T>, needed: Permission) -> Result<(), Status> {
        let role = self.authenticate(req)?;
        if !needed.satisfied_by(role) {
            return Err(Status::permission_denied(format!(
                "{:?} permission required (role={:?})", needed, role
            )));
        }
        // G6 rate limit. Extract the raw key (same logic as authenticate)
        // and charge the bucket. Missing header / anonymous / no buckets
        // configured → no charge, no denial.
        if let Some(key) = req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s))
        {
            self.check_rate_limit(key)?;
        }
        Ok(())
    }

    /// Extract a short, non-secret principal identifier for audit logs.
    /// Returns the prefix of the API key (first 8 chars) or "anonymous".
    pub fn principal<T>(&self, req: &Request<T>) -> String {
        if !self.auth_required() {
            return "anonymous".to_string();
        }
        req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s))
            .map(|k| {
                let take = k.len().min(8);
                format!("key:{}…", &k[..take])
            })
            .unwrap_or_else(|| "missing".to_string())
    }
}

// ════════════════════════════════════════════
// MetaStore-backed key registry (B1.2.3)
// ════════════════════════════════════════════

/// Prefix under which API key → role mappings live in the MetaStore.
/// Individual keys land at `auth/keys/{key}`; value is a JSON `{ "role": "Read|Write|Admin" }`.
pub const AUTH_KEYS_PREFIX: &str = "auth/keys/";

/// JSON value stored in MetaStore for each key. Kept minimal so future
/// additions (labels, quota, rotation-due) are additive.
#[derive(serde::Serialize, serde::Deserialize)]
struct KeyEntry {
    role: String,
}

/// Load the live key registry from a MetaStore instance.
///
/// Returns a (possibly empty) HashMap. Missing / malformed entries are
/// logged and skipped rather than failing the whole load — a single
/// broken entry must not lock out every other key.
pub async fn load_from_metastore(
    store: &dyn lance_distributed_meta::store::MetaStore,
) -> Result<HashMap<String, Role>, lance_distributed_meta::store::MetaError> {
    let all = store.get_prefix(AUTH_KEYS_PREFIX).await?;
    let mut out = HashMap::new();
    for (full_key, versioned) in all {
        let key = full_key
            .strip_prefix(AUTH_KEYS_PREFIX)
            .unwrap_or(&full_key)
            .to_string();
        match serde_json::from_str::<KeyEntry>(&versioned.value) {
            Ok(entry) => match Role::parse(&entry.role) {
                Some(role) => {
                    out.insert(key, role);
                }
                None => log::warn!(
                    "auth: skipping key '{}' in MetaStore: unknown role '{}'",
                    full_key, entry.role
                ),
            },
            Err(e) => log::warn!(
                "auth: skipping key '{}' in MetaStore: {}",
                full_key, e
            ),
        }
    }
    Ok(out)
}

/// Bootstrap the MetaStore with the config-provided keys **if and only if**
/// `auth/keys/` is currently empty. Used on first-run to migrate a 0.1-style
/// config-baked-in key list into the durable runtime-rotatable store.
///
/// Returns the number of keys written. Idempotent: later calls are no-ops
/// because the second check sees a populated prefix.
pub async fn bootstrap_metastore_if_empty(
    store: &dyn lance_distributed_meta::store::MetaStore,
    seed_keys: &HashMap<String, Role>,
) -> Result<usize, lance_distributed_meta::store::MetaError> {
    if seed_keys.is_empty() {
        return Ok(0);
    }
    let existing = store.list_keys(AUTH_KEYS_PREFIX).await?;
    if !existing.is_empty() {
        return Ok(0);
    }
    let mut written = 0usize;
    for (key, role) in seed_keys {
        let entry = KeyEntry { role: format!("{:?}", role) };
        let payload = serde_json::to_string(&entry)
            .map_err(|e| lance_distributed_meta::store::MetaError::StorageError(
                format!("auth bootstrap serialize: {e}")
            ))?;
        let full_key = format!("{AUTH_KEYS_PREFIX}{key}");
        // expected_version=0 is "create only"; if it races with another
        // coordinator doing the same bootstrap we just skip that key.
        match store.put(&full_key, &payload, 0).await {
            Ok(_) => written += 1,
            Err(lance_distributed_meta::store::MetaError::VersionConflict { .. }) => {
                log::debug!("auth bootstrap: key '{}' already created by another coordinator", full_key);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn make_request_with_key(key: &str) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {}", key)).unwrap(),
        );
        req
    }

    #[test]
    fn test_no_auth_configured() {
        let interceptor = ApiKeyInterceptor::new(vec![]);
        assert!(!interceptor.auth_required());
        let req = Request::new(());
        assert!(interceptor.check(req).is_ok());
    }

    #[test]
    fn test_valid_key() {
        let interceptor = ApiKeyInterceptor::new(vec!["secret-key-1".into()]);
        assert!(interceptor.auth_required());
        let req = make_request_with_key("secret-key-1");
        assert!(interceptor.check(req).is_ok());
    }

    #[test]
    fn test_invalid_key() {
        let interceptor = ApiKeyInterceptor::new(vec!["secret-key-1".into()]);
        let req = make_request_with_key("wrong-key");
        let err = interceptor.check(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("Invalid"));
    }

    #[test]
    fn test_missing_header() {
        let interceptor = ApiKeyInterceptor::new(vec!["secret-key-1".into()]);
        let req = Request::new(());
        let err = interceptor.check(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(err.message().contains("Missing"));
    }

    #[test]
    fn test_multiple_valid_keys() {
        let interceptor = ApiKeyInterceptor::new(vec!["key-a".into(), "key-b".into()]);
        assert!(interceptor.check(make_request_with_key("key-a")).is_ok());
        assert!(interceptor.check(make_request_with_key("key-b")).is_ok());
        assert!(interceptor.check(make_request_with_key("key-c")).is_err());
    }

    #[test]
    fn test_key_without_bearer_prefix() {
        let interceptor = ApiKeyInterceptor::new(vec!["raw-key".into()]);
        let mut req = Request::new(());
        req.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("raw-key").unwrap(),
        );
        assert!(interceptor.check(req).is_ok());
    }

    #[test]
    fn test_role_hierarchy() {
        assert!(Permission::Read.satisfied_by(Role::Read));
        assert!(Permission::Read.satisfied_by(Role::Write));
        assert!(Permission::Read.satisfied_by(Role::Admin));
        assert!(!Permission::Write.satisfied_by(Role::Read));
        assert!(Permission::Write.satisfied_by(Role::Write));
        assert!(Permission::Admin.satisfied_by(Role::Admin));
        assert!(!Permission::Admin.satisfied_by(Role::Write));
    }

    #[test]
    fn test_rbac_read_cannot_admin() {
        let mut m = HashMap::new();
        m.insert("reader".to_string(), Role::Read);
        m.insert("writer".to_string(), Role::Write);
        m.insert("boss".to_string(), Role::Admin);
        let ic = ApiKeyInterceptor::with_roles(m);

        let r = make_request_with_key("reader");
        assert!(ic.authorize(&r, Permission::Read).is_ok());
        let r = make_request_with_key("reader");
        assert!(ic.authorize(&r, Permission::Write).is_err());
        let r = make_request_with_key("reader");
        assert!(ic.authorize(&r, Permission::Admin).is_err());

        let r = make_request_with_key("writer");
        assert!(ic.authorize(&r, Permission::Write).is_ok());
        let r = make_request_with_key("writer");
        assert!(ic.authorize(&r, Permission::Admin).is_err());

        let r = make_request_with_key("boss");
        assert!(ic.authorize(&r, Permission::Admin).is_ok());
    }

    #[test]
    fn test_role_parse() {
        assert_eq!(Role::parse("read"), Some(Role::Read));
        assert_eq!(Role::parse("WRITE"), Some(Role::Write));
        assert_eq!(Role::parse("Admin"), Some(Role::Admin));
        assert_eq!(Role::parse("bogus"), None);
    }

    // ── B1.2.3: hot-reload tests ──

    #[test]
    fn test_reload_atomic_replace() {
        // Start with one key, reload with a different one, verify the
        // old one no longer authenticates and the new one does.
        let ic = ApiKeyInterceptor::new(vec!["old".into()]);
        assert!(ic.check(make_request_with_key("old")).is_ok());

        let mut new_map = HashMap::new();
        new_map.insert("new".to_string(), Role::Admin);
        ic.reload(new_map);

        assert!(ic.check(make_request_with_key("old")).is_err(),
                "old key must be rejected after reload");
        assert!(ic.check(make_request_with_key("new")).is_ok(),
                "new key must authenticate after reload");
    }

    #[test]
    fn test_reload_changes_role() {
        // Confirm reload is not just adding keys — it can also demote them.
        let mut m = HashMap::new();
        m.insert("k".to_string(), Role::Admin);
        let ic = ApiKeyInterceptor::with_roles(m);
        assert!(ic.authorize(&make_request_with_key("k"), Permission::Admin).is_ok());

        let mut demoted = HashMap::new();
        demoted.insert("k".to_string(), Role::Read);
        ic.reload(demoted);
        assert!(ic.authorize(&make_request_with_key("k"), Permission::Admin).is_err(),
                "role demotion must take effect immediately");
        assert!(ic.authorize(&make_request_with_key("k"), Permission::Read).is_ok());
    }

    #[test]
    fn test_reload_to_empty_disables_auth() {
        let ic = ApiKeyInterceptor::new(vec!["sekret".into()]);
        assert!(ic.auth_required());
        ic.reload(HashMap::new());
        assert!(!ic.auth_required(),
                "empty reload disables auth (matches startup behavior)");
    }

    #[test]
    fn test_key_count() {
        let mut m = HashMap::new();
        m.insert("a".to_string(), Role::Read);
        m.insert("b".to_string(), Role::Write);
        let ic = ApiKeyInterceptor::with_roles(m);
        assert_eq!(ic.key_count(), 2);
    }

    // ── B1.2.3: MetaStore integration ──

    async fn make_test_metastore() -> (
        std::sync::Arc<dyn lance_distributed_meta::store::MetaStore>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.json").to_string_lossy().to_string();
        let store = lance_distributed_meta::store::FileMetaStore::new(&path).await.unwrap();
        (std::sync::Arc::new(store), dir)
    }

    #[tokio::test]
    async fn test_bootstrap_metastore_writes_when_empty() {
        let (store, _dir) = make_test_metastore().await;
        let mut seed = HashMap::new();
        seed.insert("admin-key".to_string(), Role::Admin);
        seed.insert("reader".to_string(), Role::Read);

        let n = bootstrap_metastore_if_empty(store.as_ref(), &seed).await.unwrap();
        assert_eq!(n, 2);
        // Second call is a no-op because the prefix is populated.
        let n2 = bootstrap_metastore_if_empty(store.as_ref(), &seed).await.unwrap();
        assert_eq!(n2, 0);
    }

    #[tokio::test]
    async fn test_bootstrap_no_op_on_empty_seed() {
        let (store, _dir) = make_test_metastore().await;
        let n = bootstrap_metastore_if_empty(store.as_ref(), &HashMap::new()).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_load_from_metastore_roundtrip() {
        let (store, _dir) = make_test_metastore().await;
        let mut seed = HashMap::new();
        seed.insert("k1".to_string(), Role::Admin);
        seed.insert("k2".to_string(), Role::Write);
        seed.insert("k3".to_string(), Role::Read);
        bootstrap_metastore_if_empty(store.as_ref(), &seed).await.unwrap();

        let loaded = load_from_metastore(store.as_ref()).await.unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.get("k1"), Some(&Role::Admin));
        assert_eq!(loaded.get("k2"), Some(&Role::Write));
        assert_eq!(loaded.get("k3"), Some(&Role::Read));
    }

    #[tokio::test]
    async fn test_load_from_metastore_skips_malformed() {
        let (store, _dir) = make_test_metastore().await;
        // Good entry.
        store.put(&format!("{AUTH_KEYS_PREFIX}good"), r#"{"role":"Admin"}"#, 0).await.unwrap();
        // Unknown role — should be skipped with a warn log.
        store.put(&format!("{AUTH_KEYS_PREFIX}bad_role"), r#"{"role":"Overlord"}"#, 0).await.unwrap();
        // Malformed JSON — should be skipped.
        store.put(&format!("{AUTH_KEYS_PREFIX}bad_json"), "not-json-at-all", 0).await.unwrap();

        let loaded = load_from_metastore(store.as_ref()).await.unwrap();
        assert_eq!(loaded.len(), 1, "only 'good' survives");
        assert!(loaded.contains_key("good"));
    }

    #[tokio::test]
    async fn test_end_to_end_bootstrap_then_reload() {
        // Simulate full flow: coord has config keys, bootstrap writes them to
        // MetaStore, a subsequent load from MetaStore produces the same map,
        // interceptor.reload() makes it live.
        let (store, _dir) = make_test_metastore().await;
        let mut config_keys = HashMap::new();
        config_keys.insert("bootstrap-admin".to_string(), Role::Admin);

        let ic = ApiKeyInterceptor::with_roles(HashMap::new());
        assert!(!ic.auth_required());

        bootstrap_metastore_if_empty(store.as_ref(), &config_keys).await.unwrap();
        let loaded = load_from_metastore(store.as_ref()).await.unwrap();
        ic.reload(loaded);

        assert!(ic.auth_required());
        assert!(ic.check(make_request_with_key("bootstrap-admin")).is_ok());
    }

    // ── G6: per-key QPS quota ──

    #[test]
    fn test_rate_limit_burst_and_block() {
        // Key with qps_limit=3: first 3 requests pass, the 4th is blocked.
        let mut keys = HashMap::new();
        keys.insert("limited".to_string(), Role::Read);
        let ic = ApiKeyInterceptor::with_roles(keys);
        let mut limits = HashMap::new();
        limits.insert("limited".to_string(), 3u32);
        ic.set_rate_limits(limits);

        // First 3 should all pass (bucket capacity = qps_limit = 3).
        for i in 0..3 {
            assert!(ic.check_rate_limit("limited").is_ok(), "request {i} should pass");
        }
        // Fourth is blocked.
        let err = ic.check_rate_limit("limited").unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(err.message().contains("3 QPS"), "error must mention the limit: {}", err.message());
    }

    #[test]
    fn test_rate_limit_unconfigured_key_allowed() {
        // A key that's not in the rate-limit map passes through unchecked.
        let ic = ApiKeyInterceptor::with_roles(HashMap::new());
        assert!(ic.check_rate_limit("anything").is_ok());
        // Even if other keys have limits, this one doesn't.
        let mut limits = HashMap::new();
        limits.insert("other".to_string(), 1u32);
        ic.set_rate_limits(limits);
        for _ in 0..10 {
            assert!(ic.check_rate_limit("anything").is_ok(),
                    "unconfigured key always passes");
        }
    }

    #[test]
    fn test_rate_limit_zero_limit_is_unlimited() {
        // qps_limit=0 in config means "no limit", so set_rate_limits
        // filters it out and check_rate_limit returns Ok.
        let ic = ApiKeyInterceptor::with_roles(HashMap::new());
        let mut limits = HashMap::new();
        limits.insert("unlimited".to_string(), 0u32);
        ic.set_rate_limits(limits);
        for _ in 0..100 {
            assert!(ic.check_rate_limit("unlimited").is_ok());
        }
    }

    #[tokio::test]
    async fn test_rate_limit_refills_over_time() {
        // After exhausting the bucket, waiting > 1/qps seconds should
        // let one request through again.
        let ic = ApiKeyInterceptor::with_roles(HashMap::new());
        let mut limits = HashMap::new();
        limits.insert("k".to_string(), 2u32);  // 2 qps = 500 ms per token
        ic.set_rate_limits(limits);
        // Drain.
        assert!(ic.check_rate_limit("k").is_ok());
        assert!(ic.check_rate_limit("k").is_ok());
        assert!(ic.check_rate_limit("k").is_err(), "3rd immediate should block");
        // Wait 600ms — refill ≈ 2 × 0.6 = 1.2 tokens.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert!(ic.check_rate_limit("k").is_ok(), "after refill one request should pass");
    }

    #[test]
    fn test_rate_limit_independent_per_key() {
        // Exhausting one key's bucket must not starve another.
        let mut keys = HashMap::new();
        keys.insert("a".to_string(), Role::Read);
        keys.insert("b".to_string(), Role::Read);
        let ic = ApiKeyInterceptor::with_roles(keys);
        let mut limits = HashMap::new();
        limits.insert("a".to_string(), 1u32);
        limits.insert("b".to_string(), 5u32);
        ic.set_rate_limits(limits);
        // Drain a.
        assert!(ic.check_rate_limit("a").is_ok());
        assert!(ic.check_rate_limit("a").is_err());
        // b is still fully available.
        for _ in 0..5 {
            assert!(ic.check_rate_limit("b").is_ok());
        }
    }

    #[test]
    fn test_authorize_charges_rate_limit() {
        // End-to-end: a rate-limited key doing RPCs via authorize() should
        // see ResourceExhausted after the burst.
        let mut keys = HashMap::new();
        keys.insert("rate".to_string(), Role::Read);
        let ic = ApiKeyInterceptor::with_roles(keys);
        let mut limits = HashMap::new();
        limits.insert("rate".to_string(), 2u32);
        ic.set_rate_limits(limits);

        assert!(ic.authorize(&make_request_with_key("rate"), Permission::Read).is_ok());
        assert!(ic.authorize(&make_request_with_key("rate"), Permission::Read).is_ok());
        let err = ic.authorize(&make_request_with_key("rate"), Permission::Read).unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted,
                   "3rd request must get ResourceExhausted, got: {err:?}");
    }

    // ── G5: namespace binding ──

    #[test]
    fn test_ns_unbound_key_passes_any_table() {
        let mut keys = HashMap::new();
        keys.insert("free".to_string(), Role::Admin);
        let ic = ApiKeyInterceptor::with_roles(keys);
        // No namespace set for this key → every table name accepted.
        let r = make_request_with_key("free");
        assert!(ic.check_namespace(&r, "anything").is_ok());
        let r = make_request_with_key("free");
        assert!(ic.check_namespace(&r, "tenant-a/orders").is_ok());
    }

    #[test]
    fn test_ns_bound_key_requires_prefix() {
        let mut keys = HashMap::new();
        keys.insert("alice".to_string(), Role::Write);
        let ic = ApiKeyInterceptor::with_roles(keys);
        let mut ns = HashMap::new();
        ns.insert("alice".to_string(), "tenant-a".to_string());
        ic.set_namespaces(ns);

        // Own namespace: allowed.
        let r = make_request_with_key("alice");
        assert!(ic.check_namespace(&r, "tenant-a/orders").is_ok());

        // Wrong namespace: PermissionDenied.
        let r = make_request_with_key("alice");
        let err = ic.check_namespace(&r, "tenant-b/orders").unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("tenant-a"),
                "error should name the expected namespace, got: {err:?}");

        // Bare name (no prefix): rejected.
        let r = make_request_with_key("alice");
        assert!(ic.check_namespace(&r, "orders").is_err());

        // Prefix-match-but-different-ns: rejected. `tenant-abc/` must not
        // satisfy a binding of `tenant-a`.
        let r = make_request_with_key("alice");
        assert!(ic.check_namespace(&r, "tenant-abc/orders").is_err(),
                "prefix check must demand a trailing slash");
    }

    #[test]
    fn test_ns_filter_hides_other_tenants_in_list() {
        let mut keys = HashMap::new();
        keys.insert("alice".to_string(), Role::Read);
        let ic = ApiKeyInterceptor::with_roles(keys);
        let mut ns = HashMap::new();
        ns.insert("alice".to_string(), "tenant-a".to_string());
        ic.set_namespaces(ns);

        let all = vec![
            "tenant-a/orders".to_string(),
            "tenant-b/leads".to_string(),
            "global/metrics".to_string(),
            "tenant-a/customers".to_string(),
        ];
        let r = make_request_with_key("alice");
        let filtered = ic.filter_namespace(&r, all);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|t| t.starts_with("tenant-a/")));
    }

    #[test]
    fn test_ns_unbound_key_sees_full_list() {
        let mut keys = HashMap::new();
        keys.insert("op".to_string(), Role::Admin);
        let ic = ApiKeyInterceptor::with_roles(keys);
        let all = vec!["a/x".to_string(), "b/y".to_string()];
        let r = make_request_with_key("op");
        let filtered = ic.filter_namespace(&r, all.clone());
        assert_eq!(filtered, all,
                   "unbound operator key must see every table name");
    }

    #[test]
    fn test_ns_empty_string_is_not_a_binding() {
        // Defensive: empty namespace string must not install a binding
        // — otherwise every table would need to start with `/`.
        let mut keys = HashMap::new();
        keys.insert("k".to_string(), Role::Read);
        let ic = ApiKeyInterceptor::with_roles(keys);
        let mut ns = HashMap::new();
        ns.insert("k".to_string(), "".to_string());
        ic.set_namespaces(ns);
        let r = make_request_with_key("k");
        assert!(ic.check_namespace(&r, "any/table").is_ok());
        let r = make_request_with_key("k");
        assert!(ic.check_namespace(&r, "bare").is_ok());
    }

    // ── D4: property tests for namespace prefix matching ──
    //
    // Unit tests above hit specific edge cases; these assert
    // cross-cutting invariants across a large random corpus.

    use proptest::prelude::*;

    // A namespace-friendly string regex: [a-z0-9-]{1,12} so generated
    // names mirror the kind of tenant IDs real deployments use
    // (DNS-label subset, no slashes, no dots).
    fn ns_regex() -> &'static str { "[a-z0-9-]{1,12}" }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

        // Invariant (a): a namespace-bound key calling a table name
        // that does NOT start with `{ns}/` is always denied.
        #[test]
        fn prop_ns_rejects_names_outside_prefix(
            ns in ns_regex(),
            other_ns in ns_regex(),
            leaf in ns_regex(),
        ) {
            prop_assume!(ns != other_ns);
            let mut keys = HashMap::new();
            keys.insert("k".to_string(), Role::Admin);
            let ic = ApiKeyInterceptor::with_roles(keys);
            let mut m = HashMap::new();
            m.insert("k".to_string(), ns.clone());
            ic.set_namespaces(m);
            let req = make_request_with_key("k");
            let table = format!("{other_ns}/{leaf}");
            prop_assert!(
                ic.check_namespace(&req, &table).is_err(),
                "ns={ns} should deny {table}"
            );
        }

        // Invariant (b): a namespace-bound key calling a table name
        // that starts with exactly `{ns}/` is always allowed.
        #[test]
        fn prop_ns_accepts_names_within_prefix(
            ns in ns_regex(),
            leaf in ns_regex(),
        ) {
            let mut keys = HashMap::new();
            keys.insert("k".to_string(), Role::Admin);
            let ic = ApiKeyInterceptor::with_roles(keys);
            let mut m = HashMap::new();
            m.insert("k".to_string(), ns.clone());
            ic.set_namespaces(m);
            let req = make_request_with_key("k");
            let table = format!("{ns}/{leaf}");
            prop_assert!(
                ic.check_namespace(&req, &table).is_ok(),
                "ns={ns} should allow {table}"
            );
        }

        // Invariant (c): `filter_namespace` output is a subset of
        // the input, preserves order, and contains only `{ns}/`-prefixed
        // entries. Guards against the `tenant-a / tenant-abc` prefix-
        // collision class of bugs.
        #[test]
        fn prop_filter_namespace_preserves_order_and_is_prefix_safe(
            ns in ns_regex(),
            tables in proptest::collection::vec(ns_regex(), 0..20),
        ) {
            let mut keys = HashMap::new();
            keys.insert("k".to_string(), Role::Read);
            let ic = ApiKeyInterceptor::with_roles(keys);
            let mut m = HashMap::new();
            m.insert("k".to_string(), ns.clone());
            ic.set_namespaces(m);

            // Build a mix of own-ns, foreign-ns, and bare names.
            let mut input: Vec<String> = Vec::new();
            for (i, t) in tables.iter().enumerate() {
                match i % 3 {
                    0 => input.push(format!("{ns}/{t}")),
                    1 => input.push(format!("x-{ns}/{t}")), // prefix-collision bait
                    _ => input.push(t.clone()),             // bare
                }
            }
            let req = make_request_with_key("k");
            let out = ic.filter_namespace(&req, input.clone());
            let required_prefix = format!("{ns}/");
            // Every output element must have the exact prefix.
            for o in &out {
                prop_assert!(o.starts_with(&required_prefix),
                             "filter_namespace leaked {o} which doesn't start with {required_prefix}");
            }
            // Output is a subset (every out element is in input).
            for o in &out {
                prop_assert!(input.contains(o));
            }
            // Order preservation: relative order within `out` matches
            // the subsequence found in `input`.
            let filtered_by_hand: Vec<&String> = input.iter()
                .filter(|s| s.starts_with(&required_prefix))
                .collect();
            prop_assert_eq!(out.len(), filtered_by_hand.len());
            for (a, b) in out.iter().zip(filtered_by_hand.iter()) {
                prop_assert_eq!(a, *b);
            }
        }

        // Invariant (d): for an UNBOUND key (no namespace installed),
        // check_namespace always succeeds and filter_namespace returns
        // the input unchanged.
        #[test]
        fn prop_unbound_key_is_pass_through(
            tables in proptest::collection::vec(ns_regex(), 0..20),
            probe in ns_regex(),
        ) {
            let mut keys = HashMap::new();
            keys.insert("op".to_string(), Role::Admin);
            let ic = ApiKeyInterceptor::with_roles(keys);
            // No set_namespaces call ⇒ "op" has no binding.
            let req = make_request_with_key("op");
            prop_assert!(ic.check_namespace(&req, &probe).is_ok());
            prop_assert!(ic.check_namespace(&req, "").is_ok());
            prop_assert!(ic.check_namespace(&req, "anything/at_all").is_ok());

            let req = make_request_with_key("op");
            let out = ic.filter_namespace(&req, tables.clone());
            prop_assert_eq!(out, tables);
        }
    }
}
