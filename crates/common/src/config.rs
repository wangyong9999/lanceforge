// Licensed under the Apache License, Version 2.0.
// Cluster configuration for LanceForge distributed retrieval.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Top-level cluster configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterConfig {
    #[serde(default)]
    pub tables: Vec<TableConfig>,
    #[serde(default)]
    pub executors: Vec<ExecutorConfig>,
    /// S3/GCS/Azure storage options (credentials, endpoint, etc.)
    /// Applied to all shard URIs that use object storage.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    /// Embedding model configuration.
    /// When set, text queries are auto-embedded to vectors before search.
    #[serde(default)]
    pub embedding: Option<EmbeddingConfig>,
    /// Server tuning (keepalive, timeouts, concurrency).
    #[serde(default)]
    pub server: ServerConfig,
    /// Query cache configuration (worker-side).
    #[serde(default)]
    pub cache: CacheConfig,
    /// Health check and data refresh configuration.
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    /// TLS and authentication configuration.
    #[serde(default)]
    pub security: SecurityConfig,
    /// Path to persistent metadata file (e.g., "/data/metadata.json" or "s3://bucket/metadata.json").
    /// When set, shard state survives coordinator restarts.
    #[serde(default)]
    pub metadata_path: Option<String>,
    /// Default storage path for runtime-created tables (CreateTable without explicit URI).
    #[serde(default)]
    pub default_table_path: Option<String>,
    /// Read-parallelism factor: how many workers are assigned to hold the same
    /// shard (same OBS URI). This is NOT a data-replication knob — data
    /// durability is provided by the object store (S3/GCS/Azure). The extra
    /// worker(s) exist purely to fan-out read queries and provide failover
    /// when the primary worker is unhealthy. YAML key kept as
    /// `replica_factor` for backward compatibility.
    #[serde(default = "ClusterConfig::default_replica_factor", alias = "read_parallelism")]
    pub replica_factor: usize,
    /// Worker-side periodic compaction.
    #[serde(default)]
    pub compaction: CompactionConfig,
    /// Coordinator-side orphan storage GC.
    #[serde(default)]
    pub orphan_gc: OrphanGcConfig,
    /// Deployment profile — selects which invariants are enforced at startup.
    /// See `DeploymentProfile` docs. Defaults to `Dev` for backward
    /// compatibility; production deployments should pin this explicitly.
    #[serde(default)]
    pub deployment_profile: DeploymentProfile,
}

impl ClusterConfig {
    fn default_replica_factor() -> usize { 2 }
}

/// Deployment profile — hints which statelessness invariants are required.
/// Selected per `ROADMAP_0.2.md` §0.1-0.2.
///
/// - `Dev`: no checks. Backward-compatible default. FileMetaStore allowed,
///   missing `metadata_path` allowed (falls back to StaticShardState).
/// - `SelfHosted`: `metadata_path` must be set; file:// and OBS URIs both
///   allowed. Multi-coordinator HA requires OBS but we don't force it here.
/// - `Saas`: `metadata_path` must be set AND must be an object-storage URI
///   (`s3://`, `gs://`, `az://`, or explicit `memory://` for tests). No
///   local-disk truth. Any violation is a startup fail-fast.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentProfile {
    #[default]
    Dev,
    SelfHosted,
    Saas,
}

impl DeploymentProfile {
    /// OBS URI schemes that satisfy SaaS statelessness. `memory://` is the
    /// in-process store used by tests; `file://` and bare local paths do not
    /// qualify.
    fn is_obs_scheme(path: &str) -> bool {
        const OBS_SCHEMES: &[&str] = &["s3://", "gs://", "az://", "azure://", "memory://"];
        OBS_SCHEMES.iter().any(|s| path.starts_with(s))
    }
}

/// Server-level tuning parameters (coordinator + worker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// gRPC query timeout in seconds.
    #[serde(default = "ServerConfig::default_query_timeout_secs")]
    pub query_timeout_secs: u64,
    /// HTTP/2 keepalive interval in seconds.
    #[serde(default = "ServerConfig::default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,
    /// HTTP/2 keepalive timeout in seconds.
    #[serde(default = "ServerConfig::default_keepalive_timeout_secs")]
    pub keepalive_timeout_secs: u64,
    /// Max concurrent gRPC streams per connection.
    #[serde(default = "ServerConfig::default_concurrency_limit")]
    pub concurrency_limit: usize,
    /// Max concurrent queries before backpressure (RESOURCE_EXHAUSTED).
    #[serde(default = "ServerConfig::default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,
    /// Query vector oversampling factor (fetch k*oversample from each shard, merge to k).
    #[serde(default = "ServerConfig::default_oversample_factor")]
    pub oversample_factor: u32,
    /// Maximum response payload size in bytes. Responses exceeding this are truncated
    /// (rows dropped from the tail) and `truncated=true` is set in the response.
    /// Default: 64 MiB. Set to 0 to disable the cap (NOT recommended).
    #[serde(default = "ServerConfig::default_max_response_bytes")]
    pub max_response_bytes: usize,
    /// Hard upper limit on `k` (and `k+offset` for paginated requests) per call.
    /// Prevents runaway requests that try to fetch millions of rows in one shot.
    /// Default: 10000.
    #[serde(default = "ServerConfig::default_max_k")]
    pub max_k: u32,
    /// Slow-query threshold in milliseconds. Queries exceeding this emit a
    /// structured warn-level log. Default: 1000 ms. Set to 0 to disable.
    #[serde(default = "ServerConfig::default_slow_query_ms")]
    pub slow_query_ms: u64,
}

impl ServerConfig {
    fn default_query_timeout_secs() -> u64 { 30 }
    fn default_keepalive_interval_secs() -> u64 { 10 }
    fn default_keepalive_timeout_secs() -> u64 { 20 }
    fn default_concurrency_limit() -> usize { 256 }
    fn default_max_concurrent_queries() -> usize { 200 }
    fn default_oversample_factor() -> u32 { 2 }
    fn default_max_response_bytes() -> usize { 256 * 1024 * 1024 }
    fn default_max_k() -> u32 { 10_000 }
    fn default_slow_query_ms() -> u64 { 1000 }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            query_timeout_secs: Self::default_query_timeout_secs(),
            keepalive_interval_secs: Self::default_keepalive_interval_secs(),
            keepalive_timeout_secs: Self::default_keepalive_timeout_secs(),
            concurrency_limit: Self::default_concurrency_limit(),
            max_concurrent_queries: Self::default_max_concurrent_queries(),
            oversample_factor: Self::default_oversample_factor(),
            max_response_bytes: Self::default_max_response_bytes(),
            max_k: Self::default_max_k(),
            slow_query_ms: Self::default_slow_query_ms(),
        }
    }
}

/// Orphan storage GC configuration (coordinator-side).
/// Scans the configured `default_table_path` for `.lance` directories that
/// are not referenced by any known shard and deletes them. Guards against
/// leaked datasets from failed CreateTable / rebalance / DropTable calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrphanGcConfig {
    #[serde(default = "OrphanGcConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "OrphanGcConfig::default_interval_secs")]
    pub interval_secs: u64,
    /// Minimum age (in seconds) before an unreferenced dataset is eligible
    /// for deletion. Protects datasets that are in the middle of being
    /// created/registered. Default: 3600 (1h).
    #[serde(default = "OrphanGcConfig::default_min_age_secs")]
    pub min_age_secs: u64,
}

impl OrphanGcConfig {
    fn default_enabled() -> bool { false }           // opt-in: destructive
    fn default_interval_secs() -> u64 { 3600 }       // 1h
    fn default_min_age_secs() -> u64 { 3600 }        // 1h
}

impl Default for OrphanGcConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            interval_secs: Self::default_interval_secs(),
            min_age_secs: Self::default_min_age_secs(),
        }
    }
}

/// Background compaction configuration for workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Whether the periodic compaction loop is enabled. Default: true.
    #[serde(default = "CompactionConfig::default_enabled")]
    pub enabled: bool,
    /// Interval in seconds between compaction sweeps. Default: 3600 (1h).
    #[serde(default = "CompactionConfig::default_interval_secs")]
    pub interval_secs: u64,
    /// Skip compaction if the worker is busy (loaded shards count > 0 always allows).
    #[serde(default)]
    pub skip_if_busy: bool,
}

impl CompactionConfig {
    fn default_enabled() -> bool { true }
    fn default_interval_secs() -> u64 { 3600 }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            interval_secs: Self::default_interval_secs(),
            skip_if_busy: false,
        }
    }
}

/// Query cache configuration for workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Cache TTL in seconds.
    #[serde(default = "CacheConfig::default_ttl_secs")]
    pub ttl_secs: u64,
    /// Maximum number of cached entries.
    #[serde(default = "CacheConfig::default_max_entries")]
    pub max_entries: usize,
    /// Maximum total cache memory in bytes. Prevents OOM from large RecordBatch
    /// caching. Default: 512 MB. Set to 0 to disable byte-level cap.
    #[serde(default = "CacheConfig::default_max_cache_bytes")]
    pub max_cache_bytes: usize,
    /// Lance dataset read consistency interval in seconds (worker-side).
    #[serde(default = "CacheConfig::default_read_consistency_secs")]
    pub read_consistency_secs: u64,
}

impl CacheConfig {
    fn default_ttl_secs() -> u64 { 5 }
    fn default_max_entries() -> usize { 1000 }
    fn default_max_cache_bytes() -> usize { 512 * 1024 * 1024 } // 512 MB
    fn default_read_consistency_secs() -> u64 { 3 }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: Self::default_ttl_secs(),
            max_entries: Self::default_max_entries(),
            max_cache_bytes: Self::default_max_cache_bytes(),
            read_consistency_secs: Self::default_read_consistency_secs(),
        }
    }
}

/// Health check configuration for coordinator→worker monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Health check interval in seconds.
    #[serde(default = "HealthCheckConfig::default_interval_secs")]
    pub interval_secs: u64,
    /// Number of consecutive failures before marking unhealthy.
    #[serde(default = "HealthCheckConfig::default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    /// Number of consecutive failures before removing executor.
    #[serde(default = "HealthCheckConfig::default_remove_threshold")]
    pub remove_threshold: u32,
    /// Dataset refresh interval in seconds (worker-side).
    #[serde(default = "HealthCheckConfig::default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
    /// Shard reconciliation interval in seconds.
    #[serde(default = "HealthCheckConfig::default_reconciliation_interval_secs")]
    pub reconciliation_interval_secs: u64,
    /// Worker connection timeout in seconds.
    #[serde(default = "HealthCheckConfig::default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Health check ping timeout in seconds.
    #[serde(default = "HealthCheckConfig::default_ping_timeout_secs")]
    pub ping_timeout_secs: u64,
}

impl HealthCheckConfig {
    fn default_interval_secs() -> u64 { 10 }
    fn default_unhealthy_threshold() -> u32 { 3 }
    fn default_remove_threshold() -> u32 { 5 }
    fn default_refresh_interval_secs() -> u64 { 3 }
    fn default_reconciliation_interval_secs() -> u64 { 5 }
    fn default_connect_timeout_secs() -> u64 { 3 }
    fn default_ping_timeout_secs() -> u64 { 3 }
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_secs: Self::default_interval_secs(),
            unhealthy_threshold: Self::default_unhealthy_threshold(),
            remove_threshold: Self::default_remove_threshold(),
            refresh_interval_secs: Self::default_refresh_interval_secs(),
            reconciliation_interval_secs: Self::default_reconciliation_interval_secs(),
            connect_timeout_secs: Self::default_connect_timeout_secs(),
            ping_timeout_secs: Self::default_ping_timeout_secs(),
        }
    }
}

/// TLS and authentication configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Path to PEM-encoded server certificate (enables TLS when set).
    #[serde(default)]
    pub tls_cert: Option<String>,
    /// Path to PEM-encoded server private key.
    #[serde(default)]
    pub tls_key: Option<String>,
    /// Path to PEM-encoded CA certificate for client-side verification.
    /// When set, coordinator verifies worker TLS certs against this CA.
    /// Also used by Python SDK for verifying coordinator/worker certs.
    #[serde(default)]
    pub tls_ca_cert: Option<String>,
    /// API keys for client authentication (empty = no auth required).
    /// Legacy format: all keys grant Admin role.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// Role-based API keys: each entry binds a key to a role (read/write/admin).
    /// When non-empty, overrides `api_keys`. Keys listed here gain only the
    /// stated role; missing keys are rejected.
    #[serde(default)]
    pub api_keys_rbac: Vec<ApiKeyEntry>,
}

/// Single role-bound API key entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyEntry {
    pub key: String,
    /// "read", "write", or "admin" (case-insensitive).
    pub role: String,
    /// Optional per-key QPS cap (G6). 0 or absent = unlimited. When set,
    /// the coordinator enforces a token-bucket rate limit on this key and
    /// returns `ResourceExhausted` when the bucket is empty. Sized to the
    /// raw RPC call rate, not per-query cost — a client making 100
    /// CreateIndex/s is rate-limited the same as 100 AnnSearch/s.
    #[serde(default)]
    pub qps_limit: u32,
}

impl SecurityConfig {
    /// Whether TLS is configured (server-side).
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert.is_some() && self.tls_key.is_some()
    }
    /// Whether client-side TLS verification is configured.
    pub fn tls_client_enabled(&self) -> bool {
        self.tls_ca_cert.is_some()
    }
    /// Whether API key authentication is required.
    pub fn auth_enabled(&self) -> bool {
        !self.api_keys.is_empty() || !self.api_keys_rbac.is_empty()
    }
}

/// Configuration for query-time text→vector embedding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    /// Model provider: "openai", "sentence-transformers"
    pub provider: String,
    /// Model name (e.g., "text-embedding-3-small", "all-MiniLM-L6-v2")
    pub model: String,
    /// API key (for OpenAI). Can also be set via OPENAI_API_KEY env var.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Output dimension
    #[serde(default)]
    pub dimension: Option<u32>,
}

/// Configuration for a logical table and its shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// Logical table name (what users query)
    pub name: String,
    /// Shards comprising this table
    pub shards: Vec<ShardConfig>,
}

/// Configuration for a single shard (one Lance table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    /// Shard identifier
    pub name: String,
    /// Lance table URI (s3://bucket/path/shard.lance)
    pub uri: String,
    /// Executor IDs this shard is assigned to
    pub executors: Vec<String>,
}

/// Configuration for an Executor node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutorConfig {
    /// Unique executor identifier
    pub id: String,
    /// Host address
    pub host: String,
    /// gRPC port
    pub port: u16,
    /// Physical read/write role preference (ROADMAP_0.2 §B2.2).
    /// Defaults to `Either` — the worker accepts both reads and writes,
    /// preserving 0.1.x behaviour. When `replica_factor >= 2`, operators
    /// can dedicate distinct workers: `WritePrimary` absorbs write I/O,
    /// `ReadPrimary` serves query traffic unchallenged. Role is a soft
    /// preference — if only one role is healthy for a shard, it still
    /// accepts both kinds of traffic as a fallback.
    #[serde(default)]
    pub role: ExecutorRole,
}

/// Soft routing preference for an executor. Applied by the coordinator
/// when more than one worker serves the same shard: readers prefer
/// `ReadPrimary` > `Either` > `WritePrimary`; writers prefer
/// `WritePrimary` > `Either` > `ReadPrimary`. Single-candidate shards
/// ignore the preference entirely.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorRole {
    #[default]
    Either,
    ReadPrimary,
    WritePrimary,
}

impl ClusterConfig {
    /// Load config from a YAML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: ClusterConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Build a shard-to-executor routing table.
    /// Returns: table_name → [(shard_name, executor_ids)]
    pub fn build_routing_table(&self) -> HashMap<String, Vec<(String, Vec<String>)>> {
        let mut routing = HashMap::new();
        for table in &self.tables {
            let shard_routes: Vec<(String, Vec<String>)> = table
                .shards
                .iter()
                .map(|s| (s.name.clone(), s.executors.clone()))
                .collect();
            routing.insert(table.name.clone(), shard_routes);
        }
        routing
    }

    /// Validate configuration for common errors. Returns list of problems.
    pub fn validate(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        // Executor ID uniqueness
        let mut seen_ids = std::collections::HashSet::new();
        for e in &self.executors {
            if e.id.is_empty() {
                errors.push("executor id must not be empty".to_string());
            }
            if !seen_ids.insert(&e.id) {
                errors.push(format!("duplicate executor id: {}", e.id));
            }
            if e.port == 0 {
                errors.push(format!("executor '{}': port must be > 0", e.id));
            }
        }
        // Table name uniqueness + shard executor references
        let executor_ids: std::collections::HashSet<&str> = self.executors.iter().map(|e| e.id.as_str()).collect();
        let mut seen_tables = std::collections::HashSet::new();
        for table in &self.tables {
            if table.name.is_empty() {
                errors.push("table name must not be empty".to_string());
            }
            if !seen_tables.insert(&table.name) {
                errors.push(format!("duplicate table name: {}", table.name));
            }
            for shard in &table.shards {
                for eid in &shard.executors {
                    if !executor_ids.contains(eid.as_str()) {
                        errors.push(format!(
                            "shard '{}' references unknown executor '{}'", shard.name, eid
                        ));
                    }
                }
            }
        }
        // Server config sanity
        if self.server.query_timeout_secs == 0 {
            errors.push("server.query_timeout_secs must be > 0".to_string());
        }
        if self.server.max_k == 0 {
            errors.push("server.max_k must be > 0".to_string());
        }
        if self.server.oversample_factor == 0 {
            errors.push("server.oversample_factor must be > 0 \
                         (1 = no oversample, the default 2 = fetch 2*k per shard)".to_string());
        }
        if self.server.max_concurrent_queries == 0 {
            errors.push("server.max_concurrent_queries must be > 0 \
                         (a 0-sized semaphore permanently blocks every query)".to_string());
        }
        // Cluster-wide bounds (H1 hardening).
        if self.replica_factor == 0 {
            errors.push("replica_factor must be >= 1 \
                         (0 would leave every shard unassigned)".to_string());
        }
        if self.health_check.interval_secs == 0 {
            errors.push("health_check.interval_secs must be > 0 \
                         (a 0-interval health loop would hot-spin)".to_string());
        }
        // Security: empty-string keys are almost certainly a config bug and
        // would authenticate any client that happens to send `Bearer `.
        for (idx, k) in self.security.api_keys.iter().enumerate() {
            if k.is_empty() {
                errors.push(format!(
                    "security.api_keys[{idx}] is an empty string — remove it or assign a non-empty secret"
                ));
            }
        }
        for (idx, entry) in self.security.api_keys_rbac.iter().enumerate() {
            if entry.key.is_empty() {
                errors.push(format!(
                    "security.api_keys_rbac[{idx}].key is empty — remove it or assign a non-empty secret"
                ));
            }
        }
        // Deployment profile invariants (ROADMAP_0.2 §0.1-0.2).
        match self.deployment_profile {
            DeploymentProfile::Dev => {}
            DeploymentProfile::SelfHosted => {
                if self.metadata_path.is_none() {
                    errors.push(
                        "deployment_profile=self_hosted requires metadata_path to be set \
                         (FileMetaStore for single-coord, OBS URI for multi-coord HA)".to_string()
                    );
                }
            }
            DeploymentProfile::Saas => {
                match &self.metadata_path {
                    None => errors.push(
                        "deployment_profile=saas requires metadata_path to be an OBS URI \
                         (s3://, gs://, az://); local paths are not allowed because any \
                         pod replacement would lose truth".to_string()
                    ),
                    Some(p) if !DeploymentProfile::is_obs_scheme(p) => errors.push(format!(
                        "deployment_profile=saas requires metadata_path to use an OBS scheme \
                         (s3://, gs://, az://), got: {}", p
                    )),
                    _ => {}
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// Get all shards assigned to a specific executor.
    pub fn shards_for_executor(&self, executor_id: &str) -> Vec<ShardConfig> {
        let mut shards = Vec::new();
        for table in &self.tables {
            for shard in &table.shards {
                if shard.executors.contains(&executor_id.to_string()) {
                    shards.push(shard.clone());
                }
            }
        }
        shards
    }

    /// Get all executor IDs that hold shards for a given table.
    pub fn executors_for_table(&self, table_name: &str) -> Vec<String> {
        let mut executor_ids: Vec<String> = Vec::new();
        for table in &self.tables {
            if table.name == table_name {
                for shard in &table.shards {
                    for eid in &shard.executors {
                        if !executor_ids.contains(eid) {
                            executor_ids.push(eid.clone());
                        }
                    }
                }
            }
        }
        executor_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parse() {
        let yaml = r#"
tables:
  - name: products
    shards:
      - name: products_shard_00
        uri: "s3://bucket/db/products_shard_00.lance"
        executors: ["executor_0"]
      - name: products_shard_01
        uri: "s3://bucket/db/products_shard_01.lance"
        executors: ["executor_0"]
      - name: products_shard_02
        uri: "s3://bucket/db/products_shard_02.lance"
        executors: ["executor_1"]
executors:
  - id: executor_0
    host: "127.0.0.1"
    port: 50051
  - id: executor_1
    host: "127.0.0.1"
    port: 50052
"#;

        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.tables.len(), 1);
        assert_eq!(config.tables[0].shards.len(), 3);
        assert_eq!(config.executors.len(), 2);

        // Test routing table
        let routing = config.build_routing_table();
        let products_routes = routing.get("products").unwrap();
        assert_eq!(products_routes.len(), 3);

        // Test shards_for_executor
        let shards_0 = config.shards_for_executor("executor_0");
        assert_eq!(shards_0.len(), 2);
        let shards_1 = config.shards_for_executor("executor_1");
        assert_eq!(shards_1.len(), 1);

        // Test executors_for_table
        let executors = config.executors_for_table("products");
        assert_eq!(executors.len(), 2);
        assert!(executors.contains(&"executor_0".to_string()));
        assert!(executors.contains(&"executor_1".to_string()));
    }

    #[test]
    fn test_server_config_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.query_timeout_secs, 30);
        assert_eq!(cfg.keepalive_interval_secs, 10);
        assert_eq!(cfg.keepalive_timeout_secs, 20);
        assert_eq!(cfg.concurrency_limit, 256);
    }

    #[test]
    fn test_cache_config_defaults() {
        let cfg = CacheConfig::default();
        assert_eq!(cfg.ttl_secs, 5);
        assert_eq!(cfg.max_entries, 1000);
    }

    #[test]
    fn test_health_check_config_defaults() {
        let cfg = HealthCheckConfig::default();
        assert_eq!(cfg.interval_secs, 10);
        assert_eq!(cfg.unhealthy_threshold, 3);
        assert_eq!(cfg.remove_threshold, 5);
        assert_eq!(cfg.refresh_interval_secs, 3);
        assert_eq!(cfg.reconciliation_interval_secs, 5);
    }

    #[test]
    fn test_config_backward_compat() {
        // Config without new fields should use defaults
        let yaml = r#"
tables: []
executors: []
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server.query_timeout_secs, 30);
        assert_eq!(config.cache.ttl_secs, 5);
        assert_eq!(config.health_check.interval_secs, 10);
    }

    /// Explicit B1.2.4 upgrade-path test: a config written against 0.1.0
    /// schema (no `deployment_profile`, no new fields) must still parse,
    /// default to `dev`, and pass validation. Breaking this test is a
    /// breaking change that requires a major version bump.
    #[test]
    fn test_config_0_1_yaml_upgrades_cleanly() {
        // Representative 0.1.0-era config with no knowledge of
        // deployment_profile. Includes the fields that were stable in 0.1.x.
        let yaml = r#"
tables: []
executors:
  - {id: w0, host: "127.0.0.1", port: 50100}
  - {id: w1, host: "127.0.0.1", port: 50101}
default_table_path: /var/lib/lanceforge
replica_factor: 2
server:
  query_timeout_secs: 30
  max_response_bytes: 67108864
cache:
  ttl_secs: 5
security:
  api_keys: ["legacy-key-1"]
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).expect("0.1 YAML must still parse");
        assert_eq!(config.deployment_profile, DeploymentProfile::Dev,
                   "missing deployment_profile must default to Dev to preserve 0.1 behaviour");
        // Validation must not introduce new errors on old configs.
        assert!(config.validate().is_ok(), "0.1 YAML must still validate");
    }

    #[test]
    fn test_config_custom_values() {
        let yaml = r#"
tables: []
executors: []
server:
  query_timeout_secs: 60
  concurrency_limit: 512
cache:
  ttl_secs: 10
  max_entries: 5000
health_check:
  interval_secs: 5
  unhealthy_threshold: 5
  remove_threshold: 10
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server.query_timeout_secs, 60);
        assert_eq!(config.server.concurrency_limit, 512);
        assert_eq!(config.cache.ttl_secs, 10);
        assert_eq!(config.cache.max_entries, 5000);
        assert_eq!(config.health_check.interval_secs, 5);
        assert_eq!(config.health_check.unhealthy_threshold, 5);
    }

    #[test]
    fn test_security_config_defaults() {
        let cfg = SecurityConfig::default();
        assert!(!cfg.tls_enabled());
        assert!(!cfg.auth_enabled());
        assert!(cfg.api_keys.is_empty());
    }

    #[test]
    fn test_security_config_with_auth() {
        let yaml = r#"
tables: []
executors: []
security:
  api_keys:
    - "key-1"
    - "key-2"
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.security.auth_enabled());
        assert!(!config.security.tls_enabled());
        assert_eq!(config.security.api_keys.len(), 2);
    }

    #[test]
    fn test_security_config_with_tls() {
        let yaml = r#"
tables: []
executors: []
security:
  tls_cert: "/etc/ssl/cert.pem"
  tls_key: "/etc/ssl/key.pem"
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.security.tls_enabled());
        assert!(!config.security.auth_enabled());
    }

    #[test]
    fn test_validate_ok() {
        let yaml = r#"
tables:
  - name: t1
    shards:
      - {name: s0, uri: "s3://b/s0.lance", executors: ["w0"]}
executors:
  - {id: w0, host: "127.0.0.1", port: 50100}
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_duplicate_executor() {
        let yaml = r#"
tables: []
executors:
  - {id: w0, host: "127.0.0.1", port: 50100}
  - {id: w0, host: "127.0.0.1", port: 50101}
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("duplicate"), "expected duplicate error: {}", err);
    }

    #[test]
    fn test_validate_bad_executor_ref() {
        let yaml = r#"
tables:
  - name: t1
    shards:
      - {name: s0, uri: "x.lance", executors: ["w_missing"]}
executors:
  - {id: w0, host: "127.0.0.1", port: 50100}
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("unknown executor"), "expected ref error: {}", err);
    }

    #[test]
    fn test_validate_zero_port() {
        let yaml = r#"
tables: []
executors:
  - {id: w0, host: "127.0.0.1", port: 0}
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }

    fn saas_base_yaml() -> &'static str {
        r#"
tables: []
executors:
  - {id: w0, host: "127.0.0.1", port: 50100}
"#
    }

    #[test]
    fn test_deployment_profile_default_is_dev() {
        let config: ClusterConfig = serde_yaml::from_str(saas_base_yaml()).unwrap();
        assert_eq!(config.deployment_profile, DeploymentProfile::Dev);
    }

    #[test]
    fn test_profile_dev_allows_missing_metadata() {
        // Default (dev) path must stay permissive — backward compat.
        let yaml = format!("{}\ndeployment_profile: dev\n", saas_base_yaml());
        let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_profile_self_hosted_requires_metadata_path() {
        let yaml = format!("{}\ndeployment_profile: self_hosted\n", saas_base_yaml());
        let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("self_hosted"), "got: {err}");
        assert!(err.contains("metadata_path"), "got: {err}");
    }

    #[test]
    fn test_profile_self_hosted_accepts_local_metadata() {
        let yaml = format!(
            "{}\ndeployment_profile: self_hosted\nmetadata_path: /data/meta.json\n",
            saas_base_yaml()
        );
        let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_profile_saas_rejects_missing_metadata() {
        let yaml = format!("{}\ndeployment_profile: saas\n", saas_base_yaml());
        let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("saas"), "got: {err}");
        assert!(err.contains("OBS"), "got: {err}");
    }

    #[test]
    fn test_profile_saas_rejects_local_metadata() {
        let yaml = format!(
            "{}\ndeployment_profile: saas\nmetadata_path: /data/meta.json\n",
            saas_base_yaml()
        );
        let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("saas"), "got: {err}");
        assert!(err.contains("/data/meta.json"), "got: {err}");
    }

    #[test]
    fn test_profile_saas_accepts_s3() {
        let yaml = format!(
            "{}\ndeployment_profile: saas\nmetadata_path: s3://bucket/meta.json\n",
            saas_base_yaml()
        );
        let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_profile_saas_accepts_gs_and_azure() {
        for path in ["gs://bucket/meta.json", "az://container/meta.json", "azure://container/meta.json"] {
            let yaml = format!(
                "{}\ndeployment_profile: saas\nmetadata_path: {}\n",
                saas_base_yaml(), path
            );
            let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
            assert!(config.validate().is_ok(), "scheme rejected: {path}");
        }
    }

    // ── H1: bounds-validation hardening ──

    #[test]
    fn test_validate_rejects_zero_oversample_factor() {
        let yaml = r#"
tables: []
executors: [{id: w0, host: "127.0.0.1", port: 50100}]
server:
  oversample_factor: 0
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("oversample_factor"), "got: {err}");
    }

    #[test]
    fn test_validate_rejects_zero_max_concurrent_queries() {
        let yaml = r#"
tables: []
executors: [{id: w0, host: "127.0.0.1", port: 50100}]
server:
  max_concurrent_queries: 0
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("max_concurrent_queries"), "got: {err}");
    }

    #[test]
    fn test_validate_rejects_zero_replica_factor() {
        let yaml = r#"
tables: []
executors: [{id: w0, host: "127.0.0.1", port: 50100}]
replica_factor: 0
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("replica_factor"), "got: {err}");
    }

    #[test]
    fn test_validate_rejects_zero_health_check_interval() {
        let yaml = r#"
tables: []
executors: [{id: w0, host: "127.0.0.1", port: 50100}]
health_check:
  interval_secs: 0
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("health_check.interval_secs"), "got: {err}");
    }

    #[test]
    fn test_validate_rejects_empty_api_key_legacy() {
        let yaml = r#"
tables: []
executors: [{id: w0, host: "127.0.0.1", port: 50100}]
security:
  api_keys: ["real-key", ""]
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("api_keys[1]"), "got: {err}");
    }

    #[test]
    fn test_validate_rejects_empty_api_key_rbac() {
        let yaml = r#"
tables: []
executors: [{id: w0, host: "127.0.0.1", port: 50100}]
security:
  api_keys_rbac:
    - {key: "real", role: "read"}
    - {key: "", role: "admin"}
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.contains("api_keys_rbac[1]"), "got: {err}");
    }

    #[test]
    fn test_validate_ok_sane_defaults() {
        // Regression: the default config must continue to pass validate().
        let yaml = r#"
tables: []
executors: [{id: w0, host: "127.0.0.1", port: 50100}]
"#;
        let config: ClusterConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_ok(),
                "default config must validate after bounds hardening");
    }

    #[test]
    fn test_profile_saas_rejects_file_scheme() {
        // file:// is explicitly not OBS — local disk with a different name.
        let yaml = format!(
            "{}\ndeployment_profile: saas\nmetadata_path: file:///data/meta.json\n",
            saas_base_yaml()
        );
        let config: ClusterConfig = serde_yaml::from_str(&yaml).unwrap();
        assert!(config.validate().is_err());
    }
}
