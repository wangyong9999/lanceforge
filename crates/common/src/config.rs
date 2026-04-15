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
}

impl ClusterConfig {
    fn default_replica_factor() -> usize { 2 }
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
    fn default_max_response_bytes() -> usize { 64 * 1024 * 1024 }
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
    /// Lance dataset read consistency interval in seconds (worker-side).
    #[serde(default = "CacheConfig::default_read_consistency_secs")]
    pub read_consistency_secs: u64,
}

impl CacheConfig {
    fn default_ttl_secs() -> u64 { 5 }
    fn default_max_entries() -> usize { 1000 }
    fn default_read_consistency_secs() -> u64 { 3 }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: Self::default_ttl_secs(),
            max_entries: Self::default_max_entries(),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    /// Unique executor identifier
    pub id: String,
    /// Host address
    pub host: String,
    /// gRPC port
    pub port: u16,
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
}
