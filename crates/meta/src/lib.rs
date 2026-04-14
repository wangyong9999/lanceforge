// Licensed under the Apache License, Version 2.0.
// lance-distributed-meta: metadata management, HA, and shard orchestration.
//
// Provides:
// - MetaStore: versioned KV store (file/S3/etcd backends)
// - Session: worker registration with TTL heartbeat
// - ShardManager: replica-aware shard assignment with affinity

pub mod store;
pub mod session;
pub mod shard_manager;
pub mod state;
