// Licensed under the Apache License, Version 2.0.
// Lance Distributed Common — shared config, state, pruning, metrics, IPC utils.
#![allow(unexpected_cfgs)]

pub mod audit;
pub mod auto_shard;
pub mod config;
pub mod ipc;
pub mod metrics;
// shard_pruning retired in R6 / v0.3 alpha.2 — Lance scalar indexes
// (BTREE/BITMAP/LABEL_LIST) subsume the hash-routing use case (ADR-003).
pub mod shard_state;
pub mod tls;
pub mod version;
