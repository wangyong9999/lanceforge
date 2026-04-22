// Licensed under the Apache License, Version 2.0.
// Lance Distributed Coordinator — query routing, scatter-gather, merge.
#![allow(unexpected_cfgs, clippy::too_many_arguments)]

pub mod auth;
pub mod connection_pool;
pub mod embedding;
pub mod ha;
pub mod merge;
pub mod rest;
pub mod scatter_gather;
pub mod service;
pub mod worker_select;

#[cfg(test)]
pub(crate) mod test_support;

// Convenience re-exports
pub use service::CoordinatorService;
