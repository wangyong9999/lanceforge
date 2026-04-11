// Licensed under the Apache License, Version 2.0.
// Lance Distributed — re-exports from split crates.
// Integration tests import from `lanceforge::*` — this facade maps to new crates.

// ── Common ──
pub use lance_distributed_common::config;
pub use lance_distributed_common::ipc;
pub use lance_distributed_common::metrics;
pub use lance_distributed_common::shard_pruning;
pub use lance_distributed_common::shard_state;

// ── Coordinator ──
pub use lance_distributed_coordinator as coordinator;
pub use lance_distributed_coordinator::embedding;
pub use lance_distributed_coordinator::merge;

// ── Worker ──
pub use lance_distributed_worker as worker;
pub use lance_distributed_worker::cache;
pub use lance_distributed_worker::executor;

// ── Proto ──
pub mod generated {
    pub use lance_distributed_proto::generated::*;
}
pub mod codec {
    pub mod proto {
        pub use lance_distributed_proto::descriptor::*;
    }
}

// ── Legacy (InProcess scatter-gather, used by test_distributed_ann) ──
#[allow(dead_code)]
pub mod scheduler;
