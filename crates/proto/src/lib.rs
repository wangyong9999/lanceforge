// Licensed under the Apache License, Version 2.0.
// Lance Distributed Proto — gRPC service definitions and shared message types.

pub mod generated {
    pub mod lance_distributed {
        include!("generated/lance.distributed.rs");
    }
}

pub mod descriptor;

// Re-export gRPC types at crate root.
pub use generated::lance_distributed::*;
