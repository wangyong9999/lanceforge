// Shared primitives for role runners.

use std::sync::Arc;
use tokio::sync::Notify;

/// Shutdown signal shared by all roles in a process. Firing this
/// starts the drain: background tasks exit their loops, gRPC
/// servers stop accepting, and `run()` futures resolve.
///
/// The monolith binary (Phase B) gives every role the same Notify
/// so a single SIGTERM fans out to all of them in parallel — this
/// matches the existing coord shape (see coordinator/src/bin/main.rs
/// `bg_shutdown`).
pub type ShutdownSignal = Arc<Notify>;

/// Create a fresh shutdown signal. Caller is responsible for
/// notifying waiters on SIGTERM / ctrl+c.
pub fn new_shutdown() -> ShutdownSignal {
    Arc::new(Notify::new())
}
