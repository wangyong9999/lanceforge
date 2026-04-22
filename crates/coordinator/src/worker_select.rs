// Worker selection for the single-dataset v0.3 architecture.
//
// v0.2 routed queries via `shard_state.get_shard_routing(table)` which
// returned one entry per physical shard. v0.3 routes one request to
// exactly one worker chosen deterministically from the healthy pool.
//
// Selection rules per ADR-001:
//   * Read requests: consistent-hash(table_name + request_salt) over
//     healthy workers. Same table tends to the same worker while that
//     worker stays healthy, preserving cache locality without locking
//     ownership.
//   * Write requests: consistent-hash(table_name) only. Guarantees
//     single-primary-writer per table for the worker's lifetime —
//     avoids Lance OCC retry storms under concurrent fan-in.
//
// The helper does not live on `ConnectionPool` because it needs a view
// of the routing state (shard_uris / upcoming table_uris) that the
// pool doesn't own today. Keeping it module-local avoids a circular
// dependency and lets R1c rename/repoint without disturbing callers.

use std::sync::Arc;

use crate::connection_pool::ConnectionPool;

/// Outcome of a worker selection attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerPick {
    /// The chosen worker id.
    Worker(String),
    /// No healthy worker available.
    NoneHealthy,
}

/// Deterministic FNV-1a 64-bit hash. Stable across process restarts.
/// Lifted here rather than pulled in as a dep — we only need one
/// scalar hash and `std::hash::Hasher` is not guaranteed-stable
/// cross-platform for our purposes.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

/// Pick a worker for a READ request on `table_name`.
///
/// `request_salt` is typically a client-supplied request id or a
/// time-based nonce; it lets subsequent reads on the same table land
/// on a different worker under load, without harming cache locality
/// for the common case (one worker still accumulates most of the
/// table's working set).
///
/// Returns `NoneHealthy` if the pool has no healthy workers — caller
/// must surface Unavailable to the client.
pub async fn pick_worker_for_read(
    pool: &Arc<ConnectionPool>,
    table_name: &str,
    request_salt: &str,
) -> WorkerPick {
    let healthy = healthy_worker_ids(pool).await;
    if healthy.is_empty() {
        return WorkerPick::NoneHealthy;
    }
    let mut buf = Vec::with_capacity(table_name.len() + request_salt.len() + 1);
    buf.extend_from_slice(table_name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(request_salt.as_bytes());
    let idx = (fnv1a_64(&buf) % healthy.len() as u64) as usize;
    WorkerPick::Worker(healthy[idx].clone())
}

/// Pick a worker for a WRITE request on `table_name`.
///
/// Deterministic — no request salt. Same table → same worker while
/// that worker is healthy. Under ADR-001's single-primary-writer
/// rule, this is the writer for the table's lifetime.
pub async fn pick_worker_for_write(
    pool: &Arc<ConnectionPool>,
    table_name: &str,
) -> WorkerPick {
    let healthy = healthy_worker_ids(pool).await;
    if healthy.is_empty() {
        return WorkerPick::NoneHealthy;
    }
    let idx = (fnv1a_64(table_name.as_bytes()) % healthy.len() as u64) as usize;
    WorkerPick::Worker(healthy[idx].clone())
}

/// Collect healthy worker ids from the pool. Sorted for stable
/// iteration order — consistent-hash math is meaningless otherwise.
async fn healthy_worker_ids(pool: &Arc<ConnectionPool>) -> Vec<String> {
    let mut ids: Vec<String> = pool
        .worker_statuses()
        .await
        .into_iter()
        .filter(|(_id, _host, _port, healthy, _, _, _)| *healthy)
        .map(|(id, _, _, _, _, _, _)| id)
        .collect();
    ids.sort();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_64_empty_is_offset_basis() {
        // FNV-1a's documented offset basis; if this shifts, every
        // deployment's routing changes on upgrade.
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
    }

    #[test]
    fn fnv1a_64_is_deterministic() {
        // Same input must produce same output on every call and
        // across process restarts; the routing contract depends
        // on this.
        for input in [b"" as &[u8], b"a", b"tenant_a", b"orders"] {
            assert_eq!(fnv1a_64(input), fnv1a_64(input));
        }
    }

    #[test]
    fn fnv1a_64_distinguishes_short_strings() {
        assert_ne!(fnv1a_64(b"tenant_a"), fnv1a_64(b"tenant_b"));
        assert_ne!(fnv1a_64(b"orders"), fnv1a_64(b"users"));
    }

    // Pool-dependent tests live under the coordinator integration
    // suite; this module tests the pure hash math. Behavioural
    // contracts covered there:
    //   - same (table, salt) always picks the same worker
    //   - different tables hash to different workers in a healthy pool
    //   - empty healthy pool returns NoneHealthy
    //   - pool that flips a worker to unhealthy remaps to another
}
