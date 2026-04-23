// Licensed under the Apache License, Version 2.0.
//
// PoC Gate 2 — fragment-to-worker router for the single-dataset path.
//
// Uses highest-random-weight (HRW, aka "rendezvous") hashing. For each
// (fragment_id, worker_id) pair we compute a 64-bit score; the worker
// with the highest score owns that fragment. Adding or removing a
// worker migrates O(1/N) fragments — same reshuffle guarantee as a
// consistent-hash ring, but with ~20 lines of code and no ring state.
//
// This is deliberately the minimum viable router. It is NOT what the
// production path will ship — Enterprise uses weighted consistent
// hashing with cache-aware replication factor R. Gate 2's job is to
// prove the end-to-end wire, not to optimise placement.

use std::collections::HashMap;

/// Splits mix for 64-bit integer hashing. Keeps HRW derivations cheap
/// and deterministic without pulling in a hasher crate.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

fn hash_str(s: &str) -> u64 {
    // Simple FNV-1a variant fed through splitmix for better avalanche.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    splitmix64(h)
}

/// Score of a (fragment_id, worker_id) pair. Worker with the highest
/// score owns the fragment.
fn hrw_score(fragment_id: u32, worker_id: &str) -> u64 {
    splitmix64(hash_str(worker_id) ^ splitmix64(fragment_id as u64))
}

/// Partition `fragment_ids` across `workers` by HRW. Returns a map
/// `worker_id → owned fragments`, omitting workers with no assignment.
/// Deterministic: same inputs → same output, regardless of input order.
pub fn assign_fragments(
    fragment_ids: &[u32],
    workers: &[String],
) -> HashMap<String, Vec<u32>> {
    let mut out: HashMap<String, Vec<u32>> = HashMap::new();
    if workers.is_empty() {
        return out;
    }
    for &frag in fragment_ids {
        let owner = workers
            .iter()
            .max_by_key(|w| hrw_score(frag, w))
            .expect("non-empty worker list");
        out.entry(owner.clone()).or_default().push(frag);
    }
    for v in out.values_mut() {
        v.sort_unstable();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fragment_gets_one_owner() {
        let frags: Vec<u32> = (0..1000).collect();
        let workers: Vec<String> = ["w0", "w1", "w2", "w3"].iter().map(|s| s.to_string()).collect();
        let assignment = assign_fragments(&frags, &workers);
        let total: usize = assignment.values().map(|v| v.len()).sum();
        assert_eq!(total, 1000, "every fragment must be owned exactly once");
        // Collision check — no fragment appears in two workers' lists.
        let mut seen = std::collections::HashSet::new();
        for v in assignment.values() {
            for f in v {
                assert!(seen.insert(*f), "fragment {f} double-owned");
            }
        }
    }

    #[test]
    fn balanced_across_workers() {
        let frags: Vec<u32> = (0..10_000).collect();
        let workers: Vec<String> = (0..4).map(|i| format!("w{i}")).collect();
        let assignment = assign_fragments(&frags, &workers);
        // 10k fragments across 4 workers → each gets ~2500. Tolerate ±5%.
        for w in &workers {
            let n = assignment.get(w).map_or(0, |v| v.len());
            let expected = 10_000 / 4;
            let delta = n.abs_diff(expected);
            assert!(
                delta < expected / 20,
                "worker {w} got {n} fragments (expected ~{expected}, delta {delta})"
            );
        }
    }

    #[test]
    fn minimum_reshuffle_on_worker_add() {
        // Adding a 4th worker should migrate ~25% of fragments, not ~75%.
        let frags: Vec<u32> = (0..10_000).collect();
        let w3: Vec<String> = ["w0", "w1", "w2"].iter().map(|s| s.to_string()).collect();
        let w4: Vec<String> = ["w0", "w1", "w2", "w3"].iter().map(|s| s.to_string()).collect();

        let a3 = assign_fragments(&frags, &w3);
        let a4 = assign_fragments(&frags, &w4);

        let flatten = |m: &HashMap<String, Vec<u32>>| -> HashMap<u32, String> {
            let mut out = HashMap::new();
            for (w, fs) in m {
                for f in fs {
                    out.insert(*f, w.clone());
                }
            }
            out
        };
        let f3 = flatten(&a3);
        let f4 = flatten(&a4);
        let migrated = f4.iter().filter(|(f, w)| f3.get(f) != Some(w)).count();
        // Expected migration ≈ 1/N_new = 25%. Allow ±5%.
        assert!(
            (2000..3000).contains(&migrated),
            "expected ~2500 migrations, got {migrated}"
        );
    }

    #[test]
    fn deterministic() {
        let frags: Vec<u32> = (0..100).collect();
        let workers: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let a1 = assign_fragments(&frags, &workers);
        let a2 = assign_fragments(&frags, &workers);
        assert_eq!(a1, a2);
    }

    #[test]
    fn empty_workers_yields_empty_map() {
        let a = assign_fragments(&[0, 1, 2], &[]);
        assert!(a.is_empty());
    }
}
