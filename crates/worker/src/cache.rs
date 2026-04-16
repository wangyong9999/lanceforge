// Licensed under the Apache License, Version 2.0.
// Query result cache with TTL for Executor-side hot query dedup.
//
// Avoids repeated S3 reads for identical queries within the TTL window.
// Uses a simple HashMap + expiry check (no external dependency).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use tokio::sync::RwLock;

/// Cache key derived from query parameters + dataset version.
/// Version ensures cache is invalidated when data changes.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct QueryCacheKey {
    pub table_name: String,
    pub query_type: i32,
    pub vector_hash: u64,
    pub filter: Option<String>,
    pub k: u32,
    pub nprobes: u32,
    pub metric_type: i32,
    pub dataset_version: u64, // invalidates cache when data refreshes
}

impl QueryCacheKey {
    pub fn from_descriptor_with_version(desc: &lance_distributed_proto::descriptor::LanceQueryDescriptor, dataset_version: u64) -> Self {
        let (vector_hash, nprobes, metric_type) = if let Some(ref vq) = desc.vector_query {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            vq.vector_data.hash(&mut hasher);
            (hasher.finish(), vq.nprobes, vq.metric_type)
        } else {
            (0, 0, 0)
        };

        Self {
            table_name: desc.table_name.clone(),
            query_type: desc.query_type,
            vector_hash,
            filter: desc.filter.clone(),
            k: desc.k,
            nprobes,
            metric_type,
            dataset_version,
        }
    }
}

struct CacheEntry {
    batch: RecordBatch,
    size_bytes: usize,
    inserted_at: Instant,
}

/// LRU-ish cache with TTL expiry and byte-level memory cap.
/// Thread-safe via RwLock.
pub struct QueryCache {
    entries: RwLock<HashMap<QueryCacheKey, CacheEntry>>,
    ttl: Duration,
    max_entries: usize,
    /// Hard cap on total cached bytes (prevents OOM from large RecordBatches).
    max_bytes: usize,
    current_bytes: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl QueryCache {
    pub fn new(ttl: Duration, max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl,
            max_entries,
            max_bytes,
            current_bytes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Disabled cache (always miss).
    pub fn disabled() -> Self {
        Self::new(Duration::ZERO, 0, 0)
    }

    pub fn is_enabled(&self) -> bool {
        self.ttl > Duration::ZERO && self.max_entries > 0
    }

    /// Current cache memory usage in bytes.
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Try to get a cached result.
    pub async fn get(&self, key: &QueryCacheKey) -> Option<RecordBatch> {
        if !self.is_enabled() {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(key)
            && entry.inserted_at.elapsed() < self.ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.batch.clone());
            }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Store a result in cache.
    pub async fn put(&self, key: QueryCacheKey, batch: RecordBatch) {
        if !self.is_enabled() || batch.num_rows() == 0 {
            return;
        }

        let entry_bytes = batch.get_array_memory_size();

        // Skip caching entries larger than 25% of max_bytes (single huge result
        // should not flush the whole cache).
        if self.max_bytes > 0 && entry_bytes > self.max_bytes / 4 {
            return;
        }

        let mut entries = self.entries.write().await;

        // Evict expired entries
        {
            let now = Instant::now();
            let before_len = entries.len();
            entries.retain(|_, v| {
                if now.duration_since(v.inserted_at) >= self.ttl {
                    self.current_bytes.fetch_sub(v.size_bytes as u64, Ordering::Relaxed);
                    false
                } else {
                    true
                }
            });
            let _ = before_len; // suppress unused
        }

        // Evict oldest until under both entry count and byte cap
        while entries.len() >= self.max_entries
            || (self.max_bytes > 0
                && self.current_bytes.load(Ordering::Relaxed) + entry_bytes as u64 > self.max_bytes as u64)
        {
            if let Some(oldest_key) = entries.iter()
                .min_by_key(|(_, v)| v.inserted_at)
                .map(|(k, _)| k.clone())
            {
                if let Some(removed) = entries.remove(&oldest_key) {
                    self.current_bytes.fetch_sub(removed.size_bytes as u64, Ordering::Relaxed);
                }
            } else {
                break;
            }
        }

        self.current_bytes.fetch_add(entry_bytes as u64, Ordering::Relaxed);
        entries.insert(key, CacheEntry {
            batch,
            size_bytes: entry_bytes,
            inserted_at: Instant::now(),
        });
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let misses = self.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total == 0.0 { 0.0 } else { hits / total }
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(ids: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("_distance", DataType::Float32, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Float32Array::from(vec![0.1; ids.len()])),
        ]).unwrap()
    }

    #[tokio::test]
    async fn test_cache_hit_miss() {
        let cache = QueryCache::new(Duration::from_secs(10), 100, 0);
        let key = QueryCacheKey {
            table_name: "t".into(),
            query_type: 0,
            vector_hash: 123,
            filter: None,
            k: 10, nprobes: 0, metric_type: 0i32, dataset_version: 1,
        };

        // Miss
        assert!(cache.get(&key).await.is_none());
        assert_eq!(cache.misses(), 1);

        // Put
        cache.put(key.clone(), make_batch(vec![1, 2, 3])).await;

        // Hit
        let result = cache.get(&key).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().num_rows(), 3);
        assert_eq!(cache.hits(), 1);
    }

    #[tokio::test]
    async fn test_cache_ttl_expiry() {
        let cache = QueryCache::new(Duration::from_millis(50), 100, 0);
        let key = QueryCacheKey {
            table_name: "t".into(), query_type: 0, vector_hash: 1, filter: None, k: 5, nprobes: 0, metric_type: 0i32, dataset_version: 1,
        };

        cache.put(key.clone(), make_batch(vec![1])).await;
        assert!(cache.get(&key).await.is_some());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(cache.get(&key).await.is_none()); // expired
    }

    #[tokio::test]
    async fn test_cache_max_entries() {
        let cache = QueryCache::new(Duration::from_secs(10), 2, 0);

        for i in 0..3 {
            let key = QueryCacheKey {
                table_name: "t".into(), query_type: 0, vector_hash: i, filter: None, k: 5, nprobes: 0, metric_type: 0i32, dataset_version: 1,
            };
            cache.put(key, make_batch(vec![i as i32])).await;
        }

        // Should have evicted oldest (i=0)
        let entries = cache.entries.read().await;
        assert!(entries.len() <= 2);
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let cache = QueryCache::disabled();
        let key = QueryCacheKey {
            table_name: "t".into(), query_type: 0, vector_hash: 0, filter: None, k: 5, nprobes: 0, metric_type: 0i32, dataset_version: 1,
        };
        cache.put(key.clone(), make_batch(vec![1])).await;
        assert!(cache.get(&key).await.is_none()); // disabled = always miss
    }

    #[tokio::test]
    async fn test_cache_hit_rate() {
        let cache = QueryCache::new(Duration::from_secs(10), 100, 0);
        let key = QueryCacheKey {
            table_name: "t".into(), query_type: 0, vector_hash: 0, filter: None, k: 5, nprobes: 0, metric_type: 0i32, dataset_version: 1,
        };

        cache.get(&key).await; // miss
        cache.put(key.clone(), make_batch(vec![1])).await;
        cache.get(&key).await; // hit
        cache.get(&key).await; // hit

        assert!((cache.hit_rate() - 0.666).abs() < 0.01); // 2 hits / 3 total
    }
}
