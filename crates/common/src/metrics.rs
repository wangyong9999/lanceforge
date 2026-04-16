// Licensed under the Apache License, Version 2.0.
// Simple metrics for LanceForge observability.
// Exposes Prometheus-compatible text format via HTTP.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Per-table counters (queries, errors, rows). Tracked server-side so that
/// Prometheus can emit labelled series (`table=` label).
#[derive(Debug, Default)]
pub struct TableMetric {
    pub query_count: AtomicU64,
    pub query_error_count: AtomicU64,
    pub rows_returned: AtomicU64,
}

/// Simple atomic counters for key metrics.
/// Thread-safe, lock-free, usable from any async context.
/// Latency histogram bucket boundaries in milliseconds.
const HISTOGRAM_BUCKETS_MS: &[u64] = &[1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

#[derive(Debug)]
pub struct Metrics {
    pub query_count: AtomicU64,
    pub query_error_count: AtomicU64,
    pub query_latency_sum_us: AtomicU64,  // microseconds
    pub query_latency_max_us: AtomicU64,
    pub executor_healthy_count: AtomicU64,
    pub executor_total_count: AtomicU64,
    pub slow_query_count: AtomicU64,
    /// Latency histogram: bucket[i] counts queries with latency <= HISTOGRAM_BUCKETS_MS[i].
    latency_buckets: Vec<AtomicU64>,
    /// Per-table metrics. RwLock is held only briefly on first-use insert;
    /// hot-path reads use `get` under read lock then atomic increments.
    pub per_table: RwLock<HashMap<String, Arc<TableMetric>>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            query_count: AtomicU64::new(0),
            query_error_count: AtomicU64::new(0),
            query_latency_sum_us: AtomicU64::new(0),
            query_latency_max_us: AtomicU64::new(0),
            executor_healthy_count: AtomicU64::new(0),
            executor_total_count: AtomicU64::new(0),
            slow_query_count: AtomicU64::new(0),
            latency_buckets: HISTOGRAM_BUCKETS_MS.iter().map(|_| AtomicU64::new(0)).collect(),
            per_table: RwLock::new(HashMap::new()),
        }
    }
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_query(&self, latency_us: u64, success: bool) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.query_error_count.fetch_add(1, Ordering::Relaxed);
        }
        self.query_latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.query_latency_max_us.fetch_max(latency_us, Ordering::Relaxed);
        // Populate histogram buckets
        let latency_ms = latency_us / 1000;
        for (i, &bound) in HISTOGRAM_BUCKETS_MS.iter().enumerate() {
            if latency_ms <= bound {
                self.latency_buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    /// Record a per-table query outcome (hot path: read-locked fast path).
    /// Uses poison-recovery on RwLock to avoid crashing the whole process
    /// if any thread panics while holding the lock.
    pub fn record_table_query(&self, table: &str, rows: u64, success: bool) {
        let tm = {
            let map = match self.per_table.read() {
                Ok(m) => m,
                Err(poisoned) => poisoned.into_inner(),
            };
            map.get(table).cloned()
        };
        let tm = match tm {
            Some(t) => t,
            None => {
                let mut map = match self.per_table.write() {
                    Ok(m) => m,
                    Err(poisoned) => poisoned.into_inner(),
                };
                map.entry(table.to_string())
                    .or_insert_with(|| Arc::new(TableMetric::default()))
                    .clone()
            }
        };
        tm.query_count.fetch_add(1, Ordering::Relaxed);
        if !success { tm.query_error_count.fetch_add(1, Ordering::Relaxed); }
        tm.rows_returned.fetch_add(rows, Ordering::Relaxed);
    }

    pub fn record_slow_query(&self) {
        self.slow_query_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove per-table metrics (called on DropTable to prevent leak).
    pub fn remove_table(&self, table: &str) {
        let mut map = match self.per_table.write() {
            Ok(m) => m,
            Err(poisoned) => poisoned.into_inner(),
        };
        map.remove(table);
    }

    pub fn set_executor_health(&self, healthy: u64, total: u64) {
        self.executor_healthy_count.store(healthy, Ordering::Relaxed);
        self.executor_total_count.store(total, Ordering::Relaxed);
    }

    /// Render Prometheus text format.
    pub fn to_prometheus(&self) -> String {
        let count = self.query_count.load(Ordering::Relaxed);
        let errors = self.query_error_count.load(Ordering::Relaxed);
        let sum_us = self.query_latency_sum_us.load(Ordering::Relaxed);
        let max_us = self.query_latency_max_us.load(Ordering::Relaxed);
        let avg_ms = if count > 0 { sum_us as f64 / count as f64 / 1000.0 } else { 0.0 };
        let healthy = self.executor_healthy_count.load(Ordering::Relaxed);
        let total = self.executor_total_count.load(Ordering::Relaxed);

        let slow = self.slow_query_count.load(Ordering::Relaxed);

        let mut out = format!(
            "# HELP lance_query_total Total number of queries\n\
             # TYPE lance_query_total counter\n\
             lance_query_total {count}\n\
             # HELP lance_query_errors_total Total query errors\n\
             # TYPE lance_query_errors_total counter\n\
             lance_query_errors_total {errors}\n\
             # HELP lance_query_latency_avg_ms Average query latency in milliseconds\n\
             # TYPE lance_query_latency_avg_ms gauge\n\
             lance_query_latency_avg_ms {avg_ms:.2}\n\
             # HELP lance_query_latency_max_ms Maximum query latency in milliseconds\n\
             # TYPE lance_query_latency_max_ms gauge\n\
             lance_query_latency_max_ms {max_ms:.2}\n\
             # HELP lance_executors_healthy Number of healthy executors\n\
             # TYPE lance_executors_healthy gauge\n\
             lance_executors_healthy {healthy}\n\
             # HELP lance_executors_total Total number of configured executors\n\
             # TYPE lance_executors_total gauge\n\
             lance_executors_total {total}\n\
             # HELP lance_slow_query_total Queries exceeding slow_query_ms threshold\n\
             # TYPE lance_slow_query_total counter\n\
             lance_slow_query_total {slow}\n",
            max_ms = max_us as f64 / 1000.0,
        );

        // Latency histogram (Prometheus histogram type)
        out.push_str("# HELP lance_query_latency_ms Histogram of query latency in milliseconds\n");
        out.push_str("# TYPE lance_query_latency_ms histogram\n");
        let mut cumulative = 0u64;
        for (i, &bound) in HISTOGRAM_BUCKETS_MS.iter().enumerate() {
            cumulative += self.latency_buckets[i].load(Ordering::Relaxed);
            out.push_str(&format!(
                "lance_query_latency_ms_bucket{{le=\"{}\"}} {}\n", bound, cumulative));
        }
        out.push_str(&format!("lance_query_latency_ms_bucket{{le=\"+Inf\"}} {}\n", count));
        out.push_str(&format!("lance_query_latency_ms_sum {:.2}\n", sum_us as f64 / 1000.0));
        out.push_str(&format!("lance_query_latency_ms_count {}\n", count));

        // Per-table labels
        if let Ok(map) = self.per_table.read()
            && !map.is_empty() {
                out.push_str("# HELP lance_table_query_total Queries per table\n");
                out.push_str("# TYPE lance_table_query_total counter\n");
                for (table, tm) in map.iter() {
                    out.push_str(&format!(
                        "lance_table_query_total{{table=\"{}\"}} {}\n",
                        escape_label(table),
                        tm.query_count.load(Ordering::Relaxed),
                    ));
                }
                out.push_str("# HELP lance_table_query_errors_total Query errors per table\n");
                out.push_str("# TYPE lance_table_query_errors_total counter\n");
                for (table, tm) in map.iter() {
                    out.push_str(&format!(
                        "lance_table_query_errors_total{{table=\"{}\"}} {}\n",
                        escape_label(table),
                        tm.query_error_count.load(Ordering::Relaxed),
                    ));
                }
                out.push_str("# HELP lance_table_rows_returned_total Rows returned per table\n");
                out.push_str("# TYPE lance_table_rows_returned_total counter\n");
                for (table, tm) in map.iter() {
                    out.push_str(&format!(
                        "lance_table_rows_returned_total{{table=\"{}\"}} {}\n",
                        escape_label(table),
                        tm.rows_returned.load(Ordering::Relaxed),
                    ));
                }
            }
        out
    }
}

fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Start a simple HTTP server on the given port that serves /metrics.
pub async fn start_metrics_server(metrics: Arc<Metrics>, port: u16) {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(l) => l,
        Err(e) => {
            log::warn!("Failed to start metrics server on port {}: {}", port, e);
            return;
        }
    };

    log::info!("Metrics server listening on http://0.0.0.0:{}/metrics", port);

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let body = metrics.to_prometheus();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_prometheus_format() {
        let m = Metrics::new();
        m.record_query(5000, true);   // 5ms success
        m.record_query(10000, true);  // 10ms success
        m.record_query(3000, false);  // 3ms error

        let output = m.to_prometheus();
        assert!(output.contains("lance_query_total 3"));
        assert!(output.contains("lance_query_errors_total 1"));
        assert!(output.contains("lance_query_latency_max_ms 10.00"));
        assert!(output.contains("lance_executors_healthy 0"));
    }

    #[test]
    fn test_metrics_empty() {
        let m = Metrics::new();
        let output = m.to_prometheus();
        assert!(output.contains("lance_query_total 0"));
        assert!(output.contains("lance_query_latency_avg_ms 0.00"));
    }

    #[test]
    fn test_histogram_buckets() {
        let m = Metrics::new();
        m.record_query(500, true);    // 0.5ms → bucket le=1
        m.record_query(5000, true);   // 5ms → bucket le=5
        m.record_query(50000, true);  // 50ms → bucket le=50
        m.record_query(500000, true); // 500ms → bucket le=500

        let output = m.to_prometheus();
        assert!(output.contains("lance_query_latency_ms_bucket{le=\"1\"} 1"));
        assert!(output.contains("lance_query_latency_ms_bucket{le=\"5\"} 2"));
        assert!(output.contains("lance_query_latency_ms_count 4"));
    }

    #[test]
    fn test_per_table_metrics() {
        let m = Metrics::new();
        m.record_table_query("table_a", 10, true);
        m.record_table_query("table_a", 5, false);
        m.record_table_query("table_b", 20, true);

        let output = m.to_prometheus();
        assert!(output.contains("lance_table_query_total{table=\"table_a\"} 2"));
        assert!(output.contains("lance_table_query_total{table=\"table_b\"} 1"));
    }

    #[test]
    fn test_remove_table() {
        let m = Metrics::new();
        m.record_table_query("ephemeral", 10, true);
        let output1 = m.to_prometheus();
        assert!(output1.contains("ephemeral"));

        m.remove_table("ephemeral");
        let output2 = m.to_prometheus();
        assert!(!output2.contains("ephemeral"));
    }

    #[test]
    fn test_executor_health() {
        let m = Metrics::new();
        m.set_executor_health(3, 5);
        let output = m.to_prometheus();
        assert!(output.contains("lance_executors_healthy 3"));
        assert!(output.contains("lance_executors_total 5"));
    }

    #[test]
    fn test_slow_query() {
        let m = Metrics::new();
        m.record_slow_query();
        m.record_slow_query();
        let output = m.to_prometheus();
        assert!(output.contains("lance_slow_query_total 2"));
    }
}
