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
#[derive(Debug, Default)]
pub struct Metrics {
    pub query_count: AtomicU64,
    pub query_error_count: AtomicU64,
    pub query_latency_sum_us: AtomicU64,  // microseconds
    pub query_latency_max_us: AtomicU64,
    pub executor_healthy_count: AtomicU64,
    pub executor_total_count: AtomicU64,
    pub slow_query_count: AtomicU64,
    /// Per-table metrics. RwLock is held only briefly on first-use insert;
    /// hot-path reads use `get` under read lock then atomic increments.
    pub per_table: RwLock<HashMap<String, Arc<TableMetric>>>,
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
    }

    /// Record a per-table query outcome (hot path: read-locked fast path).
    pub fn record_table_query(&self, table: &str, rows: u64, success: bool) {
        let tm = {
            let map = self.per_table.read().unwrap();
            map.get(table).cloned()
        };
        let tm = match tm {
            Some(t) => t,
            None => {
                let mut map = self.per_table.write().unwrap();
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

        // Per-table labels
        if let Ok(map) = self.per_table.read() {
            if !map.is_empty() {
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
}
