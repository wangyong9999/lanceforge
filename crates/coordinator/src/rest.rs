// Licensed under the Apache License, Version 2.0.
// Lightweight REST API for LanceForge coordinator.
//
// Provides HTTP endpoints alongside gRPC for easy integration.
// Uses raw tokio TCP (no framework dependency) for minimal footprint.

use std::sync::Arc;

use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use lance_distributed_common::metrics::Metrics;

/// Start a simple HTTP server that serves /metrics and /v1/status.
/// Search endpoints require the gRPC client — use the Python SDK or gRPC directly.
pub async fn start_rest_server(metrics: Arc<Metrics>, port: u16) {
    let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(l) => l,
        Err(e) => {
            log::warn!("Failed to start REST server on port {}: {}", port, e);
            return;
        }
    };

    info!("REST/metrics server on http://0.0.0.0:{}", port);
    info!("  GET /metrics    — Prometheus metrics");
    info!("  GET /healthz    — Health probe");
    info!("  GET /v1/status  — Cluster status JSON");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = match stream.read(&mut buf).await {
                    Ok(n) if n > 0 => n,
                    _ => return,
                };
                let request = String::from_utf8_lossy(&buf[..n]);
                let path = extract_path(&request);

                let (status, content_type, body) = match path {
                    "/metrics" => {
                        ("200 OK", "text/plain", metrics.to_prometheus())
                    }
                    "/healthz" => {
                        ("200 OK", "application/json", r#"{"status":"ok"}"#.to_string())
                    }
                    "/v1/status" => {
                        let count = metrics.query_count.load(std::sync::atomic::Ordering::Relaxed);
                        let errors = metrics.query_error_count.load(std::sync::atomic::Ordering::Relaxed);
                        let body = format!(
                            r#"{{"query_count":{},"query_errors":{},"healthy":true}}"#,
                            count, errors
                        );
                        ("200 OK", "application/json", body)
                    }
                    _ => {
                        let body = r#"{"error":"not found","endpoints":["/metrics","/healthz","/v1/status"]}"#.to_string();
                        ("404 Not Found", "application/json", body)
                    }
                };

                let response = format!(
                    "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\n\r\n{}",
                    status, content_type, body.len(), body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}

fn extract_path(request: &str) -> &str {
    // Parse "GET /path HTTP/1.1" → "/path"
    request
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_path() {
        assert_eq!(extract_path("GET /metrics HTTP/1.1\r\n"), "/metrics");
        assert_eq!(extract_path("GET /v1/status?foo=bar HTTP/1.1\r\n"), "/v1/status");
        assert_eq!(extract_path("GET /healthz HTTP/1.1\r\n"), "/healthz");
        assert_eq!(extract_path(""), "/");
    }
}
