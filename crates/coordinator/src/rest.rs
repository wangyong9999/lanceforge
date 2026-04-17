// Licensed under the Apache License, Version 2.0.
// Lightweight REST API for LanceForge coordinator.
//
// Provides HTTP JSON endpoints alongside gRPC for easy integration.
// Search endpoints proxy to the local gRPC coordinator via loopback.

use std::sync::Arc;

use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use lance_distributed_common::metrics::Metrics;
use lance_distributed_proto::generated::lance_distributed as pb;

/// Start the REST HTTP server.
/// `grpc_port` is the coordinator's gRPC port for loopback search proxying.
pub async fn start_rest_server(metrics: Arc<Metrics>, port: u16, grpc_port: u16) {
    let listener = match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
        Ok(l) => l,
        Err(e) => {
            log::warn!("Failed to start REST server on port {}: {}", port, e);
            return;
        }
    };

    info!("REST server on http://0.0.0.0:{}", port);
    info!("  GET  /healthz         — Health probe");
    info!("  GET  /metrics         — Prometheus metrics");
    info!("  GET  /v1/status       — Cluster status JSON");
    info!("  POST /v1/search       — ANN vector search");
    info!("  POST /v1/fts          — Full-text search");
    info!("  POST /v1/count        — Count rows");
    info!("  POST /v1/tables       — List tables");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let metrics = metrics.clone();
            let gport = grpc_port;
            tokio::spawn(async move {
                // Read HTTP request: may need multiple reads for large POST bodies
                let mut buf = Vec::with_capacity(65536);
                let mut tmp = vec![0u8; 65536];
                loop {
                    let n = match stream.read(&mut tmp).await {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(_) => break,
                    };
                    buf.extend_from_slice(&tmp[..n]);
                    // Check if we have complete headers + body
                    let s = String::from_utf8_lossy(&buf);
                    if let Some(hdr_end) = s.find("\r\n\r\n") {
                        let headers = &s[..hdr_end];
                        let content_len = headers.lines()
                            .find(|l| l.to_lowercase().starts_with("content-length:"))
                            .and_then(|l| l.split(':').nth(1))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        let body_start = hdr_end + 4;
                        if buf.len() >= body_start + content_len { break; }
                    }
                    if buf.len() > 1_000_000 { break; } // safety cap
                }
                if buf.is_empty() { return; }
                let request = String::from_utf8_lossy(&buf);
                let method = request.split_whitespace().next().unwrap_or("GET");
                let path = extract_path(&request);
                let body_str = extract_body(&request);

                let (status, content_type, body) = match (method, path) {
                    (_, "/metrics") => {
                        ("200 OK", "text/plain", metrics.to_prometheus())
                    }
                    (_, "/healthz") => {
                        ("200 OK", "application/json", r#"{"status":"ok"}"#.to_string())
                    }
                    (_, "/v1/status") => {
                        let count = metrics.query_count.load(std::sync::atomic::Ordering::Relaxed);
                        let errors = metrics.query_error_count.load(std::sync::atomic::Ordering::Relaxed);
                        ("200 OK", "application/json", format!(
                            r#"{{"query_count":{},"query_errors":{},"healthy":true}}"#, count, errors))
                    }
                    ("POST", "/v1/search") => {
                        handle_search(gport, body_str).await
                    }
                    ("POST", "/v1/fts") => {
                        handle_fts(gport, body_str).await
                    }
                    ("POST", "/v1/count") => {
                        handle_count(gport, body_str).await
                    }
                    ("POST", "/v1/tables") | ("GET", "/v1/tables") => {
                        handle_list_tables(gport).await
                    }
                    ("POST", "/v1/query") => {
                        handle_query(gport, body_str).await
                    }
                    ("POST", "/v1/batch_search") => {
                        handle_batch_search(gport, body_str).await
                    }
                    ("POST", "/v1/drop_table") => {
                        handle_drop_table(gport, body_str).await
                    }
                    _ => {
                        ("404 Not Found", "application/json",
                         r#"{"error":"not found","endpoints":["/healthz","/metrics","/v1/status","/v1/search","/v1/fts","/v1/query","/v1/batch_search","/v1/count","/v1/tables","/v1/drop_table"]}"#.to_string())
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

/// POST /v1/search — ANN vector search
/// Body: {"table":"t","vector":[0.1,0.2,...],"k":10,"filter":"...","nprobes":20}
async fn handle_search(grpc_port: u16, body: &str) -> (&'static str, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return ("400 Bad Request", "application/json",
            format!(r#"{{"error":"invalid JSON: {}"}}"#, e)),
    };

    let table = parsed["table"].as_str().unwrap_or("");
    let k = parsed["k"].as_u64().unwrap_or(10) as u32;
    let filter = parsed.get("filter").and_then(|v| v.as_str()).map(|s| s.to_string());
    let nprobes = parsed["nprobes"].as_u64().unwrap_or(20) as u32;

    let vector: Vec<f32> = match parsed.get("vector").and_then(|v| v.as_array()) {
        Some(arr) => arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect(),
        None => return ("400 Bad Request", "application/json",
            r#"{"error":"'vector' array required"}"#.to_string()),
    };

    let query_vector: Vec<u8> = vector.iter().flat_map(|f| f.to_le_bytes()).collect();
    let dimension = vector.len() as u32;

    match connect_grpc(grpc_port).await {
        Ok(mut client) => {
            let req = pb::AnnSearchRequest {
                table_name: table.to_string(),
                vector_column: "vector".to_string(),
                query_vector,
                dimension,
                k,
                nprobes,
                filter,
                columns: vec![],
                metric_type: 0,
                query_text: None,
                offset: 0,
            };
            match client.ann_search(tonic::Request::new(req)).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() {
                        return ("500 Internal Server Error", "application/json",
                            format!(r#"{{"error":"{}"}}"#, r.error));
                    }
                    let json = ipc_to_json(&r.arrow_ipc_data, r.num_rows, r.latency_ms);
                    ("200 OK", "application/json", json)
                }
                Err(e) => ("500 Internal Server Error", "application/json",
                    format!(r#"{{"error":"{}"}}"#, e.message())),
            }
        }
        Err(e) => ("503 Service Unavailable", "application/json",
            format!(r#"{{"error":"grpc connect: {}"}}"#, e)),
    }
}

/// POST /v1/fts — Full-text BM25 search
/// Body: {"table":"t","query":"text","k":10,"text_column":"text"}
async fn handle_fts(grpc_port: u16, body: &str) -> (&'static str, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return ("400 Bad Request", "application/json",
            format!(r#"{{"error":"invalid JSON: {}"}}"#, e)),
    };

    let table = parsed["table"].as_str().unwrap_or("");
    let query = parsed["query"].as_str().unwrap_or("");
    let k = parsed["k"].as_u64().unwrap_or(10) as u32;
    let text_column = parsed["text_column"].as_str().unwrap_or("text").to_string();

    match connect_grpc(grpc_port).await {
        Ok(mut client) => {
            let req = pb::FtsSearchRequest {
                table_name: table.to_string(),
                text_column,
                query_text: query.to_string(),
                k,
                filter: None,
                columns: vec![],
                offset: 0,
            };
            match client.fts_search(tonic::Request::new(req)).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() {
                        return ("500 Internal Server Error", "application/json",
                            format!(r#"{{"error":"{}"}}"#, r.error));
                    }
                    let json = ipc_to_json(&r.arrow_ipc_data, r.num_rows, r.latency_ms);
                    ("200 OK", "application/json", json)
                }
                Err(e) => ("500 Internal Server Error", "application/json",
                    format!(r#"{{"error":"{}"}}"#, e.message())),
            }
        }
        Err(e) => ("503 Service Unavailable", "application/json",
            format!(r#"{{"error":"grpc connect: {}"}}"#, e)),
    }
}

/// POST /v1/count — Count rows
/// Body: {"table":"t"}
async fn handle_count(grpc_port: u16, body: &str) -> (&'static str, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return ("400 Bad Request", "application/json",
            r#"{"error":"invalid JSON"}"#.to_string()),
    };
    let table = parsed["table"].as_str().unwrap_or("");
    match connect_grpc(grpc_port).await {
        Ok(mut client) => {
            match client.count_rows(tonic::Request::new(
                pb::CountRowsRequest { table_name: table.to_string(), filter: None }
            )).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    ("200 OK", "application/json",
                        format!(r#"{{"table":"{}","count":{}}}"#, table, r.count))
                }
                Err(e) => ("500 Internal Server Error", "application/json",
                    format!(r#"{{"error":"{}"}}"#, e.message())),
            }
        }
        Err(e) => ("503 Service Unavailable", "application/json",
            format!(r#"{{"error":"{}"}}"#, e)),
    }
}

/// GET/POST /v1/tables — List tables
async fn handle_list_tables(grpc_port: u16) -> (&'static str, &'static str, String) {
    match connect_grpc(grpc_port).await {
        Ok(mut client) => {
            match client.list_tables(tonic::Request::new(pb::ListTablesRequest {})).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    let tables: Vec<String> = r.table_names.iter()
                        .map(|t| format!("\"{}\"", t)).collect();
                    ("200 OK", "application/json",
                        format!(r#"{{"tables":[{}]}}"#, tables.join(",")))
                }
                Err(e) => ("500 Internal Server Error", "application/json",
                    format!(r#"{{"error":"{}"}}"#, e.message())),
            }
        }
        Err(e) => ("503 Service Unavailable", "application/json",
            format!(r#"{{"error":"{}"}}"#, e)),
    }
}

/// POST /v1/query — Expression scan (filter without vector)
/// Body: {"table":"t","filter":"category='tech'","limit":100}
async fn handle_query(grpc_port: u16, body: &str) -> (&'static str, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v, Err(e) => return ("400 Bad Request", "application/json",
            format!(r#"{{"error":"invalid JSON: {}"}}"#, e)),
    };
    let table = parsed["table"].as_str().unwrap_or("");
    let filter = parsed["filter"].as_str().unwrap_or("");
    let limit = parsed["limit"].as_u64().unwrap_or(100) as u32;
    let offset = parsed["offset"].as_u64().unwrap_or(0) as u32;

    if filter.is_empty() {
        return ("400 Bad Request", "application/json", r#"{"error":"'filter' required"}"#.to_string());
    }
    match connect_grpc(grpc_port).await {
        Ok(mut client) => {
            match client.query(tonic::Request::new(pb::QueryRequest {
                table_name: table.to_string(), filter: filter.to_string(),
                limit, offset, columns: vec![],
            })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() { return ("500 Internal Server Error", "application/json",
                        format!(r#"{{"error":"{}"}}"#, r.error)); }
                    ("200 OK", "application/json", ipc_to_json(&r.arrow_ipc_data, r.num_rows, r.latency_ms))
                }
                Err(e) => ("500 Internal Server Error", "application/json",
                    format!(r#"{{"error":"{}"}}"#, e.message())),
            }
        }
        Err(e) => ("503 Service Unavailable", "application/json", format!(r#"{{"error":"{}"}}"#, e)),
    }
}

/// POST /v1/batch_search — Multiple ANN queries in one call
/// Body: {"table":"t","vectors":[[0.1,...],[0.2,...]],"k":10}
async fn handle_batch_search(grpc_port: u16, body: &str) -> (&'static str, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v, Err(e) => return ("400 Bad Request", "application/json",
            format!(r#"{{"error":"invalid JSON: {}"}}"#, e)),
    };
    let table = parsed["table"].as_str().unwrap_or("");
    let k = parsed["k"].as_u64().unwrap_or(10) as u32;
    let nprobes = parsed["nprobes"].as_u64().unwrap_or(20) as u32;

    let vectors_raw = match parsed.get("vectors").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return ("400 Bad Request", "application/json",
            r#"{"error":"'vectors' array of arrays required"}"#.to_string()),
    };

    let mut query_vectors = Vec::new();
    let mut dimension = 0u32;
    for vec_val in vectors_raw {
        let floats: Vec<f32> = match vec_val.as_array() {
            Some(arr) => arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect(),
            None => return ("400 Bad Request", "application/json",
                r#"{"error":"each vector must be array of floats"}"#.to_string()),
        };
        if dimension == 0 { dimension = floats.len() as u32; }
        let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();
        query_vectors.push(bytes);
    }

    match connect_grpc(grpc_port).await {
        Ok(mut client) => {
            match client.batch_search(tonic::Request::new(pb::BatchSearchRequest {
                table_name: table.to_string(), query_vectors, vector_column: "vector".to_string(),
                dimension, k, nprobes, filter: None, columns: vec![], metric_type: 0,
            })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() { return ("500 Internal Server Error", "application/json",
                        format!(r#"{{"error":"{}"}}"#, r.error)); }
                    let results_json: Vec<String> = r.results.iter()
                        .map(|sr| ipc_to_json(&sr.arrow_ipc_data, sr.num_rows, sr.latency_ms))
                        .collect();
                    ("200 OK", "application/json",
                        format!(r#"{{"count":{},"results":[{}]}}"#, results_json.len(), results_json.join(",")))
                }
                Err(e) => ("500 Internal Server Error", "application/json",
                    format!(r#"{{"error":"{}"}}"#, e.message())),
            }
        }
        Err(e) => ("503 Service Unavailable", "application/json", format!(r#"{{"error":"{}"}}"#, e)),
    }
}

/// POST /v1/drop_table — Drop a table
/// Body: {"table":"t"}
async fn handle_drop_table(grpc_port: u16, body: &str) -> (&'static str, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v, Err(_) => return ("400 Bad Request", "application/json",
            r#"{"error":"invalid JSON"}"#.to_string()),
    };
    let table = parsed["table"].as_str().unwrap_or("");
    match connect_grpc(grpc_port).await {
        Ok(mut client) => {
            match client.drop_table(tonic::Request::new(pb::DropTableRequest {
                table_name: table.to_string(),
            })).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if !r.error.is_empty() { return ("500 Internal Server Error", "application/json",
                        format!(r#"{{"error":"{}"}}"#, r.error)); }
                    ("200 OK", "application/json", format!(r#"{{"dropped":"{}"}}"#, table))
                }
                Err(e) => ("500 Internal Server Error", "application/json",
                    format!(r#"{{"error":"{}"}}"#, e.message())),
            }
        }
        Err(e) => ("503 Service Unavailable", "application/json", format!(r#"{{"error":"{}"}}"#, e)),
    }
}

/// Create a gRPC loopback client to the local coordinator.
async fn connect_grpc(port: u16)
    -> Result<pb::lance_scheduler_service_client::LanceSchedulerServiceClient<tonic::transport::Channel>, String>
{
    let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", port))
        .map_err(|e| format!("uri: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("connect: {e}"))?;
    Ok(pb::lance_scheduler_service_client::LanceSchedulerServiceClient::new(channel)
        .max_decoding_message_size(256 * 1024 * 1024))
}

/// Convert Arrow IPC bytes to a JSON array of row objects.
fn ipc_to_json(ipc_data: &[u8], num_rows: u32, latency_ms: u64) -> String {
    if ipc_data.is_empty() || num_rows == 0 {
        return format!(r#"{{"num_rows":0,"latency_ms":{},"rows":[]}}"#, latency_ms);
    }

    let batch = match lance_distributed_common::ipc::ipc_to_record_batch(ipc_data) {
        Ok(b) => b,
        Err(_) => return format!(r#"{{"error":"IPC decode failed","num_rows":{}}}"#, num_rows),
    };

    // Convert RecordBatch to JSON rows using arrow's json writer
    let buf = Vec::new();
    let mut writer = arrow::json::LineDelimitedWriter::new(buf);
    let _ = writer.write(&batch);
    let _ = writer.finish();
    let json_bytes = writer.into_inner();
    let lines = String::from_utf8_lossy(&json_bytes);
    let rows: Vec<&str> = lines.trim().split('\n').filter(|l| !l.is_empty()).collect();

    format!(r#"{{"num_rows":{},"latency_ms":{},"rows":[{}]}}"#,
        batch.num_rows(), latency_ms, rows.join(","))
}

fn extract_path(request: &str) -> &str {
    request.split_whitespace().nth(1).unwrap_or("/").split('?').next().unwrap_or("/")
}

fn extract_body(request: &str) -> &str {
    // HTTP body comes after \r\n\r\n
    request.split("\r\n\r\n").nth(1).unwrap_or("{}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_path() {
        assert_eq!(extract_path("GET /metrics HTTP/1.1\r\n"), "/metrics");
        assert_eq!(extract_path("GET /v1/status?foo=bar HTTP/1.1\r\n"), "/v1/status");
        assert_eq!(extract_path("POST /v1/search HTTP/1.1\r\n"), "/v1/search");
        assert_eq!(extract_path(""), "/");
    }

    #[test]
    fn test_extract_body() {
        assert_eq!(extract_body("POST /v1/search HTTP/1.1\r\nContent-Type: application/json\r\n\r\n{\"k\":10}"), "{\"k\":10}");
        assert_eq!(extract_body("GET / HTTP/1.1\r\n\r\n"), "");
    }

    #[test]
    fn test_ipc_to_json_empty() {
        let json = ipc_to_json(&[], 0, 5);
        assert!(json.contains("\"num_rows\":0"));
        assert!(json.contains("\"latency_ms\":5"));
    }

    #[test]
    fn test_extract_path_handles_missing_method() {
        // A bodyless garbage request must not panic.
        assert_eq!(extract_path("/"), "/");
        // No whitespace at all → fallback to "/".
        assert_eq!(extract_path("GET"), "/");
    }

    #[test]
    fn test_extract_path_strips_query_string() {
        assert_eq!(extract_path("GET /v1/search?k=5&fields=id HTTP/1.1\r\n"), "/v1/search");
        // Query string with only '?' is still handled.
        assert_eq!(extract_path("GET /v1/count? HTTP/1.1\r\n"), "/v1/count");
    }

    #[test]
    fn test_extract_body_with_crlf_in_payload() {
        // Body contains CRLF but the split only fires on the first \r\n\r\n
        // (the header/body boundary), so the payload is preserved verbatim.
        let req = "POST /v1/upload HTTP/1.1\r\nContent-Type: text/plain\r\n\r\nline1\r\nline2\r\n";
        let body = extract_body(req);
        assert_eq!(body, "line1\r\nline2\r\n");
    }

    #[test]
    fn test_extract_body_missing_boundary_returns_default_json() {
        // If the request is malformed (no \r\n\r\n), downstream handlers
        // get "{}" so serde_json parsing yields sensible defaults rather
        // than a parse error.
        assert_eq!(extract_body("POST /v1/search HTTP/1.1\r\nContent-Type: application/json"),
            "{}");
    }

    #[test]
    fn test_ipc_to_json_with_real_batch() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ]).unwrap();
        let ipc = lance_distributed_common::ipc::record_batch_to_ipc(&batch).unwrap();

        let json = ipc_to_json(&ipc, 2, 42);
        assert!(json.contains("\"num_rows\":2"));
        assert!(json.contains("\"latency_ms\":42"));
        // Should include the actual row data in JSON Lines form.
        assert!(json.contains("\"id\":1"));
        assert!(json.contains("\"name\":\"a\""));
        assert!(json.contains("\"id\":2"));
        assert!(json.contains("\"name\":\"b\""));
    }

    #[test]
    fn test_ipc_to_json_malformed_data_returns_error_marker() {
        // Invalid IPC bytes must return a JSON error, not panic.
        let json = ipc_to_json(&[0xff, 0xfe, 0xfd], 3, 0);
        assert!(json.contains("error"), "got: {json}");
    }
}
