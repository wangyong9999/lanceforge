// Licensed under the Apache License, Version 2.0.
// Worker-side gRPC service: handles local search requests from Coordinator.

use std::sync::Arc;

use log::{debug, info, warn};
use tonic::{Request, Response, Status};

/// Extract the 32-hex trace_id portion of a W3C traceparent metadata
/// header. Matches `crates/coordinator/src/service.rs::extract_trace_id`
/// — worker logs use the same format so a grep for `trace_id=<hex>`
/// in coord + worker logs returns a continuous request story (B1).
fn extract_trace_id<T>(req: &Request<T>) -> Option<String> {
    let raw = req.metadata().get("traceparent")?.to_str().ok()?;
    let mut parts = raw.split('-');
    parts.next()?;
    let trace = parts.next()?;
    if trace.len() != 32 || !trace.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(trace.to_string())
}

use lance_distributed_common::ipc::record_batch_to_ipc;
use lance_distributed_proto::descriptor::{FtsQueryParams, LanceQueryDescriptor, VectorQueryParams};
use crate::executor::LanceTableRegistry;
use lance_distributed_proto::generated::lance_distributed as pb;
use pb::{
    lance_executor_service_server::LanceExecutorService,
    LocalSearchRequest, LocalSearchResponse,
};

/// gRPC service running on each Worker (Executor) node.
pub struct WorkerService {
    registry: Arc<LanceTableRegistry>,
    /// Per-response payload cap (bytes). 0 = disabled. When exceeded, the
    /// worker shrinks the returned batch to fit, preventing a hot-row-size
    /// query from ballooning the coordinator's gather buffer.
    max_response_bytes: usize,
}

impl WorkerService {
    pub fn new(registry: Arc<LanceTableRegistry>) -> Self {
        Self { registry, max_response_bytes: 128 * 1024 * 1024 }
    }

    pub fn with_max_response_bytes(registry: Arc<LanceTableRegistry>, max_bytes: usize) -> Self {
        Self { registry, max_response_bytes: max_bytes }
    }
}

#[tonic::async_trait]
impl LanceExecutorService for WorkerService {
    #[tracing::instrument(
        skip_all,
        fields(
            table = %request.get_ref().table_name,
            query_type = ?request.get_ref().query_type,
            k = ?request.get_ref().k,
            trace_id = tracing::field::Empty,
        )
    )]
    async fn execute_local_search(
        &self,
        request: Request<LocalSearchRequest>,
    ) -> Result<Response<LocalSearchResponse>, Status> {
        // B1 + D7: surface trace_id in both the info! log line and as
        // a recorded field on the enclosing tracing span, so OTLP
        // consumers (Jaeger / Tempo) see `trace_id` as a first-class
        // attribute without having to parse the raw traceparent
        // header.
        if let Some(tid) = extract_trace_id(&request) {
            tracing::Span::current().record("trace_id", tid.as_str());
            info!("local_search trace_id={tid} table={}", request.get_ref().table_name);
        }
        let req = request.into_inner();
        debug!("Worker: table={}, type={}", req.table_name, req.query_type);

        // Phase 1: fungible worker path — when coord ships a URI, ensure
        // the table is loaded. open_table is idempotent (fast-path when
        // already registered), so no pre-check needed. Lets coord
        // dispatch to any healthy worker without requiring ShardConfig
        // preload.
        if let Some(ref uri) = req.shard_uri
            && let Err(e) = self
                .registry
                .open_table(&req.table_name, uri, &std::collections::HashMap::new())
                .await
        {
            warn!("lazy open_table({}) failed: {}", req.table_name, e);
            return Ok(Response::new(LocalSearchResponse {
                arrow_ipc_data: vec![],
                num_rows: 0,
                error: format!("lazy open failed: {e}"),
            }));
        }

        let descriptor = local_request_to_descriptor(&req);

        match self.registry.execute_query(&descriptor).await {
            Ok(mut batch) => {
                let mut ipc_data = record_batch_to_ipc(&batch)
                    .map_err(|e| Status::internal(format!("IPC error: {e}")))?;
                // Cap response size. If oversized, shrink rows by halving until under cap.
                if self.max_response_bytes > 0 && ipc_data.len() > self.max_response_bytes {
                    let mut rows = batch.num_rows();
                    while rows > 1 {
                        rows = rows.div_ceil(2);
                        let trial = batch.slice(0, rows);
                        if let Ok(buf) = record_batch_to_ipc(&trial)
                            && buf.len() <= self.max_response_bytes {
                                batch = trial; ipc_data = buf; break;
                            }
                    }
                    warn!("Worker response capped to {} rows ({} bytes) by max_response_bytes={}",
                        batch.num_rows(), ipc_data.len(), self.max_response_bytes);
                }
                Ok(Response::new(LocalSearchResponse {
                    arrow_ipc_data: ipc_data,
                    num_rows: batch.num_rows() as u32,
                    error: String::new(),
                }))
            }
            Err(e) => {
                warn!("Worker error: {}", e);
                Ok(Response::new(LocalSearchResponse {
                    arrow_ipc_data: vec![],
                    num_rows: 0,
                    error: e.to_string(),
                }))
            }
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            table = %request.get_ref().table_name,
            write_type = ?request.get_ref().write_type,
            trace_id = tracing::field::Empty,
        )
    )]
    async fn execute_local_write(
        &self,
        request: Request<pb::LocalWriteRequest>,
    ) -> Result<Response<pb::LocalWriteResponse>, Status> {
        if let Some(tid) = extract_trace_id(&request) {
            tracing::Span::current().record("trace_id", tid.as_str());
            info!("local_write trace_id={tid} table={} type={}",
                  request.get_ref().table_name, request.get_ref().write_type);
        }
        let req = request.into_inner();
        debug!("Worker write: table={}, type={}", req.table_name, req.write_type);

        // Phase 3: lazy-open before write (mirrors phase 1 read path).
        // Coord routes single-shard writes to the HRW-picked primary,
        // which may not have the shard preloaded.
        if let Some(ref uri) = req.shard_uri
            && let Err(e) = self
                .registry
                .open_table(&req.table_name, uri, &std::collections::HashMap::new())
                .await
        {
            warn!("lazy open_table({}) for write failed: {}", req.table_name, e);
            return Ok(Response::new(pb::LocalWriteResponse {
                affected_rows: 0,
                new_version: 0,
                error: format!("lazy open failed: {e}"),
            }));
        }

        match self.registry.execute_write(&req).await {
            Ok((affected_rows, new_version)) => {
                Ok(Response::new(pb::LocalWriteResponse {
                    affected_rows,
                    new_version,
                    error: String::new(),
                }))
            }
            Err(e) => {
                warn!("Worker write error: {}", e);
                Ok(Response::new(pb::LocalWriteResponse {
                    affected_rows: 0,
                    new_version: 0,
                    error: e.to_string(),
                }))
            }
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(shard = %request.get_ref().shard_name)
    )]
    async fn load_shard(
        &self,
        request: Request<pb::LoadShardRequest>,
    ) -> Result<Response<pb::LoadShardResponse>, Status> {
        let req = request.into_inner();
        debug!("Worker: loading shard {} from {}", req.shard_name, req.uri);

        let storage_opts: std::collections::HashMap<String, String> = req.storage_options;
        match self.registry.load_shard(&req.shard_name, &req.uri, &storage_opts).await {
            Ok(num_rows) => Ok(Response::new(pb::LoadShardResponse {
                num_rows,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::LoadShardResponse {
                num_rows: 0,
                error: e.to_string(),
            })),
        }
    }

    async fn unload_shard(
        &self,
        request: Request<pb::UnloadShardRequest>,
    ) -> Result<Response<pb::UnloadShardResponse>, Status> {
        let req = request.into_inner();
        self.registry.unload_shard(&req.shard_name).await;
        Ok(Response::new(pb::UnloadShardResponse { error: String::new() }))
    }

    #[tracing::instrument(
        skip_all,
        fields(table = %request.get_ref().table_name)
    )]
    async fn open_table(
        &self,
        request: Request<pb::OpenTableRequest>,
    ) -> Result<Response<pb::OpenTableResponse>, Status> {
        let req = request.into_inner();
        debug!("Worker: open_table {} from {}", req.table_name, req.uri);
        let storage_opts: std::collections::HashMap<String, String> = req.storage_options;
        match self.registry.open_table(&req.table_name, &req.uri, &storage_opts).await {
            Ok(num_rows) => Ok(Response::new(pb::OpenTableResponse {
                num_rows,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::OpenTableResponse {
                num_rows: 0,
                error: e.to_string(),
            })),
        }
    }

    async fn close_table(
        &self,
        request: Request<pb::CloseTableRequest>,
    ) -> Result<Response<pb::CloseTableResponse>, Status> {
        let req = request.into_inner();
        self.registry.close_table(&req.table_name).await;
        Ok(Response::new(pb::CloseTableResponse { error: String::new() }))
    }

    async fn execute_create_index(
        &self,
        request: Request<pb::CreateIndexRequest>,
    ) -> Result<Response<pb::CreateIndexResponse>, Status> {
        let req = request.into_inner();
        match self.registry.create_index_on_table(&req.table_name, &req.column, &req.index_type, req.num_partitions).await {
            Ok(()) => Ok(Response::new(pb::CreateIndexResponse { error: String::new() })),
            Err(e) => Ok(Response::new(pb::CreateIndexResponse { error: e.to_string() })),
        }
    }

    /// #5.3 Schema evolution — ADD COLUMN NULLABLE on this worker's
    /// shard. Coord holds the cross-coord DDL lease (#5.2) before
    /// fanning out; this handler does no locking of its own.
    async fn execute_alter_table(
        &self,
        request: Request<pb::LocalAlterTableRequest>,
    ) -> Result<Response<pb::LocalAlterTableResponse>, Status> {
        let req = request.into_inner();
        // R3: apply ADD, then DROP, then RENAME in that order. Keeps
        // the invariant that a dropped-then-renamed column can't
        // alias a fresh ADD of the same name within one request.
        if !req.add_columns_arrow_ipc.is_empty()
            && let Err(e) = self.registry
                .add_columns_on_shard(&req.shard_name, &req.add_columns_arrow_ipc)
                .await
        {
            return Ok(Response::new(pb::LocalAlterTableResponse { error: e.to_string() }));
        }
        if !req.drop_columns.is_empty()
            && let Err(e) = self.registry
                .drop_columns_on_shard(&req.shard_name, &req.drop_columns)
                .await
        {
            return Ok(Response::new(pb::LocalAlterTableResponse { error: e.to_string() }));
        }
        if !req.rename_columns.is_empty()
            && let Err(e) = self.registry
                .rename_columns_on_shard(&req.shard_name, &req.rename_columns)
                .await
        {
            return Ok(Response::new(pb::LocalAlterTableResponse { error: e.to_string() }));
        }
        Ok(Response::new(pb::LocalAlterTableResponse { error: String::new() }))
    }

    async fn get_table_info(
        &self,
        request: Request<pb::GetTableInfoRequest>,
    ) -> Result<Response<pb::GetTableInfoResponse>, Status> {
        let req = request.into_inner();
        match self.registry.get_table_info(&req.table_name).await {
            Ok((num_rows, columns)) => Ok(Response::new(pb::GetTableInfoResponse {
                num_rows, columns, error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::GetTableInfoResponse {
                num_rows: 0, columns: vec![], error: e.to_string(),
            })),
        }
    }

    async fn compact_all(
        &self,
        _request: Request<pb::CompactRequest>,
    ) -> Result<Response<pb::CompactResponse>, Status> {
        match self.registry.compact_all().await {
            Ok(count) => Ok(Response::new(pb::CompactResponse {
                tables_compacted: count,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::CompactResponse {
                tables_compacted: 0,
                error: e.to_string(),
            })),
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(shard = %request.get_ref().shard_name)
    )]
    async fn create_local_shard(
        &self,
        request: Request<pb::CreateLocalShardRequest>,
    ) -> Result<Response<pb::CreateLocalShardResponse>, Status> {
        let req = request.into_inner();
        if req.shard_name.is_empty() || req.parent_uri.is_empty() || req.arrow_ipc_data.is_empty() {
            return Err(Status::invalid_argument("shard_name, parent_uri, and data required"));
        }
        let batch = lance_distributed_common::ipc::ipc_to_record_batch(&req.arrow_ipc_data)
            .map_err(|e| Status::internal(format!("IPC decode: {e}")))?;
        let index_col = if req.index_column.is_empty() { None } else { Some(req.index_column.as_str()) };

        match self.registry.create_local_shard(
            &req.shard_name, &req.parent_uri, batch,
            index_col, req.index_num_partitions, &req.storage_options,
        ).await {
            Ok((num_rows, uri)) => Ok(Response::new(pb::CreateLocalShardResponse {
                num_rows, uri, error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::CreateLocalShardResponse {
                num_rows: 0, uri: String::new(), error: e.to_string(),
            })),
        }
    }

    async fn local_query(
        &self,
        request: Request<pb::QueryRequest>,
    ) -> Result<Response<pb::LocalSearchResponse>, Status> {
        let req = request.into_inner();
        if req.filter.is_empty() {
            return Err(Status::invalid_argument("filter required for query scan"));
        }
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let columns: Vec<String> = req.columns.into_iter().collect();
        match self.registry.scan_query(
            &req.table_name, &req.filter, limit, req.offset as usize, &columns
        ).await {
            Ok(batch) => {
                let ipc_data = record_batch_to_ipc(&batch)
                    .map_err(|e| Status::internal(format!("IPC: {e}")))?;
                Ok(Response::new(pb::LocalSearchResponse {
                    arrow_ipc_data: ipc_data,
                    num_rows: batch.num_rows() as u32,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(pb::LocalSearchResponse {
                arrow_ipc_data: vec![], num_rows: 0, error: e.to_string(),
            })),
        }
    }

    async fn local_get_by_ids(
        &self,
        request: Request<pb::GetByIdsRequest>,
    ) -> Result<Response<pb::LocalSearchResponse>, Status> {
        let req = request.into_inner();
        let id_col = if req.id_column.is_empty() { "id" } else { &req.id_column };
        let columns: Vec<String> = req.columns.into_iter().collect();
        match self.registry.get_by_ids(&req.table_name, &req.ids, id_col, &columns).await {
            Ok(batch) => {
                let ipc_data = record_batch_to_ipc(&batch)
                    .map_err(|e| Status::internal(format!("IPC: {e}")))?;
                Ok(Response::new(pb::LocalSearchResponse {
                    arrow_ipc_data: ipc_data,
                    num_rows: batch.num_rows() as u32,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(pb::LocalSearchResponse {
                arrow_ipc_data: vec![], num_rows: 0, error: e.to_string(),
            })),
        }
    }

    async fn health_check(
        &self,
        _request: Request<pb::HealthCheckRequest>,
    ) -> Result<Response<pb::HealthCheckResponse>, Status> {
        let (loaded_shards, total_rows) = self.registry.status().await;
        let shard_names = self.registry.shard_names().await;
        Ok(Response::new(pb::HealthCheckResponse {
            healthy: true,
            loaded_shards,
            total_rows,
            shard_names,
            server_version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    async fn execute_create_tag(
        &self,
        request: Request<pb::LocalCreateTagRequest>,
    ) -> Result<Response<pb::CreateTagResponse>, Status> {
        let req = request.into_inner();
        match self.registry.create_tag_on_shard(&req.shard_name, &req.tag_name, req.version).await {
            Ok(v) => Ok(Response::new(pb::CreateTagResponse {
                tagged_version: v, error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::CreateTagResponse {
                tagged_version: 0, error: e.to_string(),
            })),
        }
    }

    async fn execute_list_tags(
        &self,
        request: Request<pb::LocalListTagsRequest>,
    ) -> Result<Response<pb::ListTagsResponse>, Status> {
        let req = request.into_inner();
        match self.registry.list_tags_on_shard(&req.shard_name).await {
            Ok(pairs) => Ok(Response::new(pb::ListTagsResponse {
                tags: pairs.into_iter().map(|(name, version)| pb::TagInfo { name, version }).collect(),
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::ListTagsResponse {
                tags: vec![], error: e.to_string(),
            })),
        }
    }

    async fn execute_delete_tag(
        &self,
        request: Request<pb::LocalDeleteTagRequest>,
    ) -> Result<Response<pb::DeleteTagResponse>, Status> {
        let req = request.into_inner();
        match self.registry.delete_tag_on_shard(&req.shard_name, &req.tag_name).await {
            Ok(()) => Ok(Response::new(pb::DeleteTagResponse { error: String::new() })),
            Err(e) => Ok(Response::new(pb::DeleteTagResponse { error: e.to_string() })),
        }
    }

    async fn execute_restore_table(
        &self,
        request: Request<pb::LocalRestoreTableRequest>,
    ) -> Result<Response<pb::RestoreTableResponse>, Status> {
        let req = request.into_inner();
        match self.registry.restore_on_shard(&req.shard_name, req.version, &req.tag).await {
            Ok(v) => Ok(Response::new(pb::RestoreTableResponse {
                new_version: v, error: String::new(),
            })),
            Err(e) => Ok(Response::new(pb::RestoreTableResponse {
                new_version: 0, error: e.to_string(),
            })),
        }
    }
}

fn local_request_to_descriptor(req: &LocalSearchRequest) -> LanceQueryDescriptor {
    LanceQueryDescriptor {
        table_name: req.table_name.clone(),
        query_type: req.query_type,
        vector_query: match (&req.vector_column, &req.query_vector) {
            (Some(col), Some(vec_data)) => Some(VectorQueryParams {
                column: col.clone(),
                vector_data: vec_data.clone(),
                dimension: req.dimension.unwrap_or(0),
                nprobes: req.nprobes.unwrap_or(10),
                metric_type: req.metric_type.unwrap_or(0),
                oversample_factor: 1,
            }),
            _ => None,
        },
        fts_query: match (&req.text_column, &req.query_text) {
            (Some(col), Some(text)) => Some(FtsQueryParams {
                query_text: text.clone(),
                column: col.clone(),
            }),
            _ => None,
        },
        filter: req.filter.clone(),
        k: req.k,
        columns: req.columns.clone(),
        fragment_ids: req.fragment_ids.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_local_req() -> LocalSearchRequest {
        LocalSearchRequest {
            query_type: 0, // Ann
            table_name: "products".to_string(),
            vector_column: Some("vector".to_string()),
            query_vector: Some(vec![0u8; 16]),
            dimension: Some(4),
            nprobes: Some(20),
            metric_type: Some(1),
            text_column: None,
            query_text: None,
            k: 10,
            filter: Some("category = 'cat_0'".to_string()),
            columns: vec!["id".to_string(), "category".to_string()],
            fragment_ids: vec![],
            shard_uri: None,
        }
    }

    #[test]
    fn test_descriptor_from_ann_request() {
        let req = make_local_req();
        let desc = local_request_to_descriptor(&req);
        assert_eq!(desc.table_name, "products");
        assert_eq!(desc.query_type, 0);
        assert_eq!(desc.k, 10);
        assert_eq!(desc.filter.as_deref(), Some("category = 'cat_0'"));
        assert_eq!(desc.columns, vec!["id", "category"]);
        let vq = desc.vector_query.unwrap();
        assert_eq!(vq.column, "vector");
        assert_eq!(vq.dimension, 4);
        assert_eq!(vq.nprobes, 20);
        assert_eq!(vq.metric_type, 1);
        assert!(desc.fts_query.is_none());
    }

    #[test]
    fn test_descriptor_from_fts_request() {
        let req = LocalSearchRequest {
            query_type: 1, // Fts
            table_name: "docs".to_string(),
            vector_column: None,
            query_vector: None,
            dimension: None,
            nprobes: None,
            metric_type: None,
            text_column: Some("content".to_string()),
            query_text: Some("hello world".to_string()),
            k: 5,
            filter: None,
            columns: vec![],
            fragment_ids: vec![],
            shard_uri: None,
        };
        let desc = local_request_to_descriptor(&req);
        assert_eq!(desc.table_name, "docs");
        assert_eq!(desc.query_type, 1);
        assert!(desc.vector_query.is_none());
        let fts = desc.fts_query.unwrap();
        assert_eq!(fts.column, "content");
        assert_eq!(fts.query_text, "hello world");
    }

    #[test]
    fn test_descriptor_from_hybrid_request() {
        let req = LocalSearchRequest {
            query_type: 2, // Hybrid
            table_name: "products".to_string(),
            vector_column: Some("vector".to_string()),
            query_vector: Some(vec![0u8; 16]),
            dimension: Some(4),
            nprobes: Some(10),
            metric_type: Some(0),
            text_column: Some("description".to_string()),
            query_text: Some("similar products".to_string()),
            k: 20,
            filter: None,
            columns: vec![],
            fragment_ids: vec![],
            shard_uri: None,
        };
        let desc = local_request_to_descriptor(&req);
        assert_eq!(desc.query_type, 2);
        assert!(desc.vector_query.is_some());
        assert!(desc.fts_query.is_some());
    }

    #[test]
    fn test_descriptor_defaults_nprobes_and_metric() {
        let req = LocalSearchRequest {
            query_type: 0,
            table_name: "t".to_string(),
            vector_column: Some("v".to_string()),
            query_vector: Some(vec![0u8; 4]),
            dimension: None,  // not provided
            nprobes: None,    // not provided
            metric_type: None, // not provided
            text_column: None,
            query_text: None,
            k: 1,
            filter: None,
            columns: vec![],
            fragment_ids: vec![],
            shard_uri: None,
        };
        let desc = local_request_to_descriptor(&req);
        let vq = desc.vector_query.unwrap();
        assert_eq!(vq.dimension, 0);  // defaults to 0
        assert_eq!(vq.nprobes, 10);    // defaults to 10
        assert_eq!(vq.metric_type, 0); // defaults to 0
    }
}
