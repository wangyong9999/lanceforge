// Licensed under the Apache License, Version 2.0.
// Worker-side gRPC service: handles local search requests from Coordinator.

use std::sync::Arc;

use log::{debug, warn};
use tonic::{Request, Response, Status};

use lance_distributed_common::ipc::record_batch_to_ipc;
use lance_distributed_proto::descriptor::{FtsQueryParams, LanceQueryDescriptor, LanceQueryType, VectorQueryParams};
use crate::executor::LanceTableRegistry;
use lance_distributed_proto::generated::lance_distributed as pb;
use pb::{
    lance_executor_service_server::LanceExecutorService,
    LocalSearchRequest, LocalSearchResponse,
};

/// gRPC service running on each Worker (Executor) node.
pub struct WorkerService {
    registry: Arc<LanceTableRegistry>,
}

impl WorkerService {
    pub fn new(registry: Arc<LanceTableRegistry>) -> Self {
        Self { registry }
    }
}

#[tonic::async_trait]
impl LanceExecutorService for WorkerService {
    async fn execute_local_search(
        &self,
        request: Request<LocalSearchRequest>,
    ) -> Result<Response<LocalSearchResponse>, Status> {
        let req = request.into_inner();
        debug!("Worker: table={}, type={}", req.table_name, req.query_type);

        let descriptor = local_request_to_descriptor(&req);

        match self.registry.execute_query(&descriptor).await {
            Ok(batch) => {
                let ipc_data = record_batch_to_ipc(&batch)
                    .map_err(|e| Status::internal(format!("IPC error: {e}")))?;
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

    async fn health_check(
        &self,
        _request: Request<pb::HealthCheckRequest>,
    ) -> Result<Response<pb::HealthCheckResponse>, Status> {
        Ok(Response::new(pb::HealthCheckResponse {
            healthy: true,
            loaded_shards: 0,
            total_rows: 0,
        }))
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
        };
        let desc = local_request_to_descriptor(&req);
        let vq = desc.vector_query.unwrap();
        assert_eq!(vq.dimension, 0);  // defaults to 0
        assert_eq!(vq.nprobes, 10);    // defaults to 10
        assert_eq!(vq.metric_type, 0); // defaults to 0
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
    }
}
