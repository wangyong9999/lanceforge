// Shared test harness: a configurable in-process tonic server that
// speaks the LanceExecutorService protocol. Used by scatter_gather
// and connection_pool tests to exercise the async / gRPC paths that
// unit-level tests of helper functions can't reach.
//
// Every behavior is adjustable at runtime via Arc<RwLock<...>> so a
// single test can flip a worker from healthy → unhealthy → back.

#![cfg(test)]
// Some variants / methods are deliberate future-use scaffolding kept
// alongside their sibling variants so adding a new test scenario
// doesn't require editing the harness. Phase C/D tests will exercise
// `Slow`, `set_search`, and `MockWorkerHandle::shutdown`.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::{Float32Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

// Mock responses include both `_distance` and `_score` metric columns so
// callers can merge by either without reconfiguring the mock. Real
// worker responses only include one; for coverage tests it doesn't
// matter as long as the schema contains the column merge looks up.
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use lance_distributed_common::ipc::record_batch_to_ipc;
use lance_distributed_proto::generated::lance_distributed as pb;
use pb::lance_executor_service_server::{
    LanceExecutorService, LanceExecutorServiceServer,
};

#[derive(Clone, Debug)]
pub enum SearchBehavior {
    Ok { rows: usize },
    Empty,
    AppError(String),
    GrpcError(String),
    GarbageIpc,
    Slow(Duration),
}

#[derive(Clone, Debug)]
pub enum HealthBehavior {
    Ok { loaded_shards: u32, total_rows: u64, shard_names: Vec<String> },
    Err,
    Slow(Duration),
}

pub struct MockState {
    pub search: RwLock<SearchBehavior>,
    pub health: RwLock<HealthBehavior>,
    pub search_calls: AtomicU64,
    pub health_calls: AtomicU64,
    pub load_shard_calls: AtomicU64,
}

impl MockState {
    pub fn new(search: SearchBehavior, health: HealthBehavior) -> Arc<Self> {
        Arc::new(Self {
            search: RwLock::new(search),
            health: RwLock::new(health),
            search_calls: AtomicU64::new(0),
            health_calls: AtomicU64::new(0),
            load_shard_calls: AtomicU64::new(0),
        })
    }

    pub async fn set_health(&self, b: HealthBehavior) {
        *self.health.write().await = b;
    }

    pub async fn set_search(&self, b: SearchBehavior) {
        *self.search.write().await = b;
    }
}

pub struct MockExecutor {
    state: Arc<MockState>,
}

pub fn sample_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("_distance", DataType::Float32, false),
        Field::new("_score", DataType::Float32, false),
    ]));
    let ids: Vec<i32> = (0..rows as i32).collect();
    let dists: Vec<f32> = (0..rows).map(|i| i as f32 * 0.1).collect();
    let scores: Vec<f32> = (0..rows).map(|i| 1.0 - (i as f32 * 0.1)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Float32Array::from(dists)),
            Arc::new(Float32Array::from(scores)),
        ],
    ).unwrap()
}

#[tonic::async_trait]
impl LanceExecutorService for MockExecutor {
    async fn execute_local_search(
        &self,
        _req: Request<pb::LocalSearchRequest>,
    ) -> Result<Response<pb::LocalSearchResponse>, Status> {
        self.state.search_calls.fetch_add(1, Ordering::SeqCst);
        let behavior = self.state.search.read().await.clone();
        match behavior {
            SearchBehavior::Ok { rows } => {
                let batch = sample_batch(rows);
                let ipc = record_batch_to_ipc(&batch)
                    .map_err(|e| Status::internal(format!("ipc: {e}")))?;
                Ok(Response::new(pb::LocalSearchResponse {
                    arrow_ipc_data: ipc,
                    num_rows: rows as u32,
                    error: String::new(),
                }))
            }
            SearchBehavior::Empty => Ok(Response::new(pb::LocalSearchResponse {
                arrow_ipc_data: vec![],
                num_rows: 0,
                error: String::new(),
            })),
            SearchBehavior::AppError(msg) => Ok(Response::new(pb::LocalSearchResponse {
                arrow_ipc_data: vec![],
                num_rows: 0,
                error: msg,
            })),
            SearchBehavior::GrpcError(msg) => Err(Status::internal(msg)),
            SearchBehavior::GarbageIpc => Ok(Response::new(pb::LocalSearchResponse {
                arrow_ipc_data: vec![0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa],
                num_rows: 1,
                error: String::new(),
            })),
            SearchBehavior::Slow(d) => {
                tokio::time::sleep(d).await;
                let batch = sample_batch(1);
                let ipc = record_batch_to_ipc(&batch)
                    .map_err(|e| Status::internal(format!("ipc: {e}")))?;
                Ok(Response::new(pb::LocalSearchResponse {
                    arrow_ipc_data: ipc,
                    num_rows: 1,
                    error: String::new(),
                }))
            }
        }
    }

    async fn execute_local_write(
        &self,
        _req: Request<pb::LocalWriteRequest>,
    ) -> Result<Response<pb::LocalWriteResponse>, Status> {
        Ok(Response::new(pb::LocalWriteResponse {
            affected_rows: 0,
            new_version: 0,
            error: String::new(),
        }))
    }

    async fn load_shard(
        &self,
        _req: Request<pb::LoadShardRequest>,
    ) -> Result<Response<pb::LoadShardResponse>, Status> {
        self.state.load_shard_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(pb::LoadShardResponse {
            num_rows: 0,
            error: String::new(),
        }))
    }

    async fn unload_shard(
        &self,
        _req: Request<pb::UnloadShardRequest>,
    ) -> Result<Response<pb::UnloadShardResponse>, Status> {
        Ok(Response::new(pb::UnloadShardResponse { error: String::new() }))
    }

    async fn open_table(
        &self,
        _req: Request<pb::OpenTableRequest>,
    ) -> Result<Response<pb::OpenTableResponse>, Status> {
        Ok(Response::new(pb::OpenTableResponse {
            num_rows: 0,
            error: String::new(),
        }))
    }

    async fn close_table(
        &self,
        _req: Request<pb::CloseTableRequest>,
    ) -> Result<Response<pb::CloseTableResponse>, Status> {
        Ok(Response::new(pb::CloseTableResponse { error: String::new() }))
    }

    async fn execute_create_index(
        &self,
        _req: Request<pb::CreateIndexRequest>,
    ) -> Result<Response<pb::CreateIndexResponse>, Status> {
        Ok(Response::new(pb::CreateIndexResponse { error: String::new() }))
    }

    async fn get_table_info(
        &self,
        _req: Request<pb::GetTableInfoRequest>,
    ) -> Result<Response<pb::GetTableInfoResponse>, Status> {
        Ok(Response::new(pb::GetTableInfoResponse {
            num_rows: 0,
            columns: vec![],
            error: String::new(),
        }))
    }

    async fn compact_all(
        &self,
        _req: Request<pb::CompactRequest>,
    ) -> Result<Response<pb::CompactResponse>, Status> {
        Ok(Response::new(pb::CompactResponse {
            tables_compacted: 0,
            error: String::new(),
        }))
    }

    async fn create_local_shard(
        &self,
        _req: Request<pb::CreateLocalShardRequest>,
    ) -> Result<Response<pb::CreateLocalShardResponse>, Status> {
        Ok(Response::new(pb::CreateLocalShardResponse {
            num_rows: 0,
            uri: String::new(),
            error: String::new(),
        }))
    }

    async fn local_get_by_ids(
        &self,
        _req: Request<pb::GetByIdsRequest>,
    ) -> Result<Response<pb::LocalSearchResponse>, Status> {
        Ok(Response::new(pb::LocalSearchResponse {
            arrow_ipc_data: vec![],
            num_rows: 0,
            error: String::new(),
        }))
    }

    async fn local_query(
        &self,
        _req: Request<pb::QueryRequest>,
    ) -> Result<Response<pb::LocalSearchResponse>, Status> {
        Ok(Response::new(pb::LocalSearchResponse {
            arrow_ipc_data: vec![],
            num_rows: 0,
            error: String::new(),
        }))
    }

    async fn execute_alter_table(
        &self,
        _req: Request<pb::LocalAlterTableRequest>,
    ) -> Result<Response<pb::LocalAlterTableResponse>, Status> {
        // Mock worker: no-op success. Coord integration tests that
        // actually care about the schema mutation run against a
        // real lance-worker binary via the Python E2E suite.
        Ok(Response::new(pb::LocalAlterTableResponse {
            error: String::new(),
        }))
    }

    async fn health_check(
        &self,
        _req: Request<pb::HealthCheckRequest>,
    ) -> Result<Response<pb::HealthCheckResponse>, Status> {
        self.state.health_calls.fetch_add(1, Ordering::SeqCst);
        let behavior = self.state.health.read().await.clone();
        match behavior {
            HealthBehavior::Ok { loaded_shards, total_rows, shard_names } => {
                Ok(Response::new(pb::HealthCheckResponse {
                    healthy: true,
                    loaded_shards,
                    total_rows,
                    shard_names,
                    server_version: String::new(),
                }))
            }
            HealthBehavior::Err => Err(Status::unavailable("mock unhealthy")),
            HealthBehavior::Slow(d) => {
                tokio::time::sleep(d).await;
                Ok(Response::new(pb::HealthCheckResponse {
                    healthy: true,
                    loaded_shards: 0,
                    total_rows: 0,
                    shard_names: vec![],
                    server_version: String::new(),
                }))
            }
        }
    }

    async fn execute_create_tag(
        &self,
        _req: Request<pb::LocalCreateTagRequest>,
    ) -> Result<Response<pb::CreateTagResponse>, Status> {
        Ok(Response::new(pb::CreateTagResponse {
            tagged_version: 0, error: String::new(),
        }))
    }

    async fn execute_list_tags(
        &self,
        _req: Request<pb::LocalListTagsRequest>,
    ) -> Result<Response<pb::ListTagsResponse>, Status> {
        Ok(Response::new(pb::ListTagsResponse {
            tags: vec![], error: String::new(),
        }))
    }

    async fn execute_delete_tag(
        &self,
        _req: Request<pb::LocalDeleteTagRequest>,
    ) -> Result<Response<pb::DeleteTagResponse>, Status> {
        Ok(Response::new(pb::DeleteTagResponse { error: String::new() }))
    }

    async fn execute_restore_table(
        &self,
        _req: Request<pb::LocalRestoreTableRequest>,
    ) -> Result<Response<pb::RestoreTableResponse>, Status> {
        Ok(Response::new(pb::RestoreTableResponse {
            new_version: 0, error: String::new(),
        }))
    }
}

pub struct MockWorkerHandle {
    pub port: u16,
    pub state: Arc<MockState>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockWorkerHandle {
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for MockWorkerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawn a mock worker on a free loopback port. The returned handle
/// keeps the server alive; drop it (or call shutdown) to stop.
pub async fn spawn_mock_worker(state: Arc<MockState>) -> MockWorkerHandle {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let svc = LanceExecutorServiceServer::new(MockExecutor { state: state.clone() });
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = rx.await;
            })
            .await;
    });
    // Give the server a brief moment to be ready to accept.
    tokio::time::sleep(Duration::from_millis(30)).await;
    MockWorkerHandle {
        port,
        state,
        shutdown: Some(tx),
    }
}
