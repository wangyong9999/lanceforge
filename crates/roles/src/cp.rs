// Control Plane role — library implementation.
//
// Phase C.3 of docs/ARCHITECTURE_V2_PLAN.md. Runs a gRPC server that
// serves the ClusterControl + NodeLifecycle protocols defined in
// Phase C.1 (crates/proto/proto/cluster_control.proto). Backed by:
//
//   - MetaStore (FileMetaStore or S3MetaStore from config)
//   - SchemaStore (Phase C.2, versioned per-table Arrow IPC bytes)
//   - ShardState (reconstructed from config + MetaStore like the
//     existing CoordinatorService does)
//
// Composition: the monolith binary (Phase B) spawns `run` as one of
// three concurrent role futures alongside qn_cp::run and pe_idx::run.
// Phase E will also build a standalone `lance-cp` bin from the same
// function. Because this is purely additive in C.3 — no QN calls CP
// yet — bringing it up does not change any observable behaviour.
//
// Phase C.3 deliberate stubs (flagged so future work has a clear
// entry point):
//
//   * SubscribeRouting returns an empty long-lived stream. Real push
//     invalidation lands in Phase D when IDX signals routing
//     mutations (compact / rebalance).
//   * GetPolicy reads from MetaStore's existing auth/keys/ prefix
//     when MetaStore is wired; otherwise returns empty (= unknown
//     key). Phase C.4 wires the QN-side policy cache.
//   * NodeLifecycle RPCs (Register / Heartbeat / Deregister)
//     currently ack everything. Real registry + healthy_set
//     aggregation lands in Phase D.
//
// For ResolveTable and ResolveSchema the implementation is real —
// those are the two reads QN needs to function in Phase C.4.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use lance_distributed_common::config::ClusterConfig;
use lance_distributed_common::shard_state::{ShardState, StaticShardState};
use lance_distributed_meta::schema_store::SchemaStore;
use lance_distributed_meta::store::{FileMetaStore, MetaStore, S3MetaStore};
use lance_distributed_proto::cluster_control_server::{
    ClusterControl, ClusterControlServer,
};
use lance_distributed_proto::node_lifecycle_server::{
    NodeLifecycle, NodeLifecycleServer,
};
use lance_distributed_proto::{
    DeregisterNodeRequest, DeregisterNodeResponse, GetPolicyRequest,
    HeartbeatRequest, HeartbeatResponse, PolicyBundle, RegisterNodeRequest,
    RegisterNodeResponse, ResolveSchemaRequest, ResolveTableRequest,
    ResolvedSchema, RoutingUpdate, ShardAssignment, SubscribeRoutingRequest,
    TableRouting,
};
use log::info;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::common::ShutdownSignal;

/// Spin up the CP gRPC server on the given port. Returns when
/// `shutdown` fires and the drain completes.
pub async fn run(
    config: ClusterConfig,
    port: u16,
    shutdown: ShutdownSignal,
) -> Result<(), Box<dyn std::error::Error>> {
    let backends = Backends::from_config(&config).await?;
    let svc = CpService {
        schema_store: Arc::new(SchemaStore::new(backends.meta.clone())),
        shard_state: backends.shard_state,
        meta: backends.meta,
    };

    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    info!("Starting Control Plane on port {port}");
    let sb = shutdown.clone();
    tonic::transport::Server::builder()
        .add_service(ClusterControlServer::new(svc.clone()))
        .add_service(NodeLifecycleServer::new(svc))
        .serve_with_shutdown(addr, async move {
            sb.notified().await;
        })
        .await?;
    info!("Control Plane on port {port} stopped");
    Ok(())
}

struct Backends {
    meta: Arc<dyn MetaStore>,
    shard_state: Arc<dyn ShardState>,
}

impl Backends {
    async fn from_config(config: &ClusterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Mirror CoordinatorService::with_meta_state's store selection.
        // When no metadata_path is configured the CP falls back to
        // StaticShardState + an unused in-memory MetaStore — in that
        // shape SchemaStore ops have nowhere to land and callers get
        // NotFound for ResolveSchema. That's fine for dev/no-HA; prod
        // deployments always set metadata_path.
        if let Some(path) = config.metadata_path.as_deref() {
            let meta: Arc<dyn MetaStore> = if path.starts_with("s3://")
                || path.starts_with("gs://")
                || path.starts_with("az://")
            {
                Arc::new(
                    S3MetaStore::new(
                        path,
                        config
                            .storage_options
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone())),
                    )
                    .await?,
                )
            } else {
                Arc::new(FileMetaStore::new(path).await?)
            };
            let state = lance_distributed_meta::state::MetaShardState::new(meta.clone(), "lanceforge");
            for e in &config.executors {
                state.register_executor(&e.id, &e.host, e.port).await;
            }
            for (table_name, shard_routes) in config.build_routing_table() {
                if state.get_current_target(&table_name).await.is_none() {
                    let mapping: lance_distributed_common::shard_state::ShardMapping =
                        shard_routes.into_iter().collect();
                    state.register_table(&table_name, mapping).await;
                }
            }
            Ok(Backends {
                meta,
                shard_state: Arc::new(state) as Arc<dyn ShardState>,
            })
        } else {
            // No metadata_path — fall back to an ephemeral FileMetaStore
            // in /tmp and StaticShardState from config. Dev / monolith
            // quickstart use this; prod deployments always configure
            // metadata_path. Schema ops still work against the tmp file
            // but state is discarded on restart.
            let tmp = std::env::temp_dir()
                .join(format!("lanceforge-cp-{}.json", std::process::id()));
            let path = tmp
                .to_str()
                .ok_or("temp path contains non-utf8")?
                .to_string();
            let meta: Arc<dyn MetaStore> = Arc::new(FileMetaStore::new(&path).await?);
            Ok(Backends {
                meta,
                shard_state: Arc::new(StaticShardState::from_config(config)),
            })
        }
    }
}

#[derive(Clone)]
struct CpService {
    meta: Arc<dyn MetaStore>,
    schema_store: Arc<SchemaStore>,
    shard_state: Arc<dyn ShardState>,
}

#[tonic::async_trait]
impl ClusterControl for CpService {
    async fn resolve_table(
        &self,
        request: Request<ResolveTableRequest>,
    ) -> Result<Response<TableRouting>, Status> {
        let req = request.into_inner();
        let routing_rows = self.shard_state.get_shard_routing(&req.table_name).await;
        if routing_rows.is_empty() {
            return Err(Status::not_found(format!("table {}", req.table_name)));
        }
        let schema_version = self
            .schema_store
            .latest_version(&req.table_name)
            .await
            .map_err(|e| Status::internal(format!("schema_store: {e}")))?
            .unwrap_or(0);
        if req.min_schema_version > 0 && schema_version < req.min_schema_version {
            return Err(Status::failed_precondition(format!(
                "schema_version {} < requested {}",
                schema_version, req.min_schema_version
            )));
        }
        let shards = routing_rows
            .into_iter()
            .map(|(shard_name, primary, secondary)| ShardAssignment {
                shard_name,
                primary_executor: primary,
                secondary_executor: secondary.unwrap_or_default(),
                // Phase E will wire the shard_uris registry through
                // ShardState and populate this. Callers (QN/PE) today
                // already know URIs via config/LoadShard so leaving
                // this empty is safe.
                uri: String::new(),
            })
            .collect();
        Ok(Response::new(TableRouting {
            table_name: req.table_name,
            shards,
            schema_version,
            // routing_version is bumped by rebalance ops (post-0.2).
            // Fixed at 0 until then so clients have a stable value.
            routing_version: 0,
        }))
    }

    type SubscribeRoutingStream =
        Pin<Box<dyn Stream<Item = Result<RoutingUpdate, Status>> + Send + 'static>>;

    async fn subscribe_routing(
        &self,
        _request: Request<SubscribeRoutingRequest>,
    ) -> Result<Response<Self::SubscribeRoutingStream>, Status> {
        // Phase C.3: empty long-lived stream. Phase D hooks the
        // sender into routing mutations (ALTER / rebalance) so
        // clients get real push notifications.
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        // Hold the sender across the process lifetime so the stream
        // doesn't close immediately. Phase D will own the sender and
        // produce updates.
        let _keep_alive = Box::leak(Box::new(tx));
        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn resolve_schema(
        &self,
        request: Request<ResolveSchemaRequest>,
    ) -> Result<Response<ResolvedSchema>, Status> {
        let req = request.into_inner();
        let resolved = if req.schema_version == 0 {
            self.schema_store.get_latest(&req.table_name).await
        } else {
            self.schema_store
                .get_schema(&req.table_name, req.schema_version)
                .await
                .map(|opt| opt.map(|bytes| (req.schema_version, bytes)))
        }
        .map_err(|e| Status::internal(format!("schema_store: {e}")))?;

        match resolved {
            Some((version, bytes)) => Ok(Response::new(ResolvedSchema {
                table_name: req.table_name,
                schema_version: version,
                arrow_ipc_schema: bytes,
            })),
            None => Err(Status::not_found(format!(
                "no schema for table {} at version {}",
                req.table_name, req.schema_version
            ))),
        }
    }

    async fn get_policy(
        &self,
        request: Request<GetPolicyRequest>,
    ) -> Result<Response<PolicyBundle>, Status> {
        // Phase C.3: read the existing coord-authored `auth/keys/{raw_key}`
        // JSON shape (see coordinator/src/auth.rs::KeyEntry — just
        // `{role}`). Unknown key returns empty PolicyBundle, which
        // the QN-side cache (Phase C.4) will translate to
        // Unauthenticated. qps_limit and namespace_prefix are not
        // in the current MetaStore JSON shape — they live in
        // coord's process memory today. Phase C.4 / D will extend
        // KeyEntry + migrate writers to the fuller shape.
        let api_key = request.into_inner().api_key;
        let store_key = format!("auth/keys/{api_key}");
        match self.meta.get(&store_key).await {
            Ok(Some(v)) => {
                #[derive(serde::Deserialize, Default)]
                struct Record {
                    #[serde(default)]
                    role: String,
                }
                let rec: Record = serde_json::from_str(&v.value).unwrap_or_default();
                Ok(Response::new(PolicyBundle {
                    role: rec.role,
                    qps_limit: 0,
                    namespace_prefix: String::new(),
                }))
            }
            Ok(None) => Ok(Response::new(PolicyBundle::default())),
            Err(e) => Err(Status::internal(format!("meta store: {e}"))),
        }
    }
}

#[tonic::async_trait]
impl NodeLifecycle for CpService {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        // Phase C.3 no-op. Phase D tracks epoch + healthy_set.
        let req = request.into_inner();
        log::debug!("CP.RegisterNode: id={} role={}", req.node_id, req.role);
        Ok(Response::new(RegisterNodeResponse {
            epoch: 1,
            error: String::new(),
        }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse {
            accepted: true,
            error: String::new(),
        }))
    }

    async fn deregister_node(
        &self,
        _request: Request<DeregisterNodeRequest>,
    ) -> Result<Response<DeregisterNodeResponse>, Status> {
        Ok(Response::new(DeregisterNodeResponse {
            error: String::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_distributed_proto::cluster_control_client::ClusterControlClient;
    use lance_distributed_proto::node_lifecycle_client::NodeLifecycleClient;
    use lance_distributed_proto::NodeRole;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn spawn_cp(config: ClusterConfig, port: u16) -> (ShutdownSignal, tokio::task::JoinHandle<()>) {
        let shutdown = crate::common::new_shutdown();
        let sb = shutdown.clone();
        let handle = tokio::spawn(async move {
            let _ = run(config, port, sb).await;
        });
        // Poll until the server accepts connections.
        for _ in 0..30 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                return (shutdown, handle);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("CP on port {port} never came up");
    }

    fn free_port() -> u16 {
        // Bind to 0 to get a free port from the kernel, then drop.
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    async fn client(port: u16) -> ClusterControlClient<tonic::transport::Channel> {
        ClusterControlClient::connect(format!("http://127.0.0.1:{port}"))
            .await
            .unwrap()
    }

    fn minimal_config(tmp: &TempDir) -> ClusterConfig {
        use lance_distributed_common::config::{
            ClusterConfig, ExecutorConfig, ShardConfig, TableConfig,
        };
        let meta_path = tmp.path().join("meta.json");
        ClusterConfig {
            metadata_path: Some(meta_path.to_str().unwrap().to_string()),
            tables: vec![TableConfig {
                name: "t".into(),
                shards: vec![ShardConfig {
                    name: "s0".into(),
                    uri: "/tmp/cp-test-s0".into(),
                    executors: vec!["w0".into()],
                }],
            }],
            executors: vec![ExecutorConfig {
                id: "w0".into(),
                host: "127.0.0.1".into(),
                port: 9999,
                role: Default::default(),
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn resolve_table_returns_configured_routing() {
        let tmp = TempDir::new().unwrap();
        let config = minimal_config(&tmp);
        let port = free_port();
        let (shutdown, handle) = spawn_cp(config, port).await;

        let mut client = client(port).await;
        let r = client
            .resolve_table(ResolveTableRequest {
                table_name: "t".into(),
                min_schema_version: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.table_name, "t");
        assert_eq!(r.shards.len(), 1);
        assert_eq!(r.shards[0].shard_name, "s0");
        assert_eq!(r.shards[0].primary_executor, "w0");
        assert_eq!(r.schema_version, 0); // no schema written yet

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn resolve_table_missing_returns_not_found() {
        let tmp = TempDir::new().unwrap();
        let config = minimal_config(&tmp);
        let port = free_port();
        let (shutdown, handle) = spawn_cp(config, port).await;

        let mut client = client(port).await;
        let err = client
            .resolve_table(ResolveTableRequest {
                table_name: "does-not-exist".into(),
                min_schema_version: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn resolve_schema_follows_schema_store() {
        let tmp = TempDir::new().unwrap();
        let config = minimal_config(&tmp);
        let port = free_port();

        // Pre-seed MetaStore with a schema via SchemaStore. FileMetaStore
        // holds an advisory file lock, so the handle must be dropped
        // before CP tries to open the same path. Scope it explicitly.
        {
            let meta_path = config.metadata_path.as_deref().unwrap();
            let meta: Arc<dyn MetaStore> =
                Arc::new(FileMetaStore::new(meta_path).await.unwrap());
            let schema_store = SchemaStore::new(meta);
            schema_store
                .put_schema("t", 1, b"arrow-ipc-bytes-v1")
                .await
                .unwrap();
            schema_store
                .put_schema("t", 2, b"arrow-ipc-bytes-v2")
                .await
                .unwrap();
        }

        let (shutdown, handle) = spawn_cp(config, port).await;
        let mut client = client(port).await;

        // version=0 → latest (v2)
        let r = client
            .resolve_schema(ResolveSchemaRequest {
                table_name: "t".into(),
                schema_version: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.schema_version, 2);
        assert_eq!(r.arrow_ipc_schema, b"arrow-ipc-bytes-v2");

        // version=1 → historical
        let r = client
            .resolve_schema(ResolveSchemaRequest {
                table_name: "t".into(),
                schema_version: 1,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.schema_version, 1);
        assert_eq!(r.arrow_ipc_schema, b"arrow-ipc-bytes-v1");

        // unknown version → NotFound
        let err = client
            .resolve_schema(ResolveSchemaRequest {
                table_name: "t".into(),
                schema_version: 99,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn resolve_table_enforces_min_schema_version() {
        let tmp = TempDir::new().unwrap();
        let config = minimal_config(&tmp);
        let port = free_port();

        // Schema v=1 exists; client asks for min_schema_version=2 → fail.
        // Scope pre-seed handles so FileMetaStore releases its lock
        // before CP opens the same path.
        {
            let meta_path = config.metadata_path.as_deref().unwrap();
            let meta: Arc<dyn MetaStore> =
                Arc::new(FileMetaStore::new(meta_path).await.unwrap());
            SchemaStore::new(meta)
                .put_schema("t", 1, b"v1")
                .await
                .unwrap();
        }

        let (shutdown, handle) = spawn_cp(config, port).await;
        let mut client = client(port).await;

        let err = client
            .resolve_table(ResolveTableRequest {
                table_name: "t".into(),
                min_schema_version: 2,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        // Same request with min=1 succeeds.
        let r = client
            .resolve_table(ResolveTableRequest {
                table_name: "t".into(),
                min_schema_version: 1,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.schema_version, 1);

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn node_lifecycle_rpcs_ack() {
        let tmp = TempDir::new().unwrap();
        let config = minimal_config(&tmp);
        let port = free_port();
        let (shutdown, handle) = spawn_cp(config, port).await;

        let mut client = NodeLifecycleClient::connect(format!("http://127.0.0.1:{port}"))
            .await
            .unwrap();

        let r = client
            .register_node(RegisterNodeRequest {
                node_id: "qn-1".into(),
                role: NodeRole::Qn as i32,
                host: "127.0.0.1".into(),
                port: 55000,
                loaded_shards: vec![],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.error, "");
        assert!(r.epoch >= 1);

        let hb = client
            .heartbeat(HeartbeatRequest {
                node_id: "qn-1".into(),
                epoch: r.epoch,
                loaded_shards: vec![],
            })
            .await
            .unwrap()
            .into_inner();
        assert!(hb.accepted);

        let de = client
            .deregister_node(DeregisterNodeRequest {
                node_id: "qn-1".into(),
                epoch: r.epoch,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(de.error, "");

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn shutdown_notification_drains() {
        let tmp = TempDir::new().unwrap();
        let config = minimal_config(&tmp);
        let port = free_port();
        let (shutdown, handle) = spawn_cp(config, port).await;

        // Verify reachable.
        let _ = client(port).await;

        shutdown.notify_waiters();
        // Drain should finish promptly once all streams close.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("CP did not drain within 5s")
            .ok();
    }

}
