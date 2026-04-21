// Licensed under the Apache License, Version 2.0.
// Lance Distributed Proto — gRPC service definitions and shared message types.

pub mod generated {
    pub mod lance_distributed {
        include!("generated/lance.distributed.rs");
    }
}

pub mod descriptor;

// Re-export gRPC types at crate root.
pub use generated::lance_distributed::*;

#[cfg(test)]
mod cluster_control_smoke {
    //! Wire-format smoke tests for the Phase C ClusterControl /
    //! NodeLifecycle messages. Not exhaustive — the goal is to catch
    //! accidental proto field-number reshuffles or type changes that
    //! would silently break compatibility across rolling upgrades.

    use super::*;
    use prost::Message;

    #[test]
    fn table_routing_roundtrip() {
        let msg = TableRouting {
            table_name: "orders".into(),
            shards: vec![
                ShardAssignment {
                    shard_name: "s0".into(),
                    primary_executor: "w0".into(),
                    secondary_executor: "w1".into(),
                    uri: "s3://bucket/t/s0.lance".into(),
                },
                ShardAssignment {
                    shard_name: "s1".into(),
                    primary_executor: "w1".into(),
                    secondary_executor: String::new(),
                    uri: "s3://bucket/t/s1.lance".into(),
                },
            ],
            schema_version: 3,
            routing_version: 7,
        };
        let bytes = msg.encode_to_vec();
        let back = TableRouting::decode(bytes.as_slice()).unwrap();
        assert_eq!(back.table_name, "orders");
        assert_eq!(back.shards.len(), 2);
        assert_eq!(back.shards[0].primary_executor, "w0");
        assert_eq!(back.schema_version, 3);
        assert_eq!(back.routing_version, 7);
    }

    #[test]
    fn routing_update_kind_variants_encode_distinctly() {
        use routing_update::Kind;
        let changed = RoutingUpdate {
            kind: Kind::TableChanged as i32,
            table_name: "t".into(),
            routing: Some(TableRouting::default()),
            healthy_executors: vec![],
        };
        let dropped = RoutingUpdate {
            kind: Kind::TableDropped as i32,
            table_name: "t".into(),
            routing: None,
            healthy_executors: vec![],
        };
        let healthy = RoutingUpdate {
            kind: Kind::HealthySetChanged as i32,
            table_name: String::new(),
            routing: None,
            healthy_executors: vec![ExecutorHealth {
                executor_id: "w0".into(),
                host: "10.0.0.1".into(),
                port: 50100,
                healthy: true,
            }],
        };
        assert_ne!(changed.encode_to_vec(), dropped.encode_to_vec());
        assert_ne!(changed.encode_to_vec(), healthy.encode_to_vec());
        let back = RoutingUpdate::decode(healthy.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back.healthy_executors.len(), 1);
        assert_eq!(back.healthy_executors[0].port, 50100);
    }

    #[test]
    fn resolved_schema_preserves_ipc_bytes() {
        let ipc = vec![0xde, 0xad, 0xbe, 0xef, 0x42, 0x00, 0xff];
        let msg = ResolvedSchema {
            table_name: "t".into(),
            schema_version: 42,
            arrow_ipc_schema: ipc.clone(),
        };
        let back = ResolvedSchema::decode(msg.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back.schema_version, 42);
        assert_eq!(back.arrow_ipc_schema, ipc);
    }

    #[test]
    fn policy_bundle_empty_role_is_unknown_key() {
        // CP convention: empty role string signals "no such key". The
        // resolver shouldn't return an error — the QN policy check
        // translates empty-role into Unauthenticated itself.
        let msg = PolicyBundle {
            role: String::new(),
            qps_limit: 0,
            namespace_prefix: String::new(),
        };
        let back = PolicyBundle::decode(msg.encode_to_vec().as_slice()).unwrap();
        assert!(back.role.is_empty());
        assert_eq!(back.qps_limit, 0);
        assert!(back.namespace_prefix.is_empty());
    }

    #[test]
    fn heartbeat_epoch_distinguishes_instances() {
        let hb1 = HeartbeatRequest {
            node_id: "w0".into(),
            epoch: 1,
            loaded_shards: vec!["s0".into(), "s1".into()],
        };
        let hb2 = HeartbeatRequest {
            node_id: "w0".into(),
            epoch: 2,
            loaded_shards: vec!["s0".into(), "s1".into()],
        };
        assert_ne!(hb1.encode_to_vec(), hb2.encode_to_vec());
    }

    #[test]
    fn node_role_numeric_stability() {
        // Wire-format numbers must match the proto definition so
        // cross-version rolling upgrades don't misinterpret the role.
        assert_eq!(NodeRole::Unspecified as i32, 0);
        assert_eq!(NodeRole::Qn as i32, 1);
        assert_eq!(NodeRole::Pe as i32, 2);
        assert_eq!(NodeRole::Idx as i32, 3);
        assert_eq!(NodeRole::Monolith as i32, 4);
    }
}
