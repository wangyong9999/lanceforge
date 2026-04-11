// Licensed under the Apache License, Version 2.0.
// Protobuf message types for Lance plan serialization.
//
// For MVP, this is minimal — the Scatter-Gather fast path sends
// query descriptors (not serialized plans) to Executors.
// Full plan serialization will be added when SQL path needs it.

use prost::Message;

/// Wrapper for Lance-specific ExecutionPlan nodes.
/// Used by LanceCodec for plan serialization over Ballista's gRPC transport.
#[derive(Clone, PartialEq, Message)]
pub struct LancePlanNode {
    /// Plan type discriminator
    #[prost(int32, tag = "1")]
    pub plan_type: i32,

    /// Serialized plan-specific payload
    #[prost(bytes = "vec", tag = "2")]
    pub payload: Vec<u8>,
}

/// Query descriptor sent via the Scatter-Gather fast path.
/// This is NOT a serialized DataFusion plan — it's a lightweight
/// description that the Executor uses to build a local plan.
#[derive(Clone, PartialEq, Message)]
pub struct LanceQueryDescriptor {
    /// Table name (as registered in Executor's SessionContext)
    #[prost(string, tag = "1")]
    pub table_name: String,

    /// Query type
    #[prost(enumeration = "LanceQueryType", tag = "2")]
    pub query_type: i32,

    /// Vector query parameters (for ANN)
    #[prost(message, optional, tag = "3")]
    pub vector_query: Option<VectorQueryParams>,

    /// Full-text search parameters (for FTS)
    #[prost(message, optional, tag = "4")]
    pub fts_query: Option<FtsQueryParams>,

    /// SQL filter expression (WHERE clause)
    #[prost(string, optional, tag = "5")]
    pub filter: Option<String>,

    /// Result limit (top-K)
    #[prost(uint32, tag = "6")]
    pub k: u32,

    /// Columns to return
    #[prost(string, repeated, tag = "7")]
    pub columns: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct VectorQueryParams {
    /// Column name containing vectors
    #[prost(string, tag = "1")]
    pub column: String,

    /// Query vector (serialized as f32 array bytes, little-endian)
    #[prost(bytes = "vec", tag = "2")]
    pub vector_data: Vec<u8>,

    /// Number of dimensions
    #[prost(uint32, tag = "3")]
    pub dimension: u32,

    /// Number of IVF probes
    #[prost(uint32, tag = "4")]
    pub nprobes: u32,

    /// Distance metric type (L2=0, Cosine=1, Dot=2)
    #[prost(int32, tag = "5")]
    pub metric_type: i32,

    /// Oversampling factor for distributed recall compensation
    #[prost(uint32, tag = "6")]
    pub oversample_factor: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct FtsQueryParams {
    /// Search text
    #[prost(string, tag = "1")]
    pub query_text: String,

    /// Column name containing text
    #[prost(string, tag = "2")]
    pub column: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum LanceQueryType {
    Ann = 0,
    Fts = 1,
    Hybrid = 2,
}

/// Result from a single Executor's local search
#[derive(Clone, PartialEq, Message)]
pub struct LocalSearchResult {
    /// Executor ID that produced this result
    #[prost(string, tag = "1")]
    pub executor_id: String,

    /// Arrow IPC encoded RecordBatch (row_id, distance/score, columns...)
    #[prost(bytes = "vec", tag = "2")]
    pub arrow_ipc_data: Vec<u8>,

    /// Number of rows in the result
    #[prost(uint32, tag = "3")]
    pub num_rows: u32,
}
