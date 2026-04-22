from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MoveShardRequest(_message.Message):
    __slots__ = ("shard_name", "to_worker", "force")
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    TO_WORKER_FIELD_NUMBER: _ClassVar[int]
    FORCE_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    to_worker: str
    force: bool
    def __init__(self, shard_name: _Optional[str] = ..., to_worker: _Optional[str] = ..., force: bool = ...) -> None: ...

class MoveShardResponse(_message.Message):
    __slots__ = ("from_worker", "to_worker", "error")
    FROM_WORKER_FIELD_NUMBER: _ClassVar[int]
    TO_WORKER_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    from_worker: str
    to_worker: str
    error: str
    def __init__(self, from_worker: _Optional[str] = ..., to_worker: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class RebalanceRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RebalanceResponse(_message.Message):
    __slots__ = ("shards_moved", "error")
    SHARDS_MOVED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    shards_moved: int
    error: str
    def __init__(self, shards_moved: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class RegisterWorkerRequest(_message.Message):
    __slots__ = ("worker_id", "host", "port")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    host: str
    port: int
    def __init__(self, worker_id: _Optional[str] = ..., host: _Optional[str] = ..., port: _Optional[int] = ...) -> None: ...

class RegisterWorkerResponse(_message.Message):
    __slots__ = ("assigned_shards", "error")
    ASSIGNED_SHARDS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    assigned_shards: int
    error: str
    def __init__(self, assigned_shards: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class LocalCreateTagRequest(_message.Message):
    __slots__ = ("shard_name", "tag_name", "version")
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    TAG_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    tag_name: str
    version: int
    def __init__(self, shard_name: _Optional[str] = ..., tag_name: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class LocalListTagsRequest(_message.Message):
    __slots__ = ("shard_name",)
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    def __init__(self, shard_name: _Optional[str] = ...) -> None: ...

class LocalDeleteTagRequest(_message.Message):
    __slots__ = ("shard_name", "tag_name")
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    TAG_NAME_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    tag_name: str
    def __init__(self, shard_name: _Optional[str] = ..., tag_name: _Optional[str] = ...) -> None: ...

class LocalRestoreTableRequest(_message.Message):
    __slots__ = ("shard_name", "version", "tag")
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    version: int
    tag: str
    def __init__(self, shard_name: _Optional[str] = ..., version: _Optional[int] = ..., tag: _Optional[str] = ...) -> None: ...

class LocalAlterTableRequest(_message.Message):
    __slots__ = ("shard_name", "add_columns_arrow_ipc", "drop_columns", "rename_columns")
    class RenameColumnsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    ADD_COLUMNS_ARROW_IPC_FIELD_NUMBER: _ClassVar[int]
    DROP_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    RENAME_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    add_columns_arrow_ipc: bytes
    drop_columns: _containers.RepeatedScalarFieldContainer[str]
    rename_columns: _containers.ScalarMap[str, str]
    def __init__(self, shard_name: _Optional[str] = ..., add_columns_arrow_ipc: _Optional[bytes] = ..., drop_columns: _Optional[_Iterable[str]] = ..., rename_columns: _Optional[_Mapping[str, str]] = ...) -> None: ...

class LocalAlterTableResponse(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: str
    def __init__(self, error: _Optional[str] = ...) -> None: ...

class CreateLocalShardRequest(_message.Message):
    __slots__ = ("shard_name", "parent_uri", "arrow_ipc_data", "index_column", "index_num_partitions", "storage_options")
    class StorageOptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_URI_FIELD_NUMBER: _ClassVar[int]
    ARROW_IPC_DATA_FIELD_NUMBER: _ClassVar[int]
    INDEX_COLUMN_FIELD_NUMBER: _ClassVar[int]
    INDEX_NUM_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    STORAGE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    parent_uri: str
    arrow_ipc_data: bytes
    index_column: str
    index_num_partitions: int
    storage_options: _containers.ScalarMap[str, str]
    def __init__(self, shard_name: _Optional[str] = ..., parent_uri: _Optional[str] = ..., arrow_ipc_data: _Optional[bytes] = ..., index_column: _Optional[str] = ..., index_num_partitions: _Optional[int] = ..., storage_options: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateLocalShardResponse(_message.Message):
    __slots__ = ("num_rows", "uri", "error")
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    num_rows: int
    uri: str
    error: str
    def __init__(self, num_rows: _Optional[int] = ..., uri: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class CompactRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CompactResponse(_message.Message):
    __slots__ = ("tables_compacted", "error")
    TABLES_COMPACTED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tables_compacted: int
    error: str
    def __init__(self, tables_compacted: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class HealthCheckRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthCheckResponse(_message.Message):
    __slots__ = ("healthy", "loaded_shards", "total_rows", "shard_names", "server_version")
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    LOADED_SHARDS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
    SHARD_NAMES_FIELD_NUMBER: _ClassVar[int]
    SERVER_VERSION_FIELD_NUMBER: _ClassVar[int]
    healthy: bool
    loaded_shards: int
    total_rows: int
    shard_names: _containers.RepeatedScalarFieldContainer[str]
    server_version: str
    def __init__(self, healthy: bool = ..., loaded_shards: _Optional[int] = ..., total_rows: _Optional[int] = ..., shard_names: _Optional[_Iterable[str]] = ..., server_version: _Optional[str] = ...) -> None: ...

class ClusterStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ClusterStatusResponse(_message.Message):
    __slots__ = ("executors", "total_shards", "total_rows")
    EXECUTORS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SHARDS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
    executors: _containers.RepeatedCompositeFieldContainer[ExecutorStatus]
    total_shards: int
    total_rows: int
    def __init__(self, executors: _Optional[_Iterable[_Union[ExecutorStatus, _Mapping]]] = ..., total_shards: _Optional[int] = ..., total_rows: _Optional[int] = ...) -> None: ...

class ExecutorStatus(_message.Message):
    __slots__ = ("executor_id", "host", "port", "healthy", "loaded_shards", "last_health_check_ms")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    LOADED_SHARDS_FIELD_NUMBER: _ClassVar[int]
    LAST_HEALTH_CHECK_MS_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    host: str
    port: int
    healthy: bool
    loaded_shards: int
    last_health_check_ms: int
    def __init__(self, executor_id: _Optional[str] = ..., host: _Optional[str] = ..., port: _Optional[int] = ..., healthy: bool = ..., loaded_shards: _Optional[int] = ..., last_health_check_ms: _Optional[int] = ...) -> None: ...

class AnnSearchRequest(_message.Message):
    __slots__ = ("table_name", "vector_column", "query_vector", "dimension", "k", "nprobes", "filter", "columns", "metric_type", "query_text", "offset", "min_schema_version", "min_commit_seq")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTOR_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    NPROBES_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    METRIC_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUERY_TEXT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    MIN_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    MIN_COMMIT_SEQ_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    vector_column: str
    query_vector: bytes
    dimension: int
    k: int
    nprobes: int
    filter: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    metric_type: int
    query_text: str
    offset: int
    min_schema_version: int
    min_commit_seq: int
    def __init__(self, table_name: _Optional[str] = ..., vector_column: _Optional[str] = ..., query_vector: _Optional[bytes] = ..., dimension: _Optional[int] = ..., k: _Optional[int] = ..., nprobes: _Optional[int] = ..., filter: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., metric_type: _Optional[int] = ..., query_text: _Optional[str] = ..., offset: _Optional[int] = ..., min_schema_version: _Optional[int] = ..., min_commit_seq: _Optional[int] = ...) -> None: ...

class FtsSearchRequest(_message.Message):
    __slots__ = ("table_name", "text_column", "query_text", "k", "filter", "columns", "offset", "min_schema_version", "min_commit_seq")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TEXT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUERY_TEXT_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    MIN_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    MIN_COMMIT_SEQ_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    text_column: str
    query_text: str
    k: int
    filter: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    offset: int
    min_schema_version: int
    min_commit_seq: int
    def __init__(self, table_name: _Optional[str] = ..., text_column: _Optional[str] = ..., query_text: _Optional[str] = ..., k: _Optional[int] = ..., filter: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., offset: _Optional[int] = ..., min_schema_version: _Optional[int] = ..., min_commit_seq: _Optional[int] = ...) -> None: ...

class HybridSearchRequest(_message.Message):
    __slots__ = ("table_name", "vector_column", "query_vector", "dimension", "nprobes", "text_column", "query_text", "k", "filter", "columns", "metric_type", "offset", "min_schema_version", "min_commit_seq")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTOR_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    NPROBES_FIELD_NUMBER: _ClassVar[int]
    TEXT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUERY_TEXT_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    METRIC_TYPE_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    MIN_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    MIN_COMMIT_SEQ_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    vector_column: str
    query_vector: bytes
    dimension: int
    nprobes: int
    text_column: str
    query_text: str
    k: int
    filter: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    metric_type: int
    offset: int
    min_schema_version: int
    min_commit_seq: int
    def __init__(self, table_name: _Optional[str] = ..., vector_column: _Optional[str] = ..., query_vector: _Optional[bytes] = ..., dimension: _Optional[int] = ..., nprobes: _Optional[int] = ..., text_column: _Optional[str] = ..., query_text: _Optional[str] = ..., k: _Optional[int] = ..., filter: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., metric_type: _Optional[int] = ..., offset: _Optional[int] = ..., min_schema_version: _Optional[int] = ..., min_commit_seq: _Optional[int] = ...) -> None: ...

class GetByIdsRequest(_message.Message):
    __slots__ = ("table_name", "ids", "id_column", "columns")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    ID_COLUMN_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    ids: _containers.RepeatedScalarFieldContainer[int]
    id_column: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, table_name: _Optional[str] = ..., ids: _Optional[_Iterable[int]] = ..., id_column: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ...) -> None: ...

class QueryRequest(_message.Message):
    __slots__ = ("table_name", "filter", "limit", "offset", "columns")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    filter: str
    limit: int
    offset: int
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, table_name: _Optional[str] = ..., filter: _Optional[str] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ..., columns: _Optional[_Iterable[str]] = ...) -> None: ...

class BatchSearchRequest(_message.Message):
    __slots__ = ("table_name", "query_vectors", "vector_column", "dimension", "k", "nprobes", "filter", "columns", "metric_type")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTORS_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COLUMN_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    NPROBES_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    METRIC_TYPE_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    query_vectors: _containers.RepeatedScalarFieldContainer[bytes]
    vector_column: str
    dimension: int
    k: int
    nprobes: int
    filter: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    metric_type: int
    def __init__(self, table_name: _Optional[str] = ..., query_vectors: _Optional[_Iterable[bytes]] = ..., vector_column: _Optional[str] = ..., dimension: _Optional[int] = ..., k: _Optional[int] = ..., nprobes: _Optional[int] = ..., filter: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., metric_type: _Optional[int] = ...) -> None: ...

class BatchSearchResponse(_message.Message):
    __slots__ = ("results", "error")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SearchResponse]
    error: str
    def __init__(self, results: _Optional[_Iterable[_Union[SearchResponse, _Mapping]]] = ..., error: _Optional[str] = ...) -> None: ...

class SearchResponse(_message.Message):
    __slots__ = ("arrow_ipc_data", "num_rows", "error", "truncated", "next_offset", "latency_ms")
    ARROW_IPC_DATA_FIELD_NUMBER: _ClassVar[int]
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    NEXT_OFFSET_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    arrow_ipc_data: bytes
    num_rows: int
    error: str
    truncated: bool
    next_offset: int
    latency_ms: int
    def __init__(self, arrow_ipc_data: _Optional[bytes] = ..., num_rows: _Optional[int] = ..., error: _Optional[str] = ..., truncated: bool = ..., next_offset: _Optional[int] = ..., latency_ms: _Optional[int] = ...) -> None: ...

class LocalSearchRequest(_message.Message):
    __slots__ = ("query_type", "table_name", "vector_column", "query_vector", "dimension", "nprobes", "metric_type", "text_column", "query_text", "k", "filter", "columns")
    QUERY_TYPE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTOR_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    NPROBES_FIELD_NUMBER: _ClassVar[int]
    METRIC_TYPE_FIELD_NUMBER: _ClassVar[int]
    TEXT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    QUERY_TEXT_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    query_type: int
    table_name: str
    vector_column: str
    query_vector: bytes
    dimension: int
    nprobes: int
    metric_type: int
    text_column: str
    query_text: str
    k: int
    filter: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, query_type: _Optional[int] = ..., table_name: _Optional[str] = ..., vector_column: _Optional[str] = ..., query_vector: _Optional[bytes] = ..., dimension: _Optional[int] = ..., nprobes: _Optional[int] = ..., metric_type: _Optional[int] = ..., text_column: _Optional[str] = ..., query_text: _Optional[str] = ..., k: _Optional[int] = ..., filter: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ...) -> None: ...

class LocalSearchResponse(_message.Message):
    __slots__ = ("arrow_ipc_data", "num_rows", "error")
    ARROW_IPC_DATA_FIELD_NUMBER: _ClassVar[int]
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    arrow_ipc_data: bytes
    num_rows: int
    error: str
    def __init__(self, arrow_ipc_data: _Optional[bytes] = ..., num_rows: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class AddRowsRequest(_message.Message):
    __slots__ = ("table_name", "arrow_ipc_data", "on_columns", "request_id")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_IPC_DATA_FIELD_NUMBER: _ClassVar[int]
    ON_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    arrow_ipc_data: bytes
    on_columns: _containers.RepeatedScalarFieldContainer[str]
    request_id: str
    def __init__(self, table_name: _Optional[str] = ..., arrow_ipc_data: _Optional[bytes] = ..., on_columns: _Optional[_Iterable[str]] = ..., request_id: _Optional[str] = ...) -> None: ...

class DeleteRowsRequest(_message.Message):
    __slots__ = ("table_name", "filter")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    filter: str
    def __init__(self, table_name: _Optional[str] = ..., filter: _Optional[str] = ...) -> None: ...

class UpsertRowsRequest(_message.Message):
    __slots__ = ("table_name", "arrow_ipc_data", "on_columns", "request_id")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_IPC_DATA_FIELD_NUMBER: _ClassVar[int]
    ON_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    arrow_ipc_data: bytes
    on_columns: _containers.RepeatedScalarFieldContainer[str]
    request_id: str
    def __init__(self, table_name: _Optional[str] = ..., arrow_ipc_data: _Optional[bytes] = ..., on_columns: _Optional[_Iterable[str]] = ..., request_id: _Optional[str] = ...) -> None: ...

class WriteResponse(_message.Message):
    __slots__ = ("affected_rows", "new_version", "error", "commit_seq")
    AFFECTED_ROWS_FIELD_NUMBER: _ClassVar[int]
    NEW_VERSION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    COMMIT_SEQ_FIELD_NUMBER: _ClassVar[int]
    affected_rows: int
    new_version: int
    error: str
    commit_seq: int
    def __init__(self, affected_rows: _Optional[int] = ..., new_version: _Optional[int] = ..., error: _Optional[str] = ..., commit_seq: _Optional[int] = ...) -> None: ...

class LocalWriteRequest(_message.Message):
    __slots__ = ("write_type", "table_name", "arrow_ipc_data", "filter", "on_columns", "target_shard")
    WRITE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_IPC_DATA_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    ON_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TARGET_SHARD_FIELD_NUMBER: _ClassVar[int]
    write_type: int
    table_name: str
    arrow_ipc_data: bytes
    filter: str
    on_columns: _containers.RepeatedScalarFieldContainer[str]
    target_shard: str
    def __init__(self, write_type: _Optional[int] = ..., table_name: _Optional[str] = ..., arrow_ipc_data: _Optional[bytes] = ..., filter: _Optional[str] = ..., on_columns: _Optional[_Iterable[str]] = ..., target_shard: _Optional[str] = ...) -> None: ...

class LocalWriteResponse(_message.Message):
    __slots__ = ("affected_rows", "new_version", "error")
    AFFECTED_ROWS_FIELD_NUMBER: _ClassVar[int]
    NEW_VERSION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    affected_rows: int
    new_version: int
    error: str
    def __init__(self, affected_rows: _Optional[int] = ..., new_version: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class LoadShardRequest(_message.Message):
    __slots__ = ("shard_name", "uri", "storage_options")
    class StorageOptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    STORAGE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    uri: str
    storage_options: _containers.ScalarMap[str, str]
    def __init__(self, shard_name: _Optional[str] = ..., uri: _Optional[str] = ..., storage_options: _Optional[_Mapping[str, str]] = ...) -> None: ...

class LoadShardResponse(_message.Message):
    __slots__ = ("num_rows", "error")
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    num_rows: int
    error: str
    def __init__(self, num_rows: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class UnloadShardRequest(_message.Message):
    __slots__ = ("shard_name",)
    SHARD_NAME_FIELD_NUMBER: _ClassVar[int]
    shard_name: str
    def __init__(self, shard_name: _Optional[str] = ...) -> None: ...

class UnloadShardResponse(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: str
    def __init__(self, error: _Optional[str] = ...) -> None: ...

class OpenTableRequest(_message.Message):
    __slots__ = ("table_name", "uri", "storage_options")
    class StorageOptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    STORAGE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    uri: str
    storage_options: _containers.ScalarMap[str, str]
    def __init__(self, table_name: _Optional[str] = ..., uri: _Optional[str] = ..., storage_options: _Optional[_Mapping[str, str]] = ...) -> None: ...

class OpenTableResponse(_message.Message):
    __slots__ = ("num_rows", "error")
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    num_rows: int
    error: str
    def __init__(self, num_rows: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class CloseTableRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...

class CloseTableResponse(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: str
    def __init__(self, error: _Optional[str] = ...) -> None: ...

class GetTableInfoRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...

class GetTableInfoResponse(_message.Message):
    __slots__ = ("num_rows", "columns", "error")
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    num_rows: int
    columns: _containers.RepeatedCompositeFieldContainer[ColumnInfo]
    error: str
    def __init__(self, num_rows: _Optional[int] = ..., columns: _Optional[_Iterable[_Union[ColumnInfo, _Mapping]]] = ..., error: _Optional[str] = ...) -> None: ...

class CreateTableRequest(_message.Message):
    __slots__ = ("table_name", "arrow_ipc_data", "uri", "index_column", "index_num_partitions", "on_columns")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_IPC_DATA_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    INDEX_COLUMN_FIELD_NUMBER: _ClassVar[int]
    INDEX_NUM_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    ON_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    arrow_ipc_data: bytes
    uri: str
    index_column: str
    index_num_partitions: int
    on_columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, table_name: _Optional[str] = ..., arrow_ipc_data: _Optional[bytes] = ..., uri: _Optional[str] = ..., index_column: _Optional[str] = ..., index_num_partitions: _Optional[int] = ..., on_columns: _Optional[_Iterable[str]] = ...) -> None: ...

class CreateTableResponse(_message.Message):
    __slots__ = ("table_name", "num_rows", "error")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    num_rows: int
    error: str
    def __init__(self, table_name: _Optional[str] = ..., num_rows: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class AlterTableRequest(_message.Message):
    __slots__ = ("table_name", "add_columns_arrow_ipc", "expected_schema_version", "drop_columns", "rename_columns")
    class RenameColumnsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ADD_COLUMNS_ARROW_IPC_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    DROP_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    RENAME_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    add_columns_arrow_ipc: bytes
    expected_schema_version: int
    drop_columns: _containers.RepeatedScalarFieldContainer[str]
    rename_columns: _containers.ScalarMap[str, str]
    def __init__(self, table_name: _Optional[str] = ..., add_columns_arrow_ipc: _Optional[bytes] = ..., expected_schema_version: _Optional[int] = ..., drop_columns: _Optional[_Iterable[str]] = ..., rename_columns: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateTagRequest(_message.Message):
    __slots__ = ("table_name", "tag_name", "version")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TAG_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    tag_name: str
    version: int
    def __init__(self, table_name: _Optional[str] = ..., tag_name: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class CreateTagResponse(_message.Message):
    __slots__ = ("tagged_version", "error")
    TAGGED_VERSION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tagged_version: int
    error: str
    def __init__(self, tagged_version: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class ListTagsRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...

class TagInfo(_message.Message):
    __slots__ = ("name", "version")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: int
    def __init__(self, name: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class ListTagsResponse(_message.Message):
    __slots__ = ("tags", "error")
    TAGS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tags: _containers.RepeatedCompositeFieldContainer[TagInfo]
    error: str
    def __init__(self, tags: _Optional[_Iterable[_Union[TagInfo, _Mapping]]] = ..., error: _Optional[str] = ...) -> None: ...

class DeleteTagRequest(_message.Message):
    __slots__ = ("table_name", "tag_name")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TAG_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    tag_name: str
    def __init__(self, table_name: _Optional[str] = ..., tag_name: _Optional[str] = ...) -> None: ...

class DeleteTagResponse(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: str
    def __init__(self, error: _Optional[str] = ...) -> None: ...

class RestoreTableRequest(_message.Message):
    __slots__ = ("table_name", "version", "tag")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    version: int
    tag: str
    def __init__(self, table_name: _Optional[str] = ..., version: _Optional[int] = ..., tag: _Optional[str] = ...) -> None: ...

class RestoreTableResponse(_message.Message):
    __slots__ = ("new_version", "error")
    NEW_VERSION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    new_version: int
    error: str
    def __init__(self, new_version: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class AlterTableResponse(_message.Message):
    __slots__ = ("new_schema_version", "error")
    NEW_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    new_schema_version: int
    error: str
    def __init__(self, new_schema_version: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...

class ListTablesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListTablesResponse(_message.Message):
    __slots__ = ("table_names", "error")
    TABLE_NAMES_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    table_names: _containers.RepeatedScalarFieldContainer[str]
    error: str
    def __init__(self, table_names: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ...) -> None: ...

class DropTableRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...

class DropTableResponse(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: str
    def __init__(self, error: _Optional[str] = ...) -> None: ...

class CreateIndexRequest(_message.Message):
    __slots__ = ("table_name", "column", "index_type", "num_partitions")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    INDEX_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUM_PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    column: str
    index_type: str
    num_partitions: int
    def __init__(self, table_name: _Optional[str] = ..., column: _Optional[str] = ..., index_type: _Optional[str] = ..., num_partitions: _Optional[int] = ...) -> None: ...

class CreateIndexResponse(_message.Message):
    __slots__ = ("error",)
    ERROR_FIELD_NUMBER: _ClassVar[int]
    error: str
    def __init__(self, error: _Optional[str] = ...) -> None: ...

class GetSchemaRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...

class GetSchemaResponse(_message.Message):
    __slots__ = ("columns", "error")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[ColumnInfo]
    error: str
    def __init__(self, columns: _Optional[_Iterable[_Union[ColumnInfo, _Mapping]]] = ..., error: _Optional[str] = ...) -> None: ...

class ColumnInfo(_message.Message):
    __slots__ = ("name", "data_type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    data_type: str
    def __init__(self, name: _Optional[str] = ..., data_type: _Optional[str] = ...) -> None: ...

class CountRowsRequest(_message.Message):
    __slots__ = ("table_name", "filter", "min_schema_version", "min_commit_seq")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    MIN_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    MIN_COMMIT_SEQ_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    filter: str
    min_schema_version: int
    min_commit_seq: int
    def __init__(self, table_name: _Optional[str] = ..., filter: _Optional[str] = ..., min_schema_version: _Optional[int] = ..., min_commit_seq: _Optional[int] = ...) -> None: ...

class CountRowsResponse(_message.Message):
    __slots__ = ("count", "error")
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    count: int
    error: str
    def __init__(self, count: _Optional[int] = ..., error: _Optional[str] = ...) -> None: ...
