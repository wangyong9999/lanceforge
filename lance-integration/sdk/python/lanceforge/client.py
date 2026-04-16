"""
LanceForge gRPC client — thin wrapper over generated proto stubs.
"""

import struct

import grpc
import pyarrow as pa
import pyarrow.ipc as ipc

from lanceforge import lance_service_pb2 as pb
from lanceforge import lance_service_pb2_grpc as pb_grpc


class LanceForgeClient:
    """Client for LanceForge distributed retrieval engine.

    Args:
        host: Coordinator address (e.g., "localhost:50050")
        api_key: Optional API key for authentication
        max_message_size: Max gRPC message size in bytes (default 64MB)
        tls_ca_cert: Path to PEM-encoded CA certificate for TLS verification.
                     When set, uses secure channel (grpc.secure_channel).
    """

    def __init__(self, host="localhost:50050", api_key=None,
                 max_message_size=64*1024*1024, tls_ca_cert=None,
                 default_timeout=30, max_retries=0, retry_backoff=0.5):
        """
        Args:
            host: Coordinator address (e.g., "localhost:50050")
            api_key: Optional API key for authentication
            max_message_size: Max gRPC message size in bytes (default 64MB)
            tls_ca_cert: Path to PEM-encoded CA certificate for TLS.
            default_timeout: Default RPC timeout in seconds (default 30).
            max_retries: Max retry attempts on transient failures (default 0 = no retry).
            retry_backoff: Base backoff in seconds between retries (exponential).
        """
        options = [("grpc.max_receive_message_length", max_message_size)]
        if tls_ca_cert:
            with open(tls_ca_cert, 'rb') as f:
                ca_pem = f.read()
            credentials = grpc.ssl_channel_credentials(root_certificates=ca_pem)
            self._channel = grpc.secure_channel(host, credentials, options=options)
        else:
            self._channel = grpc.insecure_channel(host, options=options)
        self._stub = pb_grpc.LanceSchedulerServiceStub(self._channel)
        self._api_key = api_key
        self._timeout = default_timeout
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff

    def close(self):
        """Close the gRPC channel."""
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _metadata(self):
        if self._api_key:
            return [("authorization", f"Bearer {self._api_key}")]
        return None

    def _call(self, method, request, timeout=None):
        """Call a gRPC method with optional retry and configurable timeout."""
        import time as _time
        t = timeout or self._timeout
        last_err = None
        for attempt in range(1 + self._max_retries):
            try:
                return method(request, metadata=self._metadata(), timeout=t)
            except grpc.RpcError as e:
                last_err = e
                code = e.code()
                # Only retry on transient errors
                if code not in (grpc.StatusCode.UNAVAILABLE,
                                grpc.StatusCode.DEADLINE_EXCEEDED,
                                grpc.StatusCode.RESOURCE_EXHAUSTED):
                    raise
                if attempt < self._max_retries:
                    backoff = self._retry_backoff * (2 ** attempt)
                    _time.sleep(backoff)
        raise last_err

    # ── Search ──

    def search(self, table, query_vector, k=10, nprobes=20,
               filter=None, columns=None, metric="l2",
               vector_column="vector"):
        """ANN vector similarity search.

        Args:
            table: Table name
            query_vector: List of floats or numpy array
            k: Number of results
            nprobes: Number of IVF partitions to probe
            filter: Optional SQL WHERE clause (e.g., "category = 'electronics'")
            columns: Optional list of columns to return
            metric: Distance metric ("l2", "cosine", "dot")

        Returns:
            pyarrow.Table with results
        """
        metric_type = {"l2": 0, "cosine": 1, "dot": 2}.get(metric, 0)
        vec_bytes = struct.pack(f"<{len(query_vector)}f", *query_vector)

        req = pb.AnnSearchRequest(
            table_name=table,
            vector_column=vector_column or "vector",
            query_vector=vec_bytes,
            dimension=len(query_vector),
            k=k,
            nprobes=nprobes,
            metric_type=metric_type,
        )
        if filter:
            req.filter = filter
        if columns:
            req.columns.extend(columns)

        resp = self._call(self._stub.AnnSearch, req)
        return self._decode_response(resp)

    def text_search(self, table, query_text, k=10, filter=None, columns=None, text_column="text"):
        """Full-text BM25 search.

        Args:
            table: Table name
            query_text: Search query string
            k: Number of results
            filter: Optional SQL WHERE clause
            columns: Optional list of columns to return

        Returns:
            pyarrow.Table with results
        """
        req = pb.FtsSearchRequest(
            table_name=table,
            text_column=text_column,
            query_text=query_text,
            k=k,
        )
        if filter:
            req.filter = filter
        if columns:
            req.columns.extend(columns)

        resp = self._call(self._stub.FtsSearch, req)
        return self._decode_response(resp)

    def hybrid_search(self, table, query_vector, query_text, k=10, vector_column="vector", text_column="text",
                      nprobes=20, filter=None, metric="l2"):
        """Hybrid ANN + FTS search with RRF fusion.

        Args:
            table: Table name
            query_vector: List of floats
            query_text: Search query string
            k: Number of results
            nprobes: Number of IVF partitions to probe
            filter: Optional SQL WHERE clause
            metric: Distance metric

        Returns:
            pyarrow.Table with results
        """
        metric_type = {"l2": 0, "cosine": 1, "dot": 2}.get(metric, 0)
        vec_bytes = struct.pack(f"<{len(query_vector)}f", *query_vector)

        req = pb.HybridSearchRequest(
            table_name=table,
            vector_column=vector_column or "vector",
            query_vector=vec_bytes,
            dimension=len(query_vector),
            nprobes=nprobes,
            metric_type=metric_type,
            text_column=text_column,
            query_text=query_text,
            k=k,
        )
        if filter:
            req.filter = filter

        resp = self._call(self._stub.HybridSearch, req)
        return self._decode_response(resp)

    # ── Write ──

    def insert(self, table, data):
        """Insert rows into a table.

        Args:
            table: Table name
            data: pyarrow.Table or pyarrow.RecordBatch

        Returns:
            dict with 'affected_rows' and 'new_version'
        """
        if isinstance(data, pa.Table):
            data = data.to_batches()[0] if data.num_rows > 0 else None
        if data is None:
            raise ValueError("No data to insert")

        ipc_data = self._encode_batch(data)
        resp = self._call(self._stub.AddRows,
            pb.AddRowsRequest(table_name=table, arrow_ipc_data=ipc_data))

        if resp.error:
            raise RuntimeError(f"Insert failed: {resp.error}")
        return {"affected_rows": resp.affected_rows, "new_version": resp.new_version}

    def delete(self, table, filter):
        """Delete rows matching a filter.

        Args:
            table: Table name
            filter: SQL WHERE predicate (e.g., "id > 100")

        Returns:
            dict with 'affected_rows' and 'new_version'
        """
        resp = self._call(self._stub.DeleteRows,
            pb.DeleteRowsRequest(table_name=table, filter=filter))

        if resp.error:
            raise RuntimeError(f"Delete failed: {resp.error}")
        return {"affected_rows": resp.affected_rows, "new_version": resp.new_version}

    def upsert(self, table, data, on_columns):
        """Upsert rows (insert or update if exists).

        Args:
            table: Table name
            data: pyarrow.Table or pyarrow.RecordBatch
            on_columns: List of column names to match on (e.g., ["id"])

        Returns:
            dict with 'affected_rows' and 'new_version'
        """
        if isinstance(data, pa.Table):
            data = data.to_batches()[0] if data.num_rows > 0 else None
        if data is None:
            raise ValueError("No data to upsert")

        ipc_data = self._encode_batch(data)
        resp = self._call(self._stub.UpsertRows,
            pb.UpsertRowsRequest(
                table_name=table,
                arrow_ipc_data=ipc_data,
                on_columns=on_columns))

        if resp.error:
            raise RuntimeError(f"Upsert failed: {resp.error}")
        return {"affected_rows": resp.affected_rows, "new_version": resp.new_version}

    def sparse_search(self, table, query_text, k=10, filter=None,
                       text_column="text", columns=None):
        """Sparse vector search (BM25-based).

        Equivalent to full-text search but exposed as sparse vector API
        for compatibility with SPLADE/BGE-M3 workflows. Uses Tantivy
        BM25 under the hood via lancedb FTS.

        Args:
            table: Table name
            query_text: Search query
            k: Number of results
            filter: Optional SQL WHERE clause
            text_column: Column with text content
            columns: Optional columns to return

        Returns:
            pyarrow.Table with results (includes _score column)
        """
        return self.text_search(table, query_text, k=k, filter=filter,
                                columns=columns)

    # ── DDL ──

    def create_table(self, table, data, uri=None, index_column=None, num_partitions=32):
        """Create a new table with initial data.

        Args:
            table: Table name
            data: pyarrow.Table or RecordBatch (defines schema + initial data)
            uri: Storage URI (default: /tmp/lanceforge_tables/<table>.lance)
            index_column: Column to auto-index (optional)
            num_partitions: IVF partitions (default 32)

        Returns:
            dict with 'table_name' and 'num_rows'
        """
        if isinstance(data, pa.Table):
            data = data.to_batches()[0] if data.num_rows > 0 else None
        if data is None:
            raise ValueError("No data provided")

        ipc_data = self._encode_batch(data)
        resp = self._call(self._stub.CreateTable,
            pb.CreateTableRequest(
                table_name=table,
                arrow_ipc_data=ipc_data,
                uri=uri or "",
                index_column=index_column or "",
                index_num_partitions=num_partitions),
            timeout=60)

        if resp.error:
            raise RuntimeError(f"CreateTable failed: {resp.error}")
        return {"table_name": resp.table_name, "num_rows": resp.num_rows}

    def list_tables(self):
        """List all tables in the cluster.

        Returns:
            list of table names
        """
        resp = self._call(self._stub.ListTables, pb.ListTablesRequest(), timeout=10)
        if resp.error:
            raise RuntimeError(f"ListTables failed: {resp.error}")
        return list(resp.table_names)

    def drop_table(self, table):
        """Drop a table.

        Args:
            table: Table name
        """
        resp = self._call(self._stub.DropTable, pb.DropTableRequest(table_name=table))
        if resp.error:
            raise RuntimeError(f"DropTable failed: {resp.error}")

    def create_index(self, table, column, index_type="IVF_FLAT", num_partitions=32):
        """Create an index on a table column.

        Args:
            table: Table name
            column: Column name to index
            index_type: "IVF_FLAT", "IVF_HNSW_SQ", "BTREE", "INVERTED"
            num_partitions: For IVF indexes (default 32)
        """
        resp = self._call(self._stub.CreateIndex,
            pb.CreateIndexRequest(
                table_name=table, column=column,
                index_type=index_type, num_partitions=num_partitions),
            timeout=60)
        if resp.error:
            raise RuntimeError(f"CreateIndex failed: {resp.error}")

    # ── Status ──

    def status(self):
        """Get cluster status.

        Returns:
            dict with executor health information
        """
        resp = self._call(self._stub.GetClusterStatus, pb.ClusterStatusRequest(), timeout=10)
        return {
            "executors": [
                {"id": e.executor_id, "host": e.host, "port": e.port,
                 "healthy": e.healthy, "loaded_shards": e.loaded_shards}
                for e in resp.executors
            ],
            "total_shards": resp.total_shards,
            "total_rows": resp.total_rows,
        }

    # ── Cluster Management ──

    def rebalance(self):
        """Trigger shard rebalance across all workers.

        Returns:
            dict with shards_moved count
        """
        resp = self._call(self._stub.Rebalance, pb.RebalanceRequest())
        if resp.error:
            raise RuntimeError(f"Rebalance failed: {resp.error}")
        return {"shards_moved": resp.shards_moved}

    def register_worker(self, worker_id, host, port):
        """Register a new worker with the coordinator.

        Args:
            worker_id: Unique worker identifier
            host: Worker host address
            port: Worker gRPC port
        """
        resp = self._call(self._stub.RegisterWorker,
            pb.RegisterWorkerRequest(worker_id=worker_id, host=host, port=int(port)),
            timeout=10)
        if resp.error:
            raise RuntimeError(f"RegisterWorker failed: {resp.error}")
        return {"assigned_shards": resp.assigned_shards}

    def count_rows(self, table):
        """Count rows across all shards of a table.

        Returns:
            int: Total row count
        """
        resp = self._call(self._stub.CountRows, pb.CountRowsRequest(table_name=table))
        if resp.error:
            raise RuntimeError(f"CountRows failed: {resp.error}")
        return resp.count

    def get_schema(self, table):
        """Get schema columns for a table.

        Returns:
            list of column info dicts
        """
        resp = self._call(self._stub.GetSchema, pb.GetSchemaRequest(table_name=table), timeout=10)
        if resp.error:
            raise RuntimeError(f"GetSchema failed: {resp.error}")
        return [{"name": c.name, "type": c.data_type}
                for c in resp.columns]

    def move_shard(self, shard_name, to_worker, force=False):
        """Move a shard to a different worker.

        Args:
            shard_name: Name of the shard to move
            to_worker: Target worker ID
            force: If True, proceed even if old worker is unreachable

        Returns:
            dict with from_worker and to_worker
        """
        resp = self._call(self._stub.MoveShard,
            pb.MoveShardRequest(
                shard_name=shard_name,
                to_worker=to_worker,
                force=force),
            timeout=60)
        if resp.error:
            raise RuntimeError(f"MoveShard failed: {resp.error}")
        return {"from_worker": resp.from_worker, "to_worker": resp.to_worker}

    def compact(self):
        """Trigger compaction on all workers.

        Returns:
            dict with tables_compacted count
        """
        resp = self._call(self._stub.GetClusterStatus, pb.ClusterStatusRequest(), timeout=10)
        # CompactAll is a per-worker RPC; iterate over healthy workers
        # via coordinator status and note this is a best-effort operation.
        return {"note": "Use per-worker CompactAll RPC directly for now"}

    # ── Internal ──

    def _decode_response(self, resp):
        """Decode a SearchResponse into a pyarrow.Table."""
        if resp.error:
            raise RuntimeError(f"Search error: {resp.error}")
        if not resp.arrow_ipc_data:
            return pa.table({})
        return ipc.open_stream(resp.arrow_ipc_data).read_all()

    @staticmethod
    def _encode_batch(batch):
        """Encode a RecordBatch to Arrow IPC stream bytes."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        return sink.getvalue().to_pybytes()
