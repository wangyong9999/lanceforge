"""
LanceForge gRPC client — thin wrapper over generated proto stubs.
"""

import struct
import sys
import os

import grpc
import pyarrow as pa
import pyarrow.ipc as ipc

# Add tools dir to path for generated proto stubs
_tools_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'tools')
if _tools_dir not in sys.path:
    sys.path.insert(0, os.path.abspath(_tools_dir))

import lance_service_pb2 as pb
import lance_service_pb2_grpc as pb_grpc


class LanceForgeClient:
    """Client for LanceForge distributed retrieval engine.

    Args:
        host: Coordinator address (e.g., "localhost:50050")
        api_key: Optional API key for authentication
        max_message_size: Max gRPC message size in bytes (default 64MB)
    """

    def __init__(self, host="localhost:50050", api_key=None, max_message_size=64*1024*1024):
        options = [("grpc.max_receive_message_length", max_message_size)]
        self._channel = grpc.insecure_channel(host, options=options)
        self._stub = pb_grpc.LanceSchedulerServiceStub(self._channel)
        self._api_key = api_key

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

    # ── Search ──

    def search(self, table, query_vector, k=10, nprobes=20,
               filter=None, columns=None, metric="l2"):
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
            vector_column="vector",
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

        resp = self._stub.AnnSearch(req, metadata=self._metadata(), timeout=30)
        return self._decode_response(resp)

    def text_search(self, table, query_text, k=10, filter=None, columns=None):
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
            text_column="text",
            query_text=query_text,
            k=k,
        )
        if filter:
            req.filter = filter
        if columns:
            req.columns.extend(columns)

        resp = self._stub.FtsSearch(req, metadata=self._metadata(), timeout=30)
        return self._decode_response(resp)

    def hybrid_search(self, table, query_vector, query_text, k=10,
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
            vector_column="vector",
            query_vector=vec_bytes,
            dimension=len(query_vector),
            nprobes=nprobes,
            metric_type=metric_type,
            text_column="text",
            query_text=query_text,
            k=k,
        )
        if filter:
            req.filter = filter

        resp = self._stub.HybridSearch(req, metadata=self._metadata(), timeout=30)
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
        resp = self._stub.AddRows(
            pb.AddRowsRequest(table_name=table, arrow_ipc_data=ipc_data),
            metadata=self._metadata(), timeout=30)

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
        resp = self._stub.DeleteRows(
            pb.DeleteRowsRequest(table_name=table, filter=filter),
            metadata=self._metadata(), timeout=30)

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
        resp = self._stub.UpsertRows(
            pb.UpsertRowsRequest(
                table_name=table,
                arrow_ipc_data=ipc_data,
                on_columns=on_columns),
            metadata=self._metadata(), timeout=30)

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

    # ── Status ──

    def status(self):
        """Get cluster status.

        Returns:
            dict with executor health information
        """
        resp = self._stub.GetClusterStatus(pb.ClusterStatusRequest(),
                                           metadata=self._metadata(), timeout=10)
        return {
            "executors": [
                {"id": e.executor_id, "host": e.host, "port": e.port, "healthy": e.healthy}
                for e in resp.executors
            ],
            "total_shards": resp.total_shards,
        }

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
