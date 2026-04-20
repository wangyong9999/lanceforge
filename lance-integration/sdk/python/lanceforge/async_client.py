"""
LanceForge async gRPC client — native grpc.aio, for FastAPI / LangChain
/ any asyncio-first app.

Mirrors the core of the sync `LanceForgeClient` (search, insert,
create_table, drop_table, list_tables, count_rows) but with `async
def` methods, built on `grpc.aio.insecure_channel` /
`grpc.aio.secure_channel`. Same proxy-env hardening as the sync
client (F8).

Not every sync method is ported in 0.2-beta.3 — the set covers the
90% retrieval-heavy workload. Open an issue when you need
upsert / batch_search / create_index / rebalance in async; those
are a mechanical expansion and shipped in 0.3.
"""

import struct

import grpc.aio
import pyarrow as pa
import pyarrow.ipc as ipc

from lanceforge import lance_service_pb2 as pb
from lanceforge import lance_service_pb2_grpc as pb_grpc
from lanceforge.client import _drop_proxy_env


class AsyncLanceForgeClient:
    """Async client for LanceForge distributed retrieval engine.

    Usage::

        async with AsyncLanceForgeClient("localhost:50050") as client:
            hits = await client.search("my_table", query_vector=[...])

    Args:
        host: Coordinator address (e.g., "localhost:50050").
        api_key: Optional API key — added to every call as
            `authorization: Bearer <key>` metadata.
        max_message_size: Max gRPC message size in bytes (default 64MB).
        tls_ca_cert: Path to PEM-encoded CA certificate. When set, uses
            `grpc.aio.secure_channel`.
        default_timeout: Per-RPC timeout in seconds.
        respect_proxy_env: When False (default), strips HTTP/HTTPS/GRPC
            proxy env vars before constructing the channel (F8 parity
            with the sync client).
    """

    def __init__(self, host="localhost:50050", api_key=None,
                 max_message_size=64 * 1024 * 1024, tls_ca_cert=None,
                 default_timeout=30, respect_proxy_env=False):
        if not respect_proxy_env:
            _drop_proxy_env()
        options = [("grpc.max_receive_message_length", max_message_size)]
        if tls_ca_cert:
            with open(tls_ca_cert, 'rb') as f:
                ca_pem = f.read()
            creds = grpc.ssl_channel_credentials(root_certificates=ca_pem)
            self._channel = grpc.aio.secure_channel(host, creds, options=options)
        else:
            self._channel = grpc.aio.insecure_channel(host, options=options)
        self._stub = pb_grpc.LanceSchedulerServiceStub(self._channel)
        self._api_key = api_key
        self._timeout = default_timeout

    async def close(self):
        """Close the async channel — releases sockets + task handles."""
        await self._channel.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _metadata(self):
        if self._api_key:
            return [("authorization", f"Bearer {self._api_key}")]
        return None

    # ── Retrieval ─────────────────────────────────────────────────────

    async def search(self, table_name, query_vector=None, query_text=None,
                     k=10, nprobes=10, filter="", metric="l2", columns=None,
                     vector_column="vector", dimension=None,
                     timeout=None):
        """ANN / text search on `table_name`. Returns
        `{arrow_ipc_data, num_rows, latency_ms, truncated,
        next_offset, error}`. Vector encoding matches the sync client."""
        if query_vector is None and not query_text:
            raise ValueError("search requires query_vector or query_text")

        metric_map = {"l2": 0, "cosine": 1, "dot": 2}
        metric_type = metric_map.get(metric, 0)

        req_kwargs = {
            "table_name": table_name,
            "vector_column": vector_column,
            "k": k,
            "nprobes": nprobes,
            "filter": filter,
            "columns": list(columns or []),
            "metric_type": metric_type,
            "offset": 0,
        }
        if query_vector is not None:
            qv = bytes(struct.pack(f"<{len(query_vector)}f", *query_vector))
            req_kwargs["query_vector"] = qv
            req_kwargs["dimension"] = dimension or len(query_vector)
        else:
            req_kwargs["query_text"] = query_text
            if dimension is not None:
                req_kwargs["dimension"] = dimension
        req = pb.AnnSearchRequest(**req_kwargs)
        resp = await self._stub.AnnSearch(
            req, metadata=self._metadata(), timeout=timeout or self._timeout,
        )
        if resp.error:
            raise RuntimeError(f"search failed: {resp.error}")
        return {
            "arrow_ipc_data": resp.arrow_ipc_data,
            "num_rows": resp.num_rows,
            "latency_ms": resp.latency_ms,
            "truncated": resp.truncated,
            "next_offset": resp.next_offset,
            "error": resp.error,
        }

    # ── DDL ───────────────────────────────────────────────────────────

    async def create_table(self, table_name, data, index_column="vector",
                           num_partitions=0, timeout=None):
        """Create table from an Arrow-compatible object (pa.Table /
        RecordBatch). Returns the create response."""
        if isinstance(data, pa.Table):
            batch = data.to_batches()[0] if data.num_rows else \
                pa.record_batch([pa.array([], type=f.type) for f in data.schema],
                                names=data.schema.names)
        else:
            batch = data
        ipc_data = _ipc_bytes(batch)
        req = pb.CreateTableRequest(
            table_name=table_name,
            arrow_ipc_data=ipc_data,
            index_column=index_column,
            index_num_partitions=num_partitions,
        )
        resp = await self._stub.CreateTable(
            req, metadata=self._metadata(), timeout=timeout or self._timeout,
        )
        if resp.error:
            raise RuntimeError(f"create_table failed: {resp.error}")
        return {"table_name": resp.table_name, "num_rows": resp.num_rows}

    async def drop_table(self, table_name, timeout=None):
        req = pb.DropTableRequest(table_name=table_name)
        resp = await self._stub.DropTable(
            req, metadata=self._metadata(), timeout=timeout or self._timeout,
        )
        if resp.error:
            raise RuntimeError(f"drop_table failed: {resp.error}")
        return True

    async def list_tables(self, timeout=None):
        resp = await self._stub.ListTables(
            pb.ListTablesRequest(), metadata=self._metadata(),
            timeout=timeout or self._timeout,
        )
        return list(resp.table_names)

    # ── Write + read ──────────────────────────────────────────────────

    async def insert(self, table_name, data, on_columns=None, timeout=None):
        """Insert rows. Matches the sync client surface; the
        `request_id` idempotency field (H3) is not yet exposed here —
        the current generated proto stub bundled with the SDK doesn't
        carry it. Regenerate `lance_service_pb2.py` from the latest
        proto to enable it."""
        if isinstance(data, pa.Table):
            batch = data.to_batches()[0] if data.num_rows else \
                pa.record_batch([pa.array([], type=f.type) for f in data.schema],
                                names=data.schema.names)
        else:
            batch = data
        req = pb.AddRowsRequest(
            table_name=table_name,
            arrow_ipc_data=_ipc_bytes(batch),
            on_columns=list(on_columns or []),
        )
        resp = await self._stub.AddRows(
            req, metadata=self._metadata(), timeout=timeout or self._timeout,
        )
        if resp.error:
            raise RuntimeError(f"insert failed: {resp.error}")
        return {"affected_rows": resp.affected_rows, "new_version": resp.new_version}

    async def count_rows(self, table_name, timeout=None):
        req = pb.CountRowsRequest(table_name=table_name)
        resp = await self._stub.CountRows(
            req, metadata=self._metadata(), timeout=timeout or self._timeout,
        )
        if resp.error:
            raise RuntimeError(f"count_rows failed: {resp.error}")
        return resp.count


def _ipc_bytes(batch):
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()
