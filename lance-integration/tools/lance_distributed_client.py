"""
Lance Distributed Client — query a Lance-Ballista cluster.

Usage:
    from lance_distributed_client import LanceDistributedClient

    client = LanceDistributedClient("localhost:50050")
    results = client.search("products", [0.1, 0.2, ...], k=10)
    print(results.to_pandas())
"""

import struct
import time
import logging
from typing import Optional

import grpc
import pyarrow as pa
import pyarrow.ipc as ipc

import lance_service_pb2
import lance_service_pb2_grpc

log = logging.getLogger("lance_distributed")

# Retryable gRPC status codes (same as Qdrant/Milvus client patterns)
_RETRYABLE_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
}


class LanceDistributedClient:
    """Client for Lance-Ballista distributed retrieval cluster.

    Args:
        address: Scheduler "host:port" (e.g. "localhost:50050")
        timeout: Query timeout in seconds (default 30)
        max_retries: Max retry attempts for transient errors (default 3)
        retry_backoff: Initial retry backoff in seconds (default 0.5)
    """

    def __init__(
        self,
        address: str,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
    ):
        self.address = address
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self._channel = grpc.insecure_channel(
            address,
            options=[
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),  # 64MB
                ("grpc.keepalive_time_ms", 30000),
                ("grpc.keepalive_timeout_ms", 10000),
            ],
        )
        self._stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(self._channel)

    def search(
        self,
        table_name: str,
        query_vector: Optional[list[float]] = None,
        query_text: Optional[str] = None,
        vector_column: str = "vector",
        k: int = 10,
        nprobes: int = 10,
        filter: Optional[str] = None,
        columns: Optional[list[str]] = None,
        metric_type: int = 0,
    ) -> pa.Table:
        """Distributed ANN vector search.

        Provide either query_vector (list of floats) or query_text (string).
        If query_text is provided, the server auto-embeds it to a vector
        using the configured embedding model.

        Returns:
            PyArrow Table with columns: id, _distance, [requested columns]
        """
        if not query_vector and not query_text:
            raise ValueError("Either query_vector or query_text must be provided")
        if k <= 0:
            raise ValueError("k must be > 0")

        request = lance_service_pb2.AnnSearchRequest(
            table_name=table_name,
            vector_column=vector_column,
            query_vector=_encode_vector(query_vector) if query_vector else b"",
            dimension=len(query_vector) if query_vector else 0,
            k=k,
            nprobes=nprobes,
            metric_type=metric_type,
            columns=columns or [],
        )
        if filter:
            request.filter = filter
        if query_text:
            request.query_text = query_text

        response = self._call_with_retry(self._stub.AnnSearch, request)
        return _parse_response(response, "ANN search")

    def fts_search(
        self,
        table_name: str,
        query_text: str,
        text_column: str = "text",
        k: int = 10,
        filter: Optional[str] = None,
        columns: Optional[list[str]] = None,
    ) -> pa.Table:
        """Distributed full-text search."""
        if not query_text:
            raise ValueError("query_text cannot be empty")

        request = lance_service_pb2.FtsSearchRequest(
            table_name=table_name,
            text_column=text_column,
            query_text=query_text,
            k=k,
            columns=columns or [],
        )
        if filter:
            request.filter = filter

        response = self._call_with_retry(self._stub.FtsSearch, request)
        return _parse_response(response, "FTS search")

    def hybrid_search(
        self,
        table_name: str,
        query_vector: list[float],
        vector_column: str = "vector",
        query_text: str = "",
        text_column: str = "text",
        k: int = 10,
        nprobes: int = 10,
        filter: Optional[str] = None,
        columns: Optional[list[str]] = None,
    ) -> pa.Table:
        """Distributed hybrid search (ANN + FTS with RRF fusion)."""
        if not query_vector:
            raise ValueError("query_vector cannot be empty")
        if not query_text:
            raise ValueError("query_text cannot be empty for hybrid search")

        request = lance_service_pb2.HybridSearchRequest(
            table_name=table_name,
            vector_column=vector_column,
            query_vector=_encode_vector(query_vector),
            dimension=len(query_vector),
            nprobes=nprobes,
            text_column=text_column,
            query_text=query_text,
            k=k,
            columns=columns or [],
            metric_type=0,
        )
        if filter:
            request.filter = filter

        response = self._call_with_retry(self._stub.HybridSearch, request)
        return _parse_response(response, "Hybrid search")

    def health_check(self) -> bool:
        """Check if the Scheduler is reachable."""
        try:
            # Use a minimal ANN request as health probe
            self._stub.AnnSearch(
                lance_service_pb2.AnnSearchRequest(
                    table_name="__health__",
                    vector_column="v",
                    query_vector=b"\x00" * 4,
                    dimension=1,
                    k=1,
                    nprobes=1,
                    metric_type=0,
                ),
                timeout=3.0,
            )
            return True
        except grpc.RpcError as e:
            # NOT_FOUND means Scheduler is up but table doesn't exist — still healthy
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return True
            return False

    def close(self):
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _call_with_retry(self, method, request):
        """Call gRPC method with exponential backoff retry on transient errors."""
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                return method(request, timeout=self.timeout)
            except grpc.RpcError as e:
                last_error = e
                if e.code() in _RETRYABLE_CODES and attempt < self.max_retries:
                    wait = self.retry_backoff * (2 ** attempt)
                    log.warning(
                        "Retry %d/%d after %s (%.1fs backoff): %s",
                        attempt + 1, self.max_retries, e.code().name, wait, e.details(),
                    )
                    time.sleep(wait)
                else:
                    break

        # Convert gRPC error to friendly message
        raise _to_client_error(last_error)


class LanceDistributedError(Exception):
    """Error from Lance-Ballista cluster."""
    def __init__(self, message: str, code: str = "UNKNOWN"):
        super().__init__(message)
        self.code = code


def _to_client_error(rpc_error: grpc.RpcError) -> LanceDistributedError:
    """Convert gRPC error to user-friendly error."""
    code = rpc_error.code()
    details = rpc_error.details() or "unknown error"

    messages = {
        grpc.StatusCode.NOT_FOUND: f"Table not found: {details}",
        grpc.StatusCode.UNAVAILABLE: f"Cluster unreachable: {details}. Check if Scheduler is running.",
        grpc.StatusCode.DEADLINE_EXCEEDED: f"Query timed out: {details}",
        grpc.StatusCode.INTERNAL: f"Server error: {details}",
        grpc.StatusCode.INVALID_ARGUMENT: f"Invalid query: {details}",
    }

    message = messages.get(code, f"gRPC error ({code.name}): {details}")
    return LanceDistributedError(message, code.name)


def _encode_vector(vector: list[float]) -> bytes:
    return struct.pack(f"<{len(vector)}f", *vector)


def _decode_arrow_ipc(data: bytes) -> pa.Table:
    if not data:
        return pa.table({})
    reader = ipc.open_stream(data)
    return reader.read_all()


def _parse_response(response, operation: str) -> pa.Table:
    resp = response  # already unwrapped by gRPC
    if hasattr(resp, 'error') and resp.error:
        raise LanceDistributedError(f"{operation} failed: {resp.error}", "SERVER_ERROR")
    return _decode_arrow_ipc(resp.arrow_ipc_data)
