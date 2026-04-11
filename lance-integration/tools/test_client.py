#!/usr/bin/env python3
"""Tests for Lance Distributed Client."""

import struct
import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from lance_distributed_client import (
    LanceDistributedClient,
    LanceDistributedError,
    _decode_arrow_ipc,
    _encode_vector,
)


class TestArrowIpcRoundtrip:
    def test_encode_decode_basic(self):
        table = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "_distance": pa.array([0.1, 0.2, 0.3], type=pa.float32()),
        })
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        result = _decode_arrow_ipc(data)
        assert result.num_rows == 3
        assert result.column("_distance").to_pylist() == pytest.approx([0.1, 0.2, 0.3], abs=1e-6)

    def test_decode_empty(self):
        result = _decode_arrow_ipc(b"")
        assert result.num_rows == 0

    def test_vector_serialization(self):
        vector = [0.1, 0.2, 0.3, 0.4]
        encoded = _encode_vector(vector)
        decoded = struct.unpack(f"<{len(vector)}f", encoded)
        assert list(decoded) == pytest.approx(vector, abs=1e-6)

    def test_large_batch(self):
        n = 1000
        table = pa.table({
            "id": pa.array(range(n), type=pa.int64()),
            "_distance": pa.array(np.random.rand(n).astype(np.float32)),
        })
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()

        result = _decode_arrow_ipc(sink.getvalue().to_pybytes())
        assert result.num_rows == n

    def test_proto_import(self):
        import lance_service_pb2
        import lance_service_pb2_grpc
        req = lance_service_pb2.AnnSearchRequest(
            table_name="test", vector_column="v",
            query_vector=b"\x00" * 32, dimension=8, k=10, nprobes=5,
        )
        assert req.table_name == "test"
        assert hasattr(lance_service_pb2_grpc, 'LanceSchedulerServiceStub')


class TestClientValidation:
    """Test client-side input validation (no server needed)."""

    def test_empty_query_rejected(self):
        """Neither vector nor text → error"""
        client = LanceDistributedClient("localhost:99999", timeout=1, max_retries=0)
        with pytest.raises(ValueError, match="Either"):
            client.search("t", k=5)

    def test_text_query_builds_request(self):
        """Text query should build valid request (connection will fail but request is valid)"""
        client = LanceDistributedClient("localhost:99999", timeout=1, max_retries=0)
        with pytest.raises(LanceDistributedError):
            # Will fail to connect but validates the request builds correctly
            client.search("t", query_text="hello world", k=5)

    def test_k_zero_rejected(self):
        client = LanceDistributedClient("localhost:99999", timeout=1, max_retries=0)
        with pytest.raises(ValueError, match="k must"):
            client.search("t", [0.1], k=0)

    def test_empty_fts_text_rejected(self):
        client = LanceDistributedClient("localhost:99999", timeout=1, max_retries=0)
        with pytest.raises(ValueError, match="empty"):
            client.fts_search("t", "", k=5)

    def test_hybrid_empty_text_rejected(self):
        client = LanceDistributedClient("localhost:99999", timeout=1, max_retries=0)
        with pytest.raises(ValueError, match="empty"):
            client.hybrid_search("t", [0.1], query_text="", k=5)


class TestClientConnection:
    """Test connection error handling."""

    def test_unreachable_server_friendly_error(self):
        client = LanceDistributedClient("localhost:99999", timeout=1, max_retries=0)
        with pytest.raises(LanceDistributedError) as exc_info:
            client.search("t", [0.1, 0.2], k=5)
        # May be UNAVAILABLE or DEADLINE_EXCEEDED depending on OS/timing
        assert exc_info.value.code in ("UNAVAILABLE", "DEADLINE_EXCEEDED")

    def test_health_check_unreachable(self):
        client = LanceDistributedClient("localhost:99999", timeout=1, max_retries=0)
        assert client.health_check() is False

    def test_context_manager(self):
        with LanceDistributedClient("localhost:99999") as client:
            assert client.address == "localhost:99999"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
