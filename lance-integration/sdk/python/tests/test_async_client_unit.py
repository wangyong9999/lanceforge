"""
Unit tests for AsyncLanceForgeClient — exercises the API surface
against mocked gRPC stubs. No running cluster needed.
"""

import asyncio
import os
import struct
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lanceforge import lance_service_pb2 as pb


def _run(coro):
    """Run a coroutine from a sync test method. Each test owns its
    event loop so the pytest-asyncio dep isn't required."""
    return asyncio.run(coro)


class TestAsyncClientBasics(unittest.TestCase):
    def _make_client(self):
        from lanceforge.async_client import AsyncLanceForgeClient
        with patch('grpc.aio.insecure_channel'):
            client = AsyncLanceForgeClient("fake:50050")
        client._stub = MagicMock()
        return client

    def test_search_encodes_vector_and_returns_payload(self):
        client = self._make_client()
        resp = pb.SearchResponse(
            arrow_ipc_data=b'\x00\x01', num_rows=3, latency_ms=7,
            truncated=False, next_offset=0, error='',
        )
        client._stub.AnnSearch = AsyncMock(return_value=resp)

        result = _run(client.search("t", query_vector=[0.1, 0.2, 0.3], k=5,
                                    metric="cosine"))
        self.assertEqual(result["num_rows"], 3)
        self.assertEqual(result["latency_ms"], 7)

        called_req = client._stub.AnnSearch.call_args[0][0]
        self.assertEqual(called_req.table_name, "t")
        self.assertEqual(called_req.k, 5)
        self.assertEqual(called_req.metric_type, 1)  # cosine
        self.assertEqual(called_req.dimension, 3)

        # Vector must be LE f32 bytes matching the input.
        expected = struct.pack("<3f", 0.1, 0.2, 0.3)
        self.assertEqual(called_req.query_vector, expected)

    def test_search_raises_on_error(self):
        client = self._make_client()
        resp = pb.SearchResponse(error='boom')
        client._stub.AnnSearch = AsyncMock(return_value=resp)
        with self.assertRaises(RuntimeError) as ctx:
            _run(client.search("t", query_vector=[0.0], k=1))
        self.assertIn("boom", str(ctx.exception))

    def test_list_tables_returns_list(self):
        client = self._make_client()
        resp = pb.ListTablesResponse(table_names=["a", "b"])
        client._stub.ListTables = AsyncMock(return_value=resp)
        names = _run(client.list_tables())
        self.assertEqual(names, ["a", "b"])

    def test_insert_encodes_ipc(self):
        client = self._make_client()
        resp = pb.WriteResponse(affected_rows=4, new_version=1, error='')
        client._stub.AddRows = AsyncMock(return_value=resp)

        table = pa.table({'id': [1, 2, 3, 4]})
        out = _run(client.insert("t", table, on_columns=["id"]))
        self.assertEqual(out["affected_rows"], 4)
        req = client._stub.AddRows.call_args[0][0]
        self.assertEqual(req.table_name, "t")
        self.assertEqual(list(req.on_columns), ["id"])
        self.assertGreater(len(req.arrow_ipc_data), 0)

    def test_count_rows(self):
        client = self._make_client()
        resp = pb.CountRowsResponse(count=1234, error='')
        client._stub.CountRows = AsyncMock(return_value=resp)
        n = _run(client.count_rows("t"))
        self.assertEqual(n, 1234)

    def test_api_key_metadata(self):
        from lanceforge.async_client import AsyncLanceForgeClient
        with patch('grpc.aio.insecure_channel'):
            client = AsyncLanceForgeClient("fake:50050", api_key="k1")
        self.assertEqual(client._metadata(), [("authorization", "Bearer k1")])

    def test_proxy_env_stripped_by_default(self):
        # F8 parity: async ctor must also clear proxy env vars.
        from lanceforge.async_client import AsyncLanceForgeClient
        with patch.dict(os.environ, {"HTTP_PROXY": "http://x/",
                                      "HTTPS_PROXY": "http://y/"},
                        clear=False):
            with patch('grpc.aio.insecure_channel'):
                AsyncLanceForgeClient("fake:50050")
            self.assertNotIn("HTTP_PROXY", os.environ)
            self.assertNotIn("HTTPS_PROXY", os.environ)

    def test_async_context_manager(self):
        # Validate `async with` protocol: __aenter__ returns self,
        # __aexit__ awaits close (no exception).
        async def scenario():
            from lanceforge.async_client import AsyncLanceForgeClient
            with patch('grpc.aio.insecure_channel') as mock_ch:
                mock_ch.return_value.close = AsyncMock()
                async with AsyncLanceForgeClient("fake:50050") as client:
                    self.assertIsNotNone(client)
        _run(scenario())


if __name__ == '__main__':
    unittest.main()
