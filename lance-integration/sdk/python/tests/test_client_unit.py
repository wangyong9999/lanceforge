"""
Unit tests for LanceForgeClient — mock gRPC, no running cluster needed.
"""

import struct
import sys
import os
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

import pyarrow as pa
import pyarrow.ipc as ipc

# Add SDK and tools to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'tools'))

import lance_service_pb2 as pb


class TestVectorEncoding(unittest.TestCase):
    """Test that search encodes vectors correctly."""

    def _make_client(self):
        """Create a client with mocked channel."""
        from lanceforge.client import LanceForgeClient
        with patch('grpc.insecure_channel'):
            client = LanceForgeClient("fake:50050")
        client._stub = MagicMock()
        return client

    def test_search_encodes_float32_vector(self):
        client = self._make_client()
        # Mock response
        resp = pb.SearchResponse(arrow_ipc_data=b'', num_rows=0, error='')
        client._stub.AnnSearch.return_value = resp

        client.search("table", query_vector=[1.0, 2.0, 3.0], k=5)

        call_args = client._stub.AnnSearch.call_args
        req = call_args[0][0]
        self.assertEqual(req.table_name, "table")
        self.assertEqual(req.k, 5)
        self.assertEqual(req.dimension, 3)
        # Verify vector is little-endian float32
        expected = struct.pack("<3f", 1.0, 2.0, 3.0)
        self.assertEqual(req.query_vector, expected)

    def test_search_default_nprobes(self):
        client = self._make_client()
        resp = pb.SearchResponse(arrow_ipc_data=b'', num_rows=0, error='')
        client._stub.AnnSearch.return_value = resp

        client.search("t", query_vector=[0.0], k=1)
        req = client._stub.AnnSearch.call_args[0][0]
        self.assertEqual(req.nprobes, 20)

    def test_search_metric_encoding(self):
        client = self._make_client()
        resp = pb.SearchResponse(arrow_ipc_data=b'', num_rows=0, error='')
        client._stub.AnnSearch.return_value = resp

        for metric, expected_type in [("l2", 0), ("cosine", 1), ("dot", 2)]:
            client.search("t", query_vector=[0.0], k=1, metric=metric)
            req = client._stub.AnnSearch.call_args[0][0]
            self.assertEqual(req.metric_type, expected_type, f"metric={metric}")


class TestWriteOperations(unittest.TestCase):
    """Test insert/delete/upsert parameter encoding."""

    def _make_client(self):
        from lanceforge.client import LanceForgeClient
        with patch('grpc.insecure_channel'):
            client = LanceForgeClient("fake:50050")
        client._stub = MagicMock()
        return client

    def test_insert_encodes_ipc(self):
        client = self._make_client()
        resp = pb.WriteResponse(affected_rows=3, new_version=1, error='')
        client._stub.AddRows.return_value = resp

        table = pa.table({'id': [1, 2, 3]})
        result = client.insert("mytable", table)

        self.assertEqual(result["affected_rows"], 3)
        req = client._stub.AddRows.call_args[0][0]
        self.assertEqual(req.table_name, "mytable")
        self.assertGreater(len(req.arrow_ipc_data), 0)

    def test_delete_sends_filter(self):
        client = self._make_client()
        resp = pb.WriteResponse(affected_rows=5, new_version=2, error='')
        client._stub.DeleteRows.return_value = resp

        result = client.delete("mytable", filter="id > 10")
        self.assertEqual(result["affected_rows"], 5)
        req = client._stub.DeleteRows.call_args[0][0]
        self.assertEqual(req.filter, "id > 10")

    def test_upsert_sends_on_columns(self):
        client = self._make_client()
        resp = pb.WriteResponse(affected_rows=2, new_version=3, error='')
        client._stub.UpsertRows.return_value = resp

        table = pa.table({'id': [1, 2], 'val': ['a', 'b']})
        result = client.upsert("mytable", table, on_columns=["id"])
        req = client._stub.UpsertRows.call_args[0][0]
        self.assertEqual(req.on_columns, ["id"])


class TestDDLOperations(unittest.TestCase):
    """Test DDL method parameter passing."""

    def _make_client(self):
        from lanceforge.client import LanceForgeClient
        with patch('grpc.insecure_channel'):
            client = LanceForgeClient("fake:50050")
        client._stub = MagicMock()
        return client

    def test_create_table(self):
        client = self._make_client()
        resp = pb.CreateTableResponse(table_name="newtbl", num_rows=5, error='')
        client._stub.CreateTable.return_value = resp

        table = pa.table({'id': [1, 2, 3, 4, 5]})
        result = client.create_table("newtbl", table, uri="/tmp/test.lance")
        self.assertEqual(result["num_rows"], 5)
        req = client._stub.CreateTable.call_args[0][0]
        self.assertEqual(req.table_name, "newtbl")
        self.assertEqual(req.uri, "/tmp/test.lance")

    def test_list_tables(self):
        client = self._make_client()
        resp = pb.ListTablesResponse(table_names=["a", "b"], error='')
        client._stub.ListTables.return_value = resp
        result = client.list_tables()
        self.assertEqual(result, ["a", "b"])

    def test_drop_table(self):
        client = self._make_client()
        resp = pb.DropTableResponse(error='')
        client._stub.DropTable.return_value = resp
        client.drop_table("old")
        req = client._stub.DropTable.call_args[0][0]
        self.assertEqual(req.table_name, "old")

    def test_count_rows(self):
        client = self._make_client()
        resp = pb.CountRowsResponse(count=42, error='')
        client._stub.CountRows.return_value = resp
        self.assertEqual(client.count_rows("t"), 42)

    def test_create_index(self):
        client = self._make_client()
        resp = pb.CreateIndexResponse(error='')
        client._stub.CreateIndex.return_value = resp
        client.create_index("t", "vec", index_type="IVF_FLAT", num_partitions=16)
        req = client._stub.CreateIndex.call_args[0][0]
        self.assertEqual(req.column, "vec")
        self.assertEqual(req.num_partitions, 16)


class TestClusterManagement(unittest.TestCase):

    def _make_client(self):
        from lanceforge.client import LanceForgeClient
        with patch('grpc.insecure_channel'):
            client = LanceForgeClient("fake:50050")
        client._stub = MagicMock()
        return client

    def test_status(self):
        client = self._make_client()
        resp = pb.ClusterStatusResponse(
            executors=[pb.ExecutorStatus(executor_id="w0", host="h", port=50100, healthy=True)],
            total_shards=2)
        client._stub.GetClusterStatus.return_value = resp
        s = client.status()
        self.assertEqual(len(s["executors"]), 1)
        self.assertTrue(s["executors"][0]["healthy"])

    def test_rebalance(self):
        client = self._make_client()
        resp = pb.RebalanceResponse(shards_moved=3, error='')
        client._stub.Rebalance.return_value = resp
        r = client.rebalance()
        self.assertEqual(r["shards_moved"], 3)

    def test_register_worker(self):
        client = self._make_client()
        resp = pb.RegisterWorkerResponse(assigned_shards=0, error='')
        client._stub.RegisterWorker.return_value = resp
        client.register_worker("w2", "127.0.0.1", 50102)
        req = client._stub.RegisterWorker.call_args[0][0]
        self.assertEqual(req.worker_id, "w2")
        self.assertEqual(req.port, 50102)


class TestErrorHandling(unittest.TestCase):

    def _make_client(self):
        from lanceforge.client import LanceForgeClient
        with patch('grpc.insecure_channel'):
            client = LanceForgeClient("fake:50050")
        client._stub = MagicMock()
        return client

    def test_search_error_raises(self):
        client = self._make_client()
        resp = pb.SearchResponse(arrow_ipc_data=b'', num_rows=0, error='table not found')
        client._stub.AnnSearch.return_value = resp
        with self.assertRaises(RuntimeError):
            client.search("missing", query_vector=[0.0], k=1)

    def test_write_error_raises(self):
        client = self._make_client()
        resp = pb.WriteResponse(affected_rows=0, new_version=0, error='write failed')
        client._stub.AddRows.return_value = resp
        with self.assertRaises(RuntimeError):
            client.insert("t", pa.table({'x': [1]}))

    def test_rebalance_error_raises(self):
        client = self._make_client()
        resp = pb.RebalanceResponse(shards_moved=0, error='no executors')
        client._stub.Rebalance.return_value = resp
        with self.assertRaises(RuntimeError):
            client.rebalance()


class TestTLSChannel(unittest.TestCase):

    @patch('grpc.ssl_channel_credentials')
    @patch('grpc.secure_channel')
    def test_tls_creates_secure_channel(self, mock_secure, mock_creds):
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.pem', mode='wb', delete=False) as f:
            f.write(b'fake-ca-cert')
            ca_path = f.name

        try:
            from lanceforge.client import LanceForgeClient
            client = LanceForgeClient("host:443", tls_ca_cert=ca_path)
            mock_creds.assert_called_once()
            mock_secure.assert_called_once()
        finally:
            os.unlink(ca_path)

    def test_no_tls_creates_insecure_channel(self):
        with patch('grpc.insecure_channel') as mock_insecure:
            from lanceforge.client import LanceForgeClient
            client = LanceForgeClient("host:50050")
            mock_insecure.assert_called_once()


class TestApiKeyAuth(unittest.TestCase):

    def test_api_key_metadata(self):
        from lanceforge.client import LanceForgeClient
        with patch('grpc.insecure_channel'):
            client = LanceForgeClient("fake:50050", api_key="secret123")
        meta = client._metadata()
        self.assertEqual(meta, [("authorization", "Bearer secret123")])

    def test_no_api_key_no_metadata(self):
        from lanceforge.client import LanceForgeClient
        with patch('grpc.insecure_channel'):
            client = LanceForgeClient("fake:50050")
        self.assertIsNone(client._metadata())


if __name__ == '__main__':
    unittest.main()
