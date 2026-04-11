"""
LanceForge wrapper for ann-benchmarks (https://github.com/erikbern/ann-benchmarks).

Drop this file into ann_benchmarks/algorithms/lanceforge/module.py
and add config.yml alongside it.

This wrapper connects to a running LanceForge cluster via gRPC.
Start the cluster separately before running ann-benchmarks.
"""

import struct
import numpy as np

from ann_benchmarks.algorithms.base import BaseANN


class LanceForge(BaseANN):
    """ann-benchmarks adapter for LanceForge distributed ANN search."""

    def __init__(self, metric, nprobes=20, coordinator="127.0.0.1:50050"):
        self._metric = metric
        self._nprobes = nprobes
        self._coordinator = coordinator
        self._stub = None
        self._dim = None

    def fit(self, X):
        """
        ann-benchmarks calls this to load the dataset.
        For LanceForge, the data must be pre-loaded into the cluster.
        This method just establishes the gRPC connection.
        """
        import grpc
        # Import from the tools directory (generated proto stubs)
        import lance_service_pb2_grpc

        self._dim = X.shape[1]
        channel = grpc.insecure_channel(self._coordinator,
            options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)])
        self._stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    def query(self, v, n):
        """Return indices of n approximate nearest neighbors of vector v."""
        import grpc
        import lance_service_pb2
        import pyarrow.ipc as ipc

        query_bytes = struct.pack(f'<{len(v)}f', *v)

        # metric_type: 0=L2, 1=Cosine, 2=DotProduct
        metric_type = 0
        if self._metric == 'angular' or self._metric == 'cosine':
            metric_type = 1
        elif self._metric == 'ip' or self._metric == 'dot':
            metric_type = 2

        r = self._stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
            table_name="benchmark",
            vector_column="vector",
            query_vector=query_bytes,
            dimension=self._dim,
            k=n,
            nprobes=self._nprobes,
            metric_type=metric_type,
        ), timeout=30)

        if r.error or not r.arrow_ipc_data:
            return []

        table = ipc.open_stream(r.arrow_ipc_data).read_all()
        return table.column("id").to_pylist()[:n]

    def __str__(self):
        return f"LanceForge(nprobes={self._nprobes})"
