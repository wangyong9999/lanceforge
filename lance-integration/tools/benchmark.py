#!/usr/bin/env python3
"""
Lance-Ballista End-to-End Benchmark

Creates test data, splits into shards, starts a cluster (Scheduler + Executors),
runs ANN queries, measures QPS/P99/recall.

Usage:
    python benchmark.py --num-vectors 100000 --dim 128 --num-shards 3 --k 10
"""

import argparse
import os
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa

from shard_splitter import split_table


def create_benchmark_data(path: str, num_vectors: int, dim: int):
    """Create a Lance table with random vectors for benchmarking."""
    print(f"Creating benchmark data: {num_vectors} vectors, dim={dim}")

    batch_size = min(50000, num_vectors)
    batches = []

    for start in range(0, num_vectors, batch_size):
        end = min(start + batch_size, num_vectors)
        n = end - start

        ids = pa.array(range(start, end), type=pa.int64())
        categories = pa.array([f"cat_{i % 100}" for i in range(start, end)], type=pa.string())
        vectors = pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n * dim).astype(np.float32)),
            list_size=dim,
        )

        batch = pa.record_batch(
            [ids, categories, vectors],
            names=["id", "category", "vector"],
        )
        batches.append(batch)

    schema = batches[0].schema
    reader = pa.RecordBatchReader.from_batches(schema, batches)

    t0 = time.time()
    lance.write_dataset(reader, path, mode="create")
    write_time = time.time() - t0
    print(f"  Written in {write_time:.1f}s")

    return path


def generate_queries(num_queries: int, dim: int, seed: int = 42) -> list[list[float]]:
    """Generate random query vectors."""
    rng = np.random.RandomState(seed)
    return [rng.randn(dim).astype(np.float32).tolist() for _ in range(num_queries)]


def compute_ground_truth(source_path: str, queries: list[list[float]], k: int):
    """Compute brute-force ground truth for recall measurement."""
    print(f"Computing ground truth for {len(queries)} queries (k={k})...")
    ds = lance.dataset(source_path)
    ground_truth = []

    for i, query in enumerate(queries):
        results = ds.to_table(
            nearest={"column": "vector", "q": query, "k": k}
        )
        gt_ids = results.column("id").to_pylist()
        ground_truth.append(set(gt_ids))

        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{len(queries)} queries done")

    return ground_truth


def run_distributed_benchmark(
    scheduler_address: str,
    queries: list[list[float]],
    table_name: str,
    k: int,
    dim: int,
    ground_truth: list[set] = None,
):
    """Run benchmark queries against distributed cluster."""
    import grpc
    import lance_service_pb2
    import lance_service_pb2_grpc

    channel = grpc.insecure_channel(scheduler_address)
    stub = lance_service_pb2_grpc.LanceSchedulerServiceStub(channel)

    latencies = []
    recalls = []
    errors = 0

    print(f"\nRunning {len(queries)} queries against {scheduler_address}...")
    overall_start = time.time()

    for i, query in enumerate(queries):
        vector_bytes = struct.pack(f"<{len(query)}f", *query)

        t0 = time.time()
        try:
            response = stub.AnnSearch(lance_service_pb2.AnnSearchRequest(
                table_name=table_name,
                vector_column="vector",
                query_vector=vector_bytes,
                dimension=dim,
                k=k,
                nprobes=10,
                metric_type=0,
                columns=[],
            ), timeout=30.0)

            latency_ms = (time.time() - t0) * 1000
            latencies.append(latency_ms)

            if response.error:
                errors += 1
                continue

            # Compute recall if ground truth available
            if ground_truth and response.num_rows > 0:
                import pyarrow.ipc as ipc
                reader = ipc.open_stream(response.arrow_ipc_data)
                result_table = reader.read_all()
                result_ids = set(result_table.column("id").to_pylist())
                recall = len(result_ids & ground_truth[i]) / k
                recalls.append(recall)

        except Exception as e:
            errors += 1
            latencies.append((time.time() - t0) * 1000)

        if (i + 1) % 50 == 0:
            avg_lat = np.mean(latencies[-50:])
            print(f"  {i + 1}/{len(queries)}: avg_latency={avg_lat:.1f}ms, errors={errors}")

    overall_time = time.time() - overall_start

    # Compute stats
    latencies_arr = np.array(latencies)
    qps = len(queries) / overall_time

    print(f"\n{'='*60}")
    print(f"BENCHMARK RESULTS")
    print(f"{'='*60}")
    print(f"  Queries:    {len(queries)}")
    print(f"  Errors:     {errors}")
    print(f"  Total time: {overall_time:.2f}s")
    print(f"  QPS:        {qps:.1f}")
    print(f"  Latency P50:  {np.percentile(latencies_arr, 50):.1f}ms")
    print(f"  Latency P95:  {np.percentile(latencies_arr, 95):.1f}ms")
    print(f"  Latency P99:  {np.percentile(latencies_arr, 99):.1f}ms")
    print(f"  Latency Mean: {np.mean(latencies_arr):.1f}ms")
    if recalls:
        print(f"  Recall@{k}:   {np.mean(recalls):.4f}")
    print(f"{'='*60}")

    return {
        "qps": qps,
        "p50_ms": float(np.percentile(latencies_arr, 50)),
        "p95_ms": float(np.percentile(latencies_arr, 95)),
        "p99_ms": float(np.percentile(latencies_arr, 99)),
        "mean_ms": float(np.mean(latencies_arr)),
        "recall": float(np.mean(recalls)) if recalls else None,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description="Lance-Ballista Benchmark")
    parser.add_argument("--num-vectors", type=int, default=100_000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--num-shards", type=int, default=3)
    parser.add_argument("--num-queries", type=int, default=100)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--scheduler", default=None, help="Existing scheduler address (host:port)")
    parser.add_argument("--output-dir", default=None, help="Output directory for data")
    parser.add_argument("--skip-data-gen", action="store_true")
    parser.add_argument("--compute-recall", action="store_true")
    args = parser.parse_args()

    output_dir = args.output_dir or tempfile.mkdtemp(prefix="lance_bench_")
    source_path = os.path.join(output_dir, "source.lance")
    shards_dir = os.path.join(output_dir, "shards")

    if not args.skip_data_gen:
        # Step 1: Create data
        create_benchmark_data(source_path, args.num_vectors, args.dim)

        # Step 2: Split into shards
        split_table(source_path, shards_dir, num_shards=args.num_shards)

    # Step 3: Generate queries
    queries = generate_queries(args.num_queries, args.dim)

    # Step 4: Compute ground truth (optional)
    ground_truth = None
    if args.compute_recall and not args.skip_data_gen:
        ground_truth = compute_ground_truth(source_path, queries, args.k)

    # Step 5: Run benchmark
    if args.scheduler:
        results = run_distributed_benchmark(
            args.scheduler, queries, "table", args.k, args.dim, ground_truth
        )
    else:
        print("\nNo --scheduler provided. Running single-node baseline instead.")
        print("To run distributed benchmark, start cluster first, then pass --scheduler host:port")

        # Single-node baseline
        ds = lance.dataset(source_path)
        latencies = []
        for query in queries:
            t0 = time.time()
            ds.to_table(nearest={"column": "vector", "q": query, "k": args.k})
            latencies.append((time.time() - t0) * 1000)

        latencies_arr = np.array(latencies)
        print(f"\nSINGLE-NODE BASELINE:")
        print(f"  P50: {np.percentile(latencies_arr, 50):.1f}ms")
        print(f"  P99: {np.percentile(latencies_arr, 99):.1f}ms")
        print(f"  QPS: {len(queries) / (sum(latencies) / 1000):.1f}")


if __name__ == "__main__":
    main()
