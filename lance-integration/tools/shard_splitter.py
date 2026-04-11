#!/usr/bin/env python3
"""
Lance Shard Splitter — Split a large Lance table into multiple shard tables.

Usage:
    python shard_splitter.py <source_uri> <output_dir> --num-shards 10
    python shard_splitter.py s3://bucket/big_table.lance ./shards/ --num-shards 10 --build-index

Each shard is an independent Lance table with its own Manifest, Fragments, and optionally Indexes.
"""

import argparse
import json
import math
import os
import sys
import time
from pathlib import Path

import lance
import pyarrow as pa


def split_table(
    source_uri: str,
    output_dir: str,
    num_shards: int,
    build_index: bool = False,
    vector_column: str = "vector",
    index_type: str = "IVF_PQ",
    num_partitions: int = 64,
    num_sub_vectors: int = 16,
):
    """Split a Lance table into N shard tables."""
    print(f"Opening source: {source_uri}")
    ds = lance.dataset(source_uri)
    total_rows = ds.count_rows()
    schema = ds.schema
    fragments = ds.get_fragments()
    num_fragments = len(fragments)

    print(f"  Total rows: {total_rows:,}")
    print(f"  Fragments: {num_fragments}")
    print(f"  Schema: {schema}")
    print(f"  Splitting into {num_shards} shards")

    os.makedirs(output_dir, exist_ok=True)

    # Distribute fragments evenly across shards
    frags_per_shard = math.ceil(num_fragments / num_shards)

    shard_info = []
    for shard_idx in range(num_shards):
        start = shard_idx * frags_per_shard
        end = min(start + frags_per_shard, num_fragments)
        if start >= num_fragments:
            break

        shard_fragments = fragments[start:end]
        shard_name = f"shard_{shard_idx:04d}"
        shard_uri = os.path.join(output_dir, f"{shard_name}.lance")

        print(f"\n--- Shard {shard_idx}: {shard_name} ---")
        print(f"  Fragments: {start}..{end} ({end - start} fragments)")

        # Read data from selected fragments
        frag_ids = [f.metadata.id for f in shard_fragments]
        scanner = ds.scanner(fragments=shard_fragments)
        table = scanner.to_table()
        shard_rows = table.num_rows

        print(f"  Rows: {shard_rows:,}")

        # Write shard table
        t0 = time.time()
        lance.write_dataset(
            table,
            shard_uri,
            mode="create",
        )
        write_time = time.time() - t0
        print(f"  Written in {write_time:.1f}s")

        # Optionally build index
        if build_index and shard_rows > 0:
            try:
                shard_ds = lance.dataset(shard_uri)
                # Check if vector column exists
                if vector_column in [f.name for f in schema]:
                    print(f"  Building {index_type} index on '{vector_column}'...")
                    t0 = time.time()
                    shard_ds.create_index(
                        vector_column,
                        index_type=index_type,
                        num_partitions=min(num_partitions, max(1, shard_rows // 100)),
                        num_sub_vectors=num_sub_vectors,
                        replace=True,
                    )
                    idx_time = time.time() - t0
                    print(f"  Index built in {idx_time:.1f}s")
                else:
                    print(f"  No vector column '{vector_column}' found, skipping index")
            except Exception as e:
                print(f"  Warning: Index build failed: {e}")

        # Verify shard
        verify_ds = lance.dataset(shard_uri)
        verify_rows = verify_ds.count_rows()
        assert verify_rows == shard_rows, f"Shard verification failed: {verify_rows} != {shard_rows}"

        shard_info.append({
            "name": shard_name,
            "uri": shard_uri,
            "rows": shard_rows,
            "fragments": end - start,
            "fragment_ids": frag_ids,
        })

    # Verify total
    total_shard_rows = sum(s["rows"] for s in shard_info)
    print(f"\n=== Summary ===")
    print(f"  Source rows: {total_rows:,}")
    print(f"  Shard total: {total_shard_rows:,}")
    assert total_shard_rows == total_rows, \
        f"Row count mismatch! Source={total_rows}, Shards={total_shard_rows}"
    print(f"  ✓ Row count verified")

    # Generate cluster config
    config = generate_config(shard_info, num_shards)
    config_path = os.path.join(output_dir, "cluster_config.yaml")
    with open(config_path, "w") as f:
        import yaml
        yaml.dump(config, f, default_flow_style=False)
    print(f"  Config written to: {config_path}")

    return shard_info


def generate_config(shard_info, num_executors=None):
    """Generate a cluster config YAML from shard info."""
    if num_executors is None:
        num_executors = min(len(shard_info), 4)

    # Round-robin assign shards to executors
    executors = []
    for i in range(num_executors):
        executors.append({
            "id": f"executor_{i}",
            "host": "127.0.0.1",
            "port": 50100 + i,
        })

    shards = []
    for idx, info in enumerate(shard_info):
        executor_idx = idx % num_executors
        shards.append({
            "name": info["name"],
            "uri": info["uri"],
            "executors": [f"executor_{executor_idx}"],
        })

    table_name = "table"  # derive from source if needed
    return {
        "tables": [{
            "name": table_name,
            "shards": shards,
        }],
        "executors": executors,
    }


def main():
    parser = argparse.ArgumentParser(description="Split Lance table into shards")
    parser.add_argument("source", help="Source Lance table URI")
    parser.add_argument("output_dir", help="Output directory for shard tables")
    parser.add_argument("--num-shards", type=int, default=10, help="Number of shards")
    parser.add_argument("--build-index", action="store_true", help="Build vector index on each shard")
    parser.add_argument("--vector-column", default="vector", help="Vector column name")
    parser.add_argument("--index-type", default="IVF_PQ", help="Index type")
    parser.add_argument("--num-partitions", type=int, default=64, help="IVF partitions")
    parser.add_argument("--num-sub-vectors", type=int, default=16, help="PQ sub-vectors")

    args = parser.parse_args()

    split_table(
        args.source,
        args.output_dir,
        num_shards=args.num_shards,
        build_index=args.build_index,
        vector_column=args.vector_column,
        index_type=args.index_type,
        num_partitions=args.num_partitions,
        num_sub_vectors=args.num_sub_vectors,
    )


if __name__ == "__main__":
    main()
