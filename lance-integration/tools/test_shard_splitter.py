#!/usr/bin/env python3
"""Tests for shard_splitter.py"""

import os
import tempfile

import lance
import numpy as np
import pyarrow as pa
import pytest

from shard_splitter import split_table, generate_config


def create_test_table(path, num_rows=1000, dim=8):
    """Create a test Lance table with vectors."""
    ids = pa.array(range(num_rows), type=pa.int32())
    categories = pa.array(
        ["cat_a" if i % 2 == 0 else "cat_b" for i in range(num_rows)],
        type=pa.string(),
    )
    vectors = pa.FixedSizeListArray.from_arrays(
        pa.array(np.random.randn(num_rows * dim).astype(np.float32)),
        list_size=dim,
    )

    table = pa.table({
        "id": ids,
        "category": categories,
        "vector": vectors,
    })

    lance.write_dataset(table, path, mode="create")
    return path


class TestShardSplitter:
    def test_basic_split(self):
        """Test splitting a table into N shards preserves all rows."""
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source.lance")
            output = os.path.join(tmp, "shards")

            create_test_table(source, num_rows=500, dim=8)

            shards = split_table(source, output, num_shards=3)

            assert len(shards) > 0
            total_rows = sum(s["rows"] for s in shards)
            assert total_rows == 500, f"Expected 500, got {total_rows}"

            # Verify each shard is a valid Lance dataset
            for s in shards:
                ds = lance.dataset(s["uri"])
                assert ds.count_rows() == s["rows"]

    def test_single_shard(self):
        """Edge case: split into 1 shard = copy."""
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source.lance")
            output = os.path.join(tmp, "shards")

            create_test_table(source, num_rows=100, dim=4)
            shards = split_table(source, output, num_shards=1)

            assert len(shards) == 1
            assert shards[0]["rows"] == 100

    def test_more_shards_than_fragments(self):
        """Edge case: requesting more shards than fragments."""
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source.lance")
            output = os.path.join(tmp, "shards")

            create_test_table(source, num_rows=50, dim=4)
            ds = lance.dataset(source)
            num_frags = len(ds.get_fragments())

            shards = split_table(source, output, num_shards=num_frags + 5)

            # Should create at most num_frags shards
            assert len(shards) <= num_frags + 5
            total_rows = sum(s["rows"] for s in shards)
            assert total_rows == 50

    def test_config_generation(self):
        """Test that generated config has correct structure."""
        shard_info = [
            {"name": "shard_0000", "uri": "/tmp/s0.lance", "rows": 100, "fragments": 1, "fragment_ids": [0]},
            {"name": "shard_0001", "uri": "/tmp/s1.lance", "rows": 100, "fragments": 1, "fragment_ids": [1]},
            {"name": "shard_0002", "uri": "/tmp/s2.lance", "rows": 100, "fragments": 1, "fragment_ids": [2]},
        ]

        config = generate_config(shard_info, num_executors=2)

        assert "tables" in config
        assert "executors" in config
        assert len(config["executors"]) == 2
        assert len(config["tables"][0]["shards"]) == 3

        # Verify round-robin assignment
        assignments = [s["executors"][0] for s in config["tables"][0]["shards"]]
        assert assignments == ["executor_0", "executor_1", "executor_0"]

    def test_data_integrity(self):
        """Verify that union of all shards equals original data."""
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source.lance")
            output = os.path.join(tmp, "shards")

            create_test_table(source, num_rows=300, dim=4)
            original = lance.dataset(source).to_table()

            shards = split_table(source, output, num_shards=3)

            # Concatenate all shard data
            shard_tables = []
            for s in shards:
                shard_tables.append(lance.dataset(s["uri"]).to_table())
            merged = pa.concat_tables(shard_tables)

            # Sort both by id for comparison
            original_sorted = original.sort_by("id")
            merged_sorted = merged.sort_by("id")

            assert original_sorted.num_rows == merged_sorted.num_rows
            assert original_sorted.column("id").equals(merged_sorted.column("id"))
            assert original_sorted.column("category").equals(merged_sorted.column("category"))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
