# LanceForge Python SDK

Python client for [LanceForge](https://github.com/wangyong9999/lanceforge) — distributed multimodal retrieval engine built on LanceDB.

## Install

```bash
# From GitHub (latest)
pip install git+https://github.com/wangyong9999/lanceforge.git#subdirectory=lance-integration/sdk/python

# From local clone
git clone https://github.com/wangyong9999/lanceforge.git
cd lanceforge/lance-integration/sdk/python
pip install .

# With LangChain support
pip install ".[langchain]"
```

## Quick Start

```python
from lanceforge import LanceForgeClient

client = LanceForgeClient("localhost:50050")

# Vector search
results = client.search("products", query_vector=[0.1, 0.2, ...], k=10)

# Full-text search
results = client.text_search("products", "wireless headphones", k=10)

# Hybrid (ANN + FTS with RRF fusion)
results = client.hybrid_search("products",
    query_vector=[0.1, 0.2, ...], query_text="headphones", k=10)
```

## API Reference

### Search

| Method | Description |
|--------|-------------|
| `search(table, query_vector, k, filter, columns, metric, nprobes)` | ANN vector similarity search |
| `text_search(table, query_text, k, filter, columns, text_column)` | Full-text BM25 search |
| `hybrid_search(table, query_vector, query_text, k, ...)` | ANN + FTS with RRF reranking |
| `sparse_search(table, query_text, k, ...)` | Sparse vector / BM25 search |

### Write

| Method | Description |
|--------|-------------|
| `insert(table, data)` | Add rows (pyarrow Table or RecordBatch) |
| `delete(table, filter)` | Delete rows matching SQL predicate |
| `upsert(table, data, on_columns)` | Insert or update by key columns |

### DDL

| Method | Description |
|--------|-------------|
| `create_table(table, data, uri, index_column, num_partitions)` | Create table with auto-sharding |
| `drop_table(table)` | Drop table and purge storage |
| `create_index(table, column, index_type, num_partitions)` | Create index (IVF_FLAT, BTREE, INVERTED) |
| `list_tables()` | List all tables |
| `get_schema(table)` | Get column names and types |
| `count_rows(table)` | Distributed row count |

### Admin

| Method | Description |
|--------|-------------|
| `status()` | Cluster health and worker info |
| `rebalance()` | Redistribute shards across workers |
| `move_shard(shard_name, to_worker, force)` | Manual shard migration |
| `register_worker(worker_id, host, port)` | Register new worker |

### Configuration

```python
client = LanceForgeClient(
    "host:50050",
    api_key="your-key",       # optional Bearer token
    tls_ca_cert="ca.pem",     # optional TLS verification
    default_timeout=30,       # seconds (all RPCs)
    max_retries=3,            # exponential backoff on transient errors
    retry_backoff=0.5,        # base backoff seconds
)
```

## LangChain

```python
from lanceforge.langchain import LanceForgeVectorStore
from langchain_openai import OpenAIEmbeddings

store = LanceForgeVectorStore(
    host="localhost:50050",
    table_name="documents",
    embedding=OpenAIEmbeddings(),
)
docs = store.similarity_search("retrieval augmented generation", k=5)
```

## License

Apache License 2.0
