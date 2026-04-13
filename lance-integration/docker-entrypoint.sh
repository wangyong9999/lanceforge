#!/bin/bash
# Docker entrypoint: generates sample data if not exists, then starts the given command.
set -e

DATA_DIR="${DATA_DIR:-/data}"
ROLE="${1:-coordinator}"

if [ "$ROLE" = "init" ]; then
    echo "Generating sample data..."
    pip install -q lance pyarrow numpy pyyaml 2>/dev/null
    python3 -c "
import numpy as np, pyarrow as pa, lance, yaml, os
np.random.seed(42)
DIM, N = 64, 5000
data_dir = '${DATA_DIR}'
os.makedirs(data_dir, exist_ok=True)
for i in range(2):
    uri = f'{data_dir}/shard_{i:02d}.lance'
    off = i * (N//2)
    n = N//2
    table = pa.table({
        'id': pa.array(range(off, off+n), type=pa.int64()),
        'text': pa.array([f'doc {j} about topic {j%5}' for j in range(off, off+n)], type=pa.string()),
        'category': pa.array([['tech','science','biz','health','sports'][j%5] for j in range(off, off+n)], type=pa.string()),
        'vector': pa.FixedSizeListArray.from_arrays(
            pa.array(np.random.randn(n*DIM).astype(np.float32)), list_size=DIM),
    })
    lance.write_dataset(table, uri, mode='overwrite')
    ds = lance.dataset(uri)
    ds.create_index('vector', index_type='IVF_FLAT', num_partitions=4)
    print(f'Shard {i}: {ds.count_rows()} rows at {uri}')

cfg = {
    'tables': [{'name': 'documents', 'shards': [
        {'name': f'documents_shard_{i:02d}', 'uri': f'{data_dir}/shard_{i:02d}.lance', 'executors': [f'worker_{i}']}
        for i in range(2)
    ]}],
    'executors': [
        {'id': 'worker_0', 'host': 'worker-0', 'port': 50100},
        {'id': 'worker_1', 'host': 'worker-1', 'port': 50101},
    ],
}
with open(f'{data_dir}/cluster_config.yaml', 'w') as f:
    yaml.dump(cfg, f)
print(f'Config written to {data_dir}/cluster_config.yaml')
"
    echo "Data ready."
    exit 0
fi

# Pass through to actual binary
exec "$@"
