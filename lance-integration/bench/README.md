# LanceForge Benchmarks

## Recall Benchmark (SIFT1M)

Measures distributed ANN recall@k against brute-force ground truth using the standard SIFT1M dataset (1M vectors, 128d, L2 distance).

### Quick Run

```bash
cd lance-integration/bench
pip install numpy lance pyarrow grpcio pyyaml

# Full benchmark: download SIFT1M, create shards, start cluster, measure recall
python recall_benchmark.py

# Custom: 5 shards, 1000 queries
python recall_benchmark.py --num-shards 5 --num-queries 1000

# Reuse existing shards
python recall_benchmark.py --skip-prep
```

### What It Measures

| Metric | Description |
|--------|-------------|
| recall@1 | Fraction of queries where the true nearest neighbor is returned |
| recall@10 | Fraction of true top-10 found in returned top-10 |
| recall@100 | Fraction of true top-100 found in returned top-100 |
| QPS | Queries per second (end-to-end through coordinator) |
| P50/P99 | Latency percentiles |

Tests are run with nprobes = {10, 20, 50} to show the recall vs speed tradeoff.

### Pass Criteria

- **recall@10 >= 0.95** at nprobes=20 (industry standard threshold)
- Results are compared against SIFT1M ground truth (pre-computed brute-force 100-NN)

### Output

Results are saved to `results/recall_sift1m.json` and printed as a table:

```
  Config                     recall@k      QPS   P50(ms)   P99(ms)
  -------------------------  ---------- --------  --------- ---------
  ✓ k=10, nprobes=20           0.9612     85.3       11.2      28.4
  ✗ k=10, nprobes=10           0.9134     120.5       7.8      19.1
```

## ann-benchmarks Integration

For standardized comparison against faiss, hnswlib, and other ANN libraries:

```bash
# Install ann-benchmarks
git clone https://github.com/erikbern/ann-benchmarks.git
cd ann-benchmarks
pip install -r requirements.txt

# Copy LanceForge wrapper
cp /path/to/lance-integration/bench/ann_bench_wrapper.py \
   ann_benchmarks/algorithms/lanceforge/module.py

# Run SIFT1M benchmark
python run.py --algorithm lanceforge --dataset sift-128-euclidean
python plot.py --dataset sift-128-euclidean
```
