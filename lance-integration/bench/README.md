# LanceForge Benchmarks

## Quick Start

```bash
# Run all core benchmarks (quick mode for CI)
python run_all.py --quick

# Full benchmark suite (takes ~30 min)
python run_all.py

# CI mode (quick + strict thresholds + exit code)
python run_all.py --ci
```

## Benchmark Suite

| Benchmark | Dataset | What it measures | Threshold |
|-----------|---------|-----------------|-----------|
| `recall_benchmark.py` | 200K×128d, 3 shards | recall@{1,10,100} vs nprobes | recall >= 0.95 |
| `bench_filtered.py` | 200K×128d, 10% filter | Filtered ANN recall + QPS | recall >= 0.95 |
| `bench_cosine.py` | 100K×128d | L2 + Cosine distance | recall >= 0.95 |
| `bench_highdim.py` | 100K×768d, 50K×1536d | High-dimension recall | recall >= 0.95 |
| `bench_scale.py` | 1M×128d, 5 shards | Scale: QPS, memory, build time | recall >= 0.95 |
| `bench_standard.py` | 500K (SIFT/GloVe) | Recall-QPS tradeoff curve | recall >= 0.95 |
| `bench_shard_scaling.py` | 500K, 1→10 shards | Scaling linearity | recall stable |
| `bench_competitor_baseline.py` | 200K×128d | LanceForge vs Qdrant | comparative |
| `bench_hybrid_ndcg.py` | 50K+text | Hybrid NDCG/MRR | RRF working |

## E2E Tests (run via `run_all.py`)

| Test | What it validates |
|------|------------------|
| `e2e_enterprise_test.py` | SDK, REST, write, concurrent, metrics (12 tests) |
| `e2e_write_test.py` | Insert → search → delete → verify (6 tests) |
| `e2e_full_system_test.py` | MinIO full-stack (10 tests, needs MinIO) |

## Latest Results (Competitor Baseline)

```
200K×128d, same machine, same dataset

System          Unfiltered       Filtered 10%     Concurrent 20
                recall  QPS      recall  QPS      QPS
Qdrant (HNSW)   1.00   79       1.00    107      11
LanceForge       1.00   106      1.00    89       47
```

## CI Integration

The `benchmark-regression` job in `.github/workflows/ci.yml` runs `run_all.py --ci`
on every push to lance-main. Results are uploaded as artifacts.

## Output

All benchmarks save JSON results to `results/`. The regression runner produces
`results/regression_report.json` with pass/fail status and timing.
