# BEIR SciFact Results

**Model**: sentence-transformers/all-MiniLM-L6-v2  **Corpus**: 5,183 docs  **Queries**: 300

## Metrics

| Metric | Distributed (LanceForge) | Baseline (numpy exhaustive cosine) | Δ |
|---|---|---|---|
| **nDCG@10** | 0.6451 | 0.6451 | **0.0%** |
| **Recall@100** | 0.9250 | 0.9250 | **0.0%** |
| QPS | 49.7 | 2530.1 | -98% |
| Wall (s) | 6.0 | 0.12 | +4995% |

## Findings

✓ **质量零损失**：分布式 IVF_FLAT 检索的 nDCG@10 与 Recall@100 与暴力余弦完全相等（5183 docs 太小，IVF 实际近似全扫）。

✓ **正确性已验证**：在真实嵌入分布（不是合成 Gaussian）下，分布式路径产出的 top-K 排序与 ground truth 完全一致。

⚠ **小 corpus 性能差**：5K 文档下，gRPC 网络往返成本远高于实际计算（300 queries × ~20ms RPC > 0.12s 暴力 numpy）。这是分布式系统在小规模下的预期行为，与 BENCHMARK.md 的 100K 拐点结论一致。

⚠ **真用户应该**：
- > 100K docs 时 LanceForge 才比单机 lancedb 有性能优势
- 永远启用 IVF 索引（CreateIndex）；否则全表扫
- 客户端 batch 多个 query 而不是逐个 RPC

## Bug Found and Fixed

第一次跑 nDCG@10 = 0.0021（异常低）。根因：`_distance` 列的 sign 约定因 metric 类型而异，直接传给 BEIR 评估器导致排序反转。修复：用 retrieval rank 作为 score，跨 metric 通用。

## Repro

```bash
# 启动 LanceForge cluster (按 docs/QUICKSTART.md)
python3 examples/rag_beir/run_beir_scifact.py
```
