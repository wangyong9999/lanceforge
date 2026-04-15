# Phase 17 Summary (生成中)

**Status**: 2026-04-15，持续更新中。完整表格在 BENCHMARK.md。

## Real bugs surfaced by benchmark & fixed

| # | 触发 | 根因 | 修复 |
|---|---|---|---|
| 1 | 第一次 CreateTable 10K × 128d | gRPC 默认 4 MiB decoding cap | coord+worker 双端提高到 `max_response_bytes + 16 MiB`；移除冗余 interceptor；client pool 256 MiB |
| 2 | 1M × 128d CreateTable | Client send cap 256 MiB < 520 MB payload | benchmark 增加分块 ingest (CreateTable + AddRows 循环)，且默认 `max_response_bytes` 提升到 256 MiB |
| 3 | 1M 查询超时 | CreateTable 未自动建索引 → 全表扫 | benchmark 在 ingest 后显式 `CreateIndex`（生产建议同样做） |
| 4 | conc=50 QPS 崩溃（先误判为服务端） | **客户端**单 gRPC channel 多线程 HTTP/2 stream 序列化 | 修 benchmark 每线程一个 stub；文档明确要求客户端做连接池 |
| 5 | 服务端锁争用怀疑 | Worker `execute_query` 持 read lock 跨 async I/O | 改为锁内快照、释放锁后执行（即使非 P0 也是好实践） |

## Docs 产出

- `docs/QUICKSTART.md` — 10 分钟首次跑通，含 5 个高频陷阱
- `docs/ARCHITECTURE.md` — 架构总览 + 为什么不做 2PC/quorum/resync 的反思
- `docs/CONFIG.md` — 每个 YAML 字段 + 调优建议 + 生产示例
- `docs/DEPLOYMENT.md` — Helm chart 部署、扩缩容、HA、升级
- `docs/OPERATIONS.md` — SRE runbook：扩容、缩容、迁移、备份、升级
- `docs/TROUBLESHOOTING.md` — 错误码 flat lookup 表
- `docs/BENCHMARK.md` — 实测数据 + 观测到的瓶颈 + 下一步清单
- `docs/LIMITATIONS.md` — 11 条诚实边界

## 第一个真场景

`examples/rag_beir/` — BEIR SciFact RAG 流水线：
- 5,183 docs + 300 queries
- embed: sentence-transformers/all-MiniLM-L6-v2 (384d)
- 对比 LanceForge 分布式 vs 单机 lancedb baseline
- 评估 nDCG@10, Recall@100

## Benchmark 真实数据

### Smoke (10K × 128d)
- 1 shard conc=10: **1878 QPS, P99=9.6 ms, recall=1.000**
- 2 shard conc=10: 1603 QPS (scatter-gather 开销 > 收益)

### Mid (100K × 128d) — 修掉客户端 channel bug 后
- 1 shard conc=10: **1876 QPS, P99=6.5 ms**
- 4 shard conc=50: **1295 QPS, P99=105 ms** (修 client bug 后提升 10 倍)
- 1 shard conc=50: 305 QPS (单 worker 并发上限，分布式才能扛住 50 并发)

### Large (1M × 128d) — 待完成
(1M 矩阵运行中，完成后填入)

## 下一步真实优先级

按 **benchmark 和 RAG 实际暴露的问题** 排序：

1. ⏳ 跑 RAG E2E 看 recall 在真实分布下是否下降
2. ⏳ 从 1M benchmark 看分片拐点在哪
3. Add hash-by-PK（如果 RAG 暴露 Upsert 一致性问题）
4. Python SDK 连接池 helper（封装 Phase 17 发现的陷阱）
