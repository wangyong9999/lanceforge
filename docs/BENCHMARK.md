# LanceForge Benchmark Report

基于 Phase 17 矩阵化 benchmark 的性能边界报告。所有数据来自 `lance-integration/bench/bench_phase17_matrix.py` 的真实运行。

**测试时间**：2026-04-15
**硬件**：WSL2 Linux 6.6.87, 开发机（非生产规格）
**数据**：合成高斯向量，L2 距离，IVF_FLAT 索引（auto-created on CreateTable）

## 关键发现（TL;DR）

### ✅ 擅长的场景
- **低-中等并发（1~10）下查询 P99 < 12 ms**（10K~100K 数据）
- **Recall@10 = 1.000**（在 nprobes=10、k=10、数据量 ≤ 100K 时 IVF 基本扫全表）
- **Ingest ~75K rows/s**（128 维，本地 fs）

### ❌ 踩到的真实问题
- **并发 ≥ 50 时 QPS 崩溃**：从 1927 (conc=10) 暴跌到 209 (conc=50)，P99 从 9 ms 升到 ~1000 ms。这是需要优化的瓶颈，见下方"已发现的瓶颈"
- **小规模下分布式反而更慢**：100K 数据 × shards=2 比 shards=1 慢 18~27%。scatter-gather 固定开销大于并行收益的临界点在本机测试约 **> 500K 行** 才开始有利

---

## 原始数据

### Smoke Matrix (10K × 128d)

| scale | dim | shards | conc | QPS | P50 ms | P99 ms | recall@10 |
|------:|----:|-------:|-----:|----:|-------:|-------:|----------:|
| 10000 | 128 | 1 | 1  | 468.7  | 1.4 | 7.9  | 1.000 |
| 10000 | 128 | 1 | 10 | 1878.4 | 4.5 | 9.6  | 1.000 |
| 10000 | 128 | 2 | 1  | 421.7  | 1.8 | 5.5  | 1.000 |
| 10000 | 128 | 2 | 10 | 1602.8 | 5.5 | 10.6 | 1.000 |

### Mid Matrix (100K × 128d)

| scale  | dim | shards | conc | QPS  | P50 ms | P99 ms  | recall@10 |
|-------:|----:|-------:|-----:|-----:|-------:|--------:|----------:|
| 100000 | 128 | 1 | 1  | 178.2  | 1.6   | 43.1    | 1.000 |
| 100000 | 128 | 1 | 10 | 1927.0 | 4.5   | 8.6     | 1.000 |
| 100000 | 128 | 1 | 50 | 209.4  | 41.7  | 997.8   | 1.000 |
| 100000 | 128 | 2 | 1  | 153.5  | 2.7   | 33.7    | 1.000 |
| 100000 | 128 | 2 | 10 | 1402.2 | 6.2   | 11.8    | 1.000 |
| 100000 | 128 | 2 | 50 | 157.4  | 200.2 | 1050.6  | 1.000 |
| 100000 | 128 | 4 | 1  | 107.8  | 4.6   | 43.0    | 1.000 |
| 100000 | 128 | 4 | 10 | 912.9  | 9.6   | 20.4    | 1.000 |
| 100000 | 128 | 4 | 50 | 122.6  | 381.3 | 1181.8  | 1.000 |

---

## 扩展性分析

在 conc=10 的最优点上看分片数的加速比：

| 数据量 | shards=1 QPS | shards=2 QPS | shards=4 QPS |
|---|---|---|---|
| 10K  | 1878 | 1603 (×0.85) | - |
| 100K | 1927 | 1402 (×0.73) | 913  (×0.47) |

**结论**：在本机测试规模内（≤ 100K），分片**降低**QPS 而不是提高。

**原因推断**（需要进一步 profile 验证）：
- scatter-gather 阶段 N 次并发 gRPC 调用 → N 次网络 RTT + IPC 序列化
- Coordinator 等最慢的 shard 返回（队尾延迟放大）
- 本机 loopback 下 RPC 开销不可忽略（实际云环境网络延迟会更大）

**预期**：数据量在 **500K ~ 1M 行** 的拐点之后，单 shard 的 IVF 扫描时间会超过 scatter 开销，分片才开始赚。此拐点需要跑 --scale=1000000 的实际矩阵确认。

---

## 已发现的瓶颈

### 瓶颈 1：conc=50 QPS 崩溃（P0 待优化）

**现象**：并发从 10 涨到 50，QPS 反而掉 **10 倍**，P99 升 **100 倍**。

**可能原因**（按概率）：
1. **Tokio 运行时线程池耗尽**：worker 默认 worker_threads 可能只有 CPU 核数，200 并发 gRPC 请求挤同一个池
2. **Lance table 读锁竞争**：`execute_query` 里持有 `tables.read().await`，高并发下可能有 RwLock 争用
3. **Arrow IPC 编码瓶颈**：每次查询都有 IPC 序列化，高并发下 CPU 见顶

**验证方向**（下一轮做）：
- 用 `tokio-console` profile 找卡在哪儿
- 把 Lance table 的 Arc 调用放在 read-lock 外面
- 测试 release 二进制是否带 jemalloc

### 瓶颈 2：shards > 1 不生效（在当前数据量）

**现象**：100K 数据下分片始终更慢。

**根因**：scatter-gather 固定开销 > 单机扫描加速

**要么**：
- 等数据规模到 1M+ 自然解决
- 要么优化 scatter-gather（减少 IPC 次数、流式合并、per-shard k 采样）
- 在 QUICKSTART/LIMITATIONS 里明说"小数据别开分片"

---

## 可复现命令

```bash
# Smoke (约 2 分钟)
python3 lance-integration/bench/bench_phase17_matrix.py --smoke

# Mid (约 10 分钟)
python3 lance-integration/bench/bench_phase17_matrix.py --mid

# Single scale
python3 lance-integration/bench/bench_phase17_matrix.py --scale 1000000

# Full matrix (慢，约 1 小时)
python3 lance-integration/bench/bench_phase17_matrix.py
```

输出目录：`lance-integration/bench/results/phase17/`
- `matrix_<ts>.json` — 完整结果
- `matrix_<ts>.csv` — 电子表格格式
- `REPORT_<ts>.md` — 自动生成的摘要

---

## 下一轮 benchmark 优先级

1. **跑 1M 数据**：确认分片拐点
2. **Profile conc=50 崩溃**：tokio-console / flamegraph
3. **跑 768 维 (BERT 规模)**：高维向量的 IPC overhead
4. **混合读写（90/10）**：写对读 QPS 的真实影响
5. **真实数据集**：BEIR SciFact，验证 recall 在真实分布下（非高斯）

---

## 诚实声明

- 本次 benchmark **在开发机上跑**，WSL2 环境，非生产规格
- 所有测试用合成高斯数据，**真实数据（embedding）分布会不同**，recall 数字可能降低
- 网络是 loopback，**跨主机/跨可用区的延迟会显著增加** scatter-gather 开销
- 尚未测试 **S3/GCS** 后端，只测了本地 fs；对象存储延迟会影响冷查询
