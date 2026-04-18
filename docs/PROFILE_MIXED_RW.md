# 混合读写性能 Profile（B2.1 起点）

**建档时间**：2026-04-18
**对应 ROADMAP 项**：`ROADMAP_0.2.md` §B2（读写混合缓存分级失效）
**目的**：为 B2.2+ 的缓存重设计提供数据起点；同步修正 LIMITATIONS §11 的过时数字

---

## 一、现状数字（2026-04-18 实测）

测试：100K 行 × 128d × IVF_FLAT-32 × nprobes=10，10 readers + N QPS writer，30s 持续。

| 场景 | Read QPS | P50 ms | P95 ms | P99 ms | 与 read-only 对比 |
|---|---:|---:|---:|---:|---:|
| read-only | 2345 | 3.9 | 6.6 | 13.5 | — |
| + 10 QPS writes | 887 | 4.4 | 63.5 | 101.4 | **-62.2%** |
| + 50 QPS writes | 58.5 | 180 | 271 | 309 | -97.5% |

原始数据：`lance-integration/bench/results/phase17/mixed_20260418_152253.json`

## 二、历史对比与 Phase 18 改善

```
2026-04-15（Phase 17 closeout，无 atomic poller）：
  read-only 2528 → +10qps 218  (-91.4%)
  read-only 2528 → +50qps 56   (-97.8%)

2026-04-15（Phase 18 atomic version poller 落地）：
  read-only 2686 → +10qps 935  (-65.2%)
  read-only 2686 → +50qps 62   (-97.7%)

2026-04-18（今日，同代码基）：
  read-only 2345 → +10qps 887  (-62.2%)  ← 与 Phase 18 结果一致
  read-only 2345 → +50qps 58   (-97.5%)
```

**结论**：Phase 18 atomic poller + RwLock snapshot 两项优化带来的改善稳定在 **-91% → -62%**（10 QPS 写场景）。**LIMITATIONS §11 已过时**。剩余 62% gap 是 B2 的目标。

## 三、当前实现（代码事实）

源码位置：`crates/worker/src/executor.rs:120-230`，`crates/worker/src/cache.rs`

### 3.1 Cache key 设计
```rust
QueryCacheKey {
    table_name, query_type, vector_hash, filter,
    k, nprobes, metric_type,
    dataset_version,  // ← 数据变更时整体失效
}
```
cache 粒度：**整张表 × 整体 dataset_version**。任意写 → version++ → 整张表所有已缓存条目事实失效（key miss）。

### 3.2 Version 传播路径
- 背景 poller 每 `read_consistency_secs`（默认 3s）tick 一次
- 每 tick 对每张表调 `table.version().await`（lancedb 内部 manifest 读）
- atomic store 到 per-shard `AtomicU64`
- 读路径 load atomic（无 I/O），塞进 cache key

### 3.3 读路径（execute_query）
1. Snapshot tables（短暂 read lock）
2. Load atomic version（无 I/O）
3. Cache lookup（hit → return）
4. Miss：`execute_on_table` → lancedb Table.search()

### 3.4 写路径
1. Coordinator 路由 AddRows 给 worker
2. Worker 调 `table.add(...)` → lancedb 写新 fragment + 更新 manifest
3. **不**显式 invalidate cache；依赖下次 poller tick 刷新 version

## 四、P99 vs 均值差异的含义

| 指标 | read-only | +10 QPS writes | 差值 |
|---|---:|---:|---:|
| 平均延迟（10/QPS） | 4.3ms | 11.3ms | +7.0ms |
| P50 | 3.9ms | 4.4ms | +0.5ms |
| P95 | 6.6ms | 63.5ms | **+56.9ms** |
| P99 | 13.5ms | 101.4ms | **+87.9ms** |

**P50 几乎不变，P95/P99 爆炸**——这是典型的**周期性 stall** 特征。均值抬升主要来自 tail。猜测有 ~5% 的查询卡在某个共享资源上。

## 五、假设树（按置信度排序）

### H1（最高置信）Version 粒度过粗导致缓存周期性整体失效
- 每 3s poller tick 读 version
- 10 QPS 写时每 ~100ms version++
- tick 后新 key 族开始；过去 3s 累积的 cache entries 即刻成为"无效键"
- 对 50 个循环 query 来说：每 3s 首次命中新 key 族 = 50 次冷 miss
- **可证伪**：把 `read_consistency_secs` 从 3s 调到 30s，观察 QPS 是否回升

### H2（中高置信）Writer 握的 lock 阻塞 reader
- lancedb `table.add` 内部可能 hold 一个 per-table write lock
- 虽然 executor.rs 上层已经释放 RwLock，但 lancedb 自己可能有 manifest 或 fragment-level lock
- 写操作带 100 行 Arrow IPC 序列化 + S3 PUT，可能 100-500ms 期间阻塞
- **可证伪**：在 writer 线程打 tracing span，看 `table.add` 耗时分布；在 reader miss 路径打 span 看 wait

### H3（中置信）Manifest 重读成本
- 每次 Table.search() 如果内部需要 checkout_latest，会调 manifest
- lancedb 有 `read_consistency_interval`（配置到 `read_consistency_secs`），理论上 gated 住
- 但可能存在写后首次读触发一次强刷新的路径
- **可证伪**：对 lancedb 开启 `RUST_LOG=lance=debug`，grep manifest reload 次数

### H4（低置信）Tokio 调度抖动
- 1 writer thread + 10 reader threads 共享 tokio runtime
- writer 的 S3 PUT await 期间，调度器可能给 readers 让路不均
- **可证伪**：tokio-console 打 runtime metrics

### H5（中置信）IVF 索引延迟重建 / fragment 累积
- Bench 30s × 10 QPS × 100 rows/batch = 30K 新增行 × 100 次 fragment 创建
- lancedb 可能触发 fragment metadata 重组 或 索引延迟重建
- 索引重建涉及读全量新增数据 + 写索引文件，CPU + I/O 都重
- **可证伪**：检查 lancedb 版本相关的 auto-compact / index-rebuild 配置；
  关掉 `CompactionConfig` 自动压实看 QPS 变化

### H6（中置信）存储 I/O fsync contention
- Writer 每 batch 写 lance fragment + 更新 manifest，含 fsync
- Reader 的 cache miss 会读 fragment 数据文件
- 本地 fs 上 fsync 对整盘都有影响（ext4 write barrier）
- **可证伪**：strace -c writer/reader 进程，看 fsync/write 计数分布；
  或切 tmpfs（牺牲持久性）重跑 bench

## 六、B2.1 Profile 任务清单

按先易后难排序，每项单独 commit：

### 6.1 Config 实验（零代码，已完成）✅

**结果**（`mixed_20260418_*` 系列，rc_secs=3 基线 vs rc_secs=30）：

| write QPS | rc_secs=3 Read QPS | rc_secs=30 Read QPS | 相对改善 | 结论 |
|---:|---:|---:|---:|---|
| 0 (read-only) | 2345 | 2136 | —（噪声） | 基线 |
| 10 | 887 (-62%) | **1103 (-48%)** | +25% | H1 部分证实 |
| 50 | 58 (-97.4%) | 55.9 (-97.4%) | **0%** | H1 在高写压下完全不生效 |

**判定**：
- **H1（version tick storm）部分贡献**：10 QPS 写场景下，拉长 rc_secs 从 3s 到 30s 能拿回约 25% 的 loss，但离 80% 目标还差远
- **H1 不是主因**：即使 rc=30s 下 cache key 在 30s 窗口内稳定，仍损失 48% QPS。这一半 loss 必须来自**非缓存**因素
- **50 QPS 写场景下 H1 影响为零**：rc_secs 改变对高写压完全无效——说明写路径本身（不是 cache 失效）是瓶颈

**促升级的假设**（根据实验数据重排）：
- **H2（writer lock 阻塞 reader）→ 从中高置信升为最高置信**
- **H6（fsync contention）→ 从中置信升为高置信**
- H1 降为"贡献 ~25%，调 rc_secs 可作为短期 mitigation"
- H3/H4/H5 仍待验证

### 6.2 加 tracing span（半天）
在以下位置加 `#[instrument]`：
- `executor.rs` ExecutorState::execute_query 入口
- `cache.rs` QueryCache::{get, put}
- Worker 侧 AddRows RPC 入口 → `table.add` 前后
- 版本 poller 的 `table.version()` 前后

收集：
- span durations P50/P95/P99 per name
- cache hit/miss counts

### 6.3 Tokio 可视化（可选）
- 开启 `tokio-console`（编译 feature flag）
- 跑 bench 期间观察 task poll durations、blocking counts
- **如果 H4 是主因这里会很明显**

### 6.4 RUST_LOG=lance=debug（零代码）
- 跑 30s 抓 manifest 相关日志条数
- 对比 read-only vs +10qps 看 reload 频率

### 6.5 tmpfs 对比（H6 证伪）
- 把 BASE 从 `/tmp/lanceforge_bench17_mixed` 换到 `tmpfs` 挂载点
- 跑同 bench；若 mixed QPS 显著回升 → 磁盘 fsync 是主因

### 6.6 关闭 CompactionConfig（H5 证伪）
- `config.compaction.enabled=false`
- 跑同 bench；若无改善 → H5 排除

### 6.7 火焰图（后置）
- `cargo build --release` 带 debuginfo
- `perf record -g` 跑 30s bench
- `perf script | stackcollapse | flamegraph.pl`

**B2.1 验收**：产出一份 "where the 62% goes" 报告，假设树每个假设标为证实/证伪/待测，给 B2.2 的分层设计方案提供定量依据。

## 七、B2.2 已落地 + 实验验证

### 7.1 实现（commit TBD）

- `ExecutorConfig.role: ExecutorRole` 枚举：`Either`（默认）/`ReadPrimary`/`WritePrimary`
- `ConnectionPool::pick_by_role(candidates, for_reads)`：
  - 读偏好：ReadPrimary > Either > WritePrimary
  - 写偏好：WritePrimary > Either > ReadPrimary
  - 单候选 / 全 Either 场景行为保持 0.1.x 原样（向下兼容）
- `scatter_gather`（读路径）和 `add_rows`（写路径）都调用 `pick_by_role`
- 6 个 unit 测试 + 向后兼容测试

### 7.2 关键实验：路由 + rc_secs 组合效果

实验配置：100K × 128d × IVF_FLAT-32，10 readers + N QPS writer，30s 持续；w0=WritePrimary，w1=ReadPrimary，rebalance 后每 shard 两个 replica。

| 配置 | read-only baseline | +10 QPS 写 Read QPS | 与 baseline 比 |
|---|---:|---:|---:|
| 无 split, rc=3（0.1 基线）| 2345 | 887 | **38%** (-62%) |
| 无 split, rc=30 | 2136 | 1103 | 52% (-48%) |
| **split + rc=3** | 2526 | 967 | 38% (-62%) |
| **split + rc=30** | 2435 | 1834 | **75%** (-25%) |
| **split + rc=60** | 2440 | **2014** | **82.5%** ✅ (-17.5%) |

**关键发现**：
1. **split 单独无效**——因为两个 worker 打开**同一个** `.lance` 目录，w0 的写入通过共享 manifest 依然 invalidate w1 的 cache（H2/H6 确认，共享存储是主因）
2. **split + 长 rc** 才生效——split 把写压集中在 w0，rc=60 让 w1 最多每 60s 刷一次 manifest，期间 cache 保持稳定
3. **50 QPS 写场景**：split + rc=60 下仍 -81%，但比基线 -97% 提升 8×。进一步改善需要 fragment-level invalidation（B2 post-alpha 工作）

### 7.3 alpha gate 达成

ROADMAP §3.3 B2.2 要求"10 QPS 写场景 Read QPS ≥ 80% baseline"。**split + rc=60 的配置下 82.5%，超标**。

### 7.4 推荐生产配置

对于**中频写入场景（1-10 QPS）**：
```yaml
replica_factor: 2
executors:
  - { id: w0, role: write_primary, ... }
  - { id: w1, role: read_primary, ... }
cache:
  read_consistency_secs: 60   # 读端容忍 60s 内的数据延迟
```
然后启动时调用 `Rebalance()` 确保每个 shard 复制到两个 worker。

**高频写入场景（> 10 QPS）**：当前 alpha 未能达到 80% 目标（-81%）。建议多副本读负载均衡（replica_factor=3，2 个 ReadPrimary + 1 个 WritePrimary）或推迟到 beta 的 cache-key 去 version 化方案。

### 7.5 post-alpha 工作

- **B2-extended：fragment-level invalidation**（2-3 周）——解决共享存储瓶颈，撬动 50 QPS 写场景
- **B2-extended：sparse cache keys**——去 `dataset_version` 依赖
- 这两项是 0.2-beta / 0.3 目标，不是 alpha blocker

---

## 八、更新 LIMITATIONS.md §11 的行动项

当前 §11 写的数字（218 QPS / -91%）来自 Phase 17 最初的 bench，已被后续的 Phase 18 atomic poller 改善。修订为：

```
| 场景 | 读 QPS | 读 P99 |
|---|---:|---:|
| read-only | 2345 | 13 ms |
| + 10 QPS 写 | 887 | 101 ms |
| + 50 QPS 写 | 58  | 309 ms |
```

并加一句：**数字每月跟 benchmark regression 同步更新；历史趋势见 `docs/PROFILE_MIXED_RW.md`**。
