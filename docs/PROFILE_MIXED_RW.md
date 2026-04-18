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

## 七、B2.2 设计方向（基于实验数据更新）

实验更新 H1 排名后，设计方向**重排**：

1. **写读物理分离（当前数据下最值钱）** ← 从 #3 升为 #1
   - 实验表明即使 cache 稳定，写路径本身仍严重阻塞读（-48% @ 10 QPS, -97% @ 50 QPS）
   - 把"写只打 primary worker，reader 只打 replica worker"做成正式调度策略
   - 不要求 cache 重写，不动 lancedb
   - 预期效果：reader 端完全隔离写压力，QPS 接近 read-only 基线
   - 工程量：~1 周（scheduler 路由逻辑 + 配置 + 文档）

2. **Cache key 去 version 化（中长期正道）**
   - IVF 索引重建频率低，即使数据变也不动索引
   - 配套 delta-search 对增量 fragments，然后合并
   - 复杂度高，2-3 周工程量
   - 但它解决的问题 H1 只贡献 25%，ROI 不如 #1

3. **Fragment-level 失效（依赖 lancedb API）**
   - 风险大，放后面
   - 需要 lancedb 暴露"写影响哪些 fragments"的信息

**当前判断**：B2.2 先落 #1（读写物理分离路由）；同步给 H2/H6 加 tracing 做根因定位（§6.2）；#2 暂不启动，ROI 不划算。

**短期 mitigation（docs）**：让用户在 LIMITATIONS §11 的基础上知道可以调 `cache.read_consistency_secs` 拿回部分 QPS（10 QPS 写场景 +25%）。已反映在 LIMITATIONS §11。

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
