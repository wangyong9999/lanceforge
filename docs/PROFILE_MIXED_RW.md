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

## 六、B2.1 Profile 任务清单

按先易后难排序，每项单独 commit：

### 6.1 Config 实验（零代码，1 小时）
- 把 `cache.read_consistency_secs` 依次设 1 / 3 / 10 / 30，跑 bench，看 +10qps 下 Read QPS 曲线
- **H1 的直接验证**。如果 30s 下 QPS 回到 >80%，说明 H1 是主因
- 如果 QPS 几乎不变，H1 排除

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

### 6.5 火焰图（后置）
- `cargo build --release` 带 debuginfo
- `perf record -g` 跑 30s bench
- `perf script | stackcollapse | flamegraph.pl`

**B2.1 验收**：产出一份 "where the 62% goes" 报告，假设树每个假设标为证实/证伪/待测，给 B2.2 的分层设计方案提供定量依据。

## 七、B2.2 预判（profile 前先写下来，事后校对）

基于 H1 + 当前 cache 结构，**当前最可能的设计方向**：

1. **Cache key 去 version 化，改按 "query + IVF snapshot" 索引**
   - IVF 索引重建频率低（只在 CreateIndex 时），即使数据变也不动索引
   - 命中 cache 返回的结果会 miss 掉增量 fragments 的 top-K 候选
   - 需要配套：对增量 fragments 做 delta-search 并合并
   - 复杂度高，但是正路

2. **Fragment-level 失效**
   - 维护 cache → fragment_id set 反向索引
   - 写入只脏受影响 fragment 的 cache 条目
   - 写入路径需要返回 "我写入了哪些 fragment"
   - 难点：lancedb 未必暴露这个信息

3. **写读物理分离（最简单，推荐作为 escape hatch）**
   - replica_factor=2 → 一 worker 标 read-only，另一 worker 标 write-primary
   - readers 只打 read-only worker，其 version 按 read_consistency_secs 懒更新
   - write-primary 自己不承接读
   - 不需要代码改动 cache 逻辑，配置层面解决

**现在的判断**：B2.2 先做 #3（配置层解法，1 周落地），给用户一个立即可用的 escape hatch；同步预研 #1（技术路径最干净，但 2-3 周工程量）。#2 风险大（依赖 lancedb API），放后面。

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
