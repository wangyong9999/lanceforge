# 关键路径覆盖率矩阵

**建档时间**：2026-04-18（0.2.0-pre.1 基线）；2026-04-21 更新（beta.5 预备）
**对应 ROADMAP 项**：`ROADMAP_0.2.md` §B4
**工具**：`cargo llvm-cov 0.8.5`
**测量范围**：`--lib` only（lib 单元测试）。完整 `lib + integration` 测量见 CI artifact（gate 70% line coverage）。

## 一、4 个关键路径模块——**beta.5 ratchet**

| 模块 | Lines 覆盖 | Regions 覆盖 | Lines Missed | 评级 |
|---|---:|---:|---:|---|
| `coordinator/src/scatter_gather.rs` | **97.19%** | 96.68% | 19 / 676 | 🟢 目标达到 |
| `coordinator/src/merge.rs`（全局 TopK）| 89.69% | 89.89% | 23 / 223 | 🟢 目标达到 |
| `meta/src/store.rs`（MetaStore CAS）| 72.95% | 74.85% | 135 / 499 | 🟡 接近目标 |
| `coordinator/src/connection_pool.rs` | **94.24%** | 94.81% | 42 / 729 | 🟢 目标达到 |

**整体 coordinator lib coverage**：77.10% line（60.43% → 77.10%，+16.7pp）。

### beta.5 补课过程（本轮工作）

**目标**：把两个 🔴 模块拉到 ≥80%。结果都到 >94%，顺带发现一个 **production 级 latent deadlock**。

- 新增 `crates/coordinator/src/test_support.rs`：可配置的 tonic mock worker（LanceExecutorService 全 11 方法），支持 SearchBehavior = {Ok, Empty, AppError, GrpcError, GarbageIpc, Slow} × HealthBehavior = {Ok, Err, Slow}，运行时通过 `Arc<RwLock<…>>` 热切换。
- `scatter_gather.rs` 新增 12 个 end-to-end 测试覆盖：table-not-found / all-unhealthy / all-failed / partial-failure warning / IPC decode error / empty responses / merge-by-distance vs merge-by-score / outer timeout wrapper / trace_id injection / straggler warn 分支。
- `connection_pool.rs` 新增 6 个 live-network 测试覆盖：connect_all 成功 / unreachable endpoint 跳过 / health_check_loop unhealthy 翻转 / remove_threshold 移除 / shutdown 停环 / LoadShard 恢复。
- **Bug 发现 → 修复**：`health_check_loop` 的 Healthy 分支在 shard recovery 时持有 `workers` 写锁去调用 `self.get_healthy_client()`（读锁），tokio RwLock 是 write-preferring + 非 reentrant → 死锁。生产条件：shard_state 已接入 + 某个 worker 重启后 shard_names 少于分配。修复：先释放写锁再进入 recovery。
- **行为纪要**（未改代码，仅测试中 pin 住现状）：`remove_threshold > unhealthy_threshold` 时移除分支实际不可达——worker 一旦翻到 !healthy，下一 tick 走 reconnect 路径而非 ping，失败计数不再增长。生产默认 (3, 5) 下永不触发。留作后续修复：reconnect 失败也计入 consecutive_failures。

## 二、缺口分析

### 🔴 `scatter_gather.rs` (49.80%)
- 大量 IO 路径只在 integration tests / E2E 下才被触发（worker 真正响应 gRPC 的部分）
- lib 测试覆盖的是纯逻辑（merge / pruning）+ 错误分支
- **lib-only 视角下不可能到 85%**，需要 integration tests 配合
- **B4 真正达标路径**：lib + integration 合并测量（CI 的 70% gate 合并后的数字）

### 🔴 `connection_pool.rs` (41.93%)
- 类似上条：真正发 gRPC 的路径不可能在 lib 单测覆盖
- 本 session 新增 A1 修复的 stale-channel 代码（warm-up 逻辑）
- lib 测试可以扩充的部分：endpoint 构建、状态机转换、shard URI registry

### 🟡 `meta/src/store.rs` (72.95%)
- 已经接近 85%
- 本 session B3.2 新增的 schema_version 相关代码已覆盖
- 未覆盖的：S3 backend 的 refresh 错误路径、CAS 冲突 race

### 🟢 `merge.rs` (89.69%)
- 目标达成
- 纯计算密集的模块，天然适合 lib 测试
- 剩余未覆盖主要是 panic unreachable

## 三、B4 alpha gate 验收

按 `ROADMAP_0.2.md` §3.3 B4-min：
- ✅ 4 模块已测量
- ✅ 数据入档（本文件）
- ✅ HTML / lcov artifact 可通过 `cargo llvm-cov --html` 生成

**不满足的是**："目标 ≥85%"——当前只有 1/4 模块达到。作为 alpha **非 blocker**，已在 §3.3 明确。

## 四、beta gate（提前登记）

`ROADMAP_0.2.md` §3.3 beta 要求"ratchet up"覆盖率。beta 前要做：

1. **scatter_gather**：写 integration-style 测试（启动 mock worker server，走完整 gRPC 路径）。预期 lib+integration 合并能到 75-85%
2. **connection_pool**：同上；另补 unit 测试覆盖错误分支（DNS 失败、endpoint 构建错误、TLS 错误）
3. **meta/store.rs**：补 S3 错误路径（用 object_store mock 注入 throttle / network error）
4. **CI ratchet**：把 70% gate 提高到 75%（乘用实际整合后数字）

## 五、工具复用

### 本地生成 HTML 报告
```bash
cargo llvm-cov --html --lib \
  -p lance-distributed-common \
  -p lance-distributed-coordinator \
  -p lance-distributed-meta \
  -p lance-distributed-worker \
  --output-dir target/cov-html
# 打开 target/cov-html/index.html
```

### 单模块深入
```bash
cargo llvm-cov --summary-only --lib -p lance-distributed-coordinator
# 看 coordinator 下每个源文件的数字
```

### 本文件更新节奏
- 每次 alpha / beta / GA 前重跑
- 大模块 refactor 后立即重跑并 commit 新基线
- 目标：数字**只升不降**。降的 PR 必须同步补测

## 六、已知限制（本次测量）

- **lib-only** 视角：不反映 integration / E2E 覆盖。真正的"代码被客户使用场景测过"的比例 CI 合并数字更接近（~70%）
- **分支覆盖率**：llvm-cov 当前版本报告 0 个分支——实际上是 **region coverage**（类似 branch 的概念，但粒度更细）。遵循 region coverage 数字
- **generated code / bin/main.rs**：CI 脚本用 `--ignore-filename-regex` 排除。本次测量未排除，所以略低估
