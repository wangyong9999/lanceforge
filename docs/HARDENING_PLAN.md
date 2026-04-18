# 0.2-alpha → 0.2-beta 硬化与覆盖计划

> **Status (2026-04-18 最终)**：计划内 23 项 + 执行中发现的 H23 全部落地。
> 最终测试：**workspace lib 291 tests pass**（基线 218 → +73），**HA E2E 8/8**。
> 衍生发现 **H24**（k=10000 查询 60% 错误率）已 filed，不在本计划闭环内。
> 可以进 0.2.0-beta.1 gate（§8 beta-ready 定义）。

## 执行记录（commit 索引）

| ID | Item | Commit |
|---|---|---|
| H1 | config validate bounds | `439598a` |
| H2 | write-RPC audit | `4d626dc` |
| H3 | AddRows/Upsert idempotency | `861e437` |
| H4 | JoinSet cancellation | `d6d8b51` |
| H5 | ShardState typed error | `27487d8` |
| H6 | scatter_gather coverage | `1af27d1` |
| H7 | connection_pool coverage | `cb4f762` |
| H8 | MetaStore S3 error paths | `99a1c93` |
| H9 | Python E2E → CI | `60e85a2` |
| H10 | dimension/query_vector consistency | `2e890cc` |
| H11 | DDL boundary coverage | `5e2defc` |
| H12 | 大 k bench + PERF_LARGE_K finding | `e9b4740` |
| H13 | per-shard latency log | `fed72e6` |
| H14 | EtcdShardState shutdown | `03bfeef` |
| H15 | REST server shutdown | `7cbab18` |
| H16 | idiomatic test asserts | `d10398c` |
| H17 | TLS loader + empty-file reject | `d790b69` |
| H18 | ServerVersion parser | `e33f413` |
| H19 | offset+k boundaries | `087ddba` |
| H20 | CI PR/push/nightly tiers | `60e85a2` |
| H21 | SIGTERM + drain timeout | `fa13726` |
| H22 | smoke bench PR gate | `60e85a2` |
| H23 | meta tests → tempdir | `486b5c5` |
| H24 | k=10000 错误率调查 | **filed, not closed** |

---


**建档时间**：2026-04-18
**目的**：在 0.2.0-alpha.1 之后系统性扫清潜在质量问题，作为 0.2-beta 的硬化基础。
**与 ROADMAP 的关系**：这份计划处理 `ROADMAP_0.2.md §1-2` 在 alpha 之后留下的"窄而深"工作；不覆盖 Layer 2/3 的 Strong-Recommend / Moat 功能项。

## 一、范围与原则

**覆盖范围**（in-scope crates）：
- `crates/{common, coordinator, meta, proto, worker}`
- `lance-integration/` 的 Rust + Python 测试层

**不在本计划**：
- `ballista/*`（已 workspace 隔离）
- Layer 2/3 ROADMAP 功能（OTEL / async SDK / sparse vectors / M2 SQL）
- `B2` 混合 RW 的性能工作（`PROFILE_MIXED_RW §8`、`ROADMAP §1.5` 单独跟踪）

**原则**：
1. **诚实优先于速度**：每一个发现必须能追溯到代码或实验证据
2. **测试先于修复**：每个 P0/P1 的修复都要带回归测试
3. **每项独立 commit**：便于 bisect / revert
4. **多轮审视**：每次 commit 前自审 1 次；每执行 5 个条目做一次 checkpoint review

## 二、审计结果（含已修正）

下表是外部 audit agent 扫描 + 我亲自核对后的事实清单。**斜体**的是 audit 的错误结论或漏项，已在 §2.1 修正/补充。

### 2.1 Audit 修正

1. **🟢 取消警告**：`executor.rs:997 decode_vector(&vq).unwrap()` audit 标为"risky production panic"——实际是在 `#[test] fn test_decode_vector_ok` 内。生产代码 (`execute_on_table` 696 + 743) 用 `decode_vector(vq)?` 正确传播 Result。**production 代码中 `.unwrap()` / `.expect()` 实际为零个有风险的**。
2. **🟢 取消警告**：audit 把 "Gap A API key rotation" 列为未修——实际 **B1.2.3 (`8cbcbb3`) 已完成**。audit 用的是 STATELESS_INVARIANTS 旧版标记。
3. **🟡 补充**：audit 遗漏了 RPC cancellation / retry idempotency / 边界值覆盖 / proto 版本协商 等维度。详 §3。

### 2.2 确认的真实发现（按代码位置）

| # | 位置 | 问题 | 严重度 |
|---|---|---|---|
| F1 | `crates/common/src/shard_state.rs` ShardState trait 方法 | 返回 `Result<T, String>`（ad-hoc 字符串错误），其他 crate 用 typed error | 🟡 一致性 |
| F2 | `crates/common/src/config.rs:450-523` validate() | 不校验 `oversample_factor == 0` / `replica_factor == 0` / API key 非空 | 🟡 边界 |
| F3 | `crates/coordinator/src/service.rs` add_rows / delete_rows / upsert_rows | 无 `audit()` 调用（仅 DDL 有） | 🟡 可观测 |
| F4 | `crates/coordinator/src/scatter_gather.rs` | 无 per-shard 延迟日志 | 🟢 改进 |
| F5 | `crates/common/src/shard_state.rs` EtcdShardState watch 循环 | 无 explicit shutdown signal | 🟢 清理 |
| F6 | `crates/coordinator/src/bin/main.rs` REST metrics server | spawn 后无 shutdown 通道 | 🟢 清理 |
| F7 | `crates/coordinator/src/scatter_gather.rs` | 49.80% line coverage（lib-only） | 🔴 覆盖 |
| F8 | `crates/coordinator/src/connection_pool.rs` | 41.93% line coverage（lib-only） | 🔴 覆盖 |
| F9 | `crates/meta/src/store.rs` | 72.95% line coverage；S3 错误路径未测 | 🟡 覆盖 |
| F10 | `.github/workflows/ci.yml` CI 配置 | 大部分 `e2e_*.py` 未进 CI（只 e2e_full_system_test）| 🟡 CI gap |
| F11 | `crates/meta/src/store.rs:713-734` tests | 用 `panic!("...")` 而非 `assert!` | 🟢 test 质量 |
| F12 | `lance-integration/tests/test_edge_cases.rs:310` | 同上 | 🟢 test 质量 |

## 三、超出 Audit 的补充关注项

这些是我对照生产级分布式系统 checklist 额外识别的 gap，audit 没覆盖。

| # | 位置 / 话题 | 问题 | 严重度 |
|---|---|---|---|
| A1 | 所有查询 / 写入 RPC | Client cancel / RPC drop 后 coord 是否 cancel 下游 worker 调用？未测 | 🟡 正确性 |
| A2 | `add_rows` 无 `on_columns` 路径 | client 重试 AddRows 会产生**重复行**。应至少有**请求 ID 幂等窗口** | 🟡 正确性 |
| A3 | `offset + k` 边界 | `validate_offset_k` 保证 ≤ `max_k`，但**未测** `offset == max_k - 1` 这类临界值 | 🟢 覆盖 |
| A4 | `dimension` vs `query_vector.len()` | coord 从 `query_vector.len() / 4` 推导 dimension；如果 client 传 `dimension=5` 但 vector 仅 16 bytes (=4 floats)——行为未测 | 🟡 边界 |
| A5 | Proto 版本协商 | `HealthCheckResponse.server_version` 字段已加（B3.1），但 client 无"如果版本 <X 则降级"的测试 | 🟢 Beta |
| A6 | 跨 crate error → gRPC Status 映射 | `ShardState` 返 `String` → coord 手动映射成 Status——缺少统一层 | 🟡 一致性 |
| A7 | CI wall time | 如果加整套 chaos/soak，单次 CI 从 5min 涨到 30min+。需要分 nightly job 划分 | 🟢 CI 设计 |
| A8 | 数值溢出 | `offset: u32` + `k: u32`，相加可能溢出。`validate_offset_k` 用的算术未审计 | 🟡 正确性 |
| A9 | 大 k 性能 | `max_k=10000`，但 `k=10000` 场景未在 bench 矩阵里 | 🟢 性能 |
| A10 | TLS 错误路径 | cert 过期 / 错误的 CA / 握手失败——行为未测 | 🟡 安全 |
| A11 | 空/零行 CreateTable | `arrow_ipc_data` 是 0 行的合法 IPC——行为未定义 | 🟢 边界 |
| A12 | DropTable 并发 | 两个客户端同时 DropTable 同一表——DDL lock 存在（service.rs:60）但未压测 | 🟢 覆盖 |

## 四、优先级列表

### P0（阻塞 beta；上线前必须解决）

目前**为空**。alpha 已关闭所有 security / panic / correctness 级别的重大问题。

### P1（影响生产稳定性 / 合规 / 可维护性）

| ID | 目标 | 验收 | 估工 |
|---|---|---|---|
| **H1** | F2 配置 validate 加 bounds | `oversample_factor` / `replica_factor` / `api_keys` 空 + validate() 测试 | 0.5h |
| **H2** | F3 写 RPC audit 打通 | `add_rows` / `delete_rows` / `upsert_rows` 都调用 `self.audit()`；有日志验证 | 1h |
| **H3** | A2 AddRows 幂等窗口 | 可选 `request_id` 字段 + coord 侧去重窗口（5min）+ 覆盖测试 | 4h |
| **H4** | A1 RPC cancellation 穿透 | 客户端 drop 后，coord 的 scatter-gather 的 JoinSet 要能 abort 下游 RPC；加单元或集成测试 | 3h |
| **H5** | F1 + A6 ShardState String → typed error | 引入 `ShardStateError` enum；全部 `String` 返回改掉；更新调用者 | 3h |
| **H6** | F7 scatter_gather unit 覆盖补齐 | 覆盖 70%+；error 分支测全（all-fail / partial-fail / prune-to-empty / worker-UNAVAILABLE） | 4h |
| **H7** | F8 connection_pool unit 覆盖补齐 | 覆盖 70%+；加 endpoint 构建错误、TLS 配置错误、健康检查状态机测试 | 3h |
| **H8** | F9 meta/store S3 错误路径测试 | CAS 冲突 / NotFound / 网络超时 / 序列化失败 4 个分支 | 2h |
| **H9** | F10 Python E2E 进 CI | 挑 5 个代表性 E2E 进 CI serial；或 nightly job | 2h |
| **H10** | A4 + A8 边界值测试 | dimension/query_vector 不一致；offset + k 溢出；max_k 临界 | 2h |
| **H11** | A11 + A12 边界 DDL 覆盖 | 0 行 CreateTable；并发 DropTable；duplicate CreateTable | 2h |
| **H12** | A9 bench 加大 k 场景 | bench 矩阵补 k = {10, 100, 1000, 10000}；记录退化拐点 | 2h |

### P2（质量 / 清洁度，可 0.2-beta 或 0.3）

| ID | 目标 | 估工 |
|---|---|---|
| **H13** | F4 per-shard 延迟日志 | 1h |
| **H14** | F5 EtcdShardState shutdown signal | 1h |
| **H15** | F6 REST metrics server shutdown | 0.5h |
| **H16** | F11 + F12 test panic → assert | 0.5h |
| **H17** | A10 TLS 错误路径测试 | 2h |
| **H18** | A5 proto 版本协商冒烟 | 1h |
| **H19** | A3 offset+k 边界显式覆盖 | 0.5h |
| **H20** | A7 CI wall time 分层（PR / nightly）| 2h |
| **H21** | Graceful shutdown drain — SIGTERM coord 有 in-flight 查询时必须等完或快速 fail 不 hang | 2h |
| **H22** | Bench PR gate — smoke mixed bench（短窗口）进 CI PR flow，回归 >15% block merge | 2h |
| **H23** | 测试路径去 /tmp 共享，用 tempfile::tempdir — 消除 meta 并行 flake | 2h |
| **H24** | k=10000 查询 60% 错误率调查（H12 副产品）— 响应大小 / timeout / merge 边界 | 3h |

## 五、执行顺序

### 波次 1 — 快速清理（4h）— ✅ 完成 2026-04-18
最小改动、最高确定性、先压低总体风险面。每项独立 commit。
1. H1 config bounds（0.5h）— ✅ `439598a`
2. H2 write RPC audit（1h）— ✅ `4d626dc`
3. H16 test panic → assert + ExecutorConfig::Default（0.5h）— ✅ `d10398c`
4. H19 offset+k 边界测试（0.5h）— ✅ `087ddba`
5. H15 REST metrics shutdown（0.5h）— ✅ `7cbab18`
6. H14 EtcdShardState shutdown（1h）— ✅ `03bfeef`

**Checkpoint 数字**：workspace lib 测试 218 → **235**（+17）。无回归。

### 波次 2 — 覆盖深度（9h）— ✅ 完成 2026-04-18
动测试代码多、生产代码少。对应 audit 最大痛点。
7. H7 connection_pool 覆盖（3h）— ✅ `cb4f762`（+9 tests, 13→22）
8. H8 meta/store S3 错误路径（2h）— ✅ `99a1c93`（+6 tests, 38→44）
9. H6 scatter_gather helper 覆盖（4h）— ✅ `1af27d1`（+7 tests, 13→20）

**Checkpoint 数字**：workspace lib 测试 235 → **257**（+22）。

**Wave 2 执行中新发现**（加进 §4 的 P1 列表）：
- **H23** meta 测试偶发 flake（`state::tests::test_shard_uri_lifecycle` 等）
  在并行跑 `cargo test --lib` 时随机失败；单独 `-p lance-distributed-meta --lib` 重跑即通过。
  推测：多个测试共享 /tmp 路径或 etcd/FileMetaStore lock 竞争。
  **影响**：CI 偶发 flake；debug 成本上升。
  **修法**：每个测试用 `tempfile::tempdir()` 或 pid+test_name 唯一化路径；审计所有 `/tmp/` 字面量。

### 波次 3 — 一致性与边界（10h）— ✅ correctness 部分完成 2026-04-18
结构性改动，需要跨 crate 协调。

**2026-04-18 收盘重排**（详见 `SESSION_HANDOFF.md` §2.2）：
把 H5（内部 API 破坏性改动）**延后到 Wave 3 末尾**，先处理 correctness 类。

10. H10 边界值测试（2h）— ✅ `2e890cc`（dimension vs query_vector 一致性，+7 tests）
11. H11 DDL 边界覆盖（2h）— ✅ `5e2defc`（split_batch 0-row/1-row/boundary，+6 tests）
12. H21 Graceful shutdown drain（2h）— ✅ `fa13726`（SIGTERM + drain timeout + parallel bg cleanup）
13. H4 RPC cancellation（3h）— ✅ `d6d8b51`（3 RPCs → JoinSet；create_table 保留 + 文档化）
14. H13 Per-shard 延迟日志（1h，P2 提前）— ✅ `fed72e6`（straggler detect at 2×median）
15. H18 Proto 版本协商（1h，P2 提前）— ✅ `e33f413`（ServerVersion parser + at_least, +11 tests）
16. H3 AddRows 幂等（4h，从 Wave 4 提前） — ⏳ 下次 session
17. H5 ShardState typed error（3h，破坏性改动）— ⏳ 下次 session

**Checkpoint 数字**：workspace lib 测试 269 → **280**（+11 from this session）。

本 session 完成 H10/H11/H21/H4/H13/H18 共 6 项；叠加上 session 的 Wave 1+2+H23，累计 16/23 硬化项。剩余 7 项 ~17h。

### 波次 4+ — 剩余 7 项终局（2026-04-18 重审优化）

> **重审理由**：H13/H18/H21 已被拉到 Wave 3；剩余项重新按**依赖图 × 价值**排序，而不是按原计划的机械波次。

| # | Item | 原估 | 依赖 | 风险 | 备注 |
|---|---|---:|---|---|---|
| 17 | **H3** AddRows 幂等窗口 | 4h | 无 | 🟡 proto 改动；需设计决策 | 最大一块；闭合 H4 create_table 的 rollback 尾巴 |
| 18 | **H12** 大 k bench | 2h | 无 | 🟢 仅数据，无代码 | 可复用 `bench_phase17_matrix.py` |
| 19 | **H17** TLS 错误路径 | 2h | 无 | 🟢 需要 test fixtures（self-signed cert）| 独立模块 |
| 20 | **H5** ShardState typed error | 3h | 无 | 🔴 内部 API breaking | 最后做，风险隔离 |
| 21 | **H9** Python E2E → CI | 2h | 无 | 🟡 CI 时长可能翻倍 | 选 2-3 个快的进 PR flow |
| 22 | **H20** CI PR/nightly 分层 | 2h | H9 | 🟢 纯 ci.yml | 先有 H9 内容再决定分层 |
| 23 | **H22** Bench PR gate | 2h | H20 | 🟡 需要 baseline fixture | 回归 > 15% block |

**执行顺序（按依赖 + 风险）**：
- **Round A**（并行独立）：H3（最大）、H12（最快）
- **Round B**（风险隔离）：H17、H5
- **Round C**（CI 串行链）：H9 → H20 → H22

**H3 设计决策**（先定再做，否则会反复）：
- **Scope**：AddRows（round-robin dup）+ UpsertRows（API 一致性，虽然 merge_insert 本身幂等）；CreateTable 暂不做
- **协议**：可选 `request_id: string` 字段。空串 = 不 dedup（0.1 行为）
- **存储**：coordinator in-memory HashMap，5 分钟 TTL。不跨 coord 同步；客户端重试在 5min 内 safe，跨 5min 是运维问题
- **返回**：cached response（不重放写）
- **向后兼容**：字段可选；旧客户端不传 = 不 dedup，一致行为

**H12 设计**：扩展 `bench_phase17_mixed.py` 或 `bench_phase17_matrix.py`，矩阵加 k ∈ {10, 100, 1000, 10000}。记录每个 k 的 QPS + P99 + 响应字节数；找 IPC 序列化拐点。

**H17 设计**：lib 测试加 `test_load_invalid_tls_cert` / `test_load_expired_cert` 覆盖 cert/key 加载路径的 Err 分支。Self-signed test cert 用 `rcgen` 程序生成（已有 workspace 依赖检查）或简单无效字节串测 parser 拒绝。

**H5 设计**：新 enum `ShardStateError { TableNotFound(String), InvalidMapping(String), StorageError(String) }`；trait 方法 `Result<T, String>` → `Result<T, ShardStateError>`；所有 impl + 调用者一起改。

**H9 选择**：
- PR-blocking（fast）：`e2e_add_upsert_consistency_test.py`（~30s）+ `e2e_ha_test.py`（~1min）
- Nightly：`e2e_full_capability_test.py`, `e2e_rebalance_test.py`, `e2e_tls_test.py`, `e2e_enterprise_test.py`

**H22 baseline**：commit `benchmarks/baseline.json` 记录当前测量值；bench 脚本对比当前 vs baseline，偏差 > 15% 且往坏方向 → fail。

**Beta gate 最终收口**（7 项全完后）：
- 全量 regression：unit + integration + HA + chaos 20 iter + 2h read-only soak
- 更新 RELEASE_NOTES 标 `0.2.0-beta.1`
- Tag 动作交用户

**总估工**：~36h（多轮 session）

## 六、审视循环（每执行 5 项后）

每完成 5 个 H 项：
1. 跑 `cargo test --lib` 确认不回归
2. 更新本文件的 "已完成" 栏
3. 看 `docs/COVERAGE_MATRIX.md` 数字是否上升（未规定必须，但期望 scatter_gather 和 connection_pool 至少涨 10+ %）
4. 检查是否**新发现** P0/P1 需要加入

## 七、已明确**不做**的事

避免 scope 膨胀，**以下项留给 0.2-beta 或 0.3**（不是"漏了"，是"刻意留"）：

- ROADMAP `B2.3-B2.7` 全部（混合 RW beta 工作）
- ROADMAP `R1-R7` 全部（OTEL / audit log / encryption-at-rest 等）
- ROADMAP `M1-M4`（sparse vectors / SQL / lake interop / GPU）
- 4 模块 ≥ 85% 覆盖**硬目标**（alpha 只 measure；本计划拉到 70%）
- 24h soak（2h 够 beta，24h 留 GA CI）
- 任何涉及 lancedb 上游 PR 的工作

## 八、完成定义（Beta-Ready gate）

当以下条件全部满足，可 bump `0.2.0-beta.1`：

- [ ] P1 所有 H 条目全绿（12 项）
- [ ] `cargo llvm-cov` 4 模块平均 line coverage ≥ 70%
- [ ] 全量 bench 矩阵（包括大 k）不回归
- [ ] chaos 20 iter 全绿
- [ ] 2h read-only soak 全绿
- [ ] RELEASE_NOTES.md 列 breaking 变更（H5 ShardState error 类型是 internal API breaking）

至此 alpha→beta 的硬化完成，再决定是否进入 R-level 功能推进。
