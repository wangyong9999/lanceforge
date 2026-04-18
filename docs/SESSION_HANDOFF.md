# Session Handoff — 2026-04-18

**目的**：下一个 session 从这份文档开始，不用回溯整个对话也能无缝继续。
**约定**：新 session 开始时先读这份 + `ROADMAP_0.2.md` + `HARDENING_PLAN.md` 三份。

## 一、工程全貌（2026-04-18 收盘）

### 1.1 版本与 git 状态

- **版本号**：`0.2.0-alpha.1`（未 tag 未 push——按 git safety rule 等用户显式授权）
- **活动分支**：`lanceforge-clean`（~50 commits ahead of `main`；`main` 冻结在 `e8aa9a3` Phase 8 TLS）
- **分支策略**：暂按现状不动（用户指示）。未来可选：把 GitHub default branch 切到 `lanceforge-clean`，或一次性把 `main` fast-forward 上来
- **上一个实质 commit**：`486b5c5` (H23 meta tests → tempdir)

### 1.2 测试 / 覆盖基线

| 维度 | 数值 | 备注 |
|---|---:|---|
| Workspace lib 测试 | **256 passed** | 全绿，验证 3×`cargo test --lib` 无 flake |
| — common | 68 | +7 from H1 (bounds) |
| — coordinator | 119 | +40 in session (H2, H19, H15, H7, H6) |
| — meta | 44 | +9 in session (H8) |
| — worker | 25 | 未动 |
| HA E2E | 8/8 pass | 基于 0.2.0-alpha.1 release 二进制 |
| Chaos (worker_kill / worker_stall) | 20/20 iter pass | 100% / 99.29% query success rate |
| Soak (read-only 5min) | 0 errors, drift < 3% | `coord_rss +1.49% / w0 +2.16% / w1 +1.20%` |
| 混合 RW 82.5% baseline gate | ✅ at 10 QPS 写（split + rc=60 config） | 50 QPS 仍 -81% |

### 1.3 已闭合的硬化项（本 session 10 个）

```
Wave 1: 439598a H1  config validate() bounds check
        4d626dc H2  write-RPC audit + truncate helper
        d10398c H16 matches! asserts + ExecutorConfig::Default
        087ddba H19 offset+k boundary coverage
        7cbab18 H15 REST server shutdown signal
        03bfeef H14 EtcdShardState watch shutdown

Wave 2: cb4f762 H7  connection_pool coverage 13→22
        99a1c93 H8  MetaStore S3 error paths 38→44
        1af27d1 H6  scatter_gather helpers 13→20

Bonus:  486b5c5 H23 meta tests → tempdir (消除 flake)
```

### 1.4 计划文档状态

- `docs/ROADMAP_0.2.md` — 前瞻性唯一真相源，**仍有效**。Layer 1 (B1-B6) 在 alpha 后大部分闭合；Layer 1.5 (B2.3-B2.7) beta 计划存在
- `docs/HARDENING_PLAN.md` — 硬化计划，**仍在执行中**。23 项里完成 10 项（~43%）
- `docs/PROFILE_MIXED_RW.md` — 性能 profile + 50 QPS 根因分析
- `docs/STATELESS_INVARIANTS.md` — SaaS 审计，Gap A/C/D 已闭合，B 部分闭合
- `docs/COMPAT_POLICY.md` — 三维度兼容性契约（B3.5）
- `docs/COVERAGE_MATRIX.md` — 4 关键模块基线（lib only；beta 计划补 integration 数字）
- `docs/LIMITATIONS.md` — 诚实限制清单，§11 已反映 50 QPS 建议 + 多副本 workaround

## 二、深度分析

### 2.1 本 session 反思

1. **Audit 的 P0 误报**率比预期高（`executor.rs:997 unwrap` 实为 test 代码；B1.2.3 被误标 open）。
   - **教训**：外部 audit 要**逐条核对**再进 P0，不要盲信
   - **影响本 session**：修正后 P0 清单为空——alpha 已覆盖所有 security/panic/correctness-critical

2. **Wave 1-2 实际用时 < 计划**（计划 13h，实际 ~4h）
   - Rust 单测添加速度快于预期
   - 但 async 代码的 integration-style 覆盖（scatter_gather 完整路径）lib-only 天花板低——这是 COVERAGE_MATRIX 的已知约束
   - **教训**：HARDENING_PLAN §4 剩余 28h 估工可能偏保守，实际或 15-20h

3. **test flakiness 是真实生产风险**
   - H23 发现 meta 测试每 ~20% 概率 spurious fail
   - 不修 → 后续 CI 会因 flake 被当成真 bug 定位浪费时间
   - 修后 3 连跑全绿
   - **下次的启示**：每次加测试都用 `tempfile::tempdir()`，不要 `/tmp/xxx`

4. **存储层共享是 50 QPS 真问题**（B2.2 实测证实）
   - role split + rc=60 在 10 QPS 下有效（82.5% baseline）
   - 50 QPS 下无效（同样 -81%）——shared `.lance` 目录 + fsync + page cache 争抢
   - ROADMAP 原计划的"fragment-level invalidation"**不是**主要解——storage 层问题需要独立方案（写批处理 / async commit / 物理分离）
   - **对 beta 路径的影响**：B2.6 S3 基线验证**必须先做**，如果 S3 下 50 QPS 本就达 80%，B2.5（物理分离）可以完全不做

### 2.2 HARDENING_PLAN 优先级重排（基于实测经验）

原计划按"波次"机械排序。基于已完成工作的观察 + 价值维度重排：

| 新排序 | Item | 价值维度 | 估工 | 为什么提前/延后 |
|---|---|---|---|---|
| 1 | **H10** 边界值测试 | 测试覆盖 | 1h | 快速、零风险、可能发现真实 bug（dimension 不一致） |
| 2 | **H11** DDL 边界 | 测试覆盖 | 2h | 同上；0-row CreateTable / 并发 drop 未覆盖 |
| 3 | **H21** Graceful shutdown drain | **正确性** | 2h | 生产 SIGTERM 期间 in-flight 查询必须不 hang |
| 4 | **H4** RPC cancellation 穿透 | **正确性** | 3h | client drop 后 coord 该 abort 下游——否则 50 QPS 写场景下会浪费 worker CPU |
| 5 | **H3** AddRows 幂等窗口 | **正确性** | 4h | retry 产生重复数据是已知数据完整性风险 |
| 6 | **H9** Python E2E 进 CI | CI 覆盖 | 2h | 当前大部分 E2E 不在 PR flow；回归易漏 |
| 7 | **H12** 大 k bench | 性能未知 | 2h | k=10000 未测过；可能有未识别拐点 |
| 8 | **H17** TLS 错误路径 | 安全 | 2h | cert 过期 / 错误 CA 行为未测 |
| 9 | **H5** ShardState typed error | 一致性 | 3h | 破坏性内部 API 改动；先做完 correctness 类再动这个 |
| 10 | **H18** Proto 版本协商冒烟 | 兼容性 | 1h | 配 H5 做完一起发 beta |
| 11 | **H20** CI PR/nightly 分层 | 工程 | 2h | 前置：H22 有依赖 |
| 12 | **H22** Bench PR gate | 工程 | 2h | 需要 H20 先落地 CI 结构 |
| 13 | **H13** Per-shard 延迟日志 | 可观测 | 1h | 最低优先级，beta 后也行 |

**重排核心原则**：correctness > coverage > perf > security > infra > hygiene。

### 2.3 Ballista 隔离的现状

- 逻辑隔离（default-members）已就位（`21147f0` L0.6）
- **遗留**：严格 `cargo build --workspace` 排除 Ballista 需要改 `ballista/*/Cargo.toml` 的 `edition = { workspace = true }`，~2d 工作
- **建议**：除非 M2（SQL 分析）真启动，否则延到 GA 前再做

### 2.4 发布路径判断

当前 0.2.0-alpha.1 **可随时 tag + push**（用户授权即可）。

**发 beta 之前需要**（hardening 视角）：
- Wave 3-6（当前 13 项剩余）
- **外加** B2.6 S3 基线实测（ROADMAP §1.5，**独立于 HARDENING_PLAN**）
- **可能外加** B2.3 写批处理（如果 B2.6 显示 S3 不够好）

**beta 到 GA**：R 层功能 + M 层 Moat 至少 2 项；严格 Ballista 隔离；24h nightly soak CI。

## 三、下个 session 第一步

### 3.1 热身（<5 min）

```bash
cd /home/alen/cc/lance-ballista
git log --oneline -10        # 确认从哪里继续
cat docs/SESSION_HANDOFF.md  # 本文件
cat docs/HARDENING_PLAN.md | grep -E "^\|.*H\d" # 看剩余项
cargo test --lib 2>&1 | grep "test result" # 确认 256 全绿
```

### 3.2 首个要做的 commit

**目标**：H10 边界值测试。最小风险、一小时内可完成。

涉及位置：
- `crates/coordinator/src/service.rs` `ann_search` RPC handler 的 `dimension` vs `query_vector.len()` 一致性验证
- 新测试：`test_ann_dimension_mismatch` / `test_ann_empty_vector_with_dim_set` 等

具体步骤：
1. 找 service.rs 里 `dimension = if req.dimension > 0 { req.dimension } else { (query_vector.len() / 4) as u32 };`（当前 line 405 附近）
2. 加校验：如果 client 同时传 `dimension` 和 `query_vector`，`query_vector.len() / 4 != dimension` 应该 reject
3. 加 4 个单测覆盖 happy / dim-only / vec-only / conflicting
4. commit 为 `H10: enforce dimension vs query_vector.len() consistency`

### 3.3 Wave 3 后续

H10 之后直接推 H11（DDL 边界），然后进 H21（shutdown drain）。三个都做完大约 5h，session 末尾 checkpoint commit。

## 四、已知 landmine（别踩）

1. **`/tmp/` 字面量路径**：以后加测试只用 `tempfile::tempdir()`。历史测试里还有一批在 state.rs 用 `rand_id()` 的——单独跑没事，但如果要大改动它们，先转 tempdir
2. **rustls crypto provider**：任何涉及 `tonic::transport::ClientTlsConfig` 的测试都要先 `rustls::crypto::ring::default_provider().install_default()`；否则 panic
3. **ExecutorConfig 字面量**：`Default` 已 derive，新测试用 `..Default::default()`
4. **Meta 测试并行**：如果你同时给 store.rs / state.rs 加测试，**必须**用 `unique_meta_path()` helper（store.rs）或 `rand_id()`（state.rs）
5. **Ballista 不在默认 build**：`cargo build`（无参）只编译 6 个 LanceForge crate；加 `--workspace` 才有 Ballista
6. **端口冲突**：本地跑 bench / chaos / soak / HA 测试时每个脚本用独立端口段（见各脚本顶部常量）。session 结束前 `pkill -9 -f 'lance-(coord|worker)'`

## 五、用户可能会问的事

- **"帮我发版本"** → 答：git safety rule，你手动 `git tag -a v0.2.0-alpha.1 -m "..."` + `git push`
- **"S3 下性能如何？"** → 答：未测。B2.6 计划。需要起 MinIO 或真 S3 跑 `bench_phase17_mixed.py` 对比
- **"XX 覆盖率能到多少？"** → 答：看 `docs/COVERAGE_MATRIX.md`；lib-only 当前有天花板（scatter_gather / connection_pool 靠 integration test 才能上来），是 beta 工作
- **"主线 main 什么时候合回？"** → 答：按用户指示暂不动；建议方案 A/B/C 在 2026-04-18 的对话里讨论过（选 C：只切 GitHub default branch 最便宜）

## 六、Commit 模板（下个 session 遵循）

```
H<N>: <one-line title>

<rationale paragraph: what was broken or missing>

<mechanism paragraph: what this commit changes>

<tests paragraph: how it's verified>

<scope note: if this touches anything the reviewer should double-check>

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

保持 Body Wrapped at ~72 cols，分段讲清楚 why + what + how-verified。
