# 0.2.0-beta.1 发布计划

**建档时间**：2026-04-18
**目的**：把 0.2-alpha 推进到 **对得起"商用可用"四个字** 的 0.2-beta。
**依据文档**：`COMPETITIVE_ANALYSIS.md`（为什么）+ `HARDENING_PLAN.md`（已完成的基础）。
**输出**：tag `v0.2.0-beta.1`，RELEASE_NOTES 诚实，LIMITATIONS 更新。

## 一、入 gate（0.2-alpha 之后必过）

### Phase 1 — 基础 gate 闭合（~1 session）

| # | 任务 | 估工 | 验收 |
|---|---|---|---|
| **G1** | H24 k=10000 错误率 | 3h | 100K × 128d × k=10000 conc=10 错误率 < 1% |
| **G2** | coverage 重测（4 模块 ≥ 70%）| 30min | `cargo llvm-cov` 报告入档 |
| **G3** | chaos 20 iter 再跑 | 10min | worker_kill 10/10 + worker_stall 10/10 |
| **G4** | 1-2h read-only soak | 1-2h | RSS/fd drift < 3% |

Phase 1 卡住则不能进 Phase 2/3/4。

### Phase 2 — Severity 1 Gap 闭合（~3-4 session）

| # | 任务 | 估工 | 验收 |
|---|---|---|---|
| **G5** | Namespace 多租户（最小可用）| 1w | API key → namespace 绑定；`{ns}/{table}` 命名；列表隔离 |
| **G6** | Per-key QPS quota | 3d | Token-bucket per key；越界 `ResourceExhausted` |
| **G7** | Audit log 持久化（S3 JSONL）| 2d | 每次写 + DDL 追加到滚动 S3 文件；含 SIEM 导入示例 |
| **G8** | Encryption at rest 文档+验证 | 2d | `docs/SECURITY.md` + MinIO SSE 集成测试 |
| **G9** | `lance-admin` backup/restore CLI | 3d | `lance-admin backup create <s3_uri>` + `restore <s3_uri>` |

### Phase 3 — Severity 2（提效，不阻塞）

| # | 任务 | 估工 | 验收 |
|---|---|---|---|
| **G10** | OTEL trace_id 穿透 | 1w | tonic metadata `traceparent` 传递；worker 日志带 trace_id |
| **G11** | Grafana dashboard 模板 | 2d | `deploy/grafana/lanceforge.json` 覆盖核心指标 |
| **G12** | chaos fault 扩展（net_partition, s3_throttle）| 3d | 两个新 scenario YAML，nightly job 全绿 |
| **G13** | Python SDK async client | 1w | `AsyncLanceForgeClient` + FastAPI 示例 |

### Phase 4 — 发布工作（~半 session）

| # | 任务 | 估工 | 验收 |
|---|---|---|---|
| **G14** | RELEASE_NOTES 0.2.0-beta.1 | 1h | 含 feature matrix、upgrade notes、known limits |
| **G15** | LIMITATIONS.md 最终校准 | 30min | 所有已解决的删除；仍存在的明确标注 |
| **G16** | COMPAT_POLICY.md 补 0.2 → 0.3 路径 | 30min | 标明 deprecation |
| **G17** | README 更新（新能力入 feature table）| 30min | multi-tenant/OTEL/quota/audit 都列进去 |
| **G18** | 版本 bump + final regression | 1h | 全部 reg-test 绿；tag 待用户动作 |

## 二、执行原则

1. **每个 G 单独 commit**，commit message 带 G-ID。
2. **Phase 2 的顺序可调**：G6 Quota 可以早做（小）；G5 Namespace 最大（最后做）；G7-G9 独立可并行。
3. **Phase 3 是"能做就做"**：如果时间紧 OTEL 和 dashboard 可以延；async SDK 可以 0.3 才做。我会按 "剩余预算" 决定做到哪一步。
4. **诚实原则**：做不完的不掩盖——LIMITATIONS 写清楚；RELEASE_NOTES 说明 beta 状态；该 flag 为 "preview / experimental" 的就 flag。

## 三、Phase-by-phase gate

**Phase 1 → Phase 2**：G1-G4 全绿。否则 beta 标签用 `0.2.0-beta.1-rc`。

**Phase 2 → Phase 3**：G5-G9 五项中至少闭合 **4/5**（必包括 G5 + G6，命名空间 + 配额是 SaaS 底线）。

**Phase 3 → Phase 4**：G10-G13 做到哪算哪，但必须**都打 0.3 tag**（每个在 LIMITATIONS.md 里标上 "planned 0.3 if not in beta"）。

**Phase 4 → tag**：G14-G18 都绿；全量 regression；手动 review RELEASE_NOTES。

## 四、Definition of Done for 0.2.0-beta.1

完全达成才能 tag：
- [ ] workspace lib tests ≥ 310（alpha 基线 291 + phase 2/3 新增 ~20）
- [ ] HA E2E 8/8
- [ ] Chaos 20/20
- [ ] 1-2h soak drift < 3%
- [ ] Phase 1（G1-G4）全绿
- [ ] Phase 2 ≥ 4/5 绿 且 G5+G6 必包含
- [ ] RELEASE_NOTES 写清楚 feature matrix + known issues + upgrade path
- [ ] COMPAT_POLICY + LIMITATIONS + README 同步
- [ ] `cargo build --release --workspace` 干净无 warning
- [ ] `cargo clippy -- -D warnings` 在 LanceForge crate 上全绿（CI 已有）

## 五、回退方案

如果 Phase 1 的 G1（H24 fix）发现根因是架构级问题（比如要动 lancedb upstream），**切换到**：
- 默认 `server.max_k: 1000`（从 10000 降）
- LIMITATIONS.md 写明 "大 k 是 experimental，生产建议 ≤ 1000"
- **仍然** 发 0.2.0-beta.1，但标注是 "cap 减小到 1000" 的 breaking change
- H24 真修变成 0.3 目标

这是**工程现实**——能发个诚实的 beta 比硬憋等完美版本好。
