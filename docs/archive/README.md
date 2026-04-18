# 归档说明

本目录保存历史计划文档——内容反映过去的决策上下文，**不再是 forward-looking 的真相源**。

当前有效的路线图：[`docs/ROADMAP_0.2.md`](../ROADMAP_0.2.md)。

## 已归档文件

### `PLAN_phase1-7.md`
- **原位置**：`lance-integration/PLAN.md`
- **归档时间**：2026-04-18
- **归档原因**：Phase 1-7 时代的迭代计划，内容的 Phase 8-9 目标已在 0.1.0 发布前全部落地（S3 MetaStore、Helm chart、RBAC、10M+ 验证、CI pipeline 均已完成）。保留作为历史记录。
- **完成度**：100%。

### `PHASE17_SUMMARY.md`
- **原位置**：`docs/PHASE17_SUMMARY.md`
- **归档时间**：2026-04-18
- **归档原因**：Phase 17（2026-04-15 完成）的交付报告，本身即为历史快照，非计划文档。
- **⚠️ 已被推翻的判断**：文末 §十"明确不做的"清单在 self-hosted + 等客户需求框架下成立，但 0.2 转向 SaaS-first 后，其中 4 项（OpenTelemetry / OBS GC / Python async / schema 演化）重新成为阻塞项。详见 `ROADMAP_0.2.md` §0.4。

## 归档规则

- 任何 forward-looking 文档（ROADMAP / PLAN / TODO 类）一旦完成或被取代，移入本目录
- 归档时保留原始内容 **不改写**，只通过本 README 记录归档时点与原因
- 已完成报告（Phase 总结、Release 回顾）直接放本目录即可，不需要再生成新计划
