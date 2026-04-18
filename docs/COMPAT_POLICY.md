# LanceForge 兼容性政策

**覆盖范围**：0.2.0 及之后的所有 minor / patch 发布。
**对应 ROADMAP 项**：`ROADMAP_0.2.md` §B3.5。

## 一、总原则

LanceForge 区分三种兼容性维度：**wire（gRPC 协议）**、**persistence（MetaStore JSON + Lance dataset）**、**config（YAML 字段）**。三者的演化规则不同，但都围绕同一个承诺：

> **任意两个相邻 minor 版本（N 与 N+1）之间：客户端可滚动升级而不中断；服务端可混跑（一个 coord 是 N，另一个是 N+1）而不数据损坏。**

## 二、版本号语义

遵循 [SemVer 2.0](https://semver.org)：

| 变更类型 | 示例 | 触发版本 bump |
|---|---|---|
| Breaking wire change | 必填 proto 字段改变、RPC 删除、语义翻转 | MAJOR（0.x → 1.0；1.x → 2.0）|
| Breaking persistence | schema_version 跳且不提供自动迁移 | MAJOR |
| Additive wire | 可选 proto 字段、新 RPC | MINOR |
| Additive persistence | 新 schema_version，旧版本可读 | MINOR |
| Bug fix、doc | 不动 API 不动数据 | PATCH |
| 预览 / 不稳定 | `-pre.N`、`-alpha.N`、`-beta.N`、`-rc.N` | 预发布后缀 |

**0.x 阶段的例外**：minor bump 允许 breaking（但必须明确 changelog 列出）。**1.0 之后**严格遵循上表。

## 三、Wire（gRPC）兼容性

### 规则
1. **proto 字段只增不减**：0.2 中新增的 `HealthCheckResponse.server_version` 在 0.3 不能删除。
2. **字段编号不重用**：即使字段删除，其 number 永久作废。
3. **可选字段可以不传**：对端看到空值 = "对端是旧版本，没这个字段"。
4. **RPC 不能删除**，只能 deprecated → 下一个 MAJOR 删。
5. **RPC 不能改名**或者改签名。改行为的话，新开一个 RPC、老的 deprecated。

### 新功能的加法
新加字段 / RPC 只有在**客户端和服务端都升级到 N+1 之后**才应该被使用。客户端代码应先检查 `server_version`，再决定是否发新字段。

### N-1 保证
服务端发行 N（比如 0.3.0）时，N-1（0.2.x）客户端仍然能完成所有 0.2 的功能。
服务端发行 N 时，N+1（0.4.0）客户端连 N 时能力优雅降级，不应 crash。

## 四、Persistence 兼容性

### MetaStore JSON Snapshot

字段：`schema_version: u32`（B3.2 已引入）。
规则：
- 读到 `schema_version < CURRENT`：允许，首次 persist 时 stamp 到 CURRENT（自动 upgrade）
- 读到 `schema_version == CURRENT`：正常
- 读到 `schema_version > CURRENT`：**拒绝加载**，返回清晰错误。避免老 server 写坏新格式

### Lance Dataset

Lance 自身的 manifest 版本由 `lancedb` crate 管理。LanceForge 不直接操作 manifest 格式。升级 `lancedb` 依赖的 minor / major 版本要在 CHANGELOG 中显式提示。

### 迁移命令

未来涉及非向后兼容的 persistence 迁移（schema_version 跳变），配 `lance-admin migrate` 子命令（B3.3，暂未实现，首次出现时加）。

## 五、Config（YAML）兼容性

### 规则
1. **新字段必须有 default**。0.1.0 YAML 应能被 N+1 server 原样读取。
2. **字段删除要经过 1 个 MINOR 的 deprecation 窗口**。该窗口内 server 仍接受但忽略，并打 warn 日志。
3. **字段语义变化视同 break**（比如 `replica_factor` 改语义——我们专门保留了旧名字以避免此类问题）。

### deployment_profile 的演化契约

0.2.0 引入的 `deployment_profile` 字段：
- 未设置 → 默认 `dev`，行为与 0.1.0 完全一致
- 有效值：`dev` / `self_hosted` / `saas`
- 任何新 profile 变体（比如 `edge`）必须向 `dev` 方向兼容

## 六、N-1 支持时长

- **MINOR** 升级：最新 MINOR 发行至少支持**前一 MINOR**（N-1）。例：0.3 发行时 0.2.x 仍获 CVE fix。
- **PATCH** 升级：zero friction，不需要 dev 侧适配。
- **MAJOR** 升级：至少提供 1 个**带迁移文档**的 MINOR（如 0.9 → 1.0 过渡期）。

## 七、Break 的操作流程

当一个变更被认定为 break（上表第 1、2 行），必须：

1. **更新 `CHANGELOG.md`**：`### Breaking Changes` 段落下详列。
2. **更新本文件相关章节**：标明何时 deprecated、何时删除。
3. **更新 `RELEASE_NOTES.md`**：在"Upgrade notes"段落写出用户侧需要的动作。
4. **如果涉及迁移**：提供命令或迁移脚本（`lance-admin migrate` 或临时一次性脚本）。

## 八、测试保护

- `crates/common/src/config.rs::test_config_0_1_yaml_upgrades_cleanly`：守护 Config 向后兼容
- `crates/meta/src/store.rs::test_schema_version_upgraded_from_pre_0_2_snapshot`：守护 Persistence 向后兼容
- `crates/meta/src/store.rs::test_schema_version_future_rejected`：守护 Persistence 向前拒绝
- 每一次 break 都要扩充或新增此类测试

## 九、当前状态（0.2.0-pre.1 视角）

| 维度 | 当前兼容性状态 |
|---|---|
| 0.1.x → 0.2 wire | ✅ 加法更新，完全兼容 |
| 0.1.x → 0.2 persistence | ✅ auto-upgrade 生效 |
| 0.1.x → 0.2 config | ⚠️ 一个 behavior change：`--auto-shard` 现在需要 `deployment_profile=dev`（0.1 默认就是没这个字段，解析为 `dev`，所以实际用户无感）|
| 0.1.x → 0.2 legacy api_keys | 🔴 **calendar for 0.2.0-alpha.1**：当前 legacy `api_keys` 默认给 Admin；B1.2.3 会把默认改为 ReadOnly（breaking），在 CHANGELOG 中列出 |

## 十、反面教材（绝不做的）

- **静默改默认值**：user 升级后行为变化但没明示
- **proto 字段类型变化**：`int32` 改 `int64` 即使值兼容也算 break
- **MetaStore key 重命名**（比如 `tables/foo` → `ns/default/tables/foo`）不配 migration
- **删除 config 字段**不经过 deprecation 窗口

---

**维护者**：本文件与 `CHANGELOG.md` / `ROADMAP_0.2.md` 互为引用。每次发布前必过此清单。
