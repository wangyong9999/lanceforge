# LanceForge 0.2 路线图：走向商用 SaaS

**状态**：0.1.0 已发（2026-04-16）；0.2 目标 GA 预期 ~12 周（2026-07 中旬）。
**维护者**：在每次影响本路线图的 commit 里同步更新本文件，保持为唯一 forward-looking 文档。
**推翻声明**：本文件明确取代 `docs/archive/PHASE17_SUMMARY.md` §十"明确不做的"清单。详见 §0.4。

---

## 0. 设计约束（不可妥协的三条）

### 0.1 SaaS-first
- 所有持久状态放在对象存储；任意 coordinator / worker pod 可以随时被 kill 并由新 pod 替换
- 本地磁盘只允许存放：query cache、shard 预热副本、临时 spill。**绝不允许**做为唯一真相源
- Scale-to-zero 是设计终点。0.2 不做实现，但任何代码都不能写出阻碍它的 invariant

### 0.2 对象存储完全无状态化
- MetaStore / session registry / shard assignment / auth registry / 审计日志：全部走 OBS
- 强一致性依赖走 OBS conditional write（S3 `If-Match` / GCS `x-goog-if-generation-match` / Azure `If-Match`）
- `FileMetaStore` 降级为 dev-only；生产默认 S3MetaStore，config 校验期硬拒绝 file:// 后端
- 新 pod 启动路径：读 OBS → 重建 in-memory → 就绪上报，**不得有"迁移老状态"步骤**

### 0.3 Ballista 模块隔离
- Ballista 子树不进入 main query path（0.2 不做集成）
- Cargo workspace 物理隔离：`ballista/` 保留但从默认 workspace 移出或 feature-gated
- 目的是**冷冻保存**未来 SQL 分析能力的底座，而不是让它腐化或污染主路径
- 0.2 验收标准：默认 build `cargo build --workspace` 不编译 ballista；feature-gated `--features sql-analytics` 下仍可编译

**当前状态（2026-04-18，L0.6 后）**：通过 `default-members` 实现逻辑隔离——`cargo build` / `cargo test` / `cargo clippy`（无参）只涉及 LanceForge 6 个 crate；Ballista 7 crate 仍在 `members` 以保留 `workspace.package` / `workspace.dependencies` 继承（`ballista/*/Cargo.toml` 的 `edition = { workspace = true }` 依赖它）。

**剩余工作（延后至 M2 时处理）**：严格 `--workspace` 隔离需要给 `ballista/` 建独立 workspace（移除对父 `workspace.*` 的继承），工作量 ~2d 但只在 M2 激活 SQL 路径时有价值。当前 CI 已全部 `-p`-based，不受 `--workspace` 是否包含 Ballista 影响。

### 0.4 对 PHASE17_SUMMARY §十的推翻

原"self-hosted + 等客户需求"框架下的"明确不做"清单，在 SaaS-first 下反转：

| 原判断 | 反转后 | 理由 |
|---|---|---|
| 不做 OpenTelemetry tracing | **R1 必做** | 企业 SRE 标配；SaaS 多租户排障无 trace id 寸步难行 |
| 不做对象存储 GC | **纳入 B1** | SaaS 账单失控风险 10× 于 self-hosted |
| Python SDK 不异步化 | **R5 必做** | 现代 AI app 全 async；sync 客户端会把整个 FastAPI event loop 卡死 |
| 不做 schema 演化 | **R7 最低限度** | ADD COLUMN NULLABLE 是绝对底线，连 Pinecone 都有 |
| 不做 time-travel / 流式 | 保留不做 | 这两项 SaaS 也不急 |
| 不做 Add 哈希路由 | 已做（commit d3aaa3e） | 已落地 |

---

## 1. 当前状态（0.1.0 基线）

### 1.1 已交付能力
| 维度 | 状态 | 证据 |
|---|---|---|
| 分布式 ANN / FTS / Hybrid | ✅ | crates/coordinator + bench/results |
| Upsert / Add 路由一致 | ✅ | commit d3aaa3e，LIMITATIONS §1 更新 |
| HA 连接池 warm-up | 🟡 代码已改，待验证 | connection_pool.rs:234-249 (A1 in flight) |
| 多 coordinator + S3 MetaStore | ✅ | LIMITATIONS §3 |
| TLS / API Key / RBAC | ✅ | crates/coordinator/src/auth.rs |
| 单元测试 | 272 | `cargo test --lib` |
| E2E 测试 | 21 suites | lance-integration/tools/ |
| 覆盖率门禁 | ≥70% | ci.yml:411-423 |
| Prometheus / healthz | ✅ | crates/coordinator/src/rest.rs |

### 1.2 已识别 gap

**A 类：LIMITATIONS.md 已披露（1-12 条，详见该文件）**。最需要严肃对待的：
- §1 Upsert/Add 一致性（已部分修复，服务端无强制校验）
- §4 S3 orphan GC 缺失
- §9 无 per-key quota
- §10 未实现：schema evolution / time-travel / OTEL / async SDK / 流式
- §11 读写混合 -91% QPS
- §12 性能拐点（<50K 单 shard 更快、大 k 序列化开销）

**B 类：0.1.0 盲区（LIMITATIONS 未列，本路线图新增跟踪）**：
1. proto / MetaStore schema 无版本化，升级路径未定义
2. 关键路径分支覆盖率未量化（scatter/merge/CAS/pool）
3. HA 测试历史 flake（commit 96779bc / 11233ac）——根因追踪机制缺
4. 无混沌测试框架（只有线性 E2E）
5. 无 24h+ soak data
6. 无 encryption-at-rest / audit log
7. 无 backup/restore CLI 工具链
8. 无 capacity planning 工具
9. 无 rolling upgrade 实测
10. benchmark 全 WSL2 单机，跨节点网络延迟未测

---

## 2. 分层路线图

### 层 0：在途收尾（本周完成）

前 Agent 会话的 A1/A2/A3 已启动，收尾：

| ID | 工作 | 状态 | 验收 |
|---|---|---|---|
| L0.1 | A1 连接池 warm-up：HA 测试验证 + commit | 🟡 测试跑中 | e2e_ha_test.py 7/7，3×1s 重试窗内稳定 |
| L0.2 | A3 删 persistent_state.rs 死代码（562 LOC） | 待执行 | `cargo build --workspace` + `cargo test --workspace` 全绿 |
| L0.3 | A2 混合 RW profile（不 fix，只收数据） | 待执行 | 产出 `docs/PROFILE_MIXED_RW.md`，含 tracing 时间线 + 热点 TOP3 |
| L0.4 | 归档老计划（PLAN.md / PHASE17_SUMMARY.md） | 待执行 | `docs/archive/` 含两份 + README 说明归档时机 |
| L0.5 | 本 ROADMAP 入库 | 本 commit | git show HEAD -- docs/ROADMAP_0.2.md |

### 层 1：Blocker（0.2-alpha，~4 周）

必须全部完成才能发布 0.2-alpha 做内部 dogfood。每项拆为小步骤，按数字顺序推进。

---

#### **B1. SaaS 无状态化完整性审计**（最高优先级）

**目标**：证明任意 coordinator/worker pod 被 kill 后 ≤5s 内由新 pod 替换，业务连续性 ≥99.5%，无状态丢失。

**小步骤**：

- **B1.1** 持久状态清单审计 — ✅ 完成
  - 产出 `docs/STATELESS_INVARIANTS.md`：分 Truth / Cache / Config 三类枚举代码库每处状态
  - 识别 4 处 Gap（A 密钥 config-only / B auto-shard 本地文件 / C FileMetaStore 仍可启用 / D StaticShardState 默认兜底）
  - 结论：整体 SaaS-first 化程度 ~75%，剩余工作集中在 Gap A-D

- **B1.2** 非 OBS truth → 迁 OBS — 4 个子项，详见 `docs/STATELESS_INVARIANTS.md` §5
  - **B1.2.1** 引入 `deployment_profile` 配置 + 启动期校验（闭 Gap C + D）— 2d
  - **B1.2.2** auto-shard config 从本地 tmp → MetaStore（闭 Gap B）— 1w
  - **B1.2.3** API key registry 从 config → MetaStore + hot-reload（闭 Gap A）— 2w
  - **B1.2.4** 兼容性 & 迁移测试：老 config → saas profile 升级路径 — 3d
  - 每项独立 commit，可单独 review

- **B1.3** worker 启动路径改造
  - 从 MetaStore 读 assigned shards → 拉数据（or lazy load on first query）→ 向 coordinator 上报就绪
  - 去掉任何 "worker 本地有状态所以要等恢复" 的分支
  - E2E：杀一个 worker 进程，启一个新 pod 用不同 hostname，**≤5s 接管其 shard**

- **B1.4** coordinator 启动路径改造
  - 从 MetaStore 读集群视图（executors + tables + shards）→ 重建 connection pool
  - 重启期间已有的客户端 RPC 全部快速 UNAVAILABLE（而非 hang），靠客户端重试

- **B1.5** OBS orphan GC（关闭 §LIMITATIONS 4 的 S3 漏洞）
  - coordinator 后台任务：list OBS prefix vs MetaStore `shard_uris` diff
  - 72h 宽限期后 soft-delete（rename 到 `_trash/`）；14d 后 hard-delete
  - 所有 delete 走 MetaStore 记账，保证可审计
  - feature flag 灰度，默认关

- **B1.6** Chaos 验证
  - `chaos/pod_kill_random.py`：N=100 轮随机 kill coordinator 或 worker pod
  - 成功判据：读 QPS 降级不超过 10%，恢复时间 p99 < 10s

- **B1.7** 文档
  - `docs/STATELESS_INVARIANTS.md`（B1.1 产物保持更新）
  - `docs/DEPLOYMENT.md` 增 SaaS 配置章节（强制 S3MetaStore + 推荐参数）

**验收**：B1.6 chaos 测试 100 轮通过；`docs/STATELESS_INVARIANTS.md` 每项都指向 OBS truth 或标为"cache, 可丢"。

---

#### **B2. 读写混合缓存分级失效**（闭合 LIMITATIONS §11）

**目标**：100K 行 + 10 QPS 写背景，读 QPS ≥ 80% of read-only baseline（当前 218 / 2528 = 8.6%，目标 ≥ 2022）。

**小步骤**：

- **B2.1** 根因 profile（L0.3 产物喂入）
  - tokio-console + `tracing::instrument` 每个 cache 触碰点
  - 确认假设：是 "dataset version change → 整个 IVF partition cache 失效" 还是 "manifest 重读"
  - 产出 `docs/PROFILE_MIXED_RW.md` 含火焰图 / spans timeline / 热点 TOP3

- **B2.2** 缓存分层重设计
  - 三层 cache：manifest (cheap to reload) / fragment metadata / IVF partition data
  - 写只脏受影响 fragment；IVF partition 数据若索引未变则保留
  - 设计文档 `docs/decisions/001-cache-layering.md`（ADR）

- **B2.3** 实现 fragment-level invalidation
  - 小 PR：fragment id → cached (manifest_ver, payload)
  - 写入返回受影响 fragment id 列表
  - 只 invalidate 列表内的条目

- **B2.4** 实现 IVF partition 复用
  - IVF 索引 manifest 独立于 data manifest 变化
  - 读路径只在 IVF manifest 变化时重建 partition
  - 配置开关 `cache.ivf_reuse: true`

- **B2.5** Bench 验收
  - `bench_phase17_mixed.py` 扩展：10 readers + {0, 1, 10, 50} QPS 写
  - 目标：10 QPS 写时读 QPS ≥ 80% 零写 baseline
  - 回归基准入 `docs/BENCHMARK.md`

- **B2.6** CI 集成
  - `bench_phase17_mixed.py` 加入 PR gate（smoke 规模，<3min）
  - 回归 > 10% 阻塞 merge

**验收**：B2.5 目标数字达到；CI gate 生效 2 周无 false positive。

---

#### **B3. 版本化与升级安全**

**目标**：0.1.0 集群可 zero-downtime 升级到 0.2.0，无数据丢失、无长连接中断 > 10s。

**小步骤**：

- **B3.1** Proto API 版本字段
  - 每个 RPC request/response 加 `api_version: string`（如 `"v1"`）
  - 服务端拒绝不兼容版本，返回明确错误
  - 客户端可降级选择 max supported version

- **B3.2** MetaStore schema 版本
  - `MetadataSnapshot` 加 `schema_version: u32`
  - 读取器兼容 N-1 版本（升级时自动写回新版本，旧 coordinator 仍能读老版本直到集群完全切换）

- **B3.3** `lance-admin migrate` 子命令
  - 离线工具：读 OBS metadata → 升级 schema → 写回（原子）
  - dry-run 模式默认开，`--apply` 显式确认

- **B3.4** 在线升级 E2E 测试
  - 起 0.1.0 集群 → 热流量 → 滚动替换为 0.2.0 pod → 验证
  - 数据一致性 + 延迟 p99 不超过基线 2×

- **B3.5** `docs/COMPAT_POLICY.md`
  - 支持 N-1 版本保证
  - break change 定义与 deprecation 窗口（至少 1 个 minor release）
  - 公开 proto schema 变更 diff 的 CHANGELOG 位置

**验收**：B3.4 E2E 在 CI nightly 跑通；文档 review by 至少 1 位外部读者。

---

#### **B4. 关键路径覆盖率矩阵**

**目标**：4 个模块每条错误分支都有 unit + fault-injection 覆盖，分支覆盖率 ≥ 90%。

**模块**：
- `coordinator::scatter_gather`
- `coordinator::global_top_k_merge`
- `meta::metastore::cas_update` (S3/File 双后端)
- `coordinator::connection_pool` (尤其 stale channel / reconnect 路径，即 A1 的领域)

**小步骤**：

- **B4.1** llvm-cov per-file HTML 报告
  - `cargo llvm-cov --html --output-dir target/cov-html`
  - CI artifact 上传，PR 可直接看覆盖率差异

- **B4.2** 4 模块分支 inspection + gap list
  - 每模块产出一份 `missing-branches.md`

- **B4.3** 补测试（逐模块小 PR）
  - fault injection：tonic timeout / S3 429 / S3 PreconditionFailed / deserialize error / partial worker response
  - 每个 PR 只动一个模块，便于 review

- **B4.4** CI 门
  - 4 模块 branch coverage < 90% 阻塞 merge
  - 全局 line coverage 维持 ≥ 70%（现有）

**验收**：nightly 报告 4 模块 ≥90% 连续 1 周稳定。

---

#### **B5. 混沌测试框架**

**目标**：nightly 100 轮混沌跑，业务连续性 ≥99%，flake rate ≤0.5%。

**小步骤**：

- **B5.1** Chaos spec DSL
  - YAML 定义：fault type × duration × recovery × invariant
  - 示例：`- fault: worker_kill, target: random, duration: instant, invariant: read_qps > 100`

- **B5.2** Fault library（逐个实现）
  - `worker_kill` / `coordinator_kill`
  - `slow_disk`（strace/tc 或 fuse 插桩）
  - `net_partition`（iptables DROP）
  - `memory_pressure`（cgroup memory cap）
  - `s3_throttle`（mock S3 返回 429）
  - `clock_skew`（libfaketime）

- **B5.3** Runner
  - `chaos/runner.py --spec xxx.yaml --iters N`
  - 输出 JSON：pass/fail/mtl/invariant-violations

- **B5.4** Nightly CI job
  - 100 iters × 6 个 fault 组合
  - dashboard：flake rate + MTTR + 失败分布

**验收**：B5.4 连续 2 周 flake rate ≤0.5%。

---

#### **B6. 24h Soak Test**

**目标**：24h 混合负载连续跑，RSS / FD / tokio task 数 漂移 < 5%。

**小步骤**：

- **B6.1** Soak harness
  - 背景：1 QPS 写 + 10 QPS 读 + 每小时 1 次 DDL
  - 运行在专用 soak 机器（自建 runner，CI 不跑长时）

- **B6.2** Metrics snapshot
  - 每分钟 `/metrics` + `ps -o rss,nfd` 存 S3

- **B6.3** 分析脚本
  - 首 10min vs 最后 10min 对比，漂移 > 5% 标红

- **B6.4** 周度自动执行 + 报告
  - 自动 PR review comment 形式通知

**验收**：连续 4 周成功完成 soak，无漂移超标。

---

### 层 1.5：beta-blocker 的 B2 延续

**背景**：alpha 达成 `+10 QPS 写 = 82.5% baseline`，但 `+50 QPS 写 = 18.5%`。根因不在 cache（原 ROADMAP 假设），而在**存储层共享资源**（详见 `docs/PROFILE_MIXED_RW.md` §8）。拆成 5 个 beta 子项：

| ID | 工作 | 估工 | 验收 |
|---|---|---|---|
| **B2.3** | Write batching — WriteBuffer 窗口 500ms / 1000 行满任一 flush；proto 加 `queued` + `commit_seq` | ~1w | 本地 fs 下 50 QPS 写场景读 QPS ≥ 60% baseline |
| **B2.4** | Fragment-level invalidation — cache key 从 `dataset_version` 换成 `fragment_id_set`；依赖 lancedb fragment API | ~2w | 10-30 QPS 写场景再拿 10-15% |
| **B2.5** | 物理存储隔离 PoC — write worker 写独立 hot tier，背景 compact 到 cold tier；readers 只读 cold | ~1w spike + N 周落地 | feasibility note + PoC 数据 |
| **B2.6** | **S3 基线验证**（前置，最先做） — 在 MinIO / 真 S3 下跑同 bench；对象存储 HTTP 语义下 worker 间本不共享存储资源，可能 50 QPS 场景本来就好 | ~2d | 实测数据入 PROFILE_MIXED_RW.md |
| **B2.7** | Async commit — `durability: {sync, async, group_commit}`；async 模式 ack 不等 fsync | ~1w（和 B2.3 叠加）| 可选加速 B2.3 的写放大收益 |

**依赖顺序**：B2.6 先跑 → 拿到本地 fs vs S3 的对比数字 → 决定 B2.3/B2.4/B2.5 哪些是真 beta blocker。如果 B2.6 显示 S3 下已经达 80%，B2.5 可能完全不做（S3 部署即解决）。

### 层 2：Strong-Recommend（0.2-beta，~8 周）

企业客户必问项。0.2-beta 目标 80% 完成。

#### R1. OpenTelemetry 分布式 tracing
- 全链路 trace_id 从 gRPC metadata 传播到 lance.rs 内部
- OTLP exporter（Jaeger / Tempo 兼容）
- Grafana dashboard 模板：`dashboards/lanceforge.json`
- 文档：`docs/OBSERVABILITY.md`

#### R2. Per-API-key 配额
- QPS quota：coordinator 侧 token bucket per key
- Storage quota：每次 AddRows / CreateTable 查 MetaStore 记账
- 超额返回 `ResourceExhausted` + Retry-After
- 多租户隔离模型 `docs/MULTI_TENANT.md`

#### R3. Audit log
- 结构化 JSON：timestamp / actor_key_hash / action / resource / result
- 输出 stdout + 可选 S3 rolling
- SIEM 接入文档（Splunk / Elastic / CloudWatch）

#### R4. Encryption-at-rest 方案
- Pattern A：S3 SSE-KMS 最简（`sse_kms_key_id` 配置项）
- Pattern B：client-side envelope（更高合规，需 KMS 集成）
- 两种 pattern 的 demo + 文档

#### R5. Python SDK async
- 新增 `grpc.aio` 客户端 `AsyncLanceForgeClient`
- 保留同步 API（底层共享 channel 配置）
- bench：async vs sync 在 FastAPI 场景的实测 QPS

#### R6. `lance-admin` CLI
- 单一二进制，子命令：
  - `snapshot` / `restore`（配合 OBS 版本控制）
  - `migrate`（B3.3 产物）
  - `compact`
  - `gc-orphan`（B1.5 产物的手动触发）
  - `cluster-status`

#### R7. 最小 schema evolution
- `ADD COLUMN ... NULLABLE`
- 版本化 schema 存 MetaStore
- Rollback：schema_version 回退

---

### 层 3：Moat（0.2-GA，~12 周，至少落 2 项）

决定 vs Pinecone / Qdrant / Weaviate 的差异化。

#### M1. Sparse vector 原生支持（SPLADE / BGE-M3）
- 数据类型：sparse_vector
- 索引：inverted（复用 tantivy）+ dim-level weight
- 混合排序：dense + sparse 融合

#### M2. Ballista / DataFusion SQL 查询路径（保持模块化隔离）
- 按 §0.3 约束：`ballista/` 保留但独立
- `crates/lance-sql` 桥接层（feature-gated `sql-analytics`）
- PoC demo：`SELECT ... FROM lance_table WHERE embedding NEAR ? LIMIT 10`
- 明确 M2 是"保留未来能力 + 一个 demo"，不是默认启用

#### M3. 数据湖互操作 demo
- Iceberg / Paimon Parquet → Lance 零转换 ingest 管道
- 对 Pinecone 最硬差异化
- 发博客 + `examples/lake_ingest/`

#### M4. GPU 加速 ANN 预研
- 不保证落地
- 产出 `docs/decisions/XXX-gpu-feasibility.md`：成本 / 吞吐 / 集成难度评估

---

## 3. 治理

### 3.1 决策记录（ADR）
- 每个大项（Bn / Rn / Mn）在实施期维护 `docs/decisions/NNN-xxx.md`
- 模板：背景 / 选项 / 选定理由 / 未来换路径的 trigger

### 3.2 本文件更新节奏
- 每次 commit 动到 B/R/M 项状态时，更新对应表格
- 每月一次 checkpoint review（commit tag `roadmap-checkpoint-YYYY-MM`）

### 3.3 Milestone 退出条件

#### 0.2-alpha exit criteria（2026-04-18 修订，落地化版本）

把每项 "B 做了" 转成 **产出物 × 量化门** 二元组——alpha 不靠主观判断 ✅。

| ID | Minimum 产出物 | 量化门 |
|---|---|---|
| **B1.2.3** | `auth.rs` 支持 MetaStore 后端 + 定期 refresh；`lance-admin auth` CLI 子命令 | `add-key` 后新 key ≤ 60s 被 coordinator 识别；legacy config 路径保留为 bootstrap-only |
| **B2.2** | `coordinator::scatter_gather` / `connection_pool` 支持 read-primary / write-primary 标记 | `bench_phase17_mixed.py` 10 QPS 写下 Read QPS ≥ 80% read-only baseline |
| **B3.5** | `docs/COMPAT_POLICY.md` | 存在，且明确 N-1 规则、proto 演化规则、break policy |
| **B4-min** | `docs/COVERAGE_MATRIX.md` + `target/cov/html/` artifacts | 4 关键模块（scatter_gather / global_top_k_merge / metastore CAS / connection_pool）分支覆盖率**已测量并入档**；目标 ≥ 85% 但不 block alpha |
| **B5-min** | `chaos/` 目录 + DSL runner + 2 faults (`worker_kill`, `slow_disk`) | 每 fault 运行 10 轮，通过率 100%，结果文件落地 |
| **B6-min** | `soak/` harness + 2h 数据采集脚本 | 2h soak 期间 RSS / open-fd 漂移 < 3% |
| **Docs** | README / QUICKSTART / CHANGELOG / RELEASE_NOTES 全部提及 `deployment_profile` + 0.2 新能力 | 无死链，docs 交叉引用一致 |
| **Regression** | 全量 `cargo test --lib` + `e2e_ha_test.py` + `bench_phase17_mixed.py` 绿跑 | 全绿，结果文件 commit |

**显式延后到 beta 或更晚**：
- B4 真正做到 4 模块 ≥ 90% 分支覆盖（alpha 只测量 + 定基线）
- B5 full chaos fault library（net_partition / OOM / s3_429 / clock_skew）
- B5 nightly 100-iter CI job（基础设施）
- B6 full 24h soak CI job
- B3.3 `lance-admin migrate` CLI（0.2→0.3 出现第一次 schema 变更时再写）

#### 0.2-beta exit criteria
- R1-R6 全部完成，R7 至少 ADD COLUMN NULLABLE
- 50+ 外部用户 + 2 个 real-world case study（含 RAG）
- Chaos / Soak 升级为 CI nightly job，连续 2 周无 regression

#### 0.2-GA exit criteria
- M 项至少落 2 项（建议 M1 sparse vector + M3 lake interop）
- 所有 breaking changes 在 `CHANGELOG.md` 明确列出
- 至少 1 个 publicly documented production deployment

---

## 4. 本周启动顺序（具体动作）

| 序 | 动作 | 状态 |
|---|---|---|
| L0.1 | 跑 e2e_ha_test.py 验证 A1 → commit | ✅ `1a30f5e` |
| L0.2 | A3 删 persistent_state.rs（562 LOC） | ✅ `0818949` |
| L0.3 | A2 profile 混合 RW，出 PROFILE_MIXED_RW.md | ✅ `8798480` |
| L0.4 | 归档 PLAN.md / PHASE17_SUMMARY.md | ✅ `87df6b0` |
| L0.5 | 本 ROADMAP commit | ✅ `87df6b0` |
| B1.1 | 持久状态审计，出 STATELESS_INVARIANTS.md | ✅ `e4bf58e` |
| L0.6 | Ballista workspace 逻辑隔离（default-members） | ✅ `21147f0` |
| B1.2.1 | `deployment_profile` 启动守卫（闭 Gap C + D） | ✅ `d31d3de` |
| B1.2.2 | auto-shard config → MetaStore（闭 Gap B） | ⏳ ~1w |
| B1.2.3 | API key registry → MetaStore + hot-reload（闭 Gap A） | ⏳ ~2w |
| B1.2.4 | 升级兼容测试 | ⏳ ~3d |

---

**末尾提示**：本文件不是承诺书，是判断与检查清单。任何一项的优先级、范围、甚至是否做，都可以在 ADR 里 flip——但 flip 必须走 commit + PR，不能悄悄放弃。
