# LanceForge v2 架构演进计划

**建档时间**：2026-04-21
**目标版本线**：beta.5（Phase A-B）→ beta.6 experimental（C-D）→ beta.7 stable（E-F）→ 0.3（移除旧二进制）
**对应 ROADMAP 项**：取代 `ROADMAP_0.2.md` §B1 的 "SaaS 无状态化完整性审计" — v2 在无状态基础上进一步把角色物理拆开

---

## 0. 动机 — 目前的双层架构为什么不够

当前 coordinator 进程同时承担（源码引用 `crates/coordinator/src/bin/main.rs`）：

- gRPC Scheduler 入口（L295–379）
- Scatter-gather 查询路由（`scatter_gather.rs`）
- ConnectionPool 健康检查 / 服务发现（L197）
- Auth / RBAC / per-key QPS 配额 / namespace binding（L120–162）
- MetaStore 60s 热重载（L214–250）
- Orphan GC（L258–268）
- Write-dedup reaper（L273）
- REST + /metrics 服务（L279–285）
- Audit sink（L178–190）

worker 进程同时承担（`crates/worker/src/bin/main.rs`）：

- gRPC Executor 入口
- `execute_local_search` 读路径
- `execute_local_write` 写路径
- `execute_create_index` 建索引
- `compact_all` + 后台 compaction loop
- LanceTableRegistry 本地 cache

这带来三个证据明确的问题：

1. **Phase 17 mixed-RW bench 实测**：worker 同一进程里 query 和 compaction 抢 CPU/IO，50 QPS 写下读 QPS 降至 18.5% baseline。
2. **H25 事后**：coord SIGTERM 瞬间 auth hot-reload、orphan_gc、conn pool 一起停，控制面和数据面没有独立 drain 窗口。
3. **对标 LanceDB Enterprise 架构**（`docs.lancedb.com/enterprise/architecture`）：他们明确分 Query Nodes / Plan Executors / Indexers / Control Plane **四种**节点类型，"Query fleets can scale for interactive traffic without also scaling background indexing capacity"。我们不分就永远不能按这三路独立扩容。

v2 解决这三件事。

---

## 1. 目标架构：四角色 + 一 monolith

### 1.1 角色定义

| 角色 | 职责 | 状态性 | 扩容指标 |
|---|---|---|---|
| **QN (Query Node)** | gRPC/REST 入口；解析 table_name；下发 scatter；合并结果；traceparent 注入；rate limit enforce | 完全无状态 | HTTP QPS / 并发连接 |
| **PE (Plan Executor)** | 接 QN scatter 指令；本地 NVMe cache；对 OBS 做 read/write；版本 polling | 只 cache 可丢 | 读 QPS / cache miss rate |
| **IDX (Indexer)** | 从 CP 拉 job；建/重建索引；compact fragments；写回 manifest | job lease（MetaStore）| backlog queue depth |
| **CP (Control Plane)** | RBAC / key registry / namespace / 配额 / routing / job queue / cluster lifecycle / audit / orphan GC | MetaStore 之上的热 cache | 控制 API 调用（低频）|

**OBS + MetaStore** 是唯一持久真相源。

### 1.2 通信

```
Client ──► QN (gRPC/REST)
QN ──► CP (resolve, policy; cache + push invalidation)
QN ──► PE (scatter LocalSearch/LocalWrite)
PE ──► OBS (Lance read/write)
IDX ──► CP (lease job)
IDX ──► OBS (compact/index)
所有节点 ──► CP (register/heartbeat)
CP ──► MetaStore (CAS)
```

### 1.3 七个不变量

| ID | 不变量 |
|---|---|
| I1 | OBS 是唯一持久真相源；任何节点 kill 后可从 OBS + MetaStore 重建 |
| I2 | CP 多副本 leader-per-operation；读操作幂等无 leader，写操作走 MetaStore CAS lock |
| I3 | PE 只 cache，不持久；pod 替换后 cache 空，warmup 期 latency 升，功能正确 |
| I4 | QN 无持久状态；session 级 RBAC 凭证可丢 |
| I5 | IDX 的 job 必须幂等；任何 job 可被另一 IDX 重跑，Lance CAS 兜底 |
| I6 | `lance-monolith` 二进制永远存在；开发/CI/小规模 first-class |
| I7 | 现有 coord/worker 二进制在 beta.5 保持行为兼容；deprecation 两个 minor 版本 |

### 1.4 二进制切分

| 二进制 | 角色组合 | 用途 |
|---|---|---|
| `lance-monolith` | QN + PE + IDX + CP | 开发 / CI / 小规模 / 测试（首选）|
| `lance-qn` | QN only | 生产水平扩 |
| `lance-pe` | PE only | 生产存算分离 |
| `lance-idx` | IDX only | 生产独立扩 compaction |
| `lance-cp` | CP only | 生产独立控制面 |
| `lance-coordinator` | alias → `monolith --roles qn,cp`（deprecated beta.7）| 迁移期兼容 |
| `lance-worker` | alias → `monolith --roles pe,idx`（deprecated beta.7）| 迁移期兼容 |

---

## 2. 决策记录：Round 1 草案 → Round 2 自审 → Round 3 收敛

**Round 1** 是四角色切分的直接初稿。

**Round 2** 的自审清单（每条在 Round 3 都有对应处理）：

| ID | 问题 | 严重度 | Round 3 处理 |
|---|---|---|---|
| R2.1 | QN 每次请求 resolve 走 CP 的 RTT 爆炸 | 🔴 | QN 本地 cache + CP 推送 invalidation，cache miss 才走网络 |
| R2.2 | PE 和 IDX 共享 cache 的协调 | 🟡 | 沿用 worker 现有 atomic version poller；中期 IDX 写完后推 PE |
| R2.3 | Write path 跨 shard 事务边界 | 🟡 | QN 承担事务协调，partial success 语义和今天一致；真原子跨 shard 是 C1/C4 的活 |
| R2.4 | 多 CP 脑裂 | 🟡 | 读幂等可并发；写走 MetaStore CAS lease（已有 pattern）|
| R2.5 | IDX job 重复执行 | 🟢 | lease TTL + Lance CAS 兜底，浪费一次 compute 可接受 |
| R2.6 | 单机部署 DX 回归 | 🔴 | `lance-monolith` 二进制保留为 first-class |
| R2.7 | 迁移撕裂 | 🔴 | coord/worker 两个小版本兼容期，逐步 deprecation |
| R2.8 | 观测性多跳 | 🟢 | OTLP + traceparent 已打通，多跳 Jaeger 仍可读 |
| R2.9 | RTT 累加 | 🟡 | QN cache 命中时退回 2 跳，和今天相同 |
| R2.10 | 分布式 rate limit 语义偏差 | 🟡 | 短期 doc 声明 "per-CP，按副本数调"，中期走分布式 token bucket（0.3）|
| R2.11 | 不覆盖 LanceDB "Custom" 部署 | 🟢 | 商业话术，不相关 |
| R2.12 | 写路径 CP 单点 | — | 设计验证：CP 不在 AddRows 热路径 ✓ |

---

## 3. 六阶段实施计划

每阶段独立 commit + 可回退，前 3 阶段不破坏 coord/worker 外部接口。

### Phase A — 代码按角色重组（1 周）

**目标**：现有 coord/worker 代码物理上按角色重新组织，二进制行为完全不变。

**改动**：
- 新 crate `crates/roles`，包含 `qn/`, `pe/`, `idx/`, `cp/` 四个模块
- 每模块暴露 `pub async fn run(...) -> Result<()>`
- coord/worker 的 `bin/main.rs` delegate 到 `roles::{qn_cp, pe_idx}::run()`
- 逻辑保持原位（crates/coordinator + crates/worker 仍是代码实际宿主），`crates/roles` 做组合

**退出**：
- 所有 E2E 套件（10/10）和 lib 单测（181/181）通过
- 配置文件、对外行为 0 变更
- 新 `cargo run -p lance-coordinator` 起动时间不回归

### Phase B — `lance-monolith` 二进制（1 周）

**目标**：发布 `lance-monolith`，`--roles` 参数控制角色组合，loopback 内部对接。

**改动**：
- 新 bin crate `crates/monolith`
- `--roles qn,pe,idx,cp`（默认全开），或等价 `qn,cp` / `pe,idx`
- 所有角色共享同一 MetaStore handle + shutdown signal
- Quickstart 文档改推荐 monolith

**退出**：
- Monolith 跑完整 E2E
- 启动时间 < coord + worker 之和
- coord/worker 保持工作

### Phase C — CP 真正独立 + Routing Push Invalidation（2 周）

**目标**：CP 作为独立角色运行；QN 通过 gRPC 订阅 routing 变更。

**改动**：
- 新 proto `ClusterControl` service：`ResolveTable` / `SubscribeRouting` (streaming) / `GetPolicy` / `RegisterNode` / `Heartbeat`
- QN 持 routing cache + subscription，30s TTL + push invalidation
- ConnectionPool 健康检查从 coord 迁到 CP
- QN 从 CP 拉 `healthy_workers` 做 scatter

**风险**：R2.1 / R2.9 — push invalidation 失效时降级到 TTL。Chaos 测试：杀 CP，QN cache 服务至少 30s。

**退出**：
- 10K QPS 下 QN↔CP 调用率 < 0.1 / request
- CP 挂掉场景 QN 仍服务 30s
- E2E 全绿

### Phase D — IDX 独立 + JobQueue（2 周）

**目标**：Indexer 独立进程；从 CP 领 compact / index-build / orphan-gc job。

**改动**：
- 新 proto `JobQueue` service：`Lease` / `Renew` / `Complete` / `Fail`
- worker（PE）不再自己 compact
- CP 实现 job lease / renew / TTL-based key on MetaStore
- IDX long-poll lease
- `orphan_gc` 从 coord 后台 → CP 入队 → IDX 执行
- `create_index` RPC 由 CP 入队

**风险**：R2.5 — 重复 compact 浪费 compute。Chaos：kill IDX 中途，job 在 < 2× TTL 内被接管。

**退出**：
- 24h soak 里 compact 不影响 query P99 > 5%
- Chaos 测试通过
- Grafana 加 IDX backlog 面板

### Phase E — 四 binary 发布（1 周）

**目标**：发布 `lance-qn` / `lance-pe` / `lance-idx` / `lance-cp`；Helm chart 更新；旧 bin 加 deprecation warning。

**改动**：
- 4 bin crate，每个 10-20 行 wrapper
- Helm chart 新增 4 个 Deployment/StatefulSet
- coord/worker 启动打 deprecation log

**退出**：
- K8s 多 pod 4-binary 跑通 E2E
- HPA metrics 接到 Prometheus（QN HTTP QPS / PE read_lat / IDX backlog）

### Phase F — 迁移文档 + 性能验证（1 周）

**目标**：完整迁移指南 + 性能对比实证。

**改动**：
- `docs/ARCHITECTURE_V2.md` 正式版（现 `_PLAN.md` 毕业）
- `docs/MIGRATION_0.2_TO_BETA5.md`
- Bench：mix workload 下 4-binary vs monolith vs coord+worker
  - 混合读写 P99 改善
  - Cold start（新 PE 起来多久能服务）
  - Scale 测试（加 1 个 QN 能吃多少 QPS）

**退出**：
- `docs/PROFILE_4BINARY.md` 入库
- 混合 RW P99 改善 ≥ 20%（IDX 隔离直接收益）
- 0 功能回归

---

## 4. 总投入

| 阶段 | 工期 | 风险 |
|---|---|---|
| A 代码结构 | 1w | 🟢 低 |
| B monolith | 1w | 🟢 低 |
| C CP + routing push | 2w | 🟡 中 |
| D IDX + JobQueue | 2w | 🟡 中 |
| E 四 binary | 1w | 🟢 低 |
| F 迁移验证 | 1w | 🟢 低 |
| **总计** | **8 周** | |

发版节奏：A-B 完成 → beta.5；C-D → beta.6 experimental；E-F → beta.7 stable；0.3 移除 coord/worker。

---

## 4.5 决策锁定（Phase C 开工前）

这三条是 Phase C 启动前必须定的架构决策。每条都写清"选择 / 理由 / 证据"，避免实施中反复。

### D1. QN routing cache 断连降级 — **选择：TTL 保底**

**场景**：QN 通过 gRPC `SubscribeRouting` 长连接订阅 CP 的 routing 更新。连接断了怎么办？

**选择**：本地 cache 带 30s TTL。订阅断连后继续用最后的 routing snapshot，直到 TTL 过期才开始拒服务。

**理由**：CP 的 rolling restart / 网络抖动应该对客户端透明。Milvus DataCoord → QueryCoord 的通知也是这个 pattern（`milvus/internal/util/sessionutil`）。硬 fail 会放大 CP 故障域。

### D2. CP 形态 — **选择：crate 库 + async task（Pattern B）**

**场景**：CP 要不要独立进程 / 独立二进制 / 独立 gRPC server？

**选择**：CP 是一个 `roles::cp::run(...)` 库函数；monolith 里和其他角色在同 runtime 里起 tokio task；Phase E 再打可选的独立 `lance-cp` bin（同 crate 不同 entry）。

**理由 + 证据**：后 2020 分布式数据库的主流是 Pattern B——单 binary 按 role 组合，而不是 Pattern A 的强制拆进程。调研结果：

| 系统 | Binary 数 | 可组合 | 模式 | 证据链接 |
|---|---:|---|---|---|
| Milvus | 1 | ✅ role flags | B | [cmd/roles](https://github.com/milvus-io/milvus/blob/master/cmd/roles/roles.go) |
| CockroachDB | 1 | ✅ symmetric peer | B | [architecture](https://www.cockroachlabs.com/docs/stable/architecture/overview) |
| Qdrant | 1 | ✅ peer Raft | B | [distributed](https://qdrant.tech/documentation/guides/distributed_deployment/) |
| etcd | 1 + `embed` pkg | ✅ | B | [embed](https://pkg.go.dev/go.etcd.io/etcd/server/v3/embed) |
| Consul | 1 | ✅ `-server` flag | B | [agent](https://developer.hashicorp.com/consul/commands/agent) |
| Neon | 6+ | 部分（dev）| A (prod) | storage_controller is own bin |
| TiDB PD | 3 | ❌ 独立 Raft | A | 小 quorum vs 大 TiKV fleet 规模不对称 |

**7/8 票选 Pattern B**。TiDB 选 A 是因为 PD 本身是小 Raft quorum 要服务几千个 TiKV 节点，规模不对称。我们没这个不对称（多租户单集群场景）。

选 B 没有锁死——Phase E 仍可以随时打 `lance-cp` 独立 bin（同 crate + 不同 `[[bin]]`）。选 A 则要立即承担部署 / 配置 / supervision 的额外税。

### D3. Schema 存储 — **选择：MetaStore 集中版本化**

**场景**：Arrow schema 目前只存在每个 shard 的 Lance manifest 里。GetSchema RPC 随便挑一个 PE 问。要改成 MetaStore 集中存吗？

**选择**：改成 MetaStore 集中存。每张表一个 `schema_version: u64`（单调递增）+ 完整 Arrow IPC schema。

**理由**：分散式 schema 导致**能力损失**：
- **原子 DDL 做不了**：ALTER TABLE 要同时改 N 个 shard 的 schema；没有全局锚点就没法协调
- **Schema 演化做不了**：add column / drop column 需要版本化（历史查询用旧 schema、新查询用新 schema）
- **Time-travel 做不了**：客户端指定 version N 读取需要知道 version N 的 schema 长什么样
- **GetSchema 性能差**：随便挑 worker 问，一次网络 RTT；集中存就是内存取

**代价**：MetaStore 多一个 key family `/tables/{name}/schema/{version}`，一个写路径（CreateTable / DDL），一个读路径（scatter 前 resolve）。CAS 保证并发 DDL 的串行化。

---

## 5. 竞品设计吸收

| 能力 | 来源 | 应用点 |
|---|---|---|
| 4 节点类型划分 | LanceDB Enterprise | §1.1 |
| Plan Executor NVMe cache | LanceDB Enterprise FAQ | PE 角色 |
| Job queue with lease | Milvus DataCoord CompactionPlanManager / sidekiq 模式 | Phase D |
| Leader-per-operation via CAS | Neon pageserver / etcd 模式 | CP 多副本 I2 |
| Long-poll subscribe for routing | Milvus DataCoord → QueryCoord 通知 | Phase C |
| Monolith 二进制 | SurrealDB / CockroachDB 开发模式 | I6 |
| Per-role HPA metric | Pinecone / Qdrant Cloud 扩容模型 | Phase E |

## 6. 测试覆盖底线

每阶段退出前必须满足：

- 现有 lib 单测 0 回归
- 现有 E2E 套件 0 回归
- 新增的每条 invariant（I1-I7）至少 1 个针对性测试
- Chaos 场景（Phase C-D）至少覆盖 kill-one-instance / network-partition / clock-skew 中的两个
- mixed-RW bench 数据入库 `docs/PROFILE_*.md`

详情见每阶段的 "退出" 一节。
