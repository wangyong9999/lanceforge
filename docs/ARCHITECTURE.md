# LanceForge 架构总览

面向**工程师而不是营销**的设计文档：解释为什么这样设计、为什么不做常见其他方案。

## 一句话

LanceForge 是**在对象存储上做分布式向量检索**的计算层。数据、持久化、原子性全部交给对象存储 + Lance manifest CAS；LanceForge 只负责路由、并发、结果合并。

## 核心架构

```
            Client (Python SDK / gRPC)
                      │
                      │ AnnSearch / AddRows / etc.
                      ▼
         ┌────────────────────────────┐
         │    Coordinator (stateless)  │
         │  • 路由: table → [shards]    │
         │  • 扇出: shards → workers   │
         │  • 合并: global top-K        │
         │  • 鉴权: RBAC + API key     │
         │  • 指标: Prometheus         │
         └──────┬──────────────────────┘
                │ gRPC (target_shard)
                ▼
   ┌────────────┬────────────┬───────────┐
   │  Worker 0   │  Worker 1  │  Worker 2  │  (gRPC handler only)
   │  └─ Lance   │  └─ Lance  │  └─ Lance  │  (lancedb::Table handles)
   └──────┬─────┴──────┬─────┴──────┬─────┘
          │            │            │
          └────────────┼────────────┘
                       ▼
            ┌─────────────────────────┐
            │  Object Storage         │
            │  (S3 / GCS / Azure)     │
            │                         │
            │  /bucket/table.lance/   │
            │    _versions/0.manifest │
            │    _versions/1.manifest │
            │    _versions/...        │
            │    data/*.lance         │
            └─────────────────────────┘
                       ▲
                       │ CAS manifest writes
            ┌──────────┴──────────┐
            │ S3MetaStore          │
            │ /bucket/metadata.json│
            │ (coordinator routing │
            │  state, version-CAS)  │
            └──────────────────────┘
```

## 为什么做这些决策

### 为什么不做副本 2PC / quorum writes

因为数据存在**对象存储**上，不在 LanceForge 里。S3/GCS/Azure 自身是 11 个 9 持久化 + 跨 AZ 副本。Lance 用 **manifest CAS** 保证写原子性：

1. 写入者读当前 manifest version N
2. 生成新的 data file + manifest N+1
3. 原子 PUT-if-not-exists 到 `_versions/N+1.manifest`
4. 失败 → 重试（读到最新 manifest，rebase 后重新提交）

所以：
- **不需要** 多副本 2PC（只有一份权威数据）
- **不需要** 分布式锁（CAS 就是分布式锁）
- **不需要** 写 quorum（OBS 的 PUT 本身就是 quorum）

`replica_factor` 参数名误导——实际是**读并行度**：多个 worker 各自打开同一个 OBS URI，一个 worker 挂了另一个顶上。不是数据冗余。

### 为什么 Coordinator 无状态

- 元数据存 S3MetaStore（全局共享，CAS 协调）
- 路由缓存可以从元数据随时重建
- 扩容 = 再起一个 coordinator（客户端 LB 自动发现）
- 崩溃 = 重启自动恢复

唯一例外：单机开发用 FileMetaStore，本地文件，只能一个 coordinator。

### 为什么 Worker 需要 StatefulSet

- Shard 路由元数据里绑定 `worker_id`（如 `w0`, `w1`）
- K8s StatefulSet 给稳定 DNS 名：`worker-0.worker.svc` → `w0`
- 否则 pod 重启后 IP 变了，路由失效

但是 Worker **本身**还是无状态的（所有数据在 OBS）。StatefulSet 只是为了稳定身份。

### 为什么 k 扇出到所有 shard 而不是单 shard

向量空间 ANN 搜索没有"range partitioning"的概念——不知道最近邻在哪个 shard。只能全扫。

优化：每个 shard 返回 `k × oversample_factor` 个本地 top-K，coordinator 全局合并成真正的 top-K。

### 为什么 Add 是 round-robin 路由

- ANN 查询本来就要扇出所有 shard，Add 的分布式策略不影响读延迟
- Round-robin 简单 + 均衡
- Hash by PK 会更一致（Upsert 更 robust），但要求 schema 有 PK 概念——目前 LanceForge 不假设这个

trade-off：Upsert 的 hash-route（Phase 16）和 Add 的 round-robin 不对齐，小概率下同 id 会在两个 shard 都存在。见 LIMITATIONS.md。

### 为什么 Delete 是 shard-level 而不是全局 fan-out

Phase 15 改的。之前 Delete 扇出到每个持有该 shard 的 **worker**；如果 replica_factor=2，两个 worker 同时对同一个 OBS URI 做 Lance delete → manifest CAS 竞争浪费。

现在：Delete per-shard 只发给该 shard 的 primary（或备用）worker 一次。

## 核心模块

### `crates/coordinator`
- `service.rs` — gRPC 入口，鉴权，路由
- `scatter_gather.rs` — 扇出到 worker，合并结果
- `connection_pool.rs` — worker 连接池 + 健康检查
- `merge.rs` — 全局 top-K 算法

### `crates/worker`
- `service.rs` — gRPC handler (薄)
- `executor.rs` — Lance 表操作（核心逻辑）
- `cache.rs` — 查询结果缓存

### `crates/meta`
- `store.rs` — MetaStore trait + FileMetaStore + S3MetaStore
- `state.rs` — MetaShardState (路由的权威来源)
- `shard_manager.rs` — assign_shards 算法（最少负载 + 大小感知）

### `crates/common`
- `config.rs` — ClusterConfig + 各种子 config
- `shard_state.rs` — ShardState trait + StaticShardState（YAML 派生）
- `auto_shard.rs` — 自动分片工具
- `metrics.rs` — Prometheus 指标

### `crates/proto`
- `lance_service.proto` — gRPC 协议（唯一 source of truth）

## 数据流详解

### Search 路径

1. Client → Coord `AnnSearch(table, vector, k=10, nprobes=10)`
2. Coord: 鉴权 → 拿 shard 路由 `[s0→w0, s1→w1, s2→w2]`
3. Coord: 并发 gRPC 扇出
   - `w0.ExecuteLocalSearch(s0, vector, k=20)` (oversample)
   - `w1.ExecuteLocalSearch(s1, vector, k=20)`
   - `w2.ExecuteLocalSearch(s2, vector, k=20)`
4. 每个 worker：`lance_table.search(vector).nearest_to(vec).limit(20)` → 本地 top-20
5. Coord: 合并 60 条候选 → 全局 top-10 → Arrow IPC 回给 client

### Write 路径 (Add)

1. Client → Coord `AddRows(table, rows)`
2. Coord: 鉴权 → round-robin 选一个 shard `s1` → 该 shard 的 primary = `w1`
3. Coord → `w1.ExecuteLocalWrite(target_shard=s1, data=rows, write_type=Add)`
4. Worker w1: `lance_table.add(rows).execute()` → Lance 生成新 manifest，CAS 提交到 OBS
5. 返回新 version

### CreateTable 路径

1. Client → Coord `CreateTable(name, initial_rows)`
2. Coord: 鉴权 → 选 N 个健康 worker (N = shard count)
3. 切 N 份数据：`split_batch(rows, N)`
4. 并发 `CreateLocalShard` 给每个 worker
5. Saga：任一失败 → UnloadShard 所有成功的（不留孤儿）
6. 注册路由 + 写入 MetaStore (CAS)
7. 返回

## 一致性模型

| 操作 | 一致性 |
|---|---|
| Add | 写后立即读到（同 shard 内） |
| Delete | 写后立即读到（同 shard 内） |
| Upsert | 写后立即读到（hash-target shard 内） |
| CreateTable | MetaStore CAS → 其他 coordinator 下次查 routing 时可见（~10s 后） |
| DropTable | 同上 |
| 跨 shard 的 query-after-write | 可能看到部分——Lance 表各自独立，没有全局事务 |

**重要**：LanceForge 不提供跨表事务、跨 shard 原子性。单 shard 内 Lance 是 serializable。

## 并发模型

- Coordinator: Tokio multi-threaded runtime + global `query_semaphore` (默认 200)
- Worker: 同上，每次 query 克隆 Lance table handle (cheap Arc)
- **客户端必须每线程一个 gRPC channel** 才能真正并发（Phase 17 发现）

## 历史反思（为什么不做某些功能）

详见 Phase 15 commit message 和 LIMITATIONS.md:

- ❌ 副本 2PC 写入 — OBS + manifest CAS 已解决
- ❌ Worker resync 协议 — 冷启动天然读 OBS 最新
- ❌ Multi-coord lease/leader election — MetaStore CAS 够用
- ❌ 读副本 failover 机制（单独的）— scatter_gather 已 cover
- ❌ Gather 流式化 — worker 侧 oversample + max_k 已封顶

这些都是"传统 Shared-Nothing DB 的惯性思维"，OBS-native 架构不需要。

## 与其他方案的对比

| 方案 | 存储 | 计算 | 写原子性 |
|---|---|---|---|
| Milvus | 本地 + 对象存储（混合）| Segcore + 协调 | 自研 WAL + 多副本 |
| Qdrant | 本地（每节点一份）| 嵌入式存储 | 自研 Raft |
| Weaviate | 本地 | 嵌入式 | 节点级 |
| **LanceForge** | **纯对象存储** | **无状态计算层** | **Lance manifest CAS** |

LanceForge 更接近 Snowflake / Neon 的**存算分离**思路，而非 Cassandra 的 shared-nothing。

## 未来方向

- **Add hash-by-PK**：让 Upsert 真正一致
- **Query-side streaming**：大结果集从 worker 流到 coord 到 client
- **OpenTelemetry**：跨节点 tracing
- **Schema evolution**：ALTER TABLE 操作
- **Multi-region**：跨地域复制（对象存储本身可做）

每一条都等**真实用户需求驱动**，不提前做。
