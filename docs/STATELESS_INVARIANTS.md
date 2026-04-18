# 无状态不变量（SaaS-first 审计）

**建档时间**：2026-04-18
**对应 ROADMAP 项**：`ROADMAP_0.2.md` §B1（SaaS 无状态化完整性审计）
**目的**：枚举代码库中每一处"状态"，按 SaaS-first 框架分类为 **Truth / Cache / Config**，识别阻碍 pod 任意替换的 invariant。

---

## 一、分类约定

| 类别 | 定义 | SaaS 验收 |
|---|---|---|
| **Truth** | 丢失即意味数据错误或功能性丢失 | 必须在对象存储，支持 CAS |
| **Cache** | 丢失只影响性能或延迟，重建即可 | 允许本地内存，失效要可 eventually-consistent |
| **Config** | 启动时从 config 源读入，运行期只读 | 允许本地；但"config 源"若是本地盘则违反 SaaS |

**核心不变量**（用于验收）：**任何 pod 被 kill 后，新 pod 启动路径只能做 (1) 读 config + (2) 读 OBS；如果需要读本地盘才能进入 serving 状态，则违反 SaaS-first。**

---

## 二、全量清单（按模块）

### 2.1 Coordinator 主进程

| 位置 | 类型 | 分类 | 备注 |
|---|---|---|---|
| `service.rs:28` `pool: Arc<ConnectionPool>` | Cache | ✅ | 连接池状态，health check 自愈 |
| `service.rs:29` `shard_state: Arc<dyn ShardState>` | Truth 或 Cache | ⚠️ | 取决于 Static vs MetaShardState，详见 §3.1 |
| `service.rs:30` `pruner: Arc<ShardPruner>` | Config | ✅ | 启动期构建，不可变 |
| `service.rs:35` `query_semaphore` | Cache | ✅ | 进程级并发限流，每进程独立 |
| `service.rs:42` `write_counter: AtomicUsize` | Cache | ✅ | round-robin 游标，重启即重置，行为无语义差异 |
| `service.rs:46` `shard_uris: Arc<RwLock<HashMap>>` | Cache | ✅ | 来自 MetaStore 的反向索引，可重建 |
| `service.rs:48` `auth: Option<Arc<ApiKeyInterceptor>>` | Config | 🔴 详见 §3.2 | 来自 config YAML，**不支持运行时轮换** |
| `service.rs:56` `bg_shutdown` | — | ✅ | 进程生命周期通知，非状态 |
| `service.rs:58` `meta_state: Option<Arc<MetaShardState>>` | Truth | ✅ | MetaStore 后端 |
| `service.rs:60` `ddl_locks: HashMap<String, Mutex>` | Cache | ✅ | 进程级互斥；多 coord 靠 MetaStore CAS |
| `auth.rs:53` `keys: Arc<HashMap<String, Role>>` | Config | 🔴 | 不可变；**加/撤 key 必须重启** |

### 2.2 Connection Pool

| 位置 | 类型 | 分类 | 备注 |
|---|---|---|---|
| `connection_pool.rs:41` `workers: HashMap<String, WorkerState>` | Cache | ✅ | 健康态 + gRPC client；A1 修复确保 stale channel 会被替换 |
| `connection_pool.rs:42` `endpoints: HashMap<String, (String, u16)>` | Cache | ✅ | 来自 cluster 视图，可从 MetaStore 重建 |
| `connection_pool.rs:47` `shutdown` | — | ✅ | 生命周期通知 |

### 2.3 Worker 进程

| 位置 | 类型 | 分类 | 备注 |
|---|---|---|---|
| `executor.rs:34` `tables: Vec<(String, lancedb::Table)>` | Cache | ✅ | lancedb Table 句柄；启动时从 assigned shards 重建 |
| `executor.rs:42` `shard_row_counts: HashMap<String, AtomicU64>` | Cache | ✅ | 心跳上报用，poller 维护 |
| `executor.rs:47` `shard_versions: HashMap<String, AtomicU64>` | Cache | ✅ | Phase 18 原子 poller 产物 |
| `cache.rs:61-68` QueryCache 全套 | Cache | ✅ | TTL-bounded；miss 即重算 |

### 2.4 Meta（持久化）

| 位置 | 类型 | 分类 | 备注 |
|---|---|---|---|
| `store.rs:84` `FileMetaStore.data: RwLock<StoreData>` | Cache | 🟡 | 后端是本地文件，**SaaS 下必须禁用** |
| `store.rs:87` `FileMetaStore.lock_file: File` | Config | 🟡 | 进程级锁，SaaS 不相干 |
| `store.rs:237-241` S3MetaStore 全套 | Truth | ✅ | ETag CAS 实现在 293-308 |
| `state.rs:18-19` `MetaShardState` | Truth | ✅ | 落地 MetaStore |
| `session.rs:34-38` SessionManager (`store` + `sessions` 内存 cache) | Truth + Cache | ✅ | Cache 来自 MetaStore，重启可重建 |

### 2.5 本地盘写点

| 位置 | 路径模式 | 分类 | 备注 |
|---|---|---|---|
| `coordinator/bin/main.rs:62` `tokio::fs::write(auto_config_path)` | `/tmp/lanceforge_autoshard_{pid}.yaml` | 🔴 Truth | **违反 SaaS-first**：auto-shard 模式生成的 config 在本地，重启丢失。详见 §3.3 |
| `meta/store.rs:145-156` FileMetaStore fsync 写 | `{metadata_path}` (local fs) | 🟡 | 只在 file:// 后端出现，SaaS 硬拒绝即可 |
| `worker/bin/main.rs:105-107` TLS cert/key 读 | `config.security.tls_cert/key` | Config | ✅ K8s secret mount 模式标准做法 |
| `coordinator/bin/main.rs:170-171` TLS cert/key 读 | 同上 | Config | ✅ |

### 2.6 配置（启动只读）

| 配置模块 | 字段要点 | 分类 | 备注 |
|---|---|---|---|
| `ClusterConfig` | 整体 config | Config | 来自 YAML；不支持 reload |
| `ServerConfig` | 超时、并发、响应大小 | Config | ✅ |
| `SecurityConfig` | TLS + api_keys | Config + 🔴 Truth | api_keys 本应是 Truth |
| `CacheConfig` | TTL、read_consistency | Config | ✅ |
| `HealthCheckConfig` | 健康检查参数 | Config | ✅ |
| `CompactionConfig` | worker 压实 | Config | ✅ |
| `OrphanGcConfig` | GC 策略 | Config | ✅ 默认关 |

### 2.7 Metrics / 无状态可观测

`metrics.rs:21-36` 整套 Metrics 都是进程级 AtomicU64 / RwLock。每进程独立。**对 SaaS**：Prometheus 层聚合即可；不需要跨进程同步。**OK。**

---

## 三、Gap 清单（按严重度）

### 🟡 ~~Gap A~~ ✅ 主要闭合于 `8cbcbb3` — API Key / RBAC 仅在 config（service.rs:48 + auth.rs:53 + coordinator/bin/main.rs:112-114）

**现状**：`SecurityConfig.api_keys` 在 YAML 里，启动读入后 `ApiKeyInterceptor.keys` 就 frozen。

**⚠️ 严重性加乘**：`coordinator/bin/main.rs:112-114` 把 legacy `api_keys`（非 RBAC 格式）**默认赋 Admin 角色**：
```rust
for k in &config.security.api_keys {
    role_map.entry(k.clone()).or_insert(Role::Admin);
}
```
这意味着：**轮换一个旧格式 key 等于发放管理员权限**。B1.2.3 里要同步修这个语义默认（改为 ReadOnly，或要求显式指定）。

**SaaS 影响**：
- 无法运行时添加/撤销 key → 客户要求轮换 key 时必须滚动重启
- 多 coord 部署下，key 变更需要同步所有 coord 的 config 文件
- 违反 SaaS "state in OBS" 原则
- Legacy key 默认 Admin 让权限最小化原则失效

**闭合状态（`8cbcbb3`）**：
- ✅ MetaStore 新增 `auth/keys/{key}` 前缀条目
- ✅ `ApiKeyInterceptor` 改为 `Arc<RwLock<Arc<HashMap>>>` + `reload()`；coord 每 60s 从 MetaStore 刷新
- ✅ 启动期 `bootstrap_metastore_if_empty` — 老 config YAML 首次启动自动迁进 MetaStore
- ⏳ **仍遗留**：legacy `api_keys`（非 RBAC 格式）默认 Admin 角色——计划在 0.3.0 flip 到 Read。参见 `COMPAT_POLICY.md` §9
- ⏳ **未实现**：admin gRPC RPC (`AddApiKey` / `RevokeApiKey`)——当前靠直接写 MetaStore 做 key 管理；`lance-admin auth` CLI 是 R6 的工作

---

### 🔴 Gap B：Auto-shard 生成的 config 落本地盘（coordinator/bin/main.rs:62）

**现状**：auto-shard 模式（coordinator 从空 YAML 推导集群结构）把生成的最终 config 写到 `/tmp/lanceforge_autoshard_{pid}.yaml`。workers 读这个文件。

**SaaS 影响**：
- 新 pod 起来没这个文件 → 不知道 shard 分布 → 无法服务
- 如果写到 `emptyDir` 或 ephemeral volume，pod 被调度到不同 node 就丢
- 实际上 auto-shard 生成的内容本就该进 MetaStore

**拟修方向**（B1.2 任务）：
- auto-shard 生成的配置直接写 MetaStore（key `cluster/auto_shard_config`）
- workers 改为从 coordinator 拉（或 MetaStore 直读，避免启动顺序依赖）
- 删除本地 tmp 文件路径；如果向下兼容需要，保留一个 env flag `LANCEFORGE_DEV_AUTOSHARD_LOCAL_FILE`

**估工**：~1 周

---

### 🟡 ~~Gap C~~ ✅ 闭合于 `d31d3de` — FileMetaStore 仍可在生产模式启用（store.rs:84-87 整个 impl）

**现状**：`metadata_path` 如果是本地路径就走 FileMetaStore。没有守卫机制。

**SaaS 影响**：
- 生产误配就成单点故障
- LIMITATIONS §3 已文档化但没有运行时强制

**拟修方向**（B1.2 任务）：
- 引入 `deployment_profile: {dev, saas, self_hosted}` 配置字段
- `deployment_profile=saas` 时启动期硬校验：`metadata_path` 必须是 `s3://` / `gs://` / `az://`
- 不满足 fail-fast 带明确错误信息
- DEPLOYMENT.md 默认配置示例改为 saas profile

**估工**：~2 天

---

### 🟡 ~~Gap D~~ ✅ 闭合于 `d31d3de` — StaticShardState 仍是默认兜底（shard_state.rs:113-117）

**现状**：如果没配 `metadata_path`，默认用 StaticShardState，所有 rebalance / MoveShard 在 coord 重启后丢失。

**SaaS 影响**：
- 与 Gap C 耦合：saas profile 必须强制走 MetaShardState
- 代码层面 StaticShardState 本身是合法的 dev 模式实现

**拟修方向**（合并进 Gap C）：
- saas profile 启动期拒绝 StaticShardState 初始化
- 保留 Static 作为 dev-only；OPERATIONS.md 明说

**估工**：~1 天（大部分在 Gap C 里）

---

### 🟢 Gap E：没有审计日志（不是现状 bug，但是 SaaS 空白）

**现状**：没有任何 audit log 相关代码。

**SaaS 影响**：企业合规必需。

**拟修方向**：R3（Strong-Recommend 层），不在 B1 范围。

---

### 🟢 Gap F：没有 per-key quota（不是现状 bug）

**现状**：`query_semaphore`（service.rs:35）是进程级全局，不区分 key。

**SaaS 影响**：多租户 SaaS 硬伤。

**拟修方向**：R2，不在 B1 范围。

---

## 四、已有好的 invariant（可以保留+强化）

- **S3MetaStore 的 ETag CAS**（store.rs:293-308）：正确用 `object_store::PutMode::Update` 做乐观锁。是 SaaS 多 coord 的基石
- **Session TTL + MetaStore 落地**（session.rs）：worker 心跳超时 30s 自动清理，Truth 在 OBS
- **Connection pool 的 A1 修复**：unhealthy→healthy 过渡重建 channel，避免 stale 引用
- **Worker 版本 poller**（executor.rs:130-159）：atomic 读 version，读热路径零 I/O
- **ShardManager 是 pure function**（shard_manager.rs）：没有隐藏状态，多 coord 跑同输入得同结果

这些是 SaaS-first 已经做对的部分，在后续工作中不要回退。

---

## 五、B1.2 工作项拆解（放入 ROADMAP §B1）

按优先级排：

| ID | 工作 | Gap | 估工 | 阻塞 saas profile 上线？ |
|---|---|---|---|---|
| B1.2.1 | 引入 `deployment_profile` 配置 + 启动期校验 | C + D | 2d | ✅ |
| B1.2.2 | auto-shard config 从本地 tmp → MetaStore | B | 1w | ✅ |
| B1.2.3 | API key registry 从 config → MetaStore + hot-reload | A | 2w | ⚠️ 不阻塞首发 alpha，但阻塞 beta |
| B1.2.4 | 兼容性 & 迁移测试：老 config → saas profile 升级路径 | — | 3d | ✅ |

**总估**：~3 周（可部分并行）。符合 B1 整体 4 周 alpha 目标。

---

## 六、验收用 chaos scenario（喂给 B1.6）

当 B1.2 全部完成后，以下 chaos 场景应该全部通过：

1. **Coord kill + 新 pod 不同 hostname 接管**：5s 内 ListTables 工作
2. **Worker kill + 新 pod 启动**：新 pod 从 MetaStore 读 assigned shards，无需本地文件
3. **Coord auto-shard 场景下 coord 全灭 + 全部重启**：auto-shard 结果从 MetaStore 恢复
4. **运行时 add-key → 新 pod 立刻认识**：不需要配置滚动
5. **`deployment_profile=saas` + `metadata_path=/tmp/x.json`**：启动期 fail-fast，日志清晰

每个 scenario 对应 `chaos/scenario_*.py`（B5 框架产物）。

---

## 七、开放问题（待 review）

1. **Lancedb Table 对象缓存**：worker `executor.rs:34` `tables: Vec<(String, lancedb::Table)>` 持有 lancedb connection。如果 lancedb 内部有本地 checkpoint 文件，会不会违反 SaaS？需要查 lancedb 本身的 stateless 属性。（建议：issue upstream）

2. **TLS 证书轮换**：目前 cert/key 在启动期读本地盘。K8s 里常用方式是 secret mount + reload-on-signal。0.2 做不做热轮换？先记录，R 层再定。

3. **Embedding 模型**：config 里的 `embedding.provider=sentence-transformers` 路径下，模型文件下载到 worker 本地 cache。这是 cache 性质（丢失可重新下载）但首次冷启动延迟可能过大。建议：
   - alpha：文档化 + docker image 预装常用模型
   - beta：MetaStore 记录"模型校验和"，pod 启动期验证

---

**结论**：0.2.0-alpha.1 累计闭合 Gap A（🟡 主要闭合）、C、D 三项，Gap B 部分闭合（saas profile 拒绝 --auto-shard，完整 MetaStore 写入延到 B1.2.2b）。整体代码库的 SaaS-first 化程度从基线 ~75% 推到 **~92%**。剩余主要是：
- 🟡 Gap A 的 0.3.0 legacy-key-role flip
- 🔴 Gap B 的完整 auto-shard → MetaStore（B1.2.2b，post-alpha）
- 🟢 Gap E audit log（R3）
- 🟢 Gap F per-key quota（R2）
