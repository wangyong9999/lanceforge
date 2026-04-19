# LanceForge 已知限制

诚实文档：记录**架构层面的真实边界**和**已知但未修复的行为差异**，避免用户踩到后才发现。

## 1. Upsert 一致性：与 Add 路由不对齐（已部分修复）

**背景**：`AddRows` 默认 round-robin 选 shard；`UpsertRows` 按 `on_columns` 哈希分 shard。两种策略不对齐 → 混用时同 id 可能落在两个 shard，CountRows +1 而不是 0（行数膨胀）。

**修复**（生效于 AddRowsRequest 新增 `on_columns` 字段的版本起）：
- `AddRows` 现在接受可选的 `on_columns`。传入后，Add 用和 Upsert 完全相同的 hash-partition 逻辑，同一 key 必然落在同一 shard。
- Python SDK：`client.insert(table, data, on_columns=["id"])`
- gRPC / REST：在 AddRowsRequest 里带 `on_columns=["id"]`

**使用约定**（违反会退化为原 bug）：
- 如果你的表有 primary key，**对这张表的每一次 Add 和 Upsert 都要传相同的 `on_columns` 列表**。
- 不想要 PK 语义的表继续用默认 `AddRows`（不传 on_columns），round-robin 行为不变。
- 服务端目前不会记住一张表"曾经用过哪个 on_columns" → 不做跨调用一致性校验。由调用方（或 SDK 包装层）保证。

**还没做的**：让 table 层面显式声明 PK、服务端强制校验。等使用反馈后再决定要不要加。

## 2. "replica_factor" 不是数据副本

**名字误导**。config.yaml 里的 `replica_factor: 2` 意思是**读并行度**：两个 worker 打开同一个 OBS URI，用于查询 QPS 扇出 + 单 worker 故障时读 failover。

- **不是数据冗余**：数据一份存在对象存储上，持久化由 S3/GCS/Azure 自身保证
- **写只发给一个 worker**（主 worker），Lance manifest CAS 保证原子提交
- 两个 worker 的本地状态是 OBS 的读缓存，崩溃重启后重新从 OBS 加载

YAML 字段名保留（向下兼容），未来可能重命名为 `read_parallelism`。

## 3. MetaStore 路径选择影响可用性

- `metadata_path: some/local/path.json` → FileMetaStore → **单 coordinator 模式**。第二个 coordinator 启动会踩同一文件但无协调
- `metadata_path: s3://bucket/meta.json` → S3MetaStore → **多 coordinator HA 模式**。多个 coordinator 可同时运行，通过对象存储 CAS 协调

HA 部署必须用 S3MetaStore。文档会在 DEPLOYMENT.md 里明说。

## 4. DropTable 只清理本 coordinator 记录的 URI

- DropTable 会删除**通过本 coordinator CreateTable 创建的 shard `.lance` 目录**
- 如果通过其他手段（手写 YAML、直接 lance.write_dataset）塞进集群的数据，`shard_uris` 注册表里没有 → DropTable 不会删
- 配合 `orphan_gc.enabled: true`（Phase 16）可以周期性清理未登记的 `.lance` 目录

**对象存储上的孤儿当前不会被 GC**：orphan_gc 目前只支持本地 fs。S3/GCS 场景需要在账单侧盯着，或手动 `aws s3 rm`。

## 5. gRPC 消息大小受 `max_response_bytes` 约束

- 服务端 gRPC receive limit = `config.server.max_response_bytes + 16 MiB`（默认 64 + 16 = 80 MiB）
- 单次 CreateTable/AddRows 的 Arrow IPC 不能超过此限制
- 大批量写入请拆分成多次 `AddRows`，或提高 `max_response_bytes`（内存占用随之上涨）

## 6. 查询结果有上限

- 单次 Search 的 `k + offset` 不能超过 `max_k`（默认 10000）
- 响应 payload 超过 `max_response_bytes` 会被 coordinator 二分截断，设置 `truncated=true` + `next_offset`
- 要翻页：用 `offset` + `next_offset`，客户端循环消费

## 7. Embedding 自动化仅限少数模型

- 配置 `embedding.provider`（openai / sentence-transformers）后，`query_text` 会自动嵌入成向量
- OpenAI 需要 API key（`OPENAI_API_KEY` 或 config）
- 本地 sentence-transformers 首次运行会下载模型（依赖 `torch + transformers` Python 环境，coordinator 里实际通过子进程调用）
- 其他模型请手动 embed 后传 `query_vector`

## 8. 单机 fs 部署的 fsync 粒度

- FileMetaStore 在 Phase 12 加了 file + dir fsync（crash-safe）
- 但 Lance 底层 dataset 写入（Worker 进程）依赖 Lance 自身的 fsync 策略
- 真生产请用 OBS 后端；单机 fs 仅适合开发/小规模场景

## 9. 多租户 / 资源隔离

**0.2.0-beta.1 起有两层**：

- ✅ **G6 per-key QPS quota**（token bucket）— `api_keys_rbac[].qps_limit`
  设置即生效；超额返回 `ResourceExhausted`。这是 RPC 调用率上限，不区分查询成本。
- ✅ **G5 API key → namespace 绑定**（名前缀校验）— `api_keys_rbac[].namespace`
  设置后，客户端访问 `table_name` 必须以 `{ns}/` 开头，`ListTables` 响应也按前缀过滤。
  不设 namespace 的 key 保持 0.1/alpha 行为（看全部表）。

**仍然不做**（明确留到 0.3 或操作层解决）：

- ❌ 每 namespace 的存储 quota / 连接数 quota
- ❌ MetaStore 层面的隔离 — 若租户拿到 OBS 凭据可直接读/写其它 prefix 下的 .lance 文件；
  真正密级隔离靠"一客户一 bucket + 独立 IAM"
- ❌ 命名空间的重命名 / 批量删除原语
- ❌ `max_concurrent_queries` 仍是全局信号量，不按 namespace 划分

多租户生产环境的硬要求：配合上层 API 网关做流量整形，或者直接 "一客户一集群"（每租户独立 bucket + 独立 IAM）。

## 10. 未实现的企业级功能

- ❌ Schema 演化（ALTER TABLE ADD COLUMN）
- ❌ Time-travel / snapshot restore
- ⚠️ **OpenTelemetry 分布式追踪** — 0.2-beta 起解析 W3C `traceparent` header 并写入
  stdout audit 行 + JSONL 记录（G10 最小版）。**尚未**做：向 OTLP collector 推 span、
  coord → worker 的 trace 穿透（0.3 计划）
- ❌ Python SDK 原生异步（当前同步 gRPC）
- ❌ 流式搜索结果（当前一次性返回）
- ⚠️ **Audit log 持久化到 OBS** — G7 本地文件路径可用；OBS URI（s3://…）暂时 warn-and-drop
  因各家 OBS 的 append 语义不统一（0.3 计划补，见 `ROADMAP_0.2.md` R3）

每一条都有明确理由不在当前版本实现（详见 Phase 15/16 的架构反思日志）。需要时再加。

## 11. 读写混合下的读 QPS 降级

**实测**：100K 行表 × 128d × IVF_FLAT-32，10 readers + 1 writer @ N QPS，30s 持续：

| 场景 | 读 QPS | 读 P99 | 与 read-only 对比 |
|---|---:|---:|---:|
| read-only | 2345 | 13 ms | — |
| + 10 QPS 写 | 887 | 101 ms | -62% |
| + 50 QPS 写 | 58 | 309 ms | -97% |

数据来自 `lance-integration/bench/results/phase17/mixed_20260418_152253.json`。

**历史**：Phase 17 首版实测 -91% / -98%；Phase 18 引入 atomic version poller + RwLock snapshot 后降至 -62% / -97%。

**根因分析与改进计划**：详见 `docs/PROFILE_MIXED_RW.md`，以及 `docs/ROADMAP_0.2.md` §B2（缓存分级失效重设计）。

**当前对策**：

- **写入低频（< 1 QPS）**：影响可忽略，任何配置都可用

- **中频写入（1-10 QPS）—— alpha 推荐配置**（B2.2 验收，实测 Read QPS = **82.5% baseline**）：
  ```yaml
  replica_factor: 2
  executors:
    - { id: w0, role: write_primary, ... }
    - { id: w1, role: read_primary, ... }
  cache:
    read_consistency_secs: 60
  ```
  启动后调用 `Rebalance()` 把每 shard 复制到两个 worker。

- **高频写入（> 10 QPS）—— alpha 不推荐用于生产**：
  实测 50 QPS 写读 QPS = 18.5% baseline（本地 fs）。根因是**存储层共享资源争抢**（manifest commit 风暴 + fsync write barrier），不是 cache invalidation。详见 `docs/PROFILE_MIXED_RW.md` §8。
  - **临时 workaround**：多副本读负载均衡
    ```yaml
    replica_factor: 3
    executors:
      - { id: w0, role: write_primary, ... }
      - { id: w1, role: read_primary, ... }
      - { id: w2, role: read_primary, ... }
    ```
    把读流量分到 w1 + w2，写压仍在 w0，但 fs 层压力没有减小——**只是把 reader 的 QPS 分摊**
  - **S3 部署下可能更好**：对象存储 HTTP 并发语义不共享 worker 本地 fs，50 QPS 场景在 S3 下未实测，`ROADMAP_0.2 §B2.6` 是计划验证。如果你在 S3 上实际跑了，请反馈数据
  - **真正的修复** 在 `ROADMAP_0.2.md` §1.5 的 B2.3-B2.7，beta 里处理

- 实现细节与多组实验数据见 `docs/PROFILE_MIXED_RW.md` §7-§8

## 12. 性能拐点（参考 BENCHMARK.md）

- 小数据量（< 50K 行）下**单 shard 比多 shard 快**，因为 scatter-gather 开销 > 并行收益
- 大 k（k > 1000）下 IPC 序列化开销开始占比上升
- 混合读写（90/10）下 QPS 较纯读下降 20~30%

具体数据见 [BENCHMARK.md](./BENCHMARK.md)。

## 13. Worker RSS 在 sustained-read 下的周期性 warmup

**实测**（G4 beta soak，见 `soak/results/soak_60min_20260419_130316.json`）：

- 20K 行表 × 64d × IVF_FLAT-16，5 readers × ~10 QPS，60 min 持续
- **Coord**：RSS +2.91%、fd +0.00% —— 稳
- **Worker**：RSS +15~17%、fd 13→15（两个 worker 各 +2 fd）

Worker 的增长**不是线性泄漏**：sampling 序列 0→30 min 属于初始 cache warmup（RSS 缓慢上升约 +3%），30→55 min 完全平（±0），55→60 min **一个 14 MB + 2 fd 的 step** —— 两个 worker 同步发生 —— 是 Lance 背景 fragment reload 事件，典型的"cache 命中一个新 segment 并把 file handle 保留"。

**与 alpha 对比**：alpha 15 min soak 的 w0/w1 RSS 增长为 +228%/+231%（H25 修复前的粗放 cache 管理）。beta 在 4 倍时长下只增 +15~17%，已经好 15 倍，但 3% 门槛对 warmup 事件太严。

**生产建议**：
- 先用 10-15 MB/worker/hour 作为"可接受 warmup 上限"规划 pod memory limit，留 20~30% headroom。
- 如果 4h+ 运行后 RSS 仍在线性上涨（而不是阶梯式 + 长时间持平），那就是真漏了，需要抓火焰图。
- Coord 的 3% 门槛**仍然是硬约束** —— coord 是 stateless scatter-gather，不应该有任何漏。

**计划**：0.3 把 soak 扩到 4h 并按 Welford 方法计算"阶梯高度 / 阶梯间隔"两个维度，区分 warmup 与泄漏。

---

## 如何上报新发现的限制

1. 如果是 bug：提 issue，标 `bug` label
2. 如果是"用起来怪"但不算 bug 的行为：提 issue 标 `limitation`，我们会评估是写进本文档还是修复
3. 不要私下 workaround 然后不告诉项目——这是最浪费时间的模式
