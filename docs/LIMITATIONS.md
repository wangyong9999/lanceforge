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

- 目前**没有**每 API key 的 QPS quota / 存储 quota
- `max_concurrent_queries` 是全局信号量，不区分 key
- 多租户场景需要自己在上层网关做限流

## 10. 未实现的企业级功能

- ❌ Schema 演化（ALTER TABLE ADD COLUMN）
- ❌ Time-travel / snapshot restore
- ❌ OpenTelemetry 分布式追踪（只有结构化日志 + Prometheus 指标）
- ❌ Python SDK 原生异步（当前同步 gRPC）
- ❌ 流式搜索结果（当前一次性返回）

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
- 写入低频（< 1 QPS）场景影响可忽略
- 中频写入场景（1-10 QPS）：调高 `cache.read_consistency_secs`（例如 30s），容忍稍旧读
- 高频写入场景（> 10 QPS）：物理读写分离（replica_factor=2，约定 read-only worker）
- 0.2 目标：10 QPS 写场景读 QPS 恢复到 ≥ 80% 基线

## 12. 性能拐点（参考 BENCHMARK.md）

- 小数据量（< 50K 行）下**单 shard 比多 shard 快**，因为 scatter-gather 开销 > 并行收益
- 大 k（k > 1000）下 IPC 序列化开销开始占比上升
- 混合读写（90/10）下 QPS 较纯读下降 20~30%

具体数据见 [BENCHMARK.md](./BENCHMARK.md)。

---

## 如何上报新发现的限制

1. 如果是 bug：提 issue，标 `bug` label
2. 如果是"用起来怪"但不算 bug 的行为：提 issue 标 `limitation`，我们会评估是写进本文档还是修复
3. 不要私下 workaround 然后不告诉项目——这是最浪费时间的模式
