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

**实测两轮 60 min read-only soak 的一致结论**：

| 轮次 | 产物 | Coord drift | Worker RSS drift | Worker fd |
|---|---|---:|---:|---:|
| beta.1 G4 (60 min) | `soak/results/soak_60min_20260419_130316.json` | +2.91% | +15.6/+16.9% | 13 → 15 |
| beta.3 B7 (60 min) | `soak/results/soak_60min_20260420_161244.json` | +4.08% | +16.4/+13.0% | 13 → 15 |
| alpha G4 (15 min) | — | -10.6% | +228/+231% | 13 → 14 |

两轮 beta 的 worker RSS 曲线形状**完全一致**：

- min 0→55：缓慢上升约 +3% 后长时间平（±0.2%），看起来是初始 cache warmup 阶段
- min 55→60：**一个 13-15 MB 的 step，两个 worker 同步发生**，fd 从 13 →15（各 +2）

两轮 60-min 窗口里各只出现 **一次** step。配合 beta.3 中途被 pkill 打断的 38 min 数据（RSS 一直平稳没出 step），三个数据点共同支持 **"事件是周期性、非线性"** 的假设——一次 Lance 背景 fragment reload 打开了额外 segment，把 file handle 保留进 cache，导致 RSS 跳一次然后继续平。

与 alpha 对比：alpha 15 min 就飙 +228%，暴涨完全不同的曲线——是 H21 之前粗放 cache 管理导致的线性漏；**beta 已经好 15× 以上**。现在是 step 形状，不是斜线。

**3% 门槛的诚实处理**：

- **Coord**：beta.3 +4.08% 略超门槛，但曲线形状也是"两次小 step（min 25 / min 55）"非线性；coord 作为 stateless scatter-gather 期望应该 < 3%，0.3 要查 coord 的那 2 次 +~500KB step 具体是什么（嫌疑最大是 TLS session cache / tonic channel 池）。
- **Worker**：+15% 超门槛但是**单次事件**而非持续增长，实际 SRE 规划意义上的 "per-hour growth" 接近 0。将 `soak/run.py --drift-threshold` 按 worker/coord 分桶、对 step 形曲线用 "峰值高度 / 连续平均斜率" 两维度判断——是 0.3 harness 改造工作。

**生产建议**：

- Pod memory limit 预留 20-30 MB/worker headroom 覆盖一次 step。
- 4h+ 长跑来特征化 step 发生间隔——0.3 nightly CI 里用 `workflow_dispatch` soak_minutes=240 跑。
- 4h 后仍线性上涨（不是"step 然后长时间平"）才算真漏，应当抓火焰图。

**计划（0.3）**：

1. 扩展 `soak/run.py` 自动识别 step（首差分 > 5 MB 认定是 step，记步高 + 时间戳），代替裸 drift% 作为 gate 标准
2. 查 coord 非零 drift 的根因（TLS session cache？tonic 连接池增长？）
3. 4h characterisation run 作为发布前强制 gate

---

## 如何上报新发现的限制

1. 如果是 bug：提 issue，标 `bug` label
2. 如果是"用起来怪"但不算 bug 的行为：提 issue 标 `limitation`，我们会评估是写进本文档还是修复
3. 不要私下 workaround 然后不告诉项目——这是最浪费时间的模式
