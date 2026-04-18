# 竞品分析与能力差距（发 0.2-beta 前的一次正视）

**建档时间**：2026-04-18
**参照版本**：LanceForge 0.2.0-alpha.1 + HARDENING_PLAN 全部闭合
**目的**：在正式发 commercial-ready 版本前，诚实对标主流竞品，识别真正需要补齐 vs 可以延后到 0.3+ 的能力差距。不是"什么都要做"——而是**让我们的发布不被一眼看穿核心功能缺失**。

## 一、对标对象

| 产品 | 定位 | 开源 | 代表特性 |
|---|---|---|---|
| **Pinecone** | SaaS-only，serverless | ❌ | 命名空间多租户；sparse+dense 混合；99.95% SLA；高 QPS 托管 |
| **Weaviate** | Open core + cloud | ✅ 部分 | 模块化 embedding；BM25+vector hybrid；GraphQL；多模态；RBAC |
| **Qdrant** | Open core + cloud | ✅ 部分 | 命名向量（multi-vec）；sparse；scalar quantization；snapshots；RBAC |
| **Milvus** | Distributed OSS | ✅ | 读写存储分离；consistency levels；多种索引；GPU；database+collection 多租户 |

**LanceForge 的定位**（用一句话讲清楚）：
> OBS-native 分布式向量检索——以 Lance 文件格式为唯一真相源，coordinator/worker 全无状态。开源、无需专用存储层、可 SaaS 也可 self-host。

这个定位的"**天然优势**"：
- Lance 格式本身就是行业最好的 columnar-vector 存储（upstream 项目背书）
- OBS-native 意味着数据在 S3 = 成本可预测，不需要 EBS/NVMe 规划
- 简单部署：2-binary（coord + worker），不需要 etcd/Pulsar/rocksdb 等外部依赖
- 无状态 pod = K8s 友好，scale-to-zero 潜在可行

"**天然劣势**"：
- Lance 生态还在早期（不像 Parquet 那样普及）
- 查询延迟天花板受 S3 读延迟影响（vs Pinecone 的专有 in-memory）
- 生态功能数量短期赶不上 10 年老的竞品

## 二、能力矩阵（诚实打分）

| 能力 | Pinecone | Weaviate | Qdrant | Milvus | **LanceForge 0.2-alpha** | LanceForge 0.2-beta 目标 |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| **向量搜索核心**|||||||
| ANN（IVF/HNSW）| ✅ | ✅ | ✅ | ✅ | ✅ IVF_FLAT/HNSW | ✅ 同 |
| Product Quantization / Scalar Quantization | ✅ | ✅ | ✅ SQ | ✅ | ⚠️ 依赖 lancedb upstream | 文档化，不主动做 |
| Named vectors（单 row 多向量）| ❌ | ✅ | ✅ | ❌ | ⚠️ 通过多字段模拟 | 0.3 M1 配合 ColBERT |
| GPU 加速 | ✅ 部分 | ❌ | ❌ | ✅ | ❌ | 延后 0.3+ |
| **混合检索** |||||||
| BM25 / FTS | ❌ | ✅ | ❌ | ✅ | ✅ Tantivy | ✅ 同 |
| Sparse vector (SPLADE/BGE-M3) | ✅ | ❌ | ✅ | ❌ | ❌ | **0.3 M1** |
| ANN + BM25 hybrid (RRF) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同 |
| **过滤与表达**|||||||
| Pre-filter 元数据 | ✅ | ✅ | ✅ | ✅ | ✅ scalar index | ✅ 同 |
| SQL-like predicate | ❌ | 部分 | 部分 | 部分 | ✅ DataFusion 表达式 | ✅ 同 |
| **SDK & API** |||||||
| REST | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同 |
| gRPC | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同 |
| Python SDK | ✅ | ✅ | ✅ | ✅ | ✅ 同步 | **0.3 R5 加 async** |
| Java/Go/JS SDKs | ✅ | ✅ | ✅ | ✅ | ❌ | **0.3+ 至少 Go** |
| GraphQL | ❌ | ✅ | ❌ | ❌ | ❌ | **不做** |
| **多租户 / 隔离** |||||||
| Namespaces / collections | ✅ | ✅ | ✅ collection | ✅ database | ⚠️ 仅 table 级 | **0.2-beta 必做** |
| Per-tenant quota | ✅ | ✅ | ✅ | ✅ | ❌ Gap F | **0.2-beta 必做（min）** |
| RBAC | ✅ | ✅ | ✅ | ✅ | ✅ 三角色 | ✅ 同 |
| **安全 / 合规** |||||||
| TLS (mTLS) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同 |
| Encryption at rest | ✅ | 部分 | ✅ | ✅ | ⚠️ 依赖 OBS SSE | **docs + 配置验证** |
| Audit log (persist + SIEM) | ✅ | 部分 | ❌ | ✅ | ⚠️ 仅 stdout | **0.2-beta 写 S3-rolling** |
| **可观测性** |||||||
| Prometheus metrics | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同 |
| Distributed tracing (OTEL) | 部分 | ✅ | ⚠️ | ✅ | ❌ R1 | **0.2-beta 加 trace_id** |
| Grafana dashboard template | ✅ | ✅ | ✅ | ✅ | ❌ | **0.2-beta 交付模板** |
| **HA / 数据管理** |||||||
| 多 coordinator | N/A SaaS | ✅ | ✅ | ✅ | ✅ S3MetaStore | ✅ 同 |
| Rebalance / scale | ✅ 自动 | ✅ | ✅ | ✅ | ✅ 手动触发 | 0.3 自动 |
| Snapshots / time-travel | ✅ | ❌ | ✅ | ✅ | ⚠️ 依赖 S3 versioning | 0.3 |
| Backup/restore CLI | ✅ | ✅ | ✅ | ✅ | ❌ | **0.2-beta `lance-admin`** |
| **运维与接入** |||||||
| Helm chart | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同 |
| K8s operator | ✅ | ✅ | ❌ | ✅ | ❌ | 0.3+ |
| Admin UI | ✅ | ✅ | ✅ | ✅ (Attu) | ❌ | 0.3+ |
| **数据生态集成** |||||||
| S3 / GCS / Azure | ✅ | ✅ | ✅ | ✅ | ✅ 原生 | ✅ |
| Iceberg / Paimon 互通 | ❌ | ❌ | ❌ | ❌ | ⚠️ Lance format → Parquet convert | **0.3 M3 差异化** |
| CDC / stream ingest | ✅ | 部分 | ❌ | ✅ Pulsar | ❌ | 0.3+ |

## 三、Gap 分类

### 🔴 Severity 1 — 商用发布前**必须**有（否则被客户一眼看穿）

1. **多租户 namespaces**：当前所有表全局共享命名空间。SaaS 场景每个客户的 table 名字会冲突。最小实现：table_name 加 namespace 前缀语义（`{ns}/{table}`）+ RBAC 绑定 namespace
2. **Per-key quota**：`security.api_keys_rbac` 目前没 rate limit。至少要一个每 key QPS cap
3. **Audit log 持久化**：当前只是 info 日志，崩了就丢。最小实现：滚动 append 到 S3 上的 JSONL 文件
4. **H24 k=10000 60% 错误率**：`PERF_LARGE_K.md` 里开的坑，必须闭合——写的是"支持 max_k=10000"但 60% 错，这是**严重的信任破坏**
5. **Encryption at rest**：必须**文档化** S3 SSE-KMS 配置 pattern，以及测试它在 LanceForge 下真能用。不一定实现新代码，但必须证明兼容
6. **Backup/restore CLI**：`lance-admin backup create / restore` 最基本的形态。生产客户会立即要

### 🟡 Severity 2 — 强烈建议有（没有会显著影响竞争力）

7. **OTEL 分布式 tracing**：至少 trace_id 穿透 client → coord → worker，让 SRE 能定位慢查询
8. **Grafana dashboard 模板**：一份即开即用的 `.json`，覆盖 QPS/latency/error/memory
9. **Python SDK async**：`grpc.aio` 客户端。现代 AI 应用 event-loop 第一位
10. **Chaos 扩展 fault library**：net_partition / OOM / S3_throttle。目前只有 worker_kill / worker_stall
11. **Soak 24h nightly**：CI job，硬证据

### 🟢 Severity 3 — 可以延到 0.3 或更晚

12. Sparse vectors (M1) — roadmap item，大工程
13. Java/Go SDK — 第一个非-Python SDK，~2w 每个语言
14. PQ/SQ quantization — 依赖 lancedb upstream
15. K8s operator、Admin UI、CDC ingest — 运维类，不卡核心功能
16. Named vectors / multi-vector — 可以用多字段模拟，痛但能用
17. Iceberg / Paimon 互通（M3 差异化）— 0.3 差异化卖点，但 beta 不需要

## 四、诚实自评：LanceForge 在 0.2-beta 时的竞争位

**和 Pinecone 比**：
- 🟢 开源，可 self-host
- 🟢 OBS-native，成本可控
- 🔴 功能广度明显小（没 sparse、没 namespaces quota、没 SDK 矩阵）
- 🔴 生态年龄差 10 年

**和 Weaviate 比**：
- 🟢 写入/查询性能不差（实测 1774 QPS @ 1M × 128d，Weaviate 类似规模 ~2000）
- 🟢 部署简单（2 binary vs 1 fat JVM）
- 🔴 没 module 体系（它们 embedding/reranker/Q&A 生态丰富）
- 🟡 没 multi-modal first-class

**和 Qdrant 比**：
- 🟢 分布式 story（Qdrant 集群模式较新）
- 🟢 OBS-native
- 🔴 没 sparse/named vectors
- 🔴 没 quantization

**和 Milvus 比**：
- 🟢 部署更简单（Milvus 依赖 etcd/Pulsar/MinIO/RocksDB）
- 🟢 代码更小更易理解
- 🔴 功能广度差距最大（consistency levels / database+collection / GPU / CDC 全没）

**一句话结论**：**LanceForge 0.2-beta 可以以"**简单、OBS-native、开源、面向 AI RAG 场景**"** 为定位打包发布，**但必须补齐 Severity 1 的 6 个 Gap 才对得起"商用"这个词**。Severity 2 是"beta→GA 之间做"，Severity 3 是"0.3 roadmap"。

## 五、见表 `RELEASE_PLAN_0.2_BETA.md`

具体实施计划（估工、顺序、验收）在独立文档。本文件只讲**为什么**做这些，那份讲**怎么**做。
