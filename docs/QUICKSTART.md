# LanceForge Quickstart

从零到跑通分布式向量搜索，**目标 10 分钟内完成**。

## TL;DR 三步走

1. 编译二进制：`cargo build --release` (首次 ~5 分钟)
2. 启动一个 coordinator + 两个 worker (下方脚本)
3. 用 Python SDK 创建表、插入、搜索

## 前置要求

| 工具 | 版本 | 用途 |
|---|---|---|
| Rust | 1.70+ | 编译 coordinator/worker |
| Python | 3.10+ | SDK + 测试 |
| `protoc` | 3.15+ | Python proto 生成 (只在调试时需要) |
| (可选) MinIO/S3 | - | 对象存储后端；不用则走本地 fs |

一次性 Python 依赖：

```bash
pip install grpcio grpcio-tools pyarrow lance lancedb numpy
```

## Step 1 — 编译

```bash
cd lance-ballista
cargo build --release
# 产物: target/release/lance-{coordinator,worker,monolith}
```

## Step 2 — 启动最小集群

**推荐：单进程 `lance-monolith`**（beta.5 起）。所有角色（QN / PE / IDX / CP）在同一 tokio runtime 内跑，loopback 对接，开发和小规模部署首选：

```bash
BASE=/tmp/lanceforge_demo
rm -rf $BASE && mkdir -p $BASE

cat > $BASE/config.yaml <<'EOF'
tables: []
executors:
  - id: w0
    host: 127.0.0.1
    port: 9102   # 必须 >= coord-port + 2（9200 + 2），避免和 REST (9201) 撞
default_table_path: /tmp/lanceforge_demo
server:
  max_k: 10000
  slow_query_ms: 1000
EOF

./target/release/lance-monolith $BASE/config.yaml \
    --coord-port 9200 --worker-port 9102 &
sleep 3
```

**传统双进程模式**（coord + worker，生产模板）：

```bash
cat > $BASE/config.yaml <<'EOF'
tables: []
executors:
  - id: w0
    host: 127.0.0.1
    port: 9100
  - id: w1
    host: 127.0.0.1
    port: 9101
default_table_path: /tmp/lanceforge_demo
server:
  max_k: 10000
  slow_query_ms: 1000
EOF

./target/release/lance-worker $BASE/config.yaml w0 9100 &
./target/release/lance-worker $BASE/config.yaml w1 9101 &
./target/release/lance-coordinator $BASE/config.yaml 9200 &
sleep 3
```

架构：`lance-coordinator` / `lance-worker` 保持行为兼容，beta.5-beta.6 期间 monolith 和双进程并存。详见 `docs/ARCHITECTURE_V2.md`。

**运维拆分模式**（生产，Phase E 起）：当需要 CP 或 IDX 独立扩缩容时：

```bash
# Worker 关闭 inline compaction (在 config.yaml 里设 compaction.enabled: false)
./target/release/lance-worker $BASE/config.yaml w0 9100 &

# Coordinator 正常
./target/release/lance-coordinator $BASE/config.yaml 9200 &

# 独立 CP（可选，目前主要被 RoutingClient 消费，0.3 才成为主路径）
./target/release/lance-cp $BASE/config.yaml 50052 &

# 独立 IDX：只跑 compaction，不抢 worker 的查询 CPU
./target/release/lance-idx $BASE/config.yaml &
```

`lance-qn` 和 `lance-pe` 独立 bin 推迟到 0.3（需要 `CoordinatorService` 重构把 auth/audit/orphan_gc 搬到 CP-side）。

健康检查：

```bash
curl http://127.0.0.1:9201/metrics | head   # Prometheus endpoint on coord_port+1
curl http://127.0.0.1:9201/healthz
```

## Step 3 — 第一次搜索（Python）

```bash
cd lance-integration/tools  # 预生成的 pb 在这里
python3 <<'PY'
import struct, numpy as np
import pyarrow as pa, pyarrow.ipc as ipc, grpc
import lance_service_pb2 as pb, lance_service_pb2_grpc as pbg

stub = pbg.LanceSchedulerServiceStub(
    grpc.insecure_channel("127.0.0.1:9200",
        options=[("grpc.max_receive_message_length", 64*1024*1024)]))

DIM = 128
N = 10_000

# 1. 构造随机数据
np.random.seed(0)
vecs = np.random.randn(N, DIM).astype(np.float32)
batch = pa.record_batch([
    pa.array(range(N), type=pa.int64()),
    pa.FixedSizeListArray.from_arrays(pa.array(vecs.flatten()), list_size=DIM),
], names=['id', 'vector'])
sink = pa.BufferOutputStream()
w = ipc.new_stream(sink, batch.schema); w.write_batch(batch); w.close()

# 2. CreateTable (自动分片到两个 worker)
r = stub.CreateTable(pb.CreateTableRequest(
    table_name="demo", arrow_ipc_data=sink.getvalue().to_pybytes()))
print(f"Created: {r.num_rows} rows, err={r.error!r}")

# 3. 搜索
query = np.random.randn(DIM).astype(np.float32)
qv = struct.pack(f"<{DIM}f", *query.tolist())
r = stub.AnnSearch(pb.AnnSearchRequest(
    table_name="demo", vector_column="vector",
    query_vector=qv, dimension=DIM, k=10, nprobes=10, metric_type=0))
print(f"Got {r.num_rows} hits")

# 4. 清理
stub.DropTable(pb.DropTableRequest(table_name="demo"))
PY
```

## 停止集群

```bash
pkill -f 'lance-coordinator|lance-worker'
```

## 下一步

- 更大规模？看 [BENCHMARK.md](./BENCHMARK.md) — 10K 至 1M 的 QPS/P99 实测
- 生产部署？看 [DEPLOYMENT.md](./DEPLOYMENT.md) — Helm chart
- 配置详解？看 [CONFIG.md](./CONFIG.md) — 所有字段 + 默认值
- 已知限制？看 [LIMITATIONS.md](./LIMITATIONS.md) — 诚实的边界说明

## 常见问题

**Q: `Error, decoded message length too large`**
A: CreateTable 数据超过 gRPC 默认 4 MiB 限制。客户端已在示例中通过 `grpc.max_receive_message_length` 提高；服务端通过 `server.max_response_bytes` 自动计算。确认你的 config.yaml 里 `server.max_response_bytes` 够大（默认 64 MiB）。

**Q: 两个 worker 为什么 QPS 比一个还低？**
A: 小数据量下（< 50K 行）scatter-gather 的跨 RPC 开销超过并行收益。数据规模 ≥ 100K 后分布式优势才显现。详见 [BENCHMARK.md](./BENCHMARK.md)。

**Q: MinIO/S3 怎么配？**
A: 在 config.yaml 加：

```yaml
storage_options:
  aws_endpoint: http://127.0.0.1:9000
  aws_access_key_id: minioadmin
  aws_secret_access_key: minioadmin
  aws_region: us-east-1
  allow_http: "true"
default_table_path: s3://mybucket/lanceforge/
```

**Q: 怎么加 API key 认证？**
A: config.yaml 里配：

```yaml
security:
  api_keys_rbac:
    - key: my-admin-key-123
      role: admin
    - key: my-read-key-456
      role: read
```

客户端发请求带 metadata `authorization: Bearer my-admin-key-123`。
