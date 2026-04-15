# LanceForge Operations Runbook

面向 SRE/运维的操作手册。每条都是可执行的命令，不含废话。

## 1. 扩容（加 worker）

### 前提
- 集群用 S3MetaStore 或 FileMetaStore（有持久化）
- 新 worker 能被 coordinator 解析到（DNS or static host）

### 步骤

```bash
# 1) 在 config.yaml 追加新 executor
vim config.yaml   # 加 w2 入口

# 2) 启动新 worker (共享同一 config)
./lance-worker config.yaml w2 9102 &

# 3) 通过 coordinator 向集群注册（可选；config 里有也会自动识别）
grpcurl -plaintext -d '{"worker_id":"w2","host":"...","port":9102}' \
  127.0.0.1:9200 lance.distributed.LanceSchedulerService/RegisterWorker

# 4) 触发 rebalance 把现有 shard 迁移一部分到新 worker
grpcurl -plaintext 127.0.0.1:9200 lance.distributed.LanceSchedulerService/Rebalance

# 5) 验证
grpcurl -plaintext 127.0.0.1:9200 lance.distributed.LanceSchedulerService/GetClusterStatus
```

**预期**：GetClusterStatus 返回 3 个 executor，每个都 healthy=true。

### 坑
- 如果新 worker 的 URI 不在 shard_uris 里（即从未 CreateTable/ingest 过），MoveShard 会报 "Shard URI unknown"。**新 worker 只能分担已有 shard**。

## 2. 缩容（下线 worker）

```bash
# 1) 把 worker 上的 shard 迁到其他节点
grpcurl -plaintext -d '{"shard_name":"products_shard_00","to_worker":"w1","force":false}' \
  127.0.0.1:9200 lance.distributed.LanceSchedulerService/MoveShard
# 重复直到该 worker loaded_shards=0

# 2) 停 worker
pkill -f 'lance-worker.*9102'

# 3) 从 config.yaml 移除该 executor
# 4) 重启 coordinator（让 reconciler 清理 routing）
pkill -f 'lance-coordinator'
./lance-coordinator config.yaml 9200 &
```

## 3. 表迁移（手动指定 shard → worker）

```bash
# 把 shard_02 从 w1 迁到 w2
grpcurl -plaintext -d '{"shard_name":"mytable_shard_02","to_worker":"w2"}' \
  127.0.0.1:9200 lance.distributed.LanceSchedulerService/MoveShard
```

三阶段保证一致性：
1. Target worker `LoadShard`（建立本地 handle）
2. 路由表原子切换（新读指向 w2）
3. Source worker `UnloadShard`（best-effort）

## 4. 备份

### 元数据备份

```bash
# FileMetaStore: 复制单文件
cp /data/metadata.json /backup/metadata_$(date +%F).json

# S3MetaStore: aws s3 sync
aws s3 cp s3://mybucket/lanceforge/metadata.json \
  /backup/metadata_$(date +%F).json
```

### 数据备份

Lance 数据就是对象存储上的 `.lance/` 目录。用 `aws s3 sync` / `rclone` 冷备。

```bash
aws s3 sync s3://mybucket/lanceforge/ s3://backup-bucket/lanceforge/$(date +%F)/
```

### 恢复

1. 停所有 coordinator + worker
2. 还原元数据文件 + `.lance` 目录到原位
3. 启动 coordinator（会从 metadata 重建路由）
4. 启动 worker
5. 等 reconciler 把 shard 重新 load（~10s）

## 5. 滚动升级

### 零停机步骤（前提：replica_factor ≥ 2）

```bash
# 逐个重启 worker
for w in w0 w1 w2; do
  pkill -f "lance-worker.*$w"
  sleep 5
  ./lance-worker config.yaml $w <port> &
  # 等 health_check 恢复
  until grpcurl ... | grep "\"id\":\"$w\",\"healthy\":true"; do sleep 2; done
done

# 重启 coordinator（无状态，外部 LB 即可）
```

### HA 升级（多 coordinator）

需要 `metadata_path: s3://...`。否则只有一个 coordinator 能跑。

```bash
# 两个 coordinator 共享同一元数据
./lance-coordinator cfg1.yaml 9200 &
./lance-coordinator cfg2.yaml 9300 &

# 逐个重启即可，另一个承接流量
```

## 6. 排障清单

### 症状：`NOT_FOUND: Table not found`

可能原因：
- 表还没 CreateTable
- 通过 DropTable 已删除
- Coordinator 元数据丢失（FileMetaStore 单机 crash 未 fsync？查 Phase 12 commit 已修）

```bash
# 检查路由
grpcurl -plaintext 127.0.0.1:9200 lance.distributed.LanceSchedulerService/ListTables
# 看协调器日志
grep -i "routing\|shard_state" /var/log/lance-coord.log | tail
```

### 症状：`RESOURCE_EXHAUSTED: Too many concurrent queries`

- 检查 `max_concurrent_queries`（默认 200）
- 看 `/metrics` 里 `lance_query_total` 增长率
- 升高 limit 或做 client 侧 backoff

### 症状：`OUT_OF_RANGE: Error, decoded message length too large`

- 客户端发的 payload > server 接收上限（`max_response_bytes + 16 MiB`）
- 客户端：拆分 AddRows；或在 client channel options 加 `grpc.max_send_message_length`
- 服务端：升高 `server.max_response_bytes`

### 症状：P99 飙高但 QPS 不崩

- 看慢查询日志：`grep slow_query /var/log/lance-coord.log | tail -20`
- 看 per-table 指标：`curl :9201/metrics | grep lance_table_`
- 可能是某张表数据倾斜 → `MoveShard` 平衡

### 症状：QPS 崩溃（像 Phase 17 observed）

- **先看客户端**：单个 gRPC channel 共享 N 线程 → HTTP/2 stream 序列化
  - 修：客户端每个 worker thread 一个 channel
- 再看 server：`max_concurrent_queries` 太低；`concurrency_limit_per_connection` 偏低

### 症状：磁盘/S3 一直涨

- 检查是否有 orphan `.lance` 目录：`ls $default_table_path/*.lance`
- 对比 registered shard URIs：`grpcurl ... GetClusterStatus` 的 total_shards
- 打开 `orphan_gc.enabled: true`（本地 fs）或手动 `aws s3 rm`（S3）

## 7. 常用观测命令

```bash
# 集群状态
curl http://127.0.0.1:9201/healthz
curl http://127.0.0.1:9201/metrics | grep -E 'lance_query_total|executors_healthy|slow_query_total'

# 实时 QPS
watch -n 1 'curl -s :9201/metrics | grep lance_query_total'

# 每个表的查询量
curl -s :9201/metrics | grep lance_table_query_total

# 审计日志（who did what）
grep '^audit' /var/log/lance-coord.log | tail -50
```

## 8. 容量规划参考

从 Phase 17 benchmark 数据推算：

| 维度 | 阈值 | 来源 |
|---|---|---|
| 单 worker 100K × 128d 并发 10 的 P50 | ~5 ms | mid matrix |
| 单 worker 100K × 128d 并发 50 的 P99 | > 900 ms（客户端 bug 导致） | mid matrix |
| 每 worker 的 CreateTable 吞吐 | ~75K rows/s (本地 fs) | mid matrix |
| shards > 1 收益的拐点 | > 500K 行/表（推测，需 1M 验证） | - |

**客户端务必**：每线程一个 gRPC channel，否则 conc > 10 时会被 HTTP/2 stream 限流（详见 QUICKSTART）。

## 9. CI/CD 集成点

- **Smoke test**: `python3 bench_phase17_matrix.py --smoke` (~2 min，每次 PR 跑)
- **E2E 回归**: `python3 e2e_phase{11..16}_test.py` (~5 min)
- **Unit**: `cargo test --release --workspace --lib` (~1 min)

发布流程里顺序：unit → E2E → smoke bench → 发布。
