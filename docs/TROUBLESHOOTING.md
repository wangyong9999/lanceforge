# LanceForge 故障排查映射表

**症状 → 原因 → 修复** 的 flat lookup。看到报错直接定位。

## gRPC 层错误

### `UNAUTHENTICATED: Missing authorization header`
- 启用了 `security.api_keys` 或 `api_keys_rbac`
- 客户端未加 `authorization: Bearer <key>` metadata
- **修**：在 gRPC stub 里加 interceptor（见 QUICKSTART.md）

### `PERMISSION_DENIED: Admin permission required (role=Read)`
- API key 的角色不够
- 查 `config.yaml` 里该 key 的 `role` 字段
- **修**：提升该 key 的 role，或使用 admin key

### `RESOURCE_EXHAUSTED: Too many concurrent queries (max 200)`
- 协调器 `max_concurrent_queries` 信号量耗尽
- **诊断**：`curl :9201/metrics | grep lance_query_total` 看近期 QPS
- **修**：升高 `server.max_concurrent_queries`；或客户端做指数退避

### `OUT_OF_RANGE: Error, decoded message length too large: found X bytes, the limit is: Y`
- 单次 Arrow IPC payload 超过 gRPC 接收上限
- **服务端**：升高 `server.max_response_bytes`（默认 256 MiB）
- **客户端发送方向**：channel options 加 `grpc.max_send_message_length`
- **客户端接收方向**：`grpc.max_receive_message_length`
- **预防**：大批量 `AddRows` 拆分成 ≤ 64 MiB 的块

### `DEADLINE_EXCEEDED` / `timeout`
- 单次查询超 `server.query_timeout_secs`（默认 30s）
- 常见原因：数据量大 + 索引未建；worker 负载过高
- **诊断**：慢查询日志 `grep slow_query coord.log`
- **修**：
  - 为列加 IVF 索引：`CreateIndex` RPC
  - 降 `k`，或用 `offset` 分页
  - 扩 worker 数 + Rebalance

### `NOT_FOUND: Table not found: X`
- 表不存在或已 DropTable
- 若刚 CreateTable 失败（Saga 回滚），表也不存在
- **修**：`ListTables` 确认；重 CreateTable

### `UNAVAILABLE: tcp connect error` (coord→worker)
- Worker 挂了或网络断
- **诊断**：`curl :9201/healthz`；`grpcurl worker:9100 ... HealthCheck`
- **修**：重启 worker；检查防火墙/DNS

## 数据一致性

### CountRows 比预期大 2-N 倍
- **历史根因**：Add 扇出到所有 worker。Phase 11 已修（target_shard）。
- **现在可能**：Upsert 和 Add 路由不一致（Upsert hash → shard A；Add round-robin → shard B）→ 同 id 两份
- **诊断**：`SELECT id, count(*) FROM t GROUP BY id HAVING count(*) > 1`（通过 Lance/DuckDB 直接查）
- **修**：见 LIMITATIONS.md §1；workaround 是只用 Add + Delete，不用 Upsert

### DropTable 之后 S3 账单没降
- DropTable 会调用 `lancedb::drop_table` 删 OBS 目录
- 但：如果该 URI 未登记在 coordinator 的 `shard_uris`（手写 YAML 场景），DropTable 不会删它
- **修**：手动 `aws s3 rm --recursive s3://bucket/path.lance/`；或用 Phase 16 的 orphan_gc（目前仅本地 fs）

### Rebalance 之后 CountRows 变了
- **Phase 11 已修**：Rebalance 只移动路由元数据，不复制/删除数据
- 如果还遇到：检查 shard_uris 里是否有重复登记（一个 URI 被注册到多个 shard 名）
- **诊断**：coord 日志里 `info!("Rebalance:")` 行，看移动的 shard 列表

## 性能

### P99 延迟突然飙升
- **检查 1**：慢查询日志：`grep slow_query coord.log`
- **检查 2**：per-table 指标是否某表独占 QPS：`curl :9201/metrics | grep lance_table_`
- **检查 3**：worker loaded_shards 是否不均（热 shard）
- **修**：`MoveShard` 把热 shard 迁到空闲 worker

### QPS 在高并发（≥ 50）时崩溃
- **Phase 17 发现**：客户端单 channel 多线程 → HTTP/2 stream 序列化
- **修**：客户端每线程一个 `grpc.insecure_channel()` + stub
- **服务端**：检查 `concurrency_limit_per_connection: 256` 是否过低

### Ingest 吞吐低于 50K rows/s
- Lance 索引建立可能慢（大 vector dim + 大 num_partitions）
- 先 `AddRows` 不建索引，最后一次性 `CreateIndex`
- 检查磁盘/S3 上传带宽是否饱和

## 启动失败

### Worker 日志：`Address already in use`
- 端口被占（可能是上次没退干净）
- `pkill -9 -f 'lance-worker.*<port>'`

### Coord 日志：`S3MetaStore init failed`
- S3 credentials 不对；endpoint 不通
- 先手动 `aws s3 ls s3://bucket/prefix/` 验证
- 检查 `storage_options` 里是否漏了 `allow_http: "true"`（MinIO）

### Worker 日志：`Failed to open table`
- Shard URI 指向的 `.lance` 目录不存在或 corrupt
- `aws s3 ls s3://.../shard.lance/` 看是否有 `_versions/` 目录
- 重建：DropTable + CreateTable

## 数据损坏（罕见）

### Metadata.json 损坏
- 备份恢复：`cp backup/metadata_*.json /data/metadata.json`
- 或重建：停集群，删 metadata，启集群，用 YAML 重新 CreateTable

### Lance manifest 冲突
- 通常 Lance 自己会重试 CAS；极端场景：手动 `dataset.checkout_latest()`
- 最后一招：`lance::Dataset::restore(version_n)` 回滚到之前版本

## 问题反馈格式

上报 issue 请附：
1. `cargo --version`，`lance-coordinator --version`
2. `config.yaml`（**屏蔽 secret**）
3. 客户端代码片段（能复现）
4. coord.log 和 worker.log 最后 200 行（trace 级别）
5. `curl :9201/metrics` 输出
6. `grpcurl :9200 ... GetClusterStatus` 输出
