# LanceForge Configuration Reference

Complete reference of all `cluster_config.yaml` fields. Every entry lists its **type**, **default**, and **when to tune**.

## Top-level Structure

```yaml
tables:                    # (optional) Pre-configured shards
executors: [...]           # Worker nodes
storage_options: {...}     # S3/GCS credentials
embedding: {...}           # Optional auto-embedding config
server: {...}              # gRPC tuning
cache: {...}               # Query cache (worker-side)
health_check: {...}        # Liveness & reconciliation
security: {...}            # TLS + API key RBAC
compaction: {...}          # Worker-side Lance compact loop
orphan_gc: {...}           # Coord-side storage leak cleaner
metadata_path: null        # File or S3 URI for persistent state
default_table_path: null   # Root for runtime-created tables
replica_factor: 2          # Read parallelism (NOT data replication)
```

## `executors` (required)

```yaml
executors:
  - id: w0
    host: 127.0.0.1
    port: 9100
  - id: w1
    host: worker-1.svc.cluster.local
    port: 9100
```

| Field | Type | Notes |
|---|---|---|
| `id` | string | Stable identifier; used in routing metadata |
| `host` | string | Resolvable hostname or IP |
| `port` | uint16 | Worker gRPC port |

**When to tune**: add entries to scale out; in Helm this is generated automatically from StatefulSet DNS.

## `tables` (optional)

Pre-configured shard layout. Typically omitted when using `CreateTable` RPC (auto-sharding).

```yaml
tables:
  - name: products
    shards:
      - name: products_shard_00
        uri: /data/products_shard_00.lance
        executors: [w0, w1]     # primary first; extras = read replicas
```

## `server`

```yaml
server:
  query_timeout_secs: 30
  keepalive_interval_secs: 10
  keepalive_timeout_secs: 20
  concurrency_limit: 256           # tonic per-connection HTTP/2 streams
  max_concurrent_queries: 200      # semaphore rejection threshold
  oversample_factor: 2             # each shard returns k*this, coord merges to k
  max_response_bytes: 268435456    # 256 MiB — per SearchResponse cap
  max_k: 10000                     # hard upper limit on offset+k
  slow_query_ms: 1000              # queries >= this emit 'slow_query' warn log
```

**Tuning guide**:
- `max_concurrent_queries` → 降低会更早返回 RESOURCE_EXHAUSTED；升高要确保 worker 能扛
- `oversample_factor=2` → 对全局 top-k 召回足够；精度要求高可以到 3-4
- `max_response_bytes` → 大查询结果需要时提高（内存成本成正比）
- `slow_query_ms=0` → 关掉慢查询日志（benchmark 时）

## `cache` (worker-side)

```yaml
cache:
  ttl_secs: 60
  max_entries: 1024
  read_consistency_secs: 5    # Lance dataset freshness check interval
```

## `health_check`

```yaml
health_check:
  ping_interval_secs: 5
  ping_timeout_secs: 2
  unhealthy_threshold: 3            # consecutive failures → mark unhealthy
  reconciliation_interval_secs: 10  # shard state reconciler period
  connect_timeout_secs: 5
```

## `security`

### API key 认证 + RBAC

```yaml
security:
  # Legacy mode: all keys get Admin role
  api_keys:
    - legacy-key-1

  # Recommended: explicit roles (read / write / admin)
  api_keys_rbac:
    - key: reader-xxx
      role: read
    - key: writer-xxx
      role: write
    - key: admin-xxx
      role: admin

  # TLS (optional)
  tls_cert: /etc/lanceforge/server.pem
  tls_key:  /etc/lanceforge/server.key
  tls_ca_cert: /etc/lanceforge/ca.pem    # coord→worker verification
```

**Role → 权限矩阵**

| Role | Search | CountRows/ListTables | AddRows/Delete/Upsert | CreateTable/DropTable/Rebalance/MoveShard |
|---|:---:|:---:|:---:|:---:|
| read  | ✓ | ✓ | ✗ | ✗ |
| write | ✓ | ✓ | ✓ | ✗ |
| admin | ✓ | ✓ | ✓ | ✓ |

## `compaction` (worker)

```yaml
compaction:
  enabled: true
  interval_secs: 3600     # 1h
  skip_if_busy: false
```

## `orphan_gc` (coordinator)

```yaml
orphan_gc:
  enabled: false          # opt-in: destructive
  interval_secs: 3600
  min_age_secs: 3600      # don't delete anything younger than this
```

**警告**：`enabled: true` 会删除 `default_table_path` 下未登记的 `.lance` 目录。请确保该路径**仅供 LanceForge 使用**。目前仅支持本地 fs（对象存储 list 待实现）。

## `storage_options` (对象存储凭据)

```yaml
storage_options:
  # S3 / MinIO
  aws_endpoint: http://127.0.0.1:9000
  aws_access_key_id: minioadmin
  aws_secret_access_key: minioadmin
  aws_region: us-east-1
  allow_http: "true"

  # GCS
  google_service_account: /path/to/sa.json

  # Azure
  azure_storage_account_name: myacct
  azure_storage_account_key: ...
```

## `embedding` (optional auto-embedding)

让用户能直接传 `query_text` 而不是 `query_vector`：

```yaml
embedding:
  provider: openai        # or: sentence-transformers
  model: text-embedding-3-small
  api_key: ${OPENAI_API_KEY}
  dimension: 1536
  batch_size: 32
```

## `metadata_path`

- Unset / null：纯内存 + YAML 路由，重启丢状态（不推荐生产）
- `/path/to/meta.json`：本地文件 + fsync + CAS。**仅单 coordinator**
- `s3://bucket/prefix/meta.json`：对象存储 CAS，**支持多 coordinator HA**

## `default_table_path`

`CreateTable` RPC 创建的表放哪里。支持本地路径或 `s3://bucket/prefix/`。

## `replica_factor`

**读并行度**。值为 N 意味着每个 shard 被分配到 N 个 worker（同一个 OBS URI，N 个打开的 handle）。查询时随机选一个；主不可用 failover 到备。**不是数据副本**（OBS 自己负责持久化）。

默认 2。单机开发可以 1。N > 健康 worker 数时自动降级。

## 完整示例

### 最小本地开发

```yaml
executors:
  - { id: w0, host: 127.0.0.1, port: 9100 }
  - { id: w1, host: 127.0.0.1, port: 9101 }
default_table_path: /tmp/lanceforge_dev
```

### 生产 S3 部署

```yaml
executors:
  - { id: w0, host: worker-0.worker.ns.svc, port: 9100 }
  - { id: w1, host: worker-1.worker.ns.svc, port: 9100 }
  - { id: w2, host: worker-2.worker.ns.svc, port: 9100 }

default_table_path: s3://mycompany-lanceforge/prod/
metadata_path: s3://mycompany-lanceforge/prod/metadata.json

storage_options:
  aws_region: us-east-1
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}

server:
  max_response_bytes: 536870912   # 512 MiB
  slow_query_ms: 500

security:
  api_keys_rbac:
    - { key: ${ADMIN_KEY}, role: admin }
    - { key: ${APP_KEY},   role: write }
    - { key: ${RO_KEY},    role: read }
  tls_cert: /etc/certs/server.pem
  tls_key:  /etc/certs/server.key

compaction: { enabled: true, interval_secs: 3600 }
orphan_gc:  { enabled: false }   # 等 S3 list 支持
replica_factor: 2
```
