# LanceForge Kubernetes 部署

基于 `deploy/helm/lanceforge/` chart 的生产部署指南。

## 架构

```
               ┌────────────┐
               │  Clients    │
               └─────┬──────┘
                     │ gRPC :9200
                     ▼
        ┌──────────────────────────────┐
        │ Coordinator Deployment (N=2) │
        │ + Service (ClusterIP)         │
        │ + Prometheus endpoint :9201   │
        └──────┬───────────────────────┘
               │ gRPC fan-out
               ▼
   ┌───────────┬──────────┬──────────┐
   │ Worker-0  │ Worker-1 │ Worker-2 │  StatefulSet (stable DNS)
   │ :9100     │ :9100    │ :9100    │  Headless Service
   └─────┬─────┴──────────┴──────────┘
         │
         ▼
   ┌────────────────────────────────┐
   │ S3 / GCS / Azure Object Store   │ (Lance .lance directories)
   │ + metadata.json (coordinator CAS)│
   └─────────────────────────────────┘
```

## 前置

- Kubernetes 1.24+
- Helm 3
- 对象存储桶 + 凭据（AWS/GCP/Azure 任一）
- （可选）ingress controller（如果需要外网访问）

## Quick Deploy

```bash
cd deploy/helm/lanceforge

# 1) 填凭据
cat > secrets.yaml <<EOF
aws_access_key_id: YOUR_KEY
aws_secret_access_key: YOUR_SECRET
EOF
kubectl create secret generic lanceforge-s3 --from-file=secrets.yaml -n lanceforge

# 2) 安装
helm install lanceforge . \
  --namespace lanceforge --create-namespace \
  --set workers.replicas=3 \
  --set coordinator.replicas=2 \
  --set storage.s3Bucket=my-lanceforge-prod \
  --set storage.s3Region=us-east-1

# 3) 验证
kubectl get pods -n lanceforge
kubectl port-forward svc/lanceforge-coordinator 9200:9200 -n lanceforge

# 测试
grpcurl -plaintext localhost:9200 lance.distributed.LanceSchedulerService/ListTables
```

## 必须调整的 values

`deploy/helm/lanceforge/values.yaml` 里：

```yaml
coordinator:
  replicas: 2                  # HA：至少 2
  image: your-registry/lanceforge-coordinator:v1.0
  resources:
    requests: {cpu: 500m, memory: 1Gi}
    limits:   {cpu: 2,    memory: 4Gi}

workers:
  replicas: 3                  # 按 QPS 规划
  image: your-registry/lanceforge-worker:v1.0
  resources:
    requests: {cpu: 1,    memory: 4Gi}
    limits:   {cpu: 4,    memory: 16Gi}

storage:
  backend: s3                  # s3 / gcs / azure
  s3Bucket: my-lanceforge-prod
  s3Region: us-east-1
  # Credentials come from the secret; see above
  metadataPath: s3://my-lanceforge-prod/metadata.json

security:
  rbacKeys:
    - key: ${ADMIN_KEY_FROM_ENV}
      role: admin
    - key: ${APP_KEY_FROM_ENV}
      role: write
  tls:
    enabled: false             # 需要时启用；证书来自 cert-manager
```

## 架构要点

### Coordinator 为什么是 Deployment（无状态）
- MetaStore 走 S3，所有 coordinator 共享状态
- 任意一个 coordinator 挂掉，其他继续服务
- 扩容：增加 replicas 即可
- 升级：滚动更新零停机

### Worker 为什么是 StatefulSet
- 需要稳定 DNS 名：`worker-0.worker.lanceforge.svc`
- Shard 路由元数据绑定到 `worker_id`（来自 pod 名）
- `podManagementPolicy: Parallel` 让启动并发，加速集群初始化

### 为什么 Worker 不需要 PVC
- 所有数据在对象存储上
- Worker 本地只是 gRPC handle + LRU 缓存
- Pod 删除重建后，自动重新从 OBS 加载

## 升级流程

```bash
# 无中断滚动升级
helm upgrade lanceforge . \
  --namespace lanceforge \
  --set coordinator.image=your-registry/lanceforge-coordinator:v1.1 \
  --set workers.image=your-registry/lanceforge-worker:v1.1

# K8s 会逐个替换 pod；客户端 gRPC 自动重连
```

**注意**：如果 proto 有 breaking change，先升级 server，再升级 client。Phase 11-16 的 proto 是向下兼容的，可直接滚动。

## HPA（自动扩缩）

当前 chart 未内置 HPA 定义，按需添加：

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: lanceforge-workers
  namespace: lanceforge
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: lanceforge-worker
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: Resource
      resource: { name: cpu, target: { type: Utilization, averageUtilization: 70 } }
    - type: Pods
      pods:
        metric: { name: lance_query_latency_avg_ms }
        target: { type: AverageValue, averageValue: "50" }
```

**警告**：Worker 扩容后数据不会自动 rebalance。需要人工触发：

```bash
kubectl exec -it lanceforge-coordinator-xxx -- \
  grpcurl -plaintext localhost:9200 lance.distributed.LanceSchedulerService/Rebalance
```

## 监控

Chart 自带 Prometheus endpoint `:9201/metrics`。接入：

```yaml
# ServiceMonitor (prometheus-operator)
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: lanceforge
spec:
  selector:
    matchLabels: { app: lanceforge-coordinator }
  endpoints:
    - port: metrics
      interval: 30s
```

关键指标：
- `lance_query_total`, `lance_query_errors_total`
- `lance_query_latency_avg_ms`, `lance_query_latency_max_ms`
- `lance_slow_query_total`
- `lance_executors_healthy` / `lance_executors_total`
- `lance_table_query_total{table="..."}` — hot table 检测

## 安全

1. **API Key 来自 K8s Secret**：不要硬编码在 values.yaml
2. **TLS 通过 cert-manager**：coord ↔ worker 加密
3. **NetworkPolicy**：只允许应用 namespace 访问 coordinator
4. **Pod Security Standard**：restricted 可以，不需要特权

## 故障恢复

### Coordinator 全挂
- 任一 coord pod 重启会重新从 S3 拉元数据
- 客户端 gRPC retry 自动恢复

### Worker 全挂
- 数据不丢（OBS 持久化）
- Pod 重建后，coord 的 reconciler 会重新分配 shard
- 恢复时间：pod startup + HealthCheck 周期（~10-30s）

### S3 暂时不可用
- 已 open 的 Table handle 可能仍能从本地缓存响应
- 新查询（需要读最新 manifest）会报错
- 恢复后自动重试

## Helm chart 源文件

详见 `deploy/helm/lanceforge/`:
- `Chart.yaml`：版本
- `values.yaml`：默认值
- `templates/coordinator-deployment.yaml`
- `templates/worker-statefulset.yaml`
- `templates/coordinator-service.yaml`
- `templates/worker-service.yaml` (headless)
- `templates/configmap.yaml`：渲染 cluster_config.yaml
- `templates/serviceaccount.yaml`
