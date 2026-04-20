# Grafana dashboard

`lanceforge.json` is an importable Grafana dashboard covering the
metrics the LanceForge coordinator exposes at `/metrics`. Ship it
with any deployment that needs a "what's happening?" view within
five minutes.

## Import

Grafana → Dashboards → New → Import → upload `lanceforge.json` →
pick your Prometheus datasource in the variable dropdown.

## Panel inventory

| Panel | Metric | Alert-worthy threshold |
|---|---|---|
| QPS | `rate(lance_query_total[1m])` | capacity planning only |
| Error rate | `errors / total` | > 1% ⇒ page |
| Healthy workers | `lance_executors_healthy / total` | < 1.0 ⇒ warn |
| **Audit records dropped** | `lance_audit_dropped_total` | **any non-zero ⇒ page (compliance)** |
| Latency P50/P95/P99 | `histogram_quantile(lance_query_latency_ms_bucket)` | P99 > SLA ⇒ warn |
| QPS by table | `rate(lance_table_query_total[1m])` by table | hot-table detection |
| Slow-query rate | `rate(lance_slow_query_total[1m])` | sustained > 0 ⇒ investigate |
| Per-table error rate | `table_errors / table_total` | > 1% ⇒ investigate |
| Max query latency | `lance_query_latency_max_ms` | tail latency outlier |
| Executor health | `healthy` and `total` | divergence ⇒ worker loss |

## Required metrics (0.2.0-beta.2 contract)

The dashboard queries these metric names. They are emitted by the
coordinator's `/metrics` endpoint.

- `lance_query_total` (counter)
- `lance_query_errors_total` (counter)
- `lance_query_latency_ms` (histogram, buckets: 1 2 5 10 25 50 100 250 500 1000 2500 5000 10000)
- `lance_query_latency_max_ms` (gauge)
- `lance_executors_healthy` (gauge)
- `lance_executors_total` (gauge)
- `lance_slow_query_total` (counter)
- `lance_audit_dropped_total` (counter) — added in 0.2.0-beta.2 / F1
- `lance_table_query_total` (counter, labelled by `table`)
- `lance_table_query_errors_total` (counter, labelled by `table`)

## Alerting suggestions

These three alerts cover the most-valuable pages an operator wants:

```yaml
- alert: LanceForgeAuditDropped
  expr: increase(lance_audit_dropped_total[5m]) > 0
  for: 1m
  labels: {severity: critical}
  annotations:
    summary: "LanceForge dropped audit records — compliance blind spot"
    runbook: "docs/SECURITY.md §7 + check coordinator logs for 'audit sink' warnings"

- alert: LanceForgeErrorRate
  expr: |
    sum(rate(lance_query_errors_total[5m]))
      / clamp_min(sum(rate(lance_query_total[5m])), 1) > 0.01
  for: 5m
  labels: {severity: page}
  annotations:
    summary: "LanceForge error rate > 1% for 5m"

- alert: LanceForgeNoHealthyExecutor
  expr: lance_executors_healthy == 0
  for: 1m
  labels: {severity: page}
  annotations:
    summary: "All LanceForge workers unhealthy"
```

## Known gaps (planned for 0.3)

- **Worker-side metrics**: `/metrics` on the worker node exposes the
  same shape but this dashboard doesn't import worker instance
  labels yet. Add a worker dashboard in 0.3.
- **Per-namespace breakdowns** (G5): once namespace is a first-class
  metric label (today it's baked into the `table` label's prefix),
  add a namespace-grouped panel.
- **Cache hit/miss** (worker-side, once the worker metrics endpoint
  is imported).
