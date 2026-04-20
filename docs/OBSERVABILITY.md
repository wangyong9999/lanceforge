# LanceForge Observability

Three pillars, three endpoints:

| Pillar | Endpoint | Mechanism |
|---|---|---|
| **Metrics** | `http://coord:${port+1}/metrics` | Prometheus text format (scrape it) |
| **Logs** | stdout / stderr of coord + worker processes | `tracing`; JSON when `LOG_FORMAT=json` |
| **Traces** | OTLP gRPC, opt-in | `OTEL_EXPORTER_OTLP_ENDPOINT` + `--features otel` build |

All three carry the same `trace_id` when the client sends a W3C
`traceparent` header — that's how you correlate a slow query line in
the worker log with the coord audit entry and (optionally) the
Tempo/Jaeger span.

## 1. Metrics (always on)

`coord:{rest_port}/metrics` emits:

- Counters: `lance_query_total`, `lance_query_errors_total`,
  `lance_slow_query_total`, `lance_audit_dropped_total`,
  `lance_table_query_total{table=}`,
  `lance_table_query_errors_total{table=}`.
- Gauges: `lance_executors_healthy`, `lance_executors_total`,
  `lance_query_latency_max_ms`, `lance_query_latency_avg_ms`.
- Histogram: `lance_query_latency_ms_bucket{le=}` with buckets
  `1 2 5 10 25 50 100 250 500 1000 2500 5000 10000` ms.

Ship-ready Grafana dashboard: `deploy/grafana/lanceforge.json`.
Suggested alert rules: `deploy/grafana/README.md`.

## 2. Logs (always on)

Every DDL and write RPC produces an audit line:

```
audit op=AddRows principal=key:alice…  target=tenant-a/orders
      details=bytes=98304 trace_id=0af7651916cd43dd8448eb211c80319c
```

Set `security.audit_log_path` in `config.yaml` to also write a
structured JSONL record per entry (local path **or** `s3://…` since
0.2.0-beta.3 — see B2 in the release notes). Record shape:

```json
{
  "ts": "2026-04-20T12:34:56.789Z",
  "op": "AddRows",
  "principal": "key:alice…",
  "target": "tenant-a/orders",
  "details": "bytes=98304 trace_id=...",
  "trace_id": "0af7651916cd43dd8448eb211c80319c"
}
```

The top-level `trace_id` field (F9) is the authoritative one; the
embedded `trace_id=<hex>` substring inside `details` is retained for
one release window for SIEM parsers built against
0.2.0-beta.1/beta.2.

### Cross-process correlation

Clients send `traceparent: 00-<32 hex>-<16 hex>-01`. The coordinator:

1. Extracts the 32-hex `trace_id` portion
2. Includes it in the audit line + JSONL record
3. Re-emits a fresh `traceparent` on each outbound worker RPC (B1)
   — same trace_id, new per-shard span_id

Worker logs then carry `local_search trace_id=<hex> table=<name>` at
`info!` level, so `grep trace_id=<hex>` across coord + worker logs
returns a continuous request story.

## 3. Traces via OTLP (opt-in)

Build with the feature flag:

```bash
cargo build --release --features otel --bin lance-coordinator
```

Set the collector endpoint before starting the coordinator:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
  lance-coordinator config.yaml
```

Spans produced by `#[tracing::instrument]` / `tracing::info!` in the
codebase are exported over gRPC to the collector. Resource attribute
`service.name=lance-coordinator` is set automatically. When the env
var is absent, the OTEL layer is not installed — zero cost.

### What a minimal collector looks like

`docker-compose.yaml` snippet:

```yaml
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml
    ports: ["4317:4317"]

  tempo:
    image: grafana/tempo:latest
    command: ["-config.file=/etc/tempo.yaml"]
    volumes:
      - ./tempo.yaml:/etc/tempo.yaml
    ports: ["3200:3200"]
```

`otel-collector-config.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
exporters:
  otlp/tempo:
    endpoint: tempo:4317
    tls: {insecure: true}
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/tempo]
```

Then point Grafana at the Tempo datasource and the `trace_id`
strings from coord's audit lines become clickable links to the
full trace.

## 4. Known limitations

- **Worker-side span emission**: worker does not yet emit OTLP
  spans. The `trace_id` propagates and appears in worker logs, but
  a worker-node span won't show up in Tempo/Jaeger until the
  worker binary also grows the `otel` feature and wire-up.
  Scheduled for 0.3.
- **REST → gRPC bridge**: HTTP `traceparent` headers hitting
  `coord:{rest_port}/v1/*` aren't bridged into the in-process
  gRPC metadata yet. Clients talking over REST get logs + metrics;
  trace correlation requires the gRPC client path.
- **Span attributes**: the current implementation emits whatever
  `tracing` spans exist (operation name, file:line, basic context)
  but no custom attributes like `table`, `k`, `recall`. Add
  `#[instrument(fields(...))]` to the hot RPCs in 0.3 — deferred
  because the useful attribute set depends on what shows up most
  often in operator investigations, which we're collecting now.
- **Sampling**: every span is exported. For high-QPS deployments,
  add an OTLP collector-side sampler (tail-based) until the
  coord-side sampler lands.
