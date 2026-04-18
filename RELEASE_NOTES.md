# LanceForge Release Notes

## 0.2.0-alpha.1 — 2026-04-18

**First alpha on the SaaS-first track.** Meets every gate in
`docs/ROADMAP_0.2.md` §3.3 (alpha exit criteria) at minimum-viable
depth. Backward compatible with 0.1.0 on wire and on disk.

### Highlights beyond 0.2.0-pre.1

- **B1.2.3 runtime API-key rotation** (closes `STATELESS_INVARIANTS`
  Gap A). When a MetaStore is configured, the coordinator bootstraps
  config keys into `auth/keys/` and a 60 s background task swaps the
  live registry atomically. Adding or revoking a key no longer
  requires a rolling restart.
- **B2.2 read/write role routing**. New `ExecutorRole::{Either,
  ReadPrimary, WritePrimary}` config knob; coordinator picks the
  best-matching worker per request. Combined with `read_consistency_
  secs=60` and `replica_factor=2`, the 10 QPS writes mixed workload
  recovers to **82.5% of read-only baseline** (up from 38% on
  0.1.0). Recommended production config is in `docs/LIMITATIONS.md`
  §11.
- **B3.1 / B3.2 version advertising** (already in pre.1): clients can
  discover server version via `HealthCheckResponse.server_version`;
  MetaStore JSON snapshots carry `schema_version = 1` with a
  forward-version reject gate and an auto-upgrade-on-write path for
  pre-0.2 snapshots.
- **B3.5 compatibility policy** — `docs/COMPAT_POLICY.md` formalises
  the three-dimensional contract (wire / persistence / config),
  deprecation windows, and break procedure.
- **B4-min coverage matrix** — `docs/COVERAGE_MATRIX.md` measures
  branch coverage on the four critical modules (scatter_gather /
  merge / metastore / connection_pool) and pins baselines that only
  ratchet up.
- **B5-min chaos framework** — `chaos/runner.py` + two scenarios
  (`worker_kill`, `worker_stall`), 10 iterations each, 100% pass
  rate required for alpha. Result archives in `chaos/results/`.
- **B6-min soak harness** — `soak/run.py` runs a light mixed
  workload and samples RSS / open-fd drift per minute. The alpha
  gate is 2 h with < 3 % drift; a 15-minute smoke run ships with
  this release.

### Not yet in alpha

- B2.2 does not yet close the 50 QPS writes scenario (still -81 %
  of baseline). That needs fragment-level cache invalidation and
  is a 0.2-beta item.
- B4's full ≥ 85 % branch coverage on the four modules — we ratchet
  per-release, the current numbers are the honest baseline.
- B5 full chaos fault library (net partition / OOM / S3 throttle /
  clock skew) and the nightly 100-iter CI job — beta.
- B6 nightly 24 h soak CI job — beta. The 2 h run is a release-
  time human-triggered gate for alpha.
- B3.3 `lance-admin migrate` CLI — deferred until we have a real
  schema migration to dispatch (probably 0.3).

### Upgrade notes (from 0.2.0-pre.1 or 0.1.0)

- Adding `deployment_profile: saas` to production configs will
  start rejecting local `metadata_path` values at boot. If your
  cluster is on `file://` MetaStore today, either keep
  `deployment_profile: self_hosted` (or unset — defaults to `dev`)
  or migrate the JSON to S3 first.
- Existing YAML with no `deployment_profile` continues to work.
- Legacy `api_keys` (non-RBAC format) still default to `Admin` in
  0.2.x. The flip to `Read` is scheduled for 0.3.0; see
  `COMPAT_POLICY.md` §9.

Full change list: [`CHANGELOG.md`](./CHANGELOG.md).

---

## 0.2.0-pre.1 — 2026-04-18

Opens the 0.2 / SaaS-first cycle. Backward compatible with 0.1.0 on
wire and on disk; pins the plumbing every later step needs.

**Highlights**:
- `deployment_profile` config field with startup-time guards
  (`saas` rejects local `metadata_path`; `self_hosted` requires
  explicit MetaStore).
- MetaStore JSON snapshot now version-stamped (`schema_version=1`);
  reads auto-upgrade from pre-0.2 snapshots, forward-version
  snapshots rejected.
- `HealthCheckResponse` advertises `server_version`.
- Connection-pool flake fixed: stale tonic Channel is now replaced
  whenever a worker transitions back from unhealthy.
- Ballista logically isolated from the default `cargo build` /
  `test` / `clippy` path via `default-members`; still buildable with
  explicit `--workspace`.

**Not yet**: the remaining 0.2 Blockers (B2 mixed-RW cache tiering,
B4 critical-path coverage matrix, B5 chaos framework, B6 24h soak)
are tracked in `docs/ROADMAP_0.2.md`. This is a **pre-alpha** — it's
safe to adopt on 0.1 workloads that already run here, not yet
recommended for net-new production deployments.

Full change list: [`CHANGELOG.md`](./CHANGELOG.md). Forward plan:
[`docs/ROADMAP_0.2.md`](./docs/ROADMAP_0.2.md). Honest limitation
inventory: [`docs/LIMITATIONS.md`](./docs/LIMITATIONS.md) +
[`docs/STATELESS_INVARIANTS.md`](./docs/STATELESS_INVARIANTS.md).

---

# LanceForge 1.0-rc Release Notes

**Release date**: 2026-04-15
**Tag**: `1.0-rc1`

## TL;DR

LanceForge is a distributed vector retrieval engine on top of Lance + object storage. After 17 phases of focused development, it now has:

- **正确性**：写入无重复、Saga 回滚、fsync、merge_insert hash-partition
- **安全**：API key + 三级 RBAC + TLS + 审计日志
- **运维**：MoveShard / Rebalance / Compaction / Orphan GC / 慢查询日志 / per-table metrics
- **弹性**：分页、响应大小上限、副本读 failover、saga 失败回滚
- **观测**：Prometheus 指标 + 慢查询日志 + 审计日志 + 健康检查
- **文档**：8 份完整文档 + 1 个真实 RAG 端到端示例
- **测试**：272 单测 + 6 套 E2E + 4 种规模 benchmark + RAG 质量验证

实测性能（128d，单机 WSL）：
- 100K × shards=1: **1927 QPS @ P99 6.5 ms** (read-only, conc=10)
- 1M × shards=2: **1774 QPS @ P99 7.0 ms** (read-only, conc=10)
- 1M × nprobes=80: **0.91 recall@10 @ 1889 QPS**
- BEIR SciFact RAG: nDCG@10 / Recall@100 与 numpy 暴力 baseline **完全一致**

## What's New since legacy releases

### Phase 11-16 — Core engine improvements (committed previously)

| Phase | 主题 |
|---|---|
| 11 | Auto-shard CreateTable + 写入无重复 + size-aware rebalance |
| 12 | MetaStore fsync + CreateTable saga 回滚 + RBAC 三级 |
| 13 | 分页 offset + 响应大小上限 + 审计日志 + 自动 Compaction |
| 14 | MoveShard 管理 RPC + 慢查询日志 + per-table 指标 |
| 15 | Delete/Upsert shard-level 路由 + DropTable 真删 OBS 文件 |
| 16 | Upsert hash-partition + 孤儿存储 GC |

### Phase 17 — Production validation (this release)

**5 个真实 bug 通过 benchmark + RAG E2E 暴露并修复**：
1. gRPC 默认 4 MiB decoding cap → 提升到 256 MiB+ 16 MiB
2. Client send 256 MiB cap → benchmark 改分块 ingest，coord default 升级
3. CreateTable 不自动建索引 → docs 强调，bench 显式建
4. conc=50 QPS 崩溃（误判服务端，实为客户端单 channel 序列化）→ docs 三处警告 + 修 bench
5. RAG nDCG 异常低（_distance sign 约定）→ rank-based scoring

**经验公式实测**：1M × 128d × 128 partitions 下达到 0.9 recall 需要 nprobes=80（之前文档预测的 30 偏低）

**8 份核心文档**：
- `docs/QUICKSTART.md` - 10 分钟首次跑通
- `docs/ARCHITECTURE.md` - 设计反思（为什么不做 2PC/quorum/resync）
- `docs/CONFIG.md` - 每个 YAML 字段
- `docs/DEPLOYMENT.md` - Helm chart + K8s
- `docs/OPERATIONS.md` - SRE runbook
- `docs/TROUBLESHOOTING.md` - 错误码 → 修复
- `docs/BENCHMARK.md` - 实测数据
- `docs/LIMITATIONS.md` - 11 条诚实边界

**第一个真场景**：`examples/rag_beir/` — BEIR SciFact RAG 端到端，质量与 numpy 暴力 baseline 完全一致

## Breaking Changes

无。Phase 11-17 全部向下兼容已部署的 YAML 配置和 gRPC proto。

## Default Changes（值得注意）

- `server.max_response_bytes`: 64 MiB → **256 MiB**（避免大 IPC 报错）
- gRPC 服务端 decoding/encoding limits 自动设为 `max_response_bytes + 16 MiB`
- gRPC 客户端连接池：256 MiB 收发上限

旧配置无需修改即可工作。

## Known Limitations

详见 [LIMITATIONS.md](docs/LIMITATIONS.md)，简要：

1. Upsert 与 Add 路由不一致（hash vs round-robin）→ 罕见场景下同 id 可能两份
2. `replica_factor` 是读并行度，不是数据副本
3. orphan_gc 当前仅本地 fs，对象存储 GC 待加
4. 多 coordinator 必须用 S3MetaStore；FileMetaStore 仅单机
5. **客户端必须每线程一个 gRPC channel**（Phase 17 实测教训）

## Performance Tips

```yaml
# 1M+ 行场景推荐起始配置
server:
  max_response_bytes: 268435456    # 256 MiB
  max_k: 10000
  slow_query_ms: 500

replica_factor: 2                   # 1M 行最优
```

```python
# 客户端：每线程一个 channel
def make_stub():
    return lance_service_pb2_grpc.LanceSchedulerServiceStub(
        grpc.insecure_channel("coord:9200",
            options=[("grpc.max_receive_message_length", 256*1024*1024),
                     ("grpc.max_send_message_length",   256*1024*1024)]))

# 高并发场景
with ThreadPoolExecutor(max_workers=50) as ex:
    futures = [ex.submit(lambda: make_stub().AnnSearch(...)) for _ in range(N)]
```

## Roadmap (driven by real user feedback)

不预先做的事，等触发条件：
- ⏳ Add hash-by-PK（等用户报 Upsert 重复 issue）
- ⏳ OpenTelemetry tracing（等首次跨节点排障失败）
- ⏳ 对象存储 GC（等 S3 客户账单异常）
- ⏳ Python SDK 异步 + 流式（等真实大批量场景）
- ⏳ Schema evolution（等真用户需求）

## Compatibility

- Rust: 1.70+
- Python: 3.10+
- Storage: S3 / GCS / Azure / 本地 fs
- K8s: 1.24+ (Helm 3)

## Acknowledgments

Built on [Lance](https://github.com/lancedb/lance) + [LanceDB](https://github.com/lancedb/lancedb) for storage and IVF indexing.

## How to Verify This Release

```bash
# 1. Unit tests
cargo test --release --workspace --lib

# 2. E2E suite (Phase 11-16) + smoke benchmark (~3 min)
bash lance-integration/tools/run_all_e2e.sh

# 3. Real-world RAG (~2 min)
python3 examples/rag_beir/run_beir_scifact.py
```

期望产出：272 单测全过；E2E 6/6 + smoke benchmark 通过；RAG nDCG@10 = baseline 一致。
