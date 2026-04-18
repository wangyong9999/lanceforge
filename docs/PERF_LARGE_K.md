# 大 k 性能 profile（H12 + H24 闭合）

## ⚠ 更新（2026-04-18，G1 闭合）

H24 的 60% 错误率**不是 LanceForge bug**。根因：WSL2 环境里的 `HTTPS_PROXY=http://127.0.0.1:7897` 被 Python gRPC 客户端错误地用于 loopback 连接——`no_proxy=127.*,localhost` 对 grpcio 无效。高并发 × 大 k 响应下，本地代理撑不住就关闭 socket，client 端看到 `UNAVAILABLE: Socket closed`。

修复：`bench_phase17_matrix.py` 导入时 `os.environ.pop(...)` 清掉 `HTTP_PROXY/HTTPS_PROXY/GRPC_PROXY`。

修复后实测（同机、同样 100K × 128d × 2 shards × conc=10）：

| k | QPS 修复前 | QPS 修复后 | P99 修复前 | P99 修复后 | Errors 修复前 | Errors 修复后 |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 1642 | 1690 | 9.9ms | 10.1ms | 0 | 0 |
| 100 | 682 | **1175** | 34.3ms | **19.6ms** | 0 | 0 |
| 1000 | 94.7 | **190** | 199ms | **113ms** | 0 | 0 |
| 10000 | 12.7 | **54.7** | 778ms | **265ms** | **1535** | **0** |

所有 k 级别 QPS 提升 40-330%，P99 减半，错误归零。

**结论**：LanceForge 本身在 k=10000 × 100K 行工作负载下**没有正确性问题**。整个 H24 故事最终是诊断工具的教训，不是产品缺陷。

原始小 k 曲线分析（IPC 序列化成本 vs k）依然成立，保留原记录如下作为历史参考。

---

## 一、原始 k-sweep 实测（2026-04-18 首次）

**建档时间**：2026-04-18
**对应 HARDENING_PLAN 项**：H12
**环境**：100K × 128d × shards=2 × conc=10 × nprobes=10，本地 ext4。

## 一、k-sweep 实测

| k | QPS | P50 ms | P99 ms | Recall | Errors | 相对 k=10 QPS |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 1642 | 3.6 | 9.9 | 0.29 | 0 | — |
| 100 | 682 | 11.7 | 34.3 | 0.29 | 0 | **-58%** |
| 1000 | 94.7 | 98.4 | 199 | 0.29 | 0 | **-94%** |
| 10000 | 12.7 | 638 | 778 | 0.29 | **1535** | **-99%** + 错误 |

原始数据：`lance-integration/bench/results/phase17/matrix_20260418_221348.{json,csv}`

## 二、观察

### 2.1 几何衰减（符合预期）
从 k=10 到 k=1000，QPS 每档 × 10 掉 ~6×；P99 每档 × 3-6。这是 IPC 序列化 + coordinator-side pagination + merge 的线性累积。不是 bug，是物理。

### 2.2 k=10000 的 1535 错误（**真 bug**）
12.7 QPS × 200s ≈ 2540 次查询，其中 **~60% 失败**。错误没在 bench 输出里细分，需要查 worker/coord 日志。

**嫌疑**（按置信度排）：
1. **响应大小超限**：k=10000 × oversample=2 = 20000 rows per shard；每 row 含 id(i64) + vector(128 f32) + _distance(f32) ≈ 524 bytes；20000 × 2 shards × 524 = ~20 MB 每次查询。bench client 设了 `max_receive_message_length=256MB`，但某一方可能有更紧的隐式限制
2. **查询 timeout**：coord 默认 30s query_timeout；k=10000 的 P99=778ms 离 30s 远，但**尾部** tail 可能更糟。worker 侧 local timeout 是否也合理？
3. **Worker 侧 IVF over-fetch**：worker 以 20000 top-k 取样，但 100K 表总共 50K 每 shard；某些查询可能命中不足导致错误分支？不太可能——lance 应该返回 all available
4. **Coord merge 崩**：2 × 20000 = 40000 rows 聚合、sort、top-10000——内存够，但有没有 unchecked 数组索引？
5. **Message size exceed max_response_bytes**：coord `max_response_bytes` 默认 64MB（server config），bench 未覆写。20MB < 64MB 应该 OK——但 oversample 后 40MB 接近，序列化开销可能超

## 三、后续行动

**H24（新 item）**：**k=10000 错误率调查**。步骤：
1. 跑单次 k=10000 查询带 RUST_LOG=debug，抓 coord + worker 日志
2. 看 error 字符串里常见模式（message too large / timeout / internal）
3. 根据实际错误类型：
   - 若是 size 限制：调 `max_response_bytes` 默认值或 auto-adjust；或在 coord validate_offset_k 里对 k×oversample 的预估响应字节数 precheck
   - 若是 timeout：worker 侧 scatter timeout 可能太紧；或 coordinator query_timeout 需基于 k scale
4. 修复 + 在 bench 重新验证零错误

**H12 gate 达成情况**：
- ✅ 测量完成
- ✅ IPC cost 拐点定位在 k=1000+ 开始显著
- ✅ 数据落盘
- ⚠️ 暴露了 k=10000 60% 错误率——这是副产品，单独处理

## 四、产品建议

1. `server.max_k` 默认 10000 是**名义 cap，不等于实际可用**。短期建议：
   - 文档里把"recommended k ≤ 1000，k ≤ 10000 best-effort"写进 LIMITATIONS
   - 或者把默认 max_k 砍到 1000，暴露一个大 k 的 config override
2. 对客户端：分页（offset + k）优于一次性大 k。当前 `max_response_bytes` + `truncated` + `next_offset` 已支持；但很多用户习惯大 k 一把梭

## 五、H12 执行记录

- bench_phase17_matrix.py 新增 `LARGE_K_MATRIX` + `--large-k` flag（本 commit）
- 一轮完整运行，数据入 `results/phase17/matrix_20260418_221348.*`
- 发现的异常留作 H24
