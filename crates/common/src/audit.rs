// Licensed under the Apache License, Version 2.0.
// Persistent audit sink (G7) — appends one JSONL record per DDL / write
// RPC to a local file or OBS object. Bounded in-memory channel so a
// slow sink doesn't back up the request path.

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// One audit record. Field order + names are part of the public format —
/// operators are expected to ingest these via SIEM tooling.
///
/// **Compatibility**: `trace_id` was added as a top-level field in
/// 0.2.0-beta.2 (F9). Before that it was only available embedded in
/// `details` as `trace_id=<hex>`. Writers in 0.2.0-beta.2+ populate both
/// the field and the details substring for one version, so SIEM
/// parsers can migrate without coordinated downtime. 0.3 drops the
/// details substring; treat the top-level field as authoritative now.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    /// ISO-8601 timestamp at the coordinator when the action was accepted.
    pub ts: String,
    /// Operation name: `CreateTable`, `AddRows`, `DropTable`, etc.
    pub op: String,
    /// Short principal identifier (`key:abc123…` or `anonymous`). Never
    /// the full API key — that's what ApiKeyInterceptor::principal does.
    pub principal: String,
    /// Target table / shard / resource the action touched. `<inline>`
    /// for creates.
    pub target: String,
    /// Freeform operator-facing details (filter text, row count, etc).
    pub details: String,
    /// W3C traceparent `trace_id` (32 hex chars) when the caller supplied
    /// one in the request metadata. `None` / absent in JSON when the
    /// caller didn't send a traceparent. Present from 0.2.0-beta.2; SIEM
    /// parsers built against 0.2.0-beta.1 will see an extra key and
    /// should ignore-unknown per JSON norms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl AuditRecord {
    pub fn new(op: &str, principal: &str, target: &str, details: &str) -> Self {
        Self {
            ts: chrono_rfc3339(),
            op: op.to_string(),
            principal: principal.to_string(),
            target: target.to_string(),
            details: details.to_string(),
            trace_id: None,
        }
    }

    /// Attach a W3C trace_id. Returns self so callers can chain.
    pub fn with_trace_id(mut self, trace_id: Option<String>) -> Self {
        self.trace_id = trace_id;
        self
    }

    pub fn to_jsonl(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Minimal RFC3339 timestamp without pulling in `chrono`. Uses
/// `std::time::SystemTime` + integer arithmetic. Precision: milliseconds.
fn chrono_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let millis = d.subsec_millis();
    // Turn secs into date + time manually (days-per-year arithmetic). We
    // ingest these in SIEM tooling, so plain ISO-8601 is good enough —
    // don't add a dep just for this.
    // Epoch day for 1970-01-01 = 0.
    let epoch_day = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);
    let (h, m, s) = (
        seconds_of_day / 3600,
        (seconds_of_day / 60) % 60,
        seconds_of_day % 60,
    );
    let (y, mo, d) = civil_from_days(epoch_day);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
}

/// Howard Hinnant's civil-from-days algorithm. Input: days since
/// 1970-01-01. Output: (year, month 1-12, day 1-31). Correct from
/// year 0 to at least year 32767.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468; // shift epoch to 0000-03-01
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Returned by [`validate_audit_path`] when a path is refused at config time.
/// The caller (coordinator startup) surfaces this and exits non-zero.
/// Currently only malformed URIs trigger this — OBS URIs are accepted
/// and routed through the batch-PUT sink (B2 / 0.2.0-beta.3).
#[derive(Debug)]
pub enum AuditConfigError {
    InvalidUri(String),
}

impl std::fmt::Display for AuditConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUri(p) => write!(
                f,
                "audit_log_path '{p}' is not a parseable local path or URI"
            ),
        }
    }
}

impl std::error::Error for AuditConfigError {}

/// Validate an `audit_log_path` value before spawning the sink.
/// Accepts local filesystem paths and OBS URIs (s3:// / gs:// / az://).
/// 0.2.0-beta.2 rejected OBS at this layer; 0.2.0-beta.3 routes them
/// through the batch-PUT sink (new object per batch, no append) —
/// SIEM shippers ingest via prefix-list.
pub fn validate_audit_path(path: &str) -> Result<(), AuditConfigError> {
    if path.is_empty() {
        return Err(AuditConfigError::InvalidUri(path.to_string()));
    }
    Ok(())
}

/// Whether an audit_log_path maps to an object-storage backend.
pub fn path_is_obs(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("gs://")
        || path.starts_with("az://") || path.starts_with("azure://")
}

/// Persistent audit sink. Spawns a background task that drains a
/// bounded channel and appends JSONL to the configured local path.
///
/// OBS URIs are rejected upstream by [`validate_audit_path`] — see F1.
#[derive(Clone)]
pub struct AuditSink {
    sender: Arc<tokio::sync::mpsc::Sender<AuditRecord>>,
    /// Shared metrics handle so `submit` can bump `audit_dropped_total`
    /// on channel-full / sink-closed (F1). Never None after spawn.
    metrics: Arc<crate::metrics::Metrics>,
}

impl AuditSink {
    /// Spawn a new sink against either a local filesystem path or an
    /// object-storage URI (`s3://` / `gs://` / `az://`). Caller must
    /// have validated the path with [`validate_audit_path`] first.
    ///
    /// Local filesystem: append-only, fsync on shutdown.
    ///
    /// OBS (B2): batch up to 100 records **or** 30 seconds (whichever
    /// first) into one object named
    /// `{uri}/audit-{epoch_ms}-{rand_hex}.jsonl`. No append — every
    /// batch is a new object. SIEM shippers (Vector, Fluent Bit) list
    /// the prefix + sort by timestamp in the key. Individual records
    /// are 300–500 bytes each; at 100 records per object the typical
    /// batch is ~40 KB. At RPC burst rates exceeding 100/s the batch
    /// flushes on the 100-record trigger; at trickle rates the 30s
    /// timer guarantees SIEM sees records within 30s.
    ///
    /// On filesystem / OBS errors the sink drains-and-drops so
    /// producers never block on channel-full; `audit_dropped_count`
    /// bumps for every lost record so operators alert on it.
    pub fn spawn(
        path: String,
        shutdown: Arc<tokio::sync::Notify>,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<AuditRecord>(1024);
        let sender = Arc::new(tx);
        if path_is_obs(&path) {
            spawn_obs_sink(path, rx, shutdown, metrics.clone());
        } else {
            spawn_local_sink(path, rx, shutdown, metrics.clone());
        }
        Self { sender, metrics }
    }

    /// Try to enqueue a record. Never blocks; drops on channel-full /
    /// sink-closed and bumps `lance_audit_dropped_total` so operators
    /// see they're losing audit visibility without having to grep logs.
    pub fn submit(&self, rec: AuditRecord) {
        match self.sender.try_send(rec) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(r)) => {
                self.metrics.record_audit_dropped();
                log::warn!("audit sink: channel full, dropping record op={} target={}",
                           r.op, r.target);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.record_audit_dropped();
                log::warn!("audit sink: sink closed, record dropped");
            }
        }
    }
}

/// Spawn the local-filesystem variant of the audit sink. Records are
/// appended to `path` and fsynced on shutdown. Drains-and-drops on
/// open failure (and bumps `audit_dropped_count` for every drop).
fn spawn_local_sink(
    path: String,
    mut rx: tokio::sync::mpsc::Receiver<AuditRecord>,
    shutdown: Arc<tokio::sync::Notify>,
    metrics: Arc<crate::metrics::Metrics>,
) {
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let p = PathBuf::from(&path);
        if let Some(parent) = p.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true).append(true).open(&p).await
        {
            Ok(f) => f,
            Err(e) => {
                log::error!("audit sink: failed to open {path}: {e}");
                loop {
                    tokio::select! {
                        _ = shutdown.notified() => return,
                        msg = rx.recv() => match msg {
                            Some(_) => metrics.record_audit_dropped(),
                            None => return,
                        }
                    }
                }
            }
        };
        log::info!("audit sink: appending to {path}");
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    let _ = file.sync_all().await;
                    log::info!("audit sink: shutdown, flushed");
                    return;
                }
                msg = rx.recv() => match msg {
                    Some(record) => {
                        let line = record.to_jsonl() + "\n";
                        if let Err(e) = file.write_all(line.as_bytes()).await {
                            metrics.record_audit_dropped();
                            log::error!("audit sink: write failed: {e}");
                        }
                    }
                    None => {
                        let _ = file.sync_all().await;
                        return;
                    }
                }
            }
        }
    });
}

/// Spawn the OBS variant (B2). Buffers records up to
/// `OBS_BATCH_RECORDS` (100) or `OBS_FLUSH_INTERVAL` (30 s), then PUTs
/// one object per batch at `{prefix}/audit-{epoch_ms}-{rand_hex}.jsonl`.
/// Uses `object_store::parse_url_opts` so the URI is handled the same
/// way Lance / S3MetaStore parse theirs — same credentials (env /
/// instance role / object_store options) apply.
///
/// On PUT failure the batch is dropped + counted. No retry yet —
/// 0.3 can layer exponential backoff when we have more operator
/// feedback on which failure modes actually happen in the wild.
const OBS_BATCH_RECORDS: usize = 100;
const OBS_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

fn spawn_obs_sink(
    uri: String,
    mut rx: tokio::sync::mpsc::Receiver<AuditRecord>,
    shutdown: Arc<tokio::sync::Notify>,
    metrics: Arc<crate::metrics::Metrics>,
) {
    tokio::spawn(async move {
        // Resolve the URI into (ObjectStore, prefix-path). `parse_url_opts`
        // needs a full URL; we pass the operator-provided URI directly.
        let parsed = match url::Url::parse(&uri) {
            Ok(u) => u,
            Err(e) => {
                log::error!("audit sink: invalid OBS URI {uri}: {e}");
                drain_and_count(&mut rx, &shutdown, &metrics).await;
                return;
            }
        };
        let (store, prefix) = match object_store::parse_url_opts::<_, String, String>(
            &parsed,
            std::iter::empty(),
        ) {
            Ok(pair) => pair,
            Err(e) => {
                log::error!("audit sink: parse_url_opts {uri} failed: {e}");
                drain_and_count(&mut rx, &shutdown, &metrics).await;
                return;
            }
        };
        log::info!("audit sink: OBS PUT-per-batch to {uri} (max {} records / {:?})",
                   OBS_BATCH_RECORDS, OBS_FLUSH_INTERVAL);

        let mut batch: Vec<AuditRecord> = Vec::with_capacity(OBS_BATCH_RECORDS);
        let mut flush_timer = tokio::time::interval(OBS_FLUSH_INTERVAL);
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    if !batch.is_empty() {
                        put_batch(&store, &prefix, &batch, &metrics).await;
                    }
                    log::info!("audit sink: OBS shutdown, last batch flushed ({} records)",
                               batch.len());
                    return;
                }
                _ = flush_timer.tick() => {
                    if !batch.is_empty() {
                        put_batch(&store, &prefix, &batch, &metrics).await;
                        batch.clear();
                    }
                }
                msg = rx.recv() => match msg {
                    Some(record) => {
                        batch.push(record);
                        if batch.len() >= OBS_BATCH_RECORDS {
                            put_batch(&store, &prefix, &batch, &metrics).await;
                            batch.clear();
                        }
                    }
                    None => {
                        if !batch.is_empty() {
                            put_batch(&store, &prefix, &batch, &metrics).await;
                        }
                        return;
                    }
                }
            }
        }
    });
}

/// Drain the channel until shutdown, bumping the dropped counter on
/// every record. Used when the OBS URI can't be parsed so we don't
/// block producers on channel-full.
async fn drain_and_count(
    rx: &mut tokio::sync::mpsc::Receiver<AuditRecord>,
    shutdown: &Arc<tokio::sync::Notify>,
    metrics: &Arc<crate::metrics::Metrics>,
) {
    loop {
        tokio::select! {
            _ = shutdown.notified() => return,
            msg = rx.recv() => match msg {
                Some(_) => metrics.record_audit_dropped(),
                None => return,
            }
        }
    }
}

/// PUT one object holding `batch` records as JSONL. Object key is
/// `{prefix}/audit-{epoch_ms}-{hex16}.jsonl`. Failures bump the
/// counter for every record in the batch.
async fn put_batch(
    store: &dyn object_store::ObjectStore,
    prefix: &object_store::path::Path,
    batch: &[AuditRecord],
    metrics: &Arc<crate::metrics::Metrics>,
) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)
        .unwrap_or_default().as_millis();
    // Randomish 16-hex suffix for uniqueness across N coord pods.
    let rand_hex = {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)
            .unwrap_or_default().subsec_nanos();
        let mix = (millis as u64).wrapping_mul(0x100000001b3) ^ nanos as u64;
        format!("{:016x}", mix)
    };
    let filename = format!("audit-{millis:013}-{rand_hex}.jsonl");
    let key = if prefix.as_ref().is_empty() {
        object_store::path::Path::from(filename.clone())
    } else {
        object_store::path::Path::from(format!("{}/{}", prefix.as_ref(), filename))
    };
    let mut body = Vec::with_capacity(batch.len() * 300);
    for r in batch {
        body.extend_from_slice(r.to_jsonl().as_bytes());
        body.push(b'\n');
    }
    match store.put(&key, bytes::Bytes::from(body).into()).await {
        Ok(_) => log::debug!("audit sink: PUT {} ({} records)", key, batch.len()),
        Err(e) => {
            log::error!("audit sink: PUT {} failed: {e}", key);
            for _ in batch {
                metrics.record_audit_dropped();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_serializes_as_single_jsonl_line() {
        let r = AuditRecord::new("AddRows", "key:abc", "t1", "bytes=100");
        let line = r.to_jsonl();
        assert!(!line.contains('\n'), "jsonl line must not contain newline");
        let decoded: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(decoded["op"], "AddRows");
        assert_eq!(decoded["principal"], "key:abc");
        assert_eq!(decoded["target"], "t1");
    }

    #[test]
    fn timestamp_is_rfc3339_shape() {
        let ts = chrono_rfc3339();
        // "YYYY-MM-DDTHH:MM:SS.mmmZ" — 24 chars.
        assert_eq!(ts.len(), 24, "got: {ts}");
        assert!(ts.ends_with('Z'));
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
        assert_eq!(&ts[10..11], "T");
    }

    #[test]
    fn civil_from_days_known_dates() {
        // 1970-01-01 = day 0
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2000-01-01 = day 10957
        assert_eq!(civil_from_days(10957), (2000, 1, 1));
        // 2020-02-29 = leap-year edge case
        assert_eq!(civil_from_days(18321), (2020, 2, 29));
        // 2026-04-18 — today in this repo's working context
        assert_eq!(civil_from_days(20561), (2026, 4, 18));
    }

    #[tokio::test]
    async fn local_sink_writes_jsonl_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log").to_string_lossy().to_string();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let metrics = crate::metrics::Metrics::new();
        let sink = AuditSink::spawn(path.clone(), shutdown.clone(), metrics.clone());
        sink.submit(AuditRecord::new("CreateTable", "key:a", "t1", ""));
        sink.submit(AuditRecord::new("AddRows", "key:a", "t1", "bytes=5"));
        // Give the background task a moment to write.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.notify_waiters();
        // Give it a moment to fsync.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "expected 2 audit lines, got {lines:?}");
        for line in lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(v["ts"].as_str().unwrap().ends_with('Z'));
        }
        // Nothing should have been dropped on the happy path.
        assert_eq!(metrics.audit_dropped_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn validate_audit_path_accepts_local_and_obs() {
        // 0.2.0-beta.3 / B2: OBS URIs are accepted (route through the
        // PUT-per-batch sink). Local paths still work. Only an empty
        // string is refused.
        for ok in ["/var/log/lance/audit.jsonl", "./audit.log", "audit.log",
                   "s3://b/a", "gs://b/a", "az://b/a"] {
            assert!(validate_audit_path(ok).is_ok(), "expected ok for {ok}");
        }
        assert!(validate_audit_path("").is_err());
    }

    #[test]
    fn path_is_obs_recognises_schemes() {
        assert!(path_is_obs("s3://b/a"));
        assert!(path_is_obs("gs://b/a"));
        assert!(path_is_obs("az://b/a"));
        assert!(path_is_obs("azure://b/a"));
        assert!(!path_is_obs("/tmp/audit.log"));
        assert!(!path_is_obs("file:///tmp/x.log"));
    }

    #[tokio::test]
    async fn submit_past_capacity_bumps_dropped_counter() {
        // F1: channel-full must bump audit_dropped_count, not just warn.
        // Trigger it by filling the 1024-slot bounded channel before the
        // background task has a chance to drain. We pause the sink
        // consumer by withholding shutdown and relying on filesystem
        // latency — faster path: call submit 2000 times in a tight loop
        // so the channel saturates before the writer can keep up.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log").to_string_lossy().to_string();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let metrics = crate::metrics::Metrics::new();
        let sink = AuditSink::spawn(path.clone(), shutdown.clone(), metrics.clone());
        for i in 0..5000 {
            sink.submit(AuditRecord::new("AddRows", "key:a", "t1", &format!("i={i}")));
        }
        // Give the writer time to drain + count any drops along the way.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        shutdown.notify_waiters();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let dropped = metrics.audit_dropped_count.load(std::sync::atomic::Ordering::Relaxed);
        let written = tokio::fs::read_to_string(&path).await
            .map(|s| s.lines().count() as u64).unwrap_or(0);
        assert_eq!(dropped + written, 5000,
                   "every submit must be either written or counted as dropped, \
                    got {written} written + {dropped} dropped");
    }
}
