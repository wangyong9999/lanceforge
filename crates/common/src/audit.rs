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
/// The caller (coordinator startup) surfaces this to stdout and exits
/// non-zero — no silent drop of compliance data.
#[derive(Debug)]
pub enum AuditConfigError {
    ObsNotSupported(String),
}

impl std::fmt::Display for AuditConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObsNotSupported(p) => write!(
                f,
                "audit_log_path '{p}' is an object-storage URI, but OBS-append \
                 is not implemented (records would be silently dropped). Use a \
                 local filesystem path + external shipper (Vector / Fluent Bit) \
                 until 0.3. See ROADMAP_0.2.md R3."
            ),
        }
    }
}

impl std::error::Error for AuditConfigError {}

/// Validate an `audit_log_path` value before spawning the sink. Currently
/// rejects any object-storage scheme (`s3://`, `gs://`, `az://`) because
/// the sink can't actually append there yet and silent drops are worse
/// than a hard startup error. Local filesystem paths pass through.
///
/// This is F1 fail-fast. Previously the sink warned once at startup and
/// then dropped every record; operators who missed the log line were
/// losing audit visibility without knowing it.
pub fn validate_audit_path(path: &str) -> Result<(), AuditConfigError> {
    const OBS_PREFIXES: &[&str] = &["s3://", "gs://", "az://", "azure://"];
    for p in OBS_PREFIXES {
        if path.starts_with(p) {
            return Err(AuditConfigError::ObsNotSupported(path.to_string()));
        }
    }
    Ok(())
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
    /// Spawn a new sink against a **local filesystem** path. Caller must
    /// have validated the path with [`validate_audit_path`] first.
    ///
    /// On filesystem errors the sink drains-and-drops so producers never
    /// block on channel-full; `metrics.audit_dropped_count` is what
    /// operators should alert on. Channel capacity is 1024 — enough for
    /// RPC burst tolerance, not enough to hide a sustained firehose.
    pub fn spawn(
        path: String,
        shutdown: Arc<tokio::sync::Notify>,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AuditRecord>(1024);
        let sender = Arc::new(tx);
        let metrics_for_spawn = metrics.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            // Local filesystem: open append-only, fsync on shutdown.
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
                    // Drain channel to avoid blocking producers forever;
                    // every dropped record bumps the counter so Prometheus
                    // alerts can fire.
                    loop {
                        tokio::select! {
                            _ = shutdown.notified() => return,
                            msg = rx.recv() => match msg {
                                Some(_) => metrics_for_spawn.record_audit_dropped(),
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
                                // Count as dropped — we couldn't persist it.
                                metrics_for_spawn.record_audit_dropped();
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
    fn validate_audit_path_rejects_obs_schemes() {
        // F1: OBS URIs must fail at config time, not silently drop records.
        for bad in ["s3://b/a", "gs://b/a", "az://b/a", "azure://b/a"] {
            let err = validate_audit_path(bad).unwrap_err();
            let msg = format!("{err}");
            assert!(msg.contains(bad), "err should name the path, got: {msg}");
            assert!(msg.contains("silently dropped") || msg.contains("OBS"));
        }
    }

    #[test]
    fn validate_audit_path_accepts_local() {
        // Local paths — absolute, relative, /tmp — all pass.
        assert!(validate_audit_path("/var/log/lance/audit.jsonl").is_ok());
        assert!(validate_audit_path("./audit.log").is_ok());
        assert!(validate_audit_path("audit.log").is_ok());
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
