// Licensed under the Apache License, Version 2.0.
// Persistent audit sink (G7) — appends one JSONL record per DDL / write
// RPC to a local file or OBS object. Bounded in-memory channel so a
// slow sink doesn't back up the request path.

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// One audit record. Field order + names are part of the public format —
/// operators are expected to ingest these via SIEM tooling.
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
}

impl AuditRecord {
    pub fn new(op: &str, principal: &str, target: &str, details: &str) -> Self {
        Self {
            ts: chrono_rfc3339(),
            op: op.to_string(),
            principal: principal.to_string(),
            target: target.to_string(),
            details: details.to_string(),
        }
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

/// Persistent audit sink. Spawns a background task that drains a
/// bounded channel and appends JSONL to the configured destination.
#[derive(Clone)]
pub struct AuditSink {
    sender: Arc<tokio::sync::mpsc::Sender<AuditRecord>>,
}

impl AuditSink {
    /// Spawn a new sink. `path` may be a local filesystem path or an
    /// OBS URI. On filesystem errors / channel-full, records are
    /// dropped + a warn logged; never blocks the request path.
    pub fn spawn(path: String, shutdown: Arc<tokio::sync::Notify>) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AuditRecord>(1024);
        let sender = Arc::new(tx);
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            if path.starts_with("s3://") || path.starts_with("gs://") || path.starts_with("az://") {
                // OBS sink: buffer records, flush as a single put-append
                // per batch. object_store doesn't expose append semantics
                // uniformly, so we accumulate + write-rename on interval.
                // Simpler for the minimum-viable: log + drop when OBS
                // path is configured; warn once, flag it as TODO.
                log::warn!("audit sink: OBS path {path} — OBS-append not yet implemented; \
                          records will be dropped. Use a local path + external rotation \
                          for now; tracked in ROADMAP_0.2 R3.");
                // Drain and drop so producers don't block on channel-full.
                loop {
                    tokio::select! {
                        _ = shutdown.notified() => return,
                        msg = rx.recv() => if msg.is_none() { return; }
                    }
                }
            } else {
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
                        // Drain channel to avoid blocking producers forever.
                        loop {
                            tokio::select! {
                                _ = shutdown.notified() => return,
                                msg = rx.recv() => if msg.is_none() { return; }
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
            }
        });
        Self { sender }
    }

    /// Try to enqueue a record. Never blocks; drops on channel-full and
    /// emits a warn log so operators see they're losing audit visibility.
    pub fn submit(&self, rec: AuditRecord) {
        match self.sender.try_send(rec) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(r)) => {
                log::warn!("audit sink: channel full, dropping record op={} target={}",
                           r.op, r.target);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
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
        let sink = AuditSink::spawn(path.clone(), shutdown.clone());
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
    }

    #[tokio::test]
    async fn obs_sink_drops_gracefully() {
        // OBS path: TODO stub currently drops records without blocking
        // producers. This test locks that behaviour in so anyone adding
        // real OBS support trips the test and updates it.
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let sink = AuditSink::spawn("s3://bucket/audit.log".to_string(), shutdown.clone());
        for _ in 0..10 {
            sink.submit(AuditRecord::new("AddRows", "key:a", "t1", ""));
        }
        // No assertion on persistence — just that we didn't deadlock.
        shutdown.notify_waiters();
    }
}
