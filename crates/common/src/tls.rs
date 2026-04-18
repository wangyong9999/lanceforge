// Licensed under the Apache License, Version 2.0.
// Server-side TLS loading helpers (H17).
//
// Both coordinator and worker binaries inline the same cert/key read +
// Identity construction. Extract it here so:
//   1. The error path (missing file, unreadable) gets consistent wording
//      the operator can diff across logs.
//   2. The filesystem-level failure modes are unit-testable without
//      standing up a full tonic server.
//
// Note: tonic::transport::Identity::from_pem is infallible at
// construction — invalid PEM content only surfaces during the TLS
// handshake. So unit tests here can cover file-level errors cleanly,
// but handshake failures (expired cert, wrong CA, etc.) need
// integration-style coverage. That's filed as beta work.

use std::path::Path;

/// Load a server-side TLS config from cert + key files on disk.
/// Returns a Result so the calling bin can surface a readable error
/// to the operator instead of panicking on startup.
pub async fn load_server_tls_config(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<tonic::transport::ServerTlsConfig, String> {
    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();
    let cert = tokio::fs::read(cert_path).await
        .map_err(|e| format!("read cert {}: {e}", cert_path.display()))?;
    let key = tokio::fs::read(key_path).await
        .map_err(|e| format!("read key {}: {e}", key_path.display()))?;
    if cert.is_empty() {
        return Err(format!("cert {} is empty", cert_path.display()));
    }
    if key.is_empty() {
        return Err(format!("key {} is empty", key_path.display()));
    }
    let identity = tonic::transport::Identity::from_pem(cert, key);
    Ok(tonic::transport::ServerTlsConfig::new().identity(identity))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn write(path: &std::path::Path, bytes: &[u8]) {
        let mut f = tokio::fs::File::create(path).await.unwrap();
        f.write_all(bytes).await.unwrap();
    }

    #[tokio::test]
    async fn load_rejects_missing_cert() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("nope.pem");
        let key = dir.path().join("key.pem");
        write(&key, b"fake key").await;
        let err = load_server_tls_config(&cert, &key).await.err().unwrap();
        assert!(err.contains("read cert"), "got: {err}");
        assert!(err.contains("nope.pem"), "error must include the offending path");
    }

    #[tokio::test]
    async fn load_rejects_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("missing.pem");
        write(&cert, b"fake cert").await;
        let err = load_server_tls_config(&cert, &key).await.err().unwrap();
        assert!(err.contains("read key"), "got: {err}");
        assert!(err.contains("missing.pem"));
    }

    #[tokio::test]
    async fn load_rejects_empty_cert() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        write(&cert, b"").await;
        write(&key, b"fake key").await;
        let err = load_server_tls_config(&cert, &key).await.err().unwrap();
        assert!(err.contains("cert") && err.contains("empty"), "got: {err}");
    }

    #[tokio::test]
    async fn load_rejects_empty_key() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        write(&cert, b"fake cert").await;
        write(&key, b"").await;
        let err = load_server_tls_config(&cert, &key).await.err().unwrap();
        assert!(err.contains("key") && err.contains("empty"), "got: {err}");
    }

    #[tokio::test]
    async fn load_accepts_nonempty_bytes() {
        // tonic Identity::from_pem is infallible at construction — it
        // doesn't parse until TLS handshake happens. So any non-empty
        // cert+key content succeeds here; actual cert validity is
        // proven by e2e_tls_test.py, not by this unit test.
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        write(&cert, b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n").await;
        write(&key, b"-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n").await;
        let res = load_server_tls_config(&cert, &key).await;
        assert!(res.is_ok(), "non-empty PEM-shaped input must succeed at load time: {res:?}");
    }
}
