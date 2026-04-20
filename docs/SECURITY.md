# LanceForge Security Guide

This document describes the security model and how to configure encryption,
authentication, and authorization in a production LanceForge deployment.

## 1. Threat model

LanceForge is a distributed vector retrieval engine that stores all durable
state in object storage (S3, GCS, Azure Blob). The coordinator and worker
processes are stateless — any state they hold in memory is a derived read
cache that is rebuilt from object storage on restart.

The threat model we defend against:

- **Network eavesdropping** on client ↔ coordinator and coordinator ↔ worker
  traffic. Mitigation: TLS 1.3 via rustls (§3).
- **Unauthorized API calls.** Mitigation: API-key auth with three-role RBAC
  (§4). Per-key QPS quota (G6) bounds abuse impact.
- **Tenant → tenant data access** in a shared deployment. Mitigation: API
  key ↔ namespace binding (G5). See §5 for what this covers and what it
  does not.
- **Storage-level data exposure** if the object-storage bucket is
  compromised or subpoenaed at rest. Mitigation: server-side encryption at
  rest via the object store's KMS / SSE support (§6).
- **Audit tampering or loss** after an incident. Mitigation: persistent
  JSONL audit log, one record per DDL or write RPC (G7). Records land in
  an append-only file or object-storage URI (§7).

What we do **not** defend against:

- A malicious operator with shell access to the coordinator/worker host.
- A malicious operator of the object storage account.
- Side-channel attacks (timing, EM). Your container orchestration is
  responsible for node-level isolation.
- Application-level vulnerabilities in client code that leak API keys.
- Denial-of-service at the network layer (run LanceForge behind an L7 LB
  or API gateway that provides DDoS protection).

## 2. Configuration summary

All security knobs live under `security:` in the coordinator's YAML:

```yaml
security:
  # TLS (server-side)
  tls_cert: /etc/lance/tls/server.crt
  tls_key:  /etc/lance/tls/server.key
  tls_ca_cert: /etc/lance/tls/ca.crt        # enables client-cert auth (mTLS)

  # API key auth
  api_keys_rbac:
    - key: "alice-read"
      role: read                             # read | write | admin
      namespace: tenant-a                    # G5: bind key to a namespace
      qps_limit: 100                         # G6: per-key rate cap
    - key: "bob-admin"
      role: admin
      namespace: tenant-b

  # Audit log (G7)
  audit_log_path: s3://ops-bucket/audit/lanceforge.jsonl

# Storage credentials + encryption options
storage_options:
  aws_access_key_id: AKIA...
  aws_secret_access_key: ...
  aws_region: us-east-1
  # SSE-KMS: server-side encryption managed by AWS KMS.
  aws_server_side_encryption: aws:kms
  aws_sse_kms_key_id: arn:aws:kms:us-east-1:123:key/abc-def
```

## 3. TLS in transit

- Provide a PEM-encoded cert + key pair via `security.tls_cert` and
  `security.tls_key`. Coordinator listens on TLS when both are set.
- Optionally require client certs by setting `security.tls_ca_cert`. When
  set, unauthenticated TCP connections are rejected before the gRPC handler
  runs.
- The loader in `crates/common/src/tls.rs` rejects empty files and files
  that don't contain a valid PEM block — misconfigured paths fail fast at
  startup, not at the first request.
- Minimum TLS version: 1.2. Preferred: 1.3. Cipher suites are the rustls
  defaults (Chacha20-Poly1305, AES-GCM).
- Workers inherit the same TLS settings when the coordinator calls them;
  in-cluster RPC can be TLS-terminated at a service mesh instead, but only
  if the mesh guarantees mTLS.

## 4. Authentication and RBAC

- API keys are arbitrary opaque strings. Clients send them in the
  `authorization` metadata header (`Authorization: Bearer <key>`).
- Three roles: `read`, `write`, `admin`. Higher roles include lower. RPC
  permission requirements:

| RPC                                | Required role |
|------------------------------------|---------------|
| AnnSearch / FtsSearch / HybridSearch / CountRows / GetSchema / ListTables | read  |
| AddRows / DeleteRows / UpsertRows  | write         |
| CreateTable / DropTable / CreateIndex / Rebalance / LoadShard | admin         |

- Keys without an explicit role (legacy `api_keys:` list) default to
  `admin`. This is kept for 0.1 → 0.2 compatibility. 0.3.0 will default
  to `read` — configure roles explicitly before upgrading.
- Key storage: config file (static) or MetaStore (`auth/keys/{key}`
  prefix, JSON-encoded role). The MetaStore-backed registry hot-reloads
  without restarting the coordinator.

## 5. Multi-tenant namespace binding (G5, minimal form)

> **Read this first.** Namespace binding is an **API-layer** boundary,
> not a storage-layer one. A tenant holding your object-storage
> credentials can read or write Lance files under any prefix — the
> LanceForge coordinator is the enforcement point, the bucket is not.
> **If your tenancy model requires cryptographic isolation (regulated
> industries, untrusted co-tenants, per-tenant data-residency
> commitments), run one LanceForge deployment per tenant with separate
> buckets and IAM roles.** The rest of this section describes what
> namespace binding does give you: a correct, auditable API-layer
> isolation that makes casual cross-tenant access return
> `PermissionDenied`. Combined with TLS + per-key quota + audit log,
> it's enough to meet "operators cannot read each other's tables via
> the documented API" requirements. Use it accordingly.

Each API key may carry a `namespace` field. When set, every RPC that
names a table (`CreateTable`, `Search`, `AddRows`, `DropTable`, …) is
rejected with `PermissionDenied` unless the table name begins with
`{namespace}/`. `ListTables` is filtered to names matching the caller's
namespace prefix.

**In scope** for the 0.2-beta minimum:

- Table-name prefix validation on every mutating RPC and every read RPC
  that takes a table argument.
- `ListTables` response filtering.
- Admin-role keys without a namespace can see and touch every table
  (escape hatch for operators).

**Out of scope** (documented in `LIMITATIONS.md` §9):

- MetaStore-side enforcement: a rogue client with direct OBS credentials
  can still read/write Lance files under any prefix (see the boxed
  warning above — this is the storage-layer caveat).
- Per-namespace quotas (storage, connection, bucket sharing).
- Namespace rename / deletion primitives.
- Automatic namespace provisioning.

## 6. Encryption at rest

LanceForge writes Lance files to object storage. Encryption at rest is
delegated to the object store — we do not implement a second envelope
encryption layer. The three supported modes, in order of preference for
production:

### 6.1 SSE-KMS (AWS S3 / GCP Cloud KMS)

Highest assurance: keys are held in a managed HSM, per-object encryption
is transparent, and the audit trail of key usage is in CloudTrail / Cloud
Audit Logs.

```yaml
storage_options:
  aws_server_side_encryption: aws:kms
  aws_sse_kms_key_id: arn:aws:kms:us-east-1:123:key/abc-def
```

Permissions required on the IAM role / service account:

- `kms:Encrypt`, `kms:Decrypt`, `kms:GenerateDataKey` on the KMS key.
- `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` on
  the bucket.

### 6.2 SSE-S3 (default S3-managed keys)

Simpler; keys are managed by S3. Acceptable for workloads that don't need
dedicated key custody.

```yaml
storage_options:
  aws_server_side_encryption: AES256
```

### 6.3 SSE-C (customer-provided keys)

The caller supplies a 256-bit key on every request. LanceForge supports
this by passing the `aws_sse_customer_*` options through to the underlying
`object_store` crate — but note that **every read and every write must
carry the key**, and losing the key means losing the data. This is tested
under `lance-integration/tests/test_encryption_sse.py` against MinIO.

```yaml
storage_options:
  aws_server_side_encryption_customer_algorithm: AES256
  aws_server_side_encryption_customer_key: <base64 of 32-byte key>
  aws_server_side_encryption_customer_key_md5: <base64 md5 of the key>
```

### 6.4 GCS / Azure

For GCS, set `google_application_credentials` to a service account with
KMS-enabled buckets. Azure Blob CMK is configured at the storage account
level; LanceForge only needs the access credentials.

### 6.5 What we do not encrypt

- **In-memory caches.** Worker RecordBatch cache holds plaintext
  page-level data. If the node is memory-dumped, this is exposed.
- **Local FileMetaStore (dev only).** The `metadata_path: /local/…` mode
  writes plaintext JSON. Production must use `s3://…` or equivalent.
- **The audit log file itself.** The audit sink writes plaintext JSONL.
  Encrypt the target path via your OBS backend (SSE-KMS on the audit
  bucket) or put the file on an encrypted filesystem.

## 7. Audit log (G7)

When `security.audit_log_path` is set, every DDL and write RPC emits one
JSONL record to the configured destination in addition to the stdout log
line. Schema:

```json
{"ts":"2026-04-19T11:22:33.456Z","op":"AddRows","principal":"key:alice-r…","target":"tenant-a/orders","details":"bytes=98304"}
```

- `ts`: RFC3339 UTC timestamp at the coordinator.
- `op`: RPC name.
- `principal`: `key:<first 8 chars>…` or `anonymous` / `no-auth`.
- `target`: table or shard touched; `<inline>` for schema-less creates.
- `details`: freeform (filter text, row count, bytes written).

Supported destinations:

- **Local filesystem path** (`/var/log/lance/audit.jsonl`). Appended,
  `fsync_all()` on shutdown. Use `logrotate` for retention.
- **Object storage URI** (`s3://…`, `gs://…`, `az://…`). **Note:** the
  0.2-beta.1 build emits a warning and drops records for OBS paths
  (object stores do not expose append semantics uniformly). Use a local
  file + external shipper (Vector, Fluent Bit) until 0.3. See
  `ROADMAP_0.2.md` R3.

**SIEM ingestion**: the JSONL format is ingest-compatible with Splunk,
Elastic, Grafana Loki, and Datadog. Example Vector config:

```toml
[sources.lance_audit]
type = "file"
include = ["/var/log/lance/audit.jsonl"]

[sinks.siem]
type = "elasticsearch"
inputs = ["lance_audit"]
endpoints = ["https://siem.internal:9200"]
```

## 8. Key rotation

- **API keys**: update the MetaStore (`auth/keys/{old_key}` → remove,
  `auth/keys/{new_key}` → add). The registry hot-reload polls every
  5 seconds; new keys become active without restart.
- **TLS certs**: restart the coordinator. Rolling restart across
  multi-coordinator HA is seamless because each instance is stateless.
- **KMS keys** (SSE-KMS): rotate in AWS/GCP console; existing objects
  stay encrypted with the old key version until re-written. No
  LanceForge action required.
- **SSE-C keys**: re-encrypt by re-writing every object under the new
  key. There is no in-place rotation path.

## 9. Checklist for a hardened production deployment

- [ ] `deployment_profile: saas` or `self_hosted` (never `dev` in prod).
- [ ] `metadata_path` points to OBS (`s3://…`), never a local path.
- [ ] TLS enabled with real certs (Let's Encrypt / internal CA).
- [ ] `api_keys_rbac` used exclusively; `api_keys` (legacy) list empty.
- [ ] Every operator key has an explicit `namespace`. Only the on-call /
      break-glass key lacks one.
- [ ] `qps_limit` set on every non-admin key.
- [ ] `audit_log_path` set; log shipper verified to ingest records.
- [ ] SSE-KMS or SSE-S3 enabled on the storage bucket.
- [ ] Bucket policy denies unencrypted writes.
- [ ] Bucket policy restricts PUT/DELETE to the LanceForge IAM role.
- [ ] Coordinator/worker pods have minimal network egress (only OBS +
      in-cluster).
- [ ] `/metrics` endpoint on coordinator and worker is **not** publicly
      exposed — scrape from a private network only.
