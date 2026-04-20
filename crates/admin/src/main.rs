// Licensed under the Apache License, Version 2.0.
// lance-admin — operator CLI for LanceForge.
//
// Scope in 0.2-beta (minimal): dump / load the MetaStore, and list the
// table → shard-URI mapping the MetaStore records. Shard .lance files
// themselves are the operator's responsibility — use `aws s3 sync` /
// `rclone` / `gsutil cp` against the URIs this tool prints. Keeping the
// copy step out of lance-admin avoids re-implementing multi-cloud object
// transport; the MetaStore snapshot is the only piece that's LanceForge-
// specific, so that's the piece this tool owns.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

use lance_distributed_meta::store::{
    FileMetaStore, MetaStore, S3MetaStore,
};

#[derive(Parser, Debug)]
#[command(
    name = "lance-admin",
    version,
    about = "LanceForge operator CLI",
    long_about = "LanceForge operator CLI. Subcommands dump, restore, and \
        inspect the coordinator's MetaStore. Shard .lance files are not \
        touched — list them via `lance-admin shards list` and copy them \
        out-of-band with your cloud provider's tooling."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// MetaStore dump / restore / list.
    Meta {
        #[command(subcommand)]
        action: MetaCmd,
    },
    /// Shard URI inspection.
    Shards {
        #[command(subcommand)]
        action: ShardsCmd,
    },
}

#[derive(Subcommand, Debug)]
enum MetaCmd {
    /// Read every key/value pair in the MetaStore and write a JSON
    /// snapshot to --out (or stdout if omitted).
    Dump {
        /// MetaStore location. Either a local path or an object-storage
        /// URI (s3://, gs://, az://).
        #[arg(long)]
        meta: String,
        /// Output file. When omitted, the snapshot is written to stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Load a JSON snapshot into the MetaStore. Does not delete keys
    /// that are in the target but not the snapshot — pass --purge to
    /// force strict replacement.
    Restore {
        #[arg(long)]
        meta: String,
        #[arg(long)]
        from: PathBuf,
        /// Delete any keys in the destination that aren't in the
        /// snapshot before writing. Dangerous on a live cluster.
        #[arg(long, default_value_t = false)]
        purge: bool,
        /// Print the set of changes that would be applied, but do not
        /// write anything. Use before running without --dry-run on a
        /// live metastore. Implies --yes (no prompting in dry mode).
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// Bypass the interactive confirmation prompt that guards
        /// `--purge`. Required for non-interactive contexts (CI, cron)
        /// when `--purge` is set. Ignored without `--purge`.
        #[arg(long, default_value_t = false)]
        yes: bool,
    },
    /// List keys at a given prefix. Useful for spot-checking.
    List {
        #[arg(long)]
        meta: String,
        #[arg(long, default_value = "")]
        prefix: String,
    },
}

#[derive(Subcommand, Debug)]
enum ShardsCmd {
    /// Print the `table -> [(shard_name, shard_uri)]` mapping the
    /// MetaStore records. Output is plain lines; pipe into `awk` to
    /// derive the list of URIs you need to copy out-of-band.
    List {
        #[arg(long)]
        meta: String,
        /// Restrict output to one table (substring match on table name).
        #[arg(long)]
        table: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Snapshot {
    /// Schema version for the snapshot file itself. Separate from the
    /// MetaStore's own schema_version key. Bumped when this tool's
    /// layout changes.
    snapshot_version: u32,
    /// Everything we read. Keys are the full MetaStore keys;
    /// `Versioned<String>` is collapsed into `{value, version}`.
    entries: std::collections::BTreeMap<String, SnapshotEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SnapshotEntry {
    value: String,
    version: u64,
}

async fn open_store(uri: &str) -> anyhow::Result<Arc<dyn MetaStore>> {
    // `s3://`, `gs://`, `az://` hit the real object-storage backend; the
    // `file://` scheme routes through S3MetaStore's `object_store`
    // adapter too, which is what production uses for single-file
    // `memory://` or local-disk test harnesses (and what
    // `crates/meta/src/store.rs` own tests exercise). Bare paths go to
    // FileMetaStore. This split lets operators point `lance-admin` at
    // the exact same URI string their `config.yaml::metadata_path`
    // holds, without having to know which backend the coord picked.
    if uri.starts_with("s3://")
        || uri.starts_with("gs://")
        || uri.starts_with("az://")
        || uri.starts_with("azure://")
        || uri.starts_with("file://")
        || uri.starts_with("memory://")
    {
        let store = S3MetaStore::new(uri, std::iter::empty::<(String, String)>()).await?;
        Ok(Arc::new(store))
    } else {
        let store = FileMetaStore::new(uri).await?;
        Ok(Arc::new(store))
    }
}

async fn cmd_dump(meta: &str, out: Option<PathBuf>) -> anyhow::Result<()> {
    let store = open_store(meta).await?;
    let all = store.get_prefix("").await?;
    let mut snap = Snapshot {
        snapshot_version: 1,
        entries: std::collections::BTreeMap::new(),
    };
    for (k, v) in all {
        snap.entries.insert(k, SnapshotEntry { value: v.value, version: v.version });
    }
    let json = serde_json::to_string_pretty(&snap)?;
    match out {
        Some(path) => {
            tokio::fs::write(&path, &json).await?;
            log::info!("wrote {} entries to {}", snap.entries.len(), path.display());
        }
        None => {
            use std::io::Write;
            std::io::stdout().write_all(json.as_bytes())?;
            std::io::stdout().write_all(b"\n")?;
        }
    }
    Ok(())
}

/// Plan rendered by `cmd_restore` before it touches the store. Kept as
/// a struct so `--dry-run` can print it deterministically and the purge
/// confirmation prompt can show the operator what's about to happen.
#[derive(Debug, Default)]
struct RestorePlan {
    /// Keys in the snapshot that don't exist in the target.
    to_create: Vec<String>,
    /// Keys in the snapshot whose value differs from the target's.
    to_update: Vec<String>,
    /// Keys in the target that aren't in the snapshot (only populated
    /// when `--purge` is set).
    to_delete: Vec<String>,
}

impl RestorePlan {
    fn is_noop(&self) -> bool {
        self.to_create.is_empty() && self.to_update.is_empty() && self.to_delete.is_empty()
    }

    fn print(&self, purge: bool) {
        println!("=== restore plan ===");
        println!("  create: {} keys", self.to_create.len());
        for k in &self.to_create { println!("    + {k}"); }
        println!("  update: {} keys", self.to_update.len());
        for k in &self.to_update { println!("    ~ {k}"); }
        if purge {
            println!("  DELETE: {} keys (--purge)", self.to_delete.len());
            for k in &self.to_delete { println!("    - {k}"); }
        } else if !self.to_delete.is_empty() {
            println!("  ({} keys in target are not in snapshot; pass --purge to delete)",
                     self.to_delete.len());
        }
    }
}

async fn compute_restore_plan(
    store: &dyn MetaStore,
    snap: &Snapshot,
    purge: bool,
) -> anyhow::Result<RestorePlan> {
    let existing = store.get_prefix("").await?;
    let mut plan = RestorePlan::default();
    for (k, e) in &snap.entries {
        match existing.get(k) {
            None => plan.to_create.push(k.clone()),
            Some(cur) if cur.value != e.value => plan.to_update.push(k.clone()),
            Some(_) => { /* same value, no-op */ }
        }
    }
    if purge {
        let kept: std::collections::HashSet<&String> = snap.entries.keys().collect();
        for k in existing.keys() {
            if !kept.contains(k) {
                plan.to_delete.push(k.clone());
            }
        }
    } else {
        // Populate to_delete anyway so dry-run shows what `--purge` would
        // hit; the print() path hides it when `--purge` is off.
        let kept: std::collections::HashSet<&String> = snap.entries.keys().collect();
        for k in existing.keys() {
            if !kept.contains(k) {
                plan.to_delete.push(k.clone());
            }
        }
    }
    Ok(plan)
}

/// Block until the operator types `yes` on stdin, or return an error if
/// stdin isn't a TTY (CI / cron case). Called before purge proceeds
/// without `--yes`.
fn prompt_purge_confirmation(plan: &RestorePlan) -> anyhow::Result<()> {
    use std::io::{BufRead, Write};
    plan.print(true);
    if !std::io::stdin().is_terminal() {
        anyhow::bail!(
            "--purge would delete {} keys from a non-interactive session; \
             pass --yes to confirm automatically, or run from a TTY",
            plan.to_delete.len()
        );
    }
    print!("\nProceed with purge + restore? Type 'yes' to continue: ");
    std::io::stdout().flush().ok();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    if line.trim() != "yes" {
        anyhow::bail!("restore aborted (operator did not type 'yes')");
    }
    Ok(())
}

async fn cmd_restore(
    meta: &str,
    from: &std::path::Path,
    purge: bool,
    dry_run: bool,
    yes: bool,
) -> anyhow::Result<()> {
    let store = open_store(meta).await?;
    let raw = tokio::fs::read_to_string(from).await?;
    let snap: Snapshot = serde_json::from_str(&raw)?;
    if snap.snapshot_version != 1 {
        anyhow::bail!(
            "snapshot version {} not supported by this lance-admin build",
            snap.snapshot_version
        );
    }

    let plan = compute_restore_plan(store.as_ref(), &snap, purge).await?;

    if dry_run {
        plan.print(purge);
        println!("(dry-run — no changes written)");
        return Ok(());
    }

    if plan.is_noop() {
        log::info!("restore: nothing to do (target already matches snapshot)");
        return Ok(());
    }

    // Guard `--purge` with a confirmation. `--yes` bypasses it.
    if purge && !yes {
        prompt_purge_confirmation(&plan)?;
    } else if purge {
        plan.print(true);
        log::info!("--yes passed, skipping interactive confirmation");
    }

    if purge {
        for k in &plan.to_delete {
            store.delete(k).await?;
            log::info!("purge: deleted {k}");
        }
    }

    let n = snap.entries.len();
    for (k, e) in snap.entries {
        // Use expected_version=0 for create; on conflict we fall back to
        // a read→put pair that preserves the snapshot's value.
        match store.put(&k, &e.value, 0).await {
            Ok(_) => log::info!("created {k}"),
            Err(lance_distributed_meta::store::MetaError::VersionConflict { actual, .. }) => {
                store.put(&k, &e.value, actual).await?;
                log::info!("updated {k}");
            }
            Err(e) => return Err(e.into()),
        }
    }
    log::info!("restored {n} entries to {meta}");
    Ok(())
}

async fn cmd_list(meta: &str, prefix: &str) -> anyhow::Result<()> {
    let store = open_store(meta).await?;
    let keys = store.list_keys(prefix).await?;
    for k in keys {
        println!("{k}");
    }
    Ok(())
}

async fn cmd_shards_list(meta: &str, table_filter: Option<&str>) -> anyhow::Result<()> {
    let store = open_store(meta).await?;
    // Shard URI registry lives at `shards/{shard_name}` in MetaShardState;
    // routing lives at `tables/{table_name}` as JSON {shard -> [workers]}.
    let table_prefix = "tables/";
    let shard_prefix = "shards/";

    let tables = store.get_prefix(table_prefix).await?;
    let shards = store.get_prefix(shard_prefix).await?;

    let shard_uri = |name: &str| -> Option<String> {
        shards.get(&format!("{shard_prefix}{name}")).map(|v| v.value.clone())
    };

    for (full_key, versioned) in tables {
        let table_name = full_key.strip_prefix(table_prefix).unwrap_or(&full_key);
        if let Some(f) = table_filter
            && !table_name.contains(f) { continue; }
        // Each table value is JSON: {shard_name: [worker_ids...]}
        let mapping: std::collections::HashMap<String, Vec<String>> =
            match serde_json::from_str(&versioned.value) {
                Ok(m) => m,
                Err(e) => {
                    log::warn!("table {table_name}: bad routing JSON: {e}");
                    continue;
                }
            };
        for shard in mapping.keys() {
            match shard_uri(shard) {
                Some(uri) => println!("{table_name}\t{shard}\t{uri}"),
                None => println!("{table_name}\t{shard}\t<no-uri>"),
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Meta { action } => match action {
            MetaCmd::Dump { meta, out } => cmd_dump(&meta, out).await,
            MetaCmd::Restore { meta, from, purge, dry_run, yes } =>
                cmd_restore(&meta, &from, purge, dry_run, yes).await,
            MetaCmd::List { meta, prefix } => cmd_list(&meta, &prefix).await,
        },
        Cmd::Shards { action } => match action {
            ShardsCmd::List { meta, table } => cmd_shards_list(&meta, table.as_deref()).await,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store() -> (Arc<dyn MetaStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.json").to_string_lossy().to_string();
        let store: Arc<dyn MetaStore> =
            Arc::new(FileMetaStore::new(&path).await.unwrap());
        (store, dir)
    }

    #[tokio::test]
    async fn dump_then_restore_roundtrip_recovers_every_entry() {
        let (src, _src_dir) = make_store().await;
        src.put("auth/keys/alice", "{\"role\":\"Read\"}", 0).await.unwrap();
        src.put("auth/keys/bob", "{\"role\":\"Admin\"}", 0).await.unwrap();
        src.put("tables/t1", "{\"shard_0\":[\"w0\"]}", 0).await.unwrap();

        // Dump manually (avoid spawning the CLI). Mirrors cmd_dump logic.
        let all = src.get_prefix("").await.unwrap();
        let snap = Snapshot {
            snapshot_version: 1,
            entries: all.into_iter()
                .map(|(k, v)| (k, SnapshotEntry { value: v.value, version: v.version }))
                .collect(),
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_string(&snap).unwrap()).unwrap();

        let dst_dir = tempfile::tempdir().unwrap();
        let dst_path = dst_dir.path().join("meta.json").to_string_lossy().to_string();
        cmd_restore(&dst_path, file.path(), false, false, false).await.unwrap();

        let dst_store = FileMetaStore::new(&dst_path).await.unwrap();
        let restored = dst_store.get_prefix("").await.unwrap();
        assert_eq!(restored.len(), 3, "every source entry must be restored");
        assert_eq!(restored.get("auth/keys/alice").unwrap().value, "{\"role\":\"Read\"}");
    }

    #[tokio::test]
    async fn restore_purge_with_yes_drops_keys_not_in_snapshot() {
        let target_dir = tempfile::tempdir().unwrap();
        let target_path = target_dir.path().join("meta.json").to_string_lossy().to_string();
        {
            let s = FileMetaStore::new(&target_path).await.unwrap();
            s.put("stale", "old", 0).await.unwrap();
        }

        // Empty snapshot → every preexisting key must vanish.
        let empty = Snapshot { snapshot_version: 1, entries: Default::default() };
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_string(&empty).unwrap()).unwrap();

        // --yes bypasses the TTY prompt, which is what CI / tests need.
        cmd_restore(&target_path, file.path(), /*purge*/ true, false, /*yes*/ true)
            .await.unwrap();

        let s2 = FileMetaStore::new(&target_path).await.unwrap();
        assert!(s2.get("stale").await.unwrap().is_none(),
                "purge must delete keys absent from the snapshot");
    }

    #[tokio::test]
    async fn restore_purge_non_tty_without_yes_refuses() {
        // F6: --purge without --yes in a non-interactive context must
        // refuse to proceed. Cargo tests run detached from a TTY, so
        // this is exactly the "CI bot with the wrong --meta" scenario.
        let target_dir = tempfile::tempdir().unwrap();
        let target_path = target_dir.path().join("meta.json").to_string_lossy().to_string();
        {
            let s = FileMetaStore::new(&target_path).await.unwrap();
            s.put("stale", "old", 0).await.unwrap();
        }
        let empty = Snapshot { snapshot_version: 1, entries: Default::default() };
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_string(&empty).unwrap()).unwrap();

        let err = cmd_restore(&target_path, file.path(),
                              /*purge*/ true, /*dry_run*/ false, /*yes*/ false)
            .await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("--yes") || msg.contains("TTY"),
                "error should name the safety gate, got: {msg}");

        // Crucially, the target must not have been mutated.
        let s2 = FileMetaStore::new(&target_path).await.unwrap();
        assert!(s2.get("stale").await.unwrap().is_some(),
                "target must be untouched when refusing to proceed");
    }

    #[tokio::test]
    async fn restore_dry_run_is_readonly() {
        // F6: --dry-run prints the plan and does not mutate the target.
        let target_dir = tempfile::tempdir().unwrap();
        let target_path = target_dir.path().join("meta.json").to_string_lossy().to_string();
        {
            let s = FileMetaStore::new(&target_path).await.unwrap();
            s.put("only_existing", "v1", 0).await.unwrap();
        }
        // Snapshot adds one new key; dry-run should print create but
        // leave the target with exactly the original set.
        let mut entries = std::collections::BTreeMap::new();
        entries.insert("only_existing".into(),
                       SnapshotEntry { value: "v1".into(), version: 1 });
        entries.insert("new_key".into(),
                       SnapshotEntry { value: "v2".into(), version: 0 });
        let snap = Snapshot { snapshot_version: 1, entries };
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_string(&snap).unwrap()).unwrap();

        cmd_restore(&target_path, file.path(),
                    /*purge*/ false, /*dry_run*/ true, /*yes*/ false)
            .await.unwrap();

        let s2 = FileMetaStore::new(&target_path).await.unwrap();
        assert!(s2.get("new_key").await.unwrap().is_none(),
                "dry-run must not create new_key");
        assert!(s2.get("only_existing").await.unwrap().is_some());
    }

    // ── F5: S3MetaStore branch coverage ──
    //
    // The `open_store` helper routes `s3://` / `gs://` / `az://` / `file://`
    // / `memory://` URIs through `S3MetaStore`. Before F5, only the
    // `FileMetaStore` branch was exercised by tests; a regression in
    // the S3 path (URI parser, object_store adapter) would slip through
    // until an operator tried it against a real bucket. This test drives
    // `open_store` through `file://` so the `S3MetaStore` code path runs
    // end-to-end — the same pattern `crates/meta/src/store.rs` uses for
    // its own S3MetaStore tests.

    #[tokio::test]
    async fn open_store_via_s3_branch_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/meta.json", dir.path().display());

        // Seed via the S3 branch directly.
        {
            let store = open_store(&uri).await.unwrap();
            store.put("auth/keys/alice", "{\"role\":\"Read\"}", 0).await.unwrap();
            store.put("tables/t1", "{\"s0\":[\"w0\"]}", 0).await.unwrap();
            store.put("shards/t1_s0", "s3://bucket/t1_s0.lance", 0).await.unwrap();
        }

        // Dump through cmd_dump.
        let out_path = dir.path().join("snap.json");
        cmd_dump(&uri, Some(out_path.clone())).await.unwrap();
        let raw = std::fs::read_to_string(&out_path).unwrap();
        let snap: Snapshot = serde_json::from_str(&raw).unwrap();
        assert_eq!(snap.entries.len(), 3, "dump must see all 3 keys");

        // Restore into a fresh S3-branch target; verify every key and value.
        let dst_uri = format!("file://{}/dst.json", dir.path().display());
        cmd_restore(&dst_uri, &out_path,
                    /*purge*/ false, /*dry_run*/ false, /*yes*/ false)
            .await.unwrap();

        let dst = open_store(&dst_uri).await.unwrap();
        let got = dst.get_prefix("").await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got.get("auth/keys/alice").unwrap().value, "{\"role\":\"Read\"}");
        assert_eq!(got.get("shards/t1_s0").unwrap().value, "s3://bucket/t1_s0.lance");
    }

    #[test]
    fn snapshot_version_mismatch_rejected() {
        let bad = Snapshot { snapshot_version: 99, entries: Default::default() };
        let raw = serde_json::to_string(&bad).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let dst = dir.path().join("meta.json").to_string_lossy().to_string();
            let file = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(file.path(), raw).unwrap();
            let err = cmd_restore(&dst, file.path(), false, false, false).await.unwrap_err();
            assert!(err.to_string().contains("snapshot version"));
        });
    }
}
