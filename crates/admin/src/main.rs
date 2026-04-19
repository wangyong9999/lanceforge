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
    if uri.starts_with("s3://") || uri.starts_with("gs://") || uri.starts_with("az://") {
        // S3MetaStore::new takes the full URI + an iterable of
        // object_store options; it parses the scheme itself. Empty
        // options rely on standard env credentials, which is enough
        // for operator tooling — full runtime uses config.yaml
        // storage_options.
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

async fn cmd_restore(meta: &str, from: &std::path::Path, purge: bool) -> anyhow::Result<()> {
    let store = open_store(meta).await?;
    let raw = tokio::fs::read_to_string(from).await?;
    let snap: Snapshot = serde_json::from_str(&raw)?;
    if snap.snapshot_version != 1 {
        anyhow::bail!(
            "snapshot version {} not supported by this lance-admin build",
            snap.snapshot_version
        );
    }
    if purge {
        let existing = store.list_keys("").await?;
        let kept: std::collections::HashSet<&String> = snap.entries.keys().collect();
        for k in existing {
            if !kept.contains(&k) {
                store.delete(&k).await?;
                log::info!("purge: deleted {k}");
            }
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
            MetaCmd::Restore { meta, from, purge } => cmd_restore(&meta, &from, purge).await,
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
        cmd_restore(&dst_path, file.path(), false).await.unwrap();

        let dst_store = FileMetaStore::new(&dst_path).await.unwrap();
        let restored = dst_store.get_prefix("").await.unwrap();
        assert_eq!(restored.len(), 3, "every source entry must be restored");
        assert_eq!(restored.get("auth/keys/alice").unwrap().value, "{\"role\":\"Read\"}");
    }

    #[tokio::test]
    async fn restore_purge_drops_keys_not_in_snapshot() {
        let target_dir = tempfile::tempdir().unwrap();
        let target_path = target_dir.path().join("meta.json").to_string_lossy().to_string();
        {
            let s = FileMetaStore::new(&target_path).await.unwrap();
            s.put("stale", "old", 0).await.unwrap();
        }

        // Empty snapshot on disk → every preexisting key must vanish.
        let empty = Snapshot { snapshot_version: 1, entries: Default::default() };
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_string(&empty).unwrap()).unwrap();

        cmd_restore(&target_path, file.path(), /*purge*/ true).await.unwrap();

        let s2 = FileMetaStore::new(&target_path).await.unwrap();
        assert!(s2.get("stale").await.unwrap().is_none(),
                "purge must delete keys absent from the snapshot");
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
            let err = cmd_restore(&dst, file.path(), false).await.unwrap_err();
            assert!(err.to_string().contains("snapshot version"));
        });
    }
}
