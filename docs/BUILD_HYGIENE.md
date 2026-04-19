# Build artefact hygiene

LanceForge is a multi-crate workspace with ~30+ integration test binaries.
Under the default dev profile each test binary is ~1.5 GB (full debuginfo +
static linking). Cargo never garbage-collects `target/`, so every rebuild
from a different commit or feature set adds another hashed copy to
`target/debug/deps/`. On an active dev cycle this reaches 400–500 GB in a
few weeks.

The workspace is already tuned so that routine work does not produce those
1.5 GB binaries. This doc lists what the tuning does and when to reach for
manual cleanup.

## What's configured

**`Cargo.toml` → `[profile.dev]`**

- `debug = "line-tables-only"` — keeps file:line in panic + debugger stack
  traces (so `RUST_BACKTRACE=1` still points at the right line), drops
  local-variable debuginfo. Integration-test binaries drop from ~1.5 GB to
  ~300 MB.
- `split-debuginfo = "unpacked"` — writes debug sections to `.dwo` sidecar
  files instead of linking them into every binary. Link time drops ~30% on
  Linux; incremental rebuilds are noticeably snappier.

**`Cargo.toml` → `[profile.ci]`** (pre-existing)

- `strip = "debuginfo"` — strips all debug info from dependencies and the
  final binary. Use this profile for routine `cargo test --lib` sweeps.

**`.cargo/config.toml` → `[alias]`**

- `cargo small` → `cargo test --profile ci --lib` — the fast routine sweep.
  Integration-test binaries stay under 200 MB.
- `cargo prune` → `cargo clean --profile dev` — drop only dev artefacts
  when the target dir gets too big. Release builds survive.

## Quick reference

| Command | Profile | Use when |
|---|---|---|
| `cargo small` | ci | Routine unit + lib tests, PR smoke |
| `cargo test --lib` | dev | Need a debugger on a lib test |
| `cargo test --test <name>` | dev | Need a debugger on an integration test |
| `cargo build --release` | release | Perf benchmarks, release artefacts |
| `cargo build --profile release-nonlto` | release-nonlto | Local release with faster link |
| `cargo llvm-cov ...` | coverage | Weekly coverage check (produces 15 GB `llvm-cov-target/`) |

## When `target/` is still too big

Run the manual cleanup tiers from least to most aggressive:

```bash
# Tier 1: drop only stale incremental cache (~40 GB typical).
rm -rf target/debug/incremental

# Tier 2: drop all dev artefacts but keep release/.
cargo prune                        # alias for `cargo clean --profile dev`

# Tier 3: drop coverage + cross-compile targets too.
rm -rf target/llvm-cov-target target/x86_64-unknown-linux-gnu

# Tier 4: nuclear. Next `cargo build` will fetch + compile everything.
cargo clean
```

After any Tier 2+ the next cold build is 10–15 min on 16 cores; subsequent
incremental builds are fast again.

## Recommended discipline

- Run `du -sh target/` at the end of each `G*` / `H*` milestone; if it
  crosses ~60 GB, run Tier 1.
- At release prep (phase 4 in RELEASE_PLAN) always Tier 2 then run a cold
  `cargo build --release` — this is what actually ships.
- If CI caches `target/`, scope the cache key to profile + feature set so
  CI never accumulates cross-profile garbage.

## Optional: install a faster linker

`mold` and `lld` cut link time by another 40–60%. Not installed by default
(needs sudo). If you're doing heavy iteration:

```bash
sudo apt install mold                                # or: sudo apt install lld
# then add to .cargo/config.toml under [target.x86_64-unknown-linux-gnu]:
#   linker = "clang"
#   rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

The default clang + ld.bfd path works fine; use a faster linker only if
per-incremental-build link time is > 10 s.
