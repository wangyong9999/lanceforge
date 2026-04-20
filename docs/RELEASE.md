# Release procedure

Every LanceForge release tag (`v0.2.0-beta.N`, `v0.2.0-rc.N`, `v0.2.0`)
must pass every gate in this document before push. Informal "I ran
tests locally" does **not** qualify — the artefacts referenced in each
gate must exist on disk under the commit being tagged.

Historical record: beta.1 shipped H25 (coord force-exit at 65 s)
because a 15-min soak was run against a pre-H21 binary and no one
re-ran after H21 landed. Each gate below exists to prevent one
previously-observed class of near-miss.

## Gate A — workspace green

```bash
cargo test --profile ci --lib -p lance-distributed-common \
                               -p lance-distributed-coordinator \
                               -p lance-distributed-meta \
                               -p lance-distributed-worker
cargo test --profile ci -p lance-admin
```

Requires: zero failures, all four crates + admin pass. Count must be
≥ the previous release tag's count (ratchet, never regress).

## Gate B — SDK green

```bash
cd lance-integration/sdk/python
python3 -m unittest discover tests
```

Requires: all sync + async client tests pass.

## Gate C — E2E suite

```bash
bash lance-integration/tools/run_all_e2e.sh
```

Requires: "E2E TOTAL: N passed, 0 failed" where N matches the
manifest in `run_all_e2e.sh`. Each new e2e test added to the
manifest increments N.

## Gate D — Chaos 10+10

```bash
python3 chaos/runner.py chaos/scenarios/worker_kill.yaml  --iters 10
python3 chaos/runner.py chaos/scenarios/worker_stall.yaml --iters 10
```

Requires: `10/10 passed` for both scenarios. `success_rate` may dip
to 99-99.5% in stall (expected — reader observes transient errors
during the stall window before health-check kicks the unhealthy
worker out of routing).

## Gate E — 4h characterisation soak

```bash
gh workflow run nightly-soak-chaos.yml \
   -f soak_minutes=240 -f chaos_iters=10
```

(Or locally: `python3 soak/run.py --minutes 240 --read-only`.)

Requires: the artefact uploaded by the CI workflow, `soak-<run-id>/`,
must contain a `soak_240min_*.json`. Inspect with:

- Linear RSS growth across the 4h window → investigate, do not tag
- Step-form growth (periods of ≥ 30 min flat punctuated by single
  jumps of ≤ 20 MB/worker) → acceptable; warmup behaviour,
  documented in LIMITATIONS §13
- Any `coord_rss=None` sample mid-run → coord crashed, do not tag

Why 4h: two 60-min soaks (beta.1 and beta.3) both saw one step event
near the end of the window. A 4h run tells us step cadence (1/hour
or less → fine; 1/10 min → regressed). Without this data no release
should claim "RSS stable under read load."

## Gate F — clippy + cargo build --release

```bash
cargo clippy --workspace --bins --tests
cargo build --release --bin lance-coordinator \
                      --bin lance-worker \
                      --bin lance-admin
```

Requires: zero new errors. Existing warnings are OK but check the
diff vs prior tag — any *new* warning introduced by the release
commits must be triaged.

## Gate G — docs honest

Walk through in order:

- `RELEASE_NOTES.md`: top entry matches the version being tagged.
  "What was broken in the previous release" section present if any
  fix-forward happened. "Deferred to next" section honest about
  what did not ship.
- `CHANGELOG.md`: Added / Changed / Fixed / Deferred sections match
  RELEASE_NOTES.
- `LIMITATIONS.md`: every known gap is dated or version-tagged; no
  entry still referring to an alpha-era number.
- `COMPAT_POLICY.md`: upgrade path from previous tag is spelled
  out, including any env var / config field additions or renames.
- `README.md`: feature matrix includes any feature this release
  promotes from "preview" to "stable"; test-count badge is
  current.

## Gate H — artefacts committed

For soak, chaos, and any bench that backs a release-note claim:

- Commit the JSON results under `soak/results/` / `chaos/results/`
  / `lance-integration/bench/results/phase17/` (the last one is
  gitignored — force-add with `git add -f` for release-gating
  bench artefacts; route others through normal add).
- Reference the exact filename in the relevant RELEASE_NOTES or
  LIMITATIONS section so readers can verify.

## Gate I — tag

Once A-H are green:

```bash
git tag -a v0.2.0-beta.N -m "LanceForge 0.2.0-beta.N

<brief summary of the fixes / additions from RELEASE_NOTES>"
git push lanceforge lanceforge-clean
git push lanceforge v0.2.0-beta.N
git ls-remote lanceforge refs/tags/v0.2.0-beta.N
```

Verify the tag resolves on the remote before announcing.

## Gate J — docker image (post-tag)

```bash
docker build -t lanceforge/lanceforge:v0.2.0-beta.N .
docker push  lanceforge/lanceforge:v0.2.0-beta.N
docker push  lanceforge/lanceforge:latest
```

Currently manual. `release-docker.yml` workflow exists but is not
auto-triggered on tag; wire into 0.3.

## Definition-of-Done gate matrix

| Gate | Tag without? | Revoke tag if missed | Cost to re-run |
|---|---|---|---|
| A lib tests | no | yes | ≤ 1 min |
| B SDK tests | no | yes | ≤ 1 min |
| C E2E suite | no | yes | ~3 min |
| D Chaos 10+10 | no | yes | ~8 min |
| **E 4h soak** | **no** | **yes** | **~4 h** |
| F clippy + release | no | yes | ~2 min |
| G docs honest | no | yes | reviewer time |
| H artefacts | no | fix-forward | minutes |
| I tag push | (this gate) | — | seconds |
| J docker push | **yes** (manual after) | fix-forward | ~5 min |

**Gate E is the expensive one.** It's also the only one that catches
linear-leak and cache-warmup-cadence regressions. Schedule 4h soak
at the start of release-day; other gates run in parallel once A-D
have passed the short smoke pass.

## Post-mortem discipline

If a bug ships despite passing every gate:

1. Write the bug up in the NEXT release's RELEASE_NOTES "what was
   broken" top section. Name the commit SHA that introduced it.
2. Add a regression test that would have caught it. File the ID
   under the `F` / `B` / `C` hardening series so future check-lists
   trace back here.
3. If the gate itself was the miss (ran but didn't assert the
   right thing), add the assertion. If the gate was skipped, add
   a CI dependency that forces it for the relevant commits.

Every beta.1 → beta.2 → beta.3 transition added gates this way. beta.4
and beyond should shrink the list only when a gate is subsumed by a
stronger automated check, never when "it takes too long."
