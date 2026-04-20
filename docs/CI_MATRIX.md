# CI tier matrix

Four layers, each owning a different cost/coverage point. Read this
before moving a check between layers — every PR currently hits
~4 min of CI, and that budget only holds if slow suites stay
nightly.

| Tier | Trigger | Wall budget | What runs | Workflow file |
|---|---|---|---|---|
| **PR / push** | `push`, `pull_request` on `main` / `lanceforge-clean` | ≤ 5 min | crate unit tests, admin tests, clippy, fmt, SDK unit | `ci.yml` |
| **Nightly** | cron `17 4 * * *` (proptest) + `47 3 * * *` (soak+chaos) | ≤ 30 min (proptest) / 45 min (chaos) / 330 min (soak) | 10k-case proptest, security audit, **20 min soak**, **chaos 5+5 iters**, **H25 uptime regression** | `nightly.yml` + `nightly-soak-chaos.yml` |
| **Release branch** | manual dispatch | no bound | 10+10 chaos, 60-min soak, full E2E matrix | same workflows, `workflow_dispatch` inputs |
| **Sanitizers** | manual dispatch | ~ 1 h | ASan / LeakSan / UBSan builds | `sanitizers.yml` |

## What changed in 0.2.0-beta.3

`nightly-soak-chaos.yml` added — previously `soak/run.py` and
`chaos/runner.py` were committed scripts with no CI coverage. Three
jobs:

- **soak** (default 20 min, dispatch input for 4 h characterisation):
  runs `soak/run.py --read-only`. Fails the job on drift > 3 %.
  Artifacts uploaded for 30 days.
- **chaos** (default 5 iters/scenario, dispatch input for 10): runs
  both `worker_kill` and `worker_stall`. Fails on any iteration
  regression.
- **h25-uptime-regression**: runs `e2e_h25_coord_uptime_test.py`.
  80 s wall-clock; catches the H21-class shutdown-timeout regression
  in isolation so its red check is specific.

## Why these three specifically

- **H25 uptime**: the bug shipped in 0.2.0-beta.1 because nobody ran
  a soak between H21 landing and the tag. Never again without a
  dedicated cheap regression check.
- **20-min soak**: shorter than the release-gate 60 min, long enough
  to catch linear leaks > ~15 MB/20min and force-exits at fixed
  intervals. Characterisation (step cadence, warmup-vs-leak) is a
  4 h manual dispatch because it blows the free-tier runner budget
  if it ran every night.
- **Chaos 5+5**: 10 iters is the release gate; 5 iters at nightly
  catches any new failure mode within one night of landing while
  keeping per-run wall-clock under 10 min. A run that's flaky at
  5 iters isn't suddenly stable at 10.

## Concurrency / port allocation

Each soak / chaos / E2E suite owns a port range so runs in the same
CI job (or the same machine during local development) don't collide:

- soak: 57900 / 57901 / 57950
- chaos: 58900 / 58901 / 58950
- e2e_phase11: 53900 / 53901 / 53950
- … (per-test, distinct)
- e2e_beta2_ns_audit: 55900 / 55901 / 55950

`run_all_e2e.sh` used to do an unqualified `pkill -9 -f
'lance-coordinator|lance-worker'` between phases, which killed every
coord/worker on the machine including unrelated soak/chaos runs.
Fixed in 0.2.0-beta.3 (commit `fbd0ef9`) — each phase now relies on
its own port-specific cleanup. Don't reintroduce the broadband pkill.

## Artifact retention

- Soak: 30 days (enough to diff across a release window).
- Chaos: 30 days.
- Proptest corpus: not retained (shrinkage tests regenerate from
  seed).
- Coverage: `coverage/` retained 14 days by the default CI job.

## What's still not covered (and why)

- **OBS audit integration**: needs a MinIO container in CI. The
  OBS branch of `AuditSink` is unit-tested through the path parser
  only; real PUT-per-batch e2e against MinIO is a 0.3 follow-up
  (would add ~2 min to nightly + MinIO service setup).
- **Mixed RW soak**: `soak/run.py` has a `--read-only` default; the
  mixed workload path exists but CI runs read-only because the
  write path's RSS signal is dominated by legitimate data growth,
  not leaks. Nightly read-only is the correct leak signal.
- **Multi-coordinator HA test**: runs in `e2e_ha_test.py` on manual
  dispatch. Adding to nightly would double the CI cost; gate it on
  commits touching `crates/coordinator/src/ha.rs` instead (0.3
  `paths:` filter work).
