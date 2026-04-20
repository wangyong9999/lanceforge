#!/usr/bin/env python3
"""
H25 regression: coord must not die at ~65s of uptime.

Background: H21 (fa13726) introduced `tokio::time::timeout(max_shutdown,
serve_fut)` which budget-bound the *entire* server lifetime, not the
drain window. Every coord pod was force-exiting at `2 * query_timeout
+ 5s` (= 65s with default config), regardless of workload. The G4
soak caught it mid-beta and H25 (37fa7ba) fixed it by moving the
seatbelt into a separate task that only starts counting after
`bg_shutdown.notified()`.

This test locks the fix in: boot a coord, wait 75s, hit `/healthz`,
verify the process is still alive. If someone re-wraps `serve_fut` in
an unconditional timeout, this test fails in CI before the regression
hits a customer.

Wall-clock is 80s — not cheap, so this test runs in the nightly tier,
not on every PR.
"""
import sys, os, time, subprocess, shutil, signal, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_helpers import wait_for_grpc

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BIN = os.environ.get("LANCEFORGE_BIN", os.path.join(ROOT, "target", "release"))
BASE = "/tmp/lanceforge_h25_test"
COORD_PORT = 54950
REST_PORT = COORD_PORT + 1
WORKER_PORTS = [54900, 54901]

# How long we wait before asserting survival. Must be strictly greater
# than `2 * query_timeout + 5s` with the default `query_timeout_secs=30`
# (= 65s) so we observe the window H21 mis-bounded.
UPTIME_PROBE_SECONDS = 75


def kill_cluster():
    os.system(
        f"pkill -9 -f 'lance-coordinator.*{COORD_PORT}|"
        f"lance-worker.*{WORKER_PORTS[0]}|lance-worker.*{WORKER_PORTS[1]}' 2>/dev/null"
    )
    time.sleep(1)


def start_cluster():
    kill_cluster()
    if os.path.exists(BASE):
        shutil.rmtree(BASE)
    os.makedirs(BASE)
    cfg = {
        'tables': [],
        'executors': [{'id': f'w{i}', 'host': '127.0.0.1', 'port': WORKER_PORTS[i]}
                      for i in range(2)],
        'default_table_path': BASE,
        'server': {'slow_query_ms': 0},
    }
    with open(f"{BASE}/config.yaml", 'w') as f:
        yaml.dump(cfg, f)

    procs = {}
    for i in range(2):
        p = subprocess.Popen(
            [f"{BIN}/lance-worker", f"{BASE}/config.yaml", f"w{i}", str(WORKER_PORTS[i])],
            stdout=open(f"{BASE}/w{i}.log", "w"), stderr=subprocess.STDOUT
        )
        procs[f"w{i}"] = p
    for i in range(2):
        if not wait_for_grpc("127.0.0.1", WORKER_PORTS[i]):
            raise RuntimeError(f"worker w{i} failed to come up")

    p = subprocess.Popen(
        [f"{BIN}/lance-coordinator", f"{BASE}/config.yaml", str(COORD_PORT)],
        stdout=open(f"{BASE}/coord.log", "w"), stderr=subprocess.STDOUT
    )
    procs["coord"] = p
    if not wait_for_grpc("127.0.0.1", COORD_PORT):
        raise RuntimeError("coord failed to come up")
    return procs


def healthz_ok() -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{REST_PORT}/healthz", timeout=2.0) as r:
            return r.status == 200
    except Exception:
        return False


def main():
    # Clear any proxy env var that would interfere with localhost RPC
    # (see H24 / RELEASE_NOTES 0.2.0-beta.1 notes).
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                "GRPC_PROXY", "grpc_proxy"):
        os.environ.pop(var, None)

    print(f"=== H25 regression: coord must survive {UPTIME_PROBE_SECONDS}s idle ===",
          flush=True)
    procs = start_cluster()
    try:
        t0 = time.time()
        # Poll `/healthz` while we sleep. If coord dies at the 65s mark
        # (the old H21 bug) `/healthz` goes unreachable before we check
        # at the end; we detect that early and fail fast.
        while time.time() - t0 < UPTIME_PROBE_SECONDS:
            time.sleep(2)
            elapsed = int(time.time() - t0)
            if elapsed > 0 and elapsed % 10 == 0:
                if not healthz_ok():
                    raise AssertionError(
                        f"coord died at t={elapsed}s — H21 shutdown-timeout bug regressed. "
                        f"Check `tokio::time::timeout(max_shutdown, serve_fut)` in main.rs."
                    )

        # Final assertion after the full window.
        assert healthz_ok(), f"coord /healthz down after {UPTIME_PROBE_SECONDS}s — regression"
        assert procs["coord"].poll() is None, (
            f"coord process exited before {UPTIME_PROBE_SECONDS}s — "
            f"H21 shutdown-timeout regression or other crash"
        )
        print(f"PASS: coord still healthy after {UPTIME_PROBE_SECONDS}s", flush=True)
    finally:
        # Graceful shutdown — this exercises the seatbelt path on the way
        # out too (coord should receive SIGTERM and drain cleanly).
        if procs["coord"].poll() is None:
            procs["coord"].send_signal(signal.SIGTERM)
        for name, p in procs.items():
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
        kill_cluster()


if __name__ == '__main__':
    main()
