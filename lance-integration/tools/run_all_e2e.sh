#!/bin/bash
# Run all LanceForge E2E regression suites (Phase 11-16) + smoke benchmark.
# Exits non-zero on any failure; prints a summary at the end.
#
# Each phase test owns its own port range (53900-53950, 54000-54050,
# etc.) and does port-specific cleanup on entry, so this wrapper does
# not need to kill cross-run stragglers. Earlier versions ran
# `pkill -9 -f 'lance-coordinator|lance-worker'` between phases; that
# matched every coordinator/worker on the machine, including soaks
# and chaos runs deliberately kept alive on other ports. The
# 2026-04-20 beta-3 soak was killed at 38 min by this exact bug —
# don't reintroduce it.
set -u

cd "$(dirname "$0")"
tests=(e2e_phase11_test.py e2e_phase12_test.py e2e_phase13_test.py
       e2e_phase14_test.py e2e_phase15_test.py e2e_phase16_test.py
       # beta.3 additions — each owns a dedicated port range:
       e2e_beta2_ns_audit_test.py
       e2e_h25_coord_uptime_test.py
       # beta.4 additions (C-series + D-series):
       e2e_beta3_rest_trace_test.py
       e2e_audit_dropped_test.py)

pass=0
fail=0
failed_suites=()
start_total=$(date +%s)

for t in "${tests[@]}"; do
    echo -e "\n=== $t ==="
    sleep 1
    if python3 "$t"; then
        pass=$((pass + 1))
    else
        fail=$((fail + 1))
        failed_suites+=("$t")
    fi
done

# Smoke bench
echo -e "\n=== smoke benchmark ==="
sleep 1
if python3 ../bench/bench_phase17_matrix.py --smoke; then
    echo "Smoke bench: PASS"
else
    echo "Smoke bench: FAIL"
    failed_suites+=("bench_smoke")
    fail=$((fail + 1))
fi

elapsed=$(($(date +%s) - start_total))
echo -e "\n================================================"
echo "  E2E TOTAL: $pass passed, $fail failed  (${elapsed}s)"
if [ ${#failed_suites[@]} -gt 0 ]; then
    echo "  Failed: ${failed_suites[*]}"
fi
echo "================================================"
exit $fail
