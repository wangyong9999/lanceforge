#!/bin/bash
# Run all LanceForge E2E regression suites (Phase 11-16) + smoke benchmark.
# Exits non-zero on any failure; prints a summary at the end.
set -u

cd "$(dirname "$0")"
tests=(e2e_phase11_test.py e2e_phase12_test.py e2e_phase13_test.py
       e2e_phase14_test.py e2e_phase15_test.py e2e_phase16_test.py)

pass=0
fail=0
failed_suites=()
start_total=$(date +%s)

for t in "${tests[@]}"; do
    echo -e "\n=== $t ==="
    pkill -9 -f 'lance-coordinator|lance-worker' 2>/dev/null
    sleep 3
    if python3 "$t"; then
        pass=$((pass + 1))
    else
        fail=$((fail + 1))
        failed_suites+=("$t")
    fi
done

# Smoke bench
echo -e "\n=== smoke benchmark ==="
pkill -9 -f 'lance-coordinator|lance-worker' 2>/dev/null
sleep 3
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
