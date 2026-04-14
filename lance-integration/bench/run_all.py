#!/usr/bin/env python3
"""
LanceForge Benchmark Regression Suite

Unified runner for all core benchmarks. Produces a structured JSON report
and exit code 0/1 for CI integration.

Usage:
    python run_all.py                  # Run all benchmarks
    python run_all.py --quick          # Quick subset (recall + filtered + enterprise E2E)
    python run_all.py --ci             # CI mode (quick + strict thresholds)

Benchmarks:
  1. recall_benchmark    — recall@{1,10,100} with IVF_FLAT (200K×128d)
  2. bench_filtered      — filtered ANN recall + QPS (200K×128d, 10% selectivity)
  3. bench_cosine        — L2 + Cosine distance validation (100K×128d)
  4. bench_highdim       — 768d + 1536d recall (100K/50K)
  5. bench_scale         — 1M scale (5 shards, QPS + memory)
  6. bench_standard      — SIFT/GloVe recall-QPS tradeoff curve (500K)
  7. e2e_full_system     — MinIO full-stack (10 tests, requires MinIO)
  8. e2e_enterprise      — SDK/REST/write/concurrent (12 tests)
  9. e2e_write           — Insert/delete consistency (6 tests)

Thresholds:
  recall@10 >= 0.95 (all scenarios)
  filtered recall@10 >= 0.95
  unfiltered QPS >= 50
  E2E pass rate = 100%
"""

import sys, os, time, subprocess, json, argparse

BENCH_DIR = os.path.dirname(__file__)
TOOLS_DIR = os.path.join(BENCH_DIR, '..', 'tools')
SDK_DIR = os.path.join(BENCH_DIR, '..', 'sdk', 'python')
RESULTS_DIR = os.path.join(BENCH_DIR, 'results')

ENV = {
    **os.environ,
    'PYTHONPATH': f"{TOOLS_DIR}:{SDK_DIR}:{os.environ.get('PYTHONPATH', '')}",
    'PYTHONUNBUFFERED': '1',
}

def run_bench(name, script_path, timeout=600):
    """Run a benchmark script and capture result."""
    print(f"\n{'='*60}")
    print(f"  Running: {name}")
    print(f"{'='*60}")

    t0 = time.time()
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            env=ENV,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=os.path.dirname(script_path),
        )
        elapsed = time.time() - t0
        passed = result.returncode == 0

        # Print output summary (last 15 lines)
        lines = result.stdout.strip().split('\n')
        for line in lines[-15:]:
            print(f"  {line}")

        if result.returncode != 0 and result.stderr:
            for line in result.stderr.strip().split('\n')[-5:]:
                print(f"  STDERR: {line}")

        return {
            'name': name,
            'passed': passed,
            'exit_code': result.returncode,
            'elapsed_s': round(elapsed, 1),
        }
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        print(f"  TIMEOUT after {elapsed:.0f}s")
        return {
            'name': name,
            'passed': False,
            'exit_code': -1,
            'elapsed_s': round(elapsed, 1),
        }
    except Exception as e:
        return {
            'name': name,
            'passed': False,
            'exit_code': -1,
            'elapsed_s': 0,
            'error': str(e),
        }

def main():
    parser = argparse.ArgumentParser(description='LanceForge Benchmark Regression')
    parser.add_argument('--quick', action='store_true', help='Quick subset only')
    parser.add_argument('--ci', action='store_true', help='CI mode (quick + strict)')
    args = parser.parse_args()

    # Kill any leftover processes
    os.system("pkill -f 'lance-coordinator|lance-worker' 2>/dev/null")
    time.sleep(1)

    # Define benchmark suites
    quick_suite = [
        ("Full Capability (21 tests)", os.path.join(TOOLS_DIR, "e2e_full_capability_test.py")),
        ("HA (8 tests)", os.path.join(TOOLS_DIR, "e2e_ha_test.py")),
        ("Read Replica (4 tests)", os.path.join(TOOLS_DIR, "e2e_replica_test.py")),
        ("Recall Benchmark (200K×128d)", os.path.join(BENCH_DIR, "recall_benchmark.py")),
        ("Filtered ANN (200K×128d)", os.path.join(BENCH_DIR, "bench_filtered.py")),
    ]

    full_suite = quick_suite + [
        ("Write Path E2E (6 tests)", os.path.join(TOOLS_DIR, "e2e_write_test.py")),
        ("Cosine Distance (100K×128d)", os.path.join(BENCH_DIR, "bench_cosine.py")),
        ("High-Dim (768d+1536d)", os.path.join(BENCH_DIR, "bench_highdim.py")),
    ]

    # Optional heavy benchmarks (skip in quick/CI)
    if not args.quick and not args.ci:
        full_suite += [
            ("1M Scale (5 shards)", os.path.join(BENCH_DIR, "bench_scale.py")),
            ("Standard (SIFT+GloVe 500K)", os.path.join(BENCH_DIR, "bench_standard.py")),
            ("Shard Scaling (1→10)", os.path.join(BENCH_DIR, "bench_shard_scaling.py")),
        ]

    suite = quick_suite if (args.quick or args.ci) else full_suite

    print("=" * 60)
    print("  LANCEFORGE BENCHMARK REGRESSION SUITE")
    print(f"  Mode: {'CI' if args.ci else 'Quick' if args.quick else 'Full'}")
    print(f"  Benchmarks: {len(suite)}")
    print("=" * 60)

    results = []
    total_start = time.time()

    for name, script in suite:
        # Kill leftover processes between benchmarks
        os.system("pkill -f 'lance-coordinator|lance-worker' 2>/dev/null")
        time.sleep(1)

        r = run_bench(name, script, timeout=600)
        results.append(r)

        status = "✓ PASS" if r['passed'] else "✗ FAIL"
        print(f"\n  → {status} ({r['elapsed_s']}s)")

    total_elapsed = time.time() - total_start

    # Summary
    passed = sum(1 for r in results if r['passed'])
    failed = sum(1 for r in results if not r['passed'])

    print(f"\n\n{'='*60}")
    print("  REGRESSION RESULTS")
    print(f"{'='*60}")
    print(f"  Total time: {total_elapsed:.0f}s")
    print(f"  Passed: {passed}/{len(results)}")
    print()
    for r in results:
        s = "✓" if r['passed'] else "✗"
        print(f"  {s} {r['name']:<35} {r['elapsed_s']:>6.1f}s")
    print(f"{'='*60}")

    # Save report
    os.makedirs(RESULTS_DIR, exist_ok=True)
    report = {
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'mode': 'ci' if args.ci else 'quick' if args.quick else 'full',
        'total_elapsed_s': round(total_elapsed, 1),
        'passed': passed,
        'failed': failed,
        'results': results,
    }
    report_path = os.path.join(RESULTS_DIR, 'regression_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report: {report_path}")

    sys.exit(0 if failed == 0 else 1)

if __name__ == '__main__':
    main()
