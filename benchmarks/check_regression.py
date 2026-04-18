#!/usr/bin/env python3
"""
H22 PR-gate helper: compare the most recent smoke-bench output to a
committed baseline, report per-config regressions above a threshold.

Baseline format (`benchmarks/baseline.json`):

    {
      "meta": {"generated_at": "2026-04-18", "note": "..."},
      "configs": [
        {"scale": 10000, "dim": 128, "shards": 1, "k": 10, "conc": 1,
         "min_qps": 400, "max_p99_ms": 15},
        ...
      ]
    }

Current input: the latest `matrix_*.json` under the --current dir.

Exits 0 if all configs pass, 1 if any config regresses and --warn-only
is not set (when --warn-only is set, always exits 0 and logs violations
so CI can surface them as ::warning:: annotations without blocking).
"""
import argparse, json, os, sys, glob


def latest_matrix(dirname):
    candidates = sorted(glob.glob(os.path.join(dirname, "matrix_*.json")))
    return candidates[-1] if candidates else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--current", required=True,
                    help="Directory containing matrix_*.json files")
    ap.add_argument("--threshold-pct", type=float, default=15.0,
                    help="Per-config regression threshold (%%). A config fails "
                         "if current_qps < baseline_min_qps * (1 - t/100) OR "
                         "current_p99 > baseline_max_p99 * (1 + t/100).")
    ap.add_argument("--warn-only", action="store_true")
    args = ap.parse_args()

    with open(args.baseline) as f:
        baseline = json.load(f)

    current_path = latest_matrix(args.current)
    if current_path is None:
        print(f"check_regression: no matrix_*.json in {args.current}", file=sys.stderr)
        return 1 if not args.warn_only else 0
    with open(current_path) as f:
        current = json.load(f)

    current_map = {}
    for r in current.get("results", []):
        key = (r["scale"], r["dim"], r["shards"], r["k"], r.get("concurrency", r.get("conc")))
        current_map[key] = r

    factor_down = 1 - args.threshold_pct / 100
    factor_up = 1 + args.threshold_pct / 100

    violations = []
    for cfg in baseline.get("configs", []):
        key = (cfg["scale"], cfg["dim"], cfg["shards"], cfg["k"], cfg["conc"])
        hit = current_map.get(key)
        if hit is None:
            print(f"::notice::baseline config {key} not present in current run (skipped)")
            continue
        cur_qps = hit.get("qps", 0)
        cur_p99 = hit.get("p99_ms", 1e9)
        min_qps = cfg.get("min_qps", 0)
        max_p99 = cfg.get("max_p99_ms", 1e9)
        if cur_qps < min_qps * factor_down:
            violations.append(
                f"{key}: QPS {cur_qps:.1f} < baseline {min_qps} * {factor_down:.2f} "
                f"(regression > {args.threshold_pct:.0f}%)"
            )
        if cur_p99 > max_p99 * factor_up:
            violations.append(
                f"{key}: P99 {cur_p99:.1f}ms > baseline {max_p99} * {factor_up:.2f} "
                f"(regression > {args.threshold_pct:.0f}%)"
            )

    if not violations:
        print(f"✓ smoke bench within ±{args.threshold_pct:.0f}% of baseline "
              f"({len(current_map)} configs compared)")
        return 0

    for v in violations:
        print(f"  regression: {v}")
    if args.warn_only:
        print(f"(warn-only: {len(violations)} regression(s) detected, not failing CI)")
        return 0
    print(f"FAIL: {len(violations)} config(s) regressed > {args.threshold_pct}%", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
