# Routing Formula Benchmark harness
# Built 2026-07 from routing-replay evidence work. Add algorithms under test per run as selectors.
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = []
# ///

# ─── How to run ───
# 1. Install uv (if not installed):
#      curl -LsSf https://astral.sh/uv/install.sh | sh
# 2. Run a benchmark:
#      uv run benchmark_quota_cache.py --source capture_copy.sqlite --benchmark-set canonical --out-dir dynamic-quota-cache-v1/canonical
# ──────────────────

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from routing_replay.benchmark_engine import load_rows
from routing_replay.benchmark_output import run_and_write
from routing_replay.benchmark_quota import infer_capacities
from routing_replay.benchmark_report import CANONICAL, STRESS_CANONICAL, write_comparison, write_final_report
from routing_replay.benchmark_selftest import run_self_test
from routing_replay.benchmark_stress import run_stress
from routing_replay.benchmark_stress_output import write_stress_replay
from routing_replay.benchmark_stress_report import write_stress_comparison
from routing_replay.benchmark_stress_types import StressConfig
from routing_replay.benchmark_types import BenchmarkSelector


def main() -> None:
    args = _parse_args()
    out_dir = Path(args.out_dir)
    if args.self_test:
        run_self_test(args.case, Path("dynamic-quota-cache-v1/qa"))
        print("self-test ok")
    elif args.mode == "infer-capacity":
        _capacity(Path(args.source), out_dir)
    elif args.stress_set == "5h-burst":
        _stress(Path(args.source), out_dir, args)
    elif args.benchmark_set == "canonical":
        results = {selector: run_and_write(Path(args.source), out_dir / selector.value, selector) for selector in CANONICAL}
        write_comparison(out_dir, results)
        write_final_report(out_dir.parent, out_dir, results)
        print(f"canonical artifacts={out_dir / 'comparison.md'}")
    else:
        result = run_and_write(Path(args.source), out_dir, BenchmarkSelector(args.selector))
        print(f"replayable_rows={len(result.decisions)} artifacts={out_dir / 'replay.sqlite'}")


def _capacity(source: Path, out_dir: Path) -> None:
    rows, exclusions, total = load_rows(source)
    inferred = infer_capacities(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "capacity_inference.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("upstream_id", "window", "capacity_tokens"))
        writer.writerows((upstream, window, capacity) for (upstream, window), capacity in sorted(inferred.capacities.items()))
    (out_dir / "capacity_inference.txt").write_text(f"source_rows={total}\nparsed_rows={len(rows)}\nparse_exclusions={sum(exclusions.values())}\ncapacity_fallback_count={inferred.fallback_count}\ncapacity_missing_count={inferred.missing_count}\n", encoding="utf-8")
    print(f"capacity_rows={len(inferred.capacities)} fallback_count={inferred.fallback_count}")


def _stress(source: Path, out_dir: Path, args: argparse.Namespace) -> None:
    config = StressConfig(args.burst_hours, args.target_multiplier, args.minimum_repeat_factor, args.quota_capacity_multiplier)
    if args.benchmark_set == "canonical":
        replays = {selector: run_stress(source, selector, config) for selector in STRESS_CANONICAL}
        for selector, replay in replays.items():
            write_stress_replay(out_dir / selector.value, replay)
        write_stress_comparison(out_dir, replays)
        print(f"stress artifacts={out_dir / 'comparison.md'}")
        return
    replay = run_stress(source, BenchmarkSelector(args.selector), config)
    write_stress_replay(out_dir, replay)
    print(f"stress served={len(replay.result.decisions)} blocked={len(replay.result.blocked)} artifacts={out_dir / 'replay.sqlite'}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dynamic quota and cache benchmark simulator")
    parser.add_argument("--source", default="capture_copy.sqlite")
    parser.add_argument("--out-dir", default="dynamic-quota-cache-v1")
    parser.add_argument("--mode", choices=("replay", "infer-capacity"), default="replay")
    parser.add_argument("--selector", choices=tuple(item.value for item in BenchmarkSelector), default=BenchmarkSelector.SMOOTHSTEP_A.value)
    parser.add_argument("--benchmark-set", choices=("single", "canonical"), default="single")
    parser.add_argument("--stress-set", choices=("5h-burst",))
    parser.add_argument("--burst-hours", type=float, default=5.0)
    parser.add_argument("--target-multiplier", type=float, default=4.0)
    parser.add_argument("--minimum-repeat-factor", type=int, default=4)
    parser.add_argument("--quota-capacity-multiplier", type=float, default=1.0)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--case", choices=("all", "parser", "parser-missing-quota", "cache", "cache-hit", "cache-expiry", "capacity", "capacity-fallback", "quota", "quota-debit", "quota-reset", "actual-choice-exclusions", "selectors", "cost-first", "comparison-missing-artifact", "stress", "stress-reset-skew", "stress-exhaustion-exclusion", "stress-all-blocked", "stress-blocked-no-mutation", "stress-earliest-5h-reset", "stress-capacity-multiplier"), default="all")
    return parser.parse_args()


if __name__ == "__main__":
    main()
