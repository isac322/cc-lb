---
name: routing-formula-bench
description: Benchmark arbitrary routing selection formulas against captured production traffic with dynamic quota and cache simulation. Use whenever asked for a routing formula benchmark, a cost/throughput test, selector comparison, quota/cache simulation replay, or "현재 코드 vs origin/master 성능"; also use to compare a worktree formula to one or more git refs, evaluate cache-hit or quota-aware routing alternatives, or stress routing under quota exhaustion.
---

# Routing Formula Benchmark

Benchmark the cost and throughput consequences of a routing selection algorithm against a `capture_v1` production trace. This harness simulates both stateful prefix caching and per-upstream quota state as requests replay in timestamp order.

Treat the routing algorithm as an input to this run, not as a built-in answer. The bundled selectors are reference implementations and baselines, never a recommendation to ship any particular formula.

## Start With The Algorithm Under Test

Accept either of these inputs:

1. A human description, for example: "route to maximum cache hit, tie-break by quota urgency."
2. One or two git targets, for example: "benchmark the current worktree's formula vs `origin/master`."

For a natural-language algorithm:

1. Restate the selector's deterministic ordering, eligibility conditions, and tie-breakers.
2. Add a selector for that exact behavior using the extension seam below.
3. Write the selector's red self-test before its implementation, then run the focused test and the full self-test suite.

For git-target comparison:

1. Resolve every requested ref without checking it out, for example `git rev-parse --verify origin/master`.
2. Read `crates/cc-lb-engine/src/builtin_filters/subscription_preference.rs` at each target with `git show REF:crates/cc-lb-engine/src/builtin_filters/subscription_preference.rs`. Read the current worktree file directly when it is one target.
3. Extract each target's candidate eligibility, scoring formula, ordering, and deterministic tie-breakers. Do not infer behavior from commit messages or a diff alone.
4. Implement one selector per target, named for the ref or formula revision, using the extension seam below.
5. Self-test each selector against a case that distinguishes it from the other target. Run the normal canonical comparison and, when throughput under pressure matters, the burst stress comparison.
6. Report the source refs, resolved SHAs, formula interpretation, selector names, commands, results, and trust boundaries. Do not claim a baseline is the correct formula.

## Simulation Model

Replay every usable captured request in order. Maintain cache and quota state independently for every upstream:

- Prefix cache entries are TTL'd from each request's breakpoint: 5 minutes or 1 hour, including recorded lookback prefixes.
- Charge each selected upstream `uncached + create5m + create1h + 0.1 * read` quota tokens for both its 5-hour and 7-day windows.
- Infer each upstream/window capacity from observed utilization deltas in the capture, with per-window median fallback where evidence permits.
- Preserve each upstream's captured reset offset and advance 5-hour and 7-day windows dynamically during replay.

## Capture Handling

Obtain a `capture_v1` SQLite database from cc-lb's feature-gated local formula capture. The proxy capture records per-request routing traces, candidates, quota snapshots, cache breakpoints, and observed usage. Pass it with `--source <path>`.

Captured traffic is sensitive. Never commit it, copy it into this skill, attach it to a report, or place it under a tracked directory. Run the harness from a gitignored evidence workdir such as `.omo/evidence/...`; keep all benchmark artifacts alongside that evidence workdir. The bundled harness contains only code and self-test fixtures.

## Run The Harness

From `harness/`:

```bash
uv run benchmark_quota_cache.py --self-test
uv run benchmark_quota_cache.py --self-test --case selectors
uv run benchmark_quota_cache.py --mode infer-capacity --source X --out-dir Y/capacity
uv run benchmark_quota_cache.py --source X --selector actual-choice --out-dir Y/calibration
uv run benchmark_quota_cache.py --source X --benchmark-set canonical --out-dir Y/canonical
uv run benchmark_quota_cache.py --source X --benchmark-set canonical --stress-set 5h-burst --burst-hours 5 --target-multiplier 4 --quota-capacity-multiplier 1 --out-dir Y/stress
```

Use `actual-choice` calibration before interpreting alternatives. Use `canonical` for normal-load comparisons. Use `5h-burst` to measure whether stateful choices serve requests until all candidates exhaust rather than merely minimizing replay cost.

Normal replay writes, per selector:

- `replay.sqlite`: structured run metadata, decisions, and summary metrics.
- `replay_decisions.csv`: request-level selected upstream, simulated cost, cache hit, quota urgency, and divergence fields.
- `replay_summary.csv`: coverage, cost, cache, quota, balance, and calibration metrics.
- `comparison.csv` and `comparison.md`: canonical-selector comparison.
- `final-report.md`: normal-load summary and simulation metadata.

Stress replay additionally writes, per selector:

- `blocked_requests.csv`: requests blocked only because every candidate is quota-exhausted.
- `quota_exhaustion.csv`: upstream/window capacity, usage, reset, and exhaustion counts.
- `stress-report.md`: 5-hour burst configuration, repeat factor, reset-offset source, and artifact inventory.

## Add A Selector

Extend the harness only for the algorithm being benchmarked. Keep each touched Python file at or below 250 pure lines and use TDD: add a focused self-test first, run it red, implement the smallest selector, then run all self-tests.

1. Add the enum member to `harness/routing_replay/benchmark_types.py::BenchmarkSelector`.
2. Add its deterministic match arm in `harness/routing_replay/benchmark_select.py::select`.
3. Add it to the stress pass-through selector list in `harness/routing_replay/benchmark_stress.py` if it can run under stress. Implement stress-only selection there when its choice depends on stress state, as `earliest-5h-reset` does.
4. Add it to the `CANONICAL` tuple in `harness/routing_replay/benchmark_report.py` when it is part of the requested comparison set.
5. Add a discriminating self-test case in `harness/routing_replay/benchmark_selftest.py` and expose it in the CLI `--case` choices when needed.

Reference selector library: `actual-choice`, `current`, `smoothstep-a`, `lowered-gate`, `max-hit`, `min-cost`, `quota-aware-min-cost`, `cost-first`, and stress-only `earliest-5h-reset`. Use them as baselines and implementation examples, not as the answer to any routing decision.

## Interpret Results Honestly

- Cost totals are micro-USD and cover replayable rows only. Do not extrapolate them to all captured traffic without reporting exclusions.
- Under stress, `served` means an eligible upstream was selected and debited. `blocked` means every candidate was quota-exhausted. `selector_exclusions` preserve the selector's own semantics, such as `actual-choice` naming an exhausted upstream while another candidate is eligible; they are neither served nor blocked.
- Gini measures concentration of selected requests across upstreams. L1-to-urgency-target measures divergence between selected share and dynamic quota-urgency share. Lower is not inherently better; report it as a routing-balance tradeoff.
- Average urgency shows use-it-or-lose-it practice: higher selection urgency means a selector tends to consume quota approaching a reset. It is descriptive, not a throughput or cost result by itself.

Every benchmark report must state all of these trust boundaries:

1. Quota capacities are inferred from the capture database; this is not Anthropic absolute quota parity.
2. Cache-read quota debit uses the `0.1` weighting assumption.
3. Captures containing only 5-minute TTL cache make `max-hit` and `cost-first` converge; they diverge when 1-hour cache pricing enters the trace.
4. A single capture is one workload sample, not a universal traffic distribution.

## Report Template

Use this table and retain the trust-boundary footer in every result report:

| selector | cost (micro-USD) | hit | served | blocked | notes |
|---|---:|---:|---:|---:|---|
| `<selector>` | `<cost>` | `<hit ratio>` | `<served>` | `<blocked>` | `<formula, exclusions, balance tradeoff>` |

Trust boundaries: capacities are DB-inferred rather than Anthropic absolute quota parity; cache reads use a 0.1 debit weight; cache-TTL composition can make selectors converge; this capture is one workload sample.

## Same-Capture Sanity Check

For the original validated capture, normal canonical cost was `100,069,987` micro-USD for `current` and `95,259,796` for each of `cost-first`, `max-hit`, and `min-cost`. The recorded 1x stress comparison served `259` versus `224` requests for its compared selectors. Use these only to catch accidental harness changes when replaying that exact capture and configuration; do not use them as an expected result for another capture or as a formula recommendation.
