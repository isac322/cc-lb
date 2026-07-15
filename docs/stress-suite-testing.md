# Stress Suite Testing

`cc-lb-stress-suite` is a manual stress-testing controller. It uses the bundled fake provider only and never sends traffic to real Anthropic. It has no CI workflow and does not need provider credentials.

## Tiers

- **T1 fast fault** uses the existing `CC_LB_CHAOS_*` Tower layer with SQLite and loopback processes. It is explicitly non-deterministic: drop behavior uses ambient randomness. T1 is excluded from replay guarantees and is not a realistic network simulation.
- **T2 Docker plus netem** is the default realistic tier. It creates a private Docker fabric with directed netem impairment and records qdisc commands and counters. Docker resources are labeled and cleaned up after the run.
- **Elevated netns** renders a host-network-namespace preview only. It does not execute elevated commands, does not invoke `sudo`, and requires an operator review outside this suite.

## Determinism Boundary

Determinism applies to the scenario and command flow materialized in a manifest: seed-derived personas, request schedule, waves, topology references, and netem command references. `replay --dry-run` verifies that manifest integrity.

Determinism does not apply to packet outcomes, network timing, scheduler timing, process startup timing, or ambient T1 chaos randomness. A replay is therefore a verification of the materialized scenario, not a promise that runtime measurements or fault outcomes repeat.

## Commands

Use the shell entrypoint for the supported command surface:

```bash
tests/load/stress-suite.sh --help
tests/load/stress-suite.sh plan --seed 42 --profile smoke --output /tmp/stress-manifest.json
tests/load/stress-suite.sh replay --manifest /tmp/stress-manifest.json --dry-run
```

`run` requires either `--seed` or `--manifest`. The current smoke run also requires its replica, run ID, and output inputs. The `full` wrapper label is reserved for the final manual profile gate; it does not start an unscoped long-running workload.

The complete CLI surface is `plan`, `run`, `replay`, `compare`, `establish-baseline`, and `preflight`.

## T1 Exercise

Run the required loopback T1 check with:

```bash
cargo run -q -p cc-lb-stress-suite -- preflight --exercise-t1-chaos --output /tmp/t1-chaos.json
```

The command starts the bundled fake upstream and two SQLite-backed cc-lb processes. It sends one `/v1/messages` request through each cc-lb proxy, first as a control and then with a 250 ms latency setting. The JSON evidence includes `control_ms`, `chaos_ms`, `chaos_observed`, response status, listener ports, and an error field. `chaos_observed` is true only when the measured increase is at least 150 ms.

This is a client-visible proxy check: the assertion requires the fake upstream response marker after traffic enters cc-lb's proxy listener. It is not a direct request to the fake upstream.

## Baselines And Comparison

Materialize a manifest before a run, retain its evidence, then record and compare a suite-owned baseline:

```bash
tests/load/stress-suite.sh establish-baseline --evidence evidence.json --output baseline.json
tests/load/stress-suite.sh compare --baseline baseline.json --evidence candidate.json --output comparison.json
```

Comparison reports `PASS`, `FAIL`, or `NOT_COMPARABLE` for performance evidence. `BLOCKED` is reserved for a preflight capability failure. Insufficient samples, environment drift, qdisc divergence, or overlapping confidence bands are not performance passes.

## Metrics

- Achieved RPS and sample count describe workload delivery.
- Latency, TTFB, TTFT, and schedule drift capture client and scheduler timing.
- Robust dispersion and confidence bands distinguish regressions from variance.
- qdisc command/counter captures prove directed network impairment.
- Request/admin event lag and replica metrics expose lifecycle and storage behavior.
- Environment compatibility keys make baseline comparisons auditable.

Run Docker plus netem preflight before realistic work. Keep SQLite files on a local filesystem, preserve evidence artifacts, and do not treat T1 timing as a replayable result.
