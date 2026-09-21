# Web CI Timeout Root Cause — 2026-09-18

## Observed failure

| Item | PR #802 | PR #803 |
|---|---|---|
| run / job | 35263994593 / 105346444454 | 35264193825 / 105347115912 |
| head | 970ff5d7a03a33d50fcf6a1c8ee3961cea5ec90d | 7b7a7fc06373da23c5ac323b572060068338db92 |
| runner pod | cc-lb-1-nzzpb-runner-8stjg | cc-lb-1-nzzpb-runner-6w2gl |
| scheduled node | rpi4 (Cortex-A72 1.5GHz) | rock5bp (RK3588 Cortex-A76 2.4GHz) |
| vitest elapsed | 1216.87s | 485.56s |
| failure | 8 files, 20 cases, all `Test timed out in 5000ms` | 1 file, 2 cases, all identical |
| CFS throttled periods | +7342 / 14193 periods | +3986 / 5461 periods |

The command is identical in both runs: `bunx --bun vitest run --passWithNoTests --maxWorkers=1 --no-file-parallelism`.

## Confirmed contributing cause

`bun-checks` runs on `cc-lb-1`, and that AutoscalingRunnerSet's container resources are `{"limits":{"cpu":1,"memory":"2Gi"},"requests":{"cpu":1,"memory":"2Gi"}}`. Vitest uses a single worker, but the bun runtime, GC, and jsdom timers together demand more than 1 core, so the cgroup is throttled in most scheduling periods. As a result, individual tests exceed the 5000ms wall-clock budget.

Quota curve measured with the pinned image (`sha256:c2f09887…`), the same CI command, and PR #802's web tree (`970ff5d7`):

| quota | vitest wall | throttled | throttled periods | result |
|---|---|---|---|---|
| 1 core | 170.6s | 156.6s | 1314/1707 (77%) | 775 passed |
| 2 cores | 70.5s | 20.2s | 39% | 775 passed |
| 4 cores | 59.0s | 0.8s | 3% | 775 passed |

At the same quota, 1 core takes 2.9× the wall clock of 4 cores. CPU usage at 4 cores is 75.8s against a 59.0s wall, so average demanded parallelism is about 1.3 and instantaneous demand exceeds 2.

## Separate infrastructure defect

The runner image provides only busybox tar (1.38.0), which rejects `-P`. Because `actions/cache` passes `-P`, Bun cache restore and save fail on every run (#802 `Cache Bun install store` misses after 49s, `bun install` 66s). Installing `gnutar` makes `/usr/bin/tar` GNU tar 1.35.90, which accepts `-P`.

## Applied fix

PR #805 (`fix/ci-web-runner-capacity`, commit `1c2f8636`):
- `.github/workflows/web.yml`: `runs-on: cc-lb-1` → `cc-lb-2`, with the measurement basis recorded in a comment.
- `.github/runner-image/Dockerfile`: added `gnutar` and a build-time assertion of GNU tar.

No test deletions, `#[ignore]`-style workarounds, testTimeout relaxation, or failure retries were made.

## Verification

- Runner package layer build succeeded; in-build GNU tar assertion passed.
- `actionlint .github/workflows/web.yml` passed.
- `hadolint` reports no new findings versus the current master file.
- The three quota runs above executed the real CI command inside the pinned image.

## Limitations

- The quota curve was measured on an arm64 workstation, not on the rpi4/rock5bp nodes themselves. Absolute times on those nodes are slower.
- The image change takes effect only after the runner image is republished and the digest referenced by CI is updated.
- `cargo-deny`, which still uses `cc-lb-1`, is outside this scope.
- At 2 cores, 39% of periods are still throttled. Eliminating the node performance difference requires a separate scheduling policy, which is a cluster change and was not applied here.
