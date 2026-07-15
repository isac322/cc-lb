# ADR 0009: Reproducible Build Scope

- Status: Accepted
- Date: 2026-07-14
- Research: [.omo/ulw-research/20260714-reproducible-build/SYNTHESIS.md](../../.omo/ulw-research/20260714-reproducible-build/SYNTHESIS.md)
- Supersedes: none

## Context

The reproducible-build audit found a healthy lockfile layer and several build-contract issues: production release metadata was not fully deterministic, admin assets were embedded with filesystem timestamps, config schema generation mutated the source tree during normal builds, and wasm test fixtures were built by a nested Cargo invocation without `--locked`.

The audit also found two broader candidates that are intentionally out of this implementation batch: removing the SQLx 0.8.6 dependency stack, and converting `CC_LB_ADMIN_SKIP_SPA` into a Cargo feature.

## Decision

### 1. Keep `CC_LB_ADMIN_SKIP_SPA` as an environment-controlled build knob

We will keep the current environment-variable behavior. We will not convert admin SPA skipping into a Cargo feature.

Cargo features are additive and unified across the dependency graph. A `skip-spa` feature could be enabled by `--all-features`, a workspace build, or a transitive feature request, and that would silently compile the placeholder SPA into an artifact that may be mistaken for a production build. The environment variable is explicit per build invocation and is not affected by Cargo feature unification.

The existing `CC_LB_ADMIN_SKIP_SPA=1` workflows remain valid and intentionally produce placeholder SPA artifacts for jobs that do not need the real dashboard bundle.

### 2. Keep `CC_LB_SKIP_WASM_FIXTURE_BUILD` behavior unchanged

The wasm fixture skip knob remains an environment-controlled escape hatch for builds where integration-test fixtures are not in scope. Release server builds can continue to skip fixture generation.

The only wasm fixture build change in this batch is to require `--locked` when the nested fixture build does run.

### 3. Do not remove the SQLx 0.8.6 stack in this batch

`cargo tree -i sqlx@0.8.6` shows the SQLx 0.8.6 stack is driven by Apalis dependencies, especially `apalis-postgres v1.0.0-rc.8`, `apalis-sqlite v1.0.0-rc.8`, `cc-lb-scheduler`, and `cc-lb-server`. The `cc-lb-admin` dev-dependency set also pins `scheduler-sqlx = { package = "sqlx", version = "=0.8.6", ... }` for scheduler-related tests.

SQLx PR #4136's deterministic migration ordering fix is present in SQLx 0.9.0 but not 0.8.6. The repository's own migration sites use the workspace SQLx 0.9 stack; the remaining 0.8.6 risk is coupled to Apalis and dev/test support. Upgrading or removing it requires an Apalis compatibility move and is out of scope for this reproducible-build hardening pass.

### 4. Preserve explicit exclusions

This batch will not perform Docker base-image digest pinning, `apk`/`apt` package version pinning, SQLite-only feature-graph rewiring, or Cargo path-hash mitigation. Those are separate policy or structural changes.

## Consequences

### Positive

- Skip behavior remains exactly as existing CI and release jobs expect.
- The production-vs-placeholder SPA distinction stays explicit at the build invocation boundary.
- SQLx/Apalis compatibility risk is not mixed into small reproducible-build hardening changes.

### Negative/Risks

- SQLx 0.8.6 remains in the resolved graph while Apalis depends on it.
- Jobs using `CC_LB_ADMIN_SKIP_SPA=1` continue to build placeholder SPA artifacts, not production dashboard artifacts.

## Out of scope

- Converting `CC_LB_ADMIN_SKIP_SPA` or `CC_LB_SKIP_WASM_FIXTURE_BUILD` into Cargo features.
- Upgrading Apalis or forcing its SQLx stack to 0.9.
- Docker digest pinning and OS package version pinning.
- SQLite-only feature isolation and `pg_notify_fanout` gating.
