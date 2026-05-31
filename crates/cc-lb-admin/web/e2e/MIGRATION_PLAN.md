# cc-lb Dashboard E2E Migration Plan

Companion document to `BDD_SCENARIOS.md`. The contract is harness-agnostic; this plan is the actionable how-to for implementing the 241 scenarios against **Playwright Test** with per-worker isolated backends.

This document is **planning only**. No source code is committed alongside it. Treat it as the brief an implementing engineer (or AI agent) reads before opening their first PR.

---

## 0. Status snapshot

| Metric | Value |
|---|---|
| Behavior contract | `e2e/BDD_SCENARIOS.md` — 56 features, 241 scenarios, 2 `@wip` features |
| Execution target | Playwright Test 1.60.x with `@playwright/test`, Chromium only |
| Existing specs | `e2e/upstreams.spec.ts` (97 lines, 6 tests), `e2e/principals.spec.ts` (132 lines, 5 tests), `e2e/plugins.spec.ts` (124 lines, 6 tests). All use `test.describe.serial` and a single shared backend on port 8082. |
| Existing infra | `e2e/global-setup.ts` spawns one `cc-lb-server` + one `fake-anthropic`; `global-teardown.ts` kills them. |
| Existing config | `playwright.config.ts` is **`workers: 1, fullyParallel: false`** — no real parallelism today. |
| CI | E2E is **NOT gated** in any `.github/workflows/*.yml` workflow today. |
| Decision log | Round-4 verdict in `BDD_SCENARIOS.md §6.9`: Playwright over agent-browser, reference-only contract over playwright-bdd. |

---

## 1. Per-feature classification (all 56 features)

Tier legend:

- **T1 — Ready**: implementable today with locators + existing seeder.
- **T2 — Minor harness**: needs `page.route` mock, an extra `Seeder` method, clipboard permission, or `page.on('dialog')`.
- **T3 — Significant harness**: needs controllable SSE, deterministic time, controllable rate-limit headers, full credential-status matrix, sparse-order collision setup.
- **T4 — Cannot be pure browser E2E**: focus-ring coverage, proving an unwired hook stays unwired, etc.

Priority legend:

- **P1**: smoke + existing specs refactored; first PR.
- **P2**: CRUD coverage; remaining happy paths.
- **P3**: cross-cutting, adversarial, exception paths.
- **P4**: significant harness investment.
- **P-blocked**: blocked on a frontend/backend change (see §8).

| § | Feature | Scenarios | Tier | Priority | Target spec file | Required helpers / harness |
|---|---|---:|:---:|:---:|---|---|
| 4.1 | Admin token gating | 9 | T1 | P1 | `auth.spec.ts` | `signIn`, `signOut` helpers; localStorage manipulation |
| 4.1 | Live connection indicator | 1 (outline ×4) | T2 | P2 | `shell.spec.ts` | `stub401` on `/admin/health`; SSE forced-disconnect via `page.route` |
| 4.1 | Sidebar navigation | 1 (outline ×9) | T1 | P1 | `shell.spec.ts` | locators by sidebar link name |
| 4.2 | Overview KPI cards | 5 | T2 / T3 | P2 | `overview.spec.ts` | seeded traffic via proxy + key OR `page.route` for `/admin/dashboard/summary` |
| 4.2 | Overview resource error banner | 2 | T2 | P2 | `overview.spec.ts` | seed error-state upstream + principal |
| 4.2 | Requests-by-model chart | 3 | T2 / T3 | P2 | `overview.spec.ts` | seeded traffic OR route mock for `/admin/usage` |
| 4.2 | Principal usage strip | 3 | T2 / T3 | P2 | `overview.spec.ts` | seeded traffic for multiple principals |
| 4.2 | Overview embedded live log | 4 | T3 | P3 | `overview.spec.ts` | SSE control |
| 4.3 | Principal directory navigation | 4 | T2 | P2 | `principal-limits.spec.ts` | `seedPrincipal('alpha')`, `seedPrincipal('beta')` |
| 4.3 | Selected principal's usage summary | 3 | T2 | P2 | `principal-limits.spec.ts` | seeded principal + proxied traffic |
| 4.3 | Rate-limit snapshots panel | 4 | T2 / T3 | P2 / P4 | `principal-limits.spec.ts` | `fake-anthropic` already emits `anthropic-ratelimit-requests-remaining: 999` and `anthropic-ratelimit-tokens-remaining: 999000` on every response (see §12). Happy path is T2. "Exhausted window" + "unobserved" + "account vs credential" matrix scenarios need either a tiny `fake-anthropic` patch to accept controllable values, or storage-level snapshot seeding (T3, Phase 4). |
| 4.4 | Realtime log streaming | 5 | T2 | P3 | `log.spec.ts` | recent-events backfill via `Seeder.driveProxyTraffic`; SSE disconnect via `fake-anthropic` `x-fake-mode: truncate-mid-stream` header (see §12); empty state happy path |
| 4.4 | Log filter bar | 5 | T2 | P2 | `log.spec.ts` | URL-query assertions + filtered event seeding |
| 4.4 | Pause and resume | 4 | T2 / T3 | P3 | `log.spec.ts` | Pause + Clear are T1. 12-event buffer assertion needs controllable event-arrival rate — `fake-anthropic` `x-fake-mode: slow` + `--slow-mode-bps` gives deterministic timing (T2). Strict cadence assertions remain T3. |
| 4.4 | Clearing the table | 1 | T1 | P2 | `log.spec.ts` | none |
| 4.4 | Row detail panel | 2 | T1 | P2 | `log.spec.ts` | none |
| 4.5 | Principal roster view | 5 | T1 / T2 | P1 | `management-principals.spec.ts` | `Seeder.stagePrincipal` (for apply-error case) |
| 4.5 | Creating a new principal | 8 | T1 | P1 | `management-principals.spec.ts` | clipboard permission for keys |
| 4.5 | Editing a principal | 5 | T1 | P2 | `management-principals.spec.ts` | none |
| 4.5 | Enabling and disabling a principal | 2 | T1 | P2 | `management-principals.spec.ts` | none |
| 4.5 | Issuing and revoking principal keys | 9 | T1 / T2 | P1 | `management-principals.spec.ts` | `context.grantPermissions(['clipboard-*'])` for one-time-reveal scenario |
| 4.5 | Draft revision banner | 2 | T1 | P2 | `management-principals.spec.ts` | `Seeder.stagePrincipal` without apply |
| 4.6 | Active credentials list | 4 | T2 | P2 | `management-credentials.spec.ts` | seed credentials via admin REST |
| 4.6 | Rotating a credential | 3 | T2 | P2 | `management-credentials.spec.ts` | clipboard + Seeder rotate |
| 4.6 | Revoking a credential | 3 | T1 | P2 | `management-credentials.spec.ts` | confirmation dialog |
| 4.7 | Schema-driven settings form | 11 | T2 / T3 | P2 | `settings.spec.ts` | autosave debounce timing; secret show/hide; constraint-validation native pseudo-class assertion |
| 4.7 | Draft autosave handles concurrent edits | 2 | T2 / T3 | P3 | `settings.spec.ts` | simulate other operator via parallel `Seeder` PUT; force 409 via `stub409Conflict` |
| 4.7 | Validate & apply | 6 | T1 / T2 | P2 | `settings.spec.ts` | confirm modal + apply happy path; conflict scenario is `@wip` per §6.8 |
| 4.7 | Configuration history drawer | 9 | T2 | P2 | `settings.spec.ts` | seed history via repeated draft → apply; diff modal; truncated-warning needs large diff |
| 4.7 | Restart-required banner | 2 | T2 | P2 | `settings.spec.ts` | seed restart-required draft change |
| 4.8 | Upstreams listing | 6 | T1 / T2 | P1 | `upstreams.spec.ts` | status badge popover for apply-error needs `Seeder.simulateApplyError` |
| 4.8 | Browser-alert mutation failures | 1 (outline ×4) | T2 | P2 | `upstreams.spec.ts` | `captureAlerts` + `stub500` on enable/disable/edit/delete |
| 4.8 | Creating an API-key upstream | 8 | T1 | P1 | `upstreams.spec.ts` | none beyond what exists |
| 4.8 | Creating an OAuth upstream | 5 | T2 | P2 | `upstreams.spec.ts` | `context.waitForEvent('popup')` + popup-blocker fallback path |
| 4.8 | Editing an upstream | 4 | T1 | P2 | `upstreams.spec.ts` | none |
| 4.8 | Enabling/disabling/deleting | 6 | T1 / T2 | P2 | `upstreams.spec.ts` | referenced-delete needs `Seeder.makeUpstreamReferenced` |
| 4.8 | Concurrency conflicts | 2 | T2 | P3 | `upstreams.spec.ts` | parallel `Seeder.updateUpstream` to advance revision; conflict UI assertion |
| 4.9 | Audit log | 8 | T2 | P2 | `activity.spec.ts` | seed varied audit kinds; redaction is DOM-only |
| 4.10 | Credential status overview | 9 | T3 | P4 | `credentials.spec.ts` | full status matrix (`valid/expiring_soon/expired/revoked/missing`) needs storage-level seeding; OAuth refresh-token presence assertion |
| 4.11 | Wasm registry listing | 4 | T1 | P1 | `plugins-registry.spec.ts` | extism_echo_plugin.wasm fixture |
| 4.11 | Uploading a Wasm plugin | 6 | T1 | P1 | `plugins-registry.spec.ts` | oversize fixture (`dd if=/dev/zero of=oversize.wasm bs=1 count=$((32*1024*1024+1))`); non-wasm fixture |
| 4.11 | Deleting a Wasm plugin | 2 | T1 | P2 | `plugins-registry.spec.ts` | in-use scenario needs chain attachment seeded |
| 4.12 | Selecting a principal exposes chains | 2 | T1 | P1 | `plugins-chains.spec.ts` | `seedPrincipal('alpha')` |
| 4.12 | Adding a plugin to a chain | 5 | T1 | P1 | `plugins-chains.spec.ts` | already exists |
| 4.12 | Removing a plugin from a chain | 1 | T1 | P2 | `plugins-chains.spec.ts` | none |
| 4.12 | Reordering a chain | 4 | T2 / T3 | P3 | `plugins-chains.spec.ts` | keyboard sensor (T1); mouse drag (T3, flaky); collision needs `Seeder.setOrderKey` |
| 4.12 | Plugin Status panel `@wip` | 2 | T4 | P-blocked | `plugins-chains.spec.ts` | requires `cc-lb-admin/src/routes.rs` to route `PluginStatus.tsx` — until then, only the **absence** assertion is testable |
| 4.13 | Unknown routes | 1 | T1 | P1 | `shell.spec.ts` | none |
| P5 | Universal modal dismissal | 2 (outline ×27) | T1 | P3 | `cross-cutting.spec.ts` | iterate all modals + Cancel/Escape/X-icon methods |
| P5 | List endpoint failure contract | 2 (two outlines) | T2 | P3 | `cross-cutting.spec.ts` | `stub500` on every list endpoint; retry button click |
| P5 | 401 topbar contract | 3 | T2 | P3 | `cross-cutting.spec.ts` | `stub401` on `/admin/health` AND `/admin/events/stream`; per-call REST 401 does NOT flip topbar (negative assertion) |
| P5 | Keyboard navigation | 1 | T1 | P3 | `cross-cutting.spec.ts` | `page.keyboard.press('Tab')` traversal; do NOT assert focus-ring pixels |
| P5 | Optimistic concurrency | 5 (three outlines) | T2 | P3 | `cross-cutting.spec.ts` | `captureRequests` + assert `If-Match` header value OR body `expected_revision` |
| P5 | Background polling | 2 (one outline) | T3 | P4 | `polling.spec.ts` | real-time wait + `captureRequests` count; OR Playwright `page.clock` (experimental) |
| P5 | Adversarial edge cases | 9 | mixed | P3 / P4 | `cross-cutting.spec.ts` | Modal Escape stack: open stacked modal manually + assertEscape closes only top. Body overflow leak: assert `document.body.style.overflow` after stacked-modal close. Negative quota: storage-level snapshot seeding. localStorage SecurityError: `context.addInitScript` overriding `localStorage` getter. Clock skew: `page.clock.setSystemTime`. String 'null': principal storage hack. Killswitch banner: `Seeder.setKillswitch(true)`. |
| P5 | "Does NOT do" negative-space | 14 | T1 / T4 | P3 | `shell.spec.ts` | each scenario asserts absence of a button/menu/control. `useGcOrphaned` etc. hook-unwired assertions are T4 (static, not E2E). |

### 1.1 Roll-up by tier

| Tier | Scenario count (approx) | % of total |
|---:|---:|---:|
| T1 — Ready | ~125 | 52% |
| T2 — Minor harness | ~80 | 33% |
| T3 — Significant harness | ~30 | 12% |
| T4 — Cannot pure E2E | ~6 | 3% |

### 1.2 Roll-up by priority

| Phase | Features | Scenarios (approx) |
|---:|---:|---:|
| P1 | 11 | ~50 |
| P2 | 24 | ~110 |
| P3 | 15 | ~55 |
| P4 | 4 | ~20 |
| P-blocked | 2 | ~6 |

---

## 2. Phased migration roadmap

Each phase is a shippable PR (or PR series). Gating criteria are concrete pass/fail signals, not subjective sign-offs.

### Phase 1 — Infrastructure + smoke (≈ 3 engineer-days)

**Deliverables**
- `e2e/fixtures.ts` — worker-scoped backend fixture (cc-lb-server + fake-anthropic + redb per worker).
- `e2e/helpers/{auth,seed,mocks}.ts` — `signIn` / `Seeder` / `stub500` / `stub409Conflict` / `stubSlow` / `captureRequests` / `captureAlerts`.
- `e2e/global-setup.ts` rewritten: build dashboard (`bun run build`) + `cargo build -p cc-lb-server -p fake-anthropic` once.
- `e2e/global-teardown.ts` shrunk to no-op (per-worker teardown moves into fixture).
- `playwright.config.ts` rewritten: `fullyParallel: true`, `workers: max(1, cpus-1)`, `retries: CI ? 1 : 0`, artifacts on first retry, no `webServer`.
- `package.json` — add `@playwright/test` 1.60.0 to devDependencies; remove unused `webServer` script if any.
- `biome.json` — include `e2e/**/*.ts` and `playwright.config.ts` in `files.includes`.
- `auth.spec.ts` — §4.1 admin token gating (9), live connection indicator outline (4 examples), sidebar navigation outline (9 examples).
- `shell.spec.ts` — §4.13 unknown route, §P5 negative-space static absence checks (14).
- Refactor `upstreams.spec.ts` / `principals.spec.ts` / `plugins.spec.ts` to import from `./fixtures`, drop `.serial` where possible, prefix tests with `§N.M.K` IDs.

**Gating signal**
- `bun run e2e` exits 0 locally with `workers: max(1, cpus-1)`.
- `bun run typecheck` clean.
- `bun run lint` clean.
- ~50 tests visible in the HTML report.

### Phase 2 — CRUD + happy paths (≈ 8 engineer-days)

**Deliverables**
- `overview.spec.ts` — KPI cards (T1 happy path; T3 deterministic-traffic scenarios stubbed `@wip`).
- `principal-limits.spec.ts` — directory + usage; rate-limit snapshots stubbed `@wip`.
- `management-principals.spec.ts` — expand to cover all 31 scenarios.
- `management-credentials.spec.ts` — rotate (clipboard) + revoke + list.
- `settings.spec.ts` — schema form happy path; validate/apply happy + cancel; history drawer; restart banner. Conflict scenarios stubbed.
- `upstreams.spec.ts` — expand to cover all 32 scenarios except cross-cutting.
- `activity.spec.ts` — audit log filtering + redaction.
- `credentials.spec.ts` — only `valid` row scenarios T1; rest `@wip`.
- `plugins-registry.spec.ts` — expand to cover all 12 scenarios.
- `plugins-chains.spec.ts` — add/remove/select; chain reorder via keyboard sensor.
- `Seeder` extensions: `stagePrincipal`, `issueKey`, `seedHistory`, `setKillswitch`, `simulateApplyError`, `uploadWasm`.

**Gating signal**
- ~170 tests passing locally on 12-way parallelism.
- HTML report shows feature-level grouping by `test.describe`.

### Phase 3 — Cross-cutting + adversarial (≈ 4 engineer-days)

**Deliverables**
- `cross-cutting.spec.ts` — universal modal dismissal (iterate every modal × {Cancel / Escape / X-icon}); list endpoint failure contract; 401 topbar contract; keyboard navigation; optimistic concurrency If-Match / expected_revision assertions; the 9 adversarial scenarios.
- Helpers extensions: stacked-modal scaffolding, body-overflow assertion utility.

**Gating signal**
- ~220 tests in suite; ≥95% pass rate (a few `@wip` allowed).
- New `@destructive` and `@conflict` tags exercised in CI nightly.

### Phase 4 — Significant harness investment (≈ 6 engineer-days)

**Deliverables**
- `polling.spec.ts` — `page.clock` driven cadence tests for all 8 polling intervals (5s, 30s, 60s).
- SSE control harness: `fake-anthropic` enhancement to emit synthetic Anthropic events with controllable timing; or admin-side test endpoint to inject events.
- `log.spec.ts` — backfill, pause/resume, filter outlines fully covered.
- `principal-limits.spec.ts` — rate-limit snapshots: `fake-anthropic` extended to emit `anthropic-ratelimit-*` headers; storage-level snapshot seeding via a Seeder method (which itself wraps a new admin endpoint OR direct redb manipulation).
- `credentials.spec.ts` — full status matrix via a Seeder method that talks to a credential-status injection endpoint (new admin API).

**Gating signal**
- ~240 tests; `@wip` count ≤ 5; flake rate < 1% over 10 consecutive CI runs.

### Phase 5 — Backend dependencies (blocked)

These cannot proceed until the listed code change lands. They are tracked separately so they do not stall the rest of the suite.

| Feature | Blocker |
|---|---|
| §4.12 Plugin Status panel | Wire `PluginStatus.tsx` into `/plugins` route in `cc-lb-admin/src/routes.rs` |
| §4.7 Apply-conflict surfacing | Replace `Settings.tsx`'s empty `catch` with proper conflict-banner state |
| §6.8 useGcOrphaned hook stays unwired | Static (unit) assertion outside Playwright |

---

## 3. Risk register

Tabular. Likelihood × Impact = priority; mitigations are concrete actions, not aspirations.

| Risk | Likelihood | Impact | Mitigation |
|---|:---:|:---:|---|
| `cargo build` cold-start exceeds 45s deadline on first CI run after cache miss | Medium | High | Pre-warm cargo target/ via build job; bump deadline to 90s on CI only; consider `actions/cache` keyed on `Cargo.lock` |
| Port collisions at 8080–8189 (12 workers × 10 ports) on shared CI runners | Low (self-hosted) / Medium (GH Actions) | High | Each worker probes for port-free before spawn; on collision, error message names the conflicting port; use container-per-job on shared runners |
| `redb` write concurrency under proxy + admin parallel load triggers stale-read bugs | Low | Medium | Use only one cc-lb-server process per worker; tests should not exercise proxy AND admin concurrently on the same backend |
| SSE backfill timing flakes in headless Chromium | Medium | Medium | Use `expect.poll` with generous deadline (10s); never use bare `waitForTimeout` for SSE assertions |
| `@dnd-kit` keyboard sensor key-event mismatch across Chromium versions | Low | Low | Pin Playwright Chromium revision in `package.json`; if break, fall back to `dispatchEvent` synthetic |
| OAuth popup blocker triggers in headless mode | Medium | Medium | Verify `--popups-allowed-by-default` Chromium flag; fixture currently uses default args — may need to add this flag |
| `fake-anthropic` rate-limit header VALUES are hardcoded to 999 / 999000 (see §12 audit) | Medium | Medium (blocks §4.3 exhausted-window scenarios) | Phase 4 work item: extend `with_fixture_headers` in `tests/fixtures/fake-anthropic/src/routes.rs` to accept controllable values via `x-fake-mode: ratelimit-<remaining>` header or new query string. ≤30 LOC. |
| Credential status matrix (`expired`, `revoked`, `missing`) requires storage seeding the REST API does not currently expose | High | Medium (blocks §4.10) | Phase 4 work item: add admin endpoint `/admin/v1/test/seed-credential-status` (test-build-only feature flag) OR direct redb manipulation in Seeder |
| Existing 3 specs use `localhost:8080` for proxy curl — leaks the 8080 default | Already realized | Already fixed in Phase 1 plan | Refactor uses `backend.proxyUrl` (per-worker port) |
| Trace artifacts at 12-way × ~240 scenarios saturate disk on local | Low | Low | `trace: 'retain-on-failure'` + `screenshot: 'only-on-failure'` already in plan; budget 1GB per run for failed-test artifacts |
| Bun-managed Playwright binary mismatch across dev / CI | Low | Medium | `bunx playwright install chromium` after `bun install` in both environments; pin Playwright minor in package.json |
| Self-hosted runner becomes a single point of failure | Medium | Medium | Document the runner setup; add GH Actions fallback workflow (slower but always available) |
| LLM-written tests use `getByRole('button')` ambiguously across multiple matching elements | High | Low | Style guide in §7: always pass `{ name: '...', exact: true }`; teach via examples |
| Tests that need clipboard fail on macOS CI due to permission prompts | Medium | Low | Linux-only CI; macOS dev should run `--ignore-default-args=--use-mock-keychain` |

---

## 4. CI integration design

### 4.1 Runner choice

| Runner | Cost | Suite time estimate | Pros | Cons |
|---|---|---|---|---|
| **Self-hosted (this workstation)** | $0 | ~2–4 min | 12 cores, 32 GB; per-worker isolation cheap | Single point of failure; needs maintenance |
| GH Actions `ubuntu-latest` (4 vCPU, 16 GB) | ~$0.008/min × 10 = $0.08/run | ~10–15 min (2–4 workers) | Always available; ephemeral | Slower; lower parallelism |
| GH Actions `ubuntu-latest-16-core` (16 vCPU, 64 GB) | ~$0.064/min × 5 = $0.32/run | ~3–5 min | Fast | $$$$ |

**Recommendation**: self-hosted primary; GH Actions standard runner as fallback for PRs from forks (no self-hosted access).

### 4.2 Workflow design

```yaml
# .github/workflows/e2e.yml (sketch — for planning only, NOT to write yet)
name: e2e
on: [push, pull_request]

jobs:
  e2e:
    runs-on: [self-hosted, linux, x64]
    steps:
      - uses: actions/checkout@v5
      - uses: actions/cache@v4
        with:
          path: |
            ~/.cargo
            target/
            crates/cc-lb-admin/web/node_modules
          key: e2e-${{ hashFiles('Cargo.lock', 'crates/cc-lb-admin/web/bun.lock') }}
      - run: cargo build -p cc-lb-server -p fake-anthropic
      - run: cargo build --release --target wasm32-wasip1 -p extism_echo_plugin
      - run: cd crates/cc-lb-admin/web && bun install
      - run: cd crates/cc-lb-admin/web && bunx playwright install chromium
      - run: cd crates/cc-lb-admin/web && bun run e2e
        env:
          CI: 'true'
      - if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: playwright-report
          path: crates/cc-lb-admin/web/playwright-report/
          retention-days: 14
```

### 4.3 Sharding strategy

At 12 workers on a single runner there's no need to shard across machines. If runtime grows past ~10 minutes:

1. Split by tag: `bun run e2e --grep '@smoke'` for PR gate; full suite on nightly.
2. Then by file group: `bun run e2e --shard=1/3` etc.
3. Only then by scenario.

### 4.4 Artifact retention

- HTML report: 14 days, on failure only.
- Traces: included in HTML report; same retention.
- Videos: `retain-on-failure` only; same retention.
- Screenshots: `only-on-failure`; same retention.

### 4.5 PR gating tiers

| Tier | Trigger | Tests run |
|---|---|---|
| Smoke | every PR | `--grep '@smoke'` (~25 scenarios) |
| Full | merge to main + nightly | all 241 |
| Adversarial | nightly only | `@regression` + `@conflict` + `@destructive` |

---

## 5. Contract-drift verification script (design)

**Goal**: fail CI when a scenario in `BDD_SCENARIOS.md` has no matching `test('§N.M.K ...')` in `e2e/**.spec.ts`, OR a test exists with a scenario ID not present in the contract.

**Pseudocode**

```python
# scripts/check-bdd-coverage.py (planning only — to be written as bash + jq or awk)

import re
from pathlib import Path

ROOT = Path('crates/cc-lb-admin/web/e2e')
contract_ids = set()

# Walk BDD_SCENARIOS.md; capture (section, feature, scenario-name).
# Hash scenario-name into a stable §N.M.K-style ID — N.M.K is the per-section, per-feature, per-scenario counter.
for line in (ROOT / 'BDD_SCENARIOS.md').read_text().splitlines():
    if line.startswith('### 4.'):  # section header → captures N.M
        section = re.match(r'### (\d+\.\d+)', line).group(1)
        feature_idx = 0
        scenario_idx = 0
    elif line.startswith('Feature:'):
        feature_idx += 1
        scenario_idx = 0
    elif re.match(r'  Scenario( Outline)?:', line):
        scenario_idx += 1
        contract_ids.add(f'§{section}.{feature_idx}.{scenario_idx}')

# Walk *.spec.ts; capture test('§N.M.K ...') names.
test_ids = set()
for spec in ROOT.glob('*.spec.ts'):
    for match in re.finditer(r"test\(['\"](§\d+\.\d+\.\d+)", spec.read_text()):
        test_ids.add(match.group(1))

missing_in_tests = contract_ids - test_ids
missing_in_contract = test_ids - contract_ids
allowed_wip = {'§4.12.5.1', '§4.7.3.7'}  # @wip scenarios; whitelist by ID

if missing_in_tests - allowed_wip:
    print('FAIL: scenarios in contract without tests:', sorted(missing_in_tests - allowed_wip))
    sys.exit(1)
if missing_in_contract:
    print('FAIL: tests with IDs not in contract:', sorted(missing_in_contract))
    sys.exit(1)
```

**Invocation**: run as a step in `.github/workflows/e2e.yml` before the Playwright run. Fast (<1s). Allowed-WIP list is committed to the script.

---

## 6. Per-scenario stub generator (design)

**Goal**: at Phase 1 start, emit one `test.fixme(...)` stub per BDD scenario into a stub `.spec.ts`, so coverage tracking starts at 0/241 on day 1 and increments as the implementer fills in bodies.

**Pseudocode**

```python
# scripts/gen-stubs.py (planning only)

# Same walker as §5. For each (section, feature, scenario, tags):
#   - Pick target spec file from a mapping {section_id: spec_path}.
#   - Emit:
#     test('§N.M.K scenario description', { tag: ['@smoke', ...] }, async ({ page, backend }) => {
#       test.fixme(true, 'unimplemented');
#       // <Gherkin body, copied verbatim as comment>
#     });

# Group by feature → emit test.describe.serial(...) wrapper when feature has shared-state tests.
```

The implementer then turns `test.fixme` into a real implementation one at a time. The Playwright HTML report shows fixme-vs-passed-vs-failed counts as a primary KPI.

---

## 7. AI authoring failure modes

When an LLM agent picks up this contract and writes Playwright tests, the same mistakes recur. Document them here so the next agent reads them once and avoids them.

| Failure mode | Symptom | Fix |
|---|---|---|
| Ambiguous `getByRole` selector | `Error: strict mode violation: getByRole('button', { name: 'Delete' }) resolved to 3 elements` | Always pass `exact: true` and scope via `getByRole('dialog').getByRole('button', { name: 'Delete', exact: true })` |
| Missing `await` on promise | Spurious passes on later assertions | TypeScript strict mode catches some; biome `noFloatingPromises` does not. Code review + `await expect(promise).resolves...` pattern |
| Locator captured before page state ready | `Error: locator.click: Target closed` mid-action | Re-locate inside the action block; locators are lazy in Playwright but the `Locator` object must be re-evaluated after navigation |
| `page.waitForTimeout` used for "should be enough" | Flake | Replace with `expect(locator).toBeVisible({ timeout: N })` or `expect.poll(...)` |
| `text=...` shorthand for unique strings | Picks up unintended siblings | Use `getByText('Saved', { exact: true })` |
| Forgetting that `expect(locator).toBeVisible` auto-retries | Adding `page.waitForSelector` before it | `toBeVisible()` already polls up to `expect.timeout` |
| Not closing dialog after `captureAlerts` | Next test's dialog event fires the old handler | `captureAlerts` returns `stop()` — call in `afterEach` |
| Asserting backend state via UI re-read instead of admin API | Slow, flaky | Use the `Seeder` to verify backend, the UI to verify rendering |
| Hardcoding `localhost:8082` | Test passes on worker 0, breaks on worker 1 | Always use `backend.adminUrl`, `backend.proxyUrl`, etc. |
| Using `page.goto('/upstreams')` (relative) | `baseURL` is not set per-worker — see Part 6 | Always go through `gotoDashboard(page, backend.adminUrl, '/upstreams')` |
| `test.describe.serial` everywhere out of habit | No real parallelism gain | Use `.serial` ONLY when tests in the same file share mutated state. Otherwise prefer per-test isolation via unique IDs |
| Snapshot-style `@eN` ref thinking transferred from agent-browser | "How do I re-snapshot?" | Playwright locators auto-retry; there is no snapshot concept. Just keep the locator and call `.click()` after page change |

---

## 8. Backend / frontend dependencies

These features cannot be fully implemented as pure E2E until the listed code changes land.

| Feature | What to change | Owner |
|---|---|---|
| §4.12 Plugin Status panel | `cc-lb-admin/src/routes.rs` must mount `PluginStatus.tsx` on `/plugins`. Currently `RustEmbed` serves only `PluginRegistry`. | frontend + backend |
| §4.7 Apply conflict UI | `Settings.tsx` empty catch must surface the apply error into a banner / status indicator. | frontend |
| §4.3 Rate-limit snapshots full matrix | `fake-anthropic` ALREADY emits `anthropic-ratelimit-requests-remaining: 999` and `anthropic-ratelimit-tokens-remaining: 999000` on every response. Only the "exhausted window" + matrix variants need controllable values via a small `with_fixture_headers` extension. ≤30 LOC. | tests/fixtures |
| §4.10 Credential status full matrix | One of: (a) admin REST endpoint to inject status, behind a `cfg(debug_assertions)` feature flag; (b) direct redb manipulation in Seeder using `redb` crate as a test dep. | backend OR Seeder |
| §4.4 SSE deterministic timing | `fake-anthropic` ALREADY supports `x-fake-mode` header (Slow / Timeout / TruncateMidStream / TamperUnknownEvent) plus `--slow-mode-bps` CLI flag. See §12 for the full enumeration. Only ultra-precise cadence assertions need extra work. | tests/fixtures (mostly done) |
| §6.6 useGcOrphaned / useExport / UpstreamCard wired? | If yes, scenarios convert from negative-space to positive. If no, they stay as negative-space tests. | product decision |

---

## 9. Resource budget

### 9.1 Memory at 12-way parallelism

Per worker:
- Chromium browser process: ~120 MB
- `cc-lb-server` (debug): ~40 MB resident with redb
- `fake-anthropic` (debug): ~15 MB
- Node worker (Playwright): ~80 MB

= ~255 MB × 12 = **~3.1 GB** workers + ~500 MB Playwright main + ~500 MB OS overhead ≈ **4–5 GB total**. Well under the 32 GB ceiling; we have headroom for 18+ workers if a future spike demands.

### 9.2 Suite runtime targets

| Phase | Local 12-way | CI self-hosted | CI GH Actions 4-core |
|---|---:|---:|---:|
| Phase 1 (50 tests) | ~30 s | ~30 s | ~2 min |
| Phase 2 (170 tests) | ~2 min | ~2 min | ~10 min |
| Phase 3 (220 tests) | ~3 min | ~3 min | ~12 min |
| Phase 4 (240 tests) | ~4 min | ~4 min | ~15 min |

Targets assume cargo target/ pre-warmed. Cold cargo build adds ~30–90s once.

### 9.3 Disk

- Per-worker `data-test-worker-N/`: ~50–200 MB during the run, deleted on teardown.
- HTML report + traces (failed tests only): ~10–200 MB per run.

---

## 10. Decision log (for future maintainers)

| Decision | Date round | Why |
|---|---|---|
| Playwright Test over agent-browser | Round 4 | agent-browser deal-breakers documented in `BDD_SCENARIOS.md §6.9` — no `--status` flag for network mock, dnd-kit mouse-drag impossible, snapshot `@eN` refs stale on every DOM change, no Playwright Trace Viewer equivalent |
| Reference-only contract (not playwright-bdd) | Round 4 | Two-file sync problem hostile to AI agents; existing 3 specs already Playwright-shaped; LLM training data favors plain Playwright |
| Per-worker isolated cc-lb-server + redb | Round 4 | <10ms backend startup with redb; deterministic state per test; no shared-DB truncate-and-seed bottleneck |
| Exec `target/debug/*` binaries directly | Round 4 | `cargo run` serializes on the cargo compilation lock; pre-build once in globalSetup, exec the binary thereafter |
| No Vite dev server at test time | Round 4 | `cc-lb-admin` embeds the SPA via `rust_embed` and serves it on the admin port — eliminates Vite proxy port juggling entirely |
| `fullyParallel: true` + `workers: cpus - 1` | Round 4 | Bench data: 120 scenarios/min at 12-way redb isolation |
| Chromium-only (no Firefox / WebKit) | Round 4 | Audit budget; production dashboard supports only modern browsers; Firefox/WebKit BiDi adoption still mid-maturity in 2026 |
| `@playwright/test` 1.60.0 (pinned) | Round 4 | Latest stable as of audit; pin minor to avoid breaking changes from auto-upgrade |
| BDD scenario ID format `§N.M.K` | Round 4 | Section number from `BDD_SCENARIOS.md §N.M`, per-feature counter, per-scenario counter. Stable as long as scenarios are not reordered. |

---

## 11. Open questions for the operator

Decisions or facts that this plan cannot resolve without operator input.

1. **Which spec files in Phase 1 are acceptable to re-write vs. extend?** The current 3 specs were authored ad hoc; the plan refactors them in place. Confirm OK to refactor, or whether to keep them and add new specs side-by-side.
2. **Is self-hosted CI runner setup in scope?** The plan assumes the 32GB/12-core workstation becomes the primary runner. If so, who registers it as a GH Actions runner and maintains it? If not, fall back to `ubuntu-latest-16-core` paid runners.
3. **Phase 5 backend dependencies** — should §4.12 Plugin Status, §4.7 Apply-conflict UI, etc. be tracked as separate frontend tickets, or as part of this E2E migration?
4. **`fake-anthropic` ownership** — extensions to emit rate-limit headers + controllable OAuth happy/error paths are non-trivial. Does the test-fixtures crate get bigger, or does this go to a different team?
5. **Credential-status injection endpoint** — is adding a `cfg(debug_assertions)`-gated admin endpoint acceptable, or should the Seeder talk directly to redb?
6. **PR gating policy** — is the smoke-tier (`@smoke`, ~25 tests) acceptable as the PR gate, or is the full suite required?

---

## 12. Round-5 verification findings

This section captures findings that AMEND the initial plan after sub-agent verification. When in doubt between earlier sections and this one, this one wins.

### 12.1 `rust_embed` debug-mode disk fallback — VERIFIED

The plan's per-worker architecture depends on `cc-lb-server` (debug binary) reading the dashboard SPA from disk at runtime, so that `bun run build` once + `cargo build` once is enough — no rebuild needed when the frontend changes.

**Verdict**: VERIFIED. `crates/cc-lb-admin/Cargo.toml` inherits `rust-embed = "8"` from the workspace; neither file enables the `debug-embed` feature. Per `rust_embed`'s default behavior, debug builds read files from `<crate_root>/web/dist/` at runtime; release builds embed them at compile time. The `RustEmbed` derive at `crates/cc-lb-admin/src/routes.rs:18-22` points at `folder = "web/dist/"`. Plan's `globalSetup` flow (build dashboard → cargo build → workers exec target/debug/cc-lb-server) is sound.

### 12.2 `fake-anthropic` capability audit

The crate at `tests/fixtures/fake-anthropic/` is much more capable than the initial plan assumed. Several scenarios initially classified T3 or P4 are now T2 / P2-eligible.

**Endpoint surface** (from `src/routes.rs:98-110`)

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/messages` | Anthropic-shaped response + 50-event SSE stream (`src/sse.rs:14-117`) |
| POST | `/v1/messages/count_tokens` | Token counting |
| GET | `/oauth/authorize` | Renders authorize page OR auto-redirects with `?state=…&code=…` |
| POST | `/oauth/token` | PKCE code exchange + refresh-token rotation |
| POST | `/v1/oauth/token` | Alias for `/oauth/token` |
| GET | `/__refresh_history` | Test-only: returns full refresh-token rotation history. **USE THIS for credential rotation assertions instead of UI scraping.** |
| GET | `/__last_request` | Test-only: returns the most recent inbound request headers/body. **USE THIS to verify the proxy forwarded the expected `x-api-key`, model, etc.** |
| GET | `/v1/models`, `GET /v1/models/{id}` | Model list / fetch |
| GET / POST | `/v1/files`, `POST` | Files API |
| GET / DELETE | `/v1/files/{id}` | File fetch / delete |

**`x-fake-mode` header** (from `src/modes.rs`)

Inbound requests can set `x-fake-mode: <mode>` to control the response. Modes available today:

| Header value | Behavior |
|---|---|
| (omitted) or anything unknown | `Ok` — normal happy path |
| `401` | `Unauthorized` — 401 with `authentication_error` JSON |
| `429` | `RateLimited` — 429 with `rate_limit_error` JSON + `Retry-After: 1` |
| `500` | `ServerError` — 500 with `api_error` JSON |
| `timeout` | `Timeout` — sleeps 60s, then 504 |
| `slow` | `Slow` — SSE stream throttled by `--slow-mode-bps` (default 1024 bps) |
| `tamper-unknown-event` | SSE injects `event: foo` at index 10 |
| `truncate-mid-stream` | SSE truncates after 3 deltas |

**Static fixture response headers** (from `src/routes.rs:444-462`)

Every response (happy path) includes:

| Header | Value |
|---|---|
| `request-id` | unique per request |
| `anthropic-organization-id` | `org_test` |
| `anthropic-ratelimit-requests-remaining` | `999` (hardcoded — needs ≤30 LOC patch for controllable values) |
| `anthropic-ratelimit-tokens-remaining` | `999000` (hardcoded) |

**OAuth implementation** (from `src/oauth.rs`)

Full PKCE flow including:
- Authorize endpoint accepts `state`, `code_challenge`, `code_challenge_method`, `code_verifier`-style params
- Token endpoint exchanges `grant_type=authorization_code` → access + refresh tokens
- Token endpoint accepts `grant_type=refresh_token` → rotates BOTH access and refresh tokens (real Anthropic behavior)
- `/__refresh_history` exposes rotation events with `old_refresh_token_fingerprint` + `new_refresh_token_fingerprint` for test assertions

**Implications for the plan**

| BDD area | Before | After |
|---|---|---|
| §4.3 rate-limit panel (rendered values) | T3, blocked on fake-anthropic | T2 happy path immediately; T3 only for "exhausted" matrix (needs ≤30 LOC patch) |
| §4.4 SSE disconnect / truncate / tamper | T3, blocked on SSE control | T2 via `x-fake-mode` header injection in `Seeder.driveProxyTraffic` |
| §4.4 Slow / backfill timing | T3 | T2 via `x-fake-mode: slow` + `--slow-mode-bps` |
| §4.8 OAuth happy path | T2 already | T2, but use `/__refresh_history` for assertions instead of UI scraping |
| Phase 4 SSE control harness | 6 engineer-days | ~1 engineer-day (just write the helper + assertions) |
| Phase 4 rate-limit fake-anthropic extension | speculative | concrete ≤30 LOC patch to `with_fixture_headers` |

**New Seeder method to add in Phase 1**

```typescript
// Seeder.driveProxyTraffic(apiKey, opts?: { fakeMode?: '401' | '429' | '500' | 'timeout' | 'slow' | 'tamper-unknown-event' | 'truncate-mid-stream' })
//   POSTs to backend.proxyUrl/v1/messages with `x-api-key` + optional `x-fake-mode`,
//   returning the proxy's response and (via /__last_request) the inbound request fake-anthropic saw.
```

This single helper unblocks §4.2 Overview traffic seeding, §4.4 Realtime Log streaming scenarios, §4.5 key-issue proxy verification, §4.8 upstream conflict simulation, and the entire Phase 4 SSE-control work without any extra backend code.

### 12.3 Updated effort estimate

| Phase | Original estimate | Revised estimate | Δ |
|---|---:|---:|---:|
| Phase 1 — infrastructure + smoke | 3 days | 3 days | 0 |
| Phase 2 — CRUD + happy paths | 8 days | 7 days | −1 (OAuth scenarios cheaper than expected) |
| Phase 3 — cross-cutting + adversarial | 4 days | 4 days | 0 |
| Phase 4 — significant harness | 6 days | 3 days | −3 (SSE control + rate-limit much cheaper) |
| **Total** | **21 days** | **17 days** | **−4** |

### 12.4 Updated risk register entries

- Removed: "fake-anthropic does NOT emit rate-limit headers" — false; they're emitted, just hardcoded.
- Reduced: "fake-anthropic SSE timing flake in headless" — `x-fake-mode: slow` gives deterministic delay; risk drops from Medium to Low.
- Added: "test author misuses `x-fake-mode` header" — the modes silently fall through on unknown values; LLM-written tests may typo. Mitigation: TypeScript enum in `helpers/mocks.ts` for the header values.
- Added: "OAuth fixture happy path drift" — `fake-anthropic`'s `/oauth/authorize` may change page layout in future releases. Tests that assert specific button text are fragile. Mitigation: use the `/__refresh_history` API to verify the OAuth round-trip happened, not the popup DOM.

### 12.5 Updated decision log entries (append to §10)

| Decision | Why |
|---|---|
| Pre-built debug `cc-lb-server` binary + `rust_embed` disk fallback | VERIFIED in §12.1: workspace `rust_embed = "8"` without `debug-embed` reads from disk in debug builds. |
| Use `x-fake-mode` header for SSE error injection (not page.route) | `fake-anthropic` already implements 8 modes including truncate/tamper. Page.route would have to re-stream all 50 deltas — wasteful. |
| Use `/__last_request` and `/__refresh_history` for backend-side assertions | Avoids UI scraping for things the test infra can directly observe. Stronger contracts, faster runs. |
