# cc-lb BDD test harness — design and source corpus

This directory is the authoritative home of the BDD specification that
drives the `cc-lb-bdd-tests` crate. The crate converts the v5.2 BDD
corpus (321 scenarios across 27 features, four writer streams) into
hand-rolled `bdd_scenario!` Rust tests that run against both the
SQLite and PostgreSQL storage backends.

## Plan

| File | Purpose |
|---|---|
| [`cc-lb-bdd-test-conversion-plan.md`](./cc-lb-bdd-test-conversion-plan.md) | v3.2 conversion plan. Defines the macro contract (§3.5), backend matrix (§6), no-real-API gate (§13), CI surface (§10), and milestone gates (§11). Cleared by four Momus review passes (v1 REJECT → v2 APPROVE → v3 REJECT → v3.1 APPROVED-WITH-EDITS → v3.2 APPROVED). |

## M0 deliverables — scenario indexing

| File | Purpose |
|---|---|
| [`cc-lb-bdd-test-conversion-map.md`](./cc-lb-bdd-test-conversion-map.md) | 321-row mapping table: scenario id → `fn_name` → persona → source file/line → backend → status. Verified totals: 321 = 98 (W1) + 66 (W2) + 63 (W3) + 94 (W4). |
| [`cc-lb-bdd-oos-manual.md`](./cc-lb-bdd-oos-manual.md) | The two visual scenarios excluded from the Rust pipeline (F4.1c page transition, F4.11b tooltip) and routed to the future Playwright / visual-qa track. |
| [`cc-lb-bdd-fast-subset.md`](./cc-lb-bdd-fast-subset.md) | The 33-scenario PR-gate fast subset. Selected for the `fast_` prefix nextest filter (`test(/fast_/)`); budgeted for ≤ 5 minutes on the standard runner. |

## M1 deliverables — harness API contracts

| File | Purpose |
|---|---|
| [`cc-lb-bdd-english-translation-queue.md`](./cc-lb-bdd-english-translation-queue.md) | 321 verbatim Korean source titles + their target English step slot, used by writers to populate `title = "..."` and `description = "..."` macro attributes. |
| [`cc-lb-bdd-persona-helper-spec.md`](./cc-lb-bdd-persona-helper-spec.md) | Rust API specification for the `Alice` / `Bob` / `Charlie` / `Dana` persona clients. Defines method surface, role boundaries, and result types. |
| [`cc-lb-bdd-ctx-api-spec.md`](./cc-lb-bdd-ctx-api-spec.md) | Rust API specification for `BddCtx` — storage bootstrap, fake-anthropic / mock-anthropic-oauth-server handles, three-layer no-real-API gate, captures sink, assertion helpers, teardown order. |

## v5.2 source corpus

| File | Writer | Scenarios | Features |
|---|---|---:|---|
| [`cc-lb-true-bdd-1-team-traffic-v5.2.md`](./cc-lb-true-bdd-1-team-traffic-v5.2.md) | W1 (day-to-day operator traffic) | 98 | F1, F2, F3, F4, F6, F19, F26 |
| [`cc-lb-true-bdd-2-credential-incident-v5.2.md`](./cc-lb-true-bdd-2-credential-incident-v5.2.md) | W2 (credentials and incident response) | 66 | F5, F7, F8, F10, F11A, F11B, F11C |
| [`cc-lb-true-bdd-3-policy-plugin-v5.2.md`](./cc-lb-true-bdd-3-policy-plugin-v5.2.md) | W3 (policy and plugin authoring) | 63 | F9, F12, F21, F25, F27, F29 |
| [`cc-lb-true-bdd-4-platform-audit-v5.2.md`](./cc-lb-true-bdd-4-platform-audit-v5.2.md) | W4 (platform, audit, observability) | 94 | F13, F14, F15, F17, F18, F20, F24 |

The source corpus is the human-readable Korean BDD report; the
English step / title / description that lands in the Rust tests is
authored from these by the writers per the translation queue.
