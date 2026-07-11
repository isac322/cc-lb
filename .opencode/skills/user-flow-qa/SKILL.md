---
name: user-flow-qa
description: Scenario-based (BDD) functional QA gate for cc-lb — prove user-facing features and flows work end to end (storage → API → admin-web UI → routing), not just that code compiles or unit tests pass. Use when QAing any cc-lb feature/flow before calling it done, checking a fix didn't regress a related flow, reproducing a user report like 'X looks wrong / doesn't update / shows stale / same graph / wrong number', or adding a QA scenario after a bug. Each scenario covers BOTH point-in-time correctness AND state-transition behavior (how each layer changes when backend data changes), reproducible by a zero-context agent. Extensible library — add a scenario whenever a feature ships or a user-facing bug is found. Complements proxy-e2e-qa (proxy request path) and frontend-fanout-qa (frontend UI/UX); owns the end-to-end feature/user-flow scenario level. Not for pure refactors, unit-test-only changes, or work with no user-observable behavior.
---

# cc-lb user-flow QA (scenario-based)

Prove that cc-lb features work the way a **user** experiences them — across storage, the HTTP admin API, the admin-web UI, and routing — using reproducible BDD scenarios. This is a **living library**: it grows one scenario at a time as features ship and bugs are found.

## Core principle
A feature is not "done" or "fixed" because it compiles, lints, or passes unit tests. It is verified only when a scenario proves the user-visible behavior end to end — the right rows in storage, the right API responses, the right UI render — **and the right CHANGES when backend state changes** (state transitions), all reproducibly. A snapshot-only pass misses "it never updates" and "it shows stale data".

## When to use
- QAing a new or changed cc-lb feature / user flow before declaring it done.
- Confirming a fix did not regress a related flow.
- Reproducing / diagnosing a user report ("looks wrong", "doesn't update", "stale", "same graph", "wrong number").
- Adding a new scenario after a user-facing bug is found (lock it so it never silently regresses).

## Every scenario has two equally-weighted halves
1. **Point-in-time** — Given a fixed backend state, is each layer correct? (storage rows, API payloads, UI render.)
2. **State-transition (BDD)** — Given an initial state, When backend data changes via a deterministic mutation, Then each layer changes as it should, within the polling latency. ~Half the value lives here.

## How to run a scenario
1. Open the relevant file under `references/scenarios/`.
2. Follow its §0 environment/preconditions (reach the service, auth, and the isolated-instance recipe when the scenario MUTATES state).
3. Execute point-in-time cases — `curl` (API), `sqlite3 -readonly` (storage), a real browser (UI) — record PASS/FAIL with evidence.
4. Execute state-transition cases — apply the deterministic MUTATION, wait the poll interval, assert the before→after delta at each layer — record PASS/VERIFIED/FAIL.
5. Fill the scenario's verdict table. Report honestly: **PASS / FAIL / VERIFIED / BLOCKED**, with evidence (curl output, sqlite query, screenshot). Never claim a UI works without opening a browser.

## How to ADD a scenario (do this after every user-facing bug or new feature)
Create `references/scenarios/<feature>.md`, then add a row to the Scenario index below. Include:
- Purpose + the user flow it covers.
- §0 environment/preconditions (+ an isolated-instance recipe if it mutates state — never mutate shared prod).
- Context data (how to discover current state + a reference snapshot).
- A deterministic mutation engine (how to change backend state on demand: SQL / CLI / admin endpoint / the writer path).
- Point-in-time cases (Given state → Expected at storage / API / UI).
- State-transition cases (Given → When mutate → Then delta at storage / API / UI, with poll latency).
- Automated-test coverage map (which Rust/web tests already cover this) + manual-only gaps.
- A verdict table.

## Shared harness & cross-cutting caveats (learned the hard way — apply to every scenario)
- **Isolate mutations.** To test state transitions, run a THROWAWAY instance on a `.backup` copy or a fresh DB with off-prod ports — NEVER hand-write into the shared prod DB. Tear it down after: kill the process (note an `exec`'d child can survive a pty kill — verify no listeners remain) and `rm -rf` the temp dir. Confirm prod is untouched.
- **Some read endpoints are cache-served, not DB-backed.** e.g. cc-lb's `/admin/v1/subscription-quotas/latest` (and the sidebar meters + snapshot cards it feeds) reads an in-memory cache updated ONLY by the live writer or startup replay — a direct SQL seed is INVISIBLE there. History/series endpoints read the DB directly. To transition a cache-served surface, use the writer path (e.g. `fire-now`) or restart the instance; direct SQL only moves DB-backed surfaces.
- **Background pollers overwrite synthetic seeds.** On a copied-prod DB with real credentials, pollers keep writing real observations through the writer path and clobber hand-seeded rows. For clean controlled UI demos, use a fresh DB with no upstreams/credentials (poller idle) or disable polling.
- **Hand-seeded IDs must match the type the read path expects.** e.g. a checkpoint `sample_id` must be a valid UUID or the series read returns 500 for that key. When seeding by SQL, match the writer's exact shapes.
- **Browser QA is mandatory for any UI claim.** Drive a real browser (agent-browser), auth via the SPA (admin bearer token in `localStorage['cc-lb-admin-token']`), and screenshot before/after. Absence of an error ≠ a correct render; verify the actual pixels/text.

## Verification gates
- Storage claim → `sqlite3 -readonly` query. API claim → `curl` with observed status/body. UI claim → real browser render (screenshot / DOM snapshot). Transition claim → before/after evidence spanning the poll interval.
- Report PASS / FAIL / VERIFIED / BLOCKED honestly. If a layer genuinely cannot be exercised, say so and why (BLOCKED), don't fake a pass.

## Scenario index
| Scenario | User flow it proves | File |
|---|---|---|
| Subscription quota — full stack | quota storage → endpoints (latest/series/analysis/aggregate/pool-history) → analysis/routing → admin dashboards, plus state transitions T1–T10 | `references/scenarios/subscription-quota-fullstack.md` |
| Subscription quota — frontend | admin-web quota surfaces: Overview pool chart, upstream-detail Quota History + snapshot cards + deficit, sidebar meters; render + range windowing | `references/scenarios/subscription-quota-frontend.md` |
| Request log observability | request logs correctly capture and display various request lifecycle events, including rate limits, slow streaming, client disconnects, and timeouts | `references/scenarios/request-log-observability.md` |
| Request log exploration | bounded live/history retention, session → All transition, time ranges, status-class filtering, select geometry, and client pagination | `references/scenarios/request-log-exploration.md` |
| Scheduler restart — quota freshness | replacement scheduler worker consumption → quota writer/SQLite latest freshness → checkpoint-series API without fabricated leading zeroes | `references/scenarios/scheduler-restart-quota-freshness.md` |

_Add a row here for every new scenario._
