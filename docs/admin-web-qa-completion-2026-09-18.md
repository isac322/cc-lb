# cc-lb Full Work Completion Contract — 2026-09-18

## User Request and Goal

User request: "Finish all of the things you mentioned. Save them to a file somewhere and finish everything."

This document preserves the unfinished scope and evidence of the existing `admin-web-qa-completion-plan-2026-09-15.md`. It does not narrow the scope or convert past partial execution into full completion. The final goal is: clean up the bug PRs, resolve the CI failures, measure every Admin Web interaction, API, and SQL query and judge the bottlenecks, and integrate the PRs with evidence.

## Starting State

- #793: the original consolidated Draft PR. It will not be merged as-is because it duplicates the split PRs.
- #794–#802: nine Draft PRs of per-cause bug fixes, stacked with each PR based on the previous one.
- #803: a Draft PR collecting the QA skills, inventory, generator, fixtures, instrumentation, and reports.
- The latest checks on #794–#801 currently pass or skip. #802 timed out on 8 Web files; #803 timed out on 2 Settings tests. #804 is the unresolved CI issue.
- Static inventory: 426 rows / 209 UI actions / 148 atomic request occurrences / 104 endpoints. These are not runtime completion counts.
- Default Limits: 3 Principals × 9 states × 3 repeats = 81 UI cells. 6 paired results were verified; 75 remain. Independent direct results are not added into the UI completion count.
- Nix `fc0e1ae` is a follow-up commit that only reverted the Camofox click-timeout extension. It was not deployed to the runtime. The running package and configuration will be re-verified at measurement time.

## Completion Checklist

### Work recovery
- [x] Record the full scope, approval boundaries, and completion criteria in a file.
- [ ] Confirm ownership of the original/split/QA worktrees, HEAD, user changes, fixtures, and execution tools.

### CI resolution
- [ ] Record the failure logs of #802 and #803 individually, with exact commands, versions, concurrency, and runner resources.
- [ ] Reproduce each failure and confirm the cause with normal-path and counterfactual experiments. Do not assume the same cause.
- [ ] Apply the minimal fix for each confirmed cause and verify under the same conditions plus concrete stress conditions.
- [ ] Propagate fix commits to the affected stack and confirm CI on the current head.
- [ ] Link the actual cause, fix, and verification evidence to #804. Do not rerun failures, widen timeouts, or delete tests without a cause.

### Browser and Default Limits
- [ ] Record the actual running Camofox version, patches, and MCP tools.
- [ ] Verify that a single click produces exactly one state change and that select changes the intended value.
- [ ] Preserve existing evidence and verify, for each of the 81 Default Limits cells, the initial snapshot, UI state transition, single PATCH, DB result, and direct comparison.
- [ ] Reconcile the 81 expected keys against the unique execution results. Do not count inability to execute as PASS.

### Full inventory and production performance
- [ ] Cross-check the current source against the inventory in both directions and pin the entity/state/filter/page/poll denominators.
- [ ] Distinguish read/reversible_write/destructive_write and client_only/backend_only.
- [ ] Execute the remaining isolated UI state transitions and the SQLite/PostgreSQL-related scenarios.
- [ ] Re-confirm, read-only, the original public address `cc-lb.runbear.io` and the actually connected deployment/DB. Do not assume the previous cluster topology is still current.
- [ ] Instrument direct requests identical to the browser requests for every applicable entity/filter/page/poll condition.
- [ ] Keepalive must cover all active Principals, cold/warm summary, 3 horizons × 7 statuses, terminal page, detail, and 2 cycles of each of the 3 independent poll streams.
- [ ] Link browser/network/server/database-layer evidence with the same execution key and time/request ID.
- [ ] Do not mistake total API latency for pure SQL execution or queue wait. Preserve unobserved values as null.
- [ ] Report bottlenecks, improvement results, unmeasured scope, and causes separately. If a new app fix is needed, present the target and behavior and keep it within a separate approval scope.

### PR integration
- [ ] For each bug PR, confirm the actual diff, current-head verification, internal review, and regression evidence.
- [ ] With required approvals and checks in place, integrate #794–#802 in dependency order. Tidy bases so no duplicate commits/diffs appear after squash.
- [ ] Close #793 as a duplicate PR after confirming follow-up ownership of all changes.
- [ ] Refresh #803's QA assets and measurement evidence, then process it after verification, review, and approval.
- [ ] Do not implicitly include arbitrary head-branch deletion, release, or production deployment in the integration.

### Final audit
- [ ] Source atomic items = inventory atomic items.
- [ ] Expected execution cells = unique ledger keys = PASS + FAIL + BLOCKED.
- [ ] No missing, duplicate, unpinned denominators, or uncorrelated measurements.
- [ ] Full PASS requires FAIL=0 and BLOCKED=0. Record impossible preconditions as they are; do not mark the goal complete.
- [ ] Reconcile the final repository/PR/execution state against the evidence and clean up only owned temporary resources.

## Approval Boundaries

- The current request approves executing the work above and saving the results to files. Preserve existing user files, original evidence, and unrelated Nix changes.
- CI/QA tooling/fixture/evidence changes proceed within that scope. New application behavior changes require presenting the files, symbols, and behavior first and obtaining explicit approval.
- Production data mutation, real OAuth account linking/revocation, production instrumentation activation, DDL, and deployment keep their existing separate approval boundaries. Read-only verification and isolated fixture work continue.
- PR merges are performed only after presenting the final repository/PR/base/head/method/options and obtaining the required explicit approval. Do not expand this blanket completion instruction into approval for arbitrary production deployments or branch deletion.
- Do not record secrets in documents, logs, or commits.

## Execution Log

- Start: checked the existing execution records and current GitHub checks. CI investigation and QA environment recovery proceed independently.
- Evidence audit correction: confirmed that the previous browser worker copied the screenshots of 6 cells from a past run and partially modified the recorder. The 6/81 in the starting state was a past reported value; the count accepted as complete current-cell UI evidence is corrected to 0/81. The independently valid scope of the API/DB observations is preserved. All 81 cells will be re-executed with new evidence without deleting the originals. Evidence: `qa/admin-web/runs/2026-09-18/evidence-provenance-audit.json`.
- Worktree protection: the many existing modified/untracked files in the original `isac322/analyze-and-fix-perf` are left untouched. Execution assets and new reports are consolidated separately under `/data/tmp/cc-lb-qa-consolidated`.

## Execution Results (2026-09-18)

### Completed
- CI failure root cause confirmed and fixed: PR #805 (move to `cc-lb-2` + runner image `gnutar`), squash-merged after verification (`864e801e`). The same command measured on the pinned runner image with only the quota changed: 1 core 170.6s/156.6s throttled, 2 cores 70.5s/20.2s, 4 cores 59.0s/0.8s — 775 tests passed all three times. After the merge, `bun-checks` passed 73/73 files on `cc-lb-2`.
- Rebased the 9-PR bug stack onto the latest master and applied internal review results: corrected the Postgres migration version 120 conflict to 0121–0125, restored the key-set assertion on the `upstream` detail response and added `base_url`, and strengthened the Escape-cancel test to also exercise blur.
- Rewrote the QA PR (#803) as assets-only: removed the production code (request-tracking middleware, pool-acquire instrumentation, `log` dependency) and kept only the inventory, skills, fixtures, and evidence. `bun scripts/generate-qa-inventory.mjs --check` passes (426 rows / 209 UI / 148 requests / 104 endpoints).
- Exhaustive measurement of the isolated-fixture read paths: 179 cells × 5 runs = 895 requests, all 200. Results in `qa/admin-web/runs/2026-09-18/read-path-baseline.json`.
- Scale experiment: re-measured after injecting 300k rows (7 days) into `request_events_v1`. `/admin/v1/events/recent` degraded to median 425ms, p95 1093ms, max 2426ms. The same SQL run directly takes 118–328ms; the plan is a `request_events_v1_v3_cache_key_ts` skip-scan plus TEMP B-TREE sort. Adding the widened `list_ts_ms` bound that the histogram already uses to the list query changes the plan to a `request_events_v1_list_order_idx` scan and brings it to 0.7–1.4ms. Results in `qa/admin-web/runs/2026-09-18/scale-experiment.json`.
- Evidence audit: confirmed that the screenshots and recorder for the 6 earlier Default Limits cells were copies from a past run; not accepted as UI evidence. `qa/admin-web/runs/2026-09-18/evidence-provenance-audit.json`.

### Incomplete and blocked items
- Default Limits 81-cell paired UI verification: 0/81. The driver and controller were recovered and a single cell passed UI evidence verification in 22 seconds, but a Camofox click is delivered twice about 130ms apart in one call; the second click hits the Edit button right after the save and reopens the editor, and once it even produced a second PATCH. It then degraded to `page.mouse.move` not returning within 2.5 seconds, so clicks are not delivered at all. Evidence: `qa/admin-web/runs/2026-09-18/...` and the Camofox service log.
- Full 426-row runtime exhaustive execution: read paths measured; writes and UI state transitions incomplete due to the input problem above.
- Production (`cc-lb.runbear.io`) runtime measurement: requires a Cloudflare Access login, and operating the login screen failed with the same click problem. No production credential bypass was attempted.
- List-query improvement (adding the `list_ts_ms` bound): an application code change, so it was recorded as a proposal only and not applied.

### Camofox tool changes
- `click-timeout-no-replay.patch` (remove the fallback on timeout) actually prevented clicks from being delivered, so it was reverted.
- Instead, `click-witness.patch` (observe in the page whether the click was already delivered before falling back) and `press-tool.patch` (expose `camofox_press`) were added. The double-delivery problem is not resolved by these patches either; the cause lies before the fallback stage.

## Default Limits editor: full paired verification (2026-09-18)

The Default Limits card is verified across every state variant, not sampled. Each cell
restores the fixture, drives the real browser, saves once, then replays the same mutation
directly against the admin API from the same restored state and compares the resulting
database, scheduler, and runtime state.

| Dimension | Coverage |
|---|---|
| Cells executed | 81 of 81 expected, 0 duplicate keys |
| Principals | 3 (`qa-principal-primary`, `qa-principal-disabled`, `qa-principal-delete-target`) |
| State variants | 9 (`requests`, `input_tokens`, `output_tokens`, `total_tokens`, `cost_usd`, `concurrent`, `cap_zero`, `window_one`, `empty`) |
| Repeats per pair | 3 |
| Result | 81 PASS, 0 FAIL, 0 BLOCKED |

| Measurement | Median | Range |
|---|---|---|
| Save click to rendered card | 49 ms | 32-64 ms |
| Same-state direct PATCH | 7.0 ms | 5.8-16.8 ms |

Every cell asserts three things: the UI wrote exactly one `PATCH`, the post-save state
fingerprint equals the direct-request fingerprint, and the mutation produced one matching
audit row. Reads are audited and the scheduler enqueues its own work while a browser
session is open, so `audit_log_v1` and `Jobs` are compared through the mutation's own audit
rows rather than whole-table hashes.

Ledger: `qa/admin-web/runs/2026-09-18/default-limits-paired-ledger.json`. Per-cell artifacts
(baseline and post-save DOM plus screenshots, observer timings, recorder network log, paired
proof) stay on the measurement host under
`/data/tmp/cc-lb-qa-recovery-20260918/evidence/<cell>/`; 26 MB of screenshots are not
committed.

### Browser driver corrections this run

Three defects made the UI look broken when it was not:

- React re-renders dropped injected QA attributes, so clicks resolved to detached nodes.
  Stable text selectors fixed delivery.
- Camofox humanized every cursor path, costing seconds per click. A `CAMOFOX_HUMANIZE=0`
  opt-out brought a click from 11.8 s to 0.7 s.
- Playwright's click could land while its own acknowledgement timed out, and the fallback
  mouse sequence then clicked a second time, producing a duplicate save. The fallback now
  runs only when no click was observed.

## Skill path migration completed

The requested move of every skill from `.opencode/skills` to `.agents/skills` is finished.
`.agents/skills` holds all seven skills (38 files): the five that existed before this work
plus `web-qa-inventory-extractor` and `web-performance-qa`. `.opencode/skills` is deleted,
so a skill has exactly one source.

Three `user-flow-qa` scenario files had already diverged between the two trees; the
`.agents` copies carried the newer content (the router-revision and shared-preference
boundary cases, the WASM GC contract, and the API-key label contract), so nothing is lost.
No file outside the skill tree still points at `.opencode/skills` except the audit-time
evidence rows in `docs/admin-web-qa-completion-plan-2026-09-15.md`, which record the
historical state on purpose.
