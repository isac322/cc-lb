# Current Findings — run anchored at 2026-09-15 UTC

This collects in-progress observations only. It is not a completion report, and no item claims production deployment or exhaustive PASS.

## 1. Controlled cache experiment (keepalive endpoints)

- Verdict: PASS. Per `cache-experiment/result-summary.json`.
- Measurement scope: only the fixed `principalKeepalive` endpoints (detail/list/summary) and seeded fixture keys were measured. Not the entire product query set.
- Condition proof: 48 cold cycles, resident pages confirmed at 0 after eviction, cold-A → warm → cold-B order.
- Results (median):
  - detail: cold-A TTFB 6.52ms / warm 3.80ms; db_exec 0.135ms → 0.085ms; blks_read 9 → 0.
  - list: cold-A TTFB 23.49ms / warm 20.65ms; db_exec 4.67ms → 6.15ms; blks_read 456 → 0, blks_hit 733 → 3,768.
  - summary: cold-A TTFB 20.35ms / warm 18.24ms; db_exec 4.21ms → 5.54ms; blks_read 453 → 0, blks_hit 689 → 3,721.
- Interpretation: in this isolated experiment, PostgreSQL `shared_blks_read` dropped to 0 and buffer hits increased, while median TTFB fell by about 2.8ms for list and about 2.1ms for summary. `shared_blks_read` is the number of blocks read into PostgreSQL buffers, not the number of physical disk I/O operations. This result alone does not determine whether cache is the primary cause of the production bottleneck.
- Limitations: mincore measures only the selected relation files (WAL/metadata/temp excluded); fadvise is advisory; a PostgreSQL restart resets only shared_buffers and does not prove storage-controller coldness.
- Pure pool wait: not observable with the current app metrics/pg_stat_statements. Reported as `null` rather than filled with 0.
- Measurement attribution: `isolated_serial_pg_stat_statements_delta`, not request-ID tracing. Non-target shared_blks_read total 0, non-target statement intervals 13 (api_key_usage_writers_v1 family). The possibility of an unobserved background task running the same SQL family could not be excluded.

## 2. Production heavy principal card

- `production-heavy-principal-card.json`: from one DOM-dispatched UI click, observed the initial request's Resource Timing **total duration of 8,442ms**, HTTP 200, and 8 subsequent automatic polls. This file does not separately record that request's TTFB.
- There is no basis to decompose 8,442ms into pure DB execution or pool queue wait. Its relationship to direct GET statistics is also undetermined.
- Of the 484 keepalive direct rows, the 6 that hit client deadlines (2s×4, 3s×1, 10s×1) are right-censored lower bounds, not HTTP server FAILs.

## 3. SQL/pool correlation status

- Server-timing isolated evidence measured the canonical RID join on both backends (SQLite 17 entries, PostgreSQL 19 entries, 6 SQL-bearing each).
- However, pure DB execution time and pure pool queue wait remain unmeasured. `sql_elapsed_secs` is query-stream lifetime; `acquire_total_secs` is the full acquire.
- The PostgreSQL capture is incomplete with `action_window_still_open`.
- The earlier Camofox native 410 evidence is preserved as a capability limitation of that time. Native capability and the isolated SQL/acquire linkage have since been confirmed, but this does not mean production instrumentation was applied or the full matrix completed.

## 4. Production native observation (overlay)

- Plugins bounded slice: 33 rows / 20 unique items / PASS 15 + PASS_TARGET_ONLY 4 / BLOCKED 4 / NOT_APPLICABLE 5 / SKIPPED_WRITE 5.
- Upstreams bounded slice: 212 rows / 44 unique items / PASS 88 / SKIPPED_WRITE 94 / BLOCKED 20 / NOT_APPLICABLE 10. One unmatched item_id (`UI-SRC-0D929B471D7A-name-edit`).
- Both files have 0 writes. Not merged into the production matrix because exact scope confirmation is pending.

## 5. Out-of-scope production GETs

- `production-read-scope-deviation.json`: a browser subtask assigned to local-only verification executed 5 production GETs (0 writes). Recorded separately; not merged into any coverage set.

## 6. Known bug (unfixed)

- The incorrect button enablement for providers without `SettingsApply` support was fixed and verified on 2026-09-16 via a separately approved capability display. The actual Apply execution implementation and the existing supported-mode response contract issue below were not changed.
- `clearBaseURL`: on 2026-09-16, an explicit-null removal contract was separately approved and fixed/verified in the local candidate. Past observations are preserved; we do not claim it was applied to production.

## 7. Source catalog status

- Live source files were regenerated against the approved uncommitted candidate (standard parser passed). Baseline archive hashes are preserved in `input_freeze.archive_member`.
- Current denominators: 433 rows / 170 storage operations / 422 production matrix rows / ui_actions 205 / api_endpoints 115.

## 8. Local additional verification after merge hold

- The PR #793 merge hold remains in place. Follow-up local verification and approved fixes were applied to the draft PR as commits `80e1180f…` and `91cd5219…`, and CI for each head was checked separately. This application is neither a merge nor a production deployment.
- `post-hold-reassessment/native-source-mapping.json`: the original 245 native observation rows were reconciled offline. 244 rows are canonical IDs; 1 row is an explicit variant alias for a name edit cancelled with Escape. Existing execution status and the production matrix were not changed; the latest classification is linked to the follow-up overlay in runtime-progress.
- `post-hold-reassessment/independent-page-oracle-check.json`: in a real local PostgreSQL Plugins page navigation, the pre-registered path/title was compared against a separately extracted DOM state, and the original PNG was checked. The closed observation window and the real registry SQL/acquire linkage were verified. The oracle that copied the same open window, lost data, and identical HTML hash to both sides remains a limitation.
- **2026-09-16 local usage transition:** `post-hold-usage-status/verification.json` records the storage → API → UI change from `$1.25 · 2.0K tok` to `$2.00 · 3.0K tok`. This verifies the display path of a directly seeded rollup, not raw request ingestion.
- **2026-09-16 local status transition:** confirmed the real apply error for an upstream without an OAuth credential, plus the danger dot and error title. After the disable API call, storage, the status API, and the UI transitioned to disabled, neutral, and no title. The hypothesis that an unreachable URL was the cause of the error was rejected. Native tooltip pixels are unverified.
- Confirmed that the RT collection script's `/admin/v1`-only filter missed `/admin/usage`. In a new same-origin `/admin/` observation, 3 real UI usage requests were each linked to 2 SQL statements and 2 acquires. The past gap was not backfilled with new measurements.
- This result verifies functional and state transitions of two local variants; it does not mean a full production run, pure DB time, pure pool wait, or overall QA PASS.

## 9. Earlier Base URL clearing fix — see §12 for the follow-up contract change

- Implemented: omitted field in update is preserved, a URL sets the value, and explicit null removes the override. Create/response nullable representation and API key/OAuth token handling were kept.
- The existing HTTP regression failed before the fix with `String(previous_url) != Null`. After the fix, all 33 checks passed, including SQLite/PostgreSQL conformance, signer, and the real Lifecycle→RecordingDispatcher default destination verification.
- In the real SettingsCard, performed clear input → save → independent GET → reload. Confirmed DB base_url NULL, the `—` on screen, and default endpoint metadata. User-visible input behavior and stored state now match.
- The isolated fixture was restored to its original loopback override and the server/browser were stopped. Revision and audit records increased normally, so this is not marked as a byte-identical restore. There were no external Anthropic requests or production changes.

## 10. Settings Apply support

- Added the real provider's `apply_supported` to the draft response. Apply is enabled only when true, together with the existing validation/revision conditions; it is disabled when false or not yet confirmed. Save and Validate are unchanged.
- 23 backend regressions including supported/unsupported HTTP paths and 700 Web tests passed. On a real file-backed server, Apply stayed disabled even after Save/Validate succeeded, and there were 0 server Apply requests during that observation window.
- Apply-specific guidance was placed outside the shared aria-live pipeline, with only Apply connected as the described target. The Validate description was kept as-is. Guidance for initial loading, query errors, and unconfirmed states is also distinguished.
- The final local typecheck's package-script run could not find tsgo due to the shell's command lookup. The `-b --noEmit` check run with the same installed tsgo version via absolute path passed. No global shell settings or dependencies were changed.
- **Separate existing contract limitation:** the supported-mode Apply handler returns `{status: applied}` but the client expects `applied_revision` etc. The incorrect revision shown in the success toast is a source-level inference, not a result executed in this browser run. The client's `expected_revision` is also not extracted by that handler. History refetch exists, but new history creation is separately unverified. This is outside the approved capability scope, so it was not fixed.

## 11. Distinguishing the current report from past observations

- `runtime-progress.json` and `.md` include the 2026-09-16 follow-up overlay. The original 1,511 normalized rows, classification coefficients, and 135 unresolved items are preserved as a past snapshot and must not be mistaken for the current remaining work count.
- The earlier report snapshot had 205 actions and 595 variant labels. After the approved safety fixes, the current source has 205 actions and 597 labels. The label count is not an execution denominator expanded over entity·page·poll and is not used in execution-rate calculations.
- Base URL clearing, file-provider Apply disablement, and the two local usage/status transitions each have their own limited resolution scope. The full production matrix, pure DB/queue wait, the non-production OAuth success path, production instrumentation application, and merge remain incomplete.
- The user confirmed they cannot currently provide a non-production OAuth test account. Only the real success path remains blocked by an external prerequisite, kept separate from the already-verified failure/cancel/isolated capability results. No production token or arbitrary account is substituted.

## 12. Approved application safety fixes

- Re-verification of head `19b05570` found real regressions. A no-edit save of a form left open from an older version cleared the Base URL, and admin actions after the newest 250 non-admin records disappeared from the Audit screen. The extra scan/sort of Audit ordering by changed scope was also confirmed. Failure materials are preserved in `application-safety/before/`.
- After the user's explicit approval, HTTP `base_url` omission/null was reverted to preserve, and only `clear_base_url: true` clears it. The UI compares against the baseline value when entering edit mode, and does not show success or close the form unless the clear response's `base_url === null` is confirmed. Key/token null semantics and CAS are preserved.
- With the final binary `f08348b6dcbb821de48ba5d5bf2576168e03ec6c184626d8033a344bdfb4e2b8`, both version directions were verified in a real browser. A no-edit save from old UI → new server preserved URL/revision; the new UI's intended clear stored DB NULL. The ignored clear from new UI → old server (one PUT 200) displayed as an error with editing retained, and the DB URL was preserved.
- The Audit UI uses `admin_only=true`, and both DBs apply the admin-target condition before LIMIT. The general API default query continues to return non-admin records. In the real UI, the previously empty list changed to show 1 admin action. The limitation that the browser ResourceTiming could not directly capture the query string remains in the original observation and was not replaced with per-request performance correlation evidence.
- Added 5 sorted/partial indexes to each of the two DBs. Verified A/B/A match of 6 query results, removal of the SQLite temp sort, partial-index use in the PG forced generic actor plan, and the real migration runner plus concurrent pre-creation path on 100k synthetic rows.
- Index write cost increased. `application-safety/after/index-tradeoff-summary.json` records SQLite autocommit inserts and PG DB execution/WAL increases with measurement limitations. The raw seed's PG token NULL differed from a real writer and produced HTTP 500, so only the test data was fixed to token 0. This fixture error was not classified as an app error, and the decoder was not relaxed.
- **Production deployment condition:** on large production PG tables, a separately approved online pre-index creation with exact definition and valid/ready verification is required. Plain startup creation blocks writes, and `lock_timeout` does not limit creation time. No production DDL, deployment, or merge was performed.
- Final regressions: 112 Rust, 706 Web, plus typecheck, build, affected-Rust all-targets/all-features Clippy, and formatter all passed. The Source inventory 433-row check also passed. Real fix requests from two independent reviews were handled, and raw evidence/cleanup results are linked in `application-safety/verification.json` and `index.json`. This is isolated verification of the three approved fixes, not completion of the full production QA.
