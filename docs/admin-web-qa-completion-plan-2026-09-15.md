# Admin Web Exhaustive QA Remaining-Work Completion Plan

- Written: 2026-09-15
- Status: in progress. Does not claim exhaustive completion or production improvement completion.
- Work branch: `qa/admin-web-exhaustive-completion`
- Implementation baseline: `ef70b347790c46fa9956435a820a51b60c547d04` (Keepalive performance improvement PR #791 merged)
- Work tree: `/data/tmp/cc-lb-admin-web-qa-completion`
- Original preserved at: `/data/data/orca/workspaces/cc-lb/analyze-and-fix-perf`
- Original production target: `https://cc-lb.runbear.io`, Kubernetes context `runbear-operation`
- The actual production deployment commit/image, DB, and measurement time window will be re-verified and pinned separately in the execution manifest.
- The goal stays in place until every TODO in this document is done. No budget limit is set.

## 1. User request and gap audit

The original request was: a QA list tracing every web view and manipulation UI → API → handler → query, real production browser execution, separation of network/browser/server/DB bottlenecks, and Git management of two reusable skills plus the QA list. A follow-up also requested migrating the entire skill path from `.opencode/skills` to `.agents/skills`.

The earlier work delivered the Keepalive improvement code, local verification, and PR/CI, but left the following seven items incomplete.

| Gap ID | Remaining obligation | Evidence at audit time | Result required for completion |
|---|---|---|---|
| GAP-01 | Git management of the two skills, inventory, generator, and valid execution results | Untracked in the original. The #791 merge tree lacks the full inventory and the two skills | Reflected in the shared repository via a reviewed, CI-checked commit/PR and an approved merge |
| GAP-02 | Full-path migration of existing skills applied | Only moved locally. Merge tree has 0 files in `.agents/skills`, 36 files in `.opencode/skills` | Migration preserving every existing file at the latest baseline, with references and discovery verified |
| GAP-03 | Proof of a complete QA list with no omissions | The existing 226 rows are generated row counts and `pending_full_reconciliation` | Independent source denominators, forward/reverse missing sets of 0, stable IDs, and execution stages |
| GAP-04 | Full production browser execution | Only partial measurements of major screens and Keepalive-focused measurement exist | Execution results and evidence for every defined target, variant, page, and poll cell |
| GAP-05 | Supplemented production Keepalive evidence and correction of wrong reporting | Past 20-page pagination cap, unproven independence of some detail timings, leftover exhaustive-completion wording in the old report | Independent raw timestamps, real terminal cursors, report correction, and new measurement |
| GAP-06 | Per-request all-layer correlated measurement | The final local API records have null SQL/pool fields on both DBs, 389 rows each; a separate plan exists | Browser/server/SQL/pool evidence linkage; real instrumentation or explicit approval/blocking for items not provided |
| GAP-07 | Controlled cold/warm comparison | First vs. subsequent requests were distinguished, but OS/DB cold cache was not controlled | Verifiable cache states in an isolated environment, repeated comparisons under identical conditions |

The analysis/proposal `docs/keepalive-performance-proposal.md` keeps the user's uncommitted-preservation instruction. That instruction is not extended to exclude the skill/QA artifacts. The past 57 local QA and 92 browser PASSes do not substitute for exhaustive production execution.

## 2. Authority and safety boundaries

1. Approved work is: writing documents, project-owned skills, QA tooling, lists, isolated fixtures, and execution evidence; investigation and measurement; and related Git/PR preparation.
2. **Each merge requires separate approval.** The approval request explains the repository, PR number/title, what it resolves, key file/behavior changes, remaining risks, verification results, base/head SHAs, merge method, and whether the branch is deleted. Do not set merge, auto-merge, or merge queue before approval. The past approval for #791 does not apply to other PRs.
3. If new application runtime code, public API, or product behavior changes are needed, present the exact files/symbols, the behavior change, the necessity, and alternatives, and do not modify before approval. Runtime instrumentation added to fill observability gaps follows the same rule.
4. Production gets only approved read measurement. Actions that cause deletion, revocation, configuration change, or external calls are verified in isolated fixtures. Even reversible production writes require presenting the specific target and recovery method and obtaining approval.
5. Even for HTTP GET, verify real side effects, updates, and possible external billing from the source. Do not classify as safe by method alone.
6. Prohibited: production DB cache flush, production PostgreSQL restart, global stats reset, heavy production load, copying real credentials, and dumps containing secret values.
7. Production read requests run at low concurrency and pause under load, errors, or queueing. A pause is recorded as BLOCKED, not completion.
8. If production data keeps changing, record the observation window and denominator changes. Do not disguise results by merging an unstable full set verified against a fixed fixture with production results.
9. Preserve the existing original work tree. Migrate the existing skills at the latest baseline and selectively apply only the original's two new skills and QA assets. Do not wholesale-copy stale app diffs, migration numbers, or uncommitted analysis.
10. Preserve raw request IDs, timestamps, and evidence, but do not record tokens, cookies, personal data, or secret request-payload values. Commit only verified de-identified evidence and reproduction procedures to Git.

## 3. Artifacts and single ownership

- This plan document: owned by Main. Running record of requests, TODOs, approvals, status, and final evidence.
- `.agents/skills/web-qa-inventory-extractor/SKILL.md`: exhaustive static UI/API/SQL extraction and independent reverse-reconciliation procedure. Does not duplicate the execution skill's role.
- `.agents/skills/web-performance-qa/SKILL.md`: browser execution, instrumentation, risk isolation, and evidence-linkage procedure for the verified list.
- `scripts/generate-qa-inventory.mjs`: reuse the existing generator. It merely prints a previously hardcoded list; do not claim current-source investigation success from it.
- `qa/admin-web/api-query-inventory.yaml` and `.md`: same stable IDs, same atomic requests, execution methods, and source evidence.
- `qa/admin-web/runs/`: per-run manifest, source denominator/execution matrix, PASS/FAIL/BLOCKED ledger, de-identified raw evidence, and result reports. Do not overwrite existing raw records with new ones.
- Prefer reusing the existing `.agents/skills/user-flow-qa` scenarios and the `web/e2e`, `web/qa` execution assets. Do not build an unnecessary separate test framework.
- The source investigator records only source and intent. They do not fabricate browser results, measurements, or PASSes. The browser executor records only cells actually executed. Main links the two bodies of evidence.

## 4. Matrix and evidence contract

### 4.1 Static list

Each UI action has a stable ID, screen/component, current source file/symbol/location, preconditions, exact click/input/scroll steps, state/filter variants, expected screen, and related atomic request IDs. Each atomic request has method/path/query/body/relevant headers, client callsite, handler, authorization, storage trait, and the SQLite/PostgreSQL query or an explicit no-query/cache justification. UI-unreachable and client-only entries record the reason.

Dynamic paths are verified against the real wire contract. Do not merge multiple methods/endpoints into one row as `A & B`. Do not use arbitrary page sizes, All buttons whose selectors overlap, nonexistent modals, or endpoints from older versions.

### 4.2 Execution scope

- For per-entity items, verify the denominator of Principal/Upstream/Plugin/target rows via the real API.
- Expand every applicable state/filter/page/poll cycle into real execution keys. For dimensions that do not apply, record the reason and cardinality 1.
- Bound continuously growing production history by a measurement anchor and cursor range, and record the amount of change. Do not claim completion through unlimited future data.
- Confirm pagination termination only by the wire `next_cursor == null` and the last response's evidence. Do not turn a hard page cap into terminal.
- Verify write/delete, error, and empty states before and after the state transition in a fit-for-purpose isolated fixture. Environment labeling for production vs. isolated runs is separate.

### 4.3 Per-request raw records

Each record carries `run_id`, `execution_key`, `environment`, `source_commit`, `deployed_image`, UTC and monotonic start/end times, method/de-identified URL/input hash, status, and response size plus a semantic hash. A UI action and a direct request have different request IDs and times. Do not simply replay a write request twice; compare against an independent run that restores the same starting state in the isolated fixture.

Preserve browser click/request/TTFB/download/state/render timings. Link server handler and SQL/pool waits with the same correlation. For cache-served/no-DB paths, record `not_applicable` with the reason, based on real observation or source evidence. Do not fill unmeasured values with 0 or another layer's value. A null is not completion; link it to a specific unmeasured/blocked reason.

### 4.4 Completion verdict

- Static completion: source→inventory and inventory→source unmapped sets each 0, duplicate IDs 0, wrong path/method/source 0.
- Execution completion: the actual expected-cell set and the unique ledger-key set are exactly equal.
- Formula: `expected cells = unique keys = PASS + FAIL + BLOCKED`.
- Final PASS: FAIL 0, BLOCKED 0, missing required evidence 0, recovery confirmed for risky actions.
- Matching row counts, a representative entity, screenshots alone, API 200, or passing unit tests are not exhaustive completion.
- If approval, access, or required instrumentation is blocked, do not complete the TODO. Ask the user for the specific approval needed to unblock it.

## 5. Detailed TODOs and completion conditions

Update a checkbox only when execution evidence exists. Keep the names identical between the TODO tool and the 86 items below. Newly discovered obligations are added with new IDs without deleting the original items.

### Documents and baseline

- [x] PLAN-01 Write the remaining-work document and completion conditions
  - Record GAP-01~07, the original authority boundaries, all TODOs, single ownership, and the evidence contract in this document, and verify list consistency.
- [x] PLAN-02 Preserve the original and prepare a latest-baseline worktree
  - Pin a separate branch on the baseline containing #791. Preserve the original analysis/skills/QA material and selectively transfer only approved assets.
- [x] PLAN-03 Establish the production deployment authenticated-observation baseline
  - Confirm the actual image/commit, Pod/DB backend, access account/permissions, WARP, and metrics/logs/traces/DB read-only paths. Build the manifest without secret values.

### Skills and assets

- [x] ASSET-01 Full-path migration of the latest existing skills
  - Migrate every file of the latest `.opencode/skills` with content preserved. Keep this separate from adding the two new skills, and update related references.
- [x] ASSET-02 Define the two QA skills' roles and execution contract
  - Separate exhaustive extraction/reverse reconciliation from real execution/instrumentation. Clarify risk classification, variable denominators, independent timing, cold-cache meaning, and the ban on false PASSes.
- [x] ASSET-03 Prepare Git management for the inventory generator and artifacts
  - Inherit the existing generator and stable IDs, and reflect current-source verification data. Confirm YAML/Markdown generation reproducibility and consistency.
- [x] ASSET-04 Correct exaggeration verdicts in the existing production report
  - Preserve the existing raw records and state the 20-page cap / detail-timing evidence limits. Retract the earlier `runtime-reconciled`/`10/10` claims and link to the new run.
- [x] ASSET-05 Verify skill discovery and re-execution at the new path
  - Confirm that a fresh session/normal discovery finds `.agents/skills` and can run each skill's documented commands/paths. Do not falsely judge that the current session's stale `skill://` mapping found the new path.

### Exhaustive source list

- [x] INV-01 Exhaustive reconciliation of Overview common auth/navigation behavior
  - Trace all of KPI/charts/time window/usage/Principal drill-down, auth gate, command palette, navigation, and mount/poll/refresh requests.
- [x] INV-02 Exhaustive reconciliation of Upstreams Warmup behavior and requests
  - Trace every currently existing conditional UI and request: list/detail/settings/enable-disable/metadata·quota/refresh/warmup/auth/keys.
- [x] INV-03 Exhaustive reconciliation of Principals Router Keepalive behavior
  - Trace create/edit/keys/permissions/limit/route settings and the Keepalive card/sheet/filter/detail/poll/cursor behavior.
- [x] INV-04 Exhaustive reconciliation of Logs SSE filter/detail behavior
  - Trace live/history, session/group, all filters/time windows/pages, the SSE lifecycle, and row/detail/payload expansion requests.
- [x] INV-05 Exhaustive reconciliation of Plugins upload/reference/delete behavior
  - Trace list/detail/upload/replace/apply/reference/GC/delete and the conditional paths in the current UI.
- [x] INV-06 Exhaustive reconciliation of Settings Audit behavior and requests
  - Trace config draft/schema/validation/apply/history and Audit search/time window/filter/page/detail against the current source.
- [x] INV-07 Admin API reverse tracing and both-stores SQL mapping
  - Reverse-reconcile every registered method/path against UI requests. Trace handler→trait→real SQL/memory/cache and classify runtime side effects too.
- [x] INV-08 Bidirectional missing-set verification of the source inventory
  - Set-compare mutually independent source denominators against the inventory. Mechanically confirm 0 unmapped/duplicate/broken references.

- [x] INV-09 Reconcile production-only Credentials Status source and differences
  - Separately record the UI/API differences between the actual production `e56d029e` and baseline `ef70b347`. Do not omit the Credentials/Status screens remaining in production or the older auth/config/API contract, and state that the newest-only features cannot be applied to production.
### Instrumentation preparation

- [ ] MEASURE-01 Pin production entity denominators and the execution matrix
  - Record the real lists and cursor page traversal, query anchors, and state/filter/poll combinations. Set variable-data rules and production-load stop conditions.
- [ ] MEASURE-02 Prepare safe write/delete isolation fixtures
  - Configure the real app + default SQLite environment and, where needed, a PostgreSQL environment. Use safe test connections for external services; do not copy real credentials. Prepare normal/empty/error/permission states and recovery paths.
  - On 2026-09-16 the user confirmed there is currently no non-production test account for real OAuth success verification. Those success/metadata-fetch paths remain blocked by an external precondition. Do not substitute production token copying, arbitrary account selection, or fake success responses, and do not add the missing account into other isolated verifications' failures.
- [x] MEASURE-03 Confirm the per-request server SQL/pool instrumentation method
  - Check whether existing observability can yield per-request SQL count/time/pool wait. If insufficient, request approval for the exact instrumentation scope needed. Do not fill real request times from a separate EXPLAIN alone.
- [x] MEASURE-04 Verify the browser independent raw-record collector
  - Link real clicks to network capture and collect raw timestamps, response hashes, and render results. Run negative verification that does not judge copied direct-fetch values or missing values as success.

- [x] MEASURE-05 Verify a SQL log filter scoped to Admin requests
  - Tested `info,[admin.request]=debug` on isolated SQLite and PostgreSQL. It kept the SQL/acquire events of each of 3 Admin queries while SQL DEBUG outside the Admin span was 0. A comparable small broad-filter control produced 99 and 144 events respectively. The general INFO and Proxy 401 control results were preserved.
  - This setting allows all DEBUG inside the Admin span, so it is not a SQL-only filter or a secret-safety verification for every endpoint. No production settings, app code, or provider accounts were changed, and the log-volume/performance figures are not extrapolated to all of production.

### Exhaustive measurement

- [ ] RUN-01 Overview common auth/navigation browser measurement
  - Execute every applicable cell from INV-01 and record environment, atomic requests, and UI evidence.
- [ ] RUN-02 Upstreams Warmup exhaustive browser measurement
  - Execute every target Upstream and variant. Verify side-effecting actions in the isolated environment.
- [ ] RUN-03 Principals Router exhaustive browser measurement
  - Execute the general management/routing UI and related state transitions for every target Principal. Use only isolated fixtures for key issuance/revocation.
- [ ] RUN-04 Keepalive account/filter/detail polling exhaustive measurement
  - Verify all active Principals, 3 horizons × 7 filters, card/settings sheet/list/detail, and at least two cycles of each of the three concurrent poll streams.
- [ ] RUN-05 Keepalive real terminal verification of every cursor
  - Traverse each applicable combination to a real null cursor. Hard caps, mid-run errors, and variable denominators are separate blockers, not recorded as terminal.
- [ ] RUN-06 Logs SSE filter/detail exhaustive browser measurement
  - Execute every defined variant of logs and SSE and confirm consistency of time window/cursor/filter/render state.
- [ ] RUN-07 Plugins upload/reference/delete browser measurement
  - Run production read cells and isolated lifecycle-mutation cells separately, and verify reference/delete state transitions.
- [ ] RUN-08 Settings Audit exhaustive browser measurement
  - Verify production reads and isolated settings apply/history/audit state transitions.
- [ ] RUN-09 Verify write/delete state transitions and recovery results
  - Check that every mutation cell proves the storage→API→UI change and recovery. Do not drop unexecuted actions from the list.
- [ ] RUN-10 Server/DB correlation analysis for every atomic request
  - Link each execution to real handler/SQL/pool observations. Record screen/transport/server/DB bottlenecks and unobserved causes per cell.

- [ ] RUN-11 Production-only Credentials Status browser measurement
  - Execute the production-only screens and behaviors identified in INV-09 under the same real-browser/raw-evidence standard. Do not drop them from the production denominator because the latest code removed them.
### Cache comparison

- [x] CACHE-01 Verify the isolated cold/warm cache control procedure
  - In an isolated (non-production) environment, specify the control scope of app cache, DB buffer, and OS page cache, and prove cache state by observation. Do not merely rename the first request "cold".
- [x] CACHE-02 Record performance comparison per controlled cache condition
  - Measure cold and warm conditions repeatedly under the same build/engine/dataset/concurrency. Separate API/SQL/pool/response/browser metrics and state the limits.

### Verification and delivery

- [ ] VERIFY-01 Re-verify after fixing defects in missing/failed evidence
  - Fix real defects in the QA assets and re-run. For app defects, present the scope and fix only after approval. Do not pass failures by deleting, renaming, or weakening them.
- [ ] VERIFY-02 Verify raw-evidence security and execution-matrix completeness
  - Exactly compare expected keys vs. actual keys and check missing/duplicate/secret/required-null fields. An independent reviewer attempts to disprove the denominators and PASS claims.
- [ ] VERIFY-03 Sync final performance-bottleneck results with documents
  - Write per-screen/account/variant results, errors and improvements, SQL-correlation evidence, cache states, and remaining limits. Keep skill/list/plan/execution-report statuses consistent.
- [x] VERIFY-04 Independent review of the whole change and repository-gate verification
  - Independently review the full diff of skill migration, QA tooling, data, and documents. Run the formatter/lint/typecheck/tests matching the change scope and a real execution once at the end.
- [x] VERIFY-05 Commit documents/skills/inventory/evidence and create the PR
  - Commit only the stated assets and verified de-identified evidence. Exclude analysis proposals, secrets, and unrelated original app changes, and explain the PR's include/exclude scope.
- [x] VERIFY-06 Current PR head CI pass and review completion
  - Confirm CI/required reviews on the current head. Do not rerun failures unmodified; fix the cause and verify.
- [x] VERIFY-07 Explain each PR's changes and request user merge approval
  - For each PR, explain §2's final contents, verification, head/base, method, and risks, and record the user's approval. Proceed with possible independent work while waiting.
- [ ] VERIFY-08 Merge only approved PRs and confirm artifact reflection
  - Merge only with the approved identical head/target/options. If anything changes, explain again and get approval. Judge goal completion after confirming file/skill-path/report reflection in the shared repository.

- [x] VERIFY-09 Fix the YAML serialization defect and verify with a parser
  - Safely serialize special mapping keys such as empty strings. Parse the generated YAML with a standard parser and verify the original key/values are preserved. Check generated-artifact byte equality and syntactic/semantic validity separately.

### Audit error fix — additional user approval

- [x] AUDIT-01 Pin the approved latest Audit query contract and impact scope
  - Fix the defect where, in a real 263-row fixture, limiting to the oldest 200 first made the newest config_export missing from the API/UI. Preserve the existing AuditStore append-order query. The new recent query applies LIMIT after ts descending plus insertion ID/seq descending tie-break, and supports All/Principal/Actor scopes. HTTP filter validation, since/after/until, the limit cap, and audit-record side effects are preserved.
- [x] AUDIT-02 Implement order-preserving latest-record queries on both DBs
  - Add a separate query_recent_audit and a borrowed AuditQueryScope to storage-api, with real limited queries on SQLite/PostgreSQL. Do not change the existing query_audit/query_audit_by_actor or the conformance append order. Update all trait implementations and related mocks. Do not include unnecessary new migrations, live config changes, or always-on instrumentation.
- [x] AUDIT-03 Wire the Audit screen and regression-verify latest records
  - Connect both Admin Audit aliases to the new query and verify: over-200 newest records, deterministic order for equal ts, late-inserted old ts, Principal/Actor filters, empty/0-limit/reversed time windows, and UI refresh after a new export. Keep the existing tests for the original oldest-order store query. Keep the real-browser pre-fix FAIL evidence and post-fix verification separate.
- [x] AUDIT-04 Update the fixed Audit contract, QA list, and evidence
  - After the code change, update the source catalog/hash/SQL/execution spec and results. Do not confuse the earlier ef70 execution evidence with the new candidate's source/binary. The fix PR/merge follows the existing per-item approval procedure.

### Additional approved error fixes

- [x] FIX-01 Verify the fix for duplicate submission on Upstream rename
  - Converge InlineNameEditor's Enter/blur duplicate submission into a real single request. Keep normal blur save and Escape cancel, and verify with a real browser that a stale-revision 409 does not overwrite after API success.
- [x] FIX-02 Verify the fix for the missing saved Base URL in responses
  - Consistently return the stored base_url in Upstreams/OAuth read/change responses and the frontend schema. Do not restore or expose secret plaintext or the origin of an unsaved api_key_env. Verify the save→fresh read→page-refresh state transition.
- [x] FIX-03 Verify the fix for the optional API-key label contract
  - Accept omitted/empty values for the optional label on both DB issuance paths and map real invalid input to 400. Preserve key generation/hashing/auth algorithm and duplicate-issuance prevention. Verify the real issue/read/proxy-use/revoke paths and negative inputs.
- [x] FIX-04 Verify Router revision sync after Principal change
  - After related mutations that change spec_revision, refresh the Router's real server revision/strategy values and block stale saves during the refresh. Keep 409 protection for genuine concurrent edits.
- [x] FIX-05 Verify the Plugins delete-completion flow and GC guidance fix
  - Clean the cache and current selection screen so refetch/Not Found errors for a deleted target do not mask success. Adjust the guidance/active conditions to match the real behavior — GC cleans only orphan blobs — without additionally deleting registered plugins.

### Approved isolated instrumentation

- [x] OBS-01 Admin-only request identification and span implementation
  - In `app.rs::admin_router` and private middleware, link a server-generated bounded ID, `x-request-id`, a sanitized request span, and the handler time through response generation. Do not change Proxy handling, auth, body, or SSE behavior.
- [x] OBS-02 Enable full-acquire-time observation on both stores
  - In SQLite `open_sqlite` and PostgreSQL `open_postgres_pool`, make it possible to enable SQLx general acquire logs at DEBUG. Keep pool size/timeout/SQL results unchanged and declare the existing transitive dependency `log` directly.
- [x] OBS-03 Verify isolated request SQL/acquire correlation
  - On both DBs' real Admin requests, link the response ID to SQL/acquire events in the same span. Prevent identifier collisions, client-ID reflection, and handler/stream-time confusion.
- [x] OBS-04 Verify instrumentation secret non-exposure and overhead
  - Confirm isolated logs do not expose credentials/cookies/bodies/raw URLs, and compare the existing candidate and the instrumented candidate under the same conditions. Also compare Proxy control requests. Do not change production settings or deploy before separate approval.

- [x] OBS-05 Implement Server-Timing UI request identification linkage
  - In the additionally approved `Server-Timing`, pass only the existing server ID in `rid.description`. Do not newly expose time, body, or auth information. The collector conservatively handles cache-ID reuse, late ambiguity, and conflicting IDs.
- [x] OBS-06 Verify real-browser SQL correlation linkage
  - On each DB, link requests actually sent by the UI to server head/SQL/acquire logs via Resource Timing IDs. Do not substitute independent fetches for UI requests, and do not fill missing logs/bodies/pure waits with other values.

### Blocked-item reassessment

- [x] REASSESS-01 Offline reconciliation of production observation rows against source IDs
  - Reconcile the already-preserved Plugins/Upstreams native observations against real source IDs. Leave rows with no basis to link a separate observation name to a canonical ID unresolved, and send no new production requests.
- [x] REASSESS-02 Separate remaining verification possible without external approval
  - Divide remaining items into: needs external approval/credentials, needs production instrumentation applied, reconcilable with existing material, and verifiable in the approved isolated environment. Do not expand a merge hold into cancellation of other work.

- [x] REASSESS-03 Verify PostgreSQL browser-observation window-close evidence
  - Keep the existing raw snapshot's `end=null` and `ui_oracle=null`, and preserve, in a new file, a window closed after a valid `endAction` call plus real UI/RID/SQL evidence in the approved local PostgreSQL environment. No production requests or code changes.
  - Confirmed the new raw window's close time and its linkage to 3 server requests. However, because the expected and observed hashes were built from the same HTML, it is not accepted as an independent UI-oracle PASS.
- [x] REASSESS-04 Verify the independent UI oracle and original screen evidence
  - Pin the minimum expected state before the click and compare it against state extracted separately from the real DOM. Preserve the screenshot's original bytes and both state objects before computing hashes. Do not use a hash comparison that copied the same observation to both sides as pass evidence.
  - Compared the `/plugins` path and `Plugins` title pinned before the click against state extracted separately from the real DOM. The original PNG was preserved and Main verified the screen and hashes. Linked the closed observation window's registry GETs to 3 real PostgreSQL query/acquire events each. This one-page navigation verification does not mean exhaustive production QA is complete.

### Isolated remaining scenarios

- [x] LOCAL-01 Verify the real-data path of the Upstream usage display
  - Confirm which real aggregation/DTO/UI field the unobserved cost/token display connects to. Verify the real state transition only when an isolated fixture generation path without external calls is confirmed.
  - Confirmed, in a new SQLite fixture's real rollup store, Admin API, and browser, the natural poll transition `$1.25 · 2.0K tok` → `$2.00 · 3.0K tok`. In the fixed capture, linked 3 real `/admin/usage` UI requests to SQL/acquire events each. The request-ingestion path was not verified.
- [x] LOCAL-02 Verify the generation path of the Upstream error-status display
  - Confirm the real generation path and UI contract of the status display. Do not assume an error state just by setting an unreachable URL, and do not manufacture pass/fail for a UI variant that does not exist.
  - The real generation path was an OAuth credential validation error, not a connection failure. Confirmed the new isolated upstream's `error`/danger dot/error title, and after a real disable API call, confirmed the store/status API/browser changed to `disabled`/neutral/no title. Native tooltip pixel display was not verified.

### Approved Base URL clearing

- [x] BASEURL-01 Pin the explicit-null clearing contract and callsite scope
  - On 2026-09-16 the user chose `approve removal via explicit null`. In update, omission stays unchanged, null removes the override, and a URL sets it. Keep the nullable representation in create/response and the key/OAuth/warmup field contracts.
- [x] BASEURL-02 Implement tri-state update in the API and both stores
  - `UpstreamUpdate.base_url`'s outer Option means whether to change; the inner Option is the nullable value. Use the existing Serde tri-state convention and explicit presence conditions on both DBs, and migrate every real caller/mock. Add no new migration, dependency, or runtime test hook.
- [x] BASEURL-03 Regression-verify omission/set/clear and conflicts
  - Added omission, explicit null, restore, and stale revision to the existing HTTP regression, and preserved the real pre-fix failure. Verify both-DB conformance and that regression after the fix, keeping key/token non-exposure and conflict protection.
- [x] BASEURL-04 Verify real UI save/re-read and proxy destination
  - In the real SettingsCard, confirm clear→save→fresh GET→refresh leaves the default state. For the real proxy path, compare a custom override and the default destination after removal at the existing RecordingDispatcher seam. Send no paid/external Anthropic requests.
  - The pre-fix HTTP regression failed by returning the old URL; after the fix, 33/33 checks passed including SQLite/PostgreSQL conformance and signer/real Lifecycle dispatch regressions. Confirmed real UI clear/save/independent GET/refresh, then DB NULL and the default endpoint display, and restored the original loopback override. No external provider requests were made.

- [x] BASEURL-05 Final catalog/document verification after the clearing fix
  - Confirmed the formatter, all-features Clippy on affected Rust targets, the source inventory check, and standard YAML parsing. Updated both DBs' real SQL and bind info together with the UI/API tri-state contract.
- [x] BASEURL-06 Commit the Base URL fix and verify PR CI
  - Apply the additionally approved fix and isolated evidence as a separate commit to the existing draft PR and check CI on the new head. Do not apply the previous head's pass to the new head; the merge hold stays.
  - Applied follow-up commit `80e1180f0bedd2054450fb68e57444e035c59ef2` to draft PR #793. Confirmed 11 successful checks on that head — Rust, Web, publish-check, and status checks — plus 1 conditionally excluded release-artifact check. The user merge hold stays; no ready transition, merge, or deployment was done.

### Approved Settings support display

- [x] SETTINGS-01 Pin the settings-provider capability and consumer scope
  - On 2026-09-16 the user approved the support-indication fix. Only InMemoryCurrentConfig and TestReloader, which have real Apply implementations, declare support; ConfigWatcher, Config, and the default provider do not.
- [x] SETTINGS-02 Implement API support flag and Apply disablement
  - Pass CurrentConfig's explicit capability as the required `apply_supported` in the draft response. The UI allows Apply only when the value is exactly true, together with the existing validation/revision conditions, and shows a disable reason for false/unconfirmed.
- [x] SETTINGS-03 Regression-verify supported/unsupported and state transitions
  - Reproduced the pre-fix failure where Apply was enabled despite no support. Verify save/validate preservation and direct-Apply 501 on unsupported providers, the existing 200 on supported providers, and the UI's false→true→unknown transitions.
- [x] SETTINGS-04 Verify real-browser guidance and request blocking
  - On a real file-based server, confirm that after Save/Validate of a valid draft, Apply stays disabled, guidance is shown, and no Apply request is sent. Preserve the prior expected state, independent DOM, original PNG, and server logs.
  - On the real file provider, after 1 successful Save and 1 successful Validate, Apply stayed disabled and server logs showed 0 Apply requests. Confirmed the final wording, the Apply-only accessibility description, the separated Validate description, and the original PNG showing the changed controls.
- [x] SETTINGS-05 Settings-capability document inventory and PR verification
  - Update the API/UI contract and exact source references, and run the affected gates and independent review. Check new-head CI without lifting the existing merge hold or applying to production.
  - Applied commit `91cd5219bd9047d1339ff40e9aec11142867a36e` to draft PR #793. Confirmed 11 successful checks on that head — Rust, Web, publish-check, and status checks — plus 1 conditionally excluded release-artifact check. The existing support mode's response revision, request CAS, and history-generation limits were left as separately unverified items; no merge or production application was done.

### Application safety re-verification

- [x] SAFETY-01 Classify app changes by purpose and pin the impact scope
  - Of the 27 source files in head `19b05570`, 2 are cfg(test)-internal changes and 25 are runtime changes. Distinguished QA tooling from intended feature fixes/observation changes.
- [x] SAFETY-02 Check for unintended behavior changes via independent review
  - Independently reviewed instrumentation, backend/storage, and frontend. The under-evidenced Plugins double-delete conjecture was retracted after comparing notification/navigation order.
- [x] SAFETY-03 Run the missing behavior comparisons and report safety evidence
  - With the current-head build, ran 104 Rust and 53 web checks, 22 SQLite/PG HTTP pairs (44 requests), and a log non-exposure check for a successful synthetic key registration. Reproduced in a real browser the DB NULL caused by an unchanged save from a Base URL form opened on the old version, and the Audit empty screen. Confirmed per-scope Audit extra sorts/scans via both DBs' execution plans. No production impact.
- [x] SAFETY-04 Re-verify three app regression items after fix approval
  - On 2026-09-16 the user explicitly approved the three fix scopes below. The earlier CI/review-complete checks are inspection history from that time and do not mean the newly found safety defects were resolved.

### Approved app-safety fixes

- [x] SAFETYFIX-01 Base URL explicit-clear contract and form fix
  - Keep HTTP omission/null as-is; a URL sets; only `clear_base_url: true` is distinguished as clearing. If a URL and clearing are delivered together, reject before changing. The earlier BASEURL-01~06 null clearing is preserved as past verification history; the current contract is superseded by this item. Keep credential semantics and revision protection.
- [x] SAFETYFIX-02 Apply the Audit admin filter before the query limit
  - The API default keeps all records and the UI specifies `admin_only=true`. On both DBs, apply the admin-target condition before LIMIT, preserving principal/actor/time ranges and newest-first order.
- [x] SAFETYFIX-03 Verify Audit sort indexes and write cost
  - Support the real filter/sort with new SQLite/PG migrations. Do not fix existing migrations or run them on the production DB. Verify query plans and added write cost with isolated data.
  - The final indexes are 5 per DB, and the actor sort index is a partial index on `actor_authority IS NOT NULL`. A/B/A results for the 6 query shapes were identical; confirmed removal of SQLite's temp sort and use of the partial index by PostgreSQL's generic actor plan. With 100k synthetic rows, both direct creation in the real app migration runner and a separate concurrent pre-creation path succeeded, preserving row counts, definitions, and valid/ready states.
  - Write cost is not zero. For synthetic data, 2,000 SQLite autocommit inserts increased by about 33% (non-admin) and about 126% (admin) versus the average baseline. For a PostgreSQL 10k-row INSERT storing token 0 like the real writer, DB execution time rose about 73%·151% and WAL about 27%·116%. The PG figures are execution/WAL numbers excluding commit/client/pool time; do not extrapolate to production latency/throughput. The earlier SQL-only fixture cost that inserted token NULL is not used as the final cost evidence.
- [x] SAFETYFIX-04 Migrate the shared API types and all consumers together
  - Migrate the new clear field and the Audit query options' types, callsites, mocks, and catalog together. Do not overwrite past execution evidence with the current contract.
- [x] SAFETYFIX-05 Reproduce the existing failures and regression-verify on both DBs
  - Verify: unchanged save from a form kept open across a deployment transition from the old version; intentional clearing in the current UI; admin-action queries after a 429 record; both DBs' results/sort/write cost; and permission/CAS/credential preservation.
  - On final binary `f08348b6dcbb821de48ba5d5bf2576168e03ec6c184626d8033a344bdfb4e2b8`, an unchanged save from old UI→new backend kept URL/revision unchanged, and the current UI's intentional clear stored DB NULL. An ignored clear on new UI→old backend (one PUT 200) displayed as error/edit-preserved and did not fake success. The previously hidden admin actions in Audit were shown in the real UI.
  - Confirmed 112 Rust and 706 Web checks, typecheck, build, affected-target Clippy, and the formatter. On real PG HTTP, also confirmed preservation/explicit clear/400 conflict/409 CAS/restore and both Audit modes. Standard YAML parsing and inventory generate/check passed at 433 rows. The raw-evidence limits and the initial PG fixture error (token NULL) were recorded separately.
- [x] SAFETYFIX-06 Independent review, safety report, and PR update
  - Independently review the fixes and re-verification of the three defects. Check the necessary repository gates and new-head CI, but do not merge, deploy to production, or change the production DB.
  - Applied app-fix commit `e993e2393b6a3b5774afd0f85a9a881fdb2f0955` to draft PR #793. Confirmed 11 successful checks on that commit — CI, Web, publish-check, and status checks — plus 1 conditionally excluded release-artifact check. Pinned the exact-head evidence in `application-safety/ci-source-commit.json`. CI for later document updates is checked separately on that head; merge and production application remain on hold.

## 6. Progress and evidence log

| Time | Item | Actual work/evidence | Remaining condition |
|---|---|---|---|
| 2026-09-15 start | All | After the gap audit, received the user's instruction to document the remaining work and keep executing. Created a goal with no budget limit. Created a separate latest-baseline worktree. | Evidence-based execution of the TODOs below |
| 2026-09-15 | PLAN-01, PLAN-02 | Confirmed 40 document TODOs and 40 unique names. Created a separate worktree on `ef70b347`. Selectively copied only the 5 existing QA assets and preserved the original. Confirmed an active goal with no budget field. | Production baseline and follow-up verification in progress |
| 2026-09-15 | ASSET-01, ASSET-02 | Byte comparison of the 36 latest-baseline skill files before/after the move: 0 mismatches; old path removal confirmed. path+SHA manifest `e7e0bb343a8e1d874926bd3937c5c67e9e7f884c69d99d6c8f3b05364a1bffb0`. Separated the two new skills' static-extraction/execution responsibilities. | Git reflection and final gates remain separate TODOs |
| 2026-09-15 | ASSET-04, ASSET-05 in progress | Retracted the old report's exhaustive-completion claims and wrote a separate review JSON; raw JSON bytes preserved identically. A new `omp --cwd=... --skills=... --no-session -p` session resolved both `skill://` URIs to real `.agents/skills` paths. Distinguished from the standalone `omp read` empty-registry failure. | New production run linkage and generator re-run verification remain |
| 2026-09-15 | PLAN-03, INV-09, RUN-11 in progress | Confirmed differences between production v0.4.9 `e56d029e`/PG16 migration114 and baseline `ef70b347`/migration118. Added 2 TODOs for the 57 Admin/Web changed files and the production-only screens. | Verify the production vs. latest-source matrix separately |
| 2026-09-15 | PLAN-03 complete | Confirmed Principal 22/Upstream 9/Plugin 2 via the real Camofox UI and GETs. Protected read access succeeded. Did not mistake the legacy deployment's auth/session 404 for latest-identity permission evidence. Recorded version/DB/observation limits in `production-preflight.json` and the independent K8s/Thanos/Tempo investigation. | Per-request SQL/pool collection and exhaustive execution are separate TODOs |
| 2026-09-15 | MEASURE-03 scope decided | The user asked about the purpose of the new always-on app instrumentation approval request, and approval was not given. Proceed with exhaustive QA using existing observability first, then judge the minimal supplementation after confirming evidence gaps for specific real requests. | No runtime change. Record missing instrumentation as null with the cause |
| 2026-09-15 | CACHE-01 partial verification | In a networkless disposable Linux container, actually observed on an owned 64MiB file: mincore 16384 pages → per-file evict 0 → inspect 0 → warm 16384. No global cache flush or production change. | Real DB-buffer/app-cache conditions and the performance comparison not yet verified |
| 2026-09-15 | ASSET-03~05 complete | Both generator and --check produced and matched 433 rows (205 UI/149 request/115 API). Experimentally confirmed that a new file, an existing-source change, and an MD mismatch in a separate scratch each fail and succeed after recovery. Native skill discovery and old-report retraction/new IN_PROGRESS-run cross-referencing complete. | Real execution/final independent review/commits remain separate TODOs |
| 2026-09-15 | MEASURE-02 in progress | On real apps with SQLite and PG16 respectively: prepare → populated snapshot → reset → restart → proof. Fixture hash/entity counts preserved; 12×200 + intended 401 confirmed each; 2 plugin references confirmed. OAuth start URL restricted to loopback. | Real-browser mutation exhaustiveness and the external-OAuth success condition remain incomplete |
| 2026-09-15 | INV-01~06, INV-09 complete | Pinned 205 UI actions/149 atomic requests/93 source files via per-area independent source investigation plus integrated correction; current hash/route/request-mapping checks pass. Preserved the 57-file diff vs. production and the production-only 10 UI/13 API spec in deployed-delta. Corrected the earlier fake modal/paths. | Independent API/SQL semantic review and final bidirectional verdict proceed separately in INV-07/08 |
| 2026-09-15 | MEASURE-04 complete | Verified recorder 1.0.5 on real Chromium: click/independent GET/original Promise·Response identity/same-URL parallel/503/abort/SSE no-clone/privacy/overflow/unfinished-fetch drain-then-settlement preservation. Latest raw SHA `d491f9de898a4dddadc1da8d034edd92f3e9fbe701c499e352c73907d9aa28be`. | The Camofox native410 failure and missing ResourceTiming candidate/server correlation are separate limits and do not mean exhaustive production PASS |
| 2026-09-15 | INV-07, INV-08 complete | 115 API registrations↔list and 149 UI-request mappings: 0 unresolved. Reviewed 70 reads across 4 independent scopes (10/21/19/20), corrected the 20 semantic errors found, and each reviewer confirmed resolution. Fixed the missing slim-checkpoint/usage-interval SQL and the upstream full-scan mis-mapping. Verified the final 168 unique operations and both-DB SQL evidence; generator/--check passed again. | See source-semantic-review.json. Real production execution/instrumentation completion is separate; the final full-diff review also remains |
| 2026-09-15 | CACHE-01, CACHE-02 complete | Current ef70 app + isolated PG16, same synthetic 100k-decision data, concurrency 1: summary/list/detail each cold-A8/warm30/cold-B8, 138 requests total. All 48 cold runs evicted the owned relation's 341 files/37684 pages to mincore 0, and target OS/PG buffer 0 was proven right before each request. Each endpoint: 46×200 and a single canonical hash value, directly verified. | See `runs/2026-09-15/cache-experiment`. A controlled experiment on PostgreSQL target-relation cache only; no claims about pure pool wait (unobserved), protocol request-ID tracing, hardware cold, whole-production improvement, or SQLite API cache comparison |
| 2026-09-15 | CACHE-01/02 reproduction-evidence supplementation pending | The measurements, 48-run conditions, and 138 responses themselves were confirmed, but a follow-up found the executor had not preserved the helper source/hash at the time. The current helper differs due to a post-experiment safety fix, so the current hash is not recorded as the code of that time. Recovering the exact version from the original tool-authoring/modification records; correcting the checkbox from complete to pending. | No measurement tampering or estimated recovery. Completion verdict after securing the reproduction script and the period helper's provenance |
| 2026-09-15 | CACHE-01/02 reproduction-evidence supplementation complete | Reverse-applied the records of the two follow-up fixes to recover the period helper (965 lines/36278 bytes); it matches existing tool snapshot D210. Recovery SHA `6b96b33f848db01c0a293325480cb480fb0635295c27a17330a3d85f2ebcaf7b`, preserved with the reproduction script and protocol. | reproducibility.json states this is post-hoc recovery, not a full SHA recorded at experiment time. No change to measurements/raw-condition evidence |
| 2026-09-15 | AUDIT-01~03 complete | The new recent query's both-DB/HTTP regressions: 32 PASS; real candidate UI 15/15 PASS with 36 network records. Confirmed newest-200/same-ts/late-old-insert/filter/new-export query state transitions. Preserved the ef70 baseline and candidate binary/patch SHAs separately. | Approved-candidate evidence is `local-settings/audit-candidate`; Settings Apply 501 unchanged |
| 2026-09-15 | Full-verification environment defect handled | Of 1512 affected-crate runs: 1511 PASS / 1 trybuild linker failure / 9 skipped. Confirmed trybuild strips RUSTFLAGS so the user Cargo config's ld64.lld could not parse the Apple SDK. Without changing global config, re-ran that real typestate test 1/1 PASS using a scratch CARGO_HOME with native clang and the existing cache/bin links. | Not a test deletion/weakening/unmodified retry. Will re-verify the full gate after integrating the final five fixes |
| 2026-09-15 | Production-execution blockage recorded | Preserved partial denominators, 74 common observations, and 484 Keepalive direct records. Corrected the short client deadline to a censored lower bound, not a server failure. Observed one heavy-card real DOM UI request at 200/8442ms, then moved on after the poll and closed the tab. | Full UI list combinations/cursors incomplete. Camofox native410/isolated-world observation limits and the production-load safety stop are not counted as complete |
| 2026-09-15 | FIX-01~05 verification | Candidate binary SHA `f564dcf57a4c2b914b2c690b8dee4e1c68f4fa472ae680bbe599d250dcd28f65`: Upstreams required 6/6, Principals 10/10, Plugins simple-delete/cascade-delete/real orphan-GC three flows PASS. Restoring the original env-var name is separated as a pre-existing write-only contract outside the approval scope. | Isolated-candidate verification; does not mean production application or exhaustive production PASS. |
| 2026-09-15 | Candidate evidence preserved | Preserved 17 raw JSON/JSONL, provenance, and follow-up regression logs by path and SHA in `qa/admin-web/runs/2026-09-15/approved-fixes/index.json`. | Excluded credentials, browser profiles, and unreviewed screenshots. Earlier baseline FAILs are not overwritten by candidate PASSes. |
| 2026-09-15 | Response regression test fix | After the first full gate passed 1,519/1,520, fixed the assertion that rejected the approved `base_url` addition. Kept the same test and the 4 warmup-history-field non-exposure checks; confirmed focused 9/9 and the fixed full gate 1,520/1,520 PASS (exit 0). | The 9 excluded tests are not counted as run. This result is regression evidence for the pre-instrumentation candidate, not exhaustive production measurement PASS. |
| 2026-09-15 | Independent review | Audit, FIX-01~05, and the response-test fix judged correct. `after` is a lower-bound-exclusive filter and does not guarantee a forward cursor. The legacy empty label string and the v1 null representation are each preserved. | The index cost of the conditional Audit sort and the risk of a DB check error message whose current reachability is unconfirmed are left unmeasured. No migration or additional API changes. |
| 2026-09-15 | YAML serialization defect | Confirmed a defect where empty-string keys in `wire_semantics` are emitted unquoted. Byte equality from the existing `--check` alone cannot claim parse success. | After catalog/generator fixes, standard-parse verification is needed. Production entity/cursor denominators, native pointer, per-request SQL/pool evidence, and non-production OAuth success credentials are also incomplete. |
| 2026-09-15 | Isolated instrumentation verification | 19 files preserved in `instrumentation/index.json`. Candidate `132e831a…`: linked ID·SQL·acquire for 12 real requests on both DBs, 2 sensitive body/cookie/header/query-marker non-exposure checks, A/B/A 300 GETs, 12 Proxy 401/request-id controls, related library tests 191/191 PASS. | A loopback round-trip comparison on an empty list; does not mean pure CPU cost or production performance. Pure DB execution and pool wait are unmeasured. |
| 2026-09-15 | Browser linkage extension | In a strict-CSP mock, read the Server-Timing ID inside the Camofox isolated world and matched it to the two server IDs. With the user's additional approval, implemented the header carrying only the existing ID. Observed IDs for 14 UI requests on both DBs on new candidate `db564601…`. | Because the earlier PG server's limited Hub tail could not recover the full logs, those 7 requests are not counted as SQL-linked. Fixed the collection path to preserve full logs in a private file, then proceeded with new observation. |
| 2026-09-15 | Collector exaggeration prevention | On collector 1.0.7, reproduced three cases where a late identical-ID candidate, a conflicting ID, or a weak existing link was wrongly retained; after the fix, the same reproductions pass. | Real-browser verification and cache-ID-reuse handling are checked separately. Also recorded the tool phenomenon where one native click call produced two requests; do not assume UI action and HTTP request are 1:1. |
| 2026-09-15 | Canonical collector linkage verification | On fixed collector 1.0.7, linked all preserved Resource Timing IDs — 17 SQLite, 19 PostgreSQL — to private server logs stored before observation. Confirmed 6 SQL-bearing entries per DB. Preserved success/failure/limit evidence separately in `qa/admin-web/runs/2026-09-15/server-timing/index.json`. | SQLite is a separate window after sequence 106, not a recovery of the earlier 103-entry drain. The PG raw snapshot's action window is open, so full UI-oracle completion is not claimed. Pure DB/queue time and the full production matrix remain incomplete. |
| 2026-09-15 | Final code gate | Rust format, Web lint/typecheck, 72 files·699 Web tests, current inventory check, recorder syntax — all PASS. Server/SQLite library tests still 191/191 PASS after the Server-Timing addition. | Passing tests/tools does not mean full production execution or merge approval. |
| 2026-09-15 | PR #793 created and CI | Pushed `7c8b6168957bff5050d6482ae7d3c137ee7efe31` on `qa/admin-web-exhaustive-completion` and created a draft PR targeting `master`. Rust, Web, and publish-check all succeeded; confirmed 11 successful checks including status checks, plus 1 conditionally excluded release-artifact check. | This result is limited to that head. Later local QA records are separate changes and do not automatically inherit the same CI evidence. |
| 2026-09-15 | Merge hold | After presenting repository/PR/base·head/squash·admin method/branch retention/unfinished scope, the user chose `hold merge`. | No draft release, merge, auto-merge, branch deletion, or production deployment. Remaining QA work proceeds only within what is possible without separate approval. |
| 2026-09-16 | Isolated usage/error state transitions | Preserved initial/transition raw DOM/PNG/API/SQL seed and UI-request correlation evidence in the 22 files of `post-hold-usage-status/index.json`. New local-feature evidence for the previously unobserved `UPSTREAM-004`, `UPSTREAM-006`; the old rows were not overwritten. | Fixed a collection defect where the `/admin/v1`-only RT filter dropped `/admin/usage`, and verified with new observation. No production requests, app-code, or PR changes; the fixture port and browser tab were closed. |

## 7. Approval and blockage log

| Target | Specific change/work requested | Approval status | Execution status |
|---|---|---|---|
| PR #793 merge | Final request with head `7c8b6168957bff5050d6482ae7d3c137ee7efe31`, base `master`, squash/admin, no branch deletion | User chose `hold merge` | Draft kept. No merge/ready transition/auto-merge/queue/branch deletion. New explicit approval required |
| Isolated Admin-request instrumentation | Admin-only ID/span, both-DB acquire logs, necessary direct-dependency declaration, and isolated verification | User chose `approve isolated instrumentation implementation` | Only the three stated runtime files and supporting metadata changed. Proxy handling, production settings, and deployment excluded |
| Production writes or production config change | Per-target request with side effects and recovery method when needed | Not approved | No change |
| Latest Audit record omission fix | Add a recent-limited query for Admin Audit, keep the existing append-order contract, verify both DBs/handler/regressions | User chose `approve Audit error fix` in ask | Only the AUDIT-01~04 scope above may be implemented. Merge/production application not approved |
| Settings Apply support display | Expose provider capability in the draft API and allow Apply in the UI only on explicit true; distinguish unsupported/loading/error/unknown guidance | User chose `approve support-indication fix` on 2026-09-16 | API/700 Web·23 backend regressions and real file-provider UI verification complete. Existing apply/reload execution and startup-fixed policy unchanged |
| Five additional app error fixes | FIX-01~05: duplicate name save/Base URL/optional label/Router revision/Plugins delete·GC display | All five approved via user multi-select | Only the stated UI/API/both-DB/regression scopes modified. Separate instrumentation, Settings Apply, deployment, merge not approved |
| Server-Timing ID delivery | In `app.rs`, pass only the same ID as the existing x-request-id via rid.description and link it to the QA collector | User chose `approve additional isolated environment` | No new time/body/credential exposure. Production settings/deployment need separate approval |
| Base URL explicit-null removal | Distinguish omission/explicit null/URL as tri-state in Upstream update; verify both DBs, existing callsites, UI, and proxy destination | User chose `approve removal via explicit null` on 2026-09-16 | Isolated implementation/verification complete. Key/token null contract and production data/merge hold preserved |
| Three app-safety fixes | Explicit `clear_base_url`, `admin_only` before LIMIT, Audit filter/sort indexes with read/write verification | User chose `approve fixing the three issues` on 2026-09-16 | Only this scope's app/API/new migrations and isolated verification approved. Supersedes the earlier null-clearing HTTP contract. Merge/production deployment/production DB execution not approved |
| Large production PG index preparation | Before production deployment, create the same 5 indexes as a separately approved online operation and verify definitions·valid/ready | Not approved | Direct startup creation can block writes, so pre-creation and pre-approval block deployment. Only isolated rehearsal performed |
| Final PR master integration | Integrate master `b07b25ee` into PR #793, keep the new Settings file-save contract and the existing independent fixes, renumber only the unreleased index migrations | User chose `approve final PR fix` on 2026-09-17 | Verified in a separate worktree, then committed/pushed to the same draft PR. Compatibility interim release, production DDL, merge, and deployment excluded |
| PostgreSQL draft revision save | Split create/conditional update in `ConfigStore::put_config_draft` and register the existing-revision regression scenario on both DB harnesses | User chose `fix and keep verifying` on 2026-09-17 | On the same DB: pre-fix HTTP 500; post-fix revision 7→8 HTTP 200 and stale-revision 409 confirmed. No public API/schema change |

The goal is not marked complete until every TODO in this document is checked, every required evidence item is satisfied, and the work needing approval is finished.

### 7.1 Request-correlation instrumentation: confirmed causes and approval boundary

After the read-only investigation, the user explicitly approved isolated instrumentation implementation. The approved app changes are the Admin-only middleware in `cc-lb-server/src/app.rs`, `cc-lb-server/src/storage_factory.rs::open_postgres_pool`, and `cc-lb-storage-sqlite/src/lib.rs::open_sqlite`. They include the necessary direct declaration of the existing `log` dependency and verification, but no version bump. Production log-config changes/deployment and Proxy-handling changes are outside the approval scope.

- `crates/cc-lb-server/src/app.rs::admin_router` and `crates/cc-lb-admin/src/routes.rs::build_router` have no Admin-request identification span or response correlation header. The Proxy's existing `request_id_middleware` is a separate path and is not a modification target.
- The minimal HTTP proposal attaches, only to Admin paths, a server-generated bounded ID, an `x-request-id` response header, and sanitized method·route template·status·handler duration. It does not unconditionally reflect untrusted client IDs or log headers/bodies/cookies/raw URLs.
- SQLite builds its pool in `crates/cc-lb-storage-sqlite/src/lib.rs::open_sqlite`; PostgreSQL in `crates/cc-lb-server/src/storage_factory.rs::open_postgres_pool`. General acquire logs are off by default in SQLx, so changing only the log filter does not enable them.
- The stores' SQLx 0.9.0 query logs use the current span. The SQLite worker also receives the span with the command. However, `QueryLogger.elapsed` can include row-streaming, consumption, and backpressure time, so it is not recorded as pure DB execution time.
- SQLx's `acquired_after_secs` is the **full acquire time**, including ping, connection creation, auth, hooks, and retries beyond semaphore wait. No built-in hook isolating pure `pool_wait_ms` was found. Do not copy the acquire value into pool wait.
- The HTTP span and pool options above are approved for isolated implementation/verification only. Real production log filters and deployment are not executed before separate user approval. Correlation IDs, driver time, acquire time, and secret non-exposure must first be proven on isolated requests; if pure DB execution and pool wait are still absent, those fields stay blocked.
- Instrumentation CPU/memory/log-volume cost has not been measured yet. Do not assume the cost is negligible or report that attribution of the whole request is already proven.

### 7.2 Production deployment conditions for the Audit indexes

PostgreSQL's plain `CREATE INDEX` blocks writes to the table while it builds. The new migration's `lock_timeout = '1s'` limits only the wait to acquire the lock, not the lock-hold time during creation. A statement timeout does not guarantee success or zero downtime either.

Therefore, on a large production `audit_log_v1`, before deploying the new app, a separately approved operation must run `CREATE INDEX CONCURRENTLY` one statement at a time outside a transaction, with the same index names, key order, and conditions as the new migration. Do not run concurrent creation inside the app migration path or disable the timeout. After creation, verify the target table, full definitions, `indisvalid`, and `indisready`. A wrong/invalid index with only the same name is not verified by `IF NOT EXISTS` alone.

Once correct pre-creation is confirmed, the app's new migration skips index creation. This path does not mean the short locks and migration bookkeeping disappear either. The isolated 100k-row rehearsal is not proof of zero-downtime production deployment or approval for production DDL. SQLite verification ran at isolated startup with no other writer; it does not claim index creation leaves other writers unaffected.

### 7.3 Boundary of the final integration and the single-maintenance transition

- The final candidate integrates PR #793's `a9da5c3c` with master `b07b25ee`. The earlier-experimented temporary key-issuance stop, migration cap, and compatibility interim release are not included in this candidate.
- Settings keeps master's editor → draft → validate → atomic file save / TOML download contract. Saving does not change the running configuration. Activating new settings requires a process restart. The existing Apply support field and the current/schema/diff/apply/reload paths are removed from the current contract. Past execution evidence is preserved as-is.
- The PostgreSQL `0119` and SQLite `0087` history-cleanup migrations already on master are preserved byte-for-byte. Only this PR's unreleased Audit indexes move to PostgreSQL `0120`–`0124` and SQLite `0088`. Past migrations' checksums are unchanged.
- History cleanup reduces queryable JSON to revision/timestamp metadata. It does not mean secrets were physically erased from the WAL, old backups, or pre-reuse storage pages. Separate maintenance/credential rotation is not performed in this integration.
- On isolated PostgreSQL 18, built schema 114 and keys with the real old-version binary, stopped it, and started the final candidate. Confirmed schema 124, preserved existing migration checksums, and pre/post-transition proxy HTTP 200 for the same key. This is a small synthetic DB result, not evidence of production DB migration time or real deployment success.
- Production transition is a separate approval item. At check time, the active app/primary was `runbear-local`, the `runbear-operation` app had 0 replicas, and the DB was standby. Re-confirm the target, backup, and old-version shutdown at execution time. This document does not approve scale, DDL, release, or ArgoCD sync.

### 7.4 PostgreSQL draft revision defect and regression evidence

- The existing SQL's `INSERT ... SELECT ... WHERE expected_revision = 0` removed the input row even when updating an existing row. Saving a second draft with the correct revision still produced a storage conflict, and the API returned 500. This SQL was identical on pre-integration master.
- Split it into an upsert limited to revision 0 for first creation and a conditional UPDATE on a row matching the current revision for later saves. Nonexistent drafts with nonzero revision and genuinely stale revisions are still rejected.
- The existing `config_draft_optimistic_revision` scenario was not registered in the harness, so this path never ran. Registered only that scenario on PostgreSQL/SQLite and added the boundary that a nonexistent row cannot be created with a nonzero revision.
- The actual registered regression was pre-fix SQLite PASS / PostgreSQL FAIL, post-fix both PASS. On the same upgraded DB, the existing revision-7 save's 500 became 200·revision 8 after the fix, and a re-sent revision 7 was rejected with 409. Post-file-save draft cleanup, expired-draft cleanup, and browser state transitions are left in the separate verification record.

### 7.5 Final integration candidate verification

- Evidence is preserved with per-file SHAs in `qa/admin-web/runs/2026-09-17/final-cutover-integration/index.json`. Test binaries are identified by the two integration parents and the real source-diff SHA. Do not confuse this with CI after the final commit.
- Generated and reconciled the current source inventory: 426 rows, 209 UI actions, 148 atomic requests, 104 API endpoints; YAML parsing confirmed. Past production records were not changed; overall production execution status is `runtime_pending`.
- Passed Web typecheck/build and 73 files·775 tests, Biome 182 files, Rust format, full test compilation, and all-features/SQLite-only Clippy.
- Post-fix full macOS workspace: 2,361 of 2,365 PASS, 4 pre-existing native PDK SIGSEGV FAILs, 14 excluded. The same final source's 4 Linux PDK tests all PASS. The full macOS run is not reported as green.
- Confirmed real old-version-binary upgrades PostgreSQL 114→124 and SQLite 81→88, preserved existing checksums, and history metadata reduction. Verified repeated draft save/validate/download/atomic file save/post-save cleanup/expiry cleanup/restart setting transitions on both DBs over real HTTP.
- Confirmed native Settings save/download/restart banner, Base URL delete/restore, Audit admin-row rendering, Router strategy/revision round-trip, and the Plugins built-in screen. The first Settings restore report reflected only the verified draft; an independent API check caught this, file-overwrite verification was completed, and final revision 12·draft null·file/effective info were confirmed. This process is also preserved in the evidence.
- Cleaned up verification services, the PostgreSQL container, credentials/config/DB files, and temporary binaries. The existing shared DB and production environment were not changed. Native Audit observation is limited to that fixture's admin-row rendering and is not evidence of exhaustive production coverage or every filter combination.
- CI on the new head pushed to the same draft PR is checked separately. Merge, release, and production application remain on hold.

### 7.6 Production access and remaining approval boundaries re-confirmed

- On 2026-09-17 08:35–08:38 UTC, re-observed health 200 and the lists — 22 Principals (22 active), 9 Upstreams (9 active), 2 Plugins — from an authenticated browser at the original public address. Reads ran once per list, sequentially; no heavy pages, repeated polling, or production mutations were executed.
- In the same check, the `runbear-operation` app had 0 replicas/endpoints and the `runbear-local` app had 2 ready replicas/endpoints. Every platform revision in the latter's immutable image digest was `5e8f74a01c74d7e3c160dc7f7c7129bb4eaa13f9`. The public health's own Git SHA is `unknown`, so this lookup is not claimed to be tied directly to a specific Pod.
- Between the preserved production source commit `e56d029e` and `5e8f74a0`, there is no source diff in the Admin, server/src, storage-api, or either storage adapter paths. List-denominator confirmation does not replace pinning the full cursor/page/filter/poll matrix or exhaustive production instrumentation.
- The old blocker that Camofox native pointer and list access were unusable is no longer valid. The state of remaining cells without new observations is left unchanged.
- PR #793's `f3fa89bf` finished exact-head CI verification, but there is still no merge-approval answer. No draft release, merge, release, deployment, or production DDL is executed. New QA material is split onto the `qa/admin-web-limits-matrix` branch so the head presented in the approval question does not change.
- The missing non-production OAuth success account and approval to enable production instrumentation remain separate preconditions. This blockage is not extended to isolated state-transition verification.

### 7.7 Isolated Default Limits execution matrix

- The target is a single current-source `UI-SRC-D74A22B571BC` / atomic request `UI-PR-06`. Based on the new SQLite fixture's real 3-Principal list, the 8 source variant groups are expanded into 9 concrete states, each repeated 3 times. The denominator is 81 UI cells plus 81 direct PATCHes replayed from the same snapshots. It is not added into completion counts for other Principal features or all of production.
- The concrete states are: empty list, requests/input_tokens/output_tokens/total_tokens/cost_usd/concurrent, cap=0, window=1. The concurrent window uses the 60 seconds the real UI can enter. Every initial state is set to Requests/2m/17 so a real state change is observed in every cell, including the empty list.
- Between each UI cell and its direct request, the owning server is stopped and the same SQLite snapshot is restored. Right after restore, the DB SHA and target revision are confirmed. Write requests are not replayed concurrently against the same unrestored state.
- Per cell, link the native click, real DOM values/screen, Resource Timing, server request ID, SQLite stored result, and SQL logs. Expected UI values are pinned before execution and not copied from actual observations. Do not convert SQLx lifetime/acquire totals into pure DB execution/queue wait or fill missing values with 0.
- [x] Pin the isolated entity and limit-variant execution matrix
- [ ] Instrument browser state transitions for every matrix cell
- [ ] Link direct requests from the same initial state to SQL
- [x] Verify matrix completeness and evidence security, then clean up — this preserves the 81 BLOCKED keys of this partial run, checks secrets, and cleans owned resources; it is not UI completion
- [x] Verify the direct-request matrix separately from the browser
- [x] Supplement the isolated fixture full-state snapshot restore

#### Current observations and invalidation handling

- All 81 independent direct requests confirmed HTTP 200, matching target SQLite values, and revision +1. 51 SQL events were linked to each request ID. Observed latency on this fixture: median 34.624ms, p95 38.562ms, max 40.153ms. Not extrapolated to production performance or UI/direct differences.
- The first browser attempt had incomplete authentication; the next two attempts produced 3 and 2 PATCHes respectively in a single measurement window due to a stale element ref and repeated Save. All were rejected for violating the single-action initial-state comparison condition. This was not judged an app duplicate-submission defect.
- After role separation using only native MCP, the baseline and collector became ready, but Camofox's 300-second idle reaper reclaimed the tab before any action. That window had no PATCH. Since then, tab state is checked during handoff via non-mutating DOM observation, without unauthorized daemon-config changes.
- During cleanup, auxiliary state such as `storage.scheduler.sqlite` and `data/replica_id` was found remaining outside the main DB snapshot. The existing 81 direct results are kept as evidence of the target API/storage state transition, but are not claimed as a controlled comparison with identical full-runtime initial state. This gap is addressed in Fixture v2 below.
- Valid UI cells and paired comparisons are still 0. All 81 expected UI keys are preserved as BLOCKED, and independent direct PASSes were not moved into the UI completion count. Raw failures and measurements are recorded with scope separation in `qa/admin-web/runs/2026-09-17/principal-limits-partial/`.

#### Fixture v2 recovery boundary

- Only the QA helper was modified. App code, the PR #793 head, and production settings were not changed. The generated config's `runtime.data_dir` is pinned to the fixture root's absolute `data` path.
- The stopped fixture's main DB and scheduler SQLite DB are preserved via the SQLite backup API, and file/directory lists plus hashes of runtime identity/plugin state are recorded together. The absence of auxiliary DBs and runtime directories is also stored explicitly.
- Reset is applied only after verifying every source hash, path, and TOML contract. External paths, symlinks, and path duplicates are rejected; a filesystem apply failure restores the original files. If the recovery itself fails, the recovery directory is not deleted.
- Confirmed by running the real app from a different cwd that both DBs and runtime identity are created inside the owned root. Verified: both-DB/identity/file restore, extra-file removal, querying the existing 3 Principals after restart, absent-state restore, target immutability before tampering, rejection of external paths/both-direction symlinks/legacy format, and recovery plus recovery-directory preservation after single/double filesystem failures.
- Existing incomplete fixtures/snapshots are rejected with a regeneration-required error rather than disguised as version 2. Past material is preserved.
- Verification covered the two SQLite stores and the common filesystem boundary. The existing PostgreSQL dump/restore execution path is not counted as re-verified in this run, and no crash-atomic transaction between PostgreSQL and the filesystem is claimed. Evidence: `qa/admin-web/runs/2026-09-17/fixture-snapshot-boundary/verification.json`.
- After creating a new v2 fixture and confirming the fingerprint of the state where main/scheduler DBs and runtime files are restored together, the 81 expected UI cells were re-pinned. However, the browser worker did not execute despite repeated READY delivery, so it was stopped/cancelled, and the same worker registration was not recovered. The new run's UI/paired executions are also 0; this is not classified as a product error or a UI pass.
- The prepared v2 matrix is preserved as BLOCKED in `qa/admin-web/runs/2026-09-17/principal-limits-v2-preparation/preparation.json`. Owned servers, credentials, and DBs were cleaned up. Resuming requires securing a working browser workflow, then creating a new fixture and running under the same full-state contract.

#### Reproduction after native-click recovery

- Recovered the interrupted kernel controller and created a new isolated fixture. Binary SHA `afbc0a74fcfde73f374768b8465eb8b3876b74923831d91f07cc58d28b2b60bc` was reconciled against existing execution evidence. Main/scheduler DB hashes and the runtime file list were confirmed right after restore, and the 81 cells' new Principal IDs were pinned.
- The browser worker performed new-tab creation, authentication, and DOM checks. However, on the first cell, the limits did not change to empty; the existing Requests/2m/17 was saved again. It was not treated as success based on a single PATCH and revision increment alone, and the direct replay was not executed.
- The cause cannot be pinned to a stale ref. After a separate restore, a single native click was invoked via the semantic XPath of the only Edit button, and an independent passive observer saw both a trusted Edit click and, 134ms later, a trusted Save click. The worker made no explicit Remove/Save call. Whether this is an app defect is undetermined; app code was not modified.
- A separate single native click was also attempted on a simple HTML counter without the app. It failed with `native mouse move timed out after 24928ms`, and the follow-up query returned `404 Tab not found`. This experiment is not a successful independent reproduction of the duplicate click; it is evidence of a separate native-input failure.
- Evidence: `qa/admin-web/runs/2026-09-17/native-click-recovery/`. Valid UI stays 0/81, paired comparisons 0. Both owned servers were stopped and no diagnostic tab remains. Until the single-action contract of native input is secured, no production writes or matrix expansion.

#### Native-click installed-copy cause investigation and approval boundary

- In the installed Camofox 1.16.0, `server.js:3905–3922` calls `dispatchMouseSequence` when `locator.click({ timeout: 3000 })` returns a timeout, without checking whether the first attempt's input was delivered. In the service log for event request `08dc7809`, confirmed this fallback warning, the subsequent mouse sequence, and HTTP 200. Together with the evidence of two trusted clicks observed from a single call, the unsafe replay path was confirmed.
- The cause of the first locator timeout and the exact dispatch-completion point are still unknown. Do not assert navigation wait as the cause, add `noWaitAfter`, or block the second click on the QA page to force a pass. The cause of the separate counter's mouse-move timeout is also unconfirmed.
- The proposed fix registers a patch in the canonical `/etc/nix-darwin/pkgs/camofox-browser/package.nix` to remove the auto-reclick after an ambiguous timeout. No direct `/nix/store` installed-copy edits, no cc-lb app changes, no timeout extension, no synthetic-click bypass. Actual application needs separate approval covering the Camofox package build and service replacement/restart.
- Investigation evidence `native-click-recovery/root-cause.json` and `native-service-log-extract.json` is preserved. System changes and post-fix verification have not been executed yet.
- On the Camofox fix/apply approval question, the user chose **hold**. No patch authoring, system-config change, service replacement, or restart proceeds. A generic "continue" instruction is not interpreted as lifting this hold or approving system changes. The related UI matrix stays BLOCKED, and the same approval is not repeatedly requested until the user explicitly lifts the hold.

### 7.8 Additional user instruction: re-verify causes, split per-bug PRs, single QA PR

This section adds to, and does not replace, the existing goal and unfinished items. All existing completion conditions remain: exhaustive production measurement, missing-set verification of the source/execution matrix, per-request correlation analysis, and the isolated cold/warm comparison.

- The user instructed: re-check whether it is a Camofox bug, and if it is a real bug, patch it and complete the remaining verification. The §7.7 Camofox-fix hold is lifted only for this scope. Permitted: canonical patch, build, Camofox-service-only replacement/restart, and pre/post-fix verification for the confirmed cause. Unrelated system changes, production data changes, PR merges, and production deployment are not included.
- Judge the single-request duplicate click, the first locator timeout, and the separate mouse-move timeout plus tab disappearance as distinct claims. The existing logs are evidence of the unsafe replay path but do not mean every timeout's cause is known.
- Distinguish the actual installed copy, its release, and current upstream. Investigate actually supported paths — ref/CSS/XPath native click, low-level mouse, keyboard activation, DOM synthetic — and compare by independent event counts, targets, and real saved state. Do not record unsupported methods as executed. Do not accept the synthetic comparison group's success as native UI verification.
- Confirmed cc-lb bugs are split into one PR per root cause. Follow-up safety hardening of the same cause is bundled into that PR; separate causes are split. Each PR includes the cause, minimal fix, and direct regression evidence.
- QA skills, list, generator, fixtures, collector, Admin instrumentation, reports, and evidence are collected into one QA PR. Classify every change in PR #793 and `qa/admin-web-limits-matrix`; do not duplicate changes already on master into the new PR.
- Existing branches and user changes are preserved. State each split PR's base/head and required dependencies, and reconcile the actual diff against verification. Do not automatically close, merge, or deploy existing PRs.

Additional completion conditions:

- [x] Record each click failure's verdict, reproduction environment, evidence grade, and remaining uncertainty
- [x] Compare independent input/state evidence per supported click method
- [x] For the confirmed Camofox defect, verify identical-condition pre-fix failure/post-fix success and the normal path
- [x] A table of each confirmed cc-lb bug's PR and complete change ownership
- [ ] A single PR containing every QA-related improvement, with execution evidence
- [ ] Run the previously incomplete matrix on the recovered browser and satisfy the original full completion conditions

### 7.9 Camofox fix verification and per-bug PR split results

- The canonical Camofox patch blocks re-dispatch after a timeout and changes click/follow-up handling to share time within the existing 30-second handler budget. Only the related package was built and the Camofox runtime replaced; no unrelated system settings were applied.
- On an independent counter/button-swap probe, all 12 runs — 3 each for CSS/ref — observed 1 trusted action per native call. Pre-fix counter CSS/ref, 3 each, produced 2 actions per call. The real extraction function's 13-condition smoke passed after the fix; the same harness failed on the duplicate-click condition before the fix.
- Six additional conditions confirmed: 2 downloads each with 1 dispatch/download, 2 forty-second navigation holds each with an ~25.5s bounded timeout and 1 dispatch, and 2 disabled-button cases each with a bounded timeout and 0 dispatches. The cause of the download tool's ~38–39s total latency was not isolated and is not treated as app-performance measurement.
- Results independently aggregated from raw observations and the patch identifier are preserved in `qa/admin-web/runs/2026-09-17-native-click-recovery/budget-validated/verification.json`. No claim that all timeout causes are resolved or that an upstream release incorporates the fix.
- Per-bug Draft PRs: #794 Base URL response, #795 Base URL clearing, #796 Audit query, #797 key label, #798 draft revision, #799 Router revision, #800 plugin delete, #801 plugin GC guidance, #802 rename duplicate submission. Each PR is a stack based on the previous bug branch; exact head/base are recorded in `pr-split-ledger.json` in the same directory.
- On the final accumulated source: frontend typecheck/lint/build, 73 files·775 tests, Rust format, and 54/54 selected regressions including real PostgreSQL/SQLite passed. The 907 tests excluded from the selection are not counted as executed passes. Each intermediate PR head's independent CI is distinguished from accumulated-source verification.
- For the isolated Default Limits 81 cells, a new execution directory was prepared with the same binary fingerprint and full-state snapshot. The earlier invalid UI evidence is preserved, and nothing is marked complete until the new browser execution and paired verification finish. The approval boundaries for production instrumentation application, a real OAuth account, merge, and deployment are unchanged.
