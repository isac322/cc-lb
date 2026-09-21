# Admin Web Runtime Progress — updated 2026-09-16; baseline 2026-09-15

> **Current status: INCOMPLETE.** This is not a completion report but a progress ledger linking the frozen source/evidence to the current executed, unexecuted, blocked, and failed sets. `runtime_complete=false`, `overall_pass=false`.

The existing normalized rows, classification coefficients, and 135 unresolved items in this document are a **past observation snapshot**. Later limited-resolution evidence is recorded separately in the `Follow-up approval/verification overlay` below and in `post_hold_resolution_overlay` in the JSON. Past rows are not overwritten with new results or converted into production execution rates.

## Verdict summary

- Current baseline: `ef70b347`
- Production deployed: `e56d029ea8641827d420c582c5f41a72ab9e2182`
- Audit candidate: `ef70b34+uncommitted-audit-patch-…` — a separate overlay for the approved latest-200 scope; it does not delete the baseline FAIL.
- Approved fix candidate: `ef70b347` + uncommitted patch `7ef93773…` (binary `f564dcf5…`) — a separate `approved_fix_overlay`; it does not change the baseline denominator or cells.
- The earlier input freeze fixed 707 inputs (browser profile, credential, and cache excluded; 0 missing and 0 JSON/JSONL parse errors at the time). This is not the total file count of the current run directory. The 6 follow-up evidence items are fixed separately by path and SHA-256 in the overlay.
- The canonical inventory was regenerated against the approved uncommitted candidate (standard parser Ruby/Psych passed, `approved-source-catalog-check.json`). The current denominators are 433 rows / 170 storage operations / 422 production matrix rows; the normalized cell figures below preserve their historical values.
- Because required SQL/pool correlation, the native keepalive pointer, and the production UI matrix are incomplete, no overall PASS is claimed.

## Counting basis: source versus execution

| Basis | Count | Meaning |
|---|---:|---|
| Current source action | 205 | Raw array length of `ui.json.items` |
| Historical base variant | 582 | Variant label count in the original snapshot. The current source has 595 labels; neither is a runtime cell count |
| UI request occurrence | 149 | Raw sum of per-action `requests` |
| API endpoint row | 115 | api-read 70 + api-write 45 |
| Canonical inventory row | 433 | ui_action 215 + network_request 162 + backend_endpoint 56 |
| Production matrix row | 422 | ui_action 208 + network_request 158 + backend_endpoint 56 |
| All input JSONL rows | 1,650 | Includes candidate subset/network duplicate files |
| Primary action/runtime row | 1,563 | Raw sum of the 7 main ledgers |
| Normalized physical cell | 1,511 | UI 1,009 + direct API 499 + supplemental 3 |
| Keepalive direct row | 484 | UI executions 0; 1 separate heavy-card UI artifact |
| Approved-candidate flow | 19 | upstreams 6 required PASS + principals 10 PASS + plugins 3 PASS (overlay, not baseline cells; a count of isolated functional checks, not browser interactions) |

The 433 source rows, 205 source actions, 422 production rows, and 484 keepalive direct rows were not counted as UI executions.

## Required classification coefficients

| Classification | Count |
|---|---:|
| notexecuted | 267 |
| safetyblocked | 258 |
| productfail | 11 |
| qaoraclecorrected final cell | 35 |
| qaoraclecorrected historical attempt | 90 |
| nativepointerblocked | 4 |
| sqlcorrelationblocked | 502 |
| measurementblocked | 6 |
| right-censored lower bound | 6 |
| behaviorverified | 1,099 |
| artifactoraclepass within declared cell oracle | 826 |

`behaviorverified` is an observed behavior-oracle pass. `artifactoraclepass` is a **functional verification pass declared by the raw artifact**, not a PASS measured across all UI/direct/SQL/pool layers.

## Full measurement gate

| Status | Cells |
|---|---:|
| not_evaluated | 1,006 |
| blocked | 505 |

This is the UI interaction + direct API + SQL + pool measurement gate. Cells without SQL/pool correlation evidence are `blocked`; cells without other measurement evidence are `not_evaluated`. No measurement was filled with 0, and no cell was counted as PASS. Server-timing evidence measured the canonical RID join, but pure DB execution and pure pool queue wait remain unmeasured.

## Runtime enum denominators

| Enum | Production Camofox/Firefox | Local Playwright/Chromium | Verdict |
|---|---:|---:|---|
| Locale (including auto) | 147 | 126 | The 21 source candidates unsupported by Chromium are not product FAILs. The local 128 cells are 126 options + 2 denominator/filter observations. |
| Timezone (including auto) | 446 | 419 | The actual supported enums differ per runtime. The local 420 cells are 419 options + 1 denominator observation. |

## Current source progress by area

| Area | source | reviewed | UI executed | direct API | partial | not executed | artifact-oracle-pass |
|---|---:|---:|---:|---:|---:|---:|---:|
| Audit | 13 | 13 | 12 | 0 | 1 | 1 | 11 |
| Global | 20 | 8 | 3 | 0 | 0 | 17 | 3 |
| Logs | 31 | 31 | 0 | 0 | 0 | 31 | 0 |
| Overview | 21 | 7 | 0 | 0 | 0 | 21 | 0 |
| Plugins | 20 | 20 | 20 | 0 | 2 | 0 | 18 |
| Principals | 40 | 40 | 31 | 0 | 31 | 9 | 0 |
| Settings | 16 | 16 | 16 | 0 | 4 | 0 | 12 |
| Shared Request Tables | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| Upstreams | 43 | 43 | 34 | 0 | 22 | 9 | 12 |

## Production matrix progress by area

| Area | source | reviewed | UI executed | direct API | partial | not executed | artifact-oracle-pass |
|---|---:|---:|---:|---:|---:|---:|---:|
| Audit | 12 | 12 | 0 | 0 | 0 | 12 | 0 |
| Credentials | 3 | 3 | 1 | 0 | 1 | 2 | 0 |
| Global | 18 | 15 | 13 | 0 | 13 | 5 | 0 |
| Logs | 31 | 0 | 0 | 0 | 0 | 31 | 0 |
| Overview | 21 | 21 | 14 | 0 | 14 | 7 | 0 |
| Plugins | 22 | 0 | 0 | 0 | 0 | 22 | 0 |
| Principals | 40 | 5 | 1 | 5 | 1 | 39 | 0 |
| Settings | 16 | 16 | 10 | 0 | 10 | 6 | 0 |
| Status | 2 | 2 | 1 | 0 | 1 | 1 | 0 |
| Upstreams | 43 | 0 | 0 | 0 | 0 | 43 | 0 |

Production UI executed counts only the UI stage. The 230 keepalive direct GET successes are direct API evidence.

## Separating the Audit baseline and candidate

- Baseline `local-settings/actions.jsonl` FAIL rows 595, 599, 651 are preserved.
- The candidate `actions-all.jsonl` 15 PASSes are 8 direct API + 7 UI.
- The candidate is evidence resolving latest-200, same-timestamp ordering, delayed-old exclusion/time filter, and export refresh/reload visibility.
- Because the candidate source version differs from the baseline, baseline FAILs are not overwritten with PASS.

## Approved fix overlay (baseline preserved)

- Candidate: `ef70b347` + uncommitted patch `7ef93773…`, binary `f564dcf5…`. Main copied the 15 reviewed JSON/JSONL files + provenance + focused regression byte-identically to `qa/admin-web/runs/2026-09-15/approved-fixes/` (hash comparison complete). profiles/credentials/unreviewed PNGs were not copied.
- Flow: upstreams required 6/6 PASS, principals 10/10 PASS, plugins 3/3 PASS (+gc-storage proof). These figures are isolated functional checks, not full runtime/measurement PASS. The 10 principals include 1 direct API-only and 1 proxy-only.
- Baseline FAILs are not deleted; resolution evidence is linked separately via `approved_fix_overlay.resolved_findings`.

| Baseline task | Source | Resolving candidate flow |
|---|---|---|
| UPSTREAM-023 | `components/upstreams/InlineNameEditor.tsx#InlineNameEditor:save` | rename_enter_exactly_one_put_no_followup_409, rename_blur_exactly_one_put, rename_escape_no_request, rename_real_stale_409_preserved |
| UPSTREAM-029 | `components/upstreams/SettingsCard.tsx#SettingsCard:save` | settings_baseurl_put_get_reload_persists |
| UPSTREAM-031 | `components/upstreams/SettingsCard.tsx#SettingsCard:save` | settings_true_null_default_and_writeonly_literal |
| PRINCIPAL-02 | `crates/cc-lb-admin/web/src/routes/principals.tsx#ApiKeysCard:issue_key` | approved-candidate-001 blank label UI issue exact-one, approved-candidate-005 non-empty NUL label rejected without key, approved-candidate-omitted-label-direct omitted label API parity |
| PLUGIN-1 | `PluginCatalog.tsx#PluginCatalog:run_gc` | gc.json, gc-storage.json |
| PLUGIN-5 | `PluginDeleteDialog.tsx#PluginDeleteDialog:confirm_delete` | simple.json, cascade.json |

- `UPSTREAM-030` (env var key): the candidate exploratory FAIL is `SUPPORTED_LIMITATION/NOT_IN_APPROVED_FIX` — the env name resolves to encryption key material at write time and GET does not return it, so reload provenance cannot be reconstructed. The secret remains blank/write-only. It is not an extra FAIL, and the baseline product_fail is also preserved.
- The `settings_null_default_get_reload` exploratory FAIL is a different leg from the required PASS; it is recorded as a triage target, not counted as a product FAIL.
- New coverage (not baseline FAIL resolution): principals blank-label list/proxy/revoke, 2 terminal refetch locks, concurrent 409 preserved.
- Regression: previous full test 1519/1520 fail → warmup test fix → focused detail-contract 9/9 PASS → whole backend gate **1520/1520 PASS** (13 slow, 9 skipped, exit 0; `cc-lb-admin-web-qa-evidence/approved-fixes-full-backend-gate.txt`). Only the backend test gate is complete; the full runtime QA gate remains incomplete. Native RECOVERED_LOCAL and production Plugins evidence are in progress.

## Keepalive progress and limitations

- 484 physical rows = 462 list + 22 summary.
- list: 209 direct successes, 5 client aborts, 248 not executed due to safety-stop.
- summary: 21 two-cycle direct successes, 1 client abort.
- The 230 direct successes and 6 aborts are not UI executions.
- The 1 heavy-card DOM UI artifact observed an initial 8,442ms HTTP 200 and 8 automatic polls.
- The 2s/3s/10s client deadlines are right-censored lower bounds, not HTTP server FAILs.
- The native pointer was blocked by Camofox 410 at the time. Native capability has since recovered in other limited observations, but the full native UI/cursor matrix for Keepalive and production SQL/pool correlation evidence have not yet been obtained.

## Unfinished work list

The **135 tasks** from the past snapshot are preserved. The classification below is not a recount of currently remaining work. `→ approved-fix` and the follow-up overlay only link new evidence for that environment/variant and do not mean full production completion.

### blocked (65)

- **PRINCIPAL-04** — `crates/cc-lb-admin/web/src/routes/principals.tsx#RouterSlotEditor:add_filter`; variant=`mutation error`; Variant was not observed in this isolated browser run.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[4]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#95`
- **PRINCIPAL-07** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ObservabilityHookEditor:add`; variant=`mutation conflict/error`; Variant was not observed in this isolated browser run.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[7]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#102`
- **SETTINGS-636** — `settings.tsx#ConfigDraftSection:apply_draft`; variant=`conflict / validation stale`; File-backed serve returns 501 apply unavailable before revision-conflict semantics; no dynamic config implementation or generation manipulation was introduced.; evidence: `cc-lb-admin-web-qa-evidence/local-settings/actions.jsonl#647`
- **SETTINGS-637** — `settings.tsx#ConfigHistorySection:mount`; variant=`populated rows after applied revision`; No history row can be created through the actual file-backed serve path because apply returns 501; empty history was verified separately.; evidence: `cc-lb-admin-web-qa-evidence/local-settings/actions.jsonl#648`
- **UPSTREAM-001** — `routes/upstreams.tsx#UpstreamsPage:mount`; variant=`Loading skeleton`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[1]`
- **UPSTREAM-002** — `routes/upstreams.tsx#UpstreamsPage:quotaLatest`; variant=`Loading skeleton mini-meters`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[4]`
- **UPSTREAM-003** — `routes/upstreams.tsx#UpstreamsPage:listUsage`; variant=`Loading skeleton`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[6]`
- **UPSTREAM-004** — `routes/upstreams.tsx#UpstreamsPage:listUsage`; variant=`Usage text ($cost · tokens)`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[7]`
- **UPSTREAM-005** — `routes/upstreams.tsx#UpstreamsPage:status`; variant=`Loading skeleton dot`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[9]`
- **UPSTREAM-006** — `routes/upstreams.tsx#UpstreamsPage:status`; variant=`Error (red with apply error tooltip)`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[11]`
- **UPSTREAM-007** — `routes/upstreams.tsx#DetailView:subscriptionMetadata`; variant=`Loading skeleton strip`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[15]`
- **UPSTREAM-008** — `routes/upstreams.tsx#DetailView:subscriptionMetadata`; variant=`Populated metadata strip (Plan, Rate, Extra Usage, Account, Org, Role, Seat, Subscribed, Billing)`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#20`
- **UPSTREAM-009** — `routes/upstreams.tsx#DetailView:subscriptionMetadata`; variant=`Trial / Payment / Promotional Credit banners`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#21`
- **UPSTREAM-010** — `routes/upstreams.tsx#DetailView:refreshMetadata`; variant=`Idle`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[18]`
- **UPSTREAM-011** — `routes/upstreams.tsx#DetailView:refreshMetadata`; variant=`Pending (spinning icon)`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[19]`
- **UPSTREAM-012** — `routes/upstreams.tsx#DetailView:refreshMetadata`; variant=`Success toast`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#23`
- **UPSTREAM-013** — `routes/upstreams.tsx#DetailView:refreshMetadata`; variant=`Error toast`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#140`
- **UPSTREAM-014** — `routes/upstreams.tsx#DetailView:quotaSeries`; variant=`Loading skeleton chart`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[22]`
- **UPSTREAM-015** — `routes/upstreams.tsx#DetailView:quotaSeries`; variant=`Multi-series AreaChart with step interpolation and marker lines`; Fixture intentionally has no provider quota samples; empty variant is the safe local state.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#25`
- **UPSTREAM-016** — `routes/upstreams.tsx#DetailView:quotaAnalysis`; variant=`Loading burn skeletons`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[25]`
- **UPSTREAM-017** — `routes/upstreams.tsx#DetailView:quotaAnalysis`; variant=`Rendered burn rates and ETA to limit`; No genuine quota samples exist.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#26`
- **UPSTREAM-018** — `routes/upstreams.tsx#DetailView:quotaAnalysis`; variant=`Quota deficit warning card`; No genuine quota samples exist.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#27`
- **UPSTREAM-019** — `routes/upstreams.tsx#DetailView:quotaAnalysis`; variant=`Analysis caveats box`; No genuine quota samples exist.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#28`
- **UPSTREAM-020** — `routes/upstreams.tsx#DetailView:rangeToggle`; variant=`7d (1800s buckets)`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#32`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#139`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#142`
- **UPSTREAM-021** — `routes/upstreams.tsx#DetailView:isolateSeries`; variant=`Isolated (other series hidden, clicked badge full opacity)`; No series means the legend isolation controls are correctly absent.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#33`
- **UPSTREAM-022** — `routes/upstreams.tsx#DetailView:isolateSeries`; variant=`Reset/None (all series visible)`; No series means the legend isolation controls are correctly absent.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#34`
- **UPSTREAM-024** — `components/upstreams/InlineNameEditor.tsx#InlineNameEditor:save`; variant=`Pending spinner`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[37]`
- **UPSTREAM-025** — `components/upstreams/InlineNameEditor.tsx#InlineNameEditor:save`; variant=`Error toast (reverts)`; Duplicate error is source-reachable, but consuming another fixture name would couple this rename leg; covered by API validation inventory, not replayed in this UI mutation sequence.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#8`
- **UPSTREAM-026** — `routes/upstreams.tsx#DetailView:toggleEnabled`; variant=`Enabling (synchronizes warmup_enabled if divergent)`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#10`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#67`
- **UPSTREAM-027** — `routes/upstreams.tsx#DetailView:toggleEnabled`; variant=`Disabling (synchronizes warmup_enabled if divergent)`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#9`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#66`
- **UPSTREAM-028** — `routes/upstreams.tsx#DetailView:toggleEnabled`; variant=`Pending spinner with label 'Enabling...' / 'Disabling...'`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[41]`
- **UPSTREAM-033** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:toggle`; variant=`Stale revision (409/412) error banner`; No second writer was introduced into the isolated UI leg.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#41`
- **UPSTREAM-034** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:fireNow`; variant=`Success (fired: true) -> 1s cooldown + toast`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#48`
- **UPSTREAM-035** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:fireNow`; variant=`Lease held (fired: false, reason: 'lease_held') -> 10s auto-dismissing warning panel`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#49`
- **UPSTREAM-036** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:openHistory`; variant=`From Last run badge`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#105`
- **UPSTREAM-037** — `routes/upstreams.tsx#DetailView:startOAuth`; variant=`Token Reconnect`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[62]`
- **UPSTREAM-038** — `routes/upstreams.tsx#DetailView:completeOAuth`; variant=`Success (closes modal, refetches status and metadata)`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[64]`
- **UPSTREAM-039** — `routes/upstreams.tsx#CreateUpstreamModal:submitNonOauth`; variant=`Environment variable name`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#118`
- **UPSTREAM-040** — `routes/upstreams.tsx#CreateUpstreamModal:startOauthDraft`; variant=`Error (inline error banner)`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[72]`
- **UPSTREAM-041** — `routes/upstreams.tsx#CreateUpstreamModal:verifyOauthCode`; variant=`Valid code (advances to oauth_confirm step)`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[73]`
- **UPSTREAM-042** — `routes/upstreams.tsx#CreateUpstreamModal:submitOauthConfirm`; variant=`Save upstream`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[75]`
- **UPSTREAM-043** — `routes/upstreams.tsx#DetailView:upstreamOAuthStatus`; variant=`Loading skeleton grid`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[76]`
- **UPSTREAM-044** — `routes/upstreams.tsx#DetailView:upstreamOAuthStatus`; variant=`Connected (Bound on this upstream with expiration, refresh token, and scopes)`; Requires a genuine non-production OAuth credential and fixed api.anthropic.com provider metadata path; synthetic token/metadata was deliberately not treated as success.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[78]`
- **UPSTREAM-045** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:summary`; variant=`Pending skeleton`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[79]`
- **UPSTREAM-046** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:summary`; variant=`Degraded (warn)`; History variants are covered in the drawer; this summary run seeds latest success only.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#36`
- **UPSTREAM-047** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:summary`; variant=`Down (danger)`; History variants are covered in the drawer; this summary run seeds latest success only.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#37`
- **UPSTREAM-048** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:summary`; variant=`Idle / Paused (neutral)`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[83]`
- **UPSTREAM-049** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:pluginRegistry`; variant=`Loading skeleton`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[84]`
- **UPSTREAM-050** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:pluginRegistry`; variant=`No plugins available link`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[85]`
- **UPSTREAM-051** — `routes/upstreams.tsx#DetailView:recentRequests`; variant=`Loading skeleton`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[87]`
- **UPSTREAM-052** — `routes/upstreams.tsx#DetailView:recentRequests`; variant=`Populated recent 5 requests table`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[89]`
- **UPSTREAM-053** — `components/upstreams/warmup/WarmupHistoryDrawer.tsx#WarmupHistoryDrawer:loadOlder`; variant=`Idle`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[95]`
- **UPSTREAM-054** — `routes/upstreams.tsx#DetailView:metadataStripExpansion`; variant=`Collapsed (+N more)`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[104]`
- **UPSTREAM-055** — `routes/upstreams.tsx#DetailView:metadataStripExpansion`; variant=`Expanded (Less)`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[105]`
- **UPSTREAM-056** — `components/upstreams/warmup/WarmupHistoryDrawer.tsx#AttemptDetail:close`; variant=`Close detail`; Browser replay did not reach a stable oracle for this variant; the failed attempt remains in actions.jsonl and is not treated as a product failure.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#63`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#87`
- **UPSTREAM-057** — `components/upstreams/warmup/WarmupHistoryDrawer.tsx#WarmupHistoryDrawer:close`; variant=`Backdrop click`; Finite source variant was not stably exercised in this local run.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[109]`
- **PRODUCTION-GLOBAL-05** — `supplemental/non-matrix`; No auth verification error state occurred, and outage injection is outside production read-only scope.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#5`
- **PRODUCTION-GLOBAL-07** — `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx#Topbar:open_mobile_menu`; Viewport stayed 1680x997 after window.resizeTo; Camofox surface exposes no resize control.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#9`
- **PRODUCTION-GLOBAL-10** — `crates/cc-lb-admin/web/src/routes/index.tsx#ValueTile:sparkline_hover`; Synthetic DOM hover reached the chart application, but no visible tooltip oracle was exposed; native Camofox pointer action times out.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#27`
- **PRODUCTION-GLOBAL-11** — `crates/cc-lb-admin/web/src/routes/index.tsx#PrincipalCostMeter:popover_hover`; Focus/mouseenter produced ambiguous popup state without the expected breakdown text; native pointer unavailable.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#28`
- **PRODUCTION-GLOBAL-12** — `crates/cc-lb-admin/web/src/routes/index.tsx#PoolQuotaStackedBar:popover_open`; Desktop click is not the hover contract and synthetic clicks did not expose the expected Util/Weight/Impact popover.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#29`
- **PRODUCTION-GLOBAL-13** — `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts#connect:delta_backfill`; No natural reconnect gap with a prior cursor occurred; outage injection was not performed.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#33`
- **PRODUCTION-GLOBAL-14** — `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`; Permanent SSE failure state did not occur; failure injection is outside this production subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#34`
- **PRODUCTION-GLOBAL-15** — `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`; Permanent SSE failure banner was not present.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#35`
- **PRODUCTION-GLOBAL-22** — `settings.tsx#ConfigDraftSection:retry_editor`; Draft/current GETs succeeded; retry banner was not present.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#52`

### measurementblocked (1)

- **KEEPALIVE-RIGHT-CENSORED** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` plus 1 more; Five list requests and one summary request hit client-imposed deadlines. They are right-censored lower bounds, not HTTP server failures.; evidence: `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`, `cc-lb-admin-web-qa-evidence/production-heavy-principal-card.json`

### nativepointerblocked (1)

- **KEEPALIVE-NATIVE-POINTER** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` plus 1 more; Camofox native pointer interaction returned 410. One DOM-dispatched heavy-card UI artifact verified behavior but does not replace native-pointer coverage.; evidence: `cc-lb-admin-web-qa-evidence/production-heavy-principal-card.json`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime-summary.json`

### notexecuted (37)

- **PLUGIN-1** — `PluginsPage.tsx#PluginsPage:mount`; variant=`Registry GET backend 500 global error toast`; No public isolated endpoint can deterministically produce registry 500; database corruption and route mocking were prohibited.; evidence: `cc-lb-admin-web-qa-evidence/local-plugins/summary.json#remaining_scope[1]`
- **PLUGIN-2** — `PluginDeleteDialog.tsx#PluginDeleteDialog:references_query`; variant=`References GET backend error warning`; No public isolated endpoint can produce references 500 without corruption; 404-after-delete is not equivalent.; evidence: `cc-lb-admin-web-qa-evidence/local-plugins/summary.json#remaining_scope[2]`
- **PRINCIPAL-05** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ShapeSlotEditor:select_shape`; variant=`retry on 409 conflict/slot_singleton`; Would require an intentional invariant violation or a precisely timed competing chain write; no database corruption/fault injection was introduced.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[5]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#98`
- **PRINCIPAL-06** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ShapeSlotEditor:select_shape`; variant=`multiple shape entries detected invariant guard`; Would require an intentional invariant violation or a precisely timed competing chain write; no database corruption/fault injection was introduced.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[6]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#99`
- **PRODUCTION-GLOBAL-16** — `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:copy_id`; Clipboard copy variants would place operational identifiers/raw event JSON outside the evidence file.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#39`
- **PRODUCTION-GLOBAL-23** — `settings.tsx#SettingsPage:download_export`; GET export would create a local configuration artifact; no sensitive export was downloaded in this subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#56`
- **PRODUCTION-GLOBAL-24** — `audit.tsx#AuditPage:mount`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#57`
- **PRODUCTION-GLOBAL-25** — `audit.tsx#AuditPage:refresh`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#58`
- **PRODUCTION-GLOBAL-26** — `audit.tsx#AuditPage:filter_principal`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#59`
- **PRODUCTION-GLOBAL-27** — `audit.tsx#TimeRangeBounds:type_start_date`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#60`
- **PRODUCTION-GLOBAL-28** — `audit.tsx#TimeRangeBounds:type_end_date`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#61`
- **PRODUCTION-GLOBAL-29** — `audit.tsx#CalendarPopover:pick_start_date`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#62`
- **PRODUCTION-GLOBAL-30** — `audit.tsx#CalendarPopover:pick_end_date`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#63`
- **PRODUCTION-GLOBAL-31** — `audit.tsx#TimeRangeBounds:apply_bounds`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#64`
- **PRODUCTION-GLOBAL-32** — `audit.tsx#AuditPage:clear_filters`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#65`
- **PRODUCTION-GLOBAL-33** — `audit.tsx#AuditPage:empty_state_clear`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#66`
- **PRODUCTION-GLOBAL-34** — `audit.tsx#AuditPage:click_row`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#67`
- **PRODUCTION-GLOBAL-35** — `audit.tsx#AuditPage:close_detail_modal`; Audit actions are outside the currently requested Global/Overview/Settings/Status/Credentials subset.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#68`
- **KEEPALIVE-UI-LIST-MATRIX** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:paginate`; The 462 list cells are direct GET denominator observations, not UI executions; all 462 UI list interactions remain unexecuted.; evidence: `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime-summary.json`
- **KEEPALIVE-UI-SUMMARY-MATRIX** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`; Twenty-two summary principal cells were measured by direct reads. Exactly one separate DOM-dispatched UI artifact exists, leaving 21 summary UI cells unexecuted.; evidence: `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`, `cc-lb-admin-web-qa-evidence/production-heavy-principal-card.json`
- **CURRENT-NOT-EXECUTED-AUDIT** — `crates/cc-lb-admin/web/src/routes/audit.tsx#AuditPage:actor_metadata`; No executed current-baseline runtime cell exists for 1 Audit source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-GLOBAL** — `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx#AuthRequiredGate:external_auth_retry`, `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx#handleSubmit:validation_empty`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:close`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:keyboard_open` plus 13 more; No executed current-baseline runtime cell exists for 17 Global source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-LOGS** — `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `components/ui/LogsPagination.tsx#LogsPagination:next_page`, `components/ui/LogsPagination.tsx#LogsPagination:prev_page` plus 27 more; No executed current-baseline runtime cell exists for 31 Logs source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-OVERVIEW** — `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:close`, `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:copy_id` plus 17 more; No executed current-baseline runtime cell exists for 21 Overview source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-PRINCIPALS** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:toggle_enabled`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter` plus 5 more; No executed current-baseline runtime cell exists for 9 Principals source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-SHARED-REQUEST-TABLES** — `crates/cc-lb-admin/web/src/components/ui/latency/LatencyCell.tsx#LatencyCell:responsibility_breakdown`; No executed current-baseline runtime cell exists for 1 Shared Request Tables source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-UPSTREAMS** — `components/upstreams/warmup/WarmupHistoryDrawer.tsx#AttemptDetail:close`, `components/upstreams/warmup/parts/WarmupConfigModal.tsx#WarmupConfigModal:copy`, `routes/upstreams.tsx#CreateUpstreamModal:submitOauthConfirm`, `routes/upstreams.tsx#DetailView:isolateSeries` plus 5 more; No executed current-baseline runtime cell exists for 9 Upstreams source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **PRODUCTION-UI-NOT-EXECUTED-AUDIT** — `audit.tsx#AuditPage:clear_filters`, `audit.tsx#AuditPage:click_row`, `audit.tsx#AuditPage:close_detail_modal`, `audit.tsx#AuditPage:empty_state_clear` plus 8 more; No UI execution exists for 12 production-matrix Audit action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-CREDENTIALS** — `crates/cc-lb-admin/web/src/routes/credentials.tsx#CredentialsPage:revoke`, `crates/cc-lb-admin/web/src/routes/credentials.tsx#CredentialsPage:rotate`; No UI execution exists for 2 production-matrix Credentials action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-GLOBAL** — `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx#handleSubmit:validation_empty`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:goto_status`, `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx#Topbar:open_mobile_menu`, `crates/cc-lb-admin/web/src/components/layout/Sidebar.tsx#Sidebar:link_credentials` plus 1 more; No UI execution exists for 5 production-matrix Global action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-LOGS** — `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `components/ui/LogsPagination.tsx#LogsPagination:next_page`, `components/ui/LogsPagination.tsx#LogsPagination:prev_page` plus 27 more; No UI execution exists for 31 production-matrix Logs action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-OVERVIEW** — `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:copy_id`, `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts#connect:delta_backfill` plus 3 more; No UI execution exists for 7 production-matrix Overview action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-PLUGINS** — `PluginCatalog.tsx#PluginCatalog:copy_sha256`, `PluginCatalog.tsx#PluginCatalog:inspect_click`, `PluginCatalog.tsx#PluginCatalog:open_delete_dialog`, `PluginCatalog.tsx#PluginCatalog:run_gc` plus 18 more; No UI execution exists for 22 production-matrix Plugins action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-PRINCIPALS** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:toggle_enabled`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` plus 35 more; No UI execution exists for 39 production-matrix Principals action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-SETTINGS** — `settings.tsx#ConfigDraftSection:apply_draft`, `settings.tsx#ConfigDraftSection:reload_daemon`, `settings.tsx#ConfigDraftSection:retry_editor`, `settings.tsx#ConfigDraftSection:save_draft` plus 2 more; No UI execution exists for 6 production-matrix Settings action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-STATUS** — `crates/cc-lb-admin/web/src/routes/status.tsx#StatusPage:toggle_killswitch`; No UI execution exists for 1 production-matrix Status action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-UPSTREAMS** — `components/upstreams/ApiUsageCard.tsx#ApiUsageCard:metricToggle`, `components/upstreams/ApiUsageCard.tsx#ApiUsageCard:rangeToggle`, `components/upstreams/InlineNameEditor.tsx#InlineNameEditor:save`, `components/upstreams/SettingsCard.tsx#SettingsCard:save` plus 39 more; No UI execution exists for 43 production-matrix Upstreams action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`

### partial (5)

- **PLUGIN-2** — `crates/cc-lb-admin/web/src/components/plugins/PluginDetail.tsx#PluginDetail:references_query`; variant=`Loading skeleton transitions to two references`; Two real 800 ms delayed-reference legs reached the final two-item list, but the transient source skeleton was not exposed in the Playwright DOM observation window.; evidence: `cc-lb-admin-web-qa-evidence/local-plugins/actions.jsonl#43`
- **PRODUCTION-GLOBAL-06** — `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx#Topbar:health_poll`; One automatic 200 poll observed in a 16.5s window; two-poll interval delta not yet captured.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#6`
- **PRODUCTION-GLOBAL-08** — `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:select_page_or_action`; Safe page selection verified; create-upstream/create-principal/plugin-upload action variants excluded from production.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#17`
- **PRODUCTION-GLOBAL-09** — `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:close`; Escape and Close button verified; backdrop click not verified.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#19`
- **PRODUCTION-GLOBAL-17** — `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:close`; The close action was only partially observed; the expected close variants were not all verified.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#40`

### product_fail (10)

- **PLUGIN-5** — `PluginDeleteDialog.tsx#PluginDeleteDialog:confirm_delete`; variant=`Simple delete success toast observation window`; Source declares Plugin deleted toast, but it was absent at 0.1/0.5/2/5 seconds after real 204 delete. → approved-fix: resolved_in_candidate; evidence: `cc-lb-admin-web-qa-evidence/local-plugins/actions.jsonl#45`
- **PRINCIPAL-02** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ApiKeysCard:issue_key`; variant=`issue without label (empty)`; Issue button accepts empty label; baseline backend returns HTTP 500 invalid input instead of client validation or a 4xx. → approved-fix: resolved_in_candidate; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[2]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#76`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#115`
- **SETTINGS-588** — `settings.tsx#SettingsPage:download_export`; variant=`success side effect config_export audit`; FAIL; evidence: `cc-lb-admin-web-qa-evidence/local-settings/actions.jsonl#595`
- **SETTINGS-592** — `audit.tsx#AuditPage:mount`; variant=`loaded with records: API 200-entry cap`; FAIL; evidence: `cc-lb-admin-web-qa-evidence/local-settings/actions.jsonl#599`
- **SETTINGS-640** — `settings.tsx#SettingsPage:download_export`; variant=`success export plus config_export audit visibility`; Unfiltered API cap omitted newest export audit row.; evidence: `cc-lb-admin-web-qa-evidence/local-settings/actions.jsonl#651`
- **UPSTREAM-023** — `components/upstreams/InlineNameEditor.tsx#InlineNameEditor:save`; variant=`Unique valid string (triggers mutation)`; One Enter action produced PUT 200 followed by a second PUT 409; the UI success state did not remain stable. → approved-fix: resolved_in_candidate; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#7`
- **UPSTREAM-029** — `components/upstreams/SettingsCard.tsx#SettingsCard:save`; variant=`Save with custom Base URL`; Settings PUT persisted the custom base URL, but a fresh list response and browser reload omitted the saved field. → approved-fix: resolved_in_candidate; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[43]`
- **UPSTREAM-030** — `components/upstreams/SettingsCard.tsx#SettingsCard:save`; variant=`Save with env var key`; Settings PUT persisted the env-key setting, but a fresh list response and browser reload omitted the saved field. → approved-fix: supported_limitation_not_in_approved_fix; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#13`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#14`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#70`, +3
- **UPSTREAM-031** — `components/upstreams/SettingsCard.tsx#SettingsCard:save`; variant=`Save with literal key`; Settings PUT persisted the literal-key setting, but a fresh list response and browser reload omitted the saved field. → approved-fix: resolved_in_candidate; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/summary.json#coverage[45]`
- **UPSTREAM-032** — `components/upstreams/warmup/WarmupCardMinimal.tsx#WarmupCardMinimal:toggle`; variant=`Enable warmup`; Warmup disable succeeded, but re-enable returned 400 when OAuth credentials were absent, so the card could not restore the prior state.; evidence: `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#40`, `cc-lb-admin-web-qa-evidence/local-upstreams/actions.jsonl#98`

### safetyblocked (9)

- **PRODUCTION-GLOBAL-03** — `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx#handleSubmit:validation_empty`; Would require forcing the authenticated production tab into the credential gate; token state preserved.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#3`
- **PRODUCTION-GLOBAL-18** — `settings.tsx#ConfigDraftSection:save_draft`; PUT /admin/config/draft forbidden.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#48`
- **PRODUCTION-GLOBAL-19** — `settings.tsx#ConfigDraftSection:validate_draft`; POST /admin/config/draft/validate forbidden.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#49`
- **PRODUCTION-GLOBAL-20** — `settings.tsx#ConfigDraftSection:apply_draft`; POST /admin/config/apply forbidden.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#50`
- **PRODUCTION-GLOBAL-21** — `settings.tsx#ConfigDraftSection:reload_daemon`; POST /admin/config/reload forbidden.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#51`
- **PRODUCTION-GLOBAL-36** — `crates/cc-lb-admin/web/src/routes/status.tsx#StatusPage:toggle_killswitch`; Emergency traffic control was visible but never clicked.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#72`
- **PRODUCTION-GLOBAL-37** — `crates/cc-lb-admin/web/src/routes/credentials.tsx#CredentialsPage:rotate`; POST credential rotate mutation forbidden in production; control not invoked.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#73`
- **PRODUCTION-GLOBAL-38** — `crates/cc-lb-admin/web/src/routes/credentials.tsx#CredentialsPage:revoke`; POST credential revoke mutation forbidden in production; control not invoked.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl#74`
- **KEEPALIVE-DIRECT-SAFETY-STOP** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` plus 1 more; After five 2s and one 10s client deadlines, 248 direct denominator rows were not issued to avoid additional production load.; evidence: `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime-summary.json`

### source_contract_mismatch (5)

- **PLUGIN-1** — `PluginCatalog.tsx#PluginCatalog:run_gc`; variant=`Backend orphan-blob sweep versus UI unused-registry label`; Backend GC contract only deletes blobs with no wasm_registry_v2 owner; UI labels active refcount=0 registry rows as deletable uploads, so the action is contract-misaligned rather than a backend GC failure. → approved-fix: resolved_in_candidate; evidence: `cc-lb-admin-web-qa-evidence/local-plugins/actions.jsonl#47`
- **PRINCIPAL-01** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ApiKeysCard:mount`; variant=`populated table with Active and Revoked badges`; Real key-list API omits revoked rows, so the combined Active and Revoked populated-table variant is not reachable after a real revoke.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[1]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#71`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#140`
- **PRINCIPAL-03** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ApiKeysCard:revoke_key`; variant=`confirm revocation`; POST revoke returned 200 and DB status became revoked, but the following GET key list omitted the revoked key; UI row disappeared instead of rendering a Revoked badge.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[3]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#77`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#116`, +2
- **PRINCIPAL-08** — `crates/cc-lb-admin/web/src/routes/principals.tsx#PrincipalDetail:mount_observability_hook_chain`; variant=`loading`; While the real chain GET was held, the editor rendered its empty/default state rather than a loading indicator.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[8]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#103`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#142`
- **PRINCIPAL-09** — `crates/cc-lb-admin/web/src/routes/principals.tsx#PrincipalDetail:mount_shape_chain`; variant=`loading`; While the real chain GET was held, the editor rendered its empty/default state rather than a loading indicator.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[9]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#104`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#141`

### sqlcorrelationblocked (1)

- **MEASURE-SQL-POOL** — `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:query_principals`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:query_upstreams`, `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx#Topbar:health_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll` plus 18 more; Browser timing and direct HTTP observations do not provide per-request server SQL/pool correlation; required SQL/pool measurement remains unavailable.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions-summary.json`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime-summary.json`, `cc-lb-admin-web-qa-evidence/production-heavy-principal-card.json`

### unsupported (1)

- **SETTINGS-635** — `settings.tsx#ConfigDraftSection:apply_draft`; variant=`success variant requested but file-backed serve returns 501`; FAIL_SOURCE_UNSUPPORTED; evidence: `cc-lb-admin-web-qa-evidence/local-settings/actions.jsonl#646`

## Preserving original FAILs and supersedes

- The 10 `SUPERSEDED_HARNESS_ATTEMPT` entries in Settings were linked to corrected cells.
- Raw failure/blocked attempts absent from the Principals final `remaining_scope` were not promoted to product FAIL.
- The Upstreams raw 142 step/retry records were separated from the final 111 variant cells. The 72 raw FAILs and 5 final FAILs were not treated as the same count.
- Approved-candidate resolutions also do not delete baseline FAILs; they are linked only via overlay.
- `historical_attempts[].superseded_by` in the machine artifact is the linking basis.

## Baseline source preservation

- The sha256 of the 7 members of `baseline-source-catalog.zip` was recomputed and confirmed to match the catalog.
- Live source files were regenerated against the approved uncommitted candidate and no longer match the archive. `archive_member` in `input_freeze` preserves the historical baseline hash, and `current_sha256`/`current_catalog_sha256` record the current state (see `approved-source-catalog-check.json`).
- The archive YAML is a historical catalog containing an empty-key serialization defect, not the current execution inventory. The current catalog passed standard parser validation.

## Total discrepancies and interpretation

| ID | Observation | Handling |
|---|---|---|
| CANONICAL-YAML-STANDARD-PARSE | A standard YAML parser rejects line 50 because an empty mapping key is emitted. | The frozen YAML hash is recorded; interactions and production_matrix_ids were extracted only from the bounded interactions and production_matrix_ids blocks. Counts were rechecked as 433 interactions and 422 production IDs. |
| SETTINGS-RECORDS-VS-CELLS | actions.jsonl has 654 records; 10 are SUPERSEDED_HARNESS_ATTEMPT, leaving 644 active records, but only 643 unique active cell_id values because one close-detail PASS cell is duplicated. | Record counts and normalized cell counts are reported separately. |
| PRINCIPALS-VARIANT-COUNT | summary catalog_variant_rows is 97, while latest_variant_status_counts sums to 105. | The 97 base variants remain the source minimum; the 105 summary statuses and 142 raw action records are not treated as the source denominator. |
| KEEPALIVE-DENOMINATOR-SNAPSHOT-SUPERSEDED | runtime-denominators.json captured 5 observed and 457 pending list first pages; the later keepalive ledger records 209 list successes, 5 list client aborts, and 248 list safety-stop rows. | The earlier snapshot is preserved as historical denominator discovery; the later frozen ledger controls current keepalive execution counts. |
| UPSTREAM-RAW-ATTEMPTS-VS-FINAL-CELLS | The upstream action ledger has 142 step/retry records and 72 raw FAIL attempts; the final coverage ledger has 111 variant cells and 5 FAIL cells. | Raw attempts are preserved in historical_attempts with final coverage links; raw attempts are not counted as final cells. |
| SOURCE-ROWS-ARE-NOT-EXECUTIONS | The canonical inventory has 433 rows and 422 production rows, while runtime evidence has separate action records, direct API rows, and UI cells. | Source rows, base variants, raw records, normalized cells, direct stages, and UI stages have separate counters. |

## Limitations and change boundary

- The complete denominator of actual page/entity/cursor/poll is undetermined. `full_runtime_cell_count` and `unresolved_upper_bound` are `null`.
- backend-only, browser-capability-unsupported, and intentional not-applicable items were not counted as execution FAILs.
- No raw production entity/request IDs, secrets, or raw URLs/queries were included.
- Application code, production state, and Git/PR were not modified. This work wrote only three files: `runtime-progress.json`, `runtime-progress.md`, and `current-findings.md`.

## Instrumentation evidence (isolated)

- `instrumentation/index.json`: 19 files, candidate `132e831a…`, baseline `f564dcf5…`.
- 12 correlation requests, 2 body privacy probes, 300-request A/B/A overhead measurement, 12 proxy rejection/header controls, library regression 191/191 PASS.
- Timing contract: `handler_ms` is next.run→response head, `sql_elapsed_secs` is driver/query-stream lifetime (not pure DB execution), `acquire_total_secs` is the full acquire including queue/check/connect (not pure queue wait).
- Isolated proof, not production deployment or full-matrix PASS.

## Server-timing evidence (isolated)

- `server-timing/index.json`: 20 files, candidate `db564601…`, recorder v1.0.7.
- Recorder source SHA: `003680df…` at verification time, final `46549022…` (post-browser change: fetch observer registration and monitor failure included in the completeness gate; three failure codes independently unit-verified).
- SQLite: 17 resource entry joins, 17 unique server IDs, 6 SQL-bearing. The source window is the retained snapshot from sequence 106; the earlier 103 drained entries are unrecovered and unclaimed.
- PostgreSQL: 19 resource entry joins, 19 unique server IDs, 6 SQL-bearing. `capture_complete=false`, reason `action_window_still_open`. The earlier PostgreSQL UI row (server log not preserved) was not replaced.
- `fetch_observation=0` (isolated world); no fake fetch lifecycle record was created.
- Pure DB execution and pure pool queue wait are unmeasured. `full_runtime_qa_complete=false`.

## Production native observation (overlay, not merged)

- `production-plugins-native-2026-09-15.json`, `production-upstreams-native-2026-09-15.json`: native Camofox bounded slice observations. The raw files are private and exact scope confirmation is pending, so they are recorded as a separate overlay rather than merged into `production_area_matrix`/`normalized_cells`.
- Plugins: 33 rows / 20 unique item_id / PASS 15, PASS_TARGET_ONLY 4, BLOCKED 4, NOT_APPLICABLE 5, SKIPPED_WRITE 5 / writes 0 / 4 kinds of measurement_blocked / 1 unconfirmed (first Edit click unresponsive — product bug not confirmed).
- Upstreams: 212 raw rows / 44 unique item_id / PASS 88, SKIPPED_WRITE 94, BLOCKED 20, NOT_APPLICABLE 10 / writes 0. A later offline reconciliation linked the suffixed ID `UI-SRC-0D929B471D7A-name-edit` to the Escape-cancelled variant of the existing name edit. The 9 write-unexecuted rows and original status are unchanged.

## Out-of-scope production GETs (recorded separately)

- `production-read-scope-deviation.json`: a browser subtask assigned to local-only verification executed 5 production GETs. 0 writes.
- GET `/admin/v1/auth/session` 404, `/admin/v1/status` 200, `/admin/v1/principals` 200 (22 rows), `/admin/v1/upstreams` 200 (9 rows), `/admin/v1/plugins/registry` 200 (0 rows).
- Recorded separately; counted neither as local proof, nor as historical production coverage, nor as absent.

## Existing product issues and follow-up approval scope

- `clearBaseURL`: the past unsupported behavior was fixed and verified in the local candidate after separate approval. No production application is claimed.
- `SettingsApply`: button enablement for unsupported providers was fixed via the capability display. The supported mode's existing response revision, `expected_revision` handling, and history creation limitations are separately unverified items and are not considered resolved by this capability change.

## Finalization gate (Main local, build/lint/test)

- `finalization-gates/results.json`: rust-format, web-lint, web-typecheck, web-tests, inventory-check, recorder-syntax all exit 0.
- Per Main's report: 699 web tests, 191 current library tests, generator 433/170 + standard parse + counterproof, independent privacy scan of 91 files with 0 secrets.
- This gate is at the build/lint/test level and is not runtime QA completion.


## Follow-up approval/verification overlay

- Source snapshot at the time this overlay was written: 205 actions, 595 declared variant labels, 149 request occurrences. The current labels after the follow-up safety fixes are recorded in the separate overlay below. The complete denominator of the runtime entity·page·poll product remains undetermined.
- Native source mapping: of the 245 preserved rows, 244 are canonical IDs and 1 is an Escape-cancel alias with confirmed basis. Original execution/measurement status was not changed.
- `post-hold-reassessment/independent-page-oracle-check.json`: in a local PostgreSQL single-page navigation, prior expectations were compared against the actual DOM separately, and the original PNG, closed window, and server connection were verified. No past open window or lost data was recovered.
- `post-hold-usage-status/verification.json`: new isolated SQLite functional/state-transition evidence for the `UPSTREAM-004` usage display and the `UPSTREAM-006` error state. Does not mean production coverage or completion of all timing fields.
- `base-url-clear/verification.json`: the omitted/explicit-null/URL tri-state was fixed after separate approval. Verified 33 regressions across both DBs, HTTP, and real Lifecycle dispatch, plus UI save, fresh query, and reload.
- `settings-apply-capability/verification.json`: verified 23 backend and 700 Web tests plus Apply blocking after real file-provider Save/Validate. Apply requests during that UI observation window: 0. Success metadata and history issues are recorded separately in `preexisting-supported-apply-limit.json`.
- The last verified PR head is `91cd5219bd9047d1339ff40e9aec11142867a36e`, with 11 checks passing and 1 conditional exclusion confirmed. PR #793 is a draft and the user's merge hold remains. This value is a record of the checked point in time and does not automatically apply to future heads.
- Production enablement of the follow-up candidate, the full production UI matrix, a real non-production OAuth success path, and pure DB execution/pool queue wait are still unmet. `runtime_complete=false` and `overall_pass=false` remain.
- The user confirmed on 2026-09-16 that no non-production OAuth test account is currently available. The real success path remains blocked and is not substituted with a production token, arbitrary account, or fake response. This answer does not approve credential access or external calls.

## Approved application safety fix overlay

- `application-safety/verification.json` and `index.json` link before/after materials for the three safety defects. The past 1,511 observation rows, classification coefficients, and unresolved list were not modified or reclassified as success.
- HTTP Base URL omission/null is preserved; only explicit `clear_base_url` clears it. In the final browser run, verified: old UI → new server no-edit save preservation, new UI clear, new UI → old server unconfirmed-clear error with editing retained, and Audit display of previously hidden admin actions. The original `base-url-clear` null clearing is verification history of the earlier contract.
- The Audit admin condition in both DBs is applied before LIMIT, and the general API's all-records default is preserved. Verified the final indexes' 6 query results, generic plan, real migration runner, and read/write costs. The index write/WAL cost increase is not hidden; large production PG requires separately approved online pre-preparation.
- Current source: 205 actions / 597 declared variant labels / 149 request occurrences / 115 API endpoints / 433 inventory rows. Not an execution matrix denominator or production completion rate.
- Final local checks: 112 Rust, 706 Web, typecheck, build, affected-Rust Clippy, formatter, inventory check, and standard YAML parsing. Subsequent CI is checked separately for each actual PR head; these local results are not used in place of full production QA or future head CI.
- The existing merge hold, production instrumentation/deployment approval, OAuth test account, full production matrix, and pure DB/queue wait blocks remain in place.
