# Admin Web Runtime Progress — 2026-09-15

> **현재 상태: INCOMPLETE.** 완료 보고가 아니라 동결한 source/evidence를 현재 실행·미실행·차단·실패 집합에 연결한 진행 ledger다. `runtime_complete=false`, `overall_pass=false`다.

## 판정 요약

- Current baseline: `ef70b347`
- Production deployed: `e56d029ea8641827d420c582c5f41a72ab9e2182`
- Audit candidate: `ef70b34+uncommitted-audit-patch-…` — 승인된 latest-200 범위의 별도 overlay이며 baseline FAIL을 삭제하지 않는다.
- Approved fix candidate: `ef70b347` + uncommitted patch `7ef93773…` (binary `f564dcf5…`) — 별도 `approved_fix_overlay`이며 baseline 분모·셀을 바꾸지 않는다.
- 입력 707개(전체 evidence/run 입력 동결; browser profile·credential·cache 디렉터리는 비입력으로 명시 제외), 누락 0개, JSON/JSONL parse error 0개.
- Canonical inventory는 approved uncommitted candidate 기준으로 재생성되었다(standard parser Ruby/Psych 통과, `approved-source-catalog-check.json`). 현재 분모는 433 rows / 170 storage operations / 422 production matrix rows이며, 아래 정규화 cell 수치는 historical 값을 그대로 보존한다.
- 필수 SQL/pool correlation, native keepalive pointer, production UI matrix가 미완료이므로 overall PASS를 주장하지 않는다.

## 수치 기준: source와 실행을 분리

| 기준 | 수치 | 의미 |
|---|---:|---|
| Current source action | 205 | `ui.json.items` 원시 배열 길이 |
| Current base variant | 582 | `variants` 합계; runtime cell 수가 아님 |
| UI request occurrence | 149 | action별 `requests` 원시 합계 |
| API endpoint row | 115 | api-read 70 + api-write 45 |
| Canonical inventory row | 433 | ui_action 215 + network_request 162 + backend_endpoint 56 |
| Production matrix row | 422 | ui_action 208 + network_request 158 + backend_endpoint 56 |
| 모든 입력 JSONL row | 1,650 | candidate subset/network 중복 파일 포함 |
| Primary action/runtime row | 1,563 | 주 ledger 7개 원시 합계 |
| Normalized physical cell | 1,511 | UI 1,009 + direct API 499 + supplemental 3 |
| Keepalive direct row | 484 | UI 실행 0; 별도 heavy-card UI artifact 1 |
| Approved-candidate flow | 19 | upstreams 6 required PASS + principals 10 PASS + plugins 3 PASS (overlay, baseline cell 아님; isolated functional check 수이며 browser interaction 수가 아님) |

433 source row, 205 source action, 422 production row, 484 keepalive direct row를 UI 실행 수로 세지 않았다.

## 필수 분류 계수

| 분류 | 수 |
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

`behaviorverified`는 관측한 동작 oracle 통과다. `artifactoraclepass`는 **원시 artifact가 선언한 기능 검증 통과**이며 UI/direct/SQL/pool 전 계층 측정 PASS가 아니다.

## Full measurement gate

| 상태 | cell 수 |
|---|---:|
| not_evaluated | 1,006 |
| blocked | 505 |

UI interaction + direct API + SQL + pool 측정 게이트다. SQL/pool correlation 증거가 없는 cell은 `blocked`, 그 외 측정 증거가 없는 cell은 `not_evaluated`다. 측정값을 0으로 채우지 않았고 PASS로 계수한 cell은 없다. Server-timing 증거는 canonical RID join을 측정했지만 pure DB 실행·pure pool queue wait는 여전히 미측정이다.

## Runtime enum 분모

| Enum | Production Camofox/Firefox | Local Playwright/Chromium | 판정 |
|---|---:|---:|---|
| Locale (auto 포함) | 147 | 126 | Chromium 미지원 21개 source 후보는 product FAIL이 아니다. Local 128 cell은 옵션 126 + 분모/필터 관측 2다. |
| Timezone (auto 포함) | 446 | 419 | runtime별 actual supported enum이 다르다. Local 420 cell은 옵션 419 + 분모 관측 1이다. |

## Current source 영역별 진행

| 영역 | source | reviewed | UI executed | direct API | partial | not executed | artifact-oracle-pass |
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

## Production matrix 영역별 진행

| 영역 | source | reviewed | UI executed | direct API | partial | not executed | artifact-oracle-pass |
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

Production UI executed는 UI stage만 센다. Keepalive direct GET 성공 230개는 direct API 증거다.

## Audit baseline과 후보판 분리

- Baseline `local-settings/actions.jsonl` FAIL row 595, 599, 651을 보존했다.
- 후보판 `actions-all.jsonl` 15 PASS는 direct API 8, UI 7이다.
- 후보판은 latest-200, same-timestamp 순서, delayed-old 배제/시간필터, export refresh/reload 가시성 해결 증거다.
- 후보판 source version이 baseline과 다르므로 baseline FAIL을 PASS로 덮지 않는다.

## Approved fix overlay (baseline 보존)

- Candidate: `ef70b347` + uncommitted patch `7ef93773…`, binary `f564dcf5…`. Main이 reviewed JSON/JSONL 15개 + provenance + focused regression을 `qa/admin-web/runs/2026-09-15/approved-fixes/`에 byte-identical 복사했다(해시 대조 완료). profiles/credentials/unreviewed PNG는 미복사.
- Flow: upstreams required 6/6 PASS, principals 10/10 PASS, plugins 3/3 PASS (+gc-storage proof). 이 수치는 isolated functional check이며 full runtime/measurement PASS가 아니다. principals 10개 중 direct API-only 1, proxy-only 1을 포함한다.
- Baseline FAIL은 삭제하지 않고 `approved_fix_overlay.resolved_findings`로 해결 증거를 분리 연결한다.

| Baseline task | Source | 해결 candidate flow |
|---|---|---|
| UPSTREAM-023 | `components/upstreams/InlineNameEditor.tsx#InlineNameEditor:save` | rename_enter_exactly_one_put_no_followup_409, rename_blur_exactly_one_put, rename_escape_no_request, rename_real_stale_409_preserved |
| UPSTREAM-029 | `components/upstreams/SettingsCard.tsx#SettingsCard:save` | settings_baseurl_put_get_reload_persists |
| UPSTREAM-031 | `components/upstreams/SettingsCard.tsx#SettingsCard:save` | settings_true_null_default_and_writeonly_literal |
| PRINCIPAL-02 | `crates/cc-lb-admin/web/src/routes/principals.tsx#ApiKeysCard:issue_key` | approved-candidate-001 blank label UI issue exact-one, approved-candidate-005 non-empty NUL label rejected without key, approved-candidate-omitted-label-direct omitted label API parity |
| PLUGIN-1 | `PluginCatalog.tsx#PluginCatalog:run_gc` | gc.json, gc-storage.json |
| PLUGIN-5 | `PluginDeleteDialog.tsx#PluginDeleteDialog:confirm_delete` | simple.json, cascade.json |

- `UPSTREAM-030` (env var key): candidate exploratory FAIL은 `SUPPORTED_LIMITATION/NOT_IN_APPROVED_FIX`다 — env 이름은 write 시 암호화 key material로 해소되고 GET이 반환하지 않아 reload provenance 재구성 불가. secret은 blank/write-only 유지. extra FAIL이 아니며 baseline product_fail도 유지.
- `settings_null_default_get_reload` exploratory FAIL은 required PASS와 다른 leg로, product FAIL로 계수하지 않고 triage 대상으로 기록.
- 새 coverage(baseline FAIL 해결 아님): principals blank-label list/proxy/revoke, terminal refetch lock 2종, concurrent 409 preserved.
- Regression: 이전 full test 1519/1520 fail → warmup test 수정 → focused detail-contract 9/9 PASS → whole backend gate **1520/1520 PASS** (13 slow, 9 skipped, exit 0; `cc-lb-admin-web-qa-evidence/approved-fixes-full-backend-gate.txt`). backend test gate만 완료이며 full runtime QA gate는 계속 미완료다. native RECOVERED_LOCAL 및 production Plugins 증거는 진행 중이다.

## Keepalive 진행과 한계

- 484 physical row = list 462 + summary 22.
- list: direct success 209, client abort 5, safety-stop 미실행 248.
- summary: two-cycle direct success 21, client abort 1.
- direct 성공 230과 abort 6은 UI 실행이 아니다.
- heavy-card DOM UI artifact 1개는 최초 8,442ms HTTP 200과 자동 poll 8회를 관측했다.
- 2s/3s/10s client deadline은 right-censored lower bound이며 HTTP server FAIL이 아니다.
- native pointer는 Camofox 410, SQL/pool correlation은 unavailable이다.

## 미완료 작업 목록

총 **135개 task**. 각 항목은 source, 환경, 사유, 원시 artifact에 연결된다. `→ approved-fix` 표시는 baseline 분류를 유지한 채 candidate 해결/제한 증거를 가리킨다.

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

- **KEEPALIVE-RIGHT-CENSORED** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` 외 1개; Five list requests and one summary request hit client-imposed deadlines. They are right-censored lower bounds, not HTTP server failures.; evidence: `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`, `cc-lb-admin-web-qa-evidence/production-heavy-principal-card.json`

### nativepointerblocked (1)

- **KEEPALIVE-NATIVE-POINTER** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` 외 1개; Camofox native pointer interaction returned 410. One DOM-dispatched heavy-card UI artifact verified behavior but does not replace native-pointer coverage.; evidence: `cc-lb-admin-web-qa-evidence/production-heavy-principal-card.json`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime-summary.json`

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
- **CURRENT-NOT-EXECUTED-GLOBAL** — `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx#AuthRequiredGate:external_auth_retry`, `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx#handleSubmit:validation_empty`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:close`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:keyboard_open` 외 13개; No executed current-baseline runtime cell exists for 17 Global source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-LOGS** — `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `components/ui/LogsPagination.tsx#LogsPagination:next_page`, `components/ui/LogsPagination.tsx#LogsPagination:prev_page` 외 27개; No executed current-baseline runtime cell exists for 31 Logs source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-OVERVIEW** — `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:close`, `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:copy_id` 외 17개; No executed current-baseline runtime cell exists for 21 Overview source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-PRINCIPALS** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:toggle_enabled`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter` 외 5개; No executed current-baseline runtime cell exists for 9 Principals source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-SHARED-REQUEST-TABLES** — `crates/cc-lb-admin/web/src/components/ui/latency/LatencyCell.tsx#LatencyCell:responsibility_breakdown`; No executed current-baseline runtime cell exists for 1 Shared Request Tables source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **CURRENT-NOT-EXECUTED-UPSTREAMS** — `components/upstreams/warmup/WarmupHistoryDrawer.tsx#AttemptDetail:close`, `components/upstreams/warmup/parts/WarmupConfigModal.tsx#WarmupConfigModal:copy`, `routes/upstreams.tsx#CreateUpstreamModal:submitOauthConfirm`, `routes/upstreams.tsx#DetailView:isolateSeries` 외 5개; No executed current-baseline runtime cell exists for 9 Upstreams source action(s). Denominator/source review alone is not execution.; evidence: `cc-lb-admin-web-qa-evidence/runtime-denominators.json`, `cc-lb-admin-web-qa-completion/qa/admin-web/source-contracts/ui.json`
- **PRODUCTION-UI-NOT-EXECUTED-AUDIT** — `audit.tsx#AuditPage:clear_filters`, `audit.tsx#AuditPage:click_row`, `audit.tsx#AuditPage:close_detail_modal`, `audit.tsx#AuditPage:empty_state_clear` 외 8개; No UI execution exists for 12 production-matrix Audit action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-CREDENTIALS** — `crates/cc-lb-admin/web/src/routes/credentials.tsx#CredentialsPage:revoke`, `crates/cc-lb-admin/web/src/routes/credentials.tsx#CredentialsPage:rotate`; No UI execution exists for 2 production-matrix Credentials action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-GLOBAL** — `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx#handleSubmit:validation_empty`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:goto_status`, `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx#Topbar:open_mobile_menu`, `crates/cc-lb-admin/web/src/components/layout/Sidebar.tsx#Sidebar:link_credentials` 외 1개; No UI execution exists for 5 production-matrix Global action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-LOGS** — `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `components/ui/LogsPagination.tsx#LogsPagination:next_page`, `components/ui/LogsPagination.tsx#LogsPagination:prev_page` 외 27개; No UI execution exists for 31 production-matrix Logs action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-OVERVIEW** — `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:dismiss`, `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx#LiveTailFailureBanner:retry`, `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx#RequestDetail:copy_id`, `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts#connect:delta_backfill` 외 3개; No UI execution exists for 7 production-matrix Overview action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-PLUGINS** — `PluginCatalog.tsx#PluginCatalog:copy_sha256`, `PluginCatalog.tsx#PluginCatalog:inspect_click`, `PluginCatalog.tsx#PluginCatalog:open_delete_dialog`, `PluginCatalog.tsx#PluginCatalog:run_gc` 외 18개; No UI execution exists for 22 production-matrix Plugins action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-PRINCIPALS** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:toggle_enabled`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` 외 35개; No UI execution exists for 39 production-matrix Principals action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-SETTINGS** — `settings.tsx#ConfigDraftSection:apply_draft`, `settings.tsx#ConfigDraftSection:reload_daemon`, `settings.tsx#ConfigDraftSection:retry_editor`, `settings.tsx#ConfigDraftSection:save_draft` 외 2개; No UI execution exists for 6 production-matrix Settings action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-STATUS** — `crates/cc-lb-admin/web/src/routes/status.tsx#StatusPage:toggle_killswitch`; No UI execution exists for 1 production-matrix Status action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`
- **PRODUCTION-UI-NOT-EXECUTED-UPSTREAMS** — `components/upstreams/ApiUsageCard.tsx#ApiUsageCard:metricToggle`, `components/upstreams/ApiUsageCard.tsx#ApiUsageCard:rangeToggle`, `components/upstreams/InlineNameEditor.tsx#InlineNameEditor:save`, `components/upstreams/SettingsCard.tsx#SettingsCard:save` 외 39개; No UI execution exists for 43 production-matrix Upstreams action(s); direct API rows and source counts are excluded from UI execution.; evidence: `cc-lb-admin-web-qa-completion/qa/admin-web/api-query-inventory.yaml#production_matrix_ids`, `cc-lb-admin-web-qa-evidence/production-global-actions.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`

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
- **KEEPALIVE-DIRECT-SAFETY-STOP** — `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_horizon`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:change_status_filter`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx#CacheKeepaliveSessionsDrawer:mount_and_poll` 외 1개; After five 2s and one 10s client deadlines, 248 direct denominator rows were not issued to avoid additional production load.; evidence: `cc-lb-admin-web-qa-evidence/production-keepalive-runtime.jsonl`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime-summary.json`

### source_contract_mismatch (5)

- **PLUGIN-1** — `PluginCatalog.tsx#PluginCatalog:run_gc`; variant=`Backend orphan-blob sweep versus UI unused-registry label`; Backend GC contract only deletes blobs with no wasm_registry_v2 owner; UI labels active refcount=0 registry rows as deletable uploads, so the action is contract-misaligned rather than a backend GC failure. → approved-fix: resolved_in_candidate; evidence: `cc-lb-admin-web-qa-evidence/local-plugins/actions.jsonl#47`
- **PRINCIPAL-01** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ApiKeysCard:mount`; variant=`populated table with Active and Revoked badges`; Real key-list API omits revoked rows, so the combined Active and Revoked populated-table variant is not reachable after a real revoke.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[1]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#71`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#140`
- **PRINCIPAL-03** — `crates/cc-lb-admin/web/src/routes/principals.tsx#ApiKeysCard:revoke_key`; variant=`confirm revocation`; POST revoke returned 200 and DB status became revoked, but the following GET key list omitted the revoked key; UI row disappeared instead of rendering a Revoked badge.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[3]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#77`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#116`, +2
- **PRINCIPAL-08** — `crates/cc-lb-admin/web/src/routes/principals.tsx#PrincipalDetail:mount_observability_hook_chain`; variant=`loading`; While the real chain GET was held, the editor rendered its empty/default state rather than a loading indicator.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[8]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#103`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#142`
- **PRINCIPAL-09** — `crates/cc-lb-admin/web/src/routes/principals.tsx#PrincipalDetail:mount_shape_chain`; variant=`loading`; While the real chain GET was held, the editor rendered its empty/default state rather than a loading indicator.; evidence: `cc-lb-admin-web-qa-evidence/local-principals/summary.json#remaining_scope[9]`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#104`, `cc-lb-admin-web-qa-evidence/local-principals/actions.jsonl#141`

### sqlcorrelationblocked (1)

- **MEASURE-SQL-POOL** — `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:query_principals`, `crates/cc-lb-admin/web/src/components/CommandPalette.tsx#CommandPalette:query_upstreams`, `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx#Topbar:health_poll`, `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx#CacheKeepaliveCard:summary_poll` 외 18개; Browser timing and direct HTTP observations do not provide per-request server SQL/pool correlation; required SQL/pool measurement remains unavailable.; evidence: `cc-lb-admin-web-qa-evidence/production-global-actions-summary.json`, `cc-lb-admin-web-qa-evidence/production-keepalive-runtime-summary.json`, `cc-lb-admin-web-qa-evidence/production-heavy-principal-card.json`

### unsupported (1)

- **SETTINGS-635** — `settings.tsx#ConfigDraftSection:apply_draft`; variant=`success variant requested but file-backed serve returns 501`; FAIL_SOURCE_UNSUPPORTED; evidence: `cc-lb-admin-web-qa-evidence/local-settings/actions.jsonl#646`

## 원본 FAIL 보존과 supersedes

- Settings의 10개 `SUPERSEDED_HARNESS_ATTEMPT`를 corrected cell에 연결했다.
- Principals final `remaining_scope`에 없는 raw 실패/차단 attempt는 product FAIL로 승격하지 않았다.
- Upstreams raw 142 step/retry와 final 111 variant cell을 분리했다. Raw FAIL 72개와 final FAIL 5개를 같은 수로 보지 않았다.
- Approved-candidate 해결도 baseline FAIL을 삭제하지 않고 overlay로만 연결한다.
- Machine artifact의 `historical_attempts[].superseded_by`가 연결 기준이다.

## Baseline source 보존

- `baseline-source-catalog.zip` 7개 멤버의 sha256을 재계산해 catalog와 일치함을 확인했다.
- Live source 파일은 approved uncommitted candidate 기준으로 재생성되어 archive와 더 이상 일치하지 않는다. `input_freeze`의 `archive_member`는 historical baseline 해시를 보존하고 `current_sha256`/`current_catalog_sha256`로 현재 상태를 기록한다(`approved-source-catalog-check.json` 참조).
- Archive의 YAML은 empty-key 직렬화 결함을 포함한 historical catalog이며 현재 실행 inventory가 아니다. 현재 catalog는 standard parser 검증을 통과했다.

## 총계 불일치와 해석

| ID | 관측 | 처리 |
|---|---|---|
| CANONICAL-YAML-STANDARD-PARSE | A standard YAML parser rejects line 50 because an empty mapping key is emitted. | The frozen YAML hash is recorded; interactions and production_matrix_ids were extracted only from the bounded interactions and production_matrix_ids blocks. Counts were rechecked as 433 interactions and 422 production IDs. |
| SETTINGS-RECORDS-VS-CELLS | actions.jsonl has 654 records; 10 are SUPERSEDED_HARNESS_ATTEMPT, leaving 644 active records, but only 643 unique active cell_id values because one close-detail PASS cell is duplicated. | Record counts and normalized cell counts are reported separately. |
| PRINCIPALS-VARIANT-COUNT | summary catalog_variant_rows is 97, while latest_variant_status_counts sums to 105. | The 97 base variants remain the source minimum; the 105 summary statuses and 142 raw action records are not treated as the source denominator. |
| KEEPALIVE-DENOMINATOR-SNAPSHOT-SUPERSEDED | runtime-denominators.json captured 5 observed and 457 pending list first pages; the later keepalive ledger records 209 list successes, 5 list client aborts, and 248 list safety-stop rows. | The earlier snapshot is preserved as historical denominator discovery; the later frozen ledger controls current keepalive execution counts. |
| UPSTREAM-RAW-ATTEMPTS-VS-FINAL-CELLS | The upstream action ledger has 142 step/retry records and 72 raw FAIL attempts; the final coverage ledger has 111 variant cells and 5 FAIL cells. | Raw attempts are preserved in historical_attempts with final coverage links; raw attempts are not counted as final cells. |
| SOURCE-ROWS-ARE-NOT-EXECUTIONS | The canonical inventory has 433 rows and 422 production rows, while runtime evidence has separate action records, direct API rows, and UI cells. | Source rows, base variants, raw records, normalized cells, direct stages, and UI stages have separate counters. |

## 한계 및 변경 경계

- Actual page/entity/cursor/poll 완전 분모는 미확정이다. `full_runtime_cell_count`와 `unresolved_upper_bound`는 `null`이다.
- backend-only, browser capability 미지원, 의도한 not-applicable은 실행 FAIL로 세지 않았다.
- raw production entity/request ID, secret, raw URL/query를 포함하지 않았다.
- application code, 운영 상태, Git/PR은 수정하지 않았다. 이 작업이 쓴 파일은 `runtime-progress.json`, `runtime-progress.md`, `current-findings.md` 세 개뿐이다.

## Instrumentation 증거 (isolated)

- `instrumentation/index.json` 19개 파일, candidate `132e831a…`, baseline `f564dcf5…`.
- Correlation request 12, body privacy probe 2, A/B/A overhead 측정 300 request, proxy rejection/header control 12, library regression 191/191 PASS.
- Timing contract: `handler_ms`는 next.run→response head, `sql_elapsed_secs`는 driver/query-stream lifetime(pure DB 실행 아님), `acquire_total_secs`는 queue/check/connect 포함 full acquire(pure queue wait 아님).
- Isolated proof이며 production 배포·full-matrix PASS가 아니다.

## Server-timing 증거 (isolated)

- `server-timing/index.json` 20개 파일, candidate `db564601…`, recorder v1.0.7.
- Recorder source SHA: verification 시점 `003680df…`, 최종 `46549022…`(post-browser 변경: fetch observer registration·monitor failure까지 completeness gate에 포함, 세 failure code를 unit으로 독립 검증).
- SQLite: resource entry 17 join, unique server ID 17, SQL-bearing 6. Source window는 sequence 106부터의 retained snapshot이며 이전 103개 drained entry는 미복구·미주장.
- PostgreSQL: resource entry 19 join, unique server ID 19, SQL-bearing 6. `capture_complete=false`, 사유 `action_window_still_open`. 이전 PostgreSQL UI row(서버 로그 미보존)는 대체하지 않았다.
- `fetch_observation=0`(isolated world); fake fetch lifecycle record를 만들지 않았다.
- Pure DB 실행·pure pool queue wait는 미측정. `full_runtime_qa_complete=false`.

## Production native 관측 (overlay, 미편입)

- `production-plugins-native-2026-09-15.json`, `production-upstreams-native-2026-09-15.json`: native Camofox bounded slice 관측. Raw 파일은 private이며 정확한 scope 확인이 pending이므로 `production_area_matrix`/`normalized_cells`에 편입하지 않고 별도 overlay로 기록한다.
- Plugins: 33 rows / 20 unique item_id / PASS 15, PASS_TARGET_ONLY 4, BLOCKED 4, NOT_APPLICABLE 5, SKIPPED_WRITE 5 / writes 0 / measurement_blocked 4종 / unconfirmed 1(첫 Edit 클릭 미반응 — product bug 미확정).
- Upstreams: 212 rows / 44 unique item_id / PASS 88, SKIPPED_WRITE 94, BLOCKED 20, NOT_APPLICABLE 10 / writes 0 / unmatched item_id 1(`UI-SRC-0D929B471D7A-name-edit`, bounded slice 범위 밖).

## Scope 외 production GET (분리 기록)

- `production-read-scope-deviation.json`: local-only 검증에 배정된 browser subtask가 production GET 5건을 실행했다. Write 0건.
- GET `/admin/v1/auth/session` 404, `/admin/v1/status` 200, `/admin/v1/principals` 200(22건), `/admin/v1/upstreams` 200(9건), `/admin/v1/plugins/registry` 200(0건).
- Local proof로도, historical production coverage로도, 부재로도 세지 않고 별도 기록한다.

## Known bug (미수정)

- `clearBaseURL`, `SettingsApply`: 알려진 product bug이며 승인되지 않은 runtime 수정을 적용하지 않았다.

## Finalization gate (Main local, build/lint/test)

- `finalization-gates/results.json`: rust-format, web-lint, web-typecheck, web-tests, inventory-check, recorder-syntax 전부 exit 0.
- Main 보고 기준 web test 699, current library test 191, generator 433/170 + standard parse + counterproof, independent privacy scan 91 files 0 secret.
- 이 gate는 build/lint/test 수준이며 runtime QA 완료가 아니다.

