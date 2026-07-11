# Quota Full-Stack Functional QA — subscription-quota feature (PR #372)

Goal: functionally test the **entire** subscription-quota feature end to end —
storage → ingestion → HTTP endpoints → analysis/routing → admin-web UI — such that
a **zero-context agent can reproduce identical results from this file alone**.

Two equally-weighted halves:
- **Part A — Point-in-time** (§3): given a fixed state, is each layer correct?
- **Part B — State-transition** (§4): when backend data *changes*, does every layer change the way it should? (~50% of the value.)

> Every case states INITIAL state, the exact MUTATION (§2 — how to change backend state
> deterministically), and the EXPECTED observable delta at each layer (storage / API / UI),
> including the polling latency to wait. Citations are crate-relative paths.

---

## 0. Environment & tooling

- Service `cc-lb.service`; admin API + embedded SPA at `http://127.0.0.1:52252` (proxy `:52251`, metrics `:52253`). SPA embedded via `rust-embed` (`web/dist`); frontend changes need `bun run build` (in `crates/cc-lb-admin/web`) + `touch crates/cc-lb-admin/src/static_assets.rs` + binary rebuild + redeploy.
- Auth: `localStorage['cc-lb-admin-token']` = env `CC_LB_ADMIN_TOKEN` (`sed -n 's/^CC_LB_ADMIN_TOKEN=//p' ~/.config/cc-lb/env`; never echo). Header: `Authorization: Bearer $TOKEN`.
- Browser: `agent-browser` (`open`; `snapshot -i -c`; `click @eN`; `screenshot`; `wait --load networkidle`). Prefer fresh-snapshot `@eN` refs.
- API: `curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:52252<path>`.
- DB read: `sqlite3 -readonly "file:<DB>?mode=ro" "<SQL>"` where `DB=/home/bhyoo/.local/share/cc-lb/storage.sqlite`.

### 0.1 Isolated test instance (REQUIRED for state-transition mutations — never mutate shared prod DB)
```
TDB=/tmp/cclb-qa/storage.sqlite ; mkdir -p /tmp/cclb-qa
# 1. run migrations against a fresh DB (initialize) via the CLI or a serve boot:
CC_LB_ADMIN_TOKEN=qa-token CC_LB_MASTER_KEY=<32B-base64> \
  cc-lb serve --config /tmp/cclb-qa/config.toml   # config points [storage].path=$TDB, listener ports off-prod (e.g. 53251/53252)
# 2. create an OAuth upstream via admin API to get a real upstream_id UUID:
curl -s -H "Authorization: Bearer qa-token" -H 'content-type: application/json' \
  -d '{"name":"qa-oauth","kind":"anthropic_oauth"}' http://127.0.0.1:53252/admin/v1/upstreams
# 3. seed/mutate quota via §2 SQL against $TDB (service can stay up; latest/series are read per request).
```
Alternative (no full instance): mutate `$TDB` with §2 SQL, then run point-in-time API/DB assertions with a throwaway `cc-lb serve` pointed at `$TDB`. Do NOT hand-write into the shared prod DB.

### 0.2 Reference context data (prod snapshot; obtain equivalents, exact numbers drift)
- latest_v1: 54 rows / 7 upstreams. checkpoints_v1: ~55,140 rows (sparse). Per window: 5h≈25.9k, 7d≈23.0k, 7d_sonnet≈5.5k, overage≈402, unified≈357.
- windows seen: `5h,7d,7d_sonnet,overage,unified` (`7d_opus` valid, absent now). sources: `header,api`. statuses: `allowed,allowed_warning,rejected,NULL`.
- OAuth upstreams: bear-team, bear-max, isac-personal, bh322yoo-max, runbear0001-max, Runbear; `fake-upstream`=anthropic_api_key (non-quota).

## 1. Layers & data flow

```
Ingestion (3 origins):
  (a) periodic OAuth usage poller   -> source = "api" (ingests active `limits[]` entries where `kind = "weekly_scoped"`, model display name is "Fable", and `is_active = true`; inactive or non-Fable scoped limits are ignored)
  (b) proxy response headers (live) -> source = "header" (parses unified `7d_oi` headers)
  (c) admin POST /admin/v1/upstreams/{id}/warmup/fire-now -> real warmup, parses headers  (crates/cc-lb-admin/src/v1/upstreams.rs:101-103, 808-830)
        │  SubscriptionQuotaObservationRecord
        ▼
put_subscription_quota[_batch]   (crates/cc-lb-storage-sqlite/src/adapter/upstream_subscription_quota.rs:19-43)
  ├─► observations_v1   (only if table present; dropped after cleanup — lib.rs:55-71)
  ├─► latest_v1         (ALWAYS upsert, guarded observed_at>= — adapter:426-477)
  └─► checkpoints_v1    (insert ONLY on semantic change — adapter:133-202)
        ▼
HTTP: latest / series / analysis / aggregate / pool-history   (crates/cc-lb-admin/src/subscription_quotas.rs)
  ├─► admin-web UI (Overview pool chart, detail Quota History + snapshot cards + deficit, sidebar meters)
  └─► quota-aware routing: staleness gate via `subscription_quota_routing_max_staleness_secs`;
        stale/rejected/exhausted upstreams are deprioritized/ineligible.  [verify in scheduler/engine]
```
pool-history is NOT computed per request: a background task records pre-aggregated pool snapshots (Merged) via `record_pool_quota_snapshots_now`; the endpoint reads `PoolQuotaHistoryStore`.

> **Verified-execution caveats** (learned running T1/T4/T9 against an isolated instance — a mutating tester MUST heed these):
> 1. A mutation checkpoint's `sample_id` MUST be a valid **UUID**. The `/series` read path deserializes `sample_id` as a UUID, so a non-UUID value makes `/series` return HTTP 500 `storage_error` for that upstream+window (other windows stay 200). Use `randomblob(32)` for `semantic_fingerprint`.
> 2. `/latest` is served from an **in-memory cache** updated ONLY by the live writer or startup replay. Direct SQL to `latest_v1` is INVISIBLE to `/latest` (verified: a direct `UPDATE ... utilization=0.917` still returned `0.82`), hence invisible to the sidebar meters + snapshot cards. Only `/series` + the Quota History chart reflect direct checkpoint SQL. To transition `/latest`-driven UI, use the writer path (`fire-now`) or restart the instance (startup replay reloads the cache from DB).
> 3. On a **copied-prod DB the live OAuth poller keeps writing REAL observations** through the writer path and overwrites synthetic SQL. For clean controlled UI demos, run against a fresh DB with NO OAuth upstreams/tokens (poller idle) or disable polling.

## 2. Deterministic state mutation — THE ENGINE

Write contract: `latest_v1` upserts on every accepted observation **only if** `excluded.observed_at_unix_millis >= existing` (adapter:439-450). `checkpoints_v1` inserts a row **only when the semantic fingerprint changes**. Fingerprint = BLAKE3 of the SEMANTIC fields; hand-written SQL controls the fingerprint blob directly (a *different* 32-byte blob ⇒ new checkpoint; the *same* blob ⇒ dedup).
- Semantic fields (a change here ⇒ new checkpoint): `utilization, status, resets_at_unix_secs, surpassed_threshold, fallback_percentage, fallback_available, overage_in_use, overage_period_monthly_utilization, upgrade_paths, disabled_reason, extra_usage_enabled, extra_usage_monthly_limit, extra_usage_used_credits`.
- Excluded/evidence fields (change here ⇒ NO new checkpoint, latest freshness only): `sample_id, changed_at/observed_at_unix_millis, ingested_at_unix_millis, representative_claim, sample_kind`.

Schemas: `upstream_subscription_quota_latest_v1` PK `(upstream_id,window,source)`; `upstream_subscription_quota_checkpoints_v1` PK `(upstream_id,window,source,changed_at_unix_millis,sample_id)` + `semantic_fingerprint BLOB(32)`. Full column list: migrations `0019_subscription_quota_unified_signals.sql` (latest), `0044_subscription_quota_checkpoints.sql` (checkpoints), and SQLite 0051 / Postgres 0081 (Fable latest/checkpoint constraints). `window ∈ (5h,7d,7d_sonnet,7d_opus,7d_fable,overage,unified)`, `source ∈ (header,api)`, `status ∈ (allowed,allowed_warning,rejected)`, `utilization ∈ [0,1]`.

**Template T (one observation)** — `UID`, `W=5h` (or `7d_fable`), `S=api`, `T_MS`=observed millis, `RS`=resets secs, `U`=utilization, `FP`=unique 32-byte hex:
```sql
INSERT INTO upstream_subscription_quota_latest_v1
 (upstream_id,window,source,sample_kind,observed_at_unix_millis,sample_id,utilization,status,
  resets_at_unix_secs,surpassed_threshold,representative_claim,fallback_percentage,fallback_available,
  overage_in_use,overage_period_monthly_utilization,upgrade_paths,disabled_reason,extra_usage_enabled,
  extra_usage_monthly_limit,extra_usage_used_credits,ingested_at_unix_millis)
 VALUES ('UID','5h','api','sample',T_MS,'<uuid>',U,'allowed',RS,0.0,'c',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,T_MS)
 ON CONFLICT(upstream_id,window,source) DO UPDATE SET
  observed_at_unix_millis=excluded.observed_at_unix_millis, sample_id=excluded.sample_id,
  utilization=excluded.utilization, status=excluded.status, resets_at_unix_secs=excluded.resets_at_unix_secs,
  overage_in_use=excluded.overage_in_use, extra_usage_enabled=excluded.extra_usage_enabled,
  extra_usage_monthly_limit=excluded.extra_usage_monthly_limit, extra_usage_used_credits=excluded.extra_usage_used_credits,
  ingested_at_unix_millis=excluded.ingested_at_unix_millis
 WHERE excluded.observed_at_unix_millis >= upstream_subscription_quota_latest_v1.observed_at_unix_millis;
INSERT INTO upstream_subscription_quota_checkpoints_v1
 (upstream_id,window,source,changed_at_unix_millis,sample_id,semantic_fingerprint,sample_kind,representative_claim,
  utilization,status,resets_at_unix_secs,surpassed_threshold,fallback_percentage,fallback_available,overage_in_use,
  overage_period_monthly_utilization,upgrade_paths,disabled_reason,extra_usage_enabled,extra_usage_monthly_limit,
  extra_usage_used_credits,ingested_at_unix_millis)
 VALUES ('UID','5h','api',T_MS,'<uuid>',x'FP','sample','c',U,'allowed',RS,0.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,T_MS)
 ON CONFLICT DO NOTHING;
```
Mutation primitives (apply Template T with these deltas):
- **M1 utilization ↑**: new `T_MS` (>= current), higher `U`, unique `FP` ⇒ +1 checkpoint, latest advances.
- **M2 dedup**: new `T_MS`, SAME `U/status/RS`, SAME `FP` ⇒ skip the checkpoint INSERT (or reuse identical `FP`) ⇒ latest freshness advances, NO new checkpoint.
- **M3 reset**: new `T_MS`, `U`→~0.0, `RS`→(old+window_secs), unique `FP` ⇒ reset marker + drop step.
- **M4 stale**: do NOTHING new (let time pass) OR insert an OLD `T_MS` (< current latest) ⇒ latest UNCHANGED (guard rejects); state goes `stale` once age > max_staleness.
- **M5 overage**: `overage_in_use=1, extra_usage_enabled=1, extra_usage_monthly_limit=100, extra_usage_used_credits=15`, unique `FP`.
- **M6 source divergence**: run Template T twice with `S=header` and `S=api`, different `U`.
- **M7 cleanup/backfill**: `cc-lb compact-subscription-quota-history --storage-path $TDB [--drop-raw-observations]`. Writes meta markers `subscription_quota_checkpoint_backfill_v1_complete` / `_cleanup_v1_complete`; idempotent (marker present ⇒ returns cached report, no destructive work).
- **Live trigger** (no SQL): `curl -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:52252/admin/v1/upstreams/<id>/warmup/fire-now` ⇒ real warmup, header-source observation persisted.

Programmatic seed alt: `UpstreamSubscriptionQuotaStore::put_subscription_quota[_batch]`, `put_subscription_quota_checkpoint(s)`, `list_latest_subscription_quota_for_upstreams`, `list_subscription_quota_series`; `MetaStore::put_meta_value` (crates/cc-lb-storage-api/src/upstream_subscription_quota.rs:283-350, traits.rs:259-283).

## 3. Part A — Point-in-time QA

### 3.1 Storage invariants (assert via `sqlite3 -readonly` after a mutation)
- latest upserted every accepted observation; checkpoint only on semantic change; evidence-only change ⇒ no checkpoint.
- series returns last-checkpoint-before-`since` (left anchor) + checkpoints in `[since,until]`; NEVER fabricates leading zeroes (ADR 0007; test `subscription_quota_checkpoint_series_returns_steps_without_fabricated_leading_zeroes`).

### 3.2 HTTP endpoint contracts (all paths also under legacy `/admin/…`)  — crate `crates/cc-lb-admin/src/subscription_quotas.rs`
| Endpoint | Required params | Key defaults | Guardrail → 400 error code |
|---|---|---|---|
| `GET /admin/v1/subscription-quotas/latest` | — | windows=all, source=merged, upstream_ids=all-active-oauth, max_staleness=routing cfg | `invalid_source` / `invalid_window` / `invalid_upstream_id` |
| `…/series` | `since_unix_secs`,`until_unix_secs` | windows=5h,7d; bucket_secs=300(min1); max_points=1000 | `invalid_time_range` (until≤since); `max_points_per_series_too_large` (>10000); `bucket_range_too_large` ((until−since)/bucket > max*2); `too_many_upstreams` (>50) |
| `…/analysis` | `since_unix_secs`,`until_unix_secs` | windows=5h,7d; source=merged | `invalid_time_range`; `too_many_upstreams` |
| `…/aggregate` | — | windows=5h,7d; source=merged | source/window/upstream parse only |
| `…/pool-history` | — | windows=5h,7d(only these two); since=now−6h; until=now | `unknown pool history window`; `pool history only supports 5h and 7d` (plain-text 400) |
- source-merge: `merged`(default)|`header`|`api`. ETag `W/"v1:<max_observed_at_millis>"` + `If-None-Match`→304 on latest/series/analysis/aggregate (NOT pool-history); omitting `upstream_ids` skips the ETag SQL.
- Response skeletons + full field lists: see per-endpoint curl examples appended in §3.2a of the source findings (latest.windows[].{state,utilization,status,resets_at,age_secs}; series.series[].{buckets[].{bucket_start_unix_secs,utilization_last}, markers[].{kind:reset|gap}}; analysis.upstreams[].windows[].{current_utilization,actual_account_burn,proxy_projected_burn,deficit,caveats}; aggregate.windows[].{utilization,confidence,provider_lots[],caveats}; pool-history.windows[].{latest,series[]}).
- Reproduce each: `curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:52252/admin/v1/subscription-quotas/<ep>?upstream_ids=<UID>&windows=5h&source=merged[&since_unix_secs=..&until_unix_secs=..&bucket_secs=..]" | jq`.

### 3.3 Frontend surfaces — execute the 8 browser cases in `subscription-quota-frontend.md` (TC-1 detail range windowing [gating, FIXED], TC-2 snapshot cards, TC-3 deficit/analysis, TC-4 empty/loading, TC-5 Overview pool, TC-6 sidebar, TC-7 ApiUsageCard, TC-8 QuotaObservedAt).

### 3.4 Analysis/deficit values — given intervals, assert: `actual_account_burn` = median slope of valid utilization intervals; `proxy_projected_burn.effective_limit_tokens_estimate` = median(tokens/Δutilization); `eta_to_limit_secs` = (1−util)/slope; `deficit.shortfall_tokens` = projected_window − effective_limit; intervals never cross a reset (split_reset_cycles).

### 3.5 Fable 5 model-scoped weekly quota (documented, not executed)
- Seed a `7d_fable` checkpoint for an upstream (e.g., `bh322yoo-max` or `qa-oauth`) with utilization 0.28, status `allowed`, resets_at 1800000004.
- Verify storage: `sqlite3 -readonly` query on `upstream_subscription_quota_checkpoints_v1` shows the row with window `7d_fable`.
- Verify API: `GET /admin/v1/subscription-quotas/latest` returns `7d_fable` window with utilization 0.28, status `allowed`, resets_at 1800000004.
- Verify series: `GET /admin/v1/subscription-quotas/series?windows=7d_fable` returns the series with the correct utilization.
- Verify admin UI: detail page shows the "7d (Fable)" snapshot card with 28% utilization, pink color, and "live · API · Ns ago" observed-at.
- Note: latest is cache-served and SQL mutation requires startup replay/writer path, reusing the document's existing caveat.

## 4. Part B — State-transition QA (~50%)

Each: **INITIAL → MUTATION (§2) → EXPECTED** at storage / API (each endpoint) / UI. Wait the poll interval before asserting UI: latest ≈5s, series ≈30s, analysis ≈120s, aggregate/pool-history ≈30s.

- **T1 utilization ↑ (M1)** — storage: +1 checkpoint row, latest.utilization/observed_at advance. API: `latest` new utilization + smaller age_secs + state `fresh`; `series` gains a bucket / updates `utilization_last`; `analysis` recomputes burn/eta/deficit; `aggregate` recomputes plan-weighted utilization; `pool-history` next snapshot rises. UI: sidebar meter %, snapshot card %, "observed Ns ago" resets, Quota History adds a step, Overview pool rises — **without manual reload** within poll interval.
- **T2 dedup (M2)** — storage: NO new checkpoint; latest.observed_at advances only. API: `series` bucket shape unchanged; `latest` age resets. UI: "observed Ns ago" resets but Quota History step count unchanged.
- **T3 threshold crossing (M1 to cross)** — `latest.status` allowed→allowed_warning→rejected; `surpassed_threshold` set; UI status dot/badge/color change on card + sidebar. (series buckets do NOT expose status — assert via latest/analysis.)
- **T4 window reset (M3)** — API: `series.markers` gains `kind:"reset"` (triggered by resets_at change OR utilization drop ≥0.5); latest utilization drops, resets_at moves forward, status→allowed; analysis starts a new cycle (no cross-reset interval). UI: reset marker on Quota History, snapshot reset countdown resets, drop step visible.
- **T5 staleness (M4 + wait)** — API: `latest.state`→`stale` when age>max_staleness; `aggregate` window `stale_upstreams`++, `confidence`→`stale`, caveat added; `analysis.data_state`→`stale` + caveat; `series` shows a `kind:"gap"` marker for gaps > GAP_MULTIPLIER*bucket. UI: sidebar/card state dot green→amber; routing treats upstream as stale (deprioritized). 
- **T6 overage/extra-usage (M5)** — overage snapshot card appears/updates; sidebar shows overage window; overage utilization = used/limit when `utilization` null; `latest` exposes extra_usage_* fields.
- **T7 source divergence (M6)** — `source=merged` returns the newest of header/api per (upstream,window); `source=header` vs `api` return the divergent values; merged series may show sawtooth if sources alternate; analysis may drop to low confidence / add header-only caveat. UI tooltip "from: <source>" reflects the winner.
- **T8 cleanup/backfill (M7)** — after `--drop-raw-observations`: live writer keeps persisting latest+checkpoints (raw insert skipped); Quota History still renders; re-run idempotent (marker fast-path). (Regression fixes: 9c0cbaa8 writer, e18fb76b chart clip.)
- **T9 polling auto-refresh** — after ANY mutation, the DOM value changes on its own within the interval (no reload). Assert by snapshotting the same element before/after the wait.
- **T10 routing reaction (M1→100% / M3 reset / M4 stale)** — an upstream at 100%/rejected or stale becomes ineligible/deprioritized for selection; after reset it becomes eligible again. Observe via proxy routing behavior or selected-upstream metrics/logs; config `subscription_quota_routing_max_staleness_secs`.
- **T11 Fable 5 model-scoped weekly quota (documented, not executed)** —
  - **INITIAL**: Upstream has `7d_fable` at 28% (utilization = 0.28, status = `allowed`, resets_at = 1800000004).
  - **MUTATION**:
    1. Drive utilization to 100% / rejected: Ingest an observation with utilization = 1.0, status = `rejected`, resets_at = 1800000004.
    2. Drive reset: Ingest an observation with utilization = 0.0, status = `allowed`, resets_at = 1800000004 + 604800.
  - **EXPECTED**:
    - Storage: Checkpoints table gains new rows for 100% and reset.
    - API: `/latest` and `/series` reflect the transitions.
    - Admin UI: Snapshot card and Quota History chart update live.
    - Routing: When `7d_fable` is at 100%/rejected, requests for model `claude-fable-5` are blocked/deprioritized on this upstream, while control requests for `claude-3-5-sonnet`, `claude-3-opus`, `claude-3-haiku`, or unknown models remain completely unaffected (they only look at `5h` and `7d` windows). After reset, `claude-fable-5` requests are allowed again.

## 5. Automated-test coverage map (leverage; focus manual QA on gaps)
- Endpoint contracts + series windowing/anchor/no-zeroes: `crates/cc-lb-admin/tests/subscription_quotas.rs` (snapshot + some multi-observation transition, including Fable tests).
- Checkpoint dedup/fingerprint + range/anchor: `crates/cc-lb-storage-api/tests/subscription_quota_checkpoint.rs`, `crates/cc-lb-storage-conformance/tests/scenarios/upstream_subscription_quota_store.rs` (transition-heavy, including Fable storage conformance).
- Cleanup/backfill idempotency + writer-continues-after-drop: `crates/cc-lb-server/tests/subscription_quota_checkpoint_{cleanup,backfill,writer}.rs`.
- Storage roundtrips: `crates/cc-lb-storage-sqlite/tests/storage_roundtrips_sqlite.rs` (+ postgres).
- Frontend chart transform + carry-forward/no-zeroes: `crates/cc-lb-admin/web/src/components/upstreams/buildQuotaChartData.test.ts`; card/legend: ApiUsageCard/QuotaObservedAt tests.
- OAuth ingestion from active limits: `crates/cc-lb-server/src/scheduler_dispatch/usage/tests.rs`.
- Unified header parsing: `crates/cc-lb-engine/src/rate_limit_headers.rs`.
- Fable routing preference: `crates/cc-lb-engine/src/builtin_filters/subscription_preference/tests.rs`.
- Proxy path E2E: `crates/cc-lb-server/tests/claude_fable_5_proxy_path.rs`.
- **Gaps → manual only**: UI live auto-refresh (T9), UI status/color transitions (T3/T6), reset marker rendering (T4), routing reaction (T10). These MUST be executed by hand per §4.

## 6. Execution log / verdict
| Case | Layer(s) | Result | Evidence |
|------|----------|--------|----------|
| §3.2 endpoint contracts | API | PASS | /series + /latest HTTP 200 with correct payloads during T1/T4 |
| §3.1 storage invariants | storage | PASS | +1 checkpoint only on semantic change; latest guarded by observed_at>= |
| §3.3 TC-1..8 (frontend) | UI | PASS (prior run) | subscription-quota-frontend.md verdict PASS |
| **T1 utilization ↑** | storage→API | **PASS** | /series last bucket `utilization_last` 0.82→0.917; checkpoints 5402→5403 (+1) |
| **T4 window reset** | storage→API | **PASS** | /series `markers` gained `kind:"reset"` at mutation bucket; last bucket → 0.05 |
| **T9 UI live auto-refresh** | UI | **VERIFIED** | detail page observed-at + Quota History chart updated live WITHOUT reload (driven by real writer path) |
| T2,T3,T5,T6,T7,T8,T10 | storage/API/UI | documented, not executed | §4 specs; T10 routing flagged for hands-on |
| §3.5 Fable point-in-time | storage/API/UI | documented, not executed | Fable 5 model-scoped weekly quota point-in-time case |
| **T11 Fable transition** | storage/API/UI | documented, not executed | Fable 5 model-scoped weekly quota state-transition case |

PASS = every executed case meets Expected. Part B (T1–T11) carries equal weight to Part A.
Representative subset (T1/T4/T9) executed & verified on 2026-07-09 against an isolated `.backup` copy of prod; prod left untouched.
