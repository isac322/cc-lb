# Quota Full-Stack Functional QA — subscription-quota feature

Goal: functionally test the **entire** subscription-quota feature end to end —
storage → ingestion → HTTP endpoints → routing → admin-web UI — such that
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
- Auth: `localStorage['cc-lb-admin-token']` = env `CC_LB_ADMIN_TOKEN` (never echo). Header: `Authorization: Bearer $TOKEN`.
- Browser: `agent-browser` (`open`; `snapshot -i -c`; `click @eN`; `screenshot`; `wait --load networkidle`). Prefer fresh-snapshot `@eN` refs.
- API: `curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:52252<path>`.
- **DB read:** `sqlite3 -readonly "file:<DB>?mode=ro" "<SQL>"` where `DB=$QA_DB`.

### 0.1 Isolated test instance (REQUIRED for state-transition mutations — never mutate shared operational data)
```
TDB=$QA_ROOT/storage.sqlite ; mkdir -p "$QA_ROOT"
# 1. run migrations against a fresh DB (initialize) via the CLI or a serve boot:
CC_LB_ADMIN_TOKEN=qa-token CC_LB_MASTER_KEY=<32B-base64> \
  cc-lb serve --config "$QA_ROOT/config.toml"   # config points [storage].path=$TDB, listener ports off the default
# 2. create an OAuth upstream via admin API to get a real upstream_id UUID:
curl -s -H "Authorization: Bearer qa-token" -H 'content-type: application/json' \
  -d '{"name":"example-upstream","kind":"anthropic_oauth"}' http://127.0.0.1:53252/admin/v1/upstreams
# 3. seed/mutate quota via §2 SQL against $TDB (service can stay up; latest/series are read per request).
```
Alternative (no full instance): mutate `$TDB` with §2 SQL, then run point-in-time API/DB assertions with a throwaway `cc-lb serve` pointed at `$TDB`. Do NOT hand-write into shared operational data.

### 0.2 Fixture data requirements
- Use an isolated OAuth upstream named `example-upstream` for the named cases
  below. Add other OAuth upstreams only when a case needs a distinct state.
- Include quota windows covering the relevant `5h`, `7d`, model-scoped, overage,
  and optional Fable paths; use the source and status values required by each
  mutation.
- Include one non-OAuth `anthropic_api_key` upstream for the non-quota path.
- Keep utilization, observation times, and upstream population synthetic and
  fixture-specific; no operational snapshot is required.

## 1. Layers & data flow

```
Ingestion (3 origins):
  (a) periodic OAuth usage poller -> source = "api" (the usage parser ingests `limits[]` entries where `kind = "weekly_scoped"` and model display name is "Fable" regardless of `is_active`; `is_active` only identifies the currently binding limit and is not an ingestion gate)
  (b) proxy response headers (live) -> source = "header" (parses unified `7d_oi` headers)
  (c) admin POST /admin/v1/upstreams/{id}/warmup/fire-now -> real warmup, parses headers  (crates/cc-lb-admin/src/v1/upstreams.rs:101-103, 808-830)
        │  SubscriptionQuotaObservationRecord
        ▼
put_subscription_quota[_batch]   (crates/cc-lb-storage-sqlite/src/adapter/upstream_subscription_quota.rs:19-43)
  ├─► observations_v1   (only if table present; dropped after cleanup — lib.rs:55-71)
  ├─► latest_v1         (ALWAYS upsert, guarded observed_at>= — adapter:426-477)
  └─► checkpoints_v1    (insert ONLY on semantic change — adapter:133-202)
        ▼
HTTP: latest / series / aggregate / pool-history   (crates/cc-lb-admin/src/subscription_quotas.rs)
  ├─► admin-web UI (Overview pool chart, detail Quota History + snapshot cards, sidebar meters)
  └─► quota-aware routing: staleness gate via `subscription_quota_routing_max_staleness_secs`;
        stale/rejected/exhausted upstreams are deprioritized/ineligible.  [verify in scheduler/engine]
```
pool-history is NOT computed per request: a background task records pre-aggregated pool snapshots (Merged) via `record_pool_quota_snapshots_now`; the endpoint reads `PoolQuotaHistoryStore`.

> **Fixture execution caveats** (the mutating tester MUST heed these):
> 1. A mutation checkpoint's `sample_id` MUST be a valid **UUID**. The `/series` read path deserializes `sample_id` as a UUID, so a non-UUID value makes `/series` return HTTP 500 `storage_error` for that upstream+window (other windows stay 200). Use `randomblob(32)` for `semantic_fingerprint`.
> 2. `/latest` is served from an **in-memory cache** updated only by the live writer or startup replay. Direct SQL to `latest_v1` is not visible to `/latest`, so it does not update sidebar meters or snapshot cards. Only `/series` plus the Quota History chart reflect direct checkpoint SQL. To transition `/latest`-driven UI, use the writer path (`fire-now`) or restart the instance (startup replay reloads the cache from DB).
> 3. Do not use a shared operational database for controlled UI demos: a live OAuth poller can overwrite synthetic SQL through the writer path. Use a fresh isolated database with polling idle or disabled.

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

Programmatic seed alt: `UpstreamSubscriptionQuotaStore::record_subscription_quota_samples`, `put_subscription_quota_checkpoints`, `list_latest_subscription_quota_for_upstreams`, `list_subscription_quota_series`; `MetaStore::put_meta_value` (crates/cc-lb-storage-api/src/upstream_subscription_quota.rs, traits.rs).

## 3. Part A — Point-in-time QA

### 3.1 Storage invariants (assert via `sqlite3 -readonly` after a mutation)
- latest upserted every accepted observation; checkpoint only on semantic change; evidence-only change ⇒ no checkpoint.
- series returns last-checkpoint-before-`since` (left anchor) + checkpoints in `[since,until]`; NEVER fabricates leading zeroes (ADR 0007; test `subscription_quota_checkpoint_series_returns_steps_without_fabricated_leading_zeroes`).

### 3.2 HTTP endpoint contracts — crate `crates/cc-lb-admin/src/subscription_quotas.rs`
| Endpoint | Required params | Key defaults | Guardrail → 400 error code |
|---|---|---|---|
| `GET /admin/v1/subscription-quotas/latest` | — | windows=all, source=merged, upstream_ids=all-active-oauth, max_staleness=routing cfg | `invalid_source` / `invalid_window` / `invalid_upstream_id` |
| `…/series` | `since_unix_secs`,`until_unix_secs` | windows=5h,7d; bucket_secs=300(min1); max_points=1000 | `invalid_time_range` (until≤since); `max_points_per_series_too_large` (>10000); `bucket_range_too_large` ((until−since)/bucket > max*2); `too_many_upstreams` (>50) |
| `…/aggregate` | — | windows=5h,7d; source=merged | source/window/upstream parse only |
| `…/pool-history` | — | windows=5h,7d(only these two); since=now−6h; until=now | `unknown pool history window`; `pool history only supports 5h and 7d` (plain-text 400) |
- source-merge: `merged`(default)|`header`|`api`. ETag `W/"v1:<max_observed_at_millis>"` + `If-None-Match`→304 on latest/series/aggregate (NOT pool-history); omitting `upstream_ids` skips the ETag SQL.
- Response skeletons + full field lists: see the endpoint contract notes in this document (latest.windows[].{state,utilization,status,resets_at,age_secs}; series.series[].{buckets[].{bucket_start_unix_secs,utilization_last}, markers[].{kind:reset|gap}}; aggregate.windows[].{utilization,confidence,provider_lots[],caveats}; pool-history.windows[].{latest,series[]}).
- Reproduce each: `curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:52252/admin/v1/subscription-quotas/<ep>?upstream_ids=<UID>&windows=5h&source=merged[&since_unix_secs=..&until_unix_secs=..&bucket_secs=..]" | jq`.

### 3.3 Frontend surfaces — execute the 7 browser cases in `subscription-quota-frontend.md` (TC-1 detail range windowing, TC-2 snapshot cards, TC-4 empty/loading, TC-5 Overview pool, TC-6 sidebar, TC-7 ApiUsageCard, TC-8 QuotaObservedAt).

### 3.5 Fable 5 model-scoped weekly quota
- Seed a `7d_fable` checkpoint for `example-upstream` with utilization 0.28, status `allowed`, resets_at 1800000004.
- Verify storage: `sqlite3 -readonly` query on `upstream_subscription_quota_checkpoints_v1` shows the row with window `7d_fable`.
- Verify API: `GET /admin/v1/subscription-quotas/latest` returns `7d_fable` window with utilization 0.28, status `allowed`, resets_at 1800000004.
- Verify series: `GET /admin/v1/subscription-quotas/series?windows=7d_fable` returns the series with the correct utilization.
- Verify admin UI: detail page shows the "7d (Fable)" snapshot card with 28% utilization, pink color, and "live · API · Ns ago" observed-at.
- Note: latest is cache-served and SQL mutation requires startup replay/writer path, reusing the document's existing caveat.

### 3.6 Memory-allocation root-fix non-regression, point-in-time: series, aggregate provider lots, and token totals
- **Source and context:** documented for isolated execution; no operational evidence is retained here.
- **Fixture state:** Seed the disposable DB with the documented quota checkpoint schema for `$QA_UPSTREAM_ID`: a fresh merged `5h` row, a valid UUID `sample_id`, `utilization=0.20`, and the fixture's deterministic capacity observations. Start or replay the isolated instance after seeding.
- **Live invocation:**
  ```bash
  NOW="$(date +%s)"; SINCE="$((NOW - 21600))"
  sqlite3 -readonly "file:$QA_DB?mode=ro" \
    "SELECT upstream_id, window, utilization, resets_at_unix_secs FROM upstream_subscription_quota_checkpoints_v1 WHERE upstream_id='$QA_UPSTREAM_ID' AND window='5h' ORDER BY changed_at_unix_millis;" \
    >"$QA_ROOT/c1-checkpoints.txt"
  curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    "$QA_ADMIN/admin/v1/subscription-quotas/series?upstream_ids=$QA_UPSTREAM_ID&windows=5h&source=merged&since_unix_secs=$SINCE&until_unix_secs=$NOW&bucket_secs=60" \
    -o "$QA_ROOT/c1-series.json"
  curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
    "$QA_ADMIN/admin/v1/subscription-quotas/aggregate?upstream_ids=$QA_UPSTREAM_ID&windows=5h&source=merged" \
    -o "$QA_ROOT/c1-aggregate.json"
  jq -e --arg id "$QA_UPSTREAM_ID" '
    (.series[] | select(.upstream_id == $id and .window == "5h") | .buckets[-1].utilization_last == 0.20)
  ' "$QA_ROOT/c1-series.json"
  jq -e --arg id "$QA_UPSTREAM_ID" '
    .windows[] | select(.window == "5h") |
    (.used_tokens >= 0) and
    (.provider_lots | length == 1) and
    (.provider_lots[0].upstream_id == $id) and
    (.provider_lots[0].capacity_estimate_tokens != null) and
    (.provider_lots[0].capacity_to_now_tokens_estimate != null) and
    (.capacity_to_now_tokens_estimate == .provider_lots[0].capacity_to_now_tokens_estimate)
  ' "$QA_ROOT/c1-aggregate.json"
  ```
- **Expected observable result:** The read-only DB output contains the seeded `5h` checkpoint. `series[].buckets[-1].utilization_last` is exactly `0.20`. Aggregate `windows[].used_tokens`, `capacity_to_now_tokens_estimate`, and the sole `provider_lots[0]` token-capacity fields are present and internally equal as asserted. HTTP responses are `200`.

## 4. Part B — State-transition QA

Each: **INITIAL → MUTATION (§2) → EXPECTED** at storage / API (each endpoint) / UI. Wait the poll interval before asserting UI: latest ≈5s, series ≈30s, aggregate/pool-history ≈30s.

- **T1 utilization ↑ (M1)** — storage: +1 checkpoint row, latest.utilization/observed_at advance. API: `latest` new utilization + smaller age_secs + state `fresh`; `series` gains a bucket / updates `utilization_last`; `aggregate` recomputes plan-weighted utilization; `pool-history` next snapshot rises. UI: sidebar meter %, snapshot card %, "observed Ns ago" resets, Quota History adds a step, Overview pool rises — **without manual reload** within poll interval.
- **T2 dedup (M2)** — storage: NO new checkpoint; latest.observed_at advances only. API: `series` bucket shape unchanged; `latest` age resets. UI: "observed Ns ago" resets but Quota History step count unchanged.
- **T3 threshold crossing (M1 to cross)** — `latest.status` allowed→allowed_warning→rejected; `surpassed_threshold` set; UI status dot/badge/color change on card + sidebar. (series buckets do NOT expose status — assert via latest.)
- **T4 window reset (M3)** — API: `series.markers` gains `kind:"reset"` (triggered by resets_at change OR utilization drop ≥0.5); latest utilization drops, resets_at moves forward, status→allowed; UI: reset marker on Quota History, snapshot reset countdown resets, drop step visible.
- **T5 staleness (M4 + wait)** — API: `latest.state`→`stale` when age>max_staleness; `aggregate` window `stale_upstreams`++, `confidence`→`stale`, caveat added; `series` shows a `kind:"gap"` marker for gaps > GAP_MULTIPLIER*bucket. UI: sidebar/card state dot green→amber; routing treats upstream as stale (deprioritized). 
- **T6 overage/extra-usage (M5)** — overage snapshot card appears/updates; sidebar shows overage window; overage utilization = used/limit when `utilization` null; `latest` exposes extra_usage_* fields.
- **T7 source divergence (M6)** — `source=merged` returns the newest of header/api per (upstream,window); `source=header` vs `api` return the divergent values; merged series may show sawtooth if sources alternate; UI tooltip "from: <source>" reflects the winner.
- **T8 cleanup/backfill (M7)** — after `--drop-raw-observations`: live writer keeps persisting latest+checkpoints (raw insert skipped); Quota History still renders; re-run idempotent (marker fast-path).
- **T9 polling auto-refresh** — after ANY mutation, the DOM value changes on its own within the interval (no reload). Assert by snapshotting the same element before/after the wait.
- **T10 routing reaction (M1→100% / M3 reset / M4 stale)** — an upstream at 100%/rejected or stale becomes ineligible/deprioritized for selection; after reset it becomes eligible again. Observe via proxy routing behavior or selected-upstream metrics/logs; config `subscription_quota_routing_max_staleness_secs`.
- **T11 Fable 5 model-scoped weekly quota** —
  - **INITIAL**: Upstream has `7d_fable` at 28% (utilization = 0.28, status = `allowed`, resets_at = 1800000004).
  - **MUTATION**:
    1. Drive utilization to 100% / rejected: Ingest an observation with utilization = 1.0, status = `rejected`, resets_at = 1800000004.
    2. Drive reset: Ingest an observation with utilization = 0.0, status = `allowed`, resets_at = 1800000004 + 604800.
  - **EXPECTED**:
    - Storage: Checkpoints table gains new rows for 100% and reset.
    - API: `/latest` and `/series` reflect the transitions.
    - Admin UI: Snapshot card, Quota History chart, and sidebar `Fable` meter update live from 28% to exhausted and then reset.
    - Routing pressure: For exact `claude-fable-5`, trace `quota_urgency_7d` is effective weekly pressure `(U_7d^6 + U_fable^6)^(1/6)`, and `quota_urgency_combined` is `(U_5h^6 + U_weekly^6)^(1/6)`. Before exhaustion, increasing scoped use-it-or-lose-it pressure changes effective weight and selection share; missing/stale/malformed/elapsed scoped data contributes zero but leaves completeness partial.
    - Routing eligibility: When `7d_fable` is at 100%/rejected, requests for model `claude-fable-5` are blocked/deprioritized on this upstream, while control requests for `claude-3-5-sonnet`, `claude-3-opus`, `claude-3-haiku`, dated/future Fable-like IDs, or unknown models remain completely unaffected (they only look at `5h` and `7d` windows). After reset, `claude-fable-5` requests are allowed again.
    - Routing trace/profile: Exact Fable reports `rendezvous_salt_version = "v11-fable"`; controls remain `v11`. Overage fallback for every model remains on the v10 selection salt.

- **T12 Memory-allocation root-fix non-regression, state transition: utilization increase reaches latest, aggregate, and pool surfaces**
  - **Source and context:** documented for isolated execution; no operational evidence is retained here.
  - **Fixture state:** From C1.1, insert a later semantic checkpoint (`utilization=0.80`, unique 32-byte semantic fingerprint) and drive the same observation through the isolated writer path so the in-memory latest cache is updated. Trigger a pool snapshot on the fixture's scheduler path before observing pool history.
  - **Live invocation:**
    ```bash
    curl -fsS -X POST -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
      "$QA_ADMIN/admin/v1/upstreams/$QA_UPSTREAM_ID/warmup/fire-now" \
      -o "$QA_ROOT/c1-fire-now.json"
    curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
      "$QA_ADMIN/admin/v1/subscription-quotas/latest?upstream_ids=$QA_UPSTREAM_ID&windows=5h&source=merged" \
      -o "$QA_ROOT/c1-latest-after.json"
    curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
      "$QA_ADMIN/admin/v1/subscription-quotas/aggregate?upstream_ids=$QA_UPSTREAM_ID&windows=5h&source=merged" \
      -o "$QA_ROOT/c1-aggregate-after.json"
    curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
      "$QA_ADMIN/admin/v1/subscription-quotas/pool-history?windows=5h" \
      -o "$QA_ROOT/c1-pool-history-after.json"
    jq -e '.upstreams[0].windows[] | select(.window == "5h") | .state == "fresh" and .utilization == 0.80' "$QA_ROOT/c1-latest-after.json"
    jq -e '.windows[] | select(.window == "5h") | .utilization == 0.80 and .provider_lots[0].utilization == 0.80' "$QA_ROOT/c1-aggregate-after.json"
    jq -e '.windows[] | select(.window == "5h") | (.latest != null) and (.series | length > 0)' "$QA_ROOT/c1-pool-history-after.json"
    ```
  - **Expected observable result:** All three admin calls return `200`. `/latest` reports `state:"fresh"` and `utilization:0.80`. Aggregate reports `utilization:0.80` both on the window and its provider lot. Pool history contains a non-null `latest` and at least one time-series point. The observation must be generated by the live isolated writer, not a post-startup SQL-only change.

- **T13 Memory-allocation root-fix non-regression, state transition: reset cycle remains visible as a reset, not fabricated usage**
  - **Source and context:** documented for isolated execution; no operational evidence is retained here.
  - **Fixture state:** Add a later semantic checkpoint with `utilization=0.05`, `status="allowed"`, a later `resets_at_unix_secs`, and a new semantic fingerprint. Update the isolated latest cache through the writer path.
  - **Live invocation:**
    ```bash
    NOW="$(date +%s)"; SINCE="$((NOW - 21600))"
    curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
      "$QA_ADMIN/admin/v1/subscription-quotas/series?upstream_ids=$QA_UPSTREAM_ID&windows=5h&source=merged&since_unix_secs=$SINCE&until_unix_secs=$NOW&bucket_secs=60" \
      -o "$QA_ROOT/c1-reset-series.json"
    curl -fsS -H "Authorization: Bearer $QA_ADMIN_TOKEN" \
      "$QA_ADMIN/admin/v1/subscription-quotas/latest?upstream_ids=$QA_UPSTREAM_ID&windows=5h&source=merged" \
      -o "$QA_ROOT/c1-reset-latest.json"
    jq -e '(.series[].markers | any(.kind == "reset")) and (.series[].buckets[-1].utilization_last == 0.05)' "$QA_ROOT/c1-reset-series.json"
    jq -e '.upstreams[0].windows[] | select(.window == "5h") | .utilization == 0.05 and .status == "allowed"' "$QA_ROOT/c1-reset-latest.json"
    ```
  - **Expected observable result:** Both endpoints return `200`. The `5h` series contains a `markers[]` entry with `kind:"reset"`, its last utilization is exactly `0.05`, and `/latest` shows `status:"allowed"` with the new reset-cycle utilization. No leading `0%` bucket is fabricated before the left anchor.



## 5. Automated-test coverage map (leverage; focus manual QA on gaps)
- Endpoint contracts + series windowing/anchor/no-zeroes: `crates/cc-lb-admin/tests/subscription_quotas.rs` (snapshot + some multi-observation transition, including Fable tests).
- Checkpoint dedup/fingerprint + range/anchor: `crates/cc-lb-storage-api/tests/subscription_quota_checkpoint.rs`, `crates/cc-lb-storage-conformance/tests/scenarios/upstream_subscription_quota_store.rs` (transition-heavy, including Fable storage conformance).
- Cleanup/backfill idempotency + writer-continues-after-drop: `crates/cc-lb-server/tests/subscription_quota_checkpoint_{cleanup,backfill,writer}.rs`.
- Storage roundtrips: `crates/cc-lb-storage-sqlite/tests/storage_roundtrips_sqlite.rs` (+ postgres).
- Frontend chart transform + carry-forward/no-zeroes: `crates/cc-lb-admin/web/src/components/upstreams/buildQuotaChartData.test.ts`; card/legend: ApiUsageCard/QuotaObservedAt tests.
- OAuth ingestion and `is_active` semantics: `crates/cc-lb-server/src/scheduler_dispatch/usage/tests.rs` (`is_active` marks the binding limit; weekly-scoped Fable observations are ingested regardless).
- Unified header parsing: `crates/cc-lb-engine/src/rate_limit_headers.rs`.
- Fable routing preference: `crates/cc-lb-engine/src/builtin_filters/subscription_preference/tests.rs`.
- Proxy path E2E: `crates/cc-lb-server/tests/claude_fable_5_proxy_path.rs`.
- **Gaps → manual only**: UI live auto-refresh (T9), UI status/color transitions (T3/T6), reset marker rendering (T4), routing reaction (T10). These MUST be executed by hand per §4.

## 6. Execution log / verdict

Record results only from the current isolated run. Do not copy operational snapshots, private database counts, host identifiers, or execution evidence into this public scenario.

PASS = every executed case meets its expected outcome. A case that was not executed remains unverified.
