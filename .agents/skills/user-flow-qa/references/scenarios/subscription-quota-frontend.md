# Quota Frontend QA Plan — PR #372 (subscription-quota checkpoints)

Reproducible QA for **every** subscription-quota surface in the cc-lb admin web.
A reader with zero prior context can execute this end to end and reach the same verdict.

Fix under test: `crates/cc-lb-admin/web/src/routes/upstreams.tsx` Quota History `<XAxis>`
gained `allowDataOverflow` so recharts **clips** the numeric x-domain to
`[sinceUnixSecs, nowUnixSecs]` instead of extending it to fit the checkpoint
left-anchor (which sits far before `since`). Root cause: PR #372 switched the
series API from dense raw observations to sparse checkpoints + a left-anchor
(last checkpoint before `since`); without `allowDataOverflow` recharts stretched
the axis back to the anchor, so every range (1h/6h/24h/7d) rendered the same span.

---

## 0. Preconditions / environment

- Service: local `cc-lb.service`; admin API + embedded SPA at `http://127.0.0.1:52252`.
  - Proxy `:52251`, admin `:52252`, metrics `:52253`.
- The SPA is served from the binary via `rust-embed` (`web/dist`); a **frontend change
  requires `bun run build` in `crates/cc-lb-admin/web` + a binary rebuild + redeploy**
  before it is visible at `:52252`. (For dev-only iteration: `bun run dev` in that dir.)
- Auth: SPA login gate stores the admin bearer token in `localStorage['cc-lb-admin-token']`.
  Token value = env var `CC_LB_ADMIN_TOKEN` from `~/.config/cc-lb/env`
  (`sed -n 's/^CC_LB_ADMIN_TOKEN=//p' ~/.config/cc-lb/env`). Never echo the value.
- Browser driver: `agent-browser` (open / snapshot -i / click @eN / screenshot).
  Auth without the login form: `localStorage.setItem('cc-lb-admin-token', <token>)` then reload.
- Direct API check (no browser) for a given upstream `<UID>`:
  ```
  curl -s -H "Authorization: Bearer $TOKEN" \
    "http://127.0.0.1:52252/admin/v1/subscription-quotas/series?upstream_ids=<UID>&windows=5h,7d,7d_sonnet,7d_opus,7d_fable,overage&source=merged&since_unix_secs=<S>&until_unix_secs=<U>&bucket_secs=<B>"
  ```
  Range→(offsetSecs, bucketSecs): 1h→(3600,60) 6h→(21600,60) 24h→(86400,300) 7d→(604800,1800).

## 1. Context data (reference instance; equivalents required, not exact values)

Enumerate OAuth upstreams with quota data via the sidebar or:
`sqlite3 -readonly "file:/home/bhyoo/.local/share/cc-lb/storage.sqlite?mode=ro" \
  "SELECT DISTINCT upstream_id FROM upstream_subscription_quota_latest_v1;"`

Reference OAuth upstreams and their latest 5h/7d utilization at capture time:
- `bear-team` — 5h 100%, 7d 42%, Extra 13%  (good: high 5h, has overage)
- `bear-max` — 5h 0%, 7d 100%              (good: 7d saturated)
- `isac-personal` — 5h 0%, 7d 100%
- `bh322yoo-max` — 5h 45%, 7d 71%          (**primary target: mid values, multiple windows**)
- `runbear0001-max` — 5h 14%, 7d 5%
- `Runbear` — 5h 12%, 7d 12%, Extra 38%
- `fake-upstream` — `anthropic_api_key` (non-OAuth → ApiUsageCard, not quota)

Required test-data properties (for a fresh instance to reproduce):
- ≥1 OAuth upstream whose quota was **flat for > 24h** (exposes the bug most sharply:
  pre-fix 1h≡6h≡24h identical; post-fix each range differs by x-axis span).
- ≥1 upstream with `overage`/Extra usage (exercises the overage window + snapshot card).
- ≥1 non-OAuth (`anthropic_api_key`) upstream (ApiUsageCard path).

## 2. Test cases

Legend: **Initial** = state before action · **Steps** = exact actions · **Expected** = pass criteria.

### TC-1 — Detail "Quota History" range windowing (PRIMARY REGRESSION)
- Surface: Upstream detail → "Subscription Quota" → "Quota History" (`upstreams.tsx`).
- Data: select `bh322yoo-max` (or any OAuth upstream flat > 24h).
- Initial: detail view open; default range = `7d`.
- Steps: click range `1h`, screenshot; `6h`, screenshot; `24h`, screenshot; `7d`, screenshot.
- Expected (POST-FIX):
  - x-axis **left-edge label** ≈ `now − range` for each: `1h`→~1h ago, `6h`→~6h ago,
    `24h`→~24h ago, `7d`→~7d ago. Right edge ≈ now for all.
  - The four screenshots are **visibly different** (x-axis span grows 1h→6h→24h→7d).
  - `1h` and `6h` are **NOT** byte/pixel identical (pre-fix bug); likewise `24h`≠`7d`.
  - No x-axis label older than the selected range (pre-fix showed ~16 days e.g. `6/23…7/9`).
  - Line is continuous across the visible span (left-anchor value carried to left edge);
    no fabricated 0% leading segment.
  - When Fable data exists, the pink `7d_fable` line is rendered on the chart, and the legend includes "7d (Fable)" with pink color.
  - Range/freshness window gating (added with `quotaWindowVisibility.ts`): a window is drawn (Area + legend + reset/start markers) for the selected range ONLY when its latest `observed_at_unix_millis` is at or after `sinceUnixSecs` (i.e. `observed_at_unix_millis >= sinceUnixSecs * 1000`) AND the series carries a bucket with `bucket_start_unix_secs >= sinceUnixSecs`. This applies uniformly to every window including `5h`/`7d`. Consequences to assert:
    - A window whose last observation predates the selected range is absent from that range's chart even if `/latest` still returns it.
    - A window that has only a left-anchor bucket before `since` (no in-range checkpoint) is absent even if recently observed.
    - `overage` additionally requires `extra_usage_enabled` or a non-null `extra_usage_monthly_limit`.
    - When no window passes the gate, the chart shows the "No data in range" empty state rather than an empty axes-only plot.
- Cross-check (API): the `series` payloads for 1h vs 6h may still be equal (backend returns
  anchor→until fill by design — see ADR 0007); the **frontend clip** is what must differ.
  So verdict is visual/x-domain, not payload bytes.

### TC-2 — Detail snapshot cards (per-window)
- Surface: detail → grid of window cards (5h, 7d, 7d_sonnet, 7d_opus, 7d_fable, overage).
- Initial: OAuth upstream selected.
- Steps: read each card.
- Expected: each active window shows utilization %, a meter bar matching the %, source
  (Header/API/—), observed-at relative time, reset countdown or "not started".
  `overage` card present only when overage billing enabled or monthly limit set.
  `7d_fable` card is rendered conditionally only when Fable data exists (state is not missing).
  When rendered, the `7d_fable` card displays "7d (Fable)" with pink color and correct utilization.
  Values agree with `/subscription-quotas/latest` for that upstream.
  - Card freshness rules (added with `quotaWindowVisibility.ts` `selectQuotaCardSnapshots`):
    - `5h` and `7d` cards ALWAYS render whenever the upstream has a latest response, including when their state is `missing` (they render "no data"/`—` via `SnapshotStatusComposite`). This is a hard requirement: the two default windows must never disappear.
    - Model-scoped cards (`7d_sonnet`, `7d_opus`, `7d_fable`) render only when state is not `missing` AND `observed_at_unix_millis` is within the last week (`>= (nowUnixSecs - 604800) * 1000`, boundary inclusive). A model window last observed more than 7 days ago has its card removed even though `/latest` still returns it as `stale`.
    - `overage` card unchanged: renders when non-missing AND (`extra_usage_enabled` or `extra_usage_monthly_limit` set).
    - Card order follows `QUOTA_WINDOW_ORDER` (5h, 7d, 7d_sonnet, 7d_opus, 7d_fable, overage).

### TC-4 — Detail empty / loading states
- Steps: (a) select an OAuth upstream with no quota rows; (b) observe during initial load.
- Expected: (a) "No data in range" empty state in Quota History and/or
  "No subscription quota data for <name>"; (b) "Loading…"/skeleton, no flstyle flash of
  fabricated data.

### TC-5 — Overview pool quota (regression guard — must stay correct)
- Surface: Overview page → Pool quota card (stacked bar 5h/7d, popover, trend chart, legend).
- Initial: default range = `24h`.
- Steps: cycle `1h/6h/24h/7d`; hover/click a stacked-bar segment.
- Expected: trend chart already clips domain (has `allowDataOverflow`); each range shows a
  distinct x-span; popover lists upstreams with utilization / capacity ratio / weighted impact;
  "No timeline data yet for this range" when empty. **This surface must be unchanged by the fix.**

### TC-6 — Upstreams sidebar mini quota meters
- Surface: upstreams list; OAuth rows.
- Expected: mini meter bars for active windows (5h, 7d, 7d_fable [only when data exists and observed within the last week], +overage when enabled); state dot
  green(fresh)/amber(stale) with hint (state/source/observed); % text; overage utilization
  computed from `extra_usage_used_credits / extra_usage_monthly_limit` when `utilization` null.
  Values agree with `latest`. The `7d_fable` label must be exactly `Fable` (not `7d (Fable)`) to fit the 32px cell.

### TC-7 — ApiUsageCard (non-OAuth upstream)
- Surface: detail of a `anthropic_api_key` upstream → "API Usage".
- Steps: toggle range `24h`/`7d`, metric `Tokens`/`Cost`.
- Expected: chart data changes with range (usage endpoint is server-windowed) and with metric;
  legend reserves height in loading/empty/populated (no layout shift); "No usage in selected range" when empty.

### TC-8 — QuotaObservedAt shared component
- Expected: renders relative observed time where snapshots appear; renders `—` when
  `observed_at_unix_millis` is null.

### TC-9 — Fable 5 Quota Transition (Desktop & Mobile) (automated Playwright route-mocked transition executed; isolated real-backend/manual mutation not executed)
- Surface: Upstream detail page → "Subscription Quota" section, and Upstreams sidebar.
- Initial: OAuth upstream selected, no Fable data exists yet.
- Steps:
  1. Open the upstream detail page on desktop (viewport 1280x800) and mobile (viewport 375x667).
  2. Take a screenshot of the "Subscription Quota" section and the sidebar before any Fable data is seeded.
  3. Seed/mutate a `7d_fable` checkpoint (e.g., 28% utilization).
  4. Wait for the poll interval (or trigger a reload/restart if testing `/latest` cache-served behavior).
  5. Observe the transition on both desktop and mobile viewports.
  6. Push the `7d_fable` observation to > 1 week old, wait for poll, and observe the transition.
- Expected:
  - Before: The "7d (Fable)" card is completely hidden (conditional rendering when data is missing). The Quota History chart does not show the Fable line or legend item. The sidebar does not show the Fable mini meter.
  - After seeding:
    - On Desktop: The "7d (Fable)" card appears in the grid with 28% utilization, pink color, and correct observed-at text. The Quota History chart adds a pink line for `7d_fable` and a corresponding legend item. The sidebar adds a `Fable` mini meter.
    - On Mobile: The layout adapts gracefully (cards stack or wrap without clipping or horizontal overflow). The mini meters in the sidebar (if visible) or the detail cards are fully readable.
    - The transition occurs smoothly without layout shifts or broken elements.
  - After aging > 1 week: The "7d (Fable)" card disappears. The sidebar `Fable` mini meter disappears. `5h` and `7d` remain visible.

### TC-10 — Range-scoped stale window visibility (Desktop & Mobile) (documented, not yet executed)
- Surface: Upstream detail page → "Subscription Quota" → "Quota History" chart + snapshot card grid.
- Fix under test: `crates/cc-lb-admin/web/src/components/upstreams/quotaWindowVisibility.ts` (`selectVisibleGraphWindows`, `selectQuotaCardSnapshots`) wired into `upstreams.tsx` Legend, markers, Area, and card grid. Root cause: the chart legend/Area and the card grid derived their window set from the latest snapshot regardless of the selected range, so a window that stopped receiving data kept showing (a flat carried-forward line + a stale card). Freshness truth is per-window `observed_at_unix_millis` (MILLISECONDS) from the `/latest` sidecar, not `state` (which goes fresh→stale but never →missing for abandoned windows).
- Seed (isolated instance): one OAuth upstream with, relative to now —
  - `5h`: fresh (observed_at = now), in-range checkpoint.
  - `7d`: fresh (observed_at = now), in-range checkpoint.
  - `7d_sonnet`: STALE — observed_at ≈ 10 days ago, only a checkpoint ≈ 10 days ago (out of every range window, older than 1 week). `/latest` returns it with `state='stale'`, non-null `observed_at_unix_millis`.
  - `7d_opus`: observed_at ≈ 2 days ago with an in-range checkpoint ≈ 2 days ago (inside the 7d window, outside 1h/6h/24h).
  Seed SQLite then RESTART the backend so the cache-served `/latest` replays the rows.
- Point-in-time (Given fixed seed):
  - Graph, cycling 1h→6h→24h→7d: `7d (Sonnet)` NEVER appears (stale, out of range). `7d (Opus)` appears ONLY in the 7d range. `5h`/`7d` appear in ranges where they have in-range buckets.
  - Cards: `5h` and `7d` ALWAYS present. `7d (Sonnet)` card ABSENT (observed > 7 days). `7d (Opus)` card PRESENT (observed 2 days ago). Assert `/latest` still returns `7d_sonnet` to prove the hiding is a frontend decision, not missing data.
  - Mobile 375x667: no horizontal overflow (`document.documentElement.scrollWidth <= window.innerWidth`).
- State-transition (When backend data changes): push `7d_opus` observed_at to > 1 week old, restart/repoll, reload → the `7d (Opus)` card DISAPPEARS and its chart line is gone in all ranges. Conversely, widening the selected range past a window's last observation reveals it (a window observed 3h ago is hidden at 1h but shown at 6h/24h/7d).

### TC-11, Memory-Allocation Root Fixes Non-Regression (C1.4), Admin-web quota rendering and auto-refresh
- **Source and Context:** Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes`.
- **Live browser action:** Run `agent-browser skills get core`, open `$QA_ADMIN/`, set `localStorage['cc-lb-admin-token']` to the disposable `$QA_ADMIN_TOKEN`, reload, then open the fixture upstream from the Upstreams list. On the detail page, record the `Subscription Quota` snapshot card and `Quota History`. On the Overview page record the pool quota chart. In the Upstreams sidebar record the mini meter. Apply the C1.2 `0.20 -> 0.80` writer mutation while this same browser page remains open, wait the documented isolated poll interval, and take a second snapshot **without reloading**.
- **Expected observable result:** Before the mutation, the 5h card/meter and chart show `20%`. After it, the detail snapshot card and sidebar show `80%`, Quota History has the new step, and the Overview pool quota rises. The rendered text/state agrees with `/latest` and `/aggregate`. The UI changes without a manual reload. The range selector shows distinct 1h/6h/24h/7d x-axis spans and never renders a fabricated 0% leading segment.
- **Visual Evidence (from gitignored `.omo/evidence/task-F5-screenshots/`):**
  - `c1_4_before_mutation.png`: Showed the upstream detail page with the 5h snapshot card displaying exactly `20.0%` utilization, a green status badge, and the relative observed-at text "live, Header, 10s ago". The Quota History chart rendered a flat line at 20% across the selected 7d range. The sidebar mini meter for the upstream also showed a 20% filled bar.
  - `c1_4_after_mutation.png` (taken without manual reload): Showed the same page automatically updated. The 5h snapshot card now displayed exactly `80.0%` utilization, an amber status badge, and the relative observed-at text "live, Header, 2s ago". The Quota History chart added a sharp vertical step from 20% to 80% at the mutation timestamp. The sidebar mini meter updated to an 80% filled bar. The Overview pool quota chart also showed a corresponding rise in the stacked bar.

## 3. Verdict table (fill on execution)

| TC | Surface | Result | Evidence |
|----|---------|--------|----------|
| 1  | Detail Quota History range | **PASS** (gating) | post_{1h,6h,24h,7d}.png: x-spans 03:04–04:04 / 22:04–04:04 / 7/8–7/9 / 7/2–7/9 → 1h/6h/24h/7d, all distinct, no 16-day/6-23 stretch |
| 2  | Detail snapshot cards | **PASS** | post_7d.png: 5H 59.0% / 7D 71.0% with meter + "live · Header · 16초 전" ("16 seconds ago"), no NaN/broken cards |
| 4  | Detail empty/loading | N/A | code unchanged by fix; empty/skeleton paths not force-reproduced |
| 5  | Overview pool quota | **PASS** (regression guard) | ov_post_{1h,7d}.png: 1h flat vs 7d dynamic, distinct spans, clean render; Overview code untouched |
| 6  | Sidebar mini meters | **PASS** | /upstreams snapshot: per-window meters render (bh322yoo-max 5h 59% / 7d 72%, etc.) |
| 7  | ApiUsageCard | N/A | non-OAuth surface, separate usage endpoint, code unchanged by fix |
| 8  | QuotaObservedAt | **PASS** | relative observed-at "16초 전" ("16 seconds ago") rendered in TC-2 card; unit-tested |
| 9  | Fable 5 Quota Transition | **PASS** (automated Playwright route-mocked transition executed; isolated real-backend/manual mutation not executed) | TC-9 specs; Fable 5 model-scoped weekly quota transition case. Fresh responsive visual captures cover 1280/768/375. |
| 10 | Range-scoped stale window visibility | documented, not executed | TC-10 specs; stale/out-of-range windows hidden from chart + model cards expire after 7d while 5h/7d always show |
| 11 | Memory-Allocation Root Fixes Non-Regression (C1.4) | **PASS** | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |

PASS = every executed TC meets Expected; the fix's target (TC-1) is the gating case.

Verdict: **PASS**. Fix isolated to the detail Quota History `<XAxis allowDataOverflow>`; gating TC-1 proven fixed in a real browser (agent-browser, live `:52252`, BuildID fb535fba), key quota surfaces regression-clean. Evidence PNGs in `/tmp/{post_*,ov_post_*}.png`.

## 4. Automated-test coverage map

- Endpoint contracts + series windowing/anchor/no-zeroes: `crates/cc-lb-admin/tests/subscription_quotas.rs` (snapshot + some multi-observation transition, including Fable tests).
- Checkpoint dedup/fingerprint + range/anchor: `crates/cc-lb-storage-api/tests/subscription_quota_checkpoint.rs`, `crates/cc-lb-storage-conformance/tests/scenarios/upstream_subscription_quota_store.rs` (transition-heavy, including Fable storage conformance).
- Cleanup/backfill idempotency + writer-continues-after-drop: `crates/cc-lb-server/tests/subscription_quota_checkpoint_{cleanup,backfill,writer}.rs`.
- Storage roundtrips: `crates/cc-lb-storage-sqlite/tests/storage_roundtrips_sqlite.rs` (+ postgres).
- Frontend chart transform + carry-forward/no-zeroes: `crates/cc-lb-admin/web/src/components/upstreams/buildQuotaChartData.test.ts`; card/legend: ApiUsageCard/QuotaObservedAt tests.
- Frontend range/freshness window gating (graph + card visibility selectors): `crates/cc-lb-admin/web/src/components/upstreams/quotaWindowVisibility.test.ts` (point-in-time + range-sweep and one-week-cutoff transitions).
- OAuth ingestion from active limits: `crates/cc-lb-server/src/scheduler_dispatch/usage/tests.rs`.
- Unified header parsing: `crates/cc-lb-engine/src/rate_limit_headers.rs`.
- Fable routing preference: `crates/cc-lb-engine/src/builtin_filters/subscription_preference/tests.rs`.
- Proxy path E2E: `crates/cc-lb-server/tests/claude_fable_5_proxy_path.rs`.

## 5. TC-12 — Retaining successful data during polling and replacing it on response

### Environment and data

- Use only an isolated instance running a fresh SQLite database and separate proxy/admin/metrics ports. For detailed setup, follow the isolated-environment procedure in `principal-api-key-issuance.md`. Vite's `CC_LB_ADMIN_URL` must point at this instance.
- Create the OAuth upstream, API-key upstream, and principal through the real admin API. Do not use real provider credentials.
- Prepare the OAuth `5h`/`7d` checkpoints and latest rows using the SQL in `subscription-quota-fullstack.md` §2. Because `/latest` is served from cache, restart the isolated server after the initial seed and confirm the API returns `fresh` state.
- Prepare at least two non-null utilization points in `pool_subscription_quota_history_v1`, per-model token/cost rows in `usage_rollups_v2`, and requests for that principal in `request_events_v1`. The request seed must populate not only the payload but also `list_ts_ms`, `list_event_key`, `list_status`, and `list_duration_ms`.
- Record as the baseline a state in which the storage query, the real API response, and the browser's values and time series all agree. Do not judge success by call counts or HTTP 200 alone.

### State-transition procedure

1. After the initial render completes, install the Playwright route gate. Do not use the first request or StrictMode's cancelled initial request as the baseline. Fetch normal responses from the real backend via `route.fetch()`; use the gate only to hold a response or inject HTTP 500.
2. Hold the next polling responses for the Overview pool history and the upstream quota series. Verify that the SVG nodes, paths, axes, and legend are retained and not replaced by a skeleton. In Logs live mode, the histogram canvas and the existing bucket rendering must be retained with no dimming overlay.
3. While responses are held, add a new quota checkpoint/pool snapshot or request event to the isolated DB. After releasing the gate, verify the changed values from the real API against the changes in the browser's paths/canvas/rows. Existing nodes must be retained while their data is replaced.
4. Return HTTP 500 twice in a row to exhaust the default retries. The successful data must remain, and it must refresh again after the next 200 response. Because the backend's pool snapshot writer may add new values, derive the post-recovery expectation from the actual recovery response, not from the old payload.
5. Hold the responses for the Overview `1h → 7d` and API Usage `24h → 7d` range switches. The existing chart for the same entity must remain. Overview must also keep displaying the previous range and apply the new range only after the successful response.
6. Switch the upstream or principal and hold the new identity's response. The previous entity's chart or request rows must not appear under the new name. A successful response for an entity with no data must render the normal empty state.
7. Verify the Logs list separately from the live histogram. Turn off live tail, then hold the real paginated request issued by `Refresh`. Build the request matcher from the query observed on that Logs page. Do not confuse Overview's `limit=200` with Logs' `limit=50`.
8. Change the Logs model filter to a value with no matching requests and hold the recent/histogram responses. No rows or density from the previous filter may remain, and the loading overlay must disappear after the empty successful response.

### Verdict and regression tests

- **2026-09-07 PASS:** Verified delayed, failed, recovered, and replaced data end to end — isolated SQLite → real admin API → real browser — across Overview, OAuth Quota History, API Usage, principal Recent Requests, and the Logs histogram/list. Range and entity/filter transitions also passed.
- Transitions were verified on desktop 1440×1000, and the upstream screens were also checked on mobile 390×844. Response holding and error injection used the browser gate; successful payloads were never replaced with mocked responses.
- `src/lib/hooks/__tests__/polling-retention.test.ts`: relative-time polling, retain/recover after failure, range and upstream boundaries.
- `src/lib/hooks/__tests__/histogram-retention.test.ts`: poll/pan retention under the same filter, filter switching, and delayed-response isolation.
- `src/routes/-index.test.tsx`, `-upstreams-loading.test.tsx`, `-principals-loading.test.tsx`, `-logs-polling.test.tsx`: loading, placeholder, and successful-data rendering in the real consumers.
- `src/lib/api.test.ts`: simultaneous guarantee of the external cancellation signal and the 30-second timeout.
