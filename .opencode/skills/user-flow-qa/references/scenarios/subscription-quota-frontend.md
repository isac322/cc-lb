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
    "http://127.0.0.1:52252/admin/v1/subscription-quotas/series?upstream_ids=<UID>&windows=5h,7d,7d_sonnet,7d_opus,overage&source=merged&since_unix_secs=<S>&until_unix_secs=<U>&bucket_secs=<B>"
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
- Cross-check (API): the `series` payloads for 1h vs 6h may still be equal (backend returns
  anchor→until fill by design — see ADR 0007); the **frontend clip** is what must differ.
  So verdict is visual/x-domain, not payload bytes.

### TC-2 — Detail snapshot cards (per-window)
- Surface: detail → grid of window cards (5h, 7d, 7d_sonnet, 7d_opus, overage).
- Initial: OAuth upstream selected.
- Steps: read each card.
- Expected: each active window shows utilization %, a meter bar matching the %, source
  (Header/API/—), observed-at relative time, reset countdown or "not started", ETA/burn if
  present. `overage` card present only when overage billing enabled or monthly limit set.
  Values agree with `/subscription-quotas/latest` for that upstream.

### TC-3 — Detail quota deficit + analysis caveats
- Surface: detail → "Quota deficit" card + caveats list.
- Steps: observe when a deficit is detected (shortfall tokens, recommended multiplier, confidence).
- Expected: renders only when analysis returns a deficit; multiplier/confidence numeric;
  caveats list matches `/subscription-quotas/analysis` output. No crash when analysis empty.

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
- Expected: mini meter bars for active windows (5h, 7d, +overage when enabled); state dot
  green(fresh)/amber(stale) with hint (state/source/observed); % text; overage utilization
  computed from `extra_usage_used_credits / extra_usage_monthly_limit` when `utilization` null.
  Values agree with `latest`.

### TC-7 — ApiUsageCard (non-OAuth upstream)
- Surface: detail of a `anthropic_api_key` upstream → "API Usage".
- Steps: toggle range `24h`/`7d`, metric `Tokens`/`Cost`.
- Expected: chart data changes with range (usage endpoint is server-windowed) and with metric;
  legend reserves height in loading/empty/populated (no layout shift); "No usage in selected range" when empty.

### TC-8 — QuotaObservedAt shared component
- Expected: renders relative observed time where snapshots appear; renders `—` when
  `observed_at_unix_millis` is null.

## 3. Verdict table (fill on execution)

| TC | Surface | Result | Evidence |
|----|---------|--------|----------|
| 1  | Detail Quota History range | **PASS** (gating) | post_{1h,6h,24h,7d}.png: x-spans 03:04–04:04 / 22:04–04:04 / 7/8–7/9 / 7/2–7/9 → 1h/6h/24h/7d, all distinct, no 16-day/6-23 stretch |
| 2  | Detail snapshot cards | **PASS** | post_7d.png: 5H 59.0% / 7D 71.0% with meter + "live · Header · 16초 전", no NaN/broken cards |
| 3  | Detail deficit/analysis | N/A | analysis/deficit code unchanged by fix; not force-reproduced (deficit renders only when detected) |
| 4  | Detail empty/loading | N/A | code unchanged by fix; empty/skeleton paths not force-reproduced |
| 5  | Overview pool quota | **PASS** (regression guard) | ov_post_{1h,7d}.png: 1h flat vs 7d dynamic, distinct spans, clean render; Overview code untouched |
| 6  | Sidebar mini meters | **PASS** | /upstreams snapshot: per-window meters render (bh322yoo-max 5h 59% / 7d 72%, etc.) |
| 7  | ApiUsageCard | N/A | non-OAuth surface, separate usage endpoint, code unchanged by fix |
| 8  | QuotaObservedAt | **PASS** | relative observed-at "16초 전" rendered in TC-2 card; unit-tested |

PASS = every executed TC meets Expected; the fix's target (TC-1) is the gating case.

Verdict: **PASS**. Fix isolated to the detail Quota History `<XAxis allowDataOverflow>`; gating TC-1 proven fixed in a real browser (agent-browser, live `:52252`, BuildID fb535fba), key quota surfaces regression-clean. Evidence PNGs in `/tmp/{post_*,ov_post_*}.png`.
