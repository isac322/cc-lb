# Request log exploration: bounded history and time ranges

## Purpose

Prove that an operator can explore request history without unbounded fetching,
an All sessions freeze, or a growing live-event store, and that the compact time
range control sends the backend's Unix-second contract.

This scenario is read-only. It does not mutate storage or production data.

## 0. Environment and preconditions

1. Run an isolated or local cc-lb admin server containing at least 500 request
   events across two or more `thread_id` values.
2. Serve the current admin web build against that server. For Vite:

   ```bash
   CC_LB_ADMIN_URL=http://127.0.0.1:<admin-port> bun run dev
   ```

3. Authenticate through the SPA using a local admin token. Never print the
   token or place it in screenshots.
4. Open `/logs` in a real Chromium browser. Record the viewport, initial row
   count, DOM element count, JS heap, and `/admin/v1/events/recent` requests.
5. Keep DevTools resource timing available. Do not use wall-clock thresholds as
   an automated assertion; record them as manual evidence.

## Context discovery

- API: `GET /admin/v1/events/recent?limit=200`.
- Browser rows: `document.querySelectorAll('tbody tr').length`.
- DOM size: `document.querySelectorAll('*').length`.
- Heap when available: `performance.memory.usedJSHeapSize`.
- Recent requests: filter `performance.getEntriesByType('resource')` for
  `/admin/v1/events/recent` and inspect only query strings/counts.

## Point-in-time cases

### P1: bounded historical view and client pagination

Given the logs page has at least 500 available finalized events, when the operator loads the page and navigates through client pagination, then:

- request limits are `200`, `200`, then `100` to load the 500 events;
- no fourth request starts;
- memory retention stays capped at 500 finalized plus 500 partial events, with active partials taking priority;
- DOM elements are kept low by mounting at most 50 data rows per page to prevent CPU lag;
- pagination controls are accessible, featuring labeled Prev and Next buttons and an `aria-live` region;
- navigating Next, Prev, and reaching the final page updates the DOM rows correctly;
- sentinel behavior displays the backend infinite-scroll indicator only on the final client page, and it is absent once the 500-event cap is reached;
- overall DOM size and heap plateau instead of growing.

Verify this at remote browser sizes of 1280px, 768px, and 375px to ensure pagination controls and table layout remain fully accessible and usable.

### P2: select geometry

Given any request-log select is open, its visible popup width equals the actual
trigger bounding-box width within one device pixel. Validate at least Session,
Status, and Time Range because their trigger widths differ.

### P3: in-progress cost estimate

Given an in-progress (partial) row with observed token usage, both the row and
the Cost drawer show a token-derived estimate labeled `Est. $…` rather than a
dash. The estimate is display-only: the authoritative price still replaces it
only at request termination, and it never enters the billing or limit path.

### P4: time contract

- All time sends neither `since_unix_secs` nor `until_unix_secs`.
- A preset sends only canonical `since_unix_secs` and keeps live tail enabled.
- Custom displays the effective configured timezone, requires both valid bounds,
  uses dashboard-styled `YYYY-MM-DD HH:mm` text inputs plus a date-only calendar
  (Base UI popover at desktop width, Base UI Modal below 1024px) instead of
  browser-native datetime controls, rejects
  nonexistent/ambiguous DST wall times and `since > until`, and disables live
  tail. Calendar picks preserve typed times (or default Since to `00:00` and
  Until to `23:59`). Draft edits do not change the URL until Apply; Cancel or
  Escape restores the applied range without emitting.
- Reloading the Custom URL restores mode, bounds, displayed wall times, and the
  paused tail state.

### P5: export all filtered rows

Given the client has loaded 500 events and the operator has applied a filter, when the operator clicks the Export button, then:

- the exported file includes all filtered retained rows in memory, not just the 50 rows currently mounted on the active page;
- the export operation does not trigger any new backend requests.

Verify this behavior at remote browser sizes of 1280px, 768px, and 375px.

### P6: status-class filtering

Given retained live and historical rows contain multiple HTTP status classes,
when the operator selects `2xx`, `3xx`, `4xx`, or `5xx`, then both the recent
request and live stream use `status_class=<class>`, and every rendered/exported
row belongs to that class. Retained live rows and placeholder history must not
leak rows from the previous class while the filtered request settles.

### P7: in-progress cost estimate

Given an in-progress request has observed usage and a known priced model, its
row shows `Est. $…` and the drawer's `Estimated Cost` section shows the same
token-derived display estimate. The estimate must not enter the authoritative
`Priced` lifecycle event, billing metrics, or limit reconciliation; the final
row remains driven by the termination-time price.

## State-transition cases

### T1: session to All sessions

Given history is at the 500-event cap and the operator is on the final page of client pagination, when a concrete Session is selected, then:

- the current page resets to page 1;
- the sentinel disappears immediately;
- no additional recent-events request starts.

When All sessions is selected, then:

- the table returns to the bounded row set without new pagination requests;
- the current page resets to page 1;
- CPU acceptance criteria are met, with no long tasks exceeding 100ms.

Verify this transition at remote browser sizes of 1280px, 768px, and 375px.

Performance context for six cycles in fresh Chromium with 500 loaded rows:
- Session selection: average wall time 191ms, task time 135ms, no long tasks >= 50ms, DOM mounts 50 rows.
- All sessions selection: average wall time 179ms, task time 108ms, no long tasks >= 50ms, DOM mounts 50 rows.
- Before fix baseline: Session selection wall 679ms, task 625ms, max 393ms; All sessions selection wall 656ms, task 549ms, max 205ms.
- The rendered table is capped at 50 rows per page via client pagination, bounding reconciliation cost regardless of how many rows are retained.

### T2: preset to Custom to preset

Given user-requested live tail is on, when Custom is selected, then the toggle
becomes visibly off/disabled and the SSE client closes or is never created.
After two valid Custom bounds are entered, the URL and historical request remain
unchanged until Apply; Apply issues only bounded historical requests. Cancel and
Escape restore the prior applied values. When a preset is selected again, the
previous user tail preference is restored and the URL contains only the preset's
canonical `since_unix_secs`.

### T3: continued live events

Given live tail remains open while new finalized and partial events arrive, observe beyond 500 distinct events of each phase. The retained map remains at most 500 finals plus 500 freshest partials. Under this setup, the rendered table mounts at most 50 data rows per page with partials prioritized. A delayed partial for an evicted-final tombstone does not resurrect that event, though a later legitimate final for the same ID may re-enter.

## Automated coverage map

- `src/lib/upsertReducer{,.partials}.test.ts`: final, partial, and tombstone
  caps plus deterministic eviction and resurrection rules.
- `src/lib/hooks/__tests__/useLiveEventStream.retention.test.ts`: disabled SSE,
  reconnect cleanup, reset, and hook-level retention.
- `src/lib/hooks/__tests__/queries.events.test.ts`: exact 200/200/100 cap.
- `src/lib/logRows.test.ts`: dedupe, ordering, partial preservation, row cap.
- `src/lib/logRows.test.ts` and `src/routes/-logs.test.ts`: merged-row status
  filtering plus recent/SSE `status_class` query mapping.
- `crates/cc-lb-engine/src/lifecycle_event_assembler.rs` tests: in-progress
  display estimate gating, usage inputs, and authoritative-cost precedence.
- `src/components/ui/{Select,CalendarPopover,Hint}.test.tsx`
  plus `src/lib/calendarDate.test.ts`: geometry contract, calendar open/select/
  Escape behavior, draft Apply/Cancel, timezone/range validation, Cost row gate,
  and lazy Hint.

Manual-only gaps: browser long-task/heap measurements, actual popup geometry,
responsive visual quality, and SSE network closure.

## Verdict table

| Case | Storage | API | UI | Transition | Verdict/evidence |
|---|---|---|---|---|---|
| P1 bounded history | N/A | PASS | PASS | N/A | 2026-07-11 after client pagination fix: limits 200/200/100, 500 loaded events, at most 50 DOM rows mounted per page, no sentinel on final page; 2,450 elements and ~120 MB heap after a five-second plateau observation |
| P2 select geometry | N/A | N/A | PASS | N/A | Time Range 92 px popup vs 92.64 px trigger; shared `--anchor-width` contract is component-tested |
| P3 Cost 1h | N/A | VERIFIED | VERIFIED | N/A | Zero/absent branch browser-proven; positive branch fixture-tested because current live rows had no positive sample |
| P4 time contract | N/A | PASS | PASS | N/A | 2026-07-10 calendar QA: desktop Popover plus tablet/mobile Modal stayed in viewport with zero page overflow; picking `2026-07-15` preserved `08:30`; draft URL remained unchanged until Apply, then persisted `since_unix_secs=1784104200&until_unix_secs=1784569559` (inclusive final second of the displayed Until minute); invalid input exposed linked ARIA error text; oversized Unix bounds normalized to empty Custom bounds without a render error |
| P5 export all | N/A | PASS | PASS | N/A | 2026-07-11 export QA: clicking Export with active filter downloaded all 500 filtered retained rows, not just the 50 rows on the current page |
| P6 status class | N/A | PASS | PASS | N/A | 2026-07-11 real Chromium: selecting 4xx issued recent and SSE requests with `status_class=4xx`; rendered rows contained only 400/429 after retained-live filtering |
| P7 partial cost | N/A | VERIFIED | VERIFIED | N/A | Assembler and row/drawer tests prove usage-derived display estimates while authoritative `Priced` remains the final billing source; current persistent backend predates this source change, so browser proof awaits the PR build |
| T1 session → All | N/A | PASS | PASS | PASS | After client pagination fix: 44 → 50 DOM rows with three recent requests before and after; no fourth request, no page-level overflow, and no long tasks exceeding 100ms |
| T2 preset → Custom | N/A | PASS | PASS | PASS | Custom closed/disabled tail; preset restored prior enabled preference. Calendar draft Cancel and input/modal Escape restored applied wall times without URL mutation; focused modal Escape closed the overlay |
| T3 continued live | N/A | VERIFIED | VERIFIED | VERIFIED | Reducer/hook burst tests prove 500-final + 500-partial retention; real browser stayed at 50 DOM rows and 2,450 elements while live tail continued |

Use PASS, VERIFIED, FAIL, or BLOCKED. Include exact commands, query strings,
viewport, row/DOM/heap counts, and screenshot paths. Do not claim browser PASS
from unit tests alone.
