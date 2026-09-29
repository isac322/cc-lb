# Request log `errors` pagination

## Purpose

Verify that the Logs `errors` status filters *before* pagination, including
4xx/5xx responses and 2xx responses with an `error_code`. No success-only
page may appear between historical errors.

## 0. Environment and isolation

Use a throwaway local SQLite instance and the Admin Web browser, never the
shared deployment. Configure admin authentication and use the same token for
the API and UI (`localStorage['cc-lb-admin-token']`). Record the chosen local
port and a fixed time window. Use the application event writer or an isolated
fixture with the materialized `list_status`, `list_ts_ms`, `list_event_key`,
`error_code` and payload columns filled correctly; do not mutate production.
Remove the temporary database and stop both servers after verification.

## Point-in-time

Given three older matching events (429, 200 with `error_code`, 500), and at
least 60 newer 200 events without errors, all within the window:

1. Inspect the SQLite rows (including `list_status` and `error_code`) and
   confirm exactly three match `list_status >= 400 OR error_code IS NOT NULL`.
2. Authenticated `GET /admin/v1/events/recent?status_class=errors&limit=2` must
   return the two newest *matching* events. Follow the returned `ts_ms` and
   `event_id` as `until_ts_ms`/`until_event_id`: the next response contains the
   remaining error, and a further request returns no events. Compare the
   `status_class=4xx` control (only 429). Capture HTTP status and JSON.
3. In a real browser visit `/logs?status=errors` with the fixed time bounds;
   check populated rows and a valid `Showing` range. If more than 50 errors
   were seeded, click Next and verify that the next page has matching rows,
   no successes, and `start <= end`. Screenshot both pages.

## Transition

Given the initial state above, append one newer error through the same writer,
then refresh the Admin page (or await its poll). Confirm the new materialized
SQLite row, first API page and UI first page all include the new error; follow
the cursor again to confirm older errors remain reachable. Append a newer
successful request and confirm that it does *not* displace the first matching
error in either the API or UI. Capture before/after payloads and screenshots.

## Automated coverage and verdict

Storage conformance `request_event_list_projects_rows_and_preserves_detail`
tests sparse matching and cursor continuation for SQLite and Postgres;
`src/routes/-logs-polling.test.tsx` checks that a live 401 partial corrected
to 200 leaves the `errors` view. List and histogram requests send
`status_class=errors`; the live stream omits it so such corrections still
arrive. Browser, authenticated API and state transitions remain manual.

| Layer | Point-in-time | Transition | Evidence |
|---|---|---|---|
| SQLite | BLOCKED | BLOCKED | |
| Admin API | BLOCKED | BLOCKED | |
| Browser | BLOCKED | BLOCKED | |
