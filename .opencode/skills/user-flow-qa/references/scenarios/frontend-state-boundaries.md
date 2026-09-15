# Frontend state boundaries — isolated user-flow QA

## Purpose and isolation

Verify the repairs recorded in `docs/frontend-state-audit.md`, separately from the polling-flicker base PR #705. Use a fresh SQLite database, private loopback ports and disposable bearer/master keys. Never change the shared service. Follow `principal-api-key-issuance.md` §0 for the server configuration. Vite must target the isolated admin listener through `CC_LB_ADMIN_URL`.

Run both the development UI and a production build/preview. The production preview is required for cold lazy-route navigation. Browser normal responses must come from the real server; route gates may delay real responses or inject explicit failure statuses only. Keep credentials out of evidence.

## Fixtures and point-in-time checks

1. Create two principals through `POST /admin/v1/principals`, with different names and `allowed_models`, and an API-key upstream with a disposable, nonfunctional upstream credential.
2. Seed at least 51 request records in the isolated SQLite database so `/admin/events/recent?limit=50` has a second page. Populate the materialized `list_status`, `list_ts_ms`, `list_event_key`, and `list_duration_ms` columns as well as the JSON payload. Match principal/upstream UUIDs. Verify list and detail API responses before browser use.
3. Create an inert terminal `cache_keepalive_sessions` row for the first principal (`status=terminal`, `terminal_reason=max_refreshes`, `ttl=5m`, recent `last_message_at_ms`). It must appear in the real `/admin/v1/principals/{id}/cache-keepalive?limit=100` list and `/cache-keepalive/{sessionId}` detail. Do not seed active jobs.
4. Save a distinguishable configuration draft through `PUT /admin/config/draft`, preserving the real expected-revision contract. Do not Apply or Reload synthetic configuration.
5. Principal create/update actions generate real Audit entries. Multiple updates within one second may have identical `request_id`; retain all entries. Verify API count and each payload rather than treating request_id as a unique row ID.

## State-transition scenarios

| Scenario | Given / When | Required outcome |
|---|---|---|
| Principal identity | Edit Alpha's Allowed Models, select Beta, edit/save Beta | Alpha's draft is absent under Beta; request uses Beta's values and revision. Alpha's server record stays unchanged. |
| Concurrent drawer edit | Open keepalive settings; change lead time to 77; externally PATCH the same principal and refetch through the mounted query client | Input remains 77. Save uses opening revision, receives the real 409 `storage_conflict`, shows conflict guidance, and does not overwrite the external server record. Close/reopen loads the latest configuration. |
| Saved configuration | Open Settings with a saved draft, edit text, save | Text is the server draft rather than a placeholder. Dirty text survives refresh; Save uses the editor revision. Validate/Apply stay disabled while text is unsaved. |
| Initial draft error | Fail the initial draft GET, then allow the Retry GET | Error is not an infinite skeleton. Retry is available; successful response initializes the editor with a valid revision. |
| Shared preferences | Change locale, timezone and theme without navigating | Settings preview, mounted RelativeTime consumers and Toaster agree. Explicit theme overrides OS changes; system theme follows OS changes. |
| Mobile detail error | At 390×844 select the real keepalive session, inject 500 only for its detail GET | Error includes Back; clicking it restores the real list. The drawer need not be closed. |
| Cold command action | In production preview, delay the destination route chunk for at least 350 ms, then choose Create principal/upstream from the command palette | URL carries `action=new` while the chunk is held. After release the dialog opens exactly once and consumes only the action parameter. |
| Plugin upload ownership | Start inline upload, open upload through command action while the request is pending | The shared mutation remains observed; duplicate upload/dismissal is blocked. Success, failure and replacement confirmation callbacks remain usable. Covered with the real mutation hook and deferred fetch in route tests. |
| Logs pagination | Visit a cursor page, then change principal/status filters | The new query uses the initial page, never a cursor from the old filters. Same-filter rerenders retain page/scroll. |
| Canvas and updates | At DPR 2 resize, brush, pan and zoom; observe consecutive histogram polls; insert a new real request row | Canvas remains rendered, selection callbacks use current data, global drag listeners detach on release, and actual API/new row appears in the UI. |
| Request completion | Render the real table/drawer with a live partial event; replace it with the same event's final list record while detail GET is pending | Visible timeline remains; no full skeleton replacement. Real detail response enriches the same request. |
| Audit filtering | Select principal and inclusive From/To bounds; clear filters | Every returned entry meets real server filters. The admin table count equals the response's `admin_action != null || kind != null` projection, including duplicate request IDs. No ghost rows remain after filtering. |

The backend has no admin endpoint for publishing partial events onto its in-process bus. The request-completion browser case therefore uses the real compiled components with controlled partial→final props and a real backend detail GET. This is component-browser evidence, not a proxy/SSE end-to-end claim.

## Cost and lifecycle counterproof

Use the real installed React Query and production hooks/components in an isolated test copy. Stub only external fetch/EventSource boundaries. Use a deterministic notification scheduler and restore it afterward.

- Identical polling payload: zero data-only consumer rerenders; changed payload: one rerender.
- Table of 200 events: unchanged props cause zero outcome computations; changing one event causes one computation and preserves the other 199 DOM rows.
- Heartbeat/cursor with unchanged event data/status: zero extra consumer renders.
- Short hide/show before grace expiration: one connection remains, zero closes.
- Filter A→B, then 50 seconds without frames: stale reconnect uses B.
- Empty reset cursor followed by pause/resume still processes new frames; intentional long pause does not count toward permanent failure.

These measure work counts and semantics, not production FPS or latency.

## Verification map and recorded outcome

- `usePolledData.test.tsx`, `query-boundary.test.ts`, `polling-retention.test.ts`: tracked query and cache behavior.
- `useLiveEventStream.test.tsx`, `stream-lifecycle.test.tsx`: SSE lifecycle and failure boundaries.
- `preferences.test.tsx`: synchronized snapshots and resource cleanup.
- Route tests for principals/settings/plugins/logs/audit: identity, revisions, pending ownership, suspended renders, and duplicate Audit rows.
- RequestEventsTable/Drawer and TimeRangeStrip tests: partial work, detail preservation, pointer/DPR cleanup.
- Keepalive tests, including previously omitted `liveMergeSessions.test.ts`: draft and session state transitions.

Verified locally: typecheck, lint and production build; 70 test files / 633 tests passed. Real isolated SQLite/editor browser scenarios passed, including 409 conflicts and mobile Back. Production browser scenarios passed for delayed route chunks, Logs cursor/filter/canvas/new-data behavior and Audit principal/time filtering. The compiled-component completion case and independent work-count counterproof passed with the scope above. No shared-service deployment or PR merge was performed.
