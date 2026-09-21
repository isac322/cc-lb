# Frontend State Management Audit and Fix Plan

- Audit date: 2026-09-07
- Baseline: source with the polling-flickering fix from `isac322/fix-flickering` applied.
- User request: parallel investigation → independent cross-verification → preserve results → fix in a PR separate from the existing flickering work.
- No repository changes during the audit: pre/post Git diff and SHA-256 of untracked contents match.

## Relationship to the existing flickering fix

The base change fixed the path that replaced successful data with skeletons/empty arrays during refetch, and the replacement of absolute-time query keys. This change covers edit-target identifiers, shared preferences, SSE lifecycle, cache invalidation, rendering cost, and missing verification. The request-detail `partial → final` timeline is a separate transition where the same data-preservation principle was missing. Because the same files are shared, the differences are separated as a stacked PR on top of the base PR.

## Confirmed and conditionally confirmed ledger

Paths are relative to `crates/cc-lb-admin/web/src/` unless noted otherwise. The entries below are pre-fix observations; completion verdicts are recorded separately in the verification-results section.

| ID | Priority | Evidence and user impact | Fix direction |
|---|---|---|---|
| S01 | P1 | `routes/principals.tsx` PrincipalDetail has no identity key, so Allowed Models/Default Limits/inline slot drafts move from A to B. A real React probe submitted B's id/revision together with A's model draft | Bind the edit subtree and mutation lifecycle to the principal identity |
| S02 | P2 | `lib/useLiveEventStream.ts` stale detector captures the first connect. After injecting a 50-second stall following an A→B filter change, the connection URL went A→B→A | A stable effect boundary that reads the latest filter/connect |
| S03 | P2 | `RequestEventDrawer.tsx` and `latency/LatencyTimeline.tsx`: on partial→final, the displayed timeline switches to a full skeleton while the detail loads. Probe observed 24 skeletons | Keep displayable data; progressively fill in only the detail that has not arrived |
| S04 | P2 | `lib/locale.ts`, `lib/theme.ts`: independent useState per hook instance. After a setter, concurrently mounted instances diverge into dark/light, ko-KR/en-US, Asia/Seoul/UTC | Single state via an external store subscription or provider |
| S05 | P2 | `routes/settings.tsx` ConfigDraftSection textarea is empty state, not connected to the server draft. Only metadata shows the server revision | Load the initial/new draft; separate the unsaved draft from the server revision |
| S06 | P2 | `lib/queries.ts` cascade plugin delete invalidates only registry/reference. Server-side chain/warmup reference removal and revision change diverge from the cache | Invalidate the actually affected plugin-chain/upstreams caches |
| S07 | P2 | `routes/audit.tsx` Upstream/Route/Status filters are ignored by the backend `AuditQuery`. Only the active-filter count changes | Implement the filter contract against the data actually supported |
| S08 | P2 conditional | CacheKeepaliveSettingsDrawer's `[open, principal]` effect. If an external actor changes the same principal and a reconnect/refetch arrives, an unsaved 77 is overwritten by the server's 30 | Initialize the draft per open/id and preserve the edit-start revision |
| S09 | P2 | SessionDetailPane omits the Back header only in the error branch. At ≤960px the list is also hidden, leaving no return path except closing the whole drawer | A common return path for loading/success/error |
| S10 | P2 conditional | CommandPalette navigates, then clicks the DOM after 100ms. A real lazy route import was confirmed in the existing production dist; if the cold route is late, the action is lost. Slow-browser reproduction was not run in the audit | Pass the action intent via URL search and consume it after mount |
| S11 | P1 | Settings Rotate token is an enabled button but only shows a toast mock; no API call | Do not present unsupported rotation as success; match the UI to the real operational procedure |
| P01 | P2 | usePolledData's spread reads all tracked query props. Identical-data refetch: 0 data-only notifications, 2 spread notifications; the data ref is identical | Preserve the tracked result; do not mix visibility status with native query status |
| P02 | P2 | RequestEventsTable runs getRequestOutcome 200 times when one of 200 rows changes or the parent renders with identical props. Unchanged-row DOM is preserved | Stable table/row boundaries; limit work per partial update |
| P03 | P2 | On heartbeat-only, version/events are unchanged but one extra render occurs. The cause is UI-unused activity/cursor state | Keep internal transport state in refs; publish only user-visible state |
| P04 | P2 | Because of `visible` in the SSE effect deps, a hide/show with grace=false creates connections 1→2→3 | Rebuild the connection only on actual pause-state transitions |
| P05 | P2 | Logs rows and pageRows merge/sort the same live snapshot twice | Remove duplicate sorting of derived data for display/export/filtering |
| P06 | P2 | On a cursor page, changing the filter issues a new-filter+old-cursor request followed by the initial-cursor request. Late-response screen pollution is defended | Separate pagination identity per filter before subscribing to the query |
| P07 | P2 conditional | CacheKeepaliveSessionsDrawer and the principals FLIP loop interleave rect reads and style writes | Read all rects first, then write styles |
| P08 | P3 conditional | TimeRangeStrip assigns dimensions on every draw, re-registers listeners due to inline callbacks, and queries rects unnecessarily on window mousemove | Set backing size only on dimension change; stable callbacks and scoped drag global listeners |
| T01 | P2 | Default Vitest collection misses usePolledData.test.tsx, useLiveEventStream.test.tsx, liveMergeSessions.test.ts. Even explicit runs report "No test files found" | Clean up per-environment collection paths and verify real state transitions |

## Verified but low-priority incidental observations

- usePolledData's `status` overwrites native pending/error/success with hidden/live, and the PolledDataResult intersection collapses to never. No production status consumer exists today. Cleaned up together with P01.
- The upsert reducer's eviction is a linear scan bounded by a fixed cap (500+500), not unbounded O(N²). It scans even below the cap, so there is wasted work, but the claim that a new heap/list data structure is needed is not adopted.
- liveFlashIds selects the first 20 Map entries, so old rows rather than new rows get highlighted. Handled under the same live-render contract in P02/P05.
- RelativeTime absolute formatting is computed on every render. localStorage/auto timezone initialization happens only at mount, not on every render.
- Hint plus native title double tooltip, URL residue after plugin delete, raw anchors inside plugin, preserved collapsed state of session config/raw: separate low-priority UX observations. They are not used as grounds for a broad overhaul unrelated to the core defect fixes.

## Rejected or narrowed claims and counterexamples

- "Three setStates produce three renders": ignores React automatic batching. Only the actual heartbeat render was measured.
- SSE zombie connections and reconnect timer leaks: defended by the generation guard/cleanup. Stale closures and visibility churn, however, were reproduced separately.
- "With the Principal modal open, sidebar clicks can mix in a secret/delete target": the modal backdrop prevents that direct manipulation. Only the inline editor issue is confirmed.
- "PluginDetail/RequestDetail/SettingsCard lack keys, so identity always leaks": excluded where the real unmount path and the parent DetailView key defend it.
- "Date.now in the relative-time queryFn is itself a wrong cache key": it is the intended contract for a rolling resource, so the existing fix stays.
- "Logs next-cursor infinite loop" and "old histogram persisting after Clear is necessarily a defect": rejected — counterexamples and intended independent view state exist.
- Fable legend constant extraction: intended contract. "Unused imports alone grow the Recharts bundle": cannot be confirmed without tree-shaking analysis.
- "Every getBoundingClientRect forces reflow / destroys GPU textures": exaggerated. Actual layout cost/FPS were not measured in the audit.
- "CommandPalette polls every 5 seconds": actually 30 seconds; the same query key is deduped and visibility gating applies. Removing the standing query is a preload-UX tradeoff, so it is not changed automatically.
- TimeRangeBounds committed-prop synchronization itself is correct. The claim that ordinary background polling alone overwrites the keepalive draft is rejected because of structural sharing and the non-polling usePrincipals.

## Verification evidence

React/Vitest probes run on a temporary copy of the current source (no repository modification):

1. The path where Principal A's draft is submitted with B's id/revision.
2. Keepalive draft 77→30 when an external revision arrives for the same principal.
3. Stale SSE URL A→B→A, connection count 1→2→3 within grace, 1 heartbeat-only render.
4. Divergence between two preference instances.
5. 200 outcome computations for a single change / identical props in a 200-row table.
6. 24 partial→final timeline skeletons.
7. Real QueryObserver identical-data notifications: 0 data-only / 2 spread.
8. The default runner does not collect the three state test files.

Load ms/FPS and slow production-chunk navigation were not measured in the audit. They are covered by post-implementation real-browser transition and work-count verification.

## Non-conflicting ownership and stages

- Shared Query owner: `queries.ts`, `usePolledData.ts`; owns the tracked result, cascade cache, and pagination query contracts.
- SSE owner: `useLiveEventStream.ts`, `upsertReducer.ts`, and their direct tests. No route edits.
- Preferences owner: `locale.ts`, `theme.ts`, the minimal necessary shared store, and tests. Public hook names/return fields stay unchanged.
- Principal owner: `principals.tsx` and route tests. Identity drafts, that file's FLIP, URL action=new consumption.
- Settings owner: `settings.tsx` and related route tests. Server draft, incorrect rotation UI.
- Keepalive owner: session/settings drawers and detail pane, plus their tests. No principal-route edits.
- Table/detail owner: RequestEventsTable, RequestEventDrawer/LatencyTimeline, and their tests. Route callbacks are handed to the Logs owner as a contract.
- Logs/Overview owner: `logs.tsx`, `index.tsx`, `logRows.ts`, and route tests. Duplicate sort, pagination identity, flash ids.
- Canvas owner: TimeRangeStrip and tests. Parent callback API stays unchanged.
- Audit owner: audit route, query filter DTO, and the necessary backend/storage scope. Only the Query owner changes shared queries.ts.
- Navigation owner: CommandPalette, upstream route action, plugin upload action. The Principal/Settings routes consume the URL action contract via their respective owners.
- Runner owner: Vitest config and the missing tests. SSE/Query tests are fixed by their owners; the runner owns only collection rules.

No subagent runs builds, tests, or formatting. Only the integration owner runs format/type/behavior/real-browser verification after parallel edits finish. Failures are fixed by naming the cause and the file owner, then re-verified. PR creation follows the approved repository's rules; merge was not requested.

## Implementation and verification results

The base flickering change was split into PR #705 (`isac322/fix-flickering`, commit `6dbb0dcc`). This document was committed first on the follow-up `isac322/frontend-state-boundaries` branch, then implemented in parallel with a single owner per file.

- S01–S11, P01–P08, T01: implemented with related regression verification complete. S07 removes the wrongly replicated request-log filters and uses the real Audit API's principal/since/until contract. S11 does not implement unsupported rotation; it corrects the UI to accurate environment-variable/service-restart guidance.
- Also fixed, as additionally confirmed in independent review: empty-cursor backfill stalling, failure-time accumulation during pause, permanent loading on initial draft-fetch error, pagination checkpoints from discarded concurrent renders, and mutation-callback loss during upload-modal transitions.
- Real-browser verification found ghost rows caused by duplicate Audit `request_id`s. The defect that left 13 API rows as 17 screen rows was fixed with composite+occurrence identity; after a filter switch, API and screen row counts match without event deletion/dedupe.
- Final local gates: `bun run typecheck`, `bun run lint`, `bun run build` pass. `bun run test --run`: 70 files, 633 tests pass. The one pre-existing Biome schema-version notice is not an error; this work changed no dependencies.
- Real SQLite/admin API/browser: Principal A/B draft isolation, real 409 `storage_conflict` with dirty-draft preservation, settings draft save, locale/timezone/Toaster sync, mobile session error/Back — all pass.
- Production preview/browser: command action with the lazy chunk held back ≥350ms; Logs cursor/filter/Canvas DPR·drag·polling and real DB→API→UI changes; Audit principal/time filters and duplicate-ID row preservation — all pass.
- request partial→final was verified with a real compiled-component browser harness and a real backend detail response. The backend in-process partial bus cannot be published via external SQLite writes, so this is not represented as proxy/SSE full-stack verification.
- Independent counterproof: identical-payload query renders 0 times, changed payload renders 1; identical-props computation in a 200-row table runs 0 times, one-row change runs 1 computation (the other 199 rows' DOM preserved); heartbeat/cursor extra renders 0; within grace, hide/show keeps 1 connection, 0 closes.
- Raw execution logs and screen evidence are preserved as isolated QA artifacts; the reproduction procedure is documented in `.agents/skills/user-flow-qa/references/scenarios/frontend-state-boundaries.md`.
