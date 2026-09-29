# ADR 0011: Cache-keepalive principal-detail read model and UI

## Status

Accepted.

Builds on ADR 0004 (cache-keepalive scheduler, keep-alive request shape, turn classifier) and ADR 0009 (typestate attempt rail for unified proxy and renewal accounting). Those ADRs cover how renewals are scheduled, dispatched, and accounted. This ADR covers how that already-recorded data is projected into a read model and surfaced on the Principal detail page, without changing any renewal dispatch or accounting semantics.

## Context

ADR 0004 and ADR 0009 give us durable `cache_keepalive_sessions`, per-turn accounting rows, and `cache_keepalive_decisions` (including not-tracked decisions). Operators had no way to see, per principal, which sessions are being kept warm, whether keepalive is paying for itself, and why each turn did or did not renew.

The UI design calls for a Cache keepalive card with four metric tiles, a Sessions drawer with per-session and per-turn profit-and-loss, a separate Settings drawer, and a live "rise" animation when a session renews. Shipping that feature raised several architecture questions the prior ADRs did not answer:

- Where is profit-and-loss computed, and from which prices?
- The stored session/turn/decision rows lack some fields the UI must display verbatim (frozen display reason, error text, schedule-time config snapshot, message timestamp). Do we reparse request bodies on read, or persist projected fields?
- How does the UI stay live without a full page refresh?
- What are the exact display states, and how are rows ordered and paginated stably?

A hard constraint framed all of these: do not change renewal dispatch, scheduler semantics, accounting rows, proxy response bytes, or the warmup runtime. Read-model additions are allowed only as additive projection data.

## Decision

### Server-side profit-and-loss from the real pricing catalog

All profit-and-loss (per turn and per session net) is computed on the server, in the admin view-model layer, using the real pricing catalog (`cc-lb-pricing`), not mock constants and not client-side math. The admin endpoint injects `cc_lb_pricing::global_catalog()` and the view-model derives economics from catalog rates. The frozen worked examples are locked by unit tests (`crates/cc-lb-admin/src/cache_keepalive_view_tests.rs`) that assert exact micro-dollar results. The client only formats already-computed numbers. This keeps pricing authority in one place and prevents the dashboard from drifting from real cost.

### Additive read-model projection, no read-time reparsing

Rather than reparse request bodies on read, the missing UI display fields are persisted as additive projection columns at schedule/decision time: `display_reason`, `error`, `config_snapshot`, and `last_message_at_ms` on the session and decision rows. These are added by contiguous migrations (SQLite `0066`/`0067`, Postgres `0095`/`0096`). They are display-only projection data: they do not participate in renewal dispatch, the typestate accounting rail, or exactly-once semantics, and the end-to-end tests prove accounting rows and event sequences are unchanged. Not-tracked decisions are surfaced by unioning `cache_keepalive_decisions` with sessions (excluding decisions that already have a turn row) so "Not tracked" entries exist even when no keepalive session was created.

### Live updates by polling, not a new event stream

The Principal-detail surfaces stay live through the existing admin-web React Query polling pattern (visibility-gated `refetchInterval`), not Server-Sent Events. We deliberately did not use the admin event stream: its subscription filters (`crates/cc-lb-admin/src/events.rs` `StreamFilters`) carry principal/model/upstream/status/source-kind but no session or cache identity, so a renewal event cannot be mapped back to a specific session row. Rather than widen that transport (a larger, riskier change), the card summary, session list, and open session detail each poll their own query; a pure client-side merge dedupes by id and trims to keep the list bounded, and a FLIP transition animates a row that rises. This reuses an established pattern and adds no new transport abstraction.

### Explicit five-state model with an orthogonal error flag

Sessions are displayed as exactly one base state — Renewed, Scheduled, Capped, Expired, or Not tracked — derived server-side from the stored status/terminal-reason/refresh-count. Error is modeled as an orthogonal nullable field (`error IS NOT NULL`), not a sixth base state, so an errored session still shows its real base state plus a red error treatment. The status filter scopes only the row list; it never changes the card metrics or the overview strip.

### Stable ordering and cursor pagination

The list is ordered `last_message_at_ms DESC` with a stable tiebreak on the opaque row id (`session_key_hash` / `entry_id ASC`), identical between the SQLite and Postgres adapters. The cursor encodes the sort key together with the principal, horizon, and filter, and the read path rejects a cursor that does not match its principal. This gives deterministic pages that do not shuffle when new renewals arrive.

## Consequences

- Pricing lives in one place; the dashboard cannot silently disagree with real cost, and the frozen examples are regression-locked.
- The read model is a thin additive projection: migrations and writes touch only display fields, so renewal/accounting behavior (ADR 0009) is provably unchanged, at the cost of a few denormalized columns to maintain when the display contract changes.
- Polling is simpler and safe but not instantaneous; updates appear within the poll interval rather than the moment a renewal lands. Making updates truly push would require giving the event stream session/cache identity — a future transport change, out of scope here.
- The five-state-plus-error model and stable cursor are shared contract between the storage adapters, the admin view-model, and the typed web client; changing a state or the sort key requires updating all three together (they are covered by parity tests).

## Verification

- `cargo test -p cc-lb-admin cache_keepalive` — view-model, P&L worked examples, and admin endpoint contract (summary/list/detail, `limit=0`, filters, cursor).
- `cargo test -p cc-lb-storage-sqlite cache_keepalive` and `cargo test -p cc-lb-storage-postgres cache_keepalive` — SQLite/Postgres read-model parity, ordering, and cursor pagination.
- `cargo test --workspace` — proves renewal/accounting rows and event sequences are unchanged by the projection writes.
- In `crates/cc-lb-admin/web`: `bun run test`, `bun run typecheck`, `bun run lint`, `bun run build` — typed client decoding, component behavior (card, drawers, session detail, live merge, FLIP/flash, accessibility/responsive).
