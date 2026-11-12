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

The Principal-detail surfaces stay live through the existing admin-web React Query polling pattern (visibility-gated `refetchInterval`), not Server-Sent Events. We deliberately did not use the admin event stream: its subscription filters (`crates/cc-lb-admin/src/events.rs` `StreamFilters`) carry principal/model/upstream/status/source-kind but no session or cache identity, and renewal completion is the only keep-alive mutation that emits an event, so it cannot replace polling. Polling is limited to data that can still change:

- The session sheet polls only the first page. Later loaded pages refresh once when row/session state-bearing summary fields or groups change, when the first-page boundary changes (the last row's id or message timestamp, or whether another page exists), or when the sheet reopens. The clock-driven `sessions_last_5m` field does not trigger a tail refresh. Cursor tokens are opaque: a rolling horizon changes their embedded horizon start without necessarily moving the page boundary. A state change on a later page can remain stale until one of those refresh conditions occurs; this is the accepted trade-off for one-request polling. Polling and "Load more sessions" share a serialization queue, and queued page loads are dropped if the selected query key changes.
- While the sheet is open the card pauses its own summary poll; when the sheet shows the 24h horizon it writes its overview summary into the card's query.
- The open session detail polls while the session is active, in the `in_request` state, or its follow-up evidence is pending, and refetches when the selected row's generation, state, or economics changes in the list.

A pure client-side merge dedupes rows by id, and a FLIP transition animates a row that rises. This reuses an established pattern and adds no new transport abstraction.

### Explicit state model with an orthogonal error flag

Each session has exactly one API/read-model base state — `renewed`, `scheduled`, `in_request`, `capped`, `expired`, `cancelled`, `failed`, or `not_tracked` — derived server-side from the stored status/terminal-reason/refresh-count. The `capped` state means only the renewal or duration cap; `failed` covers dispatch, decrypt, unsupported-provider, and stale renewal terminals. An expired real-request marker projects as `expired` with the neutral reason “Request did not complete before its deadline”.

The rendered lifecycle labels are separate: `scheduled` and `renewed` → **Warm** (live/accent), `in_request` → **In use** (neutral), `cancelled` → **Returned** (ok/green), `capped` → **At limit** (neutral), `expired` → **Expired** (neutral), `failed` → **Failed** (danger), and `not_tracked` → **Not tracked** (neutral). The list uses filled badges, and the detail header uses the same wording. API/read-model states and status-filter values remain unchanged.

Error is modeled as an orthogonal nullable field (`error IS NOT NULL`), not an extra base state, so an errored session keeps its rendered lifecycle label with a danger treatment. The status filter scopes only the row list; it never changes the card metrics or the overview strip. When keep-alive is disabled for the principal, the sheet lists with the `all` status so retained decisions stay visible.

### Durable cycle/evidence projection

Renewal attribution is backed by a retention-managed evidence projection
rather than inferred from `hit_miss` alone. Each new cycle has a purge-safe
cycle identity. Renewal evidence records the request start, effective prefix
fingerprint/hash and policy/schema identity, provider TTL, model, upstream,
cache-read usage, and exact persisted cost. Real follow-up evidence records
the request start, every candidate prefix hash that can represent the
effective prefix up to a bounded candidate set, positive cache read, and
request identity. Evidence writes are committed atomically with their
request-event or renewal projection.

A renewal receives avoided credit at most once. The first positive-cache-read
follow-up that matches a cycle's candidate prefix hashes is attributed to the
latest renewal in that cycle whose TTL window contains it. Legacy, missing,
malformed, or ambiguous evidence is `unknown`, never an inferred zero. Final
scheduler caps do not close the evidence window.

### Retained economics and aggregate reads

The read model treats retained history as accounting history, not lifetime
session state. Exact persisted spend is summed from retained turn rows,
including misses, and survives session reset or session purge. Money is
real-time: pending or unknown follow-up evidence counts as zero avoided so far,
so net is avoided minus spent. Avoided and net are null only when catalog
pricing or a turn's cost is unknown. A valid
renewal without a proven follow-up has spend but no avoided credit.

For a renewal with a proven follow-up, avoided cost is its cache-read token
count multiplied by the applicable TTL's cache-creation price minus the
cache-read price. The follow-up still pays to read the preserved cache; that
read cost is not a saving. Each renewal is priced and rounded to microdollars
before summing, identically for rows, groups, and the header. Both rates must
be known; incomplete pricing leaves avoided cost and net null.

Summary-only requests use SQL aggregates over retained turns and evidence,
including grouped model/TTL avoided-token totals. They do not materialize all
sessions, turns, or snapshots in Rust.

The session sheet lists with the `visible` status: tracked sessions plus
not-tracked rows that carry an error or a 12h TTL. Other not-tracked decisions
never leave the server for that view. A list request without a cursor is an
overview: it returns the summary, the trigger groups (count, spent, avoided,
net over every matching row, computed in bounded batches), and the first page,
all for one horizon start. The summary aggregate and the group batches are
separate storage reads, not one snapshot, so the header equals the group
totals plus the deleted-sessions aggregate when the underlying data stays
unchanged between those reads; a renewal or a newly attributed follow-up that
lands in between can make one response differ until the next poll. The
browser never needs to load every row to reconcile them. A list
request with a cursor returns rows only and skips the summary and group
aggregates. Rows carry their server-computed trigger group. Detail loads only
the requested session. Current-cycle detail renewal counts use cycle-linked evidence;
retained history remains available independently. The Admin Web card disables
summary polling while keep-alive is disabled.

Sessions in the `cancelled` state are tracked and visible only when they have a retained
renewal turn inside the selected horizon. Lifetime refresh counts and turns
outside that horizon do not keep them in the groups or recent-session count.
The All time horizon includes their retained historical renewals and spend.

Cancellation stores its nullable `cancel_reason` independently of the
schedule-time trigger in `display_reason`. List and detail responses expose
that cause and render its reason without a cancellation prefix, such as
“A new request arrived”. Legacy rows with no cause use “Renewals ended” in the
API and show the Returned lifecycle label without a separate cancellation box;
they never infer why the session stopped. Terminal
rows do not advertise a scheduled first or next renewal. Trigger grouping
continues to use the original schedule-time descriptor.

Before a real request dispatches, its fenced provisional record retains the
durable `terminal/cancelled/new_request` representation and adds a request-start
timestamp plus a deadline derived from the server's configured request timeout.
Before that deadline, the read model projects `in_request` with a neutral
“In use” badge and a paused-renewal explanation. It is tracked and visible
without retained renewal turns, but is neither scheduled nor renewed and does
not contribute to `renewing_now`. Completion replaces or resolves the marker.
An expired marker is terminal/stale and projects as `expired` (shown as Expired),
without claiming a known abort or error. Legacy `new_request` rows without a
request-start marker remain `cancelled` (shown as Returned). The `in_request`
API filter selects only fresh request markers.


### Stable ordering and cursor pagination

The list is ordered `last_message_at_ms DESC` with a stable tiebreak on the opaque row id (`session_key_hash` / `entry_id ASC`), identical between the SQLite and Postgres adapters. The cursor encodes the sort key together with the principal, horizon, and filter, and the read path rejects a cursor that does not match its principal. This gives deterministic pages that do not shuffle when new renewals arrive.

## Consequences

- Pricing lives in one place; the dashboard cannot silently disagree with real cost, and the frozen examples are regression-locked.
- The read model is a thin additive projection: migrations and writes touch only display fields, so renewal/accounting behavior (ADR 0009) is provably unchanged, at the cost of a few denormalized columns to maintain when the display contract changes.
- Polling is simpler and safe but not instantaneous; updates appear within the poll interval rather than the moment a renewal lands. Making updates truly push would require giving the event stream session/cache identity — a future transport change, out of scope here.
- The explicit base-state-plus-error model and stable cursor are shared contracts between the storage adapters, the admin view-model, and the typed web client; changing a state or the sort key requires updating all three together (they are covered by parity tests).

## Verification

- `cargo test -p cc-lb-admin cache_keepalive` — view-model, P&L worked examples, and admin endpoint contract (summary/list/detail, `limit=0`, filters, cursor).
- `cargo test -p cc-lb-storage-sqlite cache_keepalive` and `cargo test -p cc-lb-storage-postgres cache_keepalive` — SQLite/Postgres read-model parity, ordering, and cursor pagination.
- `cargo test --workspace` — proves renewal/accounting rows and event sequences are unchanged by the projection writes.
- In `crates/cc-lb-admin/web`: `bun run test`, `bun run typecheck`, `bun run lint`, `bun run build` — typed client decoding, component behavior (card, drawers, session detail, live merge, FLIP/flash, accessibility/responsive).
