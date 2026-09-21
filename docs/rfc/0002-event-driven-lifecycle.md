# RFC-0002: Event-driven lifecycle

- Feature Name: `event-driven-lifecycle`
- Start Date: 2026-07-01
- Status: Implemented — see §Implementation status (2026-07-02) below. The
  original phased plan is preserved for historical context; the
  as-shipped architecture is captured in the amendment.
- Related PRs: #207 (gzip decoder sidecar), #222 (terminal observation
  guarantee), #241 (16-path integration tests), #242 (unignore
  `terminal_success_stream`)

## Summary

Split cc-lb's request handler into two concerns: (1) an **admission and
response** pipeline that runs synchronously on the request task and produces
the client-visible response, and (2) an **observation and accounting** flow
that runs on background subscribers listening to a per-request event stream.
The proxy request task emits a fixed vocabulary of `LifecycleEvent`s and does
nothing else post-response — no DB writes, no pricing math, no cache
observation, no ObservabilityHook fanout. Each of those is a subscriber on
the existing `RequestEventBus`.

PR #222 already installed the primitives (`RequestEventBus`,
`TerminalObserver`, `RequestEventUpdate`, `RequestEventWriter`). This RFC
generalises them into a full lifecycle-event pipeline and provides the phased
migration that lets every intermediate state ship independently while
preserving the byte-for-byte contract of the current `request_events_v1`
persistence layer.

## Implementation status (2026-07-02)

The migration is complete on branch `opencode/rfc-0002-grace-period`. The
handler no longer builds `RequestEvent` rows; it emits `LifecycleEvent`s
which the assembler and per-concern subscribers observe. The legacy
`request_event_writer` path was removed in Phase 13 and the shadow-vs-legacy
comparison machinery deleted with it.

### As-shipped shape

- **Event vocabulary** lives in `cc-lb-lifecycle` (the resolution of
  unresolved question #2 in the original RFC). Variants match the RFC's
  10-variant shape, with `HeaderSnapshot`, `RouteInfo`, `AuthInfo`,
  `ParseInfo`, `LimitDecisionKind`, `UsageSnapshot`, `StreamSuccess`,
  `StreamError`, and `TerminationReason` supporting types. `predicted_cache_read_tokens`
  was added to `RouteInfo` for the prompt-cache drift subscriber.
- **Persisted `RequestEvent` rows** live in `cc-lb-request-log`, and the
  lifecycle event bus lives in `cc-lb-control`.
- **`LifecycleContext`** in `crates/cc-lb-engine/src/terminal_observer.rs` is
  the successor to `TerminalObserver`. Its state is now just the 9 fields
  the terminal event carries; `make_request_event` is gone. `Drop` emits
  `RequestTerminated { reason: Dropped }` via the terminal-CAS-guarded
  `emit_terminated` helper.
- **Assembler** (`crates/cc-lb-engine/src/lifecycle_event_assembler.rs`) is
  the sole producer of `RequestEvent` rows. It maintains a
  `HashMap<EventId, Partial>` capped at 4096 entries with a 300s TTL and a
  30s sweeper. On `RequestTerminated` it writes a shadow row
  (`event_id = <fresh v7>, shadow_event_id = <incoming EventId>`) and
  republishes the finalized row to the admin SSE broadcast.
- **Per-concern subscribers** replace formerly-inline observations for pricing,
  limit reconciliation, prompt-cache observations, hook fanout, rate-limit
  headers, subscription quota, limit rejection audit, API-key metrics,
  cache hit/miss, and prompt-cache drift.
- **Bus wiring**: `InMemoryBus` fans `LifecycleEvent` out to bounded
  per-subscriber mpsc channels. Required subscribers are always attached.
  Each channel drops-newest on overflow and increments
  `cc_lb_dropped_events_total{reason="<channel>_full"}`. The
  `RequestEventUpdate` broadcast channel remains, now fed exclusively by
  the assembler's post-write republish.
- **Configuration**: required subscribers are attached unconditionally. There
  are no lifecycle subscriber kill switches or alternate request-event writers.
  The assembler is the only writer.

### Deviations from the original phased plan

- **Phase count**: 15 executed phases vs the RFC's 9. The RFC bundled the
  six inline-observation removals into a single "Phase 4 — ObservabilityHook
  adapter subscriber" step; in practice each observation had a distinct
  event footprint (rate-limit headers, subscription quota, limit
  rejection audit, API-key metrics, cache hit/miss, prompt-cache drift)
  and was extracted into its own subscriber (Phases 5a-5f + 6a-6f in the
  as-shipped stack).
- **Subscriber topology**: chain-vs-broadcast — the RFC proposed a linear
  chain (`Pricing → CacheObservation → ObservabilityHook → Assembler →
  Writer`). The as-shipped design is broadcast: each subscriber has its
  own bounded mpsc off `InMemoryBus`, and the assembler is not chained
  downstream of the others. This is because each subscriber reconstructs
  its own `Partial<EventId>` state from the same event stream, so the
  linear-chain premise (that each stage annotates the event with derived
  fields the next stage consumes) turned out not to apply — the assembler
  computes cost from `Priced` events published by the pricing subscriber,
  and admin SSE tees off the same broadcast, but everything else is
  independent.
- **Shadow-mode diff tolerance** (unresolved question #3): the shipped
  implementation writes only shadow rows in production; the transitional
  `Both` writer mode + `cc_lb_lifecycle_shadow_field_drift_total` metric
  existed on the working branch and were removed in Phase 13 when Shadow
  became the sole writer. The 5ms slack tolerance the RFC proposed for
  timing fields was never adopted — the observed drift on `writer_source
  = both` runs was exact for all 68 fields except a handful of legacy
  gaps (35 fields the legacy writer captured that the assembler missed
  early on), which were closed by Phases 1-4 before cutover.
- **Reservation ownership redesign** (RFC Phase 7): the id-based reconcile
  API landed in a pre-existing commit (070c9614 — `H4 - remove handler
  inline limit_engine.reconcile()`) before this branch started. The RFC's
  Phase 7 is therefore listed as pre-work rather than a phase of this
  migration.
- **Assembler eviction policy** (unresolved question #1): shipped with
  drop-oldest at cap 4096 + a `cap_evicted` outcome label on the metric,
  matching the RFC's draft "drop oldest with metric" recommendation.
  Rationale: 4096 in-flight requests per instance far exceeds the observed
  peak (~50 concurrent), and a runaway is a real bug worth an alarm
  rather than silently trading memory.

### Where the code lives

- `crates/cc-lb-lifecycle/` — event vocabulary crate.
- `crates/cc-lb-request-log/` — persisted `RequestEvent` rows.
- `crates/cc-lb-control/src/event_bus.rs` — `InMemoryBus` and per-channel
  attach methods.
- `crates/cc-lb-engine/src/terminal_observer.rs` — `LifecycleContext`.
- `crates/cc-lb-engine/src/lifecycle_event_assembler.rs` — the assembler.
- `crates/cc-lb-engine/src/lifecycle_*_subscriber.rs` — the per-concern
  subscribers.
- `crates/cc-lb-engine/src/event_bus.rs` — `InMemoryBus` + per-channel
  attach methods.
- `crates/cc-lb-server/tests/rfc_0002_fix_live_qa.rs` — end-to-end
  verification (5 tests).

## Motivation

### The observation guarantee's remaining rough edges

PR #222 established the "every request that reached the handler produces
exactly one row, no exceptions except SIGKILL/OOM/abort" contract via
`TerminalObserver`. It works — the 40-row live-QA sample after deploy shows
`terminal_dropped = 0`, `event_id = NULL` count of 0, and no writer failures.

But the handler still owns work that has nothing to do with producing a
response:

1. **RequestEvent assembly** — every success path in `lifecycle.rs` inlines
   ~30 lines that gather usage counts, cache metadata, timing, and cost, then
   hands the built event to `observer.set_prebuilt_event(...)`. This must
   happen in the handler because the observer needs the finished event before
   `finish()`.
2. **ObservabilityHook fanout** — `observe_finished_for_principal` is called
   synchronously in the handler tail (`lifecycle.rs:2830`) and iterates every
   registered hook. Hook implementations can be slow.
3. **Pricing calculation** — `cost_usd_micros` is computed inline before
   `set_prebuilt_event`, calling into the price catalog on the response path.
4. **Cache observation** — prompt-cache prefix hashing and observation
   emission happen mid-response.
5. **Limit reconciliation** — `LimitEngine::reconcile(Reservation,
   UsageCounts)` runs on the handler task after the stream ends. Because
   `Reservation` is a Rust owned value, moving reconciliation off the request
   task would require the engine to keep reservations internally by ID
   (see §Reservation ownership).

The costs of leaving all five in the handler:

- **Response latency**: every one of them is on the tail of every request.
  Sample metric `observability_post_ms` from live data is 0-1ms today, but
  pricing lookup or ObservabilityHook fanout could grow unbounded as we add
  hooks/subscribers.
- **Termination-path complexity**: because the handler owns "build the event
  and persist it", every new termination path (currently 16) requires wiring
  observer state through the specific failure site. Additions to
  `RequestEvent` fields (like PR #222's 11 new columns) touch every success
  path. Field additions in a subscriber-owned world would touch one file.
- **Tower timeout classification**: tower's `TimeoutLayer` wraps the handler
  future and drops it on elapsed. Because `TerminalObserver` is currently
  handler-local, the observer's `Drop` fires with the generic
  `terminal_dropped` code. There is no way to say "this drop was caused by
  the tower timeout" without hoisting the observer into an outer layer — a
  band-aid that only fixes one termination path.

### User constraints (verbatim, m0776–m0787)

Direct quotes from the design conversation that gate this RFC:

> "Because it's an essential control feature, users must be able to
> configure limits and model selection per principal in the cc-lb
> dashboard the way they set them up. Why turn that into events?" (m0786)

**HC-1**: Any decision that determines whether a request is served must
remain synchronous on the response path. This includes authentication,
principal-scoped model allow-lists, `try_reserve` (429 admission),
router selection, signer setup, and dispatch. **Only post-response accounting
may move to events.**

> "I said it has to be low-cost and low-latency — are you keeping that?" (m0784)

**HC-2**: The event bus write must not add measurable latency. The current
`RequestEventBus::publish` is a `try_send` over an mpsc — microseconds — and
this budget must be preserved.

> "If each one is merged and deployed separately, does it still work correctly?" (m0784)

**HC-3**: Every PR in the migration must be independently deployable and
backwards compatible with the previous state of production. A partially
migrated system must produce the same `request_events_v1` rows as a fully
legacy or fully migrated one. No "big bang" cutover.

> "This is an extremely important product feature. Without this kind of visibility, the proxy has no value." (from m0189 originally, still holds)

**HC-4**: The observation guarantee (§PR #222) may not regress at any step.
Any bug in an event-driven subscriber must not cause a row to go missing.

## Guide-level explanation

### What the request handler looks like after full migration

```rust
async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let (ctx, body_too_large) = self.parse(req);
    let lifecycle = LifecycleContext::new(&ctx, &self.event_bus, &self.clock);

    if let Some(response) = body_too_large {
        lifecycle.terminate(StatusCode::PAYLOAD_TOO_LARGE, error_codes::BODY_TOO_LARGE);
        return Ok(*response);
    }

    let principal = match self.authenticate(&ctx) {
        Ok(p) => { lifecycle.emit_auth_ok(&p); p }
        Err(reason) => {
            lifecycle.terminate_auth_failed(reason);
            return Ok(unauthorized_response());
        }
    };

    let reservation = match self.limit_engine.try_reserve(&principal, &ctx) {
        Ok(r) => { lifecycle.emit_limit_reserved(r.id(), r.amount()); r }
        Err(reject) => {
            lifecycle.terminate_limit_rejected(reject);
            return Ok(too_many_requests_response());
        }
    };

    let route = self.router.route(&ctx, &principal)?;
    lifecycle.emit_route_ok(&route);

    let attempt = self.dispatch(&ctx, &route, &principal).await;
    match attempt {
        Ok(response) => {
            let usage_tap = UsageTap::new(lifecycle.clone(), reservation.id());
            let taped_response = usage_tap.wrap(response);
            lifecycle.emit_upstream_response_started(taped_response.status());
            Ok(taped_response)  // client sees response here
            // ... UsageTap drives per-frame UsageObserved emissions
            // ... on final frame OR drop, UsageTap emits StreamCompleted
            // ... LifecycleContext::Drop emits RequestTerminated if
            //     StreamCompleted did not already do so via terminal CAS
        }
        Err(dispatch_error) => {
            lifecycle.terminate_dispatch_failed(dispatch_error);
            Ok(bad_gateway_response())
        }
    }
}
```

There is no `storage.append_request_event` call, no
`observe_finished_for_principal`, no `pricing::compute_cost`, no
`observer.set_prebuilt_event`. The handler emits `LifecycleEvent`s only and
returns the response. Everything else is a subscriber.

### What a subscriber looks like

```rust
struct PricingSubscriber {
    catalog: Arc<PriceCatalog>,
    tx_downstream: mpsc::Sender<LifecycleEvent>,
}

impl PricingSubscriber {
    async fn run(mut self, mut rx: mpsc::Receiver<LifecycleEvent>) {
        while let Some(event) = rx.recv().await {
            let LifecycleEvent::RequestTerminated { event_id, model, usage, .. } = event else {
                let _ = self.tx_downstream.try_send(event);
                continue;
            };
            let cost = self.catalog.compute_cost(&model, &usage);
            let _ = self.tx_downstream.try_send(LifecycleEvent::Priced {
                event_id, cost_usd_micros: cost,
            });
        }
    }
}
```

Subscribers chain: `Pricing → CacheObservation → ObservabilityHook →
RequestEventAssembler → RequestEventWriter`. Each can bail on any input
individually without breaking the chain.

### What the wire looks like

`RequestEventBus` publishes `LifecycleEvent`, not `RequestEventUpdate`. Each
event carries `event_id` (the same UUID v7 the observer generates today) so
subscribers can correlate. The Admin SSE endpoint subscribes to the whole
stream; the writer subscribes only downstream of the assembler.

## Reference-level explanation

### Event vocabulary

```rust
#[non_exhaustive]
pub enum LifecycleEvent {
    RequestStarted {
        event_id: EventId,
        request_id: String,
        ts_ms: u64,
        stream: bool,
    },
    ParseCompleted {
        event_id: EventId,
        result: Result<ParseInfo, ParseFailure>,
    },
    AuthCompleted {
        event_id: EventId,
        result: Result<AuthInfo, AuthFailure>,
    },
    RouteCompleted {
        event_id: EventId,
        result: Result<RouteInfo, RouteFailure>,
    },
    LimitDecision {
        event_id: EventId,
        decision: LimitDecisionKind,
    },
    UpstreamAttempt {
        event_id: EventId,
        attempt_num: u32,
        upstream_id: UpstreamRecordId,
    },
    UpstreamResponseStarted {
        event_id: EventId,
        status: u16,
        headers: HeaderSnapshot,
    },
    UsageObserved {
        event_id: EventId,
        usage: UsageCounts,
        source: UsageSource,  // MessageStart | MessageDelta | MessageStop | NonStreamBody
    },
    StreamCompleted {
        event_id: EventId,
        result: Result<StreamSuccess, StreamError>,
    },
    RequestTerminated {
        event_id: EventId,
        reason: TerminationReason,
        client_status: u16,
        duration_ms: u64,
    },
}
```

**Design notes**:

- **Result-based stage variants.** `Result<Info, Failure>` per stage
  collapses the current 15-variant staccato into 10 events. Each `Failure`
  variant carries the same shape as the current `error_code` constants (e.g.
  `AuthFailure::PrincipalMissing`, `AuthFailure::InvalidToken`).
- **Non-exhaustive.** New stage results in future may add variants without
  breaking subscribers via `#[non_exhaustive]`.
- **`event_id` on every event.** Required for subscriber correlation. Same
  UUID v7 the current `TerminalObserver` generates.
- **No `seq: u64` field.** Oracle recommended sequence numbers to disambiguate
  concurrent emissions, but analysis in m0787 concluded most events flow
  through the handler task sequentially; the only real race is the terminal
  emission, which is protected by the existing `finalized` AtomicBool CAS.
  Sequence numbers would be premature complexity — add later only if a
  subscriber needs parallel processing.

### LifecycleContext (successor to TerminalObserver)

```rust
pub struct LifecycleContext {
    inner: Arc<LifecycleInner>,
}

struct LifecycleInner {
    event_id: EventId,
    request_id: String,
    bus: Arc<dyn RequestEventBus>,
    started: Instant,
    started_unix_ms: u64,
    stream: AtomicBool,          // set by RequestStarted
    terminated: AtomicBool,      // CAS gate for RequestTerminated
    downstream_status: AtomicU16,// last known status for Drop-time fallback
    state: Mutex<LifecycleAccumulator>,  // event_id-scoped snapshot for Drop
}
```

The mutex-guarded accumulator holds enough context to synthesise a
`RequestTerminated` event on Drop with the correct `TerminationReason` even
if the handler never called `terminate_*` explicitly — same design as the
current `TerminalObserver::Drop`. Drop-time emission checks `terminated`
CAS; if already set, no-op.

`LifecycleContext` gets injected into `axum::Request::extensions_mut` at the
outermost middleware, above the tower timeout layer. This resolves the tower
timeout classification (a subclass of the reason this RFC exists):

```rust
async fn timeout_error(error: BoxError) -> Response<Body> {
    let mut response = if error.is::<Elapsed>() {
        let mut resp = Response::new(Body::from("timeout"));
        *resp.status_mut() = StatusCode::GATEWAY_TIMEOUT;
        resp.extensions_mut().insert(TowerTimeoutMarker);
        resp
    } else {
        internal_error_response()
    };
    response
}

async fn lifecycle_middleware(
    State(state): State<LifecycleMiddlewareState>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let ctx = LifecycleContext::new(&request, &state.bus, &state.clock);
    request.extensions_mut().insert(ctx.clone());
    let response = next.run(request).await;
    if response.extensions().get::<TowerTimeoutMarker>().is_some() {
        ctx.terminate_tower_timeout();
    }
    // Drop of ctx here catches everything else (client disconnect, panic
    // unwind in debug builds, etc.) via LifecycleInner::Drop.
    response
}
```

### Backpressure policy per subscriber

Each subscriber has an independent bounded mpsc plus an explicit drop policy.
The bus multiplexes on publish:

```rust
pub struct SubscriberConfig {
    pub name: &'static str,
    pub capacity: usize,
    pub drop_policy: DropPolicy,
}

pub enum DropPolicy {
    DropNewest { warn_ratio: f64 },
    DropOldest { warn_ratio: f64 },
    Block { max_wait: Duration },  // only for durable-critical subscribers
}
```

**Concrete policies**:

| Subscriber | Capacity | DropPolicy | Rationale |
|---|---|---|---|
| Admin SSE | 256 | DropOldest (warn @ 50%) | Live UI; brief lag acceptable; new events more valuable than old for viewers. |
| Pricing | 512 | DropNewest (warn @ 25%) | Cost is nice-to-have; dropped events yield NULL `cost_usd_micros` in the row. |
| Cache observation | 256 | DropNewest (warn @ 25%) | Non-critical analytics. |
| ObservabilityHook fanout | 128 | DropNewest (warn @ 25%) | Hooks are opt-in observability; hook backpressure is caller's concern. |
| RequestEventAssembler | 8192 | Block (max_wait 100ms) | **Feeds the writer** — losing this loses persistence. Large capacity, brief block on overflow, then fail loudly. |
| RequestEventWriter (downstream of assembler) | 4096 | DropNewest (warn @ 25%) + metric alarm | Assembler output; if writer overflows, that's a DB backpressure problem the ops team must see. |

**Metrics** (extending the existing `cc_lb_dropped_events_total{reason=...}`):

- `cc_lb_dropped_events_total{reason="subscriber_full", subscriber="pricing"}` — per-subscriber drops
- `cc_lb_subscriber_lag_ratio{subscriber="pricing"}` — gauge, `queued / capacity`
- `cc_lb_dropped_events_total{reason="assembler_block_timeout"}` — assembler could not enqueue → row lost → alarm

### Reservation ownership redesign

Today (`crates/cc-lb-scheduler/src/lib.rs`):

```rust
impl LimitEngine {
    pub fn try_reserve(&self, ...) -> Result<Reservation, RejectReason>;
    pub fn reconcile(&self, reservation: Reservation, usage: UsageCounts);
}
```

`Reservation` is a moved value. Only the current owner can reconcile it.
Dropping without `reconcile` auto-refunds the full reservation.

Proposed:

```rust
impl LimitEngine {
    pub fn try_reserve(&self, ...) -> Result<ReservationId, RejectReason>;

    pub fn reconcile_by_id(&self, id: ReservationId, usage: UsageCounts) -> Result<(), ReconcileError>;
    pub fn refund_by_id(&self, id: ReservationId) -> Result<(), ReconcileError>;

    // TTL-based sweeper task; refunds reservations older than N minutes with
    // no reconcile signal. Prevents leaks if a subscriber drops an event.
    pub fn spawn_ttl_sweeper(&self, ttl: Duration) -> JoinHandle<()>;
}
```

Internally the engine keeps `Arc<Mutex<HashMap<ReservationId, InternalReservation>>>`.
`reconcile_by_id` is idempotent (second call is a no-op with `Err(AlreadyReconciled)`).
TTL sweeper protects against event loss — worst case is over-refund on
correctly-served requests, which is safer than under-refund.

This decouples `try_reserve` (still on the request task, still synchronous
admission) from `reconcile_by_id` (which can move to a subscriber after
Phase E lands).

### Subscriber chain layout

Events flow through a linear chain, not a broadcast fanout. Each subscriber
consumes events, optionally emits new derived events, and forwards
everything else to the next subscriber:

```
publish → [Admin SSE tee] → [Pricing] → [CacheObservation] → [ObservabilityHook fanout] → [RequestEventAssembler] → [Writer]
```

Admin SSE is a tee (broadcast-style) because it observes without producing
new events. Everything else is a pipeline stage.

**Why chain and not broadcast?** Broadcast requires each subscriber to
independently handle every event kind; a chain lets each stage attach state
(Priced, CacheObserved, HooksFired) so the assembler at the end sees the
complete accumulated event stream with a single unified view.

### Assembly and persistence

`RequestEventAssembler` maintains an in-memory `HashMap<EventId,
PartialRequestEvent>`. On each incoming `LifecycleEvent`, it merges the field
into the partial. On `RequestTerminated`, it finalises and hands the
completed `RequestEvent` to the writer, then evicts from the map.

Orphan protection: a TTL sweeper evicts entries older than 5 minutes and
emits a "orphaned partial" metric. The assembler also caps its map at N
entries and drops-oldest with a metric when N is exceeded — because losing
memory to a runaway is worse than losing a row.

The writer (unchanged from PR #222) does `storage.append_request_event(&row)`
with `ON CONFLICT(event_id) DO NOTHING`.

### Shadow mode (Phase D)

To make the writer cutover safe:

```rust
enum WriterMode {
    LegacyOnly,   // current: handler → observer → writer
    Shadow,      // handler → observer → writer_legacy AND assembler → writer_shadow
    NewOnly,     // handler → LifecycleContext → assembler → writer
}
```

Configured via `Config::features::lifecycle_events`. In shadow mode both
paths run, both write to `request_events_v1` — but with distinct
`event_id` values. A comparison job diffs the two rows for the same
`request_id`. When the diff is empty for N days, switch to `NewOnly` and
remove the legacy path.

## Phased migration

> The plan below is preserved as the design intent. The as-shipped phase
> stack is 15 steps rather than 9, split as described in
> §Implementation status → Deviations from the original phased plan.

Every phase below is a self-contained PR that ships to production, is
backwards compatible, and produces the same set of `request_events_v1` rows
as the phase before it. **HC-3 satisfied.**

**Metric of correctness at each phase**: same 40-row-per-day workload
produces the same 40 rows with the same `error_code` distribution and
`event_id` populated 100%.

### Phase 1 — LifecycleContext middleware + tower timeout classification

- Rename `TerminalObserver` → `LifecycleContext` (type alias for BC).
- Move creation from inside `handle()` to an axum middleware placed above the
  tower timeout layer.
- Add `TowerTimeoutMarker` extension and rewire `timeout_error` to insert it.
- `lifecycle_middleware` checks for the marker post-response and calls
  `ctx.terminate_tower_timeout()`.
- All existing observer method call sites in `lifecycle.rs` become
  `req.extensions().get::<LifecycleContext>()`. Fallback to inline creation
  preserves existing unit tests.
- Add `TOWER_TIMEOUT` to error_code catalog usage.

**Deliverable**: new `error_code="tower_timeout"` rows in DB for the tower
timeout case. Everything else unchanged. ~200 LOC.

### Phase 2 — LifecycleEvent enum + shadow bus

- Define `LifecycleEvent` enum with the 10-variant vocabulary.
- Add `RequestEventBus::publish_lifecycle(event)` alongside existing
  `publish(update)`. Bus internally has two broadcast channels: one for
  `RequestEventUpdate` (legacy), one for `LifecycleEvent` (new).
- Wire the handler to emit both: every place that currently calls an
  observer method also calls `ctx.emit_lifecycle(event)`. **Legacy path
  still primary and authoritative — new events are advisory.**
- Add a no-op `LifecycleEventLogger` subscriber that just increments metrics
  by event kind. Confirms the emission is happening.

**Deliverable**: new metric `cc_lb_lifecycle_events_total{kind="..."}`
matches the row count 1:1. No behaviour change. ~400 LOC.

### Phase 3 — RequestEventAssembler subscriber (shadow write)

- Implement `RequestEventAssembler` that consumes `LifecycleEvent`s and
  emits `RequestEvent`s to a dedicated writer, distinct from the legacy
  writer.
- Extend the SQLite/Postgres schema with a `shadow_event_id TEXT NULL`
  column on `request_events_v1` — nullable, no unique constraint. Shadow
  rows carry `event_id = <shadow uuid>, shadow_event_id = <legacy uuid>`;
  legacy rows carry `event_id = <legacy uuid>, shadow_event_id = NULL`.
  This lets both paths coexist without collision on the partial unique
  index.
- During the migration phase, the shadow writer was enabled for an overnight
  comparison before the clean cutover.
- Add a comparison SQL query documented in `docs/runbook/lifecycle-shadow.md`
  that diffs shadow vs legacy rows for the same `request_id`.

**Deliverable**: both paths writing rows in shadow mode. `error_code`,
`status`, `event_id` present on both. ~600 LOC + 2 migrations. Legacy path
still authoritative.

### Phase 4 — Direct observability hooks

- Keep `ObservabilityHook` delivery on the request path, where principal plugin
  chains are already resolved.
- Do not duplicate hook delivery through the lifecycle event bus. The event bus
  remains responsible for durable lifecycle-derived subscribers only.

**Deliverable**: hooks fire once through the resolved principal pipeline.

### Phase 5 — Pricing + CacheObservation subscribers

- Implement `PricingSubscriber` and `CacheObservationSubscriber`. Both are
  additive — they emit `LifecycleEvent::Priced { cost_usd_micros }` and
  `LifecycleEvent::CacheObserved { ... }` respectively. Assembler picks
  these up.
- **Handler continues computing cost + emitting cache observations** in this
  phase — again shadow behaviour.

**Deliverable**: shadow rows now have all downstream-computed fields
populated. Compare with legacy rows via runbook query. ~500 LOC.

### Phase 6 — Legacy writer cutover

- Feature flag `features.request_event_writer_source = "shadow" | "legacy"
  | "both"`, default `"legacy"`.
- Set to `"both"` in staging for a week, watch the diff.
- Set to `"both"` in prod for a week, watch the diff.
- If empty diff sustained, set to `"shadow"`.
- Remove legacy code in the following PR (Phase 7 pre-requisite).

**Deliverable**: cutover PR is trivial (single config change). Rollback is
also a single config change. ~50 LOC + follow-up cleanup PR.

### Phase 7 — Reservation ownership redesign

- Refactor `LimitEngine` internally to store reservations by ID in a
  `HashMap`. Add `reconcile_by_id`, `refund_by_id`, TTL sweeper.
- **API surface change is internal to the crate**; the handler still calls
  `try_reserve` and gets back an ID, still calls `reconcile` (now a thin
  wrapper over `reconcile_by_id`) synchronously.
- No behaviour change externally; sweeper is off by default this phase.

**Deliverable**: engine internals redesigned. All existing tests pass. TTL
sweeper opt-in via feature flag. ~700 LOC.

### Phase 8 — Limit reconciliation via events

- Implement `LimitReconcileSubscriber` that consumes
  `LifecycleEvent::LimitReserved { reservation_id }` +
  `LifecycleEvent::RequestTerminated { usage }` and calls
  `engine.reconcile_by_id(id, usage)`.
- Handler stops calling `reconcile` inline.
- TTL sweeper enabled unconditionally to handle event loss (worst case:
  full refund of a reservation whose subscriber-side reconcile was
  dropped).
- Shadow mode: run reconcile both inline AND via subscriber for a week,
  compare limit engine metrics.

**Deliverable**: post-response reservation reconciliation. Latency
`limit_reconcile_ms` observable drop of 1-5ms on the handler tail. ~400 LOC.

### Phase 9 — Handler cleanup

- Delete `set_prebuilt_event`, `attach_cache_metadata`, `update_usage` from
  the observer API — subscribers own that state now.
- Handler emits only `LifecycleEvent`s.
- `TerminalObserver` fully renamed to `LifecycleContext`.
- BC shim removed.

**Deliverable**: handler is ~500 LOC lighter. Observation contract preserved
by phase-by-phase shadow-mode validation. ~-1000 LOC net.

### Total estimated effort

~3800 net LOC across 9 PRs, of which most is additive (Phases 2-5).
Phase 6+ is where net LOC starts going negative.

Timeline: 2-4 weeks with focused iteration, longer if shadow-mode diffs
surface unexpected divergences.

## Drawbacks

- **Complexity budget.** Nine PRs is a lot. Every intermediate state is a
  fully-functional production deployment, but the intermediate states have
  more moving parts than either the current design or the fully migrated
  end state. Someone unfamiliar looking at the codebase mid-migration will
  see two parallel paths and have to know which is authoritative.
- **Test surface doubling.** Every intermediate phase needs tests for both
  the legacy and new paths, plus a shadow-comparison suite.
- **Ordering assumption.** The chained-subscriber design assumes each
  subscriber processes events strictly in the order they arrive. If a
  subscriber becomes async-heavy and needs concurrent processing, the
  design breaks — sequence numbers would need to be added (deferred
  decision).

## Rationale and alternatives

### Alternative 1 — do nothing beyond PR #222

**Rejected.** The handler still owns telemetry work that the user considers
non-response-critical. Adding future subscribers (metrics exporters, audit
replay, third-party analytics) forces changing the handler each time.
Also, tower timeout stays as `terminal_dropped`.

### Alternative 2 — middleware hoist band-aid (§Section 1 of m0785)

**Rejected by user at m0776**: "If you're going to make such a
fundamental change, the proxy functionality is core logic, so keep only
what's strictly necessary in the proxy-related function call tree..." —
the band-aid solves only tower timeout classification and leaves
the rest of the handler still doing telemetry. Effort spent on the band-aid
would be discarded by this RFC anyway.

### Alternative 3 — synchronous DB fallback on subscriber overflow

**Rejected.** If the writer channel overflows, doing a synchronous SQLite
write on the request path blocks the response. Explicit HC-2 violation. The
correct fallback is a durability-critical alarm + oversized channel, which
this RFC specifies.

### Alternative 4 — sequence numbers on every event

**Rejected for v1.** Oracle in m0782 recommended `(event_id, seq)` tuple
per event. Analysis in m0787 showed most events flow through the handler
task sequentially; only terminal race is real, and it's already handled by
the AtomicBool CAS. Sequence numbers can be added in a future RFC if a
subscriber needs parallel processing.

### Alternative 5 — replay from persisted event log

**Rejected as non-goal.** Persisting the raw event trail would enable
process-crash recovery of in-flight requests. But durability guarantees
against SIGKILL/OOM were already declared out-of-scope by the user's
original spec (m0189: "as long as the process wasn't force-killed in a
way the server couldn't recover from"). Adding an event log is a much
larger project — separate RFC.

## Prior art

- **Linkerd2-proxy** — separates timeout metrics from route error metrics
  via a custom stream-wrapping middleware. Similar spirit to Phase 1 but
  focused on stream timeouts (which cc-lb doesn't have; we timeout the
  entire request future). Documented in the librarian's bg_21c21929
  research at m0767.
- **Cloudflare Pingora** — tracks connection lifecycle via `Tracing::Drop`.
  Same pattern as cc-lb's current `TerminalObserver::Drop` — this RFC
  generalises the pattern to a full event stream, not just terminal state.
- **Otel Collector** — chained subscribers with per-hop backpressure. This
  RFC's subscriber chain design is directly inspired by the Otel Collector's
  processor pipeline, minus the DAG complexity.

## Unresolved questions

All three questions raised in the original RFC were resolved during
implementation; the resolutions are recorded in
§Implementation status → Deviations from the original phased plan and
summarised here.

1. **Assembler eviction policy under memory pressure.** *Resolved:*
   drop-oldest at cap 4096 with a `cap_evicted` outcome label on
   `cc_lb_lifecycle_assembler_events_total`. TTL 300s + 30s sweeper.
   4096 far exceeds observed peak concurrency (~50), so hitting the cap
   is a bug-detection signal, not a routine backpressure event.
2. **Cross-crate event definition location.** *Resolved:* the event
   vocabulary lives in `cc-lb-lifecycle`; the event bus is in `cc-lb-control`,
   and persisted `RequestEvent` rows live in `cc-lb-request-log`. No
   compile-fan-out issues surfaced in practice.
3. **Shadow-mode diff tolerance.** *Resolved (moot):* the transitional
   `writer_source = both` mode + drift metric were removed in Phase 13.
   Empirically, exact equality held on all 68 fields once Phases 1-4
   closed the 35-field gap in the assembler. No slack tolerance ended up
   being needed.

## Future extensions (out of scope for this RFC)

- **Persisted event log** for process-crash replay (separate RFC).
- **Multi-instance event bus** for HA cc-lb clusters (separate RFC —
  requires per-instance shard key, possibly Redis Streams).
- **PDK/plugin lifecycle event injection** so plugins can observe
  request-level events they don't currently see (natural extension after
  the internal bus stabilises).
- **Cost-based dynamic throttling** using live `Priced` events to adjust
  admission decisions (crosses HC-1 — would need explicit user sign-off).
