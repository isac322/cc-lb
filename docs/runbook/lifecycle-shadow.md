# RFC-0002 Phase 3 — Lifecycle Shadow Writer Runbook (historical)

> **Historical.** The dual-write shadow mode this document describes was
> removed in Phase 13. The assembler is now the only writer of
> `request_events_v1`. For current operational guidance see
> `docs/runbook/rfc-0002.md`. This file is kept for rollout audit
> context.

## What Phase 3 did

When `features.lifecycle_shadow_writer.enabled = true`, cc-lb runs BOTH writer paths for every request:

- **Legacy path** (authoritative): handler → `TerminalObserver::finish()` → `RequestEventWriter` → `request_events_v1` with `event_id = <legacy uuid>`, `shadow_event_id = NULL`.
- **Shadow path** (advisory, new): handler → `LifecycleContext::emit_lifecycle(...)` → `InMemoryBus::publish_lifecycle` → `RequestEventAssembler` → `request_events_v1` with `event_id = <shadow uuid>`, `shadow_event_id = <legacy uuid>`.

The two rows are distinct on `event_id` (partial unique index enforces `event_id IS NOT NULL` uniqueness). Comparison joins on `shadow.shadow_event_id = legacy.event_id`.

## Enable

Add to `cc-lb.toml`:

```toml
[lifecycle_shadow_writer]
enabled = true
```

Restart the service. Legacy behaviour is unchanged; a second row is written per request as the assembler receives events.

## Comparison query (SQLite)

Diff shadow vs legacy for a rolling window. Empty result = shadow matches legacy on the audited columns.

```sql
WITH pairs AS (
  SELECT
    shadow.request_id,
    legacy.event_id  AS legacy_event_id,
    shadow.event_id  AS shadow_event_id,
    legacy.status    AS legacy_status,
    shadow.status    AS shadow_status,
    legacy.error_code AS legacy_error_code,
    shadow.error_code AS shadow_error_code
  FROM request_events_v1 shadow
  JOIN request_events_v1 legacy
    ON legacy.event_id = shadow.shadow_event_id
   AND legacy.request_id = shadow.request_id
  WHERE shadow.shadow_event_id IS NOT NULL
    AND shadow.ts >= strftime('%s', 'now', '-24 hours')
)
SELECT *
FROM pairs
WHERE legacy_status     IS NOT shadow_status
   OR legacy_error_code IS NOT shadow_error_code;
```

## Comparison query (Postgres)

```sql
WITH pairs AS (
  SELECT
    shadow.request_id,
    legacy.event_id   AS legacy_event_id,
    shadow.event_id   AS shadow_event_id,
    legacy.status     AS legacy_status,
    shadow.status     AS shadow_status,
    legacy.error_code AS legacy_error_code,
    shadow.error_code AS shadow_error_code
  FROM request_events_v1 shadow
  JOIN request_events_v1 legacy
    ON legacy.event_id = shadow.shadow_event_id
   AND legacy.request_id = shadow.request_id
  WHERE shadow.shadow_event_id IS NOT NULL
    AND shadow.ts >= NOW() - INTERVAL '24 hours'
)
SELECT *
FROM pairs
WHERE legacy_status IS DISTINCT FROM shadow_status
   OR legacy_error_code IS DISTINCT FROM shadow_error_code;
```

## Metrics

- `cc_lb_lifecycle_assembler_rows_total{outcome="written"}`: shadow row persisted successfully.
- `cc_lb_lifecycle_assembler_rows_total{outcome="terminated_without_partial"}`: `RequestTerminated` arrived without a prior `RequestStarted` — legacy row still exists, shadow row is skipped.
- `cc_lb_lifecycle_assembler_rows_total{outcome="orphan_ttl_evicted"}`: a partial sat in the map longer than 5 minutes without a terminator — memory hygiene, expected to be zero in steady state.
- `cc_lb_lifecycle_assembler_rows_total{outcome="cap_evicted"}`: assembler map exceeded 4096 in-flight partials; oldest dropped — indicates load spike or a missing terminator bug.
- `cc_lb_dropped_events_total{reason="lifecycle_assembler_full"}`: assembler mpsc backpressure; events lost before they reach the map.
- `cc_lb_dropped_events_total{reason="lifecycle_assembler_storage_error"}`: assembler could not persist the row (DB down or migration missing).

## Disable / rollback

Set `features.lifecycle_shadow_writer.enabled = false` and restart. Legacy path is unaffected; the assembler task exits on shutdown and no new shadow rows are written. Existing shadow rows can stay in the table; they are ignored by every consumer that filters on `shadow_event_id IS NULL`.

## Cleanup

Shadow rows accumulate at 1x the request volume. Rely on the existing `prune_request_events_before` job (already retention-bounded) or run a targeted delete:

```sql
DELETE FROM request_events_v1
WHERE shadow_event_id IS NOT NULL
  AND ts < strftime('%s', 'now', '-7 days');
```

## RFC-0002 Phase 7 — Reservation ownership redesign

`LimitEngine` now stores reservations in an ID-keyed `HashMap<ReservationId, ReservationRecord>`. New public methods:

- `reconcile_by_id(id, actual_input, actual_output, actual_cost_micros) -> bool`
- `refund_by_id(id) -> bool`

Existing `LimitEngine::reconcile(reservation, ...)` is now a thin wrapper over `reconcile_by_id`. `Drop for Reservation` refunds full via id lookup if nobody consumed the record.

`LifecycleEvent::LimitDecision::Reserved.reservation_id` now carries the real engine ID (a Uuid v7 string) instead of the synthetic `{principal_id}-{key_id}`.

### TTL sweeper (opt-in)

Enable via config:

```toml
[features.limit_reservation_ttl]
enabled = true       # default false
ttl_secs = 300       # default 300s
tick_secs = 30       # default 30s
```

Metric: `cc_lb_limit_reservation_ttl_evicted_total` — reservations refunded because they outlived `ttl_secs`. Nonzero in steady state suggests handler paths that reserve but never reconcile (bug) or clients that hang up without cancellation propagation.

## RFC-0002 Phase 8 — LimitReconcileSubscriber (shadow)

New subscriber `LimitReconcileSubscriber` consumes `LifecycleEvent`s and can reconcile out of band with the handler.

### Rollout stages

1. **`features.lifecycle_limit_reconcile_subscriber.enabled = false`** (default): subscriber not spawned; handler inline reconciles as before.
2. **`enabled = true, shadow = true`**: subscriber runs and increments `cc_lb_limit_reconcile_subscriber_rows_total{outcome="shadow_would_reconcile"}` on every reserved+success termination but does NOT touch the engine. Handler inline reconcile stays authoritative. Watch the shadow counter for a soak week; it must match request success rate 1:1.
3. **`enabled = true, shadow = false`**: subscriber calls `reconcile_by_id`. The handler must ALSO be updated to stop calling `reconcile` inline (a future PR) — otherwise reservations double-refund. Do NOT flip `shadow=false` without that change.

### Metrics

- `cc_lb_limit_reconcile_subscriber_rows_total{outcome="shadow_would_reconcile"}`: shadow-mode measurement of reconciles the subscriber would have made.
- `cc_lb_limit_reconcile_subscriber_rows_total{outcome="reconciled"}`: authoritative-mode reconciles that hit a live reservation.
- `cc_lb_limit_reconcile_subscriber_rows_total{outcome="id_unknown"}`: authoritative-mode attempts against an unknown id (already reconciled by handler, refunded by TTL, or never issued).
- `cc_lb_limit_reconcile_subscriber_rows_total{outcome="skipped_non_success"}`: skipped because the request terminated with an error (matches legacy behavior — the handler doesn't reconcile on failure either).
- `cc_lb_limit_reconcile_subscriber_rows_total{outcome="skipped_no_usage"}`: reserved but no usage event ever arrived (client hang-up, upstream failure before body).
- `cc_lb_limit_reconcile_subscriber_rows_total{outcome="no_reservation"|"empty_reservation_id"}`: terminated request without a matching reservation event — expected for calls that never hit the limit engine.
- `cc_lb_dropped_events_total{reason="lifecycle_limit_reconcile_full"}`: subscriber mpsc backpressure; events lost before they reach the partial map.

## RFC-0002 Phase 9 — Handler cleanup (partial)

Phase 9 in this PR is a partial preparation for the final cutover:

- `TerminalObserver` type alias is deleted; every call site now uses `LifecycleContext` directly.
- The three observer methods that will be deleted after the writer cutover (`update_usage`, `set_prebuilt_event`, `attach_cache_metadata`) carry `TODO(rfc-0002-phase-9-cutover)` markers documenting the precondition.

The RFC-0002 Phase 9 spec requires deleting those methods and making the handler emit only `LifecycleEvent`s. That deletion is safe ONLY when:

1. `features.request_event_writer_source = "shadow"` has been in prod for the soak period defined by the writer cutover (Phase 6).
2. `features.lifecycle_limit_reconcile_subscriber.{enabled=true, shadow=false}` has been in prod for a soak period and reservation refund parity is verified.

A follow-up PR will perform the final deletion.
