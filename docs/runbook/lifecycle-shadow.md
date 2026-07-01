# RFC-0002 Phase 3 — Lifecycle Shadow Writer Runbook

## What Phase 3 does

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
