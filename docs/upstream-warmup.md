# Upstream Warm-up

This document describes the operator reference for the upstream warm-up system in cc-lb.

## What it does

Anthropic OAuth upstreams require periodic activity to keep their five-hour rate limit reset windows active. If an upstream remains idle for too long, the reset window closes, which can lead to unexpected rate limits when traffic resumes. This warm-up system automatically sends minimal requests to opted-in upstreams to keep these windows ticking. It ensures that your upstreams are always ready to handle incoming requests without artificial delays.

## Enabling per upstream

To enable warm-up for a specific upstream, send a PATCH request to the admin API. Set the `warmup_enabled` field to `true` in the request body. PATCH requires an `If-Match` ETag from a prior GET to prevent lost updates.

```bash
# 1. fetch current revision
ETAG=$(curl -sI http://localhost:8080/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" | grep -i '^etag:' | awk '{print $2}' | tr -d '\r')

# 2. PATCH with If-Match
curl -X PATCH http://localhost:8080/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -H "If-Match: $ETAG" \
  -d '{"warmup_enabled": true}'
```

## Disabling per upstream

To disable warm-up for a specific upstream, send a PATCH request to the admin API with `If-Match` from a prior GET.

```bash
ETAG=$(curl -sI http://localhost:8080/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" | grep -i '^etag:' | awk '{print $2}' | tr -d '\r')

curl -X PATCH http://localhost:8080/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -H "If-Match: $ETAG" \
  -d '{"warmup_enabled": false}'
```

## Emergency stop

If you need to stop all warm-up activity immediately across all OAuth upstreams, you can run a direct SQL update on the database. This disables the warm-up flag for all Anthropic OAuth upstreams at once. The `kind` column stores `UpstreamKind::as_str()` output, which is `anthropic_oauth` (snake_case). The update bumps `spec_revision` and `updated_at` so admin replicas pick up the change on the next reconcile.

```sql
UPDATE upstream_spec_v1
SET warmup_enabled = false,
    spec_revision = spec_revision + 1,
    updated_at = NOW()
WHERE kind = 'anthropic_oauth' AND deleted_at IS NULL;
```

## Manual fire

To manually trigger a warm-up cycle or reconcile all jobs immediately, send a POST request to the scheduler reconcile endpoint. This triggers a full reconciliation of all active upstreams and enqueues any missing warmup jobs.

```bash
curl -X POST http://localhost:8080/admin/scheduler/reconcile \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

The endpoint returns:

- **202 Accepted**: The reconciliation job was successfully enqueued.
  ```json
  {
    "status": "enqueued"
  }
  ```

Note that there is no per-row manual fire endpoint in this version. All manual triggers are handled via the global scheduler reconciliation endpoint.

## What you'll see

The warm-up system logs its activity using tracing events. All events use the `warmup` target. You can monitor these logs to track the lifecycle of each warm-up cycle.

- **dispatch_start**: The replica started sending the warm-up request to Anthropic.
  - Fields: `upstream_id`, `action = "dispatch_start"`
- **dispatch_result**: The warm-up request completed, and the replica classified the response.
  - Fields: `upstream_id`, `action = "dispatch_result"`, `status`, `outcome`
- **cycle_key_written**: The replica successfully wrote the new cycle key to the database, marking the cycle complete.
  - Fields: `upstream_id`, `action = "cycle_key_written"`, `cycle_key`
- **cycle_abandoned**: The warm-up cycle was abandoned, either because of missing observations or a permanent failure.
  - Fields: `upstream_id`, `reason`, `error` (optional)

## Multi-replica notes

Warm-up is implemented as an Apalis entity job (`UpstreamWarmupJob`). Cross-replica coordination no longer depends on per-upstream scheduling columns.

Cross-replica safety is guaranteed by two layers:
1. **Apalis claim uniqueness**: Only one worker replica can claim and execute a given `UpstreamWarmupJob` at a time.
2. **Apalis idempotency key uniqueness**: The Jobs table enforces one row per `(job_type, idempotency_key)`, so a completed warm-up cycle key cannot be re-enqueued with the same key.

A crash between dispatch and DB write can cause one extra `max_tokens=1` Haiku call. This at-least-once cost is accepted.

## Cost

Each warm-up is one `max_tokens=1` request to `claude-haiku-4-5-20251001`. At one per 5h per opted-in upstream, expected cost is negligible (less than $0.01/upstream/day).

## Wave-0 outcomes

Initial testing validated the warm-up configuration. The table below lists the verified outcomes:

| Outcome | Value |
|---|---|
| **A1 Model** | `claude-haiku-4-5-20251001` |
| **A2 Branch** | `wait-for-observation` |
| **A5 Estimator Note** | `PASS, cadence=60s sustained (sliding window 5 reqs/300s per token, measured against /api/oauth/usage), hint-required=NO, enforced by PollScheduleEstimator in poll_schedule_estimator.rs` |

## Limitations

The warm-up system has the following limitations:

- No API-key upstream support.
- No per-account dedup, meaning multiple upstream rows backing the same Anthropic account warm independently.
- No automatic SKU rotation.
- No custom warm-up payload.
