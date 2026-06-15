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

If you need to stop all warm-up activity immediately across all OAuth upstreams, you can run a direct SQL update on the database. This disables the warm-up flag for all Anthropic OAuth upstreams at once. The `kind` column stores `UpstreamKind::as_str()` output, which is `anthropic_oauth` (snake_case).

```sql
UPDATE upstreams_v1 SET warmup_enabled = false WHERE kind = 'anthropic_oauth';
```

## Manual fire

You can manually trigger a warm-up cycle for a specific upstream by sending a POST request to the fire-now endpoint.

```bash
curl -X POST http://localhost:8080/admin/v1/upstreams/11111111-2222-3333-4444-555555555555/warmup/fire-now \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

The endpoint returns different response shapes depending on the state of the upstream:

- **200 OK**: The warm-up request succeeded.
  ```json
  {
    "fired": true,
    "cycle_key": 1718000000
  }
  ```
- **202 Accepted**: The warm-up was skipped because another replica or process holds the lease.
  ```json
  {
    "fired": false,
    "reason": "lease_held",
    "held_by": "replica-abc"
  }
  ```
- **400 Bad Request**: The upstream is not an OAuth upstream, or warm-up is disabled for it.
- **502 Bad Gateway**: The warm-up failed permanently, for example due to invalid credentials.
- **503 Service Unavailable**: The warm-up failed due to a transient error, and you should retry later.

## What you'll see

The warm-up system logs its activity using tracing events. All events use the `warmup` target. You can monitor these logs to track the lifecycle of each warm-up cycle.

- **lease_claimed**: A replica successfully claimed the database lease to perform the warm-up.
  - Fields: `upstream_id`, `action = "lease_claimed"`
- **dispatch_start**: The replica started sending the warm-up request to Anthropic.
  - Fields: `upstream_id`, `action = "dispatch_start"`
- **dispatch_result**: The warm-up request completed, and the replica classified the response.
  - Fields: `upstream_id`, `action = "dispatch_result"`, `status`, `outcome`
- **cycle_key_written**: The replica successfully wrote the new cycle key to the database, marking the cycle complete.
  - Fields: `upstream_id`, `action = "cycle_key_written"`, `cycle_key`
- **cycle_abandoned**: The warm-up cycle was abandoned, either because of missing observations or a permanent failure.
  - Fields: `upstream_id`, `reason`, `error` (optional)

## Multi-replica notes

Each replica scans every ~30s; a row warm-up fires exactly once per observed 5h reset thanks to a per-row DB lease and an authoritative cycle key. Container restarts are handled by lease TTL (120s). A crash between dispatch and DB write can cause one extra `max_tokens=1` Haiku call; this cost is accepted.

## Cost

Each warm-up is one `max_tokens=1` request to `claude-haiku-4-5-20251001`. At one per 5h per opted-in upstream, expected cost is negligible (≪ $0.01/upstream/day).

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
