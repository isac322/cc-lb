---
title: Prompt-cache continuity
description: Learn how cc-lb schedules best-effort keepalive requests for active Anthropic sessions.
slug: docs/concepts/prompt-cache
---

Prompt-cache keepalive is a principal-scoped feature. When enabled, cc-lb observes eligible responses, stores the minimum encrypted state needed to rebuild a cache probe, and schedules a later request before the configured TTL expires.

## One-hour cache mode

A principal carries one prompt-cache policy:

| `prompt_cache_mode` | Cache controls sent upstream | Keepalive |
|---|---|---|
| `preserve_client` (default) | exactly as the caller sent them | normal |
| `one_hour_no_keepalive` | recognized `ephemeral` controls promoted to `ttl: "1h"` | prohibited |

One-hour mode rewrites only the locations the cache contract already reads: the top-level automatic `cache_control`, tool entries, `system` blocks, and message content blocks. It promotes only markers the provider contract defines - an `ephemeral` control with no `ttl`, `ttl: "5m"`, or `ttl: "1h"`; a control the contract would reject is left as sent. Nested JSON such as tool input schemas, tool arguments, and user payloads is left alone, and a request without cache controls gains none. Other protocols and the count-token endpoints are unchanged.

The saved keepalive configuration is kept rather than deleted, and reading it back shows it as configured. It is dormant while one-hour mode is on, and switching back to `preserve_client` restores it. Enabling the mode retires the principal's active keepalive sessions in the same storage transaction as the policy write, so an older request cannot restart one.

## What the scheduler does

- Uses the response-begin cache anchor, including the streaming `message_start` event when available.
- Builds a cache-control-only request with `max_tokens: 0`.
- Uses a durable generation fence so a newer real request makes older work a no-op.
- Limits refreshes by the configured lead time, maximum refresh count, and maximum session duration.
- Records bounded outcomes such as scheduled, hit, miss, stale, or error in metrics.

The feature is best effort. A keepalive can miss because the cache changed, credentials failed, the session expired, or the provider rejected the request. Those are terminal business outcomes for that scheduled generation rather than a retry promise.

## Configuration shape

```json
{
  "prompt_cache_mode": "preserve_client",
  "cache_keepalive": {
    "enabled": true,
    "refresh_lead_time_5m_secs": 30,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 12,
    "max_total_duration_secs": 14400,
    "snapshot_max_bytes": 524288
  }
}
```

Keepalive state is encrypted at rest and bound to the principal, session hash, generation, upstream, and job purpose. Downstream authorization headers are not stored in the snapshot.

Optional subscription 429 failover is controlled by `prompt_cache_shadow.subscription_failover` (default `false`, startup setting). One alternate subscription may be selected through the complete configured routing policy if process-local conversation protection permits it. Full-TTL protection is independent of grace-shortened cache-hit eligibility and survives delayed observation writes. Historical observations have unknown ownership after restart. See [configuration](/docs/reference/configuration/#subscription-http-429-failover) for limits.

Persisted keepalive renewal anchors retain full-TTL protection after restart. Their model is encrypted, so this evidence can conservatively veto optional fallback across models until expiry; ordinary primary sharing remains available.
