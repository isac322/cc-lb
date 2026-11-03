---
title: Prompt-cache continuity
description: Learn how cc-lb schedules best-effort keepalive requests for active Anthropic sessions.
slug: docs/concepts/prompt-cache
---

Prompt-cache keepalive is a principal-scoped feature. When enabled, cc-lb observes eligible responses, stores the minimum encrypted state needed to rebuild a cache probe, and schedules a later request before the configured TTL expires.

## What the scheduler does

- Uses the response-begin cache anchor, including the streaming `message_start` event when available.
- Builds a cache-control-only request with `max_tokens: 0`.
- Uses a durable generation fence so a newer real request makes older work a no-op.
- Limits refreshes by the configured lead time, maximum refresh count, and maximum session duration.
- Records bounded outcomes such as scheduled, hit, miss, stale, or error in metrics.

The feature is best effort. A keepalive can miss because the cache changed, credentials failed, the session expired, or the provider rejected the request. Those are terminal business outcomes for that scheduled generation rather than a retry promise.

## Compaction boundaries

When a completed Anthropic response contains `stop_reason: "compaction"` or a
top-level `compaction` content block, cc-lb retires the matching keepalive
generation instead of scheduling the pre-compaction request snapshot. This
response-derived fence works without gateway hint headers. A later main
completion built from the adopted summary can establish a fresh generation;
ordinary `pause_turn` and tool-use responses without a compaction block retain
their existing behavior. Explicit compaction-class requests remain excluded
from keepalive state changes.

## Configuration shape

```json
{
  "cache_keepalive": {
    "enabled": true,
    "refresh_lead_time_5m_secs": 30,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 12,
    "max_total_duration_secs": 14400,
    "snapshot_max_bytes": 8388608,
    "min_cache_tokens": 10000
  }
}
```

Keepalive state is encrypted at rest and bound to the principal, session hash, generation, upstream, and job purpose. Downstream authorization headers are not stored in the snapshot.
