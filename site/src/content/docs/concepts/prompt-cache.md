---
title: Prompt-cache continuity
description: Learn how cc-lb schedules best-effort keepalive requests for active Anthropic sessions.
slug: docs/concepts/prompt-cache
---

Prompt-cache keepalive is a principal-scoped feature. When enabled, cc-lb observes eligible responses, stores the minimum encrypted state needed to rebuild a cache probe, and schedules a later request before the configured TTL expires.

## What the scheduler does

- Uses the response-begin cache anchor, including the streaming `message_start` event when available.
- Builds a cache-control-only request with `max_tokens: 0`, retaining the full prefix through the last breakpoint and its cache-control markers.
- Uses a durable generation fence so a newer real request makes older work a no-op.
- Limits refreshes by the configured lead time, maximum refresh count, and maximum session duration.
- Records bounded outcomes such as scheduled, hit, miss, stale, or error in metrics.

The feature is best effort. A keepalive can miss because the cache changed, credentials failed, the session expired, or the provider rejected the request. Those are terminal business outcomes for that scheduled generation rather than a retry promise.

## Cadence and limits

Keepalive uses the TTL of the last retained breakpoint in the captured upstream-shaped request. A valid mixed request with a 1h breakpoint followed by a 5m breakpoint keeps both segments and both markers, and uses the 5m cadence: 270 seconds with the default 30-second lead time. Single-TTL requests remain unchanged: 270 seconds for 5m (including an omitted TTL), or 3300 seconds for 1h. The initial delay subtracts the cache anchor's age, with a one-second minimum.

The defaults remain 12 successful refreshes and 14400 seconds of total session duration. The scheduler checks these limits after a successful dispatch before scheduling another renewal; the duration setting is not a hard pre-dispatch cutoff or a promise of four hours of keepalive. At an ideal 270-second cadence, 12 refreshes span about 54 minutes, excluding request time and anchor-age adjustments.

Sessions and jobs saved by an older version keep their captured TTL and cadence until the next real request completes. An upgrade does not rewrite that saved policy.

## Attribution and observations

For ordinary responses where routing predicted no match, cc-lb can infer one deepest eligible resolved explicit or automatic breakpoint when usage reports numeric positive `cache_read_input_tokens` and explicit zero cache creation. Zero creation can be a flat `cache_creation_input_tokens: 0`, complete nested `cache_creation` counters with zero `ephemeral_5m_input_tokens` and `ephemeral_1h_input_tokens`, or the complete zero suffix pair `cache_creation_input_tokens_5m` and `cache_creation_input_tokens_1h`. Every supplied representation must contain complete, valid unsigned counters with matching totals. Suffix fields are evidence-only; numeric accounting remains unchanged. Missing, null, partial, invalid, contradictory, or positive-creation usage does not establish evidence for this inferred branch. For SSE, omitted or null fields preserve the corresponding prior evidence components; complete numeric flat, nested, or suffix snapshots replace the prior representation and may change evidence state. Partial, malformed, or contradictory snapshots invalidate evidence; partial snapshots cannot be combined into zero evidence. Predicted-match attribution remains unchanged.

Eligible successful read-only keep-alive renewals publish observations from the exact transformed dispatched body through a bounded, nonblocking best-effort sink, including final capped renewals. Misses, errors, unsupported providers, and ambiguous usage produce no observation. Publication is asynchronous and does not guarantee a synchronous commit, provider acceptance, or cache hit. Observation expiry is anchored immediately before signed dispatch; scheduling still uses response-begin anchors and shared-store decisions are unchanged.

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
