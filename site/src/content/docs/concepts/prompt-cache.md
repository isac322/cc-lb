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

## Request eligibility

An explicit `x-claude-code-request-class: auxiliary` excludes a request from keep-alive completion sequencing, generation observation or mutation, enqueueing, cancellation, and detached bookkeeping. The exclusion follows the explicit header even when body classification reports `unknown`, `subagent`, `main`, or `advisor`. Existing named side-request exclusions remain in place. Headerless `unknown` and `subagent` requests retain their existing eligibility, and real subagent turns remain isolated under their agent-scoped session key.

Claude Code gateway hints are optional client metadata. The [official gateway hint contract](https://code.claude.com/docs/en/llm-gateway-protocol#gateway-hint-headers) documents `x-claude-code-request-class` for Claude Code v2.1.273 and later. With a custom `ANTHROPIC_BASE_URL`, clients must opt in with `CLAUDE_CODE_GATEWAY_HINT_HEADERS=1`; the hints are otherwise off by default. cc-lb does not promise to consume every hint, and this behavior does not identify side requests that omit the explicit class.

Keep-alive firing currently supports direct Anthropic OAuth upstreams. API-key and other upstream kinds are skipped at fire time.

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
