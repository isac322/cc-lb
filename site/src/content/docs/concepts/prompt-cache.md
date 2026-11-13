---
title: Prompt-cache continuity
description: Learn how cc-lb schedules provider-bounded prompt-cache keepalive work for Anthropic OAuth and opted-in OpenAI Responses sessions.
slug: docs/concepts/prompt-cache
---

Prompt-cache keepalive is a principal-scoped feature. When enabled, cc-lb observes eligible responses, stores the minimum encrypted state needed to rebuild a cache probe, and schedules a later request before the configured TTL expires.

## Provider scope and renewal modes

### Anthropic OAuth (`anthropic_oauth`)

Direct Anthropic OAuth sessions use cache-control breakpoints and zero-output renewals with `max_tokens: 0`.

### Anthropic API keys (`anthropic_api_key`)

Replay remains disabled until storage-backed signing is available for this path. Downstream proxy credentials are never replayed to Anthropic.

### OpenAI Responses (`openai_api_key`, `openai_oauth`)

For GPT-5.6 and later, opted-in OpenAI Responses sessions require both `cache_keepalive.enabled` and `cache_keepalive.classifier.openai_responses_enabled`. They accept `prompt_cache_options.ttl` values of `"30m"` or `"12h"`; omitted TTL defaults to `"30m"`. A positive native cached-token read of at least 1,024 tokens and the existing refresh budget are required, including at least 13 refreshes for GPT-5.6 with a 30-minute lifetime.

The selector uses the last supported explicit content breakpoint, or the latest eligible implicit message ending when no explicit breakpoint exists. A selected `"12h"` prefix records `not_tracked` without scheduling a renewal because reads do not restart its clock. Renewals use `max_output_tokens: 16` with `stream: false`, so they can consume paid output tokens and do not guarantee a cache hit; any saving is credited only when a matching follow-up starts after natural expiry. See the [OpenAI keep-alive operator guide](https://github.com/isac322/cc-lb/blob/master/docs/cache-keepalive-operator.md) for the full TTL, breakpoint provenance, and refresh-budget rules.

For `openai_api_key` upstreams, snapshots preserve client-supplied `OpenAI-Organization`, `OpenAI-Project`, and `OpenAI-Processing-Region` selectors; absent selectors remain absent. Credentials remain signer-owned and are never captured in snapshots. Historical snapshots without those selectors require a fresh eligible real request.

### Gemini (`gemini_api_key`, `gemini_oauth`)

Background prompt-cache replay is unsupported. Gemini cache evidence is a separate behavior and is not treated as absent by this scheduler.

Keep-alive firing is provider-gated: direct Anthropic OAuth and eligible, opted-in OpenAI Responses sessions may fire; Anthropic API-key replay and Gemini background replay remain unsupported. Unsupported enabled upstream kinds terminate the session with `terminal_reason=unsupported_provider`.

## What the scheduler does

### Anthropic scheduling

Anthropic scheduling uses the response-begin cache anchor, including the streaming `message_start` event when available. Its renewal request is cache-control-only with `max_tokens: 0`.

### OpenAI scheduling

OpenAI Responses scheduling uses the selected native cache boundary and its provider-specific bounded renewal described above; it does not use Anthropic cache-control or Anthropic cache-creation buckets.

### Shared scheduling behavior

Both provider paths use a durable generation fence so a newer real request makes older work a no-op. Both limit refreshes by the configured lead time, maximum refresh count, and maximum session duration, and record bounded outcomes such as scheduled, hit, miss, stale, or error in metrics.

The feature is best effort. A keepalive can miss because the cache changed, credentials failed, the session expired, or the provider rejected the request. Those are terminal business outcomes for that scheduled generation rather than a retry promise.

## Anthropic compaction boundaries

When a completed Anthropic response contains `stop_reason: "compaction"` or a
top-level `compaction` content block, cc-lb retires the matching keepalive
generation instead of scheduling the pre-compaction request snapshot. This
response-derived fence also works when the current request has no cache
breakpoint, and it works without gateway hint headers. A later main completion
built from the adopted summary can establish a fresh generation; ordinary
no-breakpoint responses remain no-ops and do not supersede pending
cache-bearing work, while ordinary `pause_turn` and tool-use responses without
a compaction block retain their existing behavior. Explicit compaction-class
requests remain excluded from keep-alive state changes.

## Anthropic renewal accounting

Keepalive renewals use Anthropic's nested 5-minute and 1-hour cache-creation counts for usage records and the local cost breakdown. A positive flat creation total with an absent, empty, or all-zero nested breakdown uses the same 5-minute fallback as regular usage accounting. It does not imply a 1-hour write.

This applies to future renewals. Historical usage rows are not rewritten.

## Anthropic renewal economics

Savings are the cache-creation cost a matching follow-up would have paid
without keep-alive. A follow-up is credited only when it starts after the
cycle's natural expiry (`cycle_anchor_ms + ttl_ms`), and credit goes to the
earliest renewal whose TTL window covers it. Avoided tokens are the minimum of
the follow-up's cache read and the credited renewal's kept tokens.

A follow-up at or before natural expiry is `not_needed`: it has zero savings
and the cycle retains the full renewal spend. Missing or ambiguous anchors and
evidence remain unknown rather than being treated as zero. The Admin Web
summary exposes `not_needed_cycles`, and follow-up outcomes distinguish
`not_needed` from saved follow-ups.

## Anthropic request eligibility

An explicit `x-claude-code-request-class: auxiliary` excludes a request from keep-alive completion sequencing, generation observation or mutation, enqueueing, cancellation, and detached bookkeeping. The exclusion follows the explicit header even when body classification reports `unknown`, `subagent`, `main`, or `advisor`. Existing named side-request exclusions remain in place. Headerless `unknown` and `subagent` requests retain their existing eligibility, and real subagent turns remain isolated under their agent-scoped session key.

Claude Code gateway hints are optional client metadata. The [official gateway hint contract](https://code.claude.com/docs/en/llm-gateway-protocol#gateway-hint-headers) documents `x-claude-code-request-class` for Claude Code v2.1.273 and later. With a custom `ANTHROPIC_BASE_URL`, clients must opt in with `CLAUDE_CODE_GATEWAY_HINT_HEADERS=1`; the hints are otherwise off by default. cc-lb does not promise to consume every hint, and this behavior does not identify side requests that omit the explicit class.

For Anthropic, keep-alive firing is limited to direct Anthropic OAuth; Anthropic API-key replay remains unsupported.

## Anthropic attribution and observations

For ordinary Anthropic responses where routing predicted no match, cc-lb can infer one deepest eligible resolved explicit or automatic breakpoint when usage reports numeric positive `cache_read_input_tokens` and explicit zero cache creation. Zero creation can be a flat `cache_creation_input_tokens: 0`, complete nested `cache_creation` counters with zero `ephemeral_5m_input_tokens` and `ephemeral_1h_input_tokens`, or the complete zero suffix pair `cache_creation_input_tokens_5m` and `cache_creation_input_tokens_1h`. Every supplied representation must contain complete, valid unsigned counters with matching totals. Suffix fields are evidence-only; numeric accounting remains unchanged. Missing, null, partial, invalid, contradictory, or positive-creation usage does not establish evidence for this inferred branch. For SSE, omitted or null fields preserve the corresponding prior evidence components; complete numeric flat, nested, or suffix snapshots replace the prior representation and may change evidence state. Partial, malformed, or contradictory snapshots invalidate evidence; partial snapshots cannot be combined into zero evidence. Predicted-match attribution remains unchanged.

Eligible successful Anthropic read-only keep-alive renewals publish observations from the exact transformed dispatched body through a bounded, nonblocking best-effort sink, including final capped renewals. Misses, errors, unsupported providers, and ambiguous usage produce no observation. Publication is asynchronous and does not guarantee a synchronous commit, provider acceptance, or cache hit. Observation expiry is anchored immediately before signed dispatch; scheduling still uses response-begin anchors and shared-store decisions are unchanged.

## Anthropic configuration example

The following configuration is an Anthropic example. OpenAI 30-minute sessions require at least 13 refreshes for GPT-5.6, so this example's `max_refreshes_per_session: 4` is not an OpenAI configuration; use the [OpenAI keep-alive operator guide](https://github.com/isac322/cc-lb/blob/master/docs/cache-keepalive-operator.md) for the OpenAI budget and TTL rules.

```json
{
  "cache_keepalive": {
    "enabled": true,
    "refresh_lead_time_5m_secs": 10,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 4,
    "max_total_duration_secs": 14400,
    "snapshot_max_bytes": 8388608,
    "min_cache_tokens": 10000
  }
}
```

Keepalive state is encrypted at rest and bound to the principal, session hash, generation, upstream, and job purpose. Downstream authorization headers are not stored in the snapshot.
