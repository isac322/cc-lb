---
title: "Upstream warm-up source notes"
description: "Curated operator notes for OAuth upstream warm-up and quota-aware scheduling."
slug: docs/reference/source/upstream-warmup
---

## What it does

Warm-up supports Anthropic OAuth, OpenAI ChatGPT OAuth, and Gemini OAuth with either the `gemini_cli` or `antigravity` profile. It periodically sends a small provider request to keep reported subscription quota windows active. Warm-up targets the provider-specific windows below and automatically skips targets the account reports as absent. Weekly-only accounts, including Claude accounts without a five-hour limit and OpenAI Pro accounts, never use a five-hour cadence; they warm their weekly window instead.

| Upstream kind | Target windows |
| --- | --- |
| `anthropic_oauth` | `5h`, `7d` |
| `openai_oauth` | `5h`, `7d` |
| `gemini_oauth` | `gemini_5h`, `gemini_7d` (Gemini models only) |

An explicitly absent window is excluded. Accounts that report no five-hour limit, such as verified Claude Ultra and OpenAI Pro accounts, warm up on their weekly window. Gemini warm-up does not start the Claude/GPT-OSS windows (`third_party_5h`, `third_party_7d`).

`warmup_enabled` defaults to `true` for all three OAuth kinds created through `POST /admin/v1/upstreams` or the OAuth sign-in flow. Disabled upstreams and upstreams without OAuth credentials do not send warm-up requests. API-key upstreams do not support warm-up. Use the admin API for the toggle, status, manual fire, and history.

### Provider requests and models

Every provider uses the same credential signing and request shaping as proxied traffic:

- Anthropic sends `POST /v1/messages` to `claude-haiku-4-5-20251001` with `max_tokens: 1`.
- OpenAI sends a ChatGPT Codex `POST /responses` request with `stream: true`, `store: false`, instructions, and one user message `"."`. It reads the SSE stream to completion and parses the Codex quota and credit response headers. The headers do not report `overage_limit_reached`; that restriction comes from the usage poll.
- Gemini sends `generateContent` with one user message `"."` and `maxOutputTokens: 1`. The issuing OAuth profile determines the envelope, host, user agent, and project.

OpenAI and Gemini models are configurable:

```toml
[warmup]
openai_model = "gpt-6.1-sol"
gemini_model = "gemini-2.5-flash-lite"
```

These are the defaults. Choose a model available to the connected subscription; Gemini warm-up uses a Gemini model, not a third-party model.

### Shape plugins

`warmup_dialect_plugin` is available for all three OAuth kinds. The selected plugin receives the minimal native request before the normal credential signer runs:

| Upstream kind | Required shape export | Plugin input |
| --- | --- | --- |
| `anthropic_oauth` | `cc_lb_shape_anthropic` or the legacy `cc_lb_shape` | Anthropic `/v1/messages` |
| `openai_oauth` | `cc_lb_shape_openai` | OpenAI `/v1/responses` body, before Codex OAuth signing |
| `gemini_oauth` | `cc_lb_shape_gemini` | Native Gemini `generateContent` body, before the Code Assist envelope and OAuth signing |

The admin API rejects OpenAI or Gemini plugin assignments with `400 warmup_plugin_unsupported_provider` when the uploaded artifact's registry capabilities lack that provider's shape export. A response-transform hook alone is not sufficient, and legacy unqualified shape exports apply only to Anthropic. An incompatible plugin that is replaced after assignment fails warm-up explicitly rather than silently sending the untransformed request.

## Re-enabling per upstream

To turn warm-up back on for an upstream you previously disabled, send a PATCH request to the admin API. Set the `warmup_enabled` field to `true` in the request body. PATCH requires an `If-Match` ETag from a prior GET to prevent lost updates. The upstream must already hold OAuth credentials; otherwise the request returns `400 warmup_requires_oauth_credentials`.

These examples use the default admin listener at `http://[::1]:9090` (`[::1]:9090`). Use your configured admin address if you changed the listener.

```bash
# 1. fetch current revision
ETAG=$(curl -sI http://[::1]:9090/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" | grep -i '^etag:' | awk '{print $2}' | tr -d '\r')

# 2. PATCH with If-Match
curl -X PATCH http://[::1]:9090/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -H "If-Match: $ETAG" \
  -d '{"warmup_enabled": true}'
```

## Disabling per upstream

To disable warm-up for a specific upstream, send a PATCH request to the admin API with `If-Match` from a prior GET.

```bash
ETAG=$(curl -sI http://[::1]:9090/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" | grep -i '^etag:' | awk '{print $2}' | tr -d '\r')

curl -X PATCH http://[::1]:9090/admin/v1/upstreams/11111111-2222-3333-4444-555555555555 \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -H "If-Match: $ETAG" \
  -d '{"warmup_enabled": false}'
```

## Emergency stop

To stop warm-up across all supported OAuth upstreams, disable their flags in the database. The update bumps `spec_revision` and `updated_at` so admin replicas pick up the change on the next reconcile. `updated_at` is integer Unix seconds on SQLite and `TIMESTAMPTZ` on PostgreSQL.

**SQLite (default):**

```sql
UPDATE upstream_spec_v1
SET warmup_enabled = false,
    spec_revision = spec_revision + 1,
    updated_at = unixepoch()
WHERE kind IN ('anthropic_oauth', 'openai_oauth', 'gemini_oauth')
  AND deleted_at IS NULL;
```

**PostgreSQL:**

```sql
UPDATE upstream_spec_v1
SET warmup_enabled = false,
    spec_revision = spec_revision + 1,
    updated_at = NOW()
WHERE kind IN ('anthropic_oauth', 'openai_oauth', 'gemini_oauth')
  AND deleted_at IS NULL;
```

## What you'll see

The warm-up system logs its activity using tracing events. All events use the `warmup` target. You can monitor these logs to track the lifecycle of each warm-up cycle.

- **dispatch_start**: The replica started sending the warm-up request to the provider.
  - Fields: `upstream_id`, `action = "dispatch_start"`
- **dispatch_result**: The warm-up request completed, and the replica classified the response.
  - Fields: `upstream_id`, `action = "dispatch_result"`, `status`, `outcome`
- **cycle_key_written**: The replica successfully wrote the new cycle key to the database, marking the cycle complete.
  - Fields: `upstream_id`, `action = "cycle_key_written"`, `cycle_key`
- **cycle_abandoned**: The warm-up cycle was abandoned, either because of missing observations or a permanent failure.
  - Fields: `upstream_id`, `reason`, `error` (optional)

## Quota-aware scheduling

Warm-up evaluates each provider's reported target windows independently. Weekly-only accounts have no five-hour target and never use a five-hour cadence. Warm-up ignores explicitly absent targets. It sends a request when any fresh target is idle. Otherwise, a known active target skips the request until the earliest known reset, even when another target is stale or missing. If no target is known active, unknown samples allow the normal warm-up request. When no target provides a known future reset, the all-target fallback is seven days for weekly-only accounts; the five-hour fallback applies only to accounts with a reported five-hour target. All scheduled evaluations retain the post-reset guard and stable jitter.

For OpenAI and Gemini, a zero-utilization rolling window whose reset is at least one window duration minus 60 seconds after observation is idle.

After a successful request, each response-present window supersedes its older cached snapshot, including when the response omits a future reset. For targets not reported by that response, the scheduler uses fresh snapshot resets. It selects the earliest known future reset, bounded by the shortest present window's normal cadence when any target still has no known future reset: five hours for accounts with a five-hour target, or seven days for weekly-only accounts.

Presence follows the latest selected sample kind, not its age: an `Absent` sample remains absent after its freshness interval expires, so an older stale five-hour sample cannot restore the five-hour cadence. A newer actual sample or a window reported by the successful response establishes presence again. `Unobserved` targets and `Stale` actual samples remain distinct unknown states, not evidence of structural absence; without an explicit `Absent` sample, their cadence remains conservative. Missing response headers alone never establish absence.

If the local usage cache or a warm-up response shows a fresh weekly window is exhausted, the attempt is skipped as `SevenDayQuotaExhausted` instead. This includes Anthropic's shared and model-scoped weekly limits, OpenAI's `7d`, and Gemini's `gemini_7d`. That skip records the reset cycle key so operators can tell the account is quota-blocked rather than merely already warm.

Reset-based jobs run at `reset + 30..59s`, using the idempotency key `adaptive:warmup:{upstream_id}:{reset}`. The guard lets the provider roll the window; stable jitter spreads requests across upstreams. Weekly exhaustion never marks an upstream successfully warmed. Anthropic accounts that report a five-hour window retain their five-hour reset cadence, and also receive a request if the weekly window is idle.

The watchdog treats any pending, queued, running, or retryable failed warm-up task with that cycle-key family as active, even when its `run_at` is in the future. It will not pull a future reset task forward. If the only remaining warm-up task is dead-lettered (`Failed` at max attempts or `Killed`), the watchdog may bootstrap a new immediate warm-up on its next tick.

### Shared usage and reset-coupon observations

For Anthropic, the OAuth usage poll requests `/api/oauth/usage?cedar_ember=1` once per upstream. The same response supplies quota windows, extra usage, and reset-coupon status. It does not send `skip_spend=1`, so the existing extra-usage collection is retained.

`GET /admin/v1/upstreams/{id}/limit-resets` reads the stored observation without contacting Anthropic. The poll resolves account identity once per credential version when coupon data is present; subsequent polls reuse that identity. Before the first observation, after credential replacement, or when the observation is stale, the API does not advertise a usable coupon.

Coupon claims still verify the live account identity and send a single provider POST. A durable, atomically acquired fence prevents overlapping claims and stops an older poll response from restoring a consumed coupon. A successful reset queues a usage poll. If a claim leaves an unresolved fence, a poll may recover it after five minutes by fetching new provider state; recovery never repeats the claim POST.

## Multi-replica notes

Warm-up is implemented as an Apalis entity job (`UpstreamWarmupJob`). Cross-replica coordination no longer depends on per-upstream scheduling columns.

Cross-replica safety is guaranteed by two layers:
1. **Apalis claim uniqueness**: Only one worker replica can claim and execute a given `UpstreamWarmupJob` at a time.
2. **Apalis idempotency key uniqueness**: The Jobs table enforces one row per `(job_type, idempotency_key)`, so a completed warm-up cycle key cannot be re-enqueued with the same key.

A crash between dispatch and the database write can cause one extra provider request. This at-least-once cost is accepted.
