---
title: Admin API
description: Use authenticated endpoints to manage upstreams, principals, plugin chains, and status.
slug: docs/reference/admin-api
---

Admin endpoints use the Bearer token from the configured authentication provider. Resource changes are database-backed and use optimistic concurrency.

The [consistency contract](/docs/reference/source/consistency-contract/) lists what a successful response guarantees for each admin action: which writes and audit records are durable, whether the serving replica's routing view is already rebuilt, and how other replicas and background work converge.

## Core resources

| Resource | Common endpoints |
| --- | --- |
| Upstreams | `POST /admin/v1/upstreams`, `GET /admin/v1/upstreams`, `PUT /admin/v1/upstreams/{id}`, `DELETE /admin/v1/upstreams/{id}` |
| Principals | `POST /admin/v1/principals`, `GET /admin/v1/principals`, `PUT /admin/v1/principals/{id}` |
| Keys | `POST /admin/v1/principals/{id}/keys`, revoke through the key lifecycle endpoint |
| Plugins | `GET /admin/v1/plugins/registry`, `POST /admin/v1/plugins/wasm` |
| Status | `GET /admin/v1/status`, `GET /admin/v1/export` |

Every mutating request should use the current ETag or revision when the endpoint documents one. Handle `409` conflicts by reading the latest resource and presenting the new state to the operator.

## OAuth flow

OAuth draft start accepts `{ "provider": "anthropic" | "openai" | "gemini", "flow": "device_code" | "browser" }`. Omitting `provider` keeps Anthropic compatibility; omitting `flow` uses OpenAI's device-code flow. The browser flow is supported for OpenAI and Gemini. Paste-code providers return `flow: "paste_code"` and an `authorize_url`. OpenAI device start returns `flow: "device_code"`, a verification URL, one-time user code, polling interval, and expiry. OpenAI browser start returns `flow: "paste_code"`, `provider: "openai"`, `authorize_url`, and `state_token`; Gemini browser start returns `flow: "browser"`, `provider: "gemini"`, `authorize_url`, and `state_token`.

Complete accepts `{ "state_token", "code" }` for paste flows. For OpenAI browser flow, `code` may be the full `http://localhost:1455/auth/callback?...` address copied from the browser or a bare authorization code. For Gemini browser flow, `code` may be the full `http://localhost:51121/oauth-callback?...` address or a bare authorization code. A callback URL with a different `state` returns `400 { "error": "state_mismatch" }`. OpenAI device completion may return `202` with `{ "status": "pending", "poll_interval_secs" }` until the account is authorized, then returns the credential metadata used by `from-oauth-draft`.

Reconnect uses the same provider-specific flow at `/admin/v1/upstreams/{id}/oauth/start` and `/complete`, selecting the provider from the existing upstream kind. OpenAI reconnect checks the ChatGPT account id; Gemini checks the Google account email. A different account returns `409 account_mismatch` without replacing the stored credential. Missing identity returns `missing_account_identity` or `missing_existing_identity`; a changed upstream revision returns `409 stale_revision`.

OpenAI device completion accepts a request containing only `state_token`. Respect `poll_interval_secs` before polling again. Provider throttling returns `429 slow_down`; expired and denied device codes return `400 device_code_expired` and `400 access_denied`. A `404` when starting device login returns `device_login_unavailable`; browser flow does not call the device endpoint.

New Gemini subscription sign-ins and reconnects use the Antigravity OAuth client. After Google approval, the browser lands on `http://localhost:51121/oauth-callback`; the page does not need to load. Copy the full URL from the address bar and paste it into the completion form. Existing Gemini CLI enterprise grants retain the `gemini_cli` profile; new subscription credentials use `antigravity`, and an absent profile defaults to `gemini_cli`. Google retired Gemini CLI consumer login on June 18, 2026; see the [transition announcement](https://developers.googleblog.com/an-important-update-transitioning-gemini-cli-to-antigravity/).

Antigravity completion uses `loadCodeAssist` and `onboardUser` with Antigravity metadata and the daily host `https://daily-cloudcode-pa.googleapis.com`. Usage polling calls `POST /v1internal:retrieveUserQuotaSummary` with `{"project":"<project-id>"}` and uses the same parser and fallback behavior as Gemini CLI sign-ins. It reports the same four windows: Gemini models have 5-hour (`gemini_5h`) and weekly (`gemini_7d`) windows; Claude and GPT-OSS models have 5-hour (`third_party_5h`) and weekly (`third_party_7d`) windows.

> [!WARNING]
> Google Antigravity Additional Terms [§6](https://antigravity.google/terms) prohibit using third-party software, tools, or services to access the Service with Antigravity OAuth. The terms state that this is a breach and may be grounds for suspension or termination of Antigravity and/or Gemini CLI accounts. Operators decide at their own risk whether to use this integration.

Creating an upstream from a completed draft stores the provider-specific credential and seeds both token refresh and subscription usage polling. OAuth endpoints remain behind admin authentication.

## Warm-up

`warmup_enabled` is accepted for OAuth upstreams of kind `anthropic_oauth`, `openai_oauth`, and `gemini_oauth`. OAuth sign-in creates these upstreams with warm-up enabled by default. API-key upstreams reject the field. Warm-up requires stored OAuth credentials; it does not send requests for disabled or credential-less upstreams.

The admin upstream detail exposes the Warm-up toggle, status, manual **Fire now** action, and history for supported OAuth upstreams. `warmup_dialect_plugin` remains available only for `anthropic_oauth`.

## Plugin chains

A principal receives the built-in `subscription-preference` router entry at creation. Use the plugin-chain endpoints to add, remove, or reorder additional entries. Registry reference counts include principal chains and upstream warm-up dialect bindings.

## Detail lookups

Keep-alive sessions, decisions, and request events use static detail paths with a required `id` query parameter:

| Resource | Endpoint |
| --- | --- |
| Keep-alive session or decision | `GET /admin/v1/principals/{principal_id}/cache-keepalive/detail?id=<encoded ID>` |
| Request event, including a keep-alive renewal | `GET /admin/v1/events/detail?id=<encoded ID>` |

Pass the original ID to `URLSearchParams` or an equivalent query encoder, such as Python's `urllib.parse.urlencode` or Rust's `url::form_urlencoded`. Encode the query value once; do not call `encodeURIComponent` before passing it to `URLSearchParams`.

```js
const sessionQuery = new URLSearchParams({ id: sessionId });
const eventQuery = new URLSearchParams({ id: eventId });
const sessionUrl = `/admin/v1/principals/${principalId}/cache-keepalive/detail?${sessionQuery}`;
const eventUrl = `/admin/v1/events/detail?${eventQuery}`;
```

Stored IDs, including composite IDs containing `/`, stay unchanged. Authentication, principal scoping, and response shapes are unchanged.

The old `/admin/v1/principals/{principal_id}/cache-keepalive/{session_or_decision_id}` and `/admin/v1/events/detail/{event_id}` routes are removed. Migrate direct scripts and API clients to the query endpoints. Event-detail audit entries use the static route `/admin/v1/events/detail` and retain the original ID in `payload.event_id`.

## Request event filters

The request-event endpoints accept `event_kind` as a comma-separated value or as repeated query keys. Both forms select the same set of effective event kinds:

```text
event_kind=messages,openai_responses
event_kind=messages&event_kind=openai_responses
```

The filter applies to recent rows, histogram buckets, delta pages, SSE backfill, and live partial and final updates. Any unknown or empty entry returns `400 {"error":"invalid_event_kind"}`. Omitting `event_kind` preserves the default exclusion of cache-keepalive renewal events. `source_kind` remains conjunctive when supplied with `event_kind`.

## Request event diagnostic fields

Request events are stored in `request_events_v1` with all 99 operational, latency-breakdown, and abnormal-stream-error scalar fields in typed relational columns. The `payload` column is dropped, so `GET /admin/v1/events/detail?id=<encoded ID>`, `GET /admin/v1/events/delta`, and final updates on `GET /admin/v1/events/stream` read the committed relational representation directly with zero TOAST overhead.

The optional `observability.request_diagnostics_log` setting additionally emits a `cc_lb.request_diagnostics.v1` stdout line after the row commits. Each line carries the deep diagnostic fields `cache_breakpoints` (without `lookback_prefixes`), `cache_control_message_indices`, `routing_trace`, `iterations`, and `internal_errors`, plus the outcome and session-correlation fields `thread_id`, `observed_session_id`, `input_tokens`, `output_tokens`, `cache_read_input_tokens`, `cache_creation_input_tokens`, `duration_ms`, and `upstream_ttfb_ms`. Optional fields are omitted when absent: `native_cache` when no native cache analysis is recorded, and `cache_write_input_tokens`, `cache_write_input_tokens_30m`, and `cache_write_input_tokens_12h` when not reported. A reported `0` is emitted as `0`, not omitted.
