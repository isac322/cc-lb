---
title: Configuration
description: Configure secrets, admin authentication, storage, proxy listeners, and scheduler behavior.
slug: docs/reference/configuration
---

cc-lb reads configuration from `cc-lb.toml` and environment-backed secret references. Keep secret values in the environment or deployment secret store.

## Required startup inputs

- `CC_LB_MASTER_KEY` for encrypted runtime fields.
- An admin authentication provider and its secret, such as `CC_LB_ADMIN_TOKEN`.
- A storage URL or path supported by the selected server build.
- A proxy and admin listener configuration.

## Admin provider

```toml
[[admin.auth.providers]]
kind = "static_token"
id = "local"
token_env = "CC_LB_ADMIN_TOKEN"
```


## Subscription OAuth providers

The optional `[oauth.anthropic]`, `[oauth.openai]`, and `[oauth.gemini]` sections override provider endpoints and installed-app client settings for authenticated subscription login. Defaults target the real provider endpoints; use local mock URLs only for tests or isolated development.

```toml
[oauth.openai]
client_id = "app_EMoamEEZ73f0CkXaXp7hrann"
auth_url = "https://auth.openai.com/oauth/authorize"
authorize_url = "https://auth.openai.com/oauth/authorize"
token_url = "https://auth.openai.com/oauth/token"
browser_redirect_uri = "http://localhost:1455/auth/callback"
device_usercode_url = "https://auth.openai.com/api/accounts/deviceauth/usercode"
device_token_url = "https://auth.openai.com/api/accounts/deviceauth/token"
device_verification_url = "https://auth.openai.com/codex/device"
device_redirect_uri = "https://auth.openai.com/deviceauth/callback"

[oauth.gemini]
client_id = "<GEMINI_CLI_CLIENT_ID>"
client_secret = "<GEMINI_CLI_CLIENT_SECRET>"
auth_url = "https://accounts.google.com/o/oauth2/v2/auth"
token_url = "https://oauth2.googleapis.com/token"
redirect_uri = "https://codeassist.google.com/authcode"
code_assist_url = "https://cloudcode-pa.googleapis.com/v1internal"
userinfo_url = "https://www.googleapis.com/oauth2/v3/userinfo"

[oauth.antigravity]
client_id = "<ANTIGRAVITY_CLIENT_ID>"
client_secret = "<ANTIGRAVITY_CLIENT_SECRET>"
```

`auth_url` and `device_redirect_uri` serve the device-code flow. The browser fallback uses `authorize_url` and `browser_redirect_uri` so the operator can paste the localhost callback address from a remote browser.

### Gemini OAuth profiles

Gemini OAuth credentials retain the profile that issued them. Existing Gemini CLI enterprise grants use `gemini_cli`; when `oauth_profile` is absent from an imported or stored bundle, it defaults to `gemini_cli`. New Gemini subscription sign-ins and reconnects use `antigravity`.

The Antigravity profile is configured under `[oauth.antigravity]`. The operator MUST provide both `client_id` and `client_secret`; there is no built-in Antigravity client credential. Starting a Gemini subscription sign-in or refreshing an existing Antigravity credential returns `oauth_client_not_configured` until both keys are set:

| Key | Default |
| --- | --- |
| `client_id` | **Required; no default** |
| `client_secret` | **Required; no default** |
| `authorize_url` | `https://accounts.google.com/o/oauth2/v2/auth` |
| `token_url` | `https://oauth2.googleapis.com/token` |
| `userinfo_url` | `https://www.googleapis.com/oauth2/v2/userinfo?alt=json` |
| `redirect_uri` | `http://localhost:51121/oauth-callback` |
| `scopes` | `https://www.googleapis.com/auth/cloud-platform`, `https://www.googleapis.com/auth/userinfo.email`, `https://www.googleapis.com/auth/userinfo.profile`, `https://www.googleapis.com/auth/cclog`, `https://www.googleapis.com/auth/experimentsandconfigs` |
| `code_assist_url` | `https://daily-cloudcode-pa.googleapis.com` |
| `user_agent` | `antigravity/hub/2.24.0 (aidev_client; os_type=darwin; arch=arm64; cl=1000335682)` |

The existing `[oauth.gemini]` section remains the `gemini_cli` profile. Keep client secrets out of source control and logs. Antigravity quota polling uses Code Assist `retrieveUserQuotaSummary`, with the same parser and fallback behavior as Gemini CLI sign-ins. It reports the same four windows: Gemini models have 5-hour (`gemini_5h`) and weekly (`gemini_7d`) windows; Claude and GPT-OSS models have 5-hour (`third_party_5h`) and weekly (`third_party_7d`) windows.

## Warm-up models

The optional `[warmup]` section selects the minimal request models used for OpenAI and Gemini OAuth warm-up. Anthropic uses its fixed Haiku warm-up model.

```toml
[warmup]
openai_model = "gpt-6.1-sol"
gemini_model = "gemini-2.5-flash-lite"
```

`openai_model` defaults to `gpt-6.1-sol`. `gemini_model` defaults to `gemini-2.5-flash-lite`. Gemini warm-up models must belong to the Gemini models quota group.

## Cache keepalive

Cache keepalive is configured per principal. The principal object controls whether the feature is enabled, its lead times, maximum refresh count, maximum duration, snapshot size, and classifier options.

For GPT-5.6+ Responses, enable both `cache_keepalive.enabled` and `cache_keepalive.classifier.openai_responses_enabled`. The documented `prompt_cache_options.ttl` is `"30m"` by default or `"12h"`; a supported input breakpoint may override that lifetime. Only eligible 30-minute prefixes with sufficient native cached-token evidence and refresh budget schedule renewals. A selected 12-hour prefix records a not-tracked decision because reads do not restart its clock. Legacy `prompt_cache_retention` is a separate policy. See the [keep-alive operator guide](https://github.com/isac322/cc-lb/blob/master/docs/cache-keepalive-operator.md) for limits and renewal costs.

## Scheduler storage

Use SQLite for local or bounded single-node operation. Use Postgres when multiple replicas must share durable job claims, idempotency keys, and cache-keepalive fences.

After changing scheduler or affinity retention settings, restart the server so all workers use the same startup policy.

## PostgreSQL TLS

Storage and scheduler connections use `storage.url`, but resolve TLS mode independently. Storage connections, including startup preflight, use `storage.pool.sslmode`. The scheduler uses `scheduler.separate_pool.sslmode`; it does not inherit `storage.pool.sslmode`.

For each pool, TLS mode follows this precedence:

1. An explicit pool `sslmode` overrides the URL mode.
2. If the pool setting is omitted, the URL's `sslmode` or `ssl-mode` query parameter applies.
3. If neither specifies a mode, cc-lb uses `prefer`, even when `PGSSLMODE` is set.

Omit each pool's `sslmode` to inherit the URL policy. An explicit `prefer` or `disable` intentionally overrides even a strict URL mode such as `require`, `verify-ca`, or `verify-full`; remove a redundant `prefer` when the URL should control TLS.

After upgrading, a strict URL mode takes effect wherever the pool setting is omitted. Connections that previously relied on the implicit `prefer` fallback may fail until the database and client TLS configuration satisfy that policy.

## Upstream affinity

Learned upstream affinity bindings are retained for 7 days from their last successful binding by default. Ordinary replay reads do not refresh the timestamp, so bindings older than `ttl_days` fail closed with `503` and are purged.

```toml
[upstream_affinity]
ttl_days = 7
```

## Request diagnostics

Request events store all 99 operational, latency-breakdown, and abnormal-stream-error scalar fields in typed relational columns. The `payload` column is dropped, so event reads have zero TOAST overhead. The deep diagnostic fields are `cache_breakpoints` (with `lookback_prefixes` omitted from diagnostics lines), `cache_control_message_indices`, `routing_trace`, `iterations`, and `internal_errors`, plus the optional `native_cache` object, which is omitted when no native cache analysis is recorded.

The optional startup setting below emits one `cc_lb.request_diagnostics.v1` JSON line to stdout after each stored request event commits. The line also carries `thread_id`, `observed_session_id`, `input_tokens`, `output_tokens`, `cache_read_input_tokens`, `cache_creation_input_tokens`, `duration_ms`, and `upstream_ttfb_ms`. The optional `cache_write_input_tokens`, `cache_write_input_tokens_30m`, and `cache_write_input_tokens_12h` integers are omitted when unreported or when the write component is invalid; a valid reported `0` is kept.

| Key | Default | Environment variable | Effect |
| --- | --- | --- | --- |
| `observability.request_diagnostics_log` | `false` | `CC_LB_OBSERVABILITY__REQUEST_DIAGNOSTICS_LOG` | Writes one diagnostics JSON line to stdout for each stored request event. |

```toml
[observability]
request_diagnostics_log = true
```

The setting is fixed at startup. `observability.log_redaction` and `observability.user_prompt_redaction` do not apply to diagnostics lines; producers redact internal error text and cap routing traces before persistence. See the [observability guide](/docs/operations/observability/#request-diagnostics-log) and the [request diagnostics runbook](https://github.com/isac322/cc-lb/blob/master/docs/runbook/request-diagnostics.md) for delivery and reconciliation boundaries.
