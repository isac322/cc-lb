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

## Cache keepalive

Cache keepalive is configured per principal. The principal object controls whether the feature is enabled, its lead times, maximum refresh count, maximum duration, snapshot size, and classifier options.

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

Request events store all 99 operational, latency-breakdown, and abnormal-stream-error scalar fields in typed relational columns. The `payload` column is dropped, so event reads have zero TOAST overhead. The five deep diagnostic fields are `cache_breakpoints` (with `lookback_prefixes` omitted from diagnostics lines), `cache_control_message_indices`, `routing_trace`, `iterations`, and `internal_errors`.

The optional startup setting below emits one `cc_lb.request_diagnostics.v1` JSON line to stdout after each stored request event commits. The line also carries `thread_id`, `observed_session_id`, `input_tokens`, `output_tokens`, `cache_read_input_tokens`, `cache_creation_input_tokens`, `duration_ms`, and `upstream_ttfb_ms`.

| Key | Default | Environment variable | Effect |
| --- | --- | --- | --- |
| `observability.request_diagnostics_log` | `false` | `CC_LB_OBSERVABILITY__REQUEST_DIAGNOSTICS_LOG` | Writes one diagnostics JSON line to stdout for each stored request event. |

```toml
[observability]
request_diagnostics_log = true
```

The setting is fixed at startup. `observability.log_redaction` and `observability.user_prompt_redaction` do not apply to diagnostics lines; producers redact internal error text and cap routing traces before persistence. See the [observability guide](/docs/operations/observability/#request-diagnostics-log) and the [request diagnostics runbook](https://github.com/isac322/cc-lb/blob/master/docs/runbook/request-diagnostics.md) for delivery and reconciliation boundaries.
