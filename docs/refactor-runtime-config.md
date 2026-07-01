# cc-lb runtime config refactor — field classification

Status: spec for the migration from TOML config file to dashboard-managed runtime config.

## Goal

Stop reading `cc-lb.toml`. Receive only boot-critical values via env vars. Every other knob is stored in the DB, edited from the dashboard, and hot-reloaded in process (no file write).

## Classification rules

A field is **env-only** when *all three* hold:
1. It must be known **before** storage can be opened, **or**
2. It cannot be hot-reloaded without re-binding kernel/socket resources, **or**
3. It names a secret env var (the *name*, not the secret itself).

Everything else is **runtime-managed**: stored in `effective_config_v1`, edited via the dashboard, hot-reloaded by broadcast. A subset are flagged **RR** (restart-required) when the change can be persisted but the running process cannot pick it up without a restart — the dashboard warns instead of silently failing.

## Field table

| Field | Bucket | Env var (env-only) / Notes |
| --- | --- | --- |
| `listener.proxy_addr` | env | `CC_LB_LISTENER__PROXY_ADDR` |
| `listener.admin_addr` | env | `CC_LB_LISTENER__ADMIN_ADDR` |
| `listener.metrics_addr` | env | `CC_LB_LISTENER__METRICS_ADDR` |
| `listener.unix_socket` | env | `CC_LB_LISTENER__UNIX_SOCKET` (optional) |
| `tls.cert_path` | env | `CC_LB_TLS__CERT_PATH` (optional) |
| `tls.key_path` | env | `CC_LB_TLS__KEY_PATH` |
| `tls.reload_on_sighup` | env | `CC_LB_TLS__RELOAD_ON_SIGHUP` (default `true`) |
| `storage.kind` | env | `CC_LB_STORAGE__KIND` (`sqlite` \| `postgres`) |
| `storage.sqlite.path` | env | `CC_LB_STORAGE__PATH` |
| `storage.postgres.url` | env | `CC_LB_STORAGE__URL` |
| `storage.postgres.pool.*` | env | `CC_LB_STORAGE__POOL__*` (5 sub-fields) |
| `aead.key_env` | env | `CC_LB_AEAD__KEY_ENV` (default `CC_LB_MASTER_KEY`) |
| `admin.token_env` | env | `CC_LB_ADMIN__TOKEN_ENV` (default `CC_LB_ADMIN_TOKEN`) |
| `runtime.data_dir` | env | `CC_LB_DATA_DIR` |
| `body.messages_cap_bytes` | runtime | hot |
| `body.files_cap_bytes` | runtime | hot |
| `body.per_route_overrides` | runtime | hot |
| `timeouts.*` (5) | runtime | hot |
| `downstream_auth.mode` | runtime | hot |
| `downstream_auth.none_mode.*` | runtime | hot |
| `api_keys.usage_retention_days` | runtime | hot |
| `api_keys.price_catalog.url` | runtime | hot |
| `api_keys.price_catalog.refresh_interval` | runtime | hot |
| `api_keys.price_catalog.cache_path` | runtime | **RR** |
| `scheduler.separate_pool.*` | runtime | **RR** (DB pool reconnect) |
| `scheduler.leader_lock_key` | runtime | **RR** |
| `scheduler.retry_classes.*` | runtime | hot |
| `scheduler.recurring_jobs.*` | runtime | hot |
| `scheduler.idempotency.*` | runtime | hot |
| `scheduler.dlq_retention_days` | runtime | hot |
| `scheduler.entity_concurrency` | runtime | **RR** |
| `scheduler.singleton_concurrency` | runtime | **RR** |
| `scheduler.staleness.*` | runtime | hot |
| `scheduler.pgbouncer_transaction_mode` | runtime | **RR** |
| `observability.tracing_level` | runtime | hot (reload_handle) |
| `observability.otlp_endpoint` | runtime | **RR** |
| `observability.prometheus_endpoint` | runtime | **RR** |
| `observability.log_redaction` | runtime | hot |
| `observability.user_prompt_redaction` | runtime | hot |
| `oauth.anthropic.*` (5) | runtime | hot |
| `subscription_quota.*` | runtime | hot |
| `runtime.startup_handshake.skip_if_fresh` | runtime | **RR** (boot-only) |
| `runtime.startup_handshake.force` | runtime | **RR** (boot-only) |
| `circuit_breaker.*` | runtime | hot |
| `bulkhead.*` | runtime | hot |
| `dns.*` | runtime | hot |
| `egress.*` | runtime | hot |
| `prompt_cache_shadow.*` | runtime | hot |

## Boot env contract

Required:
- `CC_LB_LISTENER__PROXY_ADDR`
- `CC_LB_LISTENER__ADMIN_ADDR`
- `CC_LB_LISTENER__METRICS_ADDR`
- `CC_LB_STORAGE__KIND`
- One of `CC_LB_STORAGE__PATH` (sqlite) or `CC_LB_STORAGE__URL` (postgres)
- `CC_LB_DATA_DIR`
- The env var named by `CC_LB_AEAD__KEY_ENV` (default `CC_LB_MASTER_KEY`) must hold a 64-char hex string.
- The env var named by `CC_LB_ADMIN__TOKEN_ENV` (default `CC_LB_ADMIN_TOKEN`) must hold the admin bearer token.

Optional (with defaults):
- `CC_LB_LISTENER__UNIX_SOCKET` — none
- `CC_LB_TLS__CERT_PATH`, `CC_LB_TLS__KEY_PATH` — none (plain HTTP)
- `CC_LB_TLS__RELOAD_ON_SIGHUP` — `true`
- `CC_LB_STORAGE__POOL__*` — sane Postgres defaults

## Migration semantics

- First boot (no `effective_config_v1` row): seed `RuntimeConfig::defaults()` at revision 0.
- Apply pipeline: draft → validate → `put_effective_config(rev, json, ts)` → `append_config_history(...)` → `ConfigReloader::reload_now()` (storage-backed).
- No filesystem write. No `cc-lb.toml`.
- `--config` CLI flag removed. `cc-lb config validate` subcommand removed.

## RR semantics

Dashboard apply succeeds for **RR** fields, but the running process refuses to swap them in. The Admin page surfaces the diff via `restart_required_changes()` so operators know to bounce the deployment.
