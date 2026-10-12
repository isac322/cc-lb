---
title: "Runtime management source notes"
description: "Curated sections from the database-backed runtime management reference."
slug: docs/reference/source/runtime-management
---

## Overview

This system transitions cc-lb from static TOML configuration files to a dynamic, database-backed runtime management model. You no longer define upstreams, principals, or plugin chains in `cc-lb.toml`. Instead, all runtime configurations are stored in the database and managed dynamically. This change allows you to add, update, or delete resources on the fly without restarting the proxy. You can perform these operations through the web dashboard or directly via the admin v1 REST API.

## Architecture

The runtime architecture relies on a lock-free `DynamicView` snapshot managed via an `ArcSwap` primitive. When configuration changes occur in the database, the server builds a new immutable `DynamicView` snapshot and swaps it atomically. This design ensures that active request-handling threads never block on configuration updates.

In multi-replica deployments, replicas synchronize their local views using Postgres `LISTEN/NOTIFY` channels for sub-second updates. If a notification is missed, a background reconciler polls the database every 60 seconds as a fallback. The system uses a single rebind primitive to safely commit staged plugin slots and rebuild the routing table.

## Admin v1 REST API

Administrative operations use the providers configured in `admin.auth.providers`. A static-token provider accepts its environment-backed Bearer token in the `Authorization` header.

The default admin listener is `[::1]:9090`; use `http://[::1]:9090` for the examples below. Proxy traffic uses a separate listener on port 8080 (`http://localhost:8080/v1/messages`).

The [Admin consistency contract](https://github.com/isac322/cc-lb/blob/master/docs/consistency-contract.md) lists what each admin response guarantees about persistence, audit records, the routing-view rebuild, and convergence across replicas.

### Local proxy key files

If an operator keeps a local proxy client key file such as a local file, treat it as a client-side cache of the plaintext key returned once by `POST /admin/v1/principals/{id}/keys`. Rotating a managed key is a two-step lifecycle: issue the replacement key, update the local key file atomically with mode `0600`, verify the proxy request path with the new key, then revoke the old key through the admin API. The server never rewrites operator key files automatically.

### Upstreams API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| POST | `/admin/v1/upstreams` | Bearer | UpstreamCreateBody | UpstreamResponse | `conflict`, `invalid_location`, `invalid_input`, `missing_api_key`, remediation codes |
| GET | `/admin/v1/upstreams` | Bearer | None | List of UpstreamResponse | None |
| GET | `/admin/v1/upstreams/{id}` | Bearer | None | UpstreamResponse | `upstream_not_found` |
| PUT | `/admin/v1/upstreams/{id}` | Bearer | UpstreamUpdateBody | UpstreamResponse | `stale_revision`, `upstream_not_found`, `invalid_input`, remediation codes |
| DELETE | `/admin/v1/upstreams/{id}` | Bearer | None | None | `referenced_by`, `stale_revision` |
| POST | `/admin/v1/upstreams/{id}/enable` | Bearer | None | UpstreamResponse | `stale_revision` |
| POST | `/admin/v1/upstreams/{id}/disable` | Bearer | None | UpstreamResponse | `stale_revision` |
| POST | `/admin/v1/upstreams/preflight` | Bearer | PreflightRequest | PreflightResponse | `missing_api_key`, `invalid_input` |
| POST | `/admin/v1/upstreams/{id}/preflight` | Bearer | None | PreflightResponse | `not_api_key_upstream`, `upstream_not_found`, `api_key_credential_decrypt_failed` |
| GET | `/admin/v1/upstreams/{id}/workspaces` | Bearer | None | `{"workspaces":[{"id","name"}]}` | `not_api_key_upstream`, `non_anthropic_base_url`, `workspace_list_denied`, `probe_unreachable`, `api_key_credential_decrypt_failed` |
| GET | `/admin/v1/upstreams/{id}/usage?range=&step=` | Bearer | None | UpstreamKeyUsageResponse | `not_api_key_upstream`, `upstream_not_found`, invalid `range`/`step` |

#### UpstreamCreateBody

```json
{
  "name": "upstream-name",
  "kind": "anthropic_api_key",
  "base_url": "https://api.anthropic.com",
  "api_key_value": "sk-ant-..."
}
```

`api_key_value` carries the literal API key. The API encrypts the credential for database storage and never returns the key; responses carry a masked `key_hint` instead. Keep literal credentials out of saved commands, shell history, and source control.

`anthropic_api_key` creates also accept:

- `workspace_id`: the `wrkspc_…` workspace an org-scoped key acts in. cc-lb sends it as `anthropic-workspace-id` on every proxied request for the upstream. It must be `wrkspc_` followed by 1–120 letters or digits. A key bound to one workspace rejects it with `400 invalid_input` (`field: "workspace_id"`).
- `skip_preflight`: `true` stores the key without contacting the provider. The upstream is saved unverified and reports `usage_never_checked` until it is re-checked.

Create normalizes the key, applies the local prefix checks, and runs preflight before anything is stored (see [API-key upstreams](#api-key-upstreams)). A rejected key answers with a nested remediation body, `{"error":{"code":"key_admin","message":"…"}}`, and stores nothing. A key that passes answers `201` with `api_key_access` and `key_hint`.

#### UpstreamUpdateBody

```json
{
  "base_url": "https://api.anthropic.com",
  "api_key_value": "sk-ant-...",
  "workspace_id": "wrkspc_...",
  "enabled": true
}
```

Every field is optional, and `PUT`/`PATCH` require `If-Match`. A missing or `null` `base_url` keeps the override and `clear_base_url: true` removes it; sending both is `400 conflicting_base_url`. `workspace_id` works the same way with `clear_workspace_id: true`, except that sending both is `400 invalid_input` with `field: "workspace_id"`. `skip_preflight` applies as on create.

On an API-key upstream, changing the key, workspace, or `base_url` re-runs preflight before the write. A `base_url`-only change never fails on the probe result; the stored access snapshot reflects the new host. The key does not have to be re-entered: an admin who can edit an upstream can direct its stored key to any host they set as `base_url`. The change is audited with the old and new hosts, never the key, and the key is re-probed at the new host. Successful create and update responses for an API-key upstream carry `Cache-Control: no-store`, like preflight.

Any upstream `base_url` must use `http` or `https` and must not contain userinfo; other values are rejected on write.

#### UpstreamResponse

```json
{
  "id": "<UPSTREAM_ID>",
  "name": "upstream-name",
  "kind": "anthropic_api_key",
  "enabled": true,
  "warmup_enabled": false,
  "warmup_dialect_plugin": null,
  "spec_revision": 1,
  "key_hint": "sk-ant-api03-...wxyz",
  "api_key_access": {
    "verified": true,
    "checked_at_unix_secs": 1760000000,
    "key_class": "org_scoped",
    "inference": "ok",
    "workspace": { "mode": "selected", "id": "wrkspc_..." },
    "workspace_list": "ok",
    "usage_report": "ok",
    "key_type": null,
    "last_error": null
  },
  "status": {
    "last_apply_error": null,
    "last_apply_at_unix_secs": null,
    "last_warmup_at_unix_secs": null
  }
}
```

`spec_revision` is the user-managed optimistic-lock counter used by `If-Match` and `ETag`. Background controller writes (status/lease/secret/token) never bump it. The nested `status` object holds system-managed operational state: only the apply daemon and warmup observer write here. `warmup_enabled` defaults to `true` for `anthropic_oauth` upstreams and `false` for other kinds; see [docs/upstream-warmup.md](https://github.com/isac322/cc-lb/blob/master/docs/upstream-warmup.md).

`key_hint` and `api_key_access` appear only on `anthropic_api_key` upstreams that have been checked. `key_hint` is the key's `sk-ant-<family>-` prefix, plus `...` and the last four characters when the key is at least 20 characters long. `api_key_access` is the last stored capability snapshot. `verified` is `false` until a probe succeeds. `key_class` is `workspace_bound`, `org_scoped`, or `unknown`. `workspace.mode` is `bound` when a check found the key works without a workspace header (`id` is set when Anthropic named the workspace), `selected` when cc-lb sends a chosen workspace `id`, and `required` with no `id` otherwise: the key was never checked, was rejected locally or by the endpoint, was unreachable, or still needs a workspace. `workspace_list` and `usage_report` are `ok`, `denied`, `error`, or `not_applicable`. `key_type` is `personal`, `service_account`, `workspace`, or `null` when Anthropic did not say.

#### API-key upstreams

cc-lb serves standard Console API keys: workspace keys (`sk-ant-api03-…`) and identity-linked personal or service-account keys (`sk-ant-usr-…`), which act in every workspace and need a `workspace_id`. Before any network call, it trims outer whitespace and rejects a key with inner spaces, line breaks, or non-ASCII characters, or one longer than 512 bytes, with `400 invalid_input` (`field: "api_key_value"`). It then rejects three key families locally, because they cannot call models:

| Prefix | Class | Code |
|---|---|---|
| `sk-ant-admin…` | Admin key | `key_admin` |
| `sk-ant-api01…` | Enterprise management key | `key_enterprise_scoped` |
| `sk-ant-oat01…` | OAuth access token; use an `anthropic_oauth` upstream instead | `key_oat` |

Other prefixes, including unknown ones, go to the probes. Preflight always answers `200` with `Cache-Control: no-store`. The `verdict` is `ok`, `needs_workspace`, `rejected`, or `unreachable`. The response also carries `key_hint`, `classification`, `workspaces`, `capabilities`, `remediation[]`, and `checked_at`. The candidate route (`POST /admin/v1/upstreams/preflight`) stores nothing. The re-check route (`POST /admin/v1/upstreams/{id}/preflight`) probes the stored key and persists the new snapshot without bumping `spec_revision`. `GET /admin/v1/upstreams/preflight` returns `405`.

The inference probe goes to the configured `base_url`, the same host proxied traffic uses: `GET /v1/models`, then `POST /v1/messages/count_tokens`. A `2xx` response that carries `anthropic-workspace-id` marks the key `workspace_bound`. A key whose account can act in several workspaces gets a `400` saying the request must include the `anthropic-workspace-id` header, so preflight reports `needs_workspace` with `workspace_required`. When a supplied workspace id is rejected by Anthropic (`400` "must be a valid workspace ID" or `404`), preflight reports `workspace_not_found`. A `2xx` with neither signal is classified `unknown` and accepted.

The org Admin API probes (workspace list, usage report, and per-key attribution) run only for an `org_scoped` key whose `base_url` is exactly `https://api.anthropic.com`: same scheme, host, and port, a root path, and no userinfo, query, or fragment. Any other host reports these capabilities as `not_applicable` with `non_anthropic_base_url`, and usage stays estimated. Probes never follow redirects, have bounded time and body size, and scrub the key from every stored or returned provider message.

The response's `remediation[].code`, the nested failure `error.code`, and `/usage` `fix.code` use one vocabulary:

| Code | Status | Meaning | Fix |
|---|---|---|---|
| `key_admin` | 400 | Admin key; it manages the org and can't call models | Use a standard Console API key |
| `key_enterprise_scoped` | 400 | Enterprise management key; no inference | Use a Console API key |
| `key_oat` | 400 | OAuth token; it expires and cc-lb cannot refresh it | Create an `anthropic_oauth` upstream |
| `key_invalid` | 400 | The configured endpoint answered `401` | Paste the key again or create a new one |
| `key_forbidden` | 400 | Valid key, but `count_tokens` answered `403` | Save it unverified with `skip_preflight` |
| `workspace_required` | 400 | The key acts in several workspaces | Send `workspace_id` |
| `workspace_not_found` | 400 | The key can't see that workspace | Check the id or the account's access |
| `workspace_id_malformed` | 400 | The id does not start with `wrkspc_` | Send a `wrkspc_…` id |
| `workspace_list_denied` | 403 | The key's account lacks the role to list workspaces | Copy the `wrkspc_…` id from Console |
| `non_anthropic_base_url` | 403 | `base_url` is not `https://api.anthropic.com`, so org probes do not run | Use the default endpoint for reported usage |
| `probe_unreachable` | 502 (504 on timeout) | The endpoint did not answer, or answered with something cc-lb can't classify: a `5xx`, a redirect (not followed), another `4xx`, or an oversized body | Try again, or save with `skip_preflight` |
| `key_workspace_scoped_usage_unavailable` | – | A bound key cannot read usage reports | Replace it with an All workspaces key |
| `role_insufficient` | – | The linked account's org role can't read usage reports | Raise the account's org role |
| `key_id_unresolved` | – | Usage can't be attributed to this key | Figures cover the whole workspace |
| `usage_never_checked` | – | Saved without a check, or created before checks existed | Re-check the key |
| `usage_poll_failed` | – | Usage reports stopped arriving | Fix the role or key; the poller retries |

Codes with a status are failure responses on create, update, and the workspace list. The rest appear only as data. Re-check, the workspace list, and any update that re-probes the stored key answer `500 {"error":"api_key_credential_decrypt_failed"}` when the stored credential cannot be decrypted; send a new `api_key_value` to replace it.

`GET /admin/v1/upstreams/{id}/usage` reads storage only and makes no outbound call. It accepts the dashboard `range` (`15m`, `1h`, `6h`, `24h`, `7d`) and `step` (`minute`, `hour`) parameters. Anthropic's reports are hourly, so a `real` response to `step=minute` returns hourly buckets and echoes `"step":"hour"`; an `estimated` response keeps the requested step. `source` is `real` when the series comes from Anthropic's usage report and `estimated` when it comes from cc-lb's own request log. `attribution` is `api_key`, or `workspace` when Anthropic can't attribute usage to the key; `attribution_workspace_id` names the workspace a `workspace` attribution sums and is `null` otherwise. `stale` is `true` while the last report is kept after polling failed. `last_reported_unix_secs` is when the poller last received a usable report, independent of `range`, or `null` when no poll has succeeded. `totals.cost_usd_micros` is always an estimate at list price (`cost_is_estimate: true`), because Anthropic reports tokens per key, not cost. `fix` carries the single remediation code that explains a degraded mode, or `null`.

Reported usage comes from the `api_key_usage_poll` scheduler job, which polls each eligible upstream every 300 seconds by default; see [docs/scheduler.md](https://github.com/isac322/cc-lb/blob/master/docs/scheduler.md#api-key-usage-polling).

`month` covers the current UTC calendar month. Its `billing_stop` is classified from the upstream's latest finished proxied request: `credits_exhausted` (400 credit balance), `spend_limit` (400 self-set usage limit), `tier_spend_cap` (429 tier monthly cap), or `billing_error` (402). Any other outcome, including a later success, clears it. Anthropic has no API for the credit balance, so cc-lb never shows one; check the Console Billing page.

The proxy strips any client-supplied `anthropic-workspace-id` header from every upstream request. API-key upstreams send their configured workspace instead.

### Principals API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| POST | `/admin/v1/principals` | Bearer | CreatePrincipalBody | PrincipalResponse | `conflict`, `unsupported_cache_keepalive_llm_judge` |
| GET | `/admin/v1/principals` | Bearer | None | List of PrincipalResponse | None |
| GET | `/admin/v1/principals/{id}` | Bearer | None | PrincipalResponse | `unknown_principal` |
| PUT | `/admin/v1/principals/{id}` | Bearer | UpdatePrincipalBody | PrincipalResponse | `stale_revision`, `unsupported_cache_keepalive_llm_judge` |
| DELETE | `/admin/v1/principals/{id}` | Bearer | None | None | `stale_revision` |
| POST | `/admin/v1/principals/{id}/enable` | Bearer | None | PrincipalResponse | `stale_revision` |
| POST | `/admin/v1/principals/{id}/disable` | Bearer | None | PrincipalResponse | `stale_revision` |
| PUT | `/admin/v1/principals/{id}/allowed_models` | Bearer | AllowedModelsBody | PrincipalResponse | `stale_revision` |

#### CreatePrincipalBody

```json
{
  "name": "principal-name",
  "kind": "machine",
  "allowed_models": ["claude-sonnet-4-5-20250929"],
  "default_limits": [],
  "cache_keepalive": {
    "enabled": true,
    "refresh_lead_time_5m_secs": 30,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 12,
    "max_total_duration_secs": 14400,
    "snapshot_max_bytes": 524288,
    "classifier": {
      "extra_wait_for_user_tools": [],
      "treat_end_turn_as_ambiguous": false
    }
  }
}
```

`cache_keepalive` is an optional object. Omit it (or pass `null`) to
disable the prompt-cache keep-alive feature for this principal. When
present, `enabled` is required. Full field semantics, defaults, and
provider-gating rules are documented in
[docs/cache-keepalive-operator.md](https://github.com/isac322/cc-lb/blob/master/docs/cache-keepalive-operator.md).
`cache_keepalive.classifier.llm_judge` is reserved for a future release;
create/update requests that set it return `unsupported_cache_keepalive_llm_judge`.

#### PrincipalResponse

```json
{
  "id": "<PRINCIPAL_ID>",
  "name": "principal-name",
  "kind": "machine",
  "enabled": true,
  "revision": 1,
  "allowed_models": ["claude-sonnet-4-5-20250929"],
  "default_limits": [],
  "cache_keepalive": null
}
```

The `cache_keepalive` field mirrors whatever was persisted on the
principal, or `null` when unset. `PATCH /admin/v1/principals/{id}` with
`"cache_keepalive": null` clears the config.

Each principal receives a built-in `subscription-preference` Router chain
entry at creation, with order `0`. Use the plugin-chain endpoints below to
remove the entry or change its position among other router plugins.

### Plugins API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| GET | `/admin/v1/plugins/registry` | Bearer | None | RegistryListResponse | None |
| GET | `/admin/v1/plugins/registry/{id}` | Bearer | None | RegistryEntryResponse | `unknown_registry_entry` |
| GET | `/admin/v1/plugins/registry/{id}/references` | Bearer | None | RegistryReferencesResponse | `unknown_registry_entry` |
| PATCH | `/admin/v1/plugins/registry/{id}` | Bearer | PatchRegistryBody | RegistryEntryResponse | `stale_revision` |
| DELETE | `/admin/v1/plugins/registry/{id}` | Bearer + `If-Match` | None | None | `referenced_by`, `stale_revision` |
| DELETE | `/admin/v1/plugins/registry/{id}?cascade=references` | Bearer + `If-Match` + `X-Reference-Fingerprint` | None | RegistryCascadeDeleteResponse | `reference_fingerprint_required`, `references_changed`, `stale_revision` |
| GET | `/admin/v1/principals/{principal_id}/plugin-chain` | Bearer | None | ChainListResponse | None |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain` | Bearer | InsertChainBody | PluginChainEntry | `conflict` |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain/reorder` | Bearer | ReorderBody | None | `stale_revision` |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain/rebalance` | Bearer | None | None | None |
| PUT | `/admin/v1/plugin-chain-entries/{id}` | Bearer | PluginChainEntryUpdate | PluginChainEntry | `stale_revision` |
| DELETE | `/admin/v1/plugin-chain-entries/{id}` | Bearer | None | None | `stale_revision` |

#### InsertChainBody

```json
{
  "slot": "router",
  "wasm_registry_id": "<PLUGIN_ID>",
  "config": {},
  "position": null
}
```

`RegistryEntryResponse` includes the inspected plugin metadata name, nullable `version`, SHA-256, supported slots, revision, and a live `refcount`. The `refcount` is computed from both principal plugin-chain bindings and upstream warmup dialect plugin bindings.

`GET /admin/v1/plugins/registry/{id}/references` returns a `reference_fingerprint` plus the concrete chain and warmup resources that currently reference the plugin. Use that fingerprint when intentionally deleting a referenced plugin with `?cascade=references`; stale fingerprints are rejected with `409 references_changed` so operators see a fresh impact list before removing bindings.

### Status and Export API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| GET | `/admin/v1/status` | Bearer | None | StatusResponse | `storage_unavailable` |
| GET | `/admin/v1/export` | Bearer | None | ExportResponse | `storage_unavailable` |

The proxy listener's `/readyz` returns `200` once the server has started, at least one principal is active, and at least one upstream is usable: enabled, applied, and, for OAuth, holding decryptable credentials that do not need a reconnect. Otherwise it returns `503` with `reason` set to `draining`, `startup_not_ready`, `no_ready_principal`, or `no_ready_upstream`. A broken OAuth upstream does not take a ready instance out of rotation: routing skips it, and `/readyz` stays `200` with `reason: "oauth_credentials_not_decryptable"` while any enabled OAuth upstream has credentials that cannot be decrypted. API-key preflight results, capability snapshots, and usage-poll failures are not part of readiness.
