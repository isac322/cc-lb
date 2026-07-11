# Runtime Management

This document describes the architecture, API, and operations of the database-backed runtime management system in cc-lb.

## Overview

This system transitions cc-lb from static TOML configuration files to a dynamic, database-backed runtime management model. You no longer define upstreams, principals, or plugin chains in `cc-lb.toml`. Instead, all runtime configurations are stored in the database and managed dynamically. This change allows you to add, update, or delete resources on the fly without restarting the proxy. You can perform these operations through the web dashboard or directly via the admin v1 REST API.

## Architecture

The runtime architecture relies on a lock-free `DynamicView` snapshot managed via an `ArcSwap` primitive. When configuration changes occur in the database, the server builds a new immutable `DynamicView` snapshot and swaps it atomically. This design ensures that active request-handling threads never block on configuration updates.

In multi-replica deployments, replicas synchronize their local views using Postgres `LISTEN/NOTIFY` channels for sub-second updates. If a notification is missed, a background reconciler polls the database every 60 seconds as a fallback. The system uses a single rebind primitive to safely commit staged plugin slots and rebuild the routing table.

## Bootstrap

When starting a fresh deployment, you can bootstrap the initial admin principal and seed resources. The server checks the `CC_LB_BOOTSTRAP_ADMIN_TOKEN` environment variable on startup. If present, it automatically seeds the admin principal with this token.

You can also place a `bootstrap.toml` file in your data directory to seed initial upstreams, principals, and plugin chains. The server processes this file once on startup, applies the resources to the database, and renames the file to `bootstrap.toml.consumed-<timestamp>` to prevent re-processing.

An example `bootstrap.toml` file:

```toml
[[ upstreams ]]
name = "primary-api"
kind = "anthropic_api_key"
api_key_env = "ANTHROPIC_API_KEY"

[[ principals ]]
name = "default-user"
kind = "user"
```

## Admin v1 REST API

All administrative operations are authenticated via a Bearer token in the `Authorization` header.

### Local proxy key files

If an operator keeps a local proxy client key file such as `~/.config/cc-lb/proxy-key`, treat it as a client-side cache of the plaintext key returned once by `POST /admin/v1/principals/{id}/keys`. Rotating a managed key is a two-step lifecycle: issue the replacement key, update the local key file atomically with mode `0600`, verify the proxy request path with the new key, then revoke the old key through the admin API. The server never rewrites operator key files automatically.

### Upstreams API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| POST | `/admin/v1/upstreams` | Bearer | UpstreamCreateBody | UpstreamResponse | `conflict`, `invalid_location` |
| GET | `/admin/v1/upstreams` | Bearer | None | List of UpstreamResponse | None |
| GET | `/admin/v1/upstreams/{id}` | Bearer | None | UpstreamResponse | `upstream_not_found` |
| PUT | `/admin/v1/upstreams/{id}` | Bearer | UpstreamUpdateBody | UpstreamResponse | `stale_revision`, `upstream_not_found` |
| DELETE | `/admin/v1/upstreams/{id}` | Bearer | None | None | `referenced_by`, `stale_revision` |
| POST | `/admin/v1/upstreams/{id}/enable` | Bearer | None | UpstreamResponse | `stale_revision` |
| POST | `/admin/v1/upstreams/{id}/disable` | Bearer | None | UpstreamResponse | `stale_revision` |

#### UpstreamCreateBody

```json
{
  "name": "upstream-name",
  "kind": "anthropic_api_key",
  "base_url": "https://api.anthropic.com",
  "api_key_env": "ANTHROPIC_API_KEY"
}
```

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
  "status": {
    "last_apply_error": null,
    "last_apply_at_unix_secs": null,
    "last_warmup_at_unix_secs": null
  }
}
```

`spec_revision` is the user-managed optimistic-lock counter used by `If-Match` and `ETag`. Background controller writes (status/lease/secret/token) never bump it. The nested `status` object holds system-managed operational state: only the apply daemon and warmup observer write here.

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
  "kind": "user",
  "allowed_models": ["claude-3-5-sonnet-20241022"],
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
[docs/cache-keepalive-operator.md](./cache-keepalive-operator.md).
`cache_keepalive.classifier.llm_judge` is reserved for a future release;
create/update requests that set it return `unsupported_cache_keepalive_llm_judge`.

#### PrincipalResponse

```json
{
  "id": "<PRINCIPAL_ID>",
  "name": "principal-name",
  "kind": "user",
  "enabled": true,
  "revision": 1,
  "allowed_models": ["claude-3-5-sonnet-20241022"],
  "default_limits": [],
  "cache_keepalive": null
}
```

The `cache_keepalive` field mirrors whatever was persisted on the
principal, or `null` when unset. `PATCH /admin/v1/principals/{id}` with
`"cache_keepalive": null` clears the config.

### Plugins API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| GET | `/admin/v1/plugins/registry` | Bearer | None | RegistryListResponse | None |
| GET | `/admin/v1/plugins/registry/{id}` | Bearer | None | RegistryEntryResponse | `unknown_registry_entry` |
| PATCH | `/admin/v1/plugins/registry/{id}` | Bearer | PatchRegistryBody | RegistryEntryResponse | `stale_revision` |
| DELETE | `/admin/v1/plugins/registry/{id}` | Bearer | None | None | `referenced_by` |
| GET | `/admin/v1/principals/{principal_id}/plugin-chain` | Bearer | None | ChainListResponse | None |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain` | Bearer | InsertChainBody | PluginChainEntry | `conflict` |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain/reorder` | Bearer | ReorderBody | None | `stale_revision` |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain/rebalance` | Bearer | None | None | None |
| PUT | `/admin/v1/plugin-chain-entries/{id}` | Bearer | PluginChainEntryUpdate | PluginChainEntry | `stale_revision` |
| DELETE | `/admin/v1/plugin-chain-entries/{id}` | Bearer | None | None | `stale_revision` |

#### InsertChainBody

```json
{
  "slot": "Router",
  "wasm_registry_id": "<PLUGIN_ID>",
  "config": {},
  "sse_per_event": false,
  "batched_events_per_flush": 0,
  "batched_flush_ms": 0,
  "position": null
}
```

### Status and Export API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| GET | `/admin/v1/status` | Bearer | None | StatusResponse | `storage_unavailable` |

`/admin/status` is a legacy alias for `/admin/v1/status` and returns the same `StatusResponse` body.
| GET | `/admin/v1/export` | Bearer | None | ExportResponse | `storage_unavailable` |

## OAuth Subscription Flow

The OAuth subscription flow allows upstreams to authenticate dynamically using OAuth PKCE.

### Step-by-Step Walkthrough

1. Create an OAuth upstream:

```bash
curl -X POST http://localhost:8001/admin/v1/upstreams \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "oauth-upstream",
    "kind": "anthropic_oauth"
  }'
```

2. Start the OAuth flow:

```bash
curl -X POST http://localhost:8001/admin/v1/upstreams/<UPSTREAM_ID>/oauth/start \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{}'
```

Response:

```json
{
  "authorize_url": "https://provider.com/oauth/authorize?client_id=<CLIENT_ID>&state=<STATE_TOKEN>...",
  "state_token": "<STATE_TOKEN>"
}
```

3. The user visits the `authorize_url` in their browser, completes the authorization, and gets redirected to the redirect URI with a `code` query parameter.

4. Complete the OAuth flow:

```bash
curl -X POST http://localhost:8001/admin/v1/upstreams/<UPSTREAM_ID>/oauth/complete \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "state_token": "<STATE_TOKEN>",
    "code": "authorization-code"
  }'
```

Response:

```json
{
  "upstream_id": "<UPSTREAM_ID>",
  "expires_at_unix_secs": 1716934800,
  "access_token_fingerprint": "a1b2c3d4"
}
```

### Refresh Semantics

The system runs a background sweeper every 60 seconds to refresh tokens that expire within 300 seconds. To prevent multiple replicas from refreshing the same token, the replica must claim a 90-second database lease.

If the background refresh fails or a token expires before the sweeper runs, the proxy uses a lazy refresh fallback. When a request is signed, the signer checks if the token is expired or within the 30-second skew window. If so, it triggers an on-demand refresh, updates the database, and retries the request once.

## Wasm Upload Workflow

You upload Wasm plugins via a multipart POST request to `/admin/v1/plugins/wasm`.

### Upload Process

1. Send a multipart request containing the Wasm binary:

```bash
curl -X POST http://localhost:8001/admin/v1/plugins/wasm \
  -H "Authorization: Bearer <TOKEN>" \
  -F "name=my-plugin" \
  -F "original_filename=plugin.wasm" \
  -F "bytes=@path/to/plugin.wasm"
```

Response:

```json
{
  "sha256_hex": "sha256-hex-string",
  "id": "<PLUGIN_ID>",
  "size_bytes": 1024,
  "original_filename": "plugin.wasm",
  "revision": 1,
  "idempotent": false
}
```

### Validation and Deduplication

- **Size Limit**: The upload is limited to 32 MiB per blob.
- **Magic Bytes**: The server performs a magic-bytes check to verify that the uploaded file starts with `\0asm`.
- **Wasmtime Validation**: The server inspects the plugin with wasmtime (`inspect_wasm`) to verify required exports (`cc_lb_alloc`, `cc_lb_free`, `memory`, and the per-slot hook), reject disallowed host imports, and confirm the embedded BLAKE3 schema hash matches the slot's wire contract.
- **SHA-256 Deduplication**: The server computes the SHA-256 hash of the bytes to deduplicate uploads. If the blob already exists, the server updates the registry metadata without duplicating the file on disk.
- **Reference Counting**: The server tracks references to each plugin blob. If you attempt to delete a registry entry that is currently referenced by a principal's plugin chain, the delete operation is blocked with a `referenced_by` conflict error.

## Multi-Replica Operations

In multi-replica deployments, replicas coordinate configuration updates and background tasks.

### Coordination Mechanisms

- **LISTEN/NOTIFY**: Replicas subscribe to Postgres `LISTEN/NOTIFY` channels to receive real-time change events. When a replica receives a notification, it rebuilds its local `DynamicView` snapshot within sub-seconds.
- **Reconciliation Fallback**: If a replica misses a notification, a background reconciler polls the database every 60 seconds as a fallback.
- **Database Lease**: For background tasks like OAuth token refresh, replicas use a database-backed UUID lease to ensure only one replica performs the refresh.
- **Status Monitoring**: You can monitor the status of all replicas and check for partial-failure states via the `/admin/v1/status` endpoint.
- **Storage Backend**: Use Postgres for multi-replica deployments.

## Restart-Required Matrix

Some configuration changes in `cc-lb.toml` cannot be applied via hot-reload and require a process restart.

| Field | Hot | RestartRequired | Reason |
|---|---|---|---|
| `listener.proxy_addr` | No | Yes | Socket bindings are fixed at process start |
| `listener.admin_addr` | No | Yes | Socket bindings are fixed at process start |
| `listener.metrics_addr` | No | Yes | Socket bindings are fixed at process start |
| `listener.tls.cert_path` | No | Yes | Listener TLS certificate changes require a process restart |
| `listener.tls.key_path` | No | Yes | Listener TLS key changes require a process restart |
| `tls.cert_path` | No | Yes | TLS certificate changes require a process restart |
| `tls.key_path` | No | Yes | TLS key changes require a process restart |
| `storage.path` | No | Yes | Storage backend changes require a process restart |
| `storage.url` | No | Yes | Storage backend changes require a process restart |
| `storage.pool` | No | Yes | Storage pool changes require a process restart |
| `aead.key_env` | No | Yes | Storage encryption key environment changes require a process restart |
| `oauth.anthropic.client_id` | No | Yes | Anthropic OAuth client changes require a process restart |
| `oauth.anthropic.auth_url` | No | Yes | Anthropic OAuth endpoint changes require a process restart |
| `oauth.anthropic.token_url` | No | Yes | Anthropic OAuth endpoint changes require a process restart |
| `oauth.anthropic.redirect_uri` | No | Yes | Anthropic OAuth redirect changes require a process restart |
| `oauth.anthropic.scopes` | No | Yes | Anthropic OAuth scope changes require a process restart |

All upstreams, principals, and plugin chains are fully dynamic (Hot: Yes, RestartRequired: No) because they are stored in the database and loaded dynamically.

## Audit and Redaction Guarantees

The system enforces strict audit and redaction guarantees to prevent sensitive credentials from leaking into logs or audit trails.

### Guarantees

- **Typed Audit Payload**: All administrative actions are logged using a typed `AuditPayload` enum, which ensures that only non-sensitive metadata is captured.
- **CI Grep Lint**: The codebase is protected by a CI grep lint that blocks any commit containing token literals or unredacted credentials.
- **AEAD Encryption**: Sensitive fields, such as OAuth tokens and API keys, are encrypted at rest using AEAD (Authenticated Encryption with Associated Data).
- **Associated Authenticated Data**: The system binds the encryption to the specific resource by using the upstream UUID as the Associated Authenticated Data (`aad = upstream_id.as_bytes()`). This prevents credentials from being copied or associated with a different upstream.

## Troubleshooting

This section lists common failures and their diagnosis steps.

### Server exits with "storage is required"

- **Symptom**: The server fails to start and logs "storage is required".
- **Diagnosis**: There is currently no Postgres-as-startup-storage path.
- **Workaround**: Use the sqlite backend for single-replica deployments until a follow-up implements Postgres startup storage.

### OAuth refresh fails persistently

- **Symptom**: OAuth tokens are not refreshed, and requests fail with expired token errors.
- **Diagnosis**: Check the `/metrics` endpoint for `cclb_oauth_refresh_total{outcome="..."}` to see the error categorization.
- **Workaround**: Verify that the network can reach the OAuth provider and that the client credentials in `cc-lb.toml` are correct. The lazy refresh mechanism will automatically retry on signing failures.

### Replica B doesn't see new upstream after 5s

- **Symptom**: Changes made on Replica A are not visible on Replica B within a few seconds.
- **Diagnosis**: This indicates a cross-replica visibility lag. Postgres `NOTIFY` should provide sub-second updates. If `NOTIFY` is blocked or misconfigured, the 60-second reconciliation fallback will eventually sync the state.
- **Workaround**: Check the database connection and verify that the database user has correct permissions for `LISTEN` and `NOTIFY` operations.

### Plugin upload returns 413

- **Symptom**: Uploading a Wasm plugin fails with a 413 Payload Too Large error.
- **Diagnosis**: The uploaded Wasm file exceeds the 32 MiB limit.
- **Workaround**: Optimize the Wasm binary size or split the plugin logic if possible. Ensure that the file size is strictly under 32 MiB.
