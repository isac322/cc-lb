---
title: Install and configure
description: Create an upstream, principal, proxy key, and first authenticated request.
slug: docs/getting-started/install
---

After the [binary or container setup](/docs/getting-started/), cc-lb keeps upstreams, principals, proxy keys, and plugin chains in database-backed runtime state. The admin API is authenticated with the Bearer token configured at startup.

The examples below use `http://127.0.0.1:9090` for the admin API and `http://127.0.0.1:8080` for the proxy. For a container deployment, use the host address and published ports you selected.

## Create an API-key upstream

The API-key upstream stores the literal Anthropic API key sent as `api_key_value` and encrypts the credential for database storage:

```bash
curl -X POST http://127.0.0.1:9090/admin/v1/upstreams \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "primary-api-key",
    "kind": "anthropic_api_key",
    "base_url": "https://api.anthropic.com",
    "api_key_value": "replace-with-your-upstream-secret"
  }'
```

Substitute your real Anthropic API key for the placeholder. Use a standard Console API key (a workspace key `sk-ant-api03-…` or a personal or service-account key `sk-ant-usr-…`); Admin keys, Enterprise management keys, and OAuth tokens are rejected before anything is stored. cc-lb checks the key against `base_url` first and answers `201` with a masked `key_hint`; it never returns the key itself. Keep literal credentials out of saved commands, shell history, and source control.

If the key was created for **All workspaces**, the create fails with `workspace_required`. Add `"workspace_id": "wrkspc_…"` with the workspace the key should bill. Anthropic reports usage for such a key when its account's org role allows it; other keys and custom endpoints show cc-lb's estimates. The credit balance is not available through Anthropic's API, so cc-lb does not show it. The [upstreams concept page](/docs/concepts/upstreams/) covers key types, workspaces, usage, and credits.

For an OAuth upstream, use `"kind": "anthropic_oauth"` and complete the OAuth flow from the admin surface.

## Create a principal and proxy key

Create a principal with the model and limits it may use:

```bash
curl -X POST http://127.0.0.1:9090/admin/v1/principals \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "team-default",
    "kind": "machine",
    "allowed_models": ["claude-sonnet-4-5-20250929"],
    "default_limits": []
  }'
```

Issue a proxy key for that principal and save the returned plaintext key when it is shown. The server stores only the protected representation and does not rewrite local key files for you:

```bash
curl -X POST http://127.0.0.1:9090/admin/v1/principals/<PRINCIPAL_ID>/keys \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Send a first request

Use the issued proxy key on the proxy listener, not the admin listener:

```bash
export CC_LB_PROXY_KEY="replace-with-the-issued-proxy-key"

curl http://127.0.0.1:8080/v1/messages \
  -H "x-api-key: $CC_LB_PROXY_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{
    "model": "claude-sonnet-4-5-20250929",
    "max_tokens": 32,
    "messages": [{"role": "user", "content": "Reply with one word."}]
  }'
```

The request follows the Anthropic Messages API shape. cc-lb authenticates the principal, selects an enabled compatible upstream, and forwards the request; responses may be streamed back to the client.

## Diagnose an incomplete stream

HTTP 200 means that response headers were sent; it does not prove that the response body completed. When a response ends unexpectedly, check the stream metrics in the [metrics reference](/docs/reference/metrics/) and distinguish the bounded outcomes `completed`, `upstream_error`, `proxy_error`, and `client_cancelled`.

For runtime state changes, read [runtime management](/docs/operations/runtime-management/). For routing behavior, read the [routing model](/docs/concepts/).
