---
title: Install and configure
description: Create an upstream, principal, proxy key, and first authenticated request.
slug: docs/getting-started/install
---

After the [binary or container setup](/docs/getting-started/), cc-lb keeps upstreams, principals, proxy keys, and plugin chains in database-backed runtime state. The admin API is authenticated with the Bearer token configured at startup.

The examples below use `http://127.0.0.1:9090` for the admin API and `http://127.0.0.1:8080` for the proxy. For a container deployment, use the host address and published ports you selected.

The complete Anthropic API-key flow is shown first; the same admin create endpoint also accepts the OpenAI and Gemini API-key kinds below.

## Create an Anthropic API-key upstream

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

## Create an OpenAI or Gemini API-key upstream

Use the shared `POST /admin/v1/upstreams` request with the provider-specific values in this table. `base_url` may be omitted to use the listed default. `project_id` is optional for either API-key kind; it is accepted for provider metadata and must be non-empty when supplied. Neither API-key kind requires an Anthropic key or an Anthropic workspace.

| Provider | `kind` | Default `base_url` | Credential field | Project requirement |
| --- | --- | --- | --- | --- |
| OpenAI | `openai_api_key` | `https://api.openai.com/` | `api_key_value`: OpenAI API key | No project required; optional non-empty `project_id` |
| Gemini | `gemini_api_key` | `https://generativelanguage.googleapis.com/` | `api_key_value`: Gemini API key | No project required; optional non-empty `project_id` |

Create an OpenAI API-key upstream:

```bash
curl -X POST http://127.0.0.1:9090/admin/v1/upstreams \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "openai-api-key",
    "kind": "openai_api_key",
    "base_url": "https://api.openai.com",
    "api_key_value": "replace-with-your-openai-api-key"
  }'
```

Create a Gemini API-key upstream:

```bash
curl -X POST http://127.0.0.1:9090/admin/v1/upstreams \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "gemini-api-key",
    "kind": "gemini_api_key",
    "base_url": "https://generativelanguage.googleapis.com",
    "api_key_value": "replace-with-your-gemini-api-key"
  }'
```

The admin API validates API-key credentials against the configured provider endpoint before storing them, encrypts them at rest, and returns only a masked key hint. Keep the literal key out of saved commands, shell history, and source control. For interactive OAuth setup, use the [provider OAuth flow](/docs/reference/admin-api/#oauth-flow). For OpenAI or Gemini bundle imports, use the [CLI/API OAuth bundle reference](/docs/reference/source/runtime-management/#cliapi-oauth-bundle-import); Gemini subscription sign-in additionally requires the operator's [Antigravity client credentials](/docs/reference/configuration/#subscription-oauth-providers) and carries the [Antigravity terms warning](/docs/reference/admin-api/#oauth-flow).

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

The example selects an Anthropic model. For an OpenAI or Gemini upstream, set `allowed_models` to a model that the selected provider supports (for example, a model returned by that provider's supported model catalog) and use that same model in the provider-native request. The principal only needs the proxy key; it does not need an Anthropic credential.

When the proxy key is issued, use the provider-native route for the selected model: OpenAI Responses at `/openai/v1/responses`, or Gemini generation at `/gemini/v1beta/models/{model}:generateContent`. The [runtime endpoint reference](/docs/reference/source/runtime-management/#proxy-capability-boundary) lists the request operations and credential eligibility.

Issue a proxy key for that principal and save the returned plaintext key when it is shown. The server stores only the protected representation and does not rewrite local key files for you:

```bash
curl -X POST http://127.0.0.1:9090/admin/v1/principals/<PRINCIPAL_ID>/keys \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Send a first Anthropic Messages request

For this Anthropic-specific example, use the issued proxy key on the proxy listener, not the admin listener:

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

This is an Anthropic Messages API request. cc-lb authenticates the principal, selects an enabled compatible upstream, and forwards the request; responses may be streamed back to the client.

## Diagnose an incomplete stream

HTTP 200 means that response headers were sent; it does not prove that the response body completed. When a response ends unexpectedly, check the stream metrics in the [metrics reference](/docs/reference/metrics/) and distinguish the bounded outcomes `completed`, `upstream_error`, `proxy_error`, and `client_cancelled`.

For runtime state changes, read [runtime management](/docs/operations/runtime-management/). For routing behavior, read the [routing model](/docs/concepts/).
