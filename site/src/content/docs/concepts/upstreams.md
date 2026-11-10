---
title: Upstreams
description: Understand the six Anthropic, OpenAI, and Gemini upstream kinds, their credential paths, and OAuth warm-up.
slug: docs/concepts/upstreams
---

An upstream is a named destination in the database-backed runtime view. cc-lb supports six upstream kinds:

| Kind | Credential path | Default warm-up |
| --- | --- | --- |
| `anthropic_api_key` | Operator-supplied Anthropic API key | Disabled; API-key warm-up is unsupported |
| `anthropic_oauth` | Admin-managed Anthropic OAuth connect flow | Enabled; requires credentials before sending |
| `openai_api_key` | Operator-supplied OpenAI API key | Disabled; API-key warm-up is unsupported |
| `openai_oauth` | Admin-managed OAuth sign-in or imported OpenAI OAuth token bundle | Enabled; requires credentials before sending |
| `gemini_api_key` | Operator-supplied Gemini API key | Disabled; API-key warm-up is unsupported |
| `gemini_oauth` | Antigravity subscription sign-in or imported Gemini OAuth token bundle; existing Gemini CLI enterprise grants retain their profile | Enabled; requires credentials before sending |

The pool can contain all six kinds. The request path selects only enabled candidates compatible with the request's provider and operation that satisfy principal and routing constraints. OAuth warm-up starts eligible subscription quota windows; it is separate from prompt-cache keepalive. Operators can opt out per upstream. See the [warm-up reference](/docs/reference/source/upstream-warmup/) for provider-specific windows and limits.

## Native route families

| Provider | Shared routes |
| --- | --- |
| Anthropic | `POST /v1/messages`, `POST /v1/messages/count_tokens` |
| OpenAI | `POST /v1/responses`, `GET /v1/responses` WebSocket |
| Gemini | `POST /v1beta/models/{model}:generateContent`, `POST /v1beta/models/{model}:streamGenerateContent`, `POST /v1beta/models/{model}:countTokens` |

Provider-prefixed routes use `/anthropic/v1/...`, `/openai/v1/...`, or `/gemini/v1beta/...` (plus Gemini model catalogs under `/gemini/v1/...`). The [runtime endpoint reference](/docs/reference/source/runtime-management/#proxy-capability-boundary) lists supported operations and credential restrictions.

cc-lb does not translate between providers and is not a universal multi-provider gateway. Bedrock, Vertex, SigV4 and ADC credentials, and account-bound provider-management APIs are outside the supported proxy scope. Supported model catalog routes are limited to those in the endpoint reference.

## Anthropic HTTP fallback between upstreams

For Anthropic HTTP requests, OAuth accounts that need a reconnect, and upstreams whose credentials failed to load, are left out of routing. If the chosen upstream cannot take a request because its credentials cannot be prepared or signed, or the provider answers `401` or `429`, cc-lb sends the same request to the next compatible upstream the routing filters pick: another OAuth account first, then an API key. A request tries at most three upstreams, and the last try goes to an API key when one is available. The client receives the last upstream's answer only when every allowed attempt fails.

cc-lb does not retry `400`, `403`, `5xx`, `529`, or connection failures on another upstream, because they either fail the same way everywhere or may already have reached the provider. Requests pinned to one account by web-search continuity stay on that account.

## Anthropic API-key upstreams

An API-key upstream stores a literal Anthropic API key supplied at create time:

```json
{
  "name": "primary-api-key",
  "kind": "anthropic_api_key",
  "base_url": "https://api.anthropic.com",
  "api_key_value": "sk-ant-..."
}
```

Send the key itself as `api_key_value` in the create request. cc-lb encrypts the credential for database storage and never returns the key; responses carry a masked `key_hint` such as `sk-ant-api03-...wxyz` instead. Keep plaintext credentials out of saved commands, shell history, and source control.

### Supported keys

| Key | Prefix | Result |
| --- | --- | --- |
| Console API key (workspace key) | `sk-ant-api03-` | Accepted after preflight |
| Personal or service-account key (identity-linked, all workspaces) | `sk-ant-usr-` | Accepted after preflight; needs a `workspace_id` |
| Admin API key | `sk-ant-admin` | Rejected with `key_admin`: it manages the org and cannot call models |
| Enterprise management key | `sk-ant-api01` | Rejected with `key_enterprise_scoped`: it has no inference access |
| OAuth access token | `sk-ant-oat01` | Rejected with `key_oat`: use an `anthropic_oauth` upstream instead |

Unknown prefixes are not rejected locally; the probes classify them.

### Preflight

Create and key, workspace, or `base_url` updates run preflight before anything is stored. `POST /admin/v1/upstreams/preflight` runs the same checks on a candidate key without saving it, and `POST /admin/v1/upstreams/{id}/preflight` re-checks a stored key. The inference probe goes to the configured `base_url`, the same host proxied traffic uses. A rejected key returns a remediation code, such as `key_invalid` for a `401`, with an operator message and nothing stored. Send `skip_preflight: true` to save a key the provider cannot be asked about; it is stored unverified until a re-check.

### Workspaces

A key created for one workspace is bound to it: Anthropic reports the workspace in a response header, and cc-lb rejects a `workspace_id` for it. A key created for **All workspaces** is org-scoped. Preflight answers `needs_workspace` with `workspace_required`, and the upstream must be saved with a `workspace_id` (`wrkspc_…`). cc-lb sends that id as `anthropic-workspace-id` on every proxied request. The proxy always strips a client-supplied `anthropic-workspace-id`.

`GET /admin/v1/upstreams/{id}/workspaces` lists the workspaces an org-scoped key can see. It works only when the key's account has the role to list them, and only on `https://api.anthropic.com`; otherwise copy the `wrkspc_…` id from the Console Workspaces page.

### Usage: reported or estimated

`GET /admin/v1/upstreams/{id}/usage` returns either `real` figures from Anthropic's usage report or `estimated` figures from cc-lb's own request log. Reported usage needs an org-scoped key, a linked account whose org role can read usage reports, and a `base_url` of exactly `https://api.anthropic.com`. Bound keys, custom endpoints, and accounts without the role stay estimated, and the response's `fix.code` names the change that would unlock reported usage. A background job refreshes reported usage every five minutes by default; when reports stop arriving, the last report is kept and marked stale.

Credits used are always an estimate at list price, because Anthropic reports tokens per key, not cost. Anthropic has no API for the credit balance, so cc-lb does not show one. When the upstream's latest request was refused for a billing reason (out of credits, a spend limit, the tier's monthly cap, or a payment problem), the month summary reports that billing stop. Any later request outcome that is not a billing refusal clears it, whether that request succeeded or failed for another reason.

### Endpoint and security

`base_url` must use `http` or `https` and must not contain credentials. Workspace lists and reported usage need the default endpoint, `https://api.anthropic.com`. Changing `base_url` does not require re-entering the key, so an admin who can edit an upstream can direct its stored key to any host. The change is audited with the old and new hosts and the key is re-checked at the new host.

API-key preflight and usage state do not affect `/readyz`.

## OpenAI and Gemini API-key upstreams

OpenAI and Gemini API-key upstreams use the same authenticated `POST /admin/v1/upstreams` create request as Anthropic. The request must include `name`, the provider-specific `kind`, and the literal provider key in `api_key_value`; `base_url` is optional and defaults as shown:

| Provider | `kind` | Default `base_url` | Credential field | `project_id` |
| --- | --- | --- | --- | --- |
| OpenAI | `openai_api_key` | `https://api.openai.com/` | `api_key_value` (OpenAI API key) | Optional; if supplied, it must be non-empty |
| Gemini | `gemini_api_key` | `https://generativelanguage.googleapis.com/` | `api_key_value` (Gemini API key) | Optional; if supplied, it must be non-empty |

cc-lb applies the stored credential using the provider-native signer: OpenAI requests use `Authorization: Bearer <key>`, and Gemini native requests use `x-goog-api-key: <key>`. Clients still authenticate to cc-lb with the issued proxy key.

For either kind, send a body such as:

```json
{
  "name": "provider-api-key",
  "kind": "openai_api_key",
  "base_url": "https://api.openai.com",
  "api_key_value": "replace-with-the-provider-api-key"
}
```

Change `kind` and `base_url` to `gemini_api_key` and `https://generativelanguage.googleapis.com` for Gemini. `project_id` is accepted for either API-key kind but is not required; Anthropic-only `workspace_id` is not used. The admin API preflights the key against the configured provider endpoint unless `skip_preflight` is requested, encrypts the stored credential, and returns only a masked key hint. The [runtime endpoint reference](/docs/reference/source/runtime-management/#proxy-capability-boundary) lists the provider-native routes and eligible API-key credentials.

Choose a model supported by the provider for the principal's `allowed_models`; the principal and proxy key are provider-neutral and do not require an Anthropic key. The request's model must match that allowed model and the provider-native route.

## OAuth upstreams

For an Anthropic, OpenAI, or Gemini OAuth upstream, use the authenticated [interactive provider OAuth flow](/docs/reference/admin-api/#oauth-flow): start the provider-specific draft, open the returned authorization URL (or follow the OpenAI device-code instructions), then complete it with the returned state token and code.

OpenAI and Gemini also accept imported OAuth token bundles through the authenticated admin API; see the [CLI/API OAuth bundle reference](/docs/reference/source/runtime-management/#cliapi-oauth-bundle-import) for provider-specific fields. Gemini subscription sign-in requires operator-configured [Antigravity client credentials](/docs/reference/configuration/#subscription-oauth-providers), does not use Vertex or ADC, and carries the [Google Antigravity Additional Terms warning](/docs/reference/admin-api/#oauth-flow).

OAuth warm-up is opt-out after credentials exist for all three OAuth kinds. Operators can disable it per upstream with an ETag-protected update.

## Revision safety

Admin updates use optimistic concurrency. Read the current ETag or revision before an update, send it with `If-Match`, and handle `stale_revision` by reading the resource again. This prevents an older operator view from overwriting a newer runtime change.
