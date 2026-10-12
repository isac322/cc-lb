---
title: Upstreams
description: Configure the Anthropic API-key and OAuth upstream kinds supported by cc-lb.
slug: docs/concepts/upstreams
---

An upstream is a named, enabled destination in the database-backed runtime view. The current public contract has two upstream kinds:

| Kind | Credential path | Default warm-up |
| --- | --- | --- |
| `anthropic_api_key` | Operator-supplied API key | Disabled |
| `anthropic_oauth` | OAuth flow managed by the admin API | Enabled after credentials exist |

The pool can contain either kind. The request path selects only enabled candidates that satisfy principal and routing constraints.

## Fallback between upstreams

OAuth accounts that need a reconnect, and upstreams whose credentials failed to load, are left out of routing. If the chosen upstream cannot take a request because its credentials cannot be prepared or signed, or the provider answers `401` or `429`, cc-lb sends the same request to the next upstream the routing filters pick: another OAuth account first, then an API key. A request tries at most three upstreams, and the last try goes to an API key when one is available. The client receives the last upstream's answer only when every allowed attempt fails.

cc-lb does not retry `400`, `403`, `5xx`, `529`, or connection failures on another upstream, because they either fail the same way everywhere or may already have reached the provider. Requests pinned to one account by web-search continuity stay on that account.

## API-key upstreams

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

## OAuth upstreams

Create an OAuth upstream, start the flow, open the returned authorization URL, then complete the flow with the returned state token and authorization code. The stored credential mode records the lifetime actually granted by the provider.

OAuth warm-up is opt-out after credentials exist. Operators can disable it per upstream with an ETag-protected update.

## Revision safety

Admin updates use optimistic concurrency. Read the current ETag or revision before an update, send it with `If-Match`, and handle `stale_revision` by reading the resource again. This prevents an older operator view from overwriting a newer runtime change.
