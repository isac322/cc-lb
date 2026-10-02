---
title: Upstreams
description: Configure the Anthropic API-key and OAuth upstream kinds supported by cc-lb.
slug: docs/concepts/upstreams
---

An upstream is a named, enabled destination in the database-backed runtime view. The current public contract has two upstream kinds:

| Kind | Credential path | Default warm-up |
| --- | --- | --- |
| `anthropic_api_key` | Environment-backed API key | Disabled |
| `anthropic_oauth` | OAuth flow managed by the admin API | Enabled after credentials exist |

The pool can contain either kind. The request path selects only enabled candidates that satisfy principal and routing constraints.

## API-key upstreams

An API-key upstream can read its secret from a named environment variable in the server process:

```json
{
  "name": "primary-api-key",
  "kind": "anthropic_api_key",
  "base_url": "https://api.anthropic.com",
  "api_key_env": "ANTHROPIC_API_KEY"
}
```

Set `ANTHROPIC_API_KEY` in the server environment before sending the create request. cc-lb reads the value and encrypts the credential for database storage; it does not keep the environment variable name as a live secret reference. The API also accepts `api_key_value` for a directly supplied secret. Supply one field, not both, and keep plaintext credentials out of saved commands and source control.

## OAuth upstreams

Create an OAuth upstream, start the flow, open the returned authorization URL, then complete the flow with the returned state token and authorization code. The stored credential mode records the lifetime actually granted by the provider.

OAuth warm-up is opt-out after credentials exist. Operators can disable it per upstream with an ETag-protected update.

## Revision safety

Admin updates use optimistic concurrency. Read the current ETag or revision before an update, send it with `If-Match`, and handle `stale_revision` by reading the resource again. This prevents an older operator view from overwriting a newer runtime change.
