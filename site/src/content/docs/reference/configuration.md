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

## Subscription HTTP 429 failover

`prompt_cache_shadow.subscription_failover` defaults to `false` and takes effect at server startup. When enabled, `/v1/messages` may try one distinct OAuth subscription before delivering an upstream 429. The complete configured filter policy and hard provider affinity still apply; same-account 401 credential refresh is preserved. No safe alternate means the original 429 is returned.

Protection is process-local and shared with cache keepalive. Primary traffic retains its existing sharing behavior while honoring protection acquired by fallback attempts. Fallback requires compatible authenticated principal/session ownership or an observed idle account; common prompt prefixes are insufficient. Persisted warmth has unknown ownership after restart and vetoes fallback through its full 5m/1h TTL. Failed or unavailable protection reads disable optional fallback. The nullable independent observation deadline survives routing grace changes and purge; it cannot restore observations already deleted or discarded before migration. This protects server-observed evidence, with no guarantee about physical provider caches or coordination between replicas.

Persisted keepalive renewal anchors retain full-TTL protection after restart. Their model is encrypted, so this evidence can conservatively veto optional fallback across models until expiry; ordinary primary sharing remains available.
