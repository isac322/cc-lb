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
