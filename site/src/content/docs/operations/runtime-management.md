---
title: Runtime management
description: Update upstreams, principals, keys, and plugin chains through the authenticated admin API.
slug: docs/operations/runtime-management
---

Runtime management stores operational entities in the database and compiles them into an immutable dynamic view. When a change is accepted, the server builds a new snapshot and rebinds it for subsequent requests.

## Authentication

Admin operations use a configured provider. The static-token provider reads a Bearer token from an environment variable. Keep that token separate from proxy keys and rotate it through the deployment secret mechanism.

## Resource lifecycle

The admin API manages:

- Upstreams, including API-key and OAuth credentials.
- Principals, allowed models, limits, and cache-keepalive settings.
- Proxy keys issued for principals.
- Plugin registry entries and principal plugin chains.
- Status and export views.

Use `If-Match` or the current revision for updates and deletes. A `stale_revision` response means another writer changed the resource; fetch the resource again before retrying.

## Local proxy key files

A local proxy key file is a client-side cache of the plaintext key returned once by the key-issue endpoint. To rotate it, issue the replacement, write the file atomically with mode `0600`, verify a request with the replacement, then revoke the old key.

The server does not rewrite operator key files automatically.

## Dynamic rebinding

A successful runtime update rebuilds the routing view. In a multi-replica Postgres deployment, replicas use database notifications and a periodic reconciliation fallback to converge their local views.
