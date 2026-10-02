---
title: Admin API
description: Use authenticated endpoints to manage upstreams, principals, plugin chains, and status.
slug: docs/reference/admin-api
---

Admin endpoints use the Bearer token from the configured authentication provider. Resource changes are database-backed and use optimistic concurrency.

## Core resources

| Resource | Common endpoints |
| --- | --- |
| Upstreams | `POST /admin/v1/upstreams`, `GET /admin/v1/upstreams`, `PUT /admin/v1/upstreams/{id}`, `DELETE /admin/v1/upstreams/{id}` |
| Principals | `POST /admin/v1/principals`, `GET /admin/v1/principals`, `PUT /admin/v1/principals/{id}` |
| Keys | `POST /admin/v1/principals/{id}/keys`, revoke through the key lifecycle endpoint |
| Plugins | `GET /admin/v1/plugins/registry`, `POST /admin/v1/plugins/wasm` |
| Status | `GET /admin/v1/status`, `GET /admin/v1/export` |

Every mutating request should use the current ETag or revision when the endpoint documents one. Handle `409` conflicts by reading the latest resource and presenting the new state to the operator.

## OAuth flow

For an OAuth upstream, create the resource, call the start endpoint, open the returned authorization URL, then complete the flow with the state token and authorization code. The response records the credential mode actually negotiated.

## Plugin chains

A principal receives the built-in `subscription-preference` router entry at creation. Use the plugin-chain endpoints to add, remove, or reorder additional entries. Registry reference counts include principal chains and upstream warm-up dialect bindings.
