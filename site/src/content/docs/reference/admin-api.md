---
title: Admin API
description: Use authenticated endpoints to manage upstreams, principals, plugin chains, and status.
slug: docs/reference/admin-api
---

Admin endpoints use the Bearer token from the configured authentication provider. Resource changes are database-backed and use optimistic concurrency.

The [consistency contract](/docs/reference/source/consistency-contract/) lists what a successful response guarantees for each admin action: which writes and audit records are durable, whether the serving replica's routing view is already rebuilt, and how other replicas and background work converge.

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

## Request event diagnostic fields

Request events are stored in `request_events_v1` with all 99 operational, latency-breakdown, and abnormal-stream-error scalar fields in typed relational columns. The `payload` column is dropped, so `GET /admin/v1/events/detail/{event_id}`, `GET /admin/v1/events/delta`, and final updates on `GET /admin/v1/events/stream` read the committed relational representation directly with zero TOAST overhead.

The optional `observability.request_diagnostics_log` setting additionally emits a `cc_lb.request_diagnostics.v1` stdout line after the row commits. Each line carries five deep diagnostic fields — `cache_breakpoints` without `lookback_prefixes`, `cache_control_message_indices`, `routing_trace`, `iterations`, and `internal_errors` — plus eight self-contained outcome and session-correlation fields: `thread_id`, `observed_session_id`, `input_tokens`, `output_tokens`, `cache_read_input_tokens`, `cache_creation_input_tokens`, `duration_ms`, and `upstream_ttfb_ms`.
