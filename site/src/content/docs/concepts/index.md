---
title: Routing model
description: Understand how cc-lb selects upstreams, preserves session cache continuity, and paces pooled quota.
slug: docs/concepts
---

cc-lb presents one operator-managed endpoint while keeping upstream and principal state explicit. The request path classifies traffic, builds an eligible candidate set, applies routing filters, and sends the request through the selected upstream.

## The five mechanisms

1. **Upstream pooling** supports Anthropic API-key and Anthropic OAuth upstream kinds.
2. **Session cache continuity** uses a best-effort keepalive scheduler for eligible active sessions.
3. **Quota pacing** applies cost-first selection and weekly pace constraints to candidate choice.
4. **Dynamic views** rebind runtime state after database-backed changes.
5. **Wasmtime hooks** let operators add filter and shape behavior without changing proxy code.

Read the focused pages for the behavior and its limits:

- [Upstreams](/docs/concepts/upstreams/)
- [Prompt-cache continuity](/docs/concepts/prompt-cache/)
- [Quota routing](/docs/concepts/quota-routing/)

## Request path boundaries

The proxy key identifies a principal. The principal's allowed models, limits, cache-keepalive settings, and plugin chain participate in request handling. Upstream credentials remain operator-managed runtime state and are not part of the client request.

The keepalive scheduler is best effort. It can reduce avoidable cache eviction during active sessions, but it does not guarantee a cache hit.
