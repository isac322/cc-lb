---
title: Operate and extend cc-lb
description: Install, operate, and extend a self-hosted Anthropic-compatible reverse proxy and load balancer.
slug: docs
---

cc-lb is a self-hosted Anthropic-compatible reverse proxy and load balancer for pooled API-key and OAuth upstreams.

Use the docs to:

- [Run a local instance](/docs/getting-started/) with an authenticated admin surface.
- Understand [upstreams, cache continuity, and quota routing](/docs/concepts/).
- Operate [runtime state, scheduling, and observability](/docs/operations/).
- Review [configuration, the admin API, and metrics](/docs/reference/).
- Build [Wasmtime filter and shape plugins](/docs/plugins/).
- Read the project's [trust and scope notes](/docs/trust/).

## Supported scope

The current upstream contract covers Anthropic API-key and Anthropic OAuth upstreams. cc-lb does not claim to be a universal multi-provider gateway. It is an independent project and is not affiliated with or endorsed by Anthropic.

## Choose a path

| If you are... | Start here |
| --- | --- |
| Setting up a shared endpoint | [Getting started](/docs/getting-started/) |
| Evaluating routing behavior | [Concepts](/docs/concepts/) |
| Running a deployment | [Operations](/docs/operations/) |
| Integrating with automation | [Reference](/docs/reference/) |
| Authoring WebAssembly hooks | [Plugins](/docs/plugins/) |
| Reviewing project boundaries | [Trust](/docs/trust/) |
