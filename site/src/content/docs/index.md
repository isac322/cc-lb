---
title: Operate and extend cc-lb
description: Install, operate, and extend cc-lb with pooled Anthropic, OpenAI, and Gemini API-key and OAuth upstreams.
slug: docs
---

cc-lb is a self-hosted reverse proxy and load balancer for pooled Anthropic, OpenAI, and Gemini API-key and OAuth upstreams.

Use the docs to:

- [Run a local instance](/docs/getting-started/) with an authenticated admin surface.
- Understand [upstreams, cache continuity, and quota routing](/docs/concepts/).
- Operate [runtime state, scheduling, and observability](/docs/operations/).
- Review [configuration, the admin API, and metrics](/docs/reference/).
- Build [Wasmtime filter and shape plugins](/docs/plugins/).
- Read the project's [trust and scope notes](/docs/trust/).

## Supported scope

The eight shipped upstream kinds are `anthropic_api_key`, `anthropic_oauth`, `openai_api_key`, `openai_oauth`, `gemini_api_key`, `gemini_oauth`, `vertex_api_key`, and `vertex_credential`.

Shared routes preserve each provider's native request and response format:

| Provider | Shared routes |
| --- | --- |
| Anthropic | `POST /v1/messages`, `POST /v1/messages/count_tokens` |
| OpenAI | `POST /v1/responses`, `GET /v1/responses` WebSocket |
| Gemini | `POST /v1beta/models/{model}:generateContent`, `POST /v1beta/models/{model}:streamGenerateContent`, `POST /v1beta/models/{model}:countTokens` |

Provider-prefixed routes select a provider explicitly. See the [runtime endpoint reference](/docs/reference/source/runtime-management/#proxy-capability-boundary) for supported operations and eligible credentials.

cc-lb does not translate requests between providers or act as a universal multi-provider gateway. Vertex AI Gemini is supported through scoped `vertex_api_key` and `vertex_credential` upstreams; Express and regional API-key targets support generate, stream, and countTokens only because Google rejects API keys for cached-content creation, the publisher model catalog, and Live. Regional credential targets additionally support approved cached-content create/use, publisher model-catalog list/get, and Live routes through Vertex. The catalog is a minimal publisher projection, not entitlement-aware. Explicit cached-content billing remains Vertex-controlled; use least-privilege service accounts. Bedrock remains unsupported; ambient ADC/metadata-server credentials, external-account files, and account-bound provider-management APIs remain outside the supported proxy scope.

cc-lb is an independent project and is not affiliated with or endorsed by Anthropic.

## Choose a path

| If you are... | Start here |
| --- | --- |
| Setting up a shared endpoint | [Getting started](/docs/getting-started/) |
| Evaluating routing behavior | [Concepts](/docs/concepts/) |
| Running a deployment | [Operations](/docs/operations/) |
| Integrating with automation | [Reference](/docs/reference/) |
| Authoring WebAssembly hooks | [Plugins](/docs/plugins/) |
| Reviewing project boundaries | [Trust](/docs/trust/) |
