---
title: Trust and scope
description: Review cc-lb's license, security path, public boundaries, and evidence-led claims.
slug: docs/trust
---

cc-lb is an independent open-source project. Its public scope is intentionally narrow:

- Eight upstream kinds: `anthropic_api_key`, `anthropic_oauth`, `openai_api_key`, `openai_oauth`, `gemini_api_key`, `gemini_oauth`, `vertex_api_key`, and `vertex_credential`.
- Bounded native generation and token-count routes: Anthropic `POST /v1/messages` and `POST /v1/messages/count_tokens`; OpenAI `POST /v1/responses` and `GET /v1/responses` WebSocket; Gemini and Vertex `POST /v1beta/models/{model}:generateContent`, `:streamGenerateContent`, and `:countTokens`.
- Self-hosted operation with explicit operator-managed secrets and runtime state.
- Wasmtime filter and shape hooks through the published crate contract.
- Unsupported scope includes Bedrock, SigV4, ambient ADC/metadata-server and external-account credential paths, cross-provider translation, and account-bound provider-management APIs. Vertex AI Gemini is supported only through the documented scoped upstream kinds and operations; “verified Vertex” and “official” claims are not made.

The owner reports internal use with 10 connected accounts. That note describes one bounded deployment; it is not a customer count, SLA, enterprise certification, or global-scale claim.

Read [security](/docs/trust/security/) for reporting guidance and [license and provenance](/docs/trust/license/) for source and release references.
