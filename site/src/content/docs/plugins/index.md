---
title: Extend cc-lb with plugins
description: Build Wasmtime filter and shape plugins against the published cc-lb contract.
slug: docs/plugins
---

Plugins extend request routing and payload handling through Wasmtime hooks. The host validates the uploaded module, reads its metadata, checks hook contracts, and binds it into a runtime chain.

## Published contract

The published crate set contains five crates:

- `cc-lb-plugin-wire`
- `cc-lb-pdk-wasmtime-macros`
- `cc-lb-pdk-wasmtime`
- `cc-lb-runtime-wasmtime`
- `cc-lb-plugin-conformance`

The current plugin contract exposes two slots:

- **filter** keeps or rejects upstream candidates.
- **shape** builds the upstream-bound request and transforms buffered and SSE responses.

Start with [author a plugin](/docs/plugins/author-guide/) and use [conformance](/docs/plugins/conformance/) before uploading an artifact.
