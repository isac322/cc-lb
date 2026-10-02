---
title: Plugin conformance
description: Validate metadata, hook exports, and wire behavior before uploading a Wasm plugin.
slug: docs/plugins/conformance
---

Conformance checks catch contract failures before a module reaches an operator's runtime.

## Admission checks

The host rejects an upload when:

- The Wasm magic or module length is invalid.
- An import is outside the allow-list.
- Required exports for the declared slot are missing.
- The `cc_lb.plugin.v1` metadata section is absent or incomplete.
- A hook wire version or schema fingerprint is unsupported.
- The module exceeds the 32 MiB upload limit.
- The runtime probe cannot instantiate or call the declared hooks.

## Author workflow

1. Build the crate for `wasm32-unknown-unknown`.
2. Run the published conformance crate against the artifact.
3. Inspect metadata and hook descriptions.
4. Upload through the authenticated admin endpoint.
5. Read the registry response and verify the supported slots and SHA-256.
6. Attach the registry entry to a principal plugin chain only after the artifact is accepted.

Keep plugin descriptions and usage text operator-facing. Avoid putting credentials, prompts, or other request data into metadata or logs.
