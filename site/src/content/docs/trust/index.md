---
title: Trust and scope
description: Review cc-lb's license, security path, public boundaries, and evidence-led claims.
slug: docs/trust
---

cc-lb is an independent open-source project. Its public scope is intentionally narrow:

- Anthropic-compatible Messages API traffic.
- Anthropic API-key and Anthropic OAuth upstream kinds.
- Self-hosted operation with explicit operator-managed secrets and runtime state.
- Wasmtime filter and shape hooks through the published crate contract.

The owner reports internal use with 10 connected accounts. That note describes one bounded deployment; it is not a customer count, SLA, enterprise certification, or global-scale claim.

Read [security](/docs/trust/security/) for reporting guidance and [license and provenance](/docs/trust/license/) for source and release references.
