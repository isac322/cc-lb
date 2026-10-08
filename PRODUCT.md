# Product

<!-- impeccable:product-schema 1 -->

Source of truth: `positioning.yml` defines public wording and its evidence references. Product behavior is defined by the implementation and the operator and plugin documentation linked below.

## Platform

web

This record covers the proxy server, embedded admin web app, public documentation site, and Wasmtime plugin ecosystem. Component-specific product records refine this shared context.

## Users

Primary: self-hosting platform engineers and tech leads operating Anthropic-compatible traffic. Secondary: Rust engineers authoring Wasmtime filter and shape plugins.

## Product purpose

A self-hosted Anthropic-compatible reverse proxy and load balancer for pooling API-key and OAuth upstreams. Operators manage the shared endpoint, credentials, routing, and access; clients use principal-scoped proxy keys.

## Positioning

cc-lb combines prompt-cache-aware routing, best-effort cache keepalive, quota-aware upstream selection, and database-backed runtime management. Wasmtime filter and shape plugins extend candidate selection and request or response handling.

The supported mechanisms and their evidence are recorded in `positioning.yml`. Cache continuity is an operating goal, not a guaranteed cache-hit rate.

## Operating context

- Operators run the published `ghcr.io/isac322/cc-lb` container and configure storage, listeners, a master key, and administrator authentication.
- The admin web app and authenticated admin API manage upstreams, principals, proxy keys, and plugin chains. Anthropic-compatible clients send requests through the proxy listener.
- SQLite is the default local storage backend and requires a local filesystem. PostgreSQL is the supported storage option for multi-replica deployments.
- Source builds and contribution setup are documented in `.github/CONTRIBUTING.md`; deployment and first-request setup are documented at `https://cc-lb.bhyoo.com/docs/getting-started/`.

## Capabilities and constraints

- Supported upstream kinds are `anthropic_api_key` and `anthropic_oauth`. cc-lb is not a universal multi-provider gateway.
- Listener, storage, master-key, and administrator-authentication configuration is fixed at process startup. Database-backed upstreams, principals, proxy keys, and plugin bindings are managed at runtime without rebuilding the binary.
- Proxy authentication precedes body buffering, parsing, routing, and upstream dispatch. Administrative configuration and runtime-management APIs require administrator authentication.
- Plugin authors use the published Wasmtime PDK and wire API. The supported plugin slots are `filter` and `shape`, including buffered-response and SSE-event transformation.
- Keepalive and quota-aware routing do not establish an SLA, guaranteed availability, or a guaranteed cache-hit rate.

## Brand commitments

Keep the let-gate mark, `cc-lb` wordmark, and Graphite palette. Use a restrained industrial editorial interface with clear hierarchy, crisp borders, and purposeful accent placement. Never use neon mesh gradients, glowing network particles, isometric gateway cubes, gradients, glows, shadows, or decorative noise.

Public copy is English, direct, and evidence-backed. cc-lb is independent of Anthropic; third-party names are referential and do not imply affiliation or endorsement.

## Product principles

- Keep public claims grounded in implementation evidence and `positioning.yml`.
- Make startup configuration and runtime management distinct to operators.
- Protect credentials and access boundaries before performing request work.
- Make routing, quota usage, and request outcomes inspectable through operator-facing state.
- Treat owner-confirmed internal use as operational evidence, not independent customer validation, an SLA, or a performance guarantee.

## Anti-claims

See `positioning.yml` `anti_claims`. Top exclusions: universal multi-provider gateway; official Anthropic/Claude integration; zero-config drop-in replacement; guaranteed 100% cache hits; enterprise-grade HA/SLA/global scale.

## Evidence on hand

- Public repository: `https://github.com/isac322/cc-lb`; public site and documentation: `https://cc-lb.bhyoo.com/`.
- Product wording and mechanism evidence: `positioning.yml`.
- Runtime and operator contracts: `docs/runtime-management.md`, `docs/upstream-warmup.md`, and `docs/scheduler.md`.
- Plugin contracts and authoring guidance: `docs/plugin-author-guide.md` and the published crate READMEs under `crates/`.
- Approved identity assets and usage guidance: `assets/brand/README.md`, `assets/brand/mark/`, `assets/brand/lockup/`, and `assets/brand/social/cc-lb-social-preview.png`.
- The public site contains real admin UI screenshots captured with synthetic fixture data; these are not production telemetry or external customer evidence.
- Internal use is owner-confirmed. Independent customer testimonials, an SLA, or externally validated performance claims must not be inferred from that evidence.
