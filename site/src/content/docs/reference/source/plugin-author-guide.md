---
title: "Plugin author source notes"
description: "Curated source sections for the Wasmtime plugin contract."
slug: docs/reference/source/plugin-author-guide
---

## Runtime model

The host loads each plugin under a single wasmtime engine configured
with `signals_based_traps` and request-path timeout/backpressure
controls rather than Wasmtime instruction metering. Local execution
defaults to on-demand allocation; each hook call gets a fresh `Store`
with `StoreLimits` derived from `runtime.wasmtime.memory_max_pages`,
and the runtime holds a process-wide store budget derived from
`pool_total_core_instances` so on-demand execution cannot instantiate
unbounded concurrent stores. Operators that need Wasmtime's pooling
allocator can opt in with `[runtime.wasmtime] allocation_strategy =
"pooling"` and tune the reservation/guard/pool totals in the same
   section.

## Quick Start

Create a Rust library crate that builds to `wasm32-unknown-unknown`:

A plugin upload travels through three gates before any user request
can reach it:

1. **`inspect_wasm` (host)** — walks raw `.wasm` bytes with
   `wasmparser`. Enforces imports allow-list (no host imports
   accepted), required exports per slot kind, and 32-byte BLAKE3
   schema hash custom section.
2. **`Engine::precompile_module` + `Module::deserialize`** —
   in-process compile (no .cwasm trust-boundary crossing).
3. **`Linker::instantiate_pre`** — produce reusable `InstancePre`
   for hot-path dispatch.

`Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
cc-lb-plugin-wire = "0.8"
cc-lb-pdk-wasmtime = "0.1"
```

Minimal filter plugin:

```rust
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse};

#[cc_lb_plugin(
    name = "accept-all",
    version = "0.1.0",
    description = "Accepts every upstream candidate.",
    usage = "Use as a smoke-test router filter.",
)]
mod accept_all {
    use super::*;

    #[handler(
        filter,
        wire = 1,
        description = "Accepts all candidates without modification.",
        usage = "Attach to a router filter chain for baseline validation.",
    )]
    pub fn filter(_req: FilterRequest) -> FilterResponse {
        FilterResponse { results: Box::from([]) }
    }
}
```

Build it:

```bash
cargo build --release --target wasm32-unknown-unknown
```

## Authoring a Plugin

The PDK expects one inline module annotated with `#[cc_lb_plugin(...)]`.
Handlers live inside that module and are annotated with `#[handler(...)]`.

Required plugin metadata:

- `name`: stable plugin identity; must match the admin upload `name` field.
- `version`: plugin package or artifact version.
- `description`: short operator-facing summary.
- `usage`: operator-facing deployment guidance.

```rust
#[cc_lb_plugin(
    name = "cache-aware",
    version = "0.1.0",
    description = "Routes requests toward warm prompt-cache upstreams.",
    usage = "Attach to router filter chains where cache locality is preferred.",
)]
mod cache_aware {
    use super::*;

    #[handler(
        filter,
        wire = 1,
        description = "Marks candidates as accepted or rejected by cache affinity.",
        usage = "Requires upstream candidate cache metadata from the host.",
    )]
    pub fn filter(req: FilterRequest) -> FilterResponse {
        let _ = req;
        FilterResponse { results: Box::from([]) }
    }
}
```

The macro emits allocator exports, hook exports, per-hook schema custom
sections, and one `cc_lb.plugin.v1` metadata custom section.

## Hook Contracts

cc-lb supports two plugin slot kinds. A wasm artifact may implement one or more hooks; upload registers the artifact for every slot whose hook it exports.

| Slot | Export | Request type | Response type | Use |
|---|---|---|---|---|
| `filter` | `cc_lb_filter` | `v1::FilterRequest` | `FilterResponse` | Keep or reject upstream candidates. The V1 request exposes the requested service tier. |
| `shape` | `cc_lb_shape`<br>`cc_lb_transform_response`<br>`cc_lb_transform_sse_event` | `ShapeRequest`<br>`TransformResponseRequest`<br>`TransformSseEventRequest` | `ShapeResponse`<br>`TransformResponseResult`<br>`TransformSseEventResult` | Unified slot that produces the upstream-bound request and transforms downstream responses (both buffered and SSE). |

Filter example:

```rust
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse, PerCandidateReason};

#[handler(
    filter,
    wire = 1,
    description = "Rejects candidates without observed cache state.",
    usage = "Attach before fallback filters in router chains.",
)]
pub fn filter(req: FilterRequest) -> FilterResponse {
    let results = req
        .candidates
        .iter()
        .map(|candidate| PerCandidateReason {
            upstream_id: candidate.upstream_id.clone(),
            decision: if candidate.observed_at_unix_secs > 0 {
                Box::from("accept")
            } else {
                Box::from("reject")
            },
            reason: Box::from("cache-observation-required"),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();

    FilterResponse { results }
}
```

Shape example (Unified Slot):

A plugin registered to the `shape` slot must implement all three wire hook functions: request shaping, buffered response transform, and SSE event transform. If your plugin only needs to shape requests or only needs to transform responses, you must explicitly implement no-op handlers for the other hooks.

Here is an example of a complete shape plugin that shapes requests and provides explicit no-op handlers for response transformation:

```rust
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v1::{
    Header, ShapeRequest, ShapeResponse,
    TransformResponseRequest, TransformResponseResult,
    TransformSseEventRequest, TransformSseEventResult,
};

#[cc_lb_plugin(
    name = "request-only-shaper",
    version = "0.1.0",
    description = "Shapes requests and passes responses through unchanged.",
    usage = "Attach to shape slots.",
)]
mod request_only_shaper {
    use super::*;

    #[handler(
        shape,
        wire = 1,
        description = "Builds the upstream request.",
        usage = "Attach to shape slots.",
    )]
    pub fn shape(req: ShapeRequest) -> ShapeResponse {
        ShapeResponse {
            url: Box::from(format!("https://api.anthropic.com{}", req.path)),
            method: req.method,
            headers: Box::from([Header {
                name: Box::from("content-type"),
                value: Box::from(&b"application/json"[..]),
            }]),
            body: req.body,
        }
    }

    #[handler(
        transform_response,
        wire = 1,
        description = "No-op buffered response transform.",
        usage = "Required by the unified shape slot.",
    )]
    pub fn transform_response(_req: TransformResponseRequest) -> TransformResponseResult {
        TransformResponseResult::Unchanged
    }

    #[handler(
        transform_sse_event,
        wire = 1,
        description = "No-op SSE event transform.",
        usage = "Required by the unified shape slot.",
    )]
    pub fn transform_sse_event(_req: TransformSseEventRequest) -> TransformSseEventResult {
        TransformSseEventResult::Unchanged
    }
}
```

Each handler declares its own `wire = N`. The published PDK supports `wire = 1`
for every hook.

## Metadata Contract

Plugin and per-hook metadata is required. The host rejects uploads when the
`cc_lb.plugin.v1` section is absent, malformed, or incomplete.

Required top-level fields:

- `name`
- `version`
- `description`
- `usage`
- `hooks`

Required per-hook fields:

- `wire_version`
- `description`
- `usage`

Upload rejection names relevant to plugin authors include:

- `missing_part`: a required multipart field is absent.
- `invalid_wasm_magic`: uploaded bytes do not start with the wasm magic.
- `invalid_wasm_length`: uploaded bytes are too short to be wasm.
- `wasm_too_large`: the wasm exceeds the 32 MiB upload limit.
- `invalid_wasm`: wasmtime admission rejected exports, metadata, wire versions,
  fingerprints, imports, or the runtime probe.

## Wire Versioning

Wire versions are independent per hook. The plugin declares the version on each
handler:

```rust
#[handler(
    filter,
    wire = 1,
    description = "Filters candidates.",
    usage = "Attach to router filter chains.",
)]
pub fn filter(req: FilterRequest) -> FilterResponse {
    let _ = req;
    FilterResponse { results: Box::from([]) }
}
```

Filter plugins receive the requested tier on the V1 filter request:

```rust
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse};

#[handler(
    filter,
    wire = 1,
    description = "Filters candidates using the requested service tier.",
    usage = "Attach to router filter chains that distinguish service tiers.",
)]
pub fn filter(req: FilterRequest) -> FilterResponse {
    let requested_service_tier = req.service_tier.as_deref();
    let _ = requested_service_tier;
    FilterResponse { results: Box::from([]) }
}
```

`service_tier` is the optional tier requested in the inbound request body. A
pre-request filter cannot observe the tier ultimately reported by an upstream
response.

The host maintains supported-version lists per hook:

- `HOST_SUPPORTED_FILTER_VERSIONS`
- `HOST_SUPPORTED_SHAPE_VERSIONS`
- `HOST_SUPPORTED_TRANSFORM_RESPONSE_VERSIONS`
- `HOST_SUPPORTED_TRANSFORM_SSE_EVENT_VERSIONS`

Admission rejects a plugin when the declared hook version is not in the host's
supported list. This rejection happens before the plugin is persisted into a
chain or called on user traffic.
