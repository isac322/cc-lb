# cc-lb Plugin Author Guide

This guide describes the current v1 wasmtime plugin contract for cc-lb plugin
authors. It covers the published crates, metadata requirements, per-hook wire
versioning, layout fingerprints, upload admission, and conformance testing.

The focused crate READMEs are also useful while building plugins:

- [`cc-lb-plugin-wire`](../crates/cc-lb-plugin-wire/README.md)
- [`cc-lb-pdk-wasmtime`](../crates/cc-lb-pdk-wasmtime/README.md)
- [`cc-lb-pdk-wasmtime-macros`](../crates/cc-lb-pdk-wasmtime-macros/README.md)
- [`cc-lb-plugin-conformance`](../crates/cc-lb-plugin-conformance/README.md)

## Contents

1. [Runtime model](#runtime-model)
2. [Authoring a plugin](#authoring-a-plugin)
3. [Hook contracts](#hook-contracts)
4. [Owned vs view mode](#owned-vs-view-mode)
5. [Pure vs stateful dispatch](#pure-vs-stateful-dispatch)
6. [Schema hash + bumping the wire](#schema-hash--bumping-the-wire)
7. [Admin upload](#admin-upload)
8. [Operational invariants](#operational-invariants)

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
cc-lb-plugin-wire = "0.2"
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

cc-lb currently supports five hook kinds. A wasm artifact may implement one or
more hooks, and upload-time `slot_kind=filter|shape|observe|transform_response|transform_sse_event`
selects which hook the registration targets.

| Hook | Export | Request type | Response type | Use |
|---|---|---|---|---|
| `filter` | `cc_lb_filter` | `FilterRequest` | `FilterResponse` | Keep or reject upstream candidates. |
| `shape` | `cc_lb_shape` | `ShapeRequest` | `ShapeResponse` | Produce the upstream-bound request. |
| `observe` | `cc_lb_observe` | `ObserveEvent` | none | Receive lifecycle events for side effects. |
| `transform_response` | `cc_lb_transform_response` | `TransformResponseRequest` | `TransformResponseResult` | Rewrite a buffered upstream response before delivery. |
| `transform_sse_event` | `cc_lb_transform_sse_event` | `TransformSseEventRequest` | `TransformSseEventResult` | Rewrite each upstream SSE event before delivery. |

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

Shape example:

```rust
use cc_lb_plugin_wire::v1::{Header, ShapeRequest, ShapeResponse};

#[handler(
    shape,
    wire = 1,
    description = "Builds the upstream Anthropic-compatible request.",
    usage = "Attach to shape slots that require URL rewriting.",
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
```

Observe example:

```rust
use cc_lb_plugin_wire::v1::ObserveEvent;

#[handler(
    observe,
    wire = 1,
    description = "Receives request lifecycle events.",
    usage = "Attach as an observability hook for audit or metrics sinks.",
)]
pub fn observe(event: ObserveEvent) {
    let _ = event;
}
```

Each handler declares its own `wire = N`. The current published PDK supports
`wire = 1`.

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
- `invalid_slot_kind`: `slot_kind` is not `filter`, `shape`, `observe`, `transform_response`, or `transform_sse_event`.
- `invalid_wasm_magic`: uploaded bytes do not start with the wasm magic.
- `invalid_wasm_length`: uploaded bytes are too short to be wasm.
- `wasm_too_large`: the wasm exceeds the 32 MiB upload limit.
- `identity_mismatch`: multipart `name` does not match plugin metadata `name`.
- `invalid_wasm`: wasmtime admission rejected exports, metadata, wire versions,
  fingerprints, imports, or the runtime probe.

## Wire Versioning

Wire versions are independent per hook. A future host can support filter v1 and
v2 while shape and observe remain on v1. The plugin declares the version on each
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

The host maintains supported-version lists per hook:

- `HOST_SUPPORTED_FILTER_VERSIONS`
- `HOST_SUPPORTED_SHAPE_VERSIONS`
- `HOST_SUPPORTED_OBSERVE_VERSIONS`

Admission rejects a plugin when the declared hook version is not in the host's
supported list. This rejection happens before the plugin is persisted into a
chain or called on user traffic.

## Layout Fingerprint (`WireSchema`)

The wire layout fingerprint is derived from the Rust type AST with
`#[derive(WireSchema)]`. The derive macro builds a canonical descriptor string
from the type name, fields, variants, and field types, then embeds
`BLAKE3(descriptor)` as a 32-byte fingerprint.

Plugin authors do not edit schema tags or manual hashes. A field edit naturally
changes the descriptor and the embedded fingerprint. During admission, the host
compares the plugin's embedded fingerprint with the current host fingerprint for
the requested hook and wire version. Mismatches are rejected as `invalid_wasm`.

## Runtime Probe (`admit_wasm`)

Static metadata and fingerprints do not catch every integration bug. The host
therefore runs `admit_wasm` for uploads, which compiles the module and executes
a canonical sample payload for each declared hook.

The runtime probe catches issues such as:

- missing allocator exports
- mismatched `cc_lb_alloc` or `cc_lb_free` behavior
- guest encoding mistakes
- hook export traps
- response buffers that cannot be decoded as the expected rkyv type

The probe runs before the upload is accepted for dispatch.

## Building the Wasm

Install the target once in your development environment:

```bash
rustup target add wasm32-unknown-unknown
```

Build the plugin artifact:

```bash
cargo build --release --target wasm32-unknown-unknown
```

The artifact is usually under:

```text
target/wasm32-unknown-unknown/release/<crate_name>.wasm
```

## Uploading

Upload through the admin API:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F "bytes=@target/wasm32-unknown-unknown/release/my_plugin.wasm" \
  -F "name=my-plugin" \
  -F "original_filename=my-plugin.wasm" \
  -F "slot_kind=filter" \
  "http://localhost:$ADMIN_PORT/admin/v1/plugins/wasm"
```

Successful upload responses include:

```json
{
  "sha256_hex": "...",
  "id": "...",
  "size_bytes": 12345,
  "original_filename": "my-plugin.wasm",
  "revision": 1,
  "idempotent": false
}
```

The registry list endpoint surfaces the persisted author metadata, including
`description`, `usage`, and `hook_metadata`:

```json
{
  "entries": [
    {
      "id": "...",
      "sha256_hex": "...",
      "name": "my-plugin",
      "description": "Accepts every upstream candidate.",
      "usage": "Use as a smoke-test router filter.",
      "hook_metadata": {
        "filter": {
          "wire_version": 1,
          "description": "Accepts all candidates without modification.",
          "usage": "Attach to a router filter chain for baseline validation."
        }
      }
    }
  ]
}
```

## Conformance Test

Add the conformance harness as a dev-dependency:

```toml
[dev-dependencies]
cc-lb-plugin-conformance = "0.2"
```

Use `ConformanceSuite::from_wasm` when the artifact exports one hook:

```rust
use cc_lb_plugin_conformance::ConformanceSuite;

fn wasm_bytes() -> Vec<u8> {
    std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/target/wasm32-unknown-unknown/release/my_plugin.wasm"
    ))
    .expect("build plugin wasm first")
}

#[test]
fn plugin_is_recognisable_by_current_host() {
    let wasm = wasm_bytes();
    ConformanceSuite::from_wasm(&wasm)
        .with_plugin_name("my-plugin")
        .assert_recognisable_by_current_host();
}

#[test]
fn plugin_passes_boundary_smoke() {
    let wasm = wasm_bytes();
    ConformanceSuite::from_wasm(&wasm)
        .with_plugin_name("my-plugin")
        .run();
}
```

`assert_recognisable_by_current_host()` is the admission gate. It proves the
current host can inspect, compile, fingerprint-check, and probe the plugin.

`run()` builds a live runtime session and performs ABI round-trips using
canonical payloads. It proves the boundary works; it does not replace semantic
tests for your plugin's routing, shaping, or observability behavior.

## Wire Version Bump Policy

Bump a hook's wire version when the hook request or response layout changes
incompatibly. Examples include adding fields, removing fields, renaming fields,
reordering fields, changing field types, or changing enum variants.

Host-side migration policy:

1. Add new v2 wire types for the changed hook.
2. Add `WireVersion::V2`.
3. Add v2 to that hook's supported-version list.
4. Keep v1 in the list while existing plugins are still supported.
5. Update admission, probe fixtures, and conformance coverage for v2.

Plugin author migration policy:

1. Update `cc-lb-plugin-wire` and `cc-lb-pdk-wasmtime`.
2. Change only the affected handler to `wire = 2`.
3. Update handler signatures and response construction for the new types.
4. Run conformance tests against the target host version.
5. Upload the rebuilt wasm and rebind chains after admission succeeds.

Because versions are per-hook, a multi-hook plugin can migrate one hook at a
time. For example, `filter` can move to v2 while `observe` remains on v1.
