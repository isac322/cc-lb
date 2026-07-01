# cc-lb plugin author guide (wasmtime PDK)

This guide is for engineers writing or porting a wasm plugin against
the cc-lb wasmtime + rkyv plugin runtime.

The canonical sources are:

- `crates/cc-lb-pdk-wasmtime` — guest PDK, re-exports `#[plugin]` /
  `#[handler]` macros + wire types.
- `crates/cc-lb-pdk-wasmtime-macros` — proc-macro implementation.
- `crates/cc-lb-plugin-types` — host ↔ guest wire types (rkyv) +
  schema constants under `cc_lb_plugin_types::schema`.
- `crates/cc-lb-runtime-wasmtime` — host runtime (engine config,
  load-time inspection, dispatch).

When this guide and the code disagree, the code wins.

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
with `consume_fuel`, `signals_based_traps`, a 4 GiB memory
reservation, and a `PoolingAllocationConfig`. Every hook call gets a
single fuel budget that must cover `cc_lb_alloc` + the hook export +
the implicit `cc_lb_free` the PDK does in the guest helper.

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

## Authoring a plugin

`Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
cc-lb-pdk-wasmtime.workspace = true
```

Plugin module:

```rust
#![cfg_attr(target_arch = "wasm32", no_std)]

use cc_lb_pdk_wasmtime::types::{
    ArchivedFilterRequest, FilterResponse, PerCandidateReason,
};

#[cc_lb_pdk_wasmtime::plugin(name = "cache-aware", version = "0.1.0")]
mod cache_aware {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(name = "filter", view)]
    pub fn filter(req: &ArchivedFilterRequest) -> FilterResponse {
        // …
    }
}
```

Build to wasm32:

```bash
cargo build --target wasm32-unknown-unknown --release -p your-plugin
```

The macro emits `cc_lb_alloc` / `cc_lb_free` / one
`cc_lb_<kind>(in_ptr, in_len) -> u64` export per handler, plus a
`cc_lb.schema.<kind>.v1` custom section per handler with the 32-byte
BLAKE3 of the wire-schema tag.

A single wasm artifact may implement multiple hook kinds; the
admin upload's `slot_kind` form field picks which exports + schema
section the host requires for that registration.

## Hook contracts

| Hook | Wire request | Wire response | Slot |
|---|---|---|---|
| `filter` | `FilterRequest` | `FilterResponse` | router-scope |
| `shape` | `ShapeRequest` | `ShapeResponse` | upstream-shape |
| `normalize_error` (optional, on a shape slot) | `NormalizeErrorRequest` | `NormalizeErrorResponse` | shape (paired) |
| `observe` | `ObserveEvent` | `(0, 0)` (no payload) | observability |

Observe is side-effect-only — the PDK dispatch helpers automatically
return `pack_ret(0, 0)`.

Minimal skeleton showing all four handlers in a single plugin:

```rust
use cc_lb_pdk_wasmtime::types::{
    ArchivedFilterRequest, ArchivedObserveEvent, ArchivedShapeRequest,
    FilterResponse, NormalizeErrorRequest, NormalizeErrorResponse,
    ShapeResponse,
};

#[cc_lb_pdk_wasmtime::plugin(name = "everything", version = "0.1.0")]
mod everything {
    use super::*;

    #[cc_lb_pdk_wasmtime::handler(name = "filter", view)]
    pub fn filter(_req: &ArchivedFilterRequest) -> FilterResponse {
        FilterResponse { results: Default::default() }
    }

    #[cc_lb_pdk_wasmtime::handler(name = "shape", view)]
    pub fn shape(_req: &ArchivedShapeRequest) -> ShapeResponse {
        ShapeResponse {
            url: "https://example.invalid".to_owned(),
            method: "POST".to_owned(),
            headers: Default::default(),
            body: Default::default(),
        }
    }

    #[cc_lb_pdk_wasmtime::handler(name = "normalize_error")]
    pub fn normalize_error(_req: NormalizeErrorRequest) -> NormalizeErrorResponse {
        NormalizeErrorResponse { normalized: None }
    }

    #[cc_lb_pdk_wasmtime::handler(name = "observe", view)]
    pub fn observe(_event: &ArchivedObserveEvent) {}
}
```

## Owned vs view mode

Each `#[handler]` accepts an optional `view` flag:

```rust
#[cc_lb_pdk_wasmtime::handler(name = "filter", view)]
pub fn filter(req: &ArchivedFilterRequest) -> FilterResponse { … }
```

vs

```rust
#[cc_lb_pdk_wasmtime::handler(name = "filter")]
pub fn filter(req: FilterRequest) -> FilterResponse { … }
```

| Mode | Argument | When to use |
|---|---|---|
| `view` | `&ArchivedT` (zero-copy) | Read-mostly handlers. Avoids deserialize cost. |
| owned (default) | `T` (deserialized) | Handlers that mutate or store the request beyond the call. |

The PDK frees the input buffer on the guest side as soon as the
borrow / owned copy is finished. The host traps if `cc_lb_alloc`
returns `0` and rejects `(out_ptr, out_len) = (0, 0)` for any
non-observe hook.

## Pure vs stateful dispatch

- **Pure** (default). Every call builds a fresh `Store`, runs the
  hook, drops the `Store`. No per-worker state across calls.
- **Stateful**. Per-worker `WorkerInstance` cached per `SlotKey` in
  a `thread_local!`. Rebuilds transactionally on `version_id` change
  or any trap.

Opt out of pure mode by setting `pure = false` in the
`PluginManifest` at upload time. The default
(`PluginManifest::pure = true` via `default_pure()`) means every
new plugin defaults to pure dispatch.

## Schema hash + bumping the wire

Wire-schema tags live in `cc_lb_plugin_types::schema`:

- `WIRE_SCHEMA_TAG_FILTER = b"cc_lb.wire.v1.filter.rkyv"`
- `WIRE_SCHEMA_TAG_SHAPE  = b"cc_lb.wire.v1.shape.rkyv"`
- `WIRE_SCHEMA_TAG_NORMALIZE_ERROR = b"cc_lb.wire.v1.normalize_error.rkyv"`
- `WIRE_SCHEMA_TAG_OBSERVE = b"cc_lb.wire.v1.observe.rkyv"`

Each plugin emits `BLAKE3(tag)` into its `cc_lb.schema.<kind>.v1`
custom section. The host computes the same hash at load time and
rejects mismatch.

To break a wire-schema: bump the tag suffix (e.g. `.v2`), update
the constant, rebuild every plugin against the new PDK. Old uploads
will be rejected with `hash mismatch` on next register.

## Admin upload

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F "bytes=@target/wasm32-unknown-unknown/release/my_plugin.wasm" \
  -F "name=my-plugin" \
  -F "original_filename=my-plugin.wasm" \
  -F "slot_kind=filter" \
  http://localhost:$ADMIN_PORT/admin/v1/plugins/wasm
```

The endpoint validates the multipart parts, runs `inspect_wasm`
inside `spawn_blocking`, computes SHA-256, persists to
`data/plugins/wasm/cache/{sha}.wasm`, inserts a `wasm_registry_v2`
row with `schema_hash` populated, and triggers a dynamic-view
rebind.

Rejections (400 + JSON `error` discriminator):

- `invalid_slot_kind` — slot_kind not `filter|shape|observe`.
- `missing_part` — required multipart part absent.
- `invalid_wasm` — `inspect_wasm` rejected (bad imports, missing
  exports, wrong schema_hash).
- `wasm_too_large` — > 32 MiB.

`gc_wasm` at `POST /admin/v1/plugins/wasm/gc` evicts orphaned blobs.

## Operational invariants

- **Imports allow-list**: any host import in the upload is a hard
  reject at `inspect_wasm` time. Phase 1 ships zero host imports.
- **Schema hash gate**: BLAKE3 of the wire tag must match
  byte-for-byte. No "compatible but newer" accepted.
- **Fuel budget**: every hook call resets fuel to `fuel_per_call`
  (default 10M instructions) before `cc_lb_alloc`. Alloc + hook +
  `cc_lb_free` all draw from this single budget.
- **`Store` drop on trap**: any trap in any of the three guest
  calls discards the `Store`. The next call rebuilds. No implicit
  circuit breaker — fuel exhaustion is the natural backpressure.
- **Adapter cell snapshot**: when a slot is re-registered, the
  previous live `DynamicView`'s adapters keep the old
  `Arc<PluginCell>` snapshot; the new view picks up the new cell.
  Atomic hot-swap with no mid-call state crossover.
- **Observe is non-blocking**: the host treats observe failures as
  `ObservabilityError::Dropped` and continues.

For deeper background see
[docs/rfc/0001-plugin-runtime-vnext.md](./rfc/0001-plugin-runtime-vnext.md).
