# cc-lb-pdk-wasmtime

`cc-lb-pdk-wasmtime` is the guest-side Plugin Development Kit for cc-lb wasm
plugins that run under wasmtime.

Most plugin authors import this crate for macros and wire types, then let the
macros generate the ABI exports.

## What this crate provides

- Re-exports for `#[cc_lb_plugin]`, `#[handler]`, and `#[derive(WireSchema)]`.
- A `types` module that re-exports `cc-lb-plugin-wire`.
- Guest-side `cc_lb_alloc` and `cc_lb_free` implementations used by generated
  exports.
- Dispatch helpers for owned and archived-view handler modes.
- A wasm32 global allocator based on `dlmalloc`.
- A wasm32 panic handler that traps so the host can discard the failed store.

## Global allocator setup

`wasm32-unknown-unknown` does not provide a default allocator. This crate
installs `dlmalloc::GlobalDlmalloc` when compiling for wasm32.

Plugin crates should depend on `cc-lb-pdk-wasmtime` directly and should not
declare their own global allocator unless they intentionally take ownership of
all guest allocation behavior.

```toml
[dependencies]
cc-lb-pdk-wasmtime = "0.1"
cc-lb-plugin-wire = "0.2"
```

## ABI exports

The host calls a small C ABI surface in guest memory:

- `cc_lb_alloc(size, align) -> ptr`
- `cc_lb_free(ptr, size, align)`
- `cc_lb_filter(in_ptr, in_len) -> packed_u64`
- `cc_lb_shape(in_ptr, in_len) -> packed_u64`
- `cc_lb_observe(in_ptr, in_len) -> packed_u64`

The `#[cc_lb_plugin]` macro emits the allocator exports. Each `#[handler]`
macro entry causes the plugin macro to emit the matching hook export.

Return values pack `(out_ptr, out_len)` into one `u64` as `(ptr << 32) | len`.
Observe handlers return `(0, 0)` because they do not produce a response body.

## Not usually called directly

Application plugin code should not call the private dispatch helpers.

Write normal Rust functions instead:

```rust
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse};

#[cc_lb_plugin(
    name = "cache-aware",
    version = "0.1.0",
    description = "Routes by cache affinity.",
    usage = "Attach to a router filter chain.",
)]
mod cache_aware {
    use super::*;

    #[handler(
        filter,
        wire = 1,
        description = "Filters upstream candidates.",
        usage = "Returns one decision per candidate.",
    )]
    pub fn filter(_req: FilterRequest) -> FilterResponse {
        FilterResponse { results: Box::from([]) }
    }
}
```

The macros handle serialization, allocation, deallocation, ABI exports, schema
fingerprints, and metadata sections.

## More information

See [`docs/plugin-author-guide.md`](../../docs/plugin-author-guide.md) for the
full plugin author workflow.
