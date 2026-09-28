# cc-lb-pdk-wasmtime-macros

`cc-lb-pdk-wasmtime-macros` contains the procedural macros for authoring cc-lb
wasmtime plugins. Plugin crates normally use these through
`cc-lb-pdk-wasmtime`.

```rust
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
```

## `#[cc_lb_plugin]`

Annotate one inline module with plugin metadata:

```rust
#[cc_lb_plugin(
    name = "cache-aware",
    version = "0.1.0",
    description = "Routes requests by cache affinity.",
    usage = "Attach to a router filter chain.",
)]
mod cache_aware {
    // handlers live here
}
```

The macro emits allocator exports, one hook export per handler, one
`cc_lb.schema.<hook>.vN` custom section per handler, and the consolidated
`cc_lb.plugin.v1` metadata section.

## `#[handler]`

Annotate each hook function with a hook kind, wire version, description, and
usage text:

```rust
#[handler(
    filter,
    wire = 1,
    description = "Keeps cache-compatible upstreams.",
    usage = "Requires candidate cache metadata from the host.",
)]
pub fn filter(req: FilterRequest) -> FilterResponse {
    // plugin logic
}
```

Supported hook kinds are `filter` and `shape`, plus the shape-owned
response-transform hooks. The PDK accepts `wire = 1` for every hook. Add `view`
when the handler wants an archived zero-copy request reference instead of an
owned request.

## `#[derive(WireSchema)]`

Derive `WireSchema` for wire structs and enums:

```rust
use cc_lb_pdk_wasmtime::WireSchema;

#[derive(WireSchema)]
pub struct RuleMatch {
    pub key: Box<str>,
    pub value: Box<[u8]>,
}
```

The derive macro produces a canonical descriptor and a BLAKE3 layout
fingerprint. The host compares those embedded fingerprints with its own current
wire expectations during admission.

## Minimal Complete Example

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
    usage = "Use as a smoke-test filter plugin.",
)]
mod accept_all {
    use super::*;

    #[handler(
        filter,
        wire = 1,
        description = "Accepts all candidates.",
        usage = "No configuration required.",
    )]
    pub fn filter(_req: FilterRequest) -> FilterResponse {
        FilterResponse { results: Box::from([]) }
    }
}
```

## Links

- Main repository: <https://github.com/isac322/cc-lb>
- Plugin author guide: <https://github.com/isac322/cc-lb/blob/master/docs/plugin-author-guide.md>
- PDK README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-pdk-wasmtime/README.md>
- Wire README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-plugin-wire/README.md>

## License

Licensed under the workspace license.
