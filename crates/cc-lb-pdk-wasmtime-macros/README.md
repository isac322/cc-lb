# cc-lb-pdk-wasmtime-macros

`cc-lb-pdk-wasmtime-macros` contains the procedural macros used to author
cc-lb wasmtime plugins. Most plugin crates use them through
`cc-lb-pdk-wasmtime`:

```rust
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
```

## `#[cc_lb_plugin]`

Annotate one inline module with plugin metadata:

```rust
#[cc_lb_plugin(
    name = "my-plugin",
    version = "1.2.3",
    description = "Routes requests with custom policy.",
    usage = "Configure as a router filter in the admin API.",
)]
mod my_plugin {
    // handlers live here
}
```

The macro emits `cc_lb_alloc`, `cc_lb_free`, one hook export per handler, one
`cc_lb.schema.<hook>.vN` section per handler, and one `cc_lb.plugin.v1`
metadata section for the whole plugin.

## `#[handler]`

Annotate each hook function with the hook kind, wire version, and hook docs:

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

Supported hook kinds are `filter`, `shape`, and `observe`.

`wire = 1` is the current baseline. The host rejects uploads whose declared
wire version is not in the supported list for that hook. Add `view` when the
function wants an archived zero-copy request.

## `#[derive(WireSchema)]`

Derive `WireSchema` for wire structs and enums:

```rust
use cc_lb_plugin_wire::WireSchema;

#[derive(WireSchema)]
pub struct RuleMatch {
    pub key: Box<str>,
    pub value: Box<[u8]>,
}
```

The derive macro produces a canonical descriptor and a BLAKE3 layout
fingerprint. Any field-level layout change produces a new fingerprint.

## Minimal plugin example

```rust
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

See [`docs/plugin-author-guide.md`](../../docs/plugin-author-guide.md) for the
full workflow.
