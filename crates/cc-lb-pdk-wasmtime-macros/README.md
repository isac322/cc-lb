# cc-lb-pdk-wasmtime-macros

`cc-lb-pdk-wasmtime-macros` contains the procedural macros for authoring cc-lb
wasmtime plugins. Plugin crates normally use these through
`cc-lb-pdk-wasmtime`.

```rust
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
```

## `#[cc_lb_plugin]`

Annotate one inline module with the plugin's name, version, description, and
usage text. The complete `accept-all` example below includes this metadata.

The macro emits allocator exports, one hook export per handler, one schema
custom section per distinct `(hook, wire)` pair, and one metadata section.
Legacy-only modules emit `cc_lb.plugin.v1`; modules with any provider-qualified
handler emit `cc_lb.plugin.v2`.

## `#[handler]`

Annotate each hook function with a hook kind, optional provider, wire version,
description, and usage text inside the plugin module. For example,
`#[handler(shape, provider = "openai", wire = 1, description = "...", usage = "...")]`
selects the OpenAI request-shaping entrypoint.

The provider selects exports such as `cc_lb_shape_openai`; the Rust function
name remains author-controlled. Omitting `provider` preserves the legacy
unqualified export, which the host maps to Anthropic only.

Supported providers are `anthropic`, `openai`, and `gemini`. Duplicate
`(hook, provider)` entries are rejected, including legacy plus qualified
Anthropic entries. Legacy entries may coexist with qualified OpenAI or Gemini
entries.

Supported hook kinds are `filter`, `shape`, `transform_response`, and
`transform_sse_event`. Provider-qualified hooks are independent; omit hooks
your plugin does not support. The PDK accepts `wire = 1` for every hook. Add
`view` when the handler wants an archived zero-copy request reference instead
of an owned request.

## Minimal Complete Example

```rust
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse, PerCandidateReason};

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
        provider = "openai",
        wire = 1,
        description = "Accepts all candidates.",
        usage = "No configuration required.",
    )]
    pub fn filter(req: FilterRequest) -> FilterResponse {
        let results = req
            .candidates
            .into_vec()
            .into_iter()
            .map(|candidate| PerCandidateReason {
                upstream_id: candidate.upstream_id,
                decision: Box::from("accept"),
                reason: Box::from(""),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        FilterResponse { results }
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
