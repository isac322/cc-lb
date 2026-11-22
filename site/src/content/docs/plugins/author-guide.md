---
title: Author a plugin
description: Create a Rust Wasm plugin with metadata, filter or shape hooks, and versioned wire contracts.
slug: docs/plugins/author-guide
---

Create a Rust library crate for `wasm32-unknown-unknown` and depend on the published PDK and wire crates.

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
cc-lb-plugin-wire = "0.9"
cc-lb-pdk-wasmtime = "0.2"
```

## Declare plugin metadata

The PDK expects one module annotated with `#[cc_lb_plugin(...)]`:

```rust
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v2::{FilterResponse, PerCandidateReason};
use cc_lb_plugin_wire::v3::FilterRequest;

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
        provider = "anthropic",
        wire = 3,
        description = "Accepts all candidates without modification.",
        usage = "Attach to a router filter chain for baseline validation.",
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

Required top-level metadata is `name`, `version`, `description`, `usage`, and `hooks`. Each hook declares its wire version and operator-facing description.

All plugins use the `cc_lb.plugin.v2` metadata section with provider-qualified hook keys.

## Choose a slot

A `filter` hook returns a decision for each upstream candidate. The host keeps
a candidate only when its `decision` is `"accept"`; every candidate absent from
`results` is rejected, so an empty `results` rejects the whole set. A `shape`
hook changes the outgoing request. Provider-qualified response, SSE, and
Realtime/Live transformations are independent optional hooks; omit those
hooks when the plugin does not need them.

## Choose a wire version

Wire versions are independent per hook slot. `cc_lb_plugin_wire::schema::host_supported_versions` defines the host admission matrix:

| Hook | Host-supported wire versions |
| --- | --- |
| `filter` | `v3` |
| `shape` | `v2` |
| `transform_response` | `v2` |
| `transform_sse_event` | `v2` |
| `transform_realtime_frame` | `v2` |
| `shape_envelope_gemini` | `v2` |

Every handler requires `provider = "anthropic"`, `provider = "openai"`, or `provider = "gemini"`. `filter` uses `wire = 3`; all shape-family hooks use `wire = 2`. `shape_envelope_gemini` is Gemini-only (`provider = "gemini"`) and requires a declared Gemini `shape` hook.

## Build and upload

```bash
cargo build --release --target wasm32-unknown-unknown
curl -X POST http://[::1]:9090/admin/v1/plugins/wasm \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F "original_filename=plugin.wasm" \
  -F "bytes=@target/wasm32-unknown-unknown/release/plugin.wasm"
```

The host validates imports, required exports, metadata, wire versions, schema fingerprints, and an upload-time runtime probe before dispatch. Uploading the same name and SHA is idempotent. Replacing an existing name may require an explicit confirmation when the new version is not higher.
