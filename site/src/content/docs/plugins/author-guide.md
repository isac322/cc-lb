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
cc-lb-plugin-wire = "0.8"
cc-lb-pdk-wasmtime = "0.1"
```

## Declare plugin metadata

The PDK expects one module annotated with `#[cc_lb_plugin(...)]`:

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

## Choose a slot

A `filter` hook returns a decision for each upstream candidate. The host keeps
a candidate only when its `decision` is `"accept"`; every candidate absent from
`results` is rejected, so an empty `results` rejects the whole set. A `shape`
plugin must implement request shaping, buffered response transformation, and
SSE event transformation. Use explicit no-op handlers for response hooks that
the plugin does not change.

Each handler currently uses `wire = 1`. Wire versions are independent per hook, so a future contract can evolve one hook without silently changing the others.

## Build and upload

```bash
cargo build --release --target wasm32-unknown-unknown
curl -X POST http://[::1]:9090/admin/v1/plugins/wasm \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F "original_filename=plugin.wasm" \
  -F "bytes=@target/wasm32-unknown-unknown/release/plugin.wasm"
```

The host validates imports, required exports, metadata, wire versions, schema fingerprints, and an upload-time runtime probe before dispatch. Uploading the same name and SHA is idempotent. Replacing an existing name may require an explicit confirmation when the new version is not higher.
