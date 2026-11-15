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

Legacy-only artifacts use the `cc_lb.plugin.v1` metadata section. Artifacts
with provider-qualified handlers use `cc_lb.plugin.v2`; bare hook keys remain
Anthropic-only.

## Choose a slot

A `filter` hook returns a decision for each upstream candidate. The host keeps
a candidate only when its `decision` is `"accept"`; every candidate absent from
`results` is rejected, so an empty `results` rejects the whole set. A `shape`
hook changes the outgoing request. Provider-qualified response, SSE, and
Realtime/Live transformations are independent optional hooks; omit those
hooks when the plugin does not need them. A legacy bare `shape` handler must
also declare `transform_response` and `transform_sse_event`, using explicit
no-op handlers when it does not transform responses.

## Choose a wire version

Wire versions are independent per hook. The example above uses the legacy V1
filter contract. `cc_lb_plugin_wire::schema::host_supported_versions` defines
the host admission matrix:

| Hook | Host-supported wire versions |
| --- | --- |
| `filter` | `v1`, `v2`, `v3` |
| `shape` | `v1`, `v2` |
| `transform_response` | `v1`, `v2` |
| `transform_sse_event` | `v1`, `v2` |
| `transform_realtime_frame` | `v2` |
| `shape_envelope_gemini` | `v2` |

V2 and V3 require provider-qualified metadata. Add
`provider = "anthropic"`, `provider = "openai"`, or `provider = "gemini"` to
the handler attribute. `shape_envelope_gemini` is Gemini-only and requires a
declared Gemini `shape` hook. The PDK's `#[handler]` macro currently supports
V1 and V2 only: `#[handler(..., wire = 3)]` does not compile, even though the
host admits filter V3.

## Build and upload

```bash
cargo build --release --target wasm32-unknown-unknown
curl -X POST http://[::1]:9090/admin/v1/plugins/wasm \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F "original_filename=plugin.wasm" \
  -F "bytes=@target/wasm32-unknown-unknown/release/plugin.wasm"
```

The host validates imports, required exports, metadata, wire versions, schema fingerprints, and an upload-time runtime probe before dispatch. Uploading the same name and SHA is idempotent. Replacing an existing name may require an explicit confirmation when the new version is not higher.
