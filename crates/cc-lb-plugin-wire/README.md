# cc-lb-plugin-wire

`cc-lb-plugin-wire` is the shared wire contract between the cc-lb host and
wasm plugins. It contains the rkyv request and response types, hook and wire
version enums, layout fingerprint trait, and upload metadata schema used by the
wasmtime runtime.

Use this crate directly when writing plugin code that needs typed hook inputs
and outputs. The `cc-lb-pdk-wasmtime` crate re-exports the same wire types for
the common PDK path.

## What This Crate Provides

- Versioned hook wire types under `cc_lb_plugin_wire::v1`.
- `WireVersion`, currently `WireVersion::V1`.
- `HookKind` for `filter`, `shape`, and `observe` hooks.
- `WireSchema`, implemented by the PDK derive macro for layout fingerprints.
- `PluginMetadata` and `HookMetadata` behind the `std` feature.
- `pack_ret` and `unpack_ret` helpers for guest ABI return values.

## WireVersion, HookKind, and WireSchema

`WireVersion` identifies an incompatible wire layout line. The current baseline
is `V1`. New versions are added when a hook request or response layout changes
incompatibly.

`HookKind` names the three supported plugin hooks:

- `Filter` maps to the `cc_lb_filter` export.
- `Shape` maps to the `cc_lb_shape` export.
- `Observe` maps to the `cc_lb_observe` export.

`WireSchema` exposes a canonical descriptor and BLAKE3 fingerprint for a type.
The trait lives in this crate. The derive macro is re-exported by
`cc-lb-pdk-wasmtime` for plugin authors.

```rust
use cc_lb_pdk_wasmtime::WireSchema;

#[derive(WireSchema)]
pub struct CacheDecision {
    pub upstream_id: Box<str>,
    pub decision: Box<str>,
    pub reason: Box<str>,
}
```

Any field addition, removal, rename, reorder, or type edit changes the derived
descriptor and fingerprint. Plugin authors do not manage hashes manually.

## PluginMetadata Schema

The host expects one `cc_lb.plugin.v1` metadata section in each upload. PDK
macros generate that section from plugin and handler attributes.

Top-level metadata contains:

- `name`
- `version`
- `description`
- `usage`
- `hooks`

Each hook metadata entry contains:

- `wire_version`
- `description`
- `usage`

The upload path rejects missing metadata, unknown hook names, unsupported wire
versions, and empty description or usage text.

## Related Crates

- `cc-lb-runtime-wasmtime`: wire-only Wasmtime admission and dispatch; the
  host adapters live in `cc-lb-server/src/wasm_host/`.
- `cc-lb-pdk-wasmtime`: guest PDK runtime helpers and macro re-exports.
- `cc-lb-pdk-wasmtime-macros`: macro implementation for `#[cc_lb_plugin]`,
  `#[handler]`, and `#[derive(WireSchema)]`.
- `cc-lb-plugin-conformance`: in-process ABI and admission test harness.

## Links

- Main repository: <https://github.com/isac322/cc-lb>
- Plugin author guide: <https://github.com/isac322/cc-lb/blob/master/docs/plugin-author-guide.md>
- PDK README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-pdk-wasmtime/README.md>

## License

Licensed under the workspace license.
