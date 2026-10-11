# cc-lb-plugin-wire

`cc-lb-plugin-wire` is the shared wire contract between the cc-lb host and
wasm plugins. It contains the rkyv request and response types, hook and wire
version enums, layout fingerprint trait, and upload metadata schema used by the
wasmtime runtime.

Use this crate directly when writing plugin code that needs typed hook inputs
and outputs. The `cc-lb-pdk-wasmtime` crate re-exports the same wire types for
the common PDK path.

## What This Crate Provides

- Original hook types under `cc_lb_plugin_wire::v1`; quota-aware filter inputs under `v2`.
- `WireVersion::V1` and filter-only `WireVersion::V2`.
- `HookKind` for `filter` and `shape` hooks.
- `WireSchema`, the layout fingerprint of every hook wire type.
- `PluginMetadata` and `HookMetadata` behind the `std` feature.
- `pack_ret` and `unpack_ret` helpers for guest ABI return values.

## WireVersion, HookKind, and WireSchema

`WireVersion` identifies the wire layout line. Filter hooks support V1 and V2;
other hooks support V1. V2 adds cached `subscription_quotas` to filter candidates.
Both filter versions share the V1 response layout. Root re-exports remain V1.

`HookKind` names the supported plugin hooks, including:

- `Filter` maps to the `cc_lb_filter` export.
- `Shape` maps to the `cc_lb_shape` export.

`WireSchema` exposes a canonical descriptor and BLAKE3 fingerprint for a hook
wire type. This crate's build script generates the impl for every wire type from
its canonical descriptor, and `#[cc_lb_plugin]` embeds the fingerprint of each
handler's request type in the plugin. V1 descriptors are frozen for compatibility. V2 descriptors are generated from
Rust declarations, including reachable nested structs, field order, and rkyv
attributes. Unsupported reachable enums, aliases, types, and layout attributes
fail the build rather than silently escaping the fingerprint. Nested field edits
also change the V2 admission fingerprint. Plugin
authors do not manage hashes manually.

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
- `cc-lb-pdk-wasmtime-macros`: macro implementation for `#[cc_lb_plugin]` and
  `#[handler]`.
- `cc-lb-plugin-conformance`: in-process ABI and admission test harness.

## Links

- Main repository: <https://github.com/isac322/cc-lb>
- Plugin author guide: <https://github.com/isac322/cc-lb/blob/master/docs/plugin-author-guide.md>
- PDK README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-pdk-wasmtime/README.md>

## License

Licensed under the workspace license.

Filter V2 supplies `routing_now_unix_secs` from the injected host clock for each
routing pass, separately from measurement timestamps. Quota windows include
current known labels with unobserved entries; match labels rather than positions
and accept future unknown labels. This projection omits policy thresholds, source
metadata, overage/fallback decisions, and extra usage.
