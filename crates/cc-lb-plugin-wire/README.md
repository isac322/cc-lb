# cc-lb-plugin-wire

`cc-lb-plugin-wire` is the shared wire contract between the cc-lb host and
wasm plugins.

Plugin authors use this crate for the hook request and response types, the
schema version markers, and the metadata parser that the host uses at upload
time.

## What this crate provides

- Versioned hook wire types under modules such as `cc_lb_plugin_wire::v1`.
- `WireVersion`, the enum used to name incompatible wire layouts.
- `HookKind`, the enum for `filter`, `shape`, and `observe` hooks.
- `WireSchema`, the trait implemented by derived layout fingerprints.
- `PluginMetadata` and `HookMetadata`, parsed from wasm custom sections.
- `pack_ret` and `unpack_ret`, the guest ABI return-value helpers.

## WireVersion

`WireVersion` identifies a complete rkyv layout line.

The current baseline is `WireVersion::V1`. A new version is added only when
the wire layout changes incompatibly, such as adding, removing, renaming, or
reordering fields.

Each hook has its own supported-version list in the host:

- `HOST_SUPPORTED_FILTER_VERSIONS`
- `HOST_SUPPORTED_SHAPE_VERSIONS`
- `HOST_SUPPORTED_OBSERVE_VERSIONS`

That means `filter` can gain `v2` independently from `shape` or `observe`.

## HookKind

`HookKind` names the plugin surface:

- `Filter` maps to `cc_lb_filter`.
- `Shape` maps to `cc_lb_shape`.
- `Observe` maps to `cc_lb_observe`.

The host uses the hook kind to select the required export, schema custom
section, wire version, and runtime probe payload.

## Metadata contract

Every plugin must embed a `cc_lb.plugin.v1` metadata section. The PDK macros
generate it from `#[cc_lb_plugin(...)]` and `#[handler(...)]` attributes.

The top-level `PluginMetadata` requires:

- `name`
- `version`
- `description`
- `usage`
- at least one hook entry

Each `HookMetadata` requires:

- `wire_version`
- `description`
- `usage`

The host rejects uploads with missing plugin metadata, missing hook metadata,
unknown hook names, or empty description and usage text.

## WireSchema derive

Use `#[derive(WireSchema)]` on structs and enums that form part of a wire
layout.

```rust
use cc_lb_plugin_wire::WireSchema;

#[derive(WireSchema)]
pub struct CacheDecision {
    pub upstream_id: Box<str>,
    pub decision: Box<str>,
    pub reason: Box<str>,
}
```

The derive macro walks the Rust AST, builds a canonical descriptor, and stores
`BLAKE3(descriptor)` in `WireSchema::FINGERPRINT`. The host compares the
embedded wasm fingerprint with its own expected fingerprint during admission.

## More information

See [`docs/plugin-author-guide.md`](../../docs/plugin-author-guide.md) for the
full plugin author workflow.
