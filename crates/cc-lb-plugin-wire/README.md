# cc-lb-plugin-wire

`cc-lb-plugin-wire` is the shared wire contract between the cc-lb host and
wasm plugins. It contains the rkyv request and response types, hook and wire
version enums, layout fingerprint trait, and upload metadata schema used by the
wasmtime runtime.

Use this crate directly when writing plugin code that needs typed hook inputs
and outputs. The `cc-lb-pdk-wasmtime` crate re-exports the same wire types for
the common PDK path.

## What This Crate Provides

- Hook wire types under `cc_lb_plugin_wire::v2` (shape-family & filter response types) and `v3` (`FilterRequest` + native cache pricing context).
- `WireVersion::V2` and `V3`.
- `HookKind` for request, response, streaming, realtime frame, and Gemini envelope hooks.
- `WireSchema`, the layout fingerprint of every hook wire type.
- `PluginMetadata` and `HookMetadata` behind the `std` feature.
- `pack_ret` and `unpack_ret` helpers for guest ABI return values.

## WireVersion, HookKind, and WireSchema

`WireVersion` identifies the wire layout line. Versions are independent per
hook; `schema::host_supported_versions` defines the host admission matrix:

| Hook | Host-supported wire versions |
| --- | --- |
| `filter` | `v3` |
| `shape` | `v2` |
| `transform_response` | `v2` |
| `transform_sse_event` | `v2` |
| `transform_realtime_frame` | `v2` |
| `shape_envelope_gemini` | `v2` |

All hooks require provider-qualified metadata in `cc_lb.plugin.v2` (`filter` uses `wire = 3`; shape-family hooks use `wire = 2`). `shape_envelope_gemini` is Gemini-only and requires a declared Gemini `shape` hook.

`HookKind` names the supported plugin hooks, which map via `ProviderFamily` to provider-qualified exports:

- `Filter` -> `cc_lb_filter_{anthropic,openai,gemini}`
- `Shape` -> `cc_lb_shape_{anthropic,openai,gemini}`
- `TransformResponse` -> `cc_lb_transform_response_{anthropic,openai,gemini}`
- `TransformSseEvent` -> `cc_lb_transform_sse_event_{anthropic,openai,gemini}`
- `TransformRealtimeFrame` -> `cc_lb_transform_realtime_frame_{anthropic,openai,gemini}`
- `ShapeEnvelopeGemini` -> `cc_lb_shape_envelope_gemini`

`ProviderFamily` names the supported provider selectors:
`anthropic`, `openai`, and `gemini`. `HookKind::qualified_export_name` maps a
hook and provider to exports such as `cc_lb_shape_openai`.
`HookKind::parse_entrypoint` parses qualified metadata keys such as
`shape_openai` and the Gemini-only `shape_envelope_gemini`; unqualified keys
such as `filter` or `shape` are rejected.

`WireSchema` exposes a canonical descriptor and BLAKE3 fingerprint for a hook
wire type. This crate's build script generates the impl for every wire type
from its canonical descriptor, and `#[cc_lb_plugin]` embeds the fingerprint
once per distinct hook and wire version. Any field addition, removal, rename,
reorder, or type edit changes the descriptor and fingerprint. Plugin authors do
not manage hashes manually.

## PluginMetadata Schema

The host accepts the `cc_lb.plugin.v2` metadata section in each upload. PDK
macros generate that section from plugin and handler attributes, with
provider-qualified hook keys such as `filter_anthropic` or `shape_openai`
(plus `shape_envelope_gemini`).

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
