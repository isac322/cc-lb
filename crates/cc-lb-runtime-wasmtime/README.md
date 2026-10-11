# cc-lb-runtime-wasmtime

Wasmtime-based plugin runtime for [cc-lb](https://github.com/isac322/cc-lb).

Wire-only Wasmtime runtime that admits and dispatches wasm plugin bytes using
[`cc-lb-plugin-wire`](https://crates.io/crates/cc-lb-plugin-wire). Among plugin
crates, it depends only on the wire contract; guest plugins use
[`cc-lb-pdk-wasmtime`](https://crates.io/crates/cc-lb-pdk-wasmtime).

## Contents

- `admit_wasm(engine, wasm_bytes)` — full admission gate: `inspect_wasm` (static
  metadata + fingerprint check) followed by canonical sample payload probe per
  declared hook and wire version. Filter probes carry
  `service_tier = Some("priority")` on the declared V1 or V2 request.
  V2 filter probes also contain a candidate with a subscription quota snapshot.
  V1 fingerprints and layouts remain unchanged; V2 is supported only for filters.
- `inspect_wasm(kind, wasm_bytes)` — structural-only load-time verification.
- Wire-level dispatch primitives consumed by host adapters. The
  `WasmtimeFilterPlugin` and `WasmtimeUpstreamDialect` adapters live in
  `cc-lb-server/src/wasm_host/`.

Plugin authors should not depend on this crate directly. It is exposed for the
[`cc-lb-plugin-conformance`](https://crates.io/crates/cc-lb-plugin-conformance)
harness and for hosts embedding wire-level cc-lb plugin dispatch out-of-tree.

## License

See the workspace [LICENSE](https://github.com/isac322/cc-lb/blob/master/LICENSE-APACHE)
in the main repository.
