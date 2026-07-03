# cc-lb-runtime-wasmtime

Wasmtime-based plugin runtime for [cc-lb](https://github.com/isac322/cc-lb).

Host-side crate that admits and dispatches wasm plugins built against
[`cc-lb-plugin-wire`](https://crates.io/crates/cc-lb-plugin-wire) via the
[`cc-lb-pdk-wasmtime`](https://crates.io/crates/cc-lb-pdk-wasmtime) macro suite.

## Contents

- `admit_wasm(engine, wasm_bytes)` — full admission gate: `inspect_wasm` (static
  metadata + fingerprint check) followed by canonical sample payload probe per
  declared hook.
- `inspect_wasm(kind, wasm_bytes)` — structural-only load-time verification.
- `WasmtimeFilterPlugin` / `WasmtimeUpstreamDialect` / `WasmtimeObservabilityHookPlugin`
  — per-hook runtime adapters implementing the trait boundaries in
  [`cc-lb-plugin-api`](https://crates.io/crates/cc-lb-plugin-api).

Plugin authors should not depend on this crate directly. It is exposed for the
[`cc-lb-plugin-conformance`](https://crates.io/crates/cc-lb-plugin-conformance)
harness and for hosts embedding the cc-lb plugin runtime out-of-tree.

## License

See the workspace [LICENSE](https://github.com/isac322/cc-lb/blob/master/LICENSE-APACHE)
in the main repository.
