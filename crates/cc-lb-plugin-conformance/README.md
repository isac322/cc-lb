# cc-lb-plugin-conformance

`cc-lb-plugin-conformance` is a development dependency for cc-lb wasm plugin
authors. It loads a compiled plugin through the same inspection, admission, and
runtime path used by the host, then runs boundary smoke tests for the exported
hook.

Use it in plugin crates to catch ABI, metadata, wire version, and layout
fingerprint drift before uploading the wasm artifact.

## Add as a Dev-Dependency

```toml
[dev-dependencies]
cc-lb-plugin-conformance = "0.2"
```

Build the plugin for `wasm32-unknown-unknown` before running the test:

```bash
cargo build --release --target wasm32-unknown-unknown
```

## `ConformanceSuite::from_wasm(bytes)`

`ConformanceSuite::from_wasm(bytes)` inspects the artifact and infers the hook
kind when the wasm exports exactly one supported hook.

```rust
use cc_lb_plugin_conformance::ConformanceSuite;

let wasm = std::fs::read(
    "target/wasm32-unknown-unknown/release/my_plugin.wasm",
)?;

let suite = ConformanceSuite::from_wasm(&wasm);
```

Use `for_filter` or `for_shape` instead when a single wasm
artifact exports multiple hooks.

## `assert_recognisable_by_current_host()`

`assert_recognisable_by_current_host()` runs the current host admission path:

- compile the wasm module with wasmtime
- verify required exports and import restrictions
- parse plugin and hook metadata
- verify declared wire versions are supported
- verify embedded layout fingerprints match the current host
- run the canonical runtime probe for each declared hook

This is the static gate to run in CI whenever the plugin or host crates change.

## `run()`

`run()` builds a live wasmtime runtime, registers the plugin, and exercises the
hook boundary with canonical sample payloads selected from the declared wire
version. Filter plugins receive a V1 request whose `service_tier` is
`Some("priority")`.

It verifies allocator exports, hook exports, and rkyv encode/decode round-trips.
It does not verify plugin business semantics such
as routing policy or URL rewriting; keep those assertions in your own tests.

## Sample Test

```rust
use cc_lb_plugin_conformance::ConformanceSuite;

fn wasm_bytes() -> Vec<u8> {
    std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/target/wasm32-unknown-unknown/release/my_plugin.wasm"
    ))
    .expect("build plugin wasm first")
}

#[test]
fn plugin_is_recognisable_by_current_host() {
    let wasm = wasm_bytes();
    ConformanceSuite::from_wasm(&wasm)
        .with_plugin_name("my-plugin")
        .assert_recognisable_by_current_host();
}

#[test]
fn plugin_passes_boundary_smoke() {
    let wasm = wasm_bytes();
    ConformanceSuite::from_wasm(&wasm)
        .with_plugin_name("my-plugin")
        .run();
}
```

## Links

- Main repository: <https://github.com/isac322/cc-lb>
- Plugin author guide: <https://github.com/isac322/cc-lb/blob/master/docs/plugin-author-guide.md>
- Wire README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-plugin-wire/README.md>
- PDK README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-pdk-wasmtime/README.md>

## License

Licensed under the workspace license.
