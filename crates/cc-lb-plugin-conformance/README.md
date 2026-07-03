# cc-lb-plugin-conformance

`cc-lb-plugin-conformance` is a dev-dependency for cc-lb wasm plugin authors.

It loads a compiled wasm plugin through the same inspection, admission, and
runtime path that the host uses, then runs boundary smoke tests for the selected
hook kind.

## Add as a dev-dependency

```toml
[dev-dependencies]
cc-lb-plugin-conformance = "0.2"
```

The plugin under test should already be built for `wasm32-unknown-unknown`.

```bash
cargo build --release --target wasm32-unknown-unknown
```

## Build a suite from wasm bytes

Read the wasm artifact and construct a suite for the hook kind you implement:

```rust
use cc_lb_plugin_conformance::ConformanceSuite;

let wasm = std::fs::read(
    "target/wasm32-unknown-unknown/release/my_plugin.wasm",
)?;

let suite = ConformanceSuite::for_shape(&wasm);
```

Use `for_filter`, `for_shape`, or `for_observe` to match the exported hook.

## `assert_recognisable_by_current_host()`

`assert_recognisable_by_current_host()` runs the current host admission path:

- parse the wasm module
- verify required exports
- parse plugin and hook metadata
- verify declared wire versions are supported
- verify embedded layout fingerprints match the host expectation
- run the canonical runtime probe for the hook

This is the fastest way to prove a plugin built against published crates is
still accepted by the current host.

## `run()`

`run()` builds a live wasmtime runtime, registers the plugin, and exercises the
hook boundary with canonical sample payloads.

It verifies ABI correctness and rkyv round-trips. It does not assert your plugin
business logic, such as which upstreams should be accepted or how URLs should be
rewritten. Keep those checks in your own tests.

## Sample test

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
    ConformanceSuite::for_shape(&wasm)
        .with_plugin_name("my-plugin")
        .assert_recognisable_by_current_host();
}

#[test]
fn plugin_passes_boundary_smoke() {
    let wasm = wasm_bytes();
    ConformanceSuite::for_shape(&wasm)
        .with_plugin_name("my-plugin")
        .run();
}
```

## Deeper tests

Use `session()` when you want to call the plugin repeatedly with your own
fixtures. The session keeps the plugin registered so semantic tests do not pay a
new runtime setup cost for every assertion.

See [`docs/plugin-author-guide.md`](../../docs/plugin-author-guide.md) for the
full plugin author workflow.
