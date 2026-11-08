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
cc-lb-plugin-conformance = "0.4"
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

## Provider-qualified workflow

The harness supports provider-qualified hooks without changing the legacy
Anthropic path. Select `ProviderFamily::Anthropic`, `ProviderFamily::Openai`,
or `ProviderFamily::Gemini` on the suite builder before running the admission
check and boundary smoke. The selected provider resolves only its qualified
exports; Anthropic also falls back to the bare legacy exports.

For a shape slot, the explicit constructor is:

```rust
use cc_lb_plugin_conformance::prelude::*;

let suite = ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Openai);
suite.run();
```

Use `for_filter_provider` for a filter slot, or call `with_provider` on
`for_filter` / `for_shape`. The provider-specific fixture builders are
available for semantic tests, including
`sample_openai_responses_shape_request`,
`sample_openai_chat_completions_shape_request`,
`sample_gemini_generate_content_shape_request`, and
`sample_gemini_stream_generate_content_shape_request`. The corresponding
response and SSE builders use the same provider-qualified names.

Provider selection and wire version are independent. For every selected hook,
the harness reads the declared wire version and encodes and decodes the
matching contract:

- V1 keeps the existing request and response layout.
- V2 shape, buffered-response, and SSE requests include a synthetic principal,
  `Upstream`, and matching `UpstreamIdentity`.
- V2 filter requests use provider-appropriate candidates and do not include
  upstream identity, because the V2 filter schema has no such field.

Provider fixtures are network-free and use synthetic identities:

- OpenAI covers Responses and Chat Completions HTTP request/response/SSE
  shapes, including a representative `response.created` event.
- Gemini covers native `generateContent` and `streamGenerateContent` request,
  response, and streaming shapes.

For a Gemini OAuth Code Assist shape plugin, the optional
`shape_envelope_gemini` export is exercised with a `ShapeEnvelopeGeminiRequest`
fixture only when present. An absent envelope export is skipped; the required
Gemini shape boundary is still exercised. Unsupported hooks should be omitted,
not implemented as placeholder handlers.

Keep `ConformanceSuite::for_filter` and `ConformanceSuite::for_shape` for
source-compatible legacy Anthropic V1 coverage. Use the provider-selected
workflow for each OpenAI or Gemini hook you implement. Conformance proves
admission, registration, wire round-trips, and trap-free boundary calls; it
does not make provider API calls or assert plugin business semantics.

The fixture builders and provider selectors are re-exported from
`cc_lb_plugin_conformance::prelude` together with the V1/V2 request and result
types used by semantic tests.

## Links


- Main repository: <https://github.com/isac322/cc-lb>
- Plugin author guide: <https://github.com/isac322/cc-lb/blob/master/docs/plugin-author-guide.md>
- Wire README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-plugin-wire/README.md>
- PDK README: <https://github.com/isac322/cc-lb/blob/master/crates/cc-lb-pdk-wasmtime/README.md>

## License

Licensed under the workspace license.
