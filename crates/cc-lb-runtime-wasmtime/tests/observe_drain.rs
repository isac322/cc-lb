//! End-to-end observe hook against the `wasmtime-observe-noop`
//! fixture.
//!
//! Verifies that
//! [`WasmtimeRuntime::register_observe`][cc_lb_runtime_wasmtime::WasmtimeRuntime::register_observe]
//! → `compile_module` → `WasmtimeObservabilityHookPlugin` accepts a
//! stream of `ObserveEvent`s and returns `Ok(())` for each — covering
//! the best-effort + bounded retention contract the dispatch helper
//! enforces (output buffer skip when guest returns `(0, 0)`).
//!
//! Pre-build the wasm artifact with:
//!
//! ```text
//! cargo build --target wasm32-unknown-unknown --release \
//!     -p wasmtime-observe-noop
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_api::{ObservabilityHook, ObserveEvent, PrincipalKind, SlotKey, Upstream};
use cc_lb_runtime_wasmtime::{WasmtimeObservabilityHookPlugin, WasmtimeRuntime};
use http::StatusCode;

fn wasm_path() -> PathBuf {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .to_path_buf();
    workspace_root.join("target/wasm32-unknown-unknown/release/wasmtime_observe_noop.wasm")
}

fn load_wasm_or_skip() -> Option<Vec<u8>> {
    let path = wasm_path();
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(err) => {
            eprintln!(
                "skipping wasmtime-observe-noop e2e: wasm artifact missing at {} ({err}). \
                 Run `cargo build --target wasm32-unknown-unknown --release -p wasmtime-observe-noop` first.",
                path.display(),
            );
            None
        }
    }
}

fn setup() -> Option<WasmtimeObservabilityHookPlugin> {
    let wasm_bytes = load_wasm_or_skip()?;
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_observe(SlotKey::global("test-observe"), "test-observe", &wasm_bytes)
        .expect("register_observe OK");
    Some(WasmtimeObservabilityHookPlugin::new(
        slot,
        runtime.config_arc(),
    ))
}

#[test]
fn observe_accepts_single_event_best_effort() {
    let Some(hook) = setup() else { return };

    hook.observe(ObserveEvent::RequestStarted {
        request_id: "req-1".to_owned(),
        downstream_user_agent: Some("anthropic-cli/0.1".to_owned()),
    })
    .expect("first observe OK");

    hook.observe(ObserveEvent::AuthnComplete {
        principal_id: "tenant-A".to_owned(),
        kind: PrincipalKind::ApiKey,
    })
    .expect("authn observe OK");

    hook.observe(ObserveEvent::RequestFinished {
        status: StatusCode::OK,
        input_tokens: Some(120),
        output_tokens: Some(42),
        cache_creation_input_tokens: Some(0),
        cache_read_input_tokens: Some(100),
        duration_ms: 350,
    })
    .expect("finished observe OK");
}

#[test]
fn observe_drains_bounded_burst_without_error() {
    // Hammer the hook with a contiguous stream to exercise the
    // per-call fresh-Store instantiation `call_observe_hook` runs
    // under. If a per-call Store leaked memory or trapped mid-burst,
    // the next iteration would surface as ObservabilityError::Dropped
    // on this test thread.
    let Some(hook) = setup() else { return };

    let upstream = Upstream::AnthropicDirect { base_url: None };
    for batch in 0..256u64 {
        hook.observe(ObserveEvent::Chunk {
            batch_index: batch,
            event_count: batch as usize % 7,
            total_bytes: (batch as usize) * 64,
        })
        .expect("chunk observe OK");
        if batch % 32 == 0 {
            hook.observe(ObserveEvent::UpstreamChosen {
                upstream: upstream.clone(),
            })
            .expect("upstream observe OK");
        }
    }
}
