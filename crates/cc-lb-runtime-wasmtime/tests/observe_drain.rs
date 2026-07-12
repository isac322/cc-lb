use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_wire::ObserveEvent as WireObserveEvent;
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime};
use rkyv::rancor::Error as RkyvError;

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

fn setup() -> Option<WasmPluginWireDispatch> {
    let wasm_bytes = load_wasm_or_skip()?;
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_observe(
            RuntimeSlotKey::global("test-observe"),
            "test-observe",
            &wasm_bytes,
        )
        .expect("register_observe OK");
    Some(WasmPluginWireDispatch::from_slot(
        slot,
        runtime.config_arc(),
    ))
}

fn encode_event(event: WireObserveEvent) -> Vec<u8> {
    rkyv::to_bytes::<RkyvError>(&event)
        .expect("rkyv encode ObserveEvent")
        .to_vec()
}

#[test]
fn observe_accepts_single_event_best_effort() {
    let Some(dispatch) = setup() else { return };

    dispatch
        .call_observe(&encode_event(WireObserveEvent::RequestStarted {
            request_id: Box::from("req-1"),
            downstream_user_agent: Some(Box::from("anthropic-cli/0.1")),
        }))
        .expect("first observe OK");

    dispatch
        .call_observe(&encode_event(WireObserveEvent::AuthnComplete {
            principal_id: Box::from("tenant-A"),
            principal_kind: Box::from("api_key"),
        }))
        .expect("authn observe OK");

    dispatch
        .call_observe(&encode_event(WireObserveEvent::RequestFinished {
            status: 200,
            input_tokens: Some(120),
            output_tokens: Some(42),
            cache_creation_input_tokens: Some(0),
            cache_read_input_tokens: Some(100),
            duration_ms: 350,
        }))
        .expect("finished observe OK");
}

#[test]
fn observe_drains_bounded_burst_without_error() {
    let Some(dispatch) = setup() else { return };

    let upstream_wire = cc_lb_plugin_wire::Upstream::AnthropicDirect { base_url: None };
    for batch in 0..256u64 {
        dispatch
            .call_observe(&encode_event(WireObserveEvent::Chunk {
                batch_index: batch,
                event_count: batch % 7,
                total_bytes: batch * 64,
            }))
            .expect("chunk observe OK");
        if batch % 32 == 0 {
            dispatch
                .call_observe(&encode_event(WireObserveEvent::UpstreamChosen {
                    upstream: upstream_wire.clone(),
                }))
                .expect("upstream observe OK");
        }
    }
}
