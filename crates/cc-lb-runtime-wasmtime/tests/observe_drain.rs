use std::sync::Arc;

use crate::support::required_wasm;
use cc_lb_plugin_wire::ObserveEvent as WireObserveEvent;
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime};
use rkyv::rancor::Error as RkyvError;

fn setup() -> WasmPluginWireDispatch {
    let wasm_bytes = required_wasm("wasmtime_observe_noop.wasm");
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_observe(
            RuntimeSlotKey::global("test-observe"),
            "test-observe",
            &wasm_bytes,
        )
        .expect("register_observe OK");
    WasmPluginWireDispatch::from_slot(slot, runtime.config_arc())
}

fn encode_event(event: WireObserveEvent) -> Vec<u8> {
    rkyv::to_bytes::<RkyvError>(&event)
        .expect("rkyv encode ObserveEvent")
        .to_vec()
}

#[test]
fn t3__observe_accepts_single_event_best_effort() {
    let dispatch = setup();

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
fn t3__observe_drains_bounded_burst_without_error() {
    let dispatch = setup();

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
