use std::sync::Arc;

use crate::support::required_wasm;
use cc_lb_plugin_wire::{
    ArchivedShapeResponse, Header, Principal, ShapeRequest, ShapeResponse, Upstream as WireUpstream,
};
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

#[test]
fn t3__scoped_shape_returns_owned_bytes_after_store_drop() {
    // Given: the real passthrough shape plugin and a valid wire request.
    let wasm_bytes = required_wasm("wasmtime_shape_passthrough.wasm");
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("scoped-shape"),
            "scoped-shape",
            &wasm_bytes,
        )
        .expect("register shape");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
    let request = ShapeRequest {
        request_id: Box::from("req-scoped"),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: None,
        headers: Box::new([Header {
            name: Box::from("content-type"),
            value: Box::from(b"application/json".as_slice()),
        }]),
        body: Box::from(b"{\"prompt\":\"hi\"}".as_slice()),
        principal: Principal {
            id: Box::from("tenant-scoped"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        upstream: WireUpstream::AnthropicDirect {
            base_url: Some(Box::from("https://example.test")),
        },
    };
    let input = rkyv::to_bytes::<RkyvError>(&request).expect("encode shape request");

    // When: the scoped callback copies the guest-memory view into owned bytes.
    let output = dispatch
        .call_shape_scoped(input.as_slice(), <[u8]>::to_vec)
        .expect("scoped shape call");

    // Then: those owned bytes remain usable after the scoped call dropped its Store.
    let mut aligned = AlignedVec::<16>::with_capacity(output.len());
    aligned.extend_from_slice(&output);
    let archived =
        rkyv::access::<ArchivedShapeResponse, RkyvError>(&aligned).expect("access response");
    let response: ShapeResponse =
        rkyv::deserialize::<ShapeResponse, RkyvError>(archived).expect("decode response");
    assert_eq!(&*response.body, b"{\"prompt\":\"hi\"}");
}
