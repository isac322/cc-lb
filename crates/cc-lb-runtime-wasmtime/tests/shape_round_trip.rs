use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_wire::schema::ProviderFamily;
use cc_lb_plugin_wire::v2::{
    ArchivedShapeResponse, Header, Principal as WirePrincipal, ShapeRequest, ShapeResponse,
    Upstream as WireUpstream, UpstreamIdentity,
};
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime,
};
use rkyv::rancor::Error as RkyvError;
use rkyv::util::AlignedVec;

fn wasm_path() -> PathBuf {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("workspace root")
        .to_path_buf();
    workspace_root.join("target/wasm32-unknown-unknown/release/wasmtime_shape_passthrough.wasm")
}

fn load_wasm_or_skip() -> Option<Vec<u8>> {
    let path = wasm_path();
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(err) => {
            eprintln!(
                "skipping wasmtime-shape-passthrough e2e: wasm artifact missing at {} ({err}). \
                 Run `cargo build --target wasm32-unknown-unknown --release -p wasmtime-shape-passthrough` first.",
                path.display(),
            );
            None
        }
    }
}

fn fixture_wire_request() -> ShapeRequest {
    ShapeRequest {
        request_id: Box::from("req-shape"),
        method: Box::from("POST"),
        path: Box::from("/v1/messages"),
        query: Some(Box::from("stream=true")),
        headers: Box::new([
            Header {
                name: Box::from("content-type"),
                value: Box::from(b"application/json".as_slice()),
            },
            Header {
                name: Box::from("x-trace-id"),
                value: Box::from(b"abc-123".as_slice()),
            },
        ]),
        body: Box::from(b"{\"prompt\":\"hi\"}".as_slice()),
        principal: WirePrincipal {
            id: Box::from("tenant-shape"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        upstream: WireUpstream::AnthropicDirect {
            base_url: Some(Box::from("https://example.test")),
        },
        upstream_identity: UpstreamIdentity {
            provider: Box::from("anthropic"),
            credential_kind: Box::from("anthropic_api_key"),
            upstream_id: Box::from("upstream-shape"),
            name: Box::from("anthropic-direct"),
            base_url: Some(Box::from("https://example.test")),
            gemini_oauth_profile: None,
            code_assist_project_id: None,
        },
    }
}

#[test]
fn shape_passthrough_echoes_request_via_wire_dispatch() {
    let Some(wasm_bytes) = load_wasm_or_skip() else {
        return;
    };

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("test-shape", RuntimeSlotRole::Shape),
            "test-shape",
            &wasm_bytes,
        )
        .expect("register_shape OK");

    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());

    let request = fixture_wire_request();
    let in_bytes = rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode ShapeRequest");

    let out_bytes = dispatch
        .call_shape_scoped(in_bytes.as_slice(), <[u8]>::to_vec)
        .expect("shape call succeeds");

    let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
    aligned.extend_from_slice(&out_bytes);
    let archived = rkyv::access::<ArchivedShapeResponse, RkyvError>(&aligned).expect("rkyv access");
    let response: ShapeResponse =
        rkyv::deserialize::<ShapeResponse, RkyvError>(archived).expect("rkyv deserialize");

    assert_eq!(
        &*response.url,
        "https://example.test/v1/messages?stream=true"
    );
    assert_eq!(&*response.method, "POST");
    assert_eq!(&*response.body, b"{\"prompt\":\"hi\"}");

    assert!(
        response.headers.iter().any(|h| &*h.name == "content-type"),
        "content-type must be forwarded"
    );
    assert!(
        response.headers.iter().any(|h| &*h.name == "x-trace-id"),
        "x-trace-id must be forwarded"
    );
}

#[test]
fn anthropic_shape_passthrough_is_not_selected_for_other_providers() {
    let Some(wasm_bytes) = load_wasm_or_skip() else {
        return;
    };
    let runtime = WasmtimeRuntime::with_defaults().expect("engine build");
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("anthropic-provider-shape", RuntimeSlotRole::Shape),
            "anthropic-provider-shape",
            &wasm_bytes,
        )
        .expect("register fixture");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_some());
    assert!(dispatch.for_provider(ProviderFamily::Openai).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Gemini).is_none());
}
