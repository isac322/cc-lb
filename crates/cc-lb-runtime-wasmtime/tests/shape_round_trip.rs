use std::path::PathBuf;
use std::sync::Arc;

use cc_lb_plugin_wire::{
    ArchivedShapeResponse, ShapeRequest, ShapeResponse, Upstream as WireUpstream,
};
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime};
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
    use cc_lb_plugin_wire::{Header, Principal as WirePrincipal};
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
    }
}

fn provider_fixture_wire_request(upstream: WireUpstream, path: &str, body: &[u8]) -> ShapeRequest {
    use cc_lb_plugin_wire::{Header, Principal as WirePrincipal};
    ShapeRequest {
        request_id: Box::from("req-provider-shape"),
        method: Box::from("POST"),
        path: Box::from(path),
        query: None,
        headers: Box::new([Header {
            name: Box::from("content-type"),
            value: Box::from(b"application/json".as_slice()),
        }]),
        body: Box::from(body),
        principal: WirePrincipal {
            id: Box::from("tenant-shape"),
            kind: Box::from("api_key"),
            claims: Box::new([]),
        },
        upstream,
    }
}

fn dispatch_shape_request(request: ShapeRequest) -> Option<ShapeResponse> {
    let wasm_bytes = load_wasm_or_skip()?;
    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("test-shape-provider"),
            "test-shape-provider",
            &wasm_bytes,
        )
        .expect("register_shape OK");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
    let in_bytes = rkyv::to_bytes::<RkyvError>(&request).expect("rkyv encode ShapeRequest");
    let out_bytes = dispatch
        .call_shape_scoped(in_bytes.as_slice(), <[u8]>::to_vec)
        .expect("shape call succeeds");
    let mut aligned = AlignedVec::<16>::with_capacity(out_bytes.len());
    aligned.extend_from_slice(&out_bytes);
    let archived = rkyv::access::<ArchivedShapeResponse, RkyvError>(&aligned).expect("rkyv access");
    Some(rkyv::deserialize::<ShapeResponse, RkyvError>(archived).expect("rkyv deserialize"))
}

#[test]
fn shape_passthrough_echoes_request_via_wire_dispatch() {
    let Some(wasm_bytes) = load_wasm_or_skip() else {
        return;
    };

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("test-shape"),
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

    let url: &str = &response.url;
    assert_eq!(url, "https://example.test/v1/messages?stream=true");
    let method: &str = &response.method;
    assert_eq!(method, "POST");
    assert_eq!(&*response.body, b"{\"prompt\":\"hi\"}");

    let has_ct = response.headers.iter().any(|h| {
        let name: &str = &h.name;
        name == "content-type"
    });
    assert!(has_ct, "content-type must be forwarded");

    let has_trace = response.headers.iter().any(|h| {
        let name: &str = &h.name;
        name == "x-trace-id"
    });
    assert!(has_trace, "x-trace-id must be forwarded");
}

#[test]
fn shape_passthrough_round_trips_openai_direct_responses() {
    let Some(response) = dispatch_shape_request(provider_fixture_wire_request(
        WireUpstream::OpenaiDirect {
            base_url: Some(Box::from("https://api.openai.com")),
        },
        "/v1/responses",
        br#"{"model":"gpt-5","input":"hi"}"#,
    )) else {
        return;
    };

    let url: &str = &response.url;
    assert_eq!(url, "https://api.openai.com/v1/responses");
    let method: &str = &response.method;
    assert_eq!(method, "POST");
    assert_eq!(&*response.body, br#"{"model":"gpt-5","input":"hi"}"#);
}

#[test]
fn shape_passthrough_round_trips_gemini_direct_generate_content() {
    let Some(response) = dispatch_shape_request(provider_fixture_wire_request(
        WireUpstream::GeminiDirect {
            base_url: Some(Box::from("https://generativelanguage.googleapis.com")),
        },
        "/v1beta/models/gemini-2.5-pro:generateContent",
        br#"{"contents":[{"parts":[{"text":"hi"}]}]}"#,
    )) else {
        return;
    };

    let url: &str = &response.url;
    assert_eq!(
        url,
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent"
    );
    let method: &str = &response.method;
    assert_eq!(method, "POST");
    assert_eq!(
        &*response.body,
        br#"{"contents":[{"parts":[{"text":"hi"}]}]}"#
    );
}
