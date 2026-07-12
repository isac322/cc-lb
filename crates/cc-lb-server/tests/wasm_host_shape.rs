use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime};
use cc_lb_server::wasm_host::WasmtimeUpstreamDialect;
use cc_lb_upstream::{DialectShapeContext, shape_request};
use http::{HeaderMap, HeaderName, HeaderValue, Method};

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
        Err(error) => {
            eprintln!(
                "skipping wasmtime-shape-passthrough e2e: wasm artifact missing at {} ({error})",
                path.display(),
            );
            None
        }
    }
}

#[test]
fn shape_adapter_when_wasm_passthrough_then_shapes_and_strips_auth_header() {
    // Given: a real PDK-built shape plugin and a request that carries credentials.
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
    let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
        slot,
        runtime.config_arc(),
    ));
    let dialect = WasmtimeUpstreamDialect::new(dispatch);
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        HeaderName::from_static("x-trace-id"),
        HeaderValue::from_static("abc-123"),
    );
    headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer secret"),
    );
    let context = DialectShapeContext {
        request_id: "req-shape".to_owned(),
        downstream_headers: headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: Some("stream=true".to_owned()),
        body_bytes: Bytes::from_static(b"{\"prompt\":\"hi\"}"),
    };
    let principal = Principal {
        id: "tenant-shape".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };
    let upstream = Upstream::AnthropicDirect {
        base_url: Some(url::Url::parse("https://example.test").expect("fixture URL")),
    };

    // When: the server-owned adapter shapes the request through wasm.
    let shaped = shape_request(&dialect, &context, &upstream, &principal).expect("shape OK");

    // Then: the guest shape is retained while host credential policy is enforced.
    assert_eq!(
        shaped.url().as_str(),
        "https://example.test/v1/messages?stream=true"
    );
    assert_eq!(shaped.method(), &Method::POST);
    assert_eq!(shaped.body().as_ref(), b"{\"prompt\":\"hi\"}");
    assert_eq!(
        shaped.headers().get(http::header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("application/json")),
    );
    assert_eq!(
        shaped.headers().get("x-trace-id"),
        Some(&HeaderValue::from_static("abc-123")),
    );
    assert!(shaped.headers().get(http::header::AUTHORIZATION).is_none());
}
