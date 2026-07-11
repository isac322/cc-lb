//! End-to-end shape against the `wasmtime-shape-passthrough` fixture.
//!
//! Verifies that
//! [`WasmtimeRuntime::register_shape`][cc_lb_runtime_wasmtime::WasmtimeRuntime::register_shape]
//! → `compile_module` → `WasmtimeUpstreamDialect` round-trips a real
//! rkyv `ShapeRequest`/`ShapeResponse` through a pdk-wasmtime-built
//! guest module.
//!
//! Pre-build the wasm artifact with:
//!
//! ```text
//! cargo build --target wasm32-unknown-unknown --release \
//!     -p wasmtime-shape-passthrough
//! ```
//!
//! Tests skip (print + return) if the artifact is missing — same
//! convention as `cache_aware_wasmtime_e2e.rs`.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_plugin_api::{
    CachePricingSummary, Principal, PrincipalKind, RequestContext, SlotKey, Upstream,
};
use cc_lb_runtime_wasmtime::{WasmtimeRuntime, WasmtimeUpstreamDialect};
use cc_lb_upstream::shape_request;
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

fn fixture_request() -> RequestContext {
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
    RequestContext {
        request_id: "req-shape".to_owned(),
        thread_id: None,
        downstream_headers: headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: Some("stream=true".to_owned()),
        body_bytes: Bytes::from_static(b"{\"prompt\":\"hi\"}"),
        cache_breakpoints: Vec::new(),
        canonical_model_id: "claude-fixture".to_owned(),
        cache_pricing: CachePricingSummary::default(),
    }
}

fn fixture_principal() -> Principal {
    Principal {
        id: "tenant-shape".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    }
}

fn fixture_upstream() -> Upstream {
    Upstream::AnthropicDirect {
        base_url: Some(url::Url::parse("https://example.test").expect("fixture URL")),
    }
}

#[test]
fn shape_passthrough_echoes_request() {
    let Some(wasm_bytes) = load_wasm_or_skip() else {
        return;
    };

    let runtime = Arc::new(WasmtimeRuntime::with_defaults().expect("engine build"));
    let slot = runtime
        .register_shape(SlotKey::global("test-shape"), "test-shape", &wasm_bytes)
        .expect("register_shape OK");

    let dialect = WasmtimeUpstreamDialect::new(slot, runtime.config_arc());

    let ctx = fixture_request();
    let principal = fixture_principal();
    let upstream = fixture_upstream();

    let shaped = shape_request(
        &dialect,
        &ctx.dialect_shape_context(),
        &upstream,
        &principal,
    )
    .expect("shape OK");

    assert_eq!(
        shaped.url().as_str(),
        "https://example.test/v1/messages?stream=true"
    );
    assert_eq!(shaped.method(), &Method::POST);
    assert_eq!(shaped.body().as_ref(), b"{\"prompt\":\"hi\"}");
    let ct = shaped
        .headers()
        .get(http::header::CONTENT_TYPE)
        .expect("content-type forwarded");
    assert_eq!(ct, "application/json");
    let trace = shaped
        .headers()
        .get("x-trace-id")
        .expect("custom header forwarded");
    assert_eq!(trace, "abc-123");
    assert!(
        shaped.headers().get(http::header::AUTHORIZATION).is_none(),
        "authorization must be stripped at the host boundary"
    );
}
