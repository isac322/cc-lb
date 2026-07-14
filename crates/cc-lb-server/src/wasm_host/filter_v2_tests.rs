use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind};
use cc_lb_plugin_wire::WireVersion;
use cc_lb_routing::{FilterPlugin, RoutingContext};
use cc_lb_runtime_wasmtime::{RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime};
use uuid::Uuid;

use super::WasmtimeFilterPlugin;

fn wasm_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release/wasmtime_filter_v2.wasm")
}

#[test]
fn declared_filter_v2_dispatches_requested_service_tier() {
    let wasm = std::fs::read(wasm_path()).expect("build script produces filter V2 fixture");
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
    let slot = runtime
        .register_filter(
            RuntimeSlotKey::global("server-filter-v2"),
            "wasmtime-filter-v2",
            &wasm,
        )
        .expect("register filter V2");
    let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
        slot,
        runtime.config_arc(),
    ));
    assert_eq!(dispatch.filter_wire_version(), Some(WireVersion::V2));
    let plugin = WasmtimeFilterPlugin::new(dispatch, Uuid::new_v4(), "wasmtime-filter-v2");
    let context = RoutingContext {
        request_id: "req-server-v2".to_owned(),
        thread_id: None,
        requested_service_tier: Some("priority".to_owned()),
        downstream_headers: http::HeaderMap::new(),
        method: http::Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
        canonical_model_id: "claude-test".to_owned(),
        cache_pricing: cc_lb_domain::CachePricingSummary::default(),
    };
    let principal = Principal {
        id: "tenant".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };

    let output = plugin
        .filter(&context, &principal, &[])
        .expect("filter V2 dispatch");

    assert!(output.reason.contains("priority"));
}
