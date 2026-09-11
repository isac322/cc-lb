use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind, UpstreamCandidate, UpstreamKind};
use cc_lb_plugin_wire::{FilterResponse, PerCandidateReason};
use cc_lb_routing::{FilterPlugin, RoutingContext};
use cc_lb_runtime_wasmtime::policy::PluginWireBounds;
use cc_lb_runtime_wasmtime::{
    HotEngineConfig, RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime,
};
use cc_lb_server::wasm_host::WasmtimeFilterPlugin;
use http::{HeaderMap, HeaderValue, Method};
use rkyv::rancor::Error as RkyvError;
use uuid::Uuid;

fn append_custom_section(module: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut payload = Vec::new();
    encode_leb128(&mut payload, name.len() as u64);
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);

    module.push(0);
    encode_leb128(module, payload.len() as u64);
    module.extend_from_slice(&payload);
}

fn encode_leb128(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn wat_with_filter_sections(wat: &str, metadata_json: &[u8]) -> Vec<u8> {
    let mut module = wat::parse_str(wat).expect("valid wat");
    let fingerprint = <cc_lb_plugin_wire::v1::FilterRequest as cc_lb_plugin_wire::schema::WireSchema>::FINGERPRINT;
    append_custom_section(&mut module, "cc_lb.schema.filter.v1", &fingerprint);
    append_custom_section(&mut module, "cc_lb.plugin.v1", metadata_json);
    module
}

fn fixture_principal() -> Principal {
    Principal {
        id: "tenant-a".to_owned(),
        kind: PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    }
}

fn fixture_routing_context() -> RoutingContext {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    RoutingContext {
        request_id: "req-123".to_owned(),
        thread_id: None,
        requested_service_tier: None,
        downstream_headers: headers,
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{\"msg\":\"hi\"}"),
        canonical_model_id: "claude-fixture".to_owned(),
        cache_pricing: cc_lb_domain::CachePricingSummary::default(),
    }
}

fn fixture_candidates() -> Vec<UpstreamCandidate> {
    vec![UpstreamCandidate {
        upstream_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        name: "upstream-1".to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        observed_rate_limits: Vec::new(),
        subscription_quotas: Vec::new(),
        observed_at_unix_secs: 0,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    }]
}

#[test]
fn t3__wasmtime_filter_rejects_output_past_wire_bound() {
    let response = FilterResponse {
        results: Box::new([PerCandidateReason {
            upstream_id: Box::from("11111111-1111-1111-1111-111111111111"),
            decision: Box::from("accept"),
            reason: Box::from("aligned-or-copy"),
        }]),
    };
    let bytes = rkyv::to_bytes::<RkyvError>(&response).expect("encode");

    let packed = cc_lb_plugin_wire::schema::pack_ret(16, bytes.len() as u32);
    let mut data_str = String::new();
    for &byte in bytes.as_slice() {
        data_str.push_str(&format!("\\{byte:02x}"));
    }
    let wat = format!(
        r#"
        (module
            (memory (export "memory") 1)
            (data (i32.const 16) "{}")
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 1024)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_filter") (param i32 i32) (result i64)
                i64.const {}
            )
        )
        "#,
        data_str, packed as i64
    );
    let metadata = r#"{"name":"malicious-filter","version":"0.0.1","description":"malicious filter plugin","usage":"test usage","hooks":{"filter":{"wire_version":1,"description":"filter hook","usage":"call filter"}}}"#;
    let wasm_bytes = wat_with_filter_sections(&wat, metadata.as_bytes());

    let config = HotEngineConfig {
        wire_bounds: PluginWireBounds {
            output_body_bytes: 10,
            ..PluginWireBounds::default()
        },
        ..HotEngineConfig::default()
    };
    let runtime = Arc::new(WasmtimeRuntime::new(config).expect("engine"));
    let slot = runtime
        .register_filter(
            RuntimeSlotKey::global("oversized"),
            "oversized",
            &wasm_bytes,
        )
        .expect("register");
    let dispatch = Arc::new(WasmPluginWireDispatch::from_slot(
        slot,
        runtime.config_arc(),
    ));
    let filter_plugin = WasmtimeFilterPlugin::new(dispatch, Uuid::from_u128(1), "oversized");

    let result = filter_plugin.filter(
        &fixture_routing_context(),
        &fixture_principal(),
        &fixture_candidates(),
    );

    let error = result.expect_err("oversized Wasmtime output must be rejected");
    assert!(
        error
            .to_string()
            .contains("exceeds wire_bounds.output_body_bytes")
    );
}
