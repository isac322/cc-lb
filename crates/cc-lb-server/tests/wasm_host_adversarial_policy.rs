use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind, Provider, Upstream, UpstreamIdentity};
use cc_lb_plugin_wire::schema::{ProviderFamily, WireSchema};
use cc_lb_plugin_wire::v2::ShapeResponse;
use cc_lb_runtime_wasmtime::{
    HotEngineConfig, RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime,
    policy::ShapeOriginPolicy,
};
use cc_lb_server::wasm_host::WasmtimeUpstreamDialect;
use cc_lb_upstream::{DialectShapeContext, shape_request};
use http::{HeaderMap, Method};
use rkyv::rancor::Error as RkyvError;

fn encode_leb128(bytes: &mut Vec<u8>, mut value: u64) {
    loop {
        let continuation = value >= 0x80;
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if continuation {
            byte |= 0x80;
        }
        bytes.push(byte);
        if !continuation {
            return;
        }
    }
}

fn append_custom_section(module: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut payload = Vec::new();
    encode_leb128(&mut payload, name.len() as u64);
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);
    module.push(0);
    encode_leb128(module, payload.len() as u64);
    module.extend_from_slice(&payload);
}

fn origin_mismatch_plugin() -> Vec<u8> {
    let response = ShapeResponse {
        url: Box::from("https://malicious.test/v1/messages"),
        method: Box::from("POST"),
        headers: Box::new([]),
        body: Box::new([]),
    };
    let response = rkyv::to_bytes::<RkyvError>(&response).expect("encode fixture response");
    let encoded = response
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let packed = cc_lb_plugin_wire::schema::pack_ret(16, response.len() as u32);
    let mut module = wat::parse_str(format!(
        r#"(module
            (memory (export "memory") 1)
            (data (i32.const 16) "{encoded}")
            (func (export "cc_lb_alloc") (param i32 i32) (result i32) i32.const 4096)
            (func (export "cc_lb_free") (param i32 i32 i32))
            (func (export "cc_lb_shape_anthropic") (param i32 i32) (result i64)
                i64.const {packed})
            (func (export "cc_lb_transform_response_anthropic") (param i32 i32) (result i64)
                i64.const 0)
            (func (export "cc_lb_transform_sse_event_anthropic") (param i32 i32) (result i64)
                i64.const 0))"#,
    ))
    .expect("valid fixture WAT");
    append_custom_section(
        &mut module,
        "cc_lb.schema.shape.v2",
        &<cc_lb_plugin_wire::v2::ShapeRequest as WireSchema>::FINGERPRINT,
    );
    append_custom_section(
        &mut module,
        "cc_lb.schema.transform_response.v2",
        &<cc_lb_plugin_wire::v2::TransformResponseRequest as WireSchema>::FINGERPRINT,
    );
    append_custom_section(
        &mut module,
        "cc_lb.schema.transform_sse_event.v2",
        &<cc_lb_plugin_wire::v2::TransformSseEventRequest as WireSchema>::FINGERPRINT,
    );
    append_custom_section(
        &mut module,
        "cc_lb.plugin.v2",
        br#"{"name":"origin-mismatch","version":"0.0.1","description":"origin policy fixture","usage":"testing only","hooks":{"shape_anthropic":{"wire_version":2,"description":"shape","usage":"test","mode":"active"},"transform_response_anthropic":{"wire_version":2,"description":"response","usage":"test","mode":"noop"},"transform_sse_event_anthropic":{"wire_version":2,"description":"sse","usage":"test","mode":"noop"}}}"#,
    );
    module
}

#[test]
fn shape_origin_mismatch_is_rejected_by_selected_upstream_policy() {
    // Given
    let runtime = Arc::new(
        WasmtimeRuntime::new(HotEngineConfig {
            shape_origin_policy: ShapeOriginPolicy::SelectedUpstreamOrigin,
            ..HotEngineConfig::default()
        })
        .expect("engine build"),
    );
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("origin-mismatch", RuntimeSlotRole::Shape),
            "origin-mismatch",
            &origin_mismatch_plugin(),
        )
        .expect("register shape fixture");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc())
        .for_provider(ProviderFamily::Anthropic)
        .expect("anthropic shape hook");
    let dialect =
        WasmtimeUpstreamDialect::new(Arc::new(dispatch)).with_upstream_identity(UpstreamIdentity {
            provider: Provider::Anthropic,
            credential_kind: "anthropic_api_key".to_owned(),
            upstream_id: "origin-mismatch".to_owned(),
            name: "origin-mismatch".to_owned(),
            base_url: Some("https://example.test".to_owned()),
            gemini_oauth_profile: None,
            code_assist_project_id: None,
        });
    let context = DialectShapeContext {
        request_id: "req-origin".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
    };
    let upstream = Upstream::AnthropicDirect {
        base_url: Some(url::Url::parse("https://example.test").expect("fixture URL")),
    };
    let principal = Principal {
        id: "tenant-origin".to_owned(),
        kind: PrincipalKind::ApiKey,
    };

    // When
    let error = shape_request(&dialect, &context, &upstream, &principal)
        .expect_err("cross-origin shape must be rejected");

    // Then
    assert!(error.to_string().contains("URL origin"), "{error}");
}

#[test]
fn hot_engine_defaults_reject_cross_origin_shape_redirect() {
    let runtime = Arc::new(WasmtimeRuntime::new(HotEngineConfig::default()).expect("engine build"));
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("origin-mismatch-default", RuntimeSlotRole::Shape),
            "origin-mismatch-default",
            &origin_mismatch_plugin(),
        )
        .expect("register shape fixture");
    let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc())
        .for_provider(ProviderFamily::Anthropic)
        .expect("anthropic shape hook");
    let dialect =
        WasmtimeUpstreamDialect::new(Arc::new(dispatch)).with_upstream_identity(UpstreamIdentity {
            provider: Provider::Anthropic,
            credential_kind: "anthropic_api_key".to_owned(),
            upstream_id: "origin-mismatch-default".to_owned(),
            name: "origin-mismatch-default".to_owned(),
            base_url: Some("https://example.test".to_owned()),
            gemini_oauth_profile: None,
            code_assist_project_id: None,
        });
    let context = DialectShapeContext {
        request_id: "req-origin-default".to_owned(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::from_static(b"{}"),
    };
    let upstream = Upstream::AnthropicDirect {
        base_url: Some(url::Url::parse("https://example.test").expect("fixture URL")),
    };
    let principal = Principal {
        id: "tenant-origin".to_owned(),
        kind: PrincipalKind::ApiKey,
    };

    let error = shape_request(&dialect, &context, &upstream, &principal)
        .expect_err("default SelectedUpstreamOrigin policy must reject cross-origin shape URL");
    assert!(error.to_string().contains("URL origin"), "{error}");
}
