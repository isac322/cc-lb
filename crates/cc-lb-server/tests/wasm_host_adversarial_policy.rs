use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{Principal, PrincipalKind, Upstream};
use cc_lb_plugin_wire::ShapeResponse;
use cc_lb_plugin_wire::schema::WireSchema;
use cc_lb_runtime_wasmtime::{
    HotEngineConfig, RuntimeSlotKey, WasmPluginWireDispatch, WasmtimeRuntime,
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
            (func (export "cc_lb_shape") (param i32 i32) (result i64)
                i64.const {packed})
            (func (export "cc_lb_transform_response") (param i32 i32) (result i64)
                i64.const 0)
            (func (export "cc_lb_transform_sse_event") (param i32 i32) (result i64)
                i64.const 0))"#,
    ))
    .expect("valid fixture WAT");
    append_custom_section(
        &mut module,
        "cc_lb.schema.shape.v1",
        &<cc_lb_plugin_wire::v1::ShapeRequest as WireSchema>::FINGERPRINT,
    );
    append_custom_section(
        &mut module,
        "cc_lb.schema.transform_response.v1",
        &<cc_lb_plugin_wire::v1::TransformResponseRequest as WireSchema>::FINGERPRINT,
    );
    append_custom_section(
        &mut module,
        "cc_lb.schema.transform_sse_event.v1",
        &<cc_lb_plugin_wire::v1::TransformSseEventRequest as WireSchema>::FINGERPRINT,
    );
    append_custom_section(
        &mut module,
        "cc_lb.plugin.v1",
        br#"{"name":"origin-mismatch","version":"0.0.1","description":"origin policy fixture","usage":"testing only","hooks":{"shape":{"wire_version":1,"description":"shape","usage":"test","mode":"active"},"transform_response":{"wire_version":1,"description":"response","usage":"test","mode":"noop"},"transform_sse_event":{"wire_version":1,"description":"sse","usage":"test","mode":"noop"}}}"#,
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
            RuntimeSlotKey::global("origin-mismatch"),
            "origin-mismatch",
            &origin_mismatch_plugin(),
        )
        .expect("register shape fixture");
    let dialect = WasmtimeUpstreamDialect::new(Arc::new(WasmPluginWireDispatch::from_slot(
        slot,
        runtime.config_arc(),
    )));
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
