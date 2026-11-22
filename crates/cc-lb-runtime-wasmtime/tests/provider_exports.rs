use cc_lb_plugin_wire::schema::{HookKind, ProviderFamily, WireSchema};
use cc_lb_plugin_wire::v2::{
    FilterResponse, ShapeRequest, ShapeResponse, TransformRealtimeFrameRequest,
    TransformRealtimeFrameResult, TransformResponseRequest, TransformResponseResult,
    TransformSseEventRequest, TransformSseEventResult,
};
use cc_lb_plugin_wire::v3::FilterRequest;
use cc_lb_runtime_wasmtime::{
    RuntimeSlotKey, RuntimeSlotRole, WasmPluginWireDispatch, WasmtimeRuntime,
};
use rkyv::rancor::Error as RkyvError;

fn append_leb(bytes: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        bytes.push(if value == 0 { byte } else { byte | 0x80 });
        if value == 0 {
            break;
        }
    }
}

fn section(bytes: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut payload = Vec::new();
    append_leb(&mut payload, name.len());
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);
    bytes.push(0);
    append_leb(bytes, payload.len());
    bytes.extend_from_slice(&payload);
}

fn shape_module(hooks: &[&str], metadata_version: u8) -> Vec<u8> {
    shape_module_with_modes(hooks, metadata_version, &[])
}

fn shape_module_with_modes(hooks: &[&str], metadata_version: u8, noop_hooks: &[&str]) -> Vec<u8> {
    let response = ShapeResponse {
        url: Box::from("https://example.test/provider"),
        method: Box::from("POST"),
        headers: Box::new([]),
        body: Box::from(&b"qualified"[..]),
    };
    let response_only = matches!(hooks, ["transform_response_openai"]);
    let sse_only = matches!(hooks, ["transform_sse_event_gemini"]);
    let frame_only = hooks
        .iter()
        .all(|hook| hook.starts_with("transform_realtime_frame_"));
    let output = if hooks.iter().any(|hook| hook.starts_with("filter")) {
        rkyv::to_bytes::<RkyvError>(&FilterResponse {
            results: Box::new([]),
        })
        .expect("encode filter")
    } else if response_only {
        rkyv::to_bytes::<RkyvError>(&TransformResponseResult::Unchanged)
            .expect("encode response transform")
    } else if sse_only {
        rkyv::to_bytes::<RkyvError>(&TransformSseEventResult::Unchanged)
            .expect("encode SSE transform")
    } else if frame_only {
        rkyv::to_bytes::<RkyvError>(&TransformRealtimeFrameResult::Unchanged)
            .expect("encode realtime frame transform")
    } else {
        rkyv::to_bytes::<RkyvError>(&response).expect("encode response")
    };
    let data: String = output.iter().map(|byte| format!("\\{byte:02x}")).collect();
    let packed = cc_lb_plugin_wire::schema::pack_ret(16, output.len() as u32);
    let exports: String = hooks
        .iter()
        .map(|hook| {
            format!(
                "(func (export \"cc_lb_{hook}\") (param i32 i32) (result i64) i64.const {})",
                packed as i64
            )
        })
        .collect();
    let mut bytes = wat::parse_str(format!(
        "(module (memory (export \"memory\") 1) (data (i32.const 16) \"{data}\") (func (export \"cc_lb_alloc\") (param i32 i32) (result i32) i32.const 8192) (func (export \"cc_lb_free\") (param i32 i32 i32)) {exports})"
    )).expect("valid fixture");
    let mut metadata = serde_json::Map::new();
    let mut kinds = std::collections::BTreeSet::new();
    for hook in hooks {
        let (kind, _) = HookKind::parse_entrypoint(hook).expect("known hook");
        kinds.insert(kind);
        let wire_version = if kind == HookKind::Filter { 3 } else { 2 };
        metadata.insert(
            (*hook).to_owned(),
            serde_json::json!({
                "wire_version": wire_version,
                "description": "provider hook", "usage": "provider hook",
                "mode": if noop_hooks.contains(hook) {
                    "noop"
                } else if matches!(kind, HookKind::Shape | HookKind::Filter)
                    || response_only
                    || sse_only
                    || frame_only
                {
                    "active"
                } else {
                    "noop"
                }
            }),
        );
    }
    for kind in kinds {
        let (fingerprint, wire_version) = match kind {
            HookKind::Shape => (<ShapeRequest as WireSchema>::FINGERPRINT, 2),
            HookKind::TransformResponse => {
                (<TransformResponseRequest as WireSchema>::FINGERPRINT, 2)
            }
            HookKind::TransformSseEvent => {
                (<TransformSseEventRequest as WireSchema>::FINGERPRINT, 2)
            }
            HookKind::Filter => (<FilterRequest as WireSchema>::FINGERPRINT, 3),
            HookKind::TransformRealtimeFrame => (
                <TransformRealtimeFrameRequest as WireSchema>::FINGERPRINT,
                2,
            ),
            HookKind::ShapeEnvelopeGemini => {
                unreachable!("fixture uses no Gemini envelope hook")
            }
        };
        section(
            &mut bytes,
            &format!("{}.v{wire_version}", kind.section_prefix()),
            &fingerprint,
        );
    }
    let metadata = serde_json::to_vec(&serde_json::json!({
        "name": "provider-test", "version": "1.0.0", "description": "provider test",
        "usage": "provider test", "hooks": metadata,
    }))
    .expect("metadata JSON");
    section(
        &mut bytes,
        &format!("cc_lb.plugin.v{metadata_version}"),
        &metadata,
    );
    bytes
}

fn decode_frame_result(bytes: &[u8]) -> TransformRealtimeFrameResult {
    let archived = rkyv::access::<
        cc_lb_plugin_wire::v2::ArchivedTransformRealtimeFrameResult,
        RkyvError,
    >(bytes)
    .expect("frame result archives");
    rkyv::deserialize::<TransformRealtimeFrameResult, RkyvError>(archived)
        .expect("frame result decodes")
}

fn dispatch(wasm: &[u8]) -> WasmPluginWireDispatch {
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
    let slot = runtime
        .register_shape(
            RuntimeSlotKey::global("provider-test", RuntimeSlotRole::Shape),
            "provider-test",
            wasm,
        )
        .expect("admitted");
    WasmPluginWireDispatch::from_slot(slot, runtime.config_arc())
}

#[test]
fn anthropic_shape_is_anthropic_only() {
    let dispatch = dispatch(&shape_module(
        &[
            "shape_anthropic",
            "transform_response_anthropic",
            "transform_sse_event_anthropic",
        ],
        2,
    ));
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_some());
    assert!(dispatch.for_provider(ProviderFamily::Openai).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Gemini).is_none());
}

#[test]
fn openai_only_shape_is_optional_for_other_families_and_calls_qualified_export() {
    let dispatch = dispatch(&shape_module(&["shape_openai"], 2));
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Gemini).is_none());
    let openai = dispatch
        .for_provider(ProviderFamily::Openai)
        .expect("OpenAI hook");
    let output = openai
        .call_shape_scoped(&[], <[u8]>::to_vec)
        .expect("qualified export called");
    assert!(!output.is_empty());
    assert!(!openai.has_response_transform());
    assert!(!openai.has_sse_event_transform());
}

#[test]
fn openai_frame_only_shape_selects_openai_and_is_provider_isolated() {
    let dispatch = dispatch(&shape_module(&["transform_realtime_frame_openai"], 2));
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Gemini).is_none());
    let openai = dispatch
        .for_provider(ProviderFamily::Openai)
        .expect("OpenAI realtime frame hook");
    assert!(openai.shape_wire_version().is_none());
    assert_eq!(
        openai.realtime_frame_wire_version(),
        Some(cc_lb_plugin_wire::schema::WireVersion::V2)
    );
    assert!(openai.has_realtime_frame_transform());
    assert!(matches!(
        decode_frame_result(
            openai
                .call_transform_realtime_frame(&[])
                .expect("qualified frame export called")
                .as_slice()
        ),
        TransformRealtimeFrameResult::Unchanged
    ));
}

#[test]
fn gemini_frame_only_shape_selects_gemini_and_is_provider_isolated() {
    let dispatch = dispatch(&shape_module(&["transform_realtime_frame_gemini"], 2));
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Openai).is_none());
    let gemini = dispatch
        .for_provider(ProviderFamily::Gemini)
        .expect("Gemini realtime frame hook");
    assert!(gemini.shape_wire_version().is_none());
    assert!(gemini.has_realtime_frame_transform());
    assert!(matches!(
        decode_frame_result(
            gemini
                .call_transform_realtime_frame(&[])
                .expect("qualified frame export called")
                .as_slice()
        ),
        TransformRealtimeFrameResult::Unchanged
    ));
}

#[test]
fn noop_frame_only_shape_is_not_selected() {
    let dispatch = dispatch(&shape_module_with_modes(
        &["transform_realtime_frame_openai"],
        2,
        &["transform_realtime_frame_openai"],
    ));
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Openai).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Gemini).is_none());
}

#[test]
fn qualified_anthropic_and_openai_shape_coexist() {
    let dispatch = dispatch(&shape_module(
        &[
            "shape_anthropic",
            "transform_response_anthropic",
            "transform_sse_event_anthropic",
            "shape_openai",
        ],
        2,
    ));
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_some());
    assert!(dispatch.for_provider(ProviderFamily::Openai).is_some());
    assert!(dispatch.for_provider(ProviderFamily::Gemini).is_none());
}

#[test]
fn qualified_response_only_plugin_is_admitted_without_a_request_hook() {
    let dispatch = dispatch(&shape_module(&["transform_response_openai"], 2));
    let openai = dispatch
        .for_provider(ProviderFamily::Openai)
        .expect("response-only OpenAI hook");
    assert!(openai.shape_wire_version().is_none());
    assert!(openai.has_response_transform());
    assert!(!openai.has_sse_event_transform());
    openai
        .call_transform_response(&[])
        .expect("qualified response export called");
}

#[test]
fn qualified_exports_reject_v1_metadata() {
    let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
    let error = runtime
        .admit_wasm_agnostic(&shape_module(&["shape_openai"], 1))
        .expect_err("v2 metadata required");
    assert!(error.to_string().contains("cc_lb.plugin.v2"));
}

#[test]
fn qualified_filter_is_provider_specific() {
    for (hooks, supported) in [
        (vec!["filter_openai"], vec![ProviderFamily::Openai]),
        (vec!["filter_anthropic"], vec![ProviderFamily::Anthropic]),
        (
            vec!["filter_anthropic", "filter_openai"],
            vec![ProviderFamily::Anthropic, ProviderFamily::Openai],
        ),
    ] {
        let runtime = WasmtimeRuntime::with_defaults().expect("runtime");
        let slot = runtime
            .register_filter(
                RuntimeSlotKey::global("filter-provider", RuntimeSlotRole::Filter),
                "filter-provider",
                &shape_module(&hooks, 2),
            )
            .expect("filter admitted");
        let dispatch = WasmPluginWireDispatch::from_slot(slot, runtime.config_arc());
        for provider in [
            ProviderFamily::Anthropic,
            ProviderFamily::Openai,
            ProviderFamily::Gemini,
        ] {
            let selected = dispatch.for_provider(provider);
            assert_eq!(selected.is_some(), supported.contains(&provider));
            if let Some(selected) = selected {
                selected
                    .call_filter_scoped(&[], |_| ())
                    .expect("selected filter export callable");
            }
        }
    }
}

#[test]
fn qualified_sse_only_plugin_has_no_request_or_buffered_response_hook() {
    let dispatch = dispatch(&shape_module(&["transform_sse_event_gemini"], 2));
    let gemini = dispatch
        .for_provider(ProviderFamily::Gemini)
        .expect("SSE-only Gemini hook");
    assert!(gemini.shape_wire_version().is_none());
    assert!(!gemini.has_response_transform());
    assert!(gemini.has_sse_event_transform());
    gemini
        .call_transform_sse_event(&[])
        .expect("qualified SSE export called");
    assert!(dispatch.for_provider(ProviderFamily::Anthropic).is_none());
    assert!(dispatch.for_provider(ProviderFamily::Openai).is_none());
}
