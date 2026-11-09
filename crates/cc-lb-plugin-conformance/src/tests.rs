use std::path::PathBuf;

use super::{ConformanceSuite, fixtures};
use cc_lb_plugin_wire::{
    ProviderFamily, Upstream,
    schema::{HookKind, WireSchema},
};
use rkyv::rancor::Error as RkyvError;

fn wasm_fixture_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release")
        .join(file_name)
}

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

fn custom_section(bytes: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut payload = Vec::new();
    append_leb(&mut payload, name.len());
    payload.extend_from_slice(name.as_bytes());
    payload.extend_from_slice(data);
    bytes.push(0);
    append_leb(bytes, payload.len());
    bytes.extend_from_slice(&payload);
}

fn frame_module(
    providers: &[ProviderFamily],
    result: &cc_lb_plugin_wire::v2::TransformRealtimeFrameResult,
    malformed_at_input_len: Option<usize>,
    active: bool,
) -> Vec<u8> {
    let output = rkyv::to_bytes::<RkyvError>(result).expect("encode frame fixture result");
    let data: String = output.iter().map(|byte| format!("\\{byte:02x}")).collect();
    let valid_ret = cc_lb_plugin_wire::schema::pack_ret(16, output.len() as u32) as i64;
    let malformed_ret = cc_lb_plugin_wire::schema::pack_ret(1024, 1) as i64;
    let exports: String = providers
        .iter()
        .map(|provider| {
            let suffix = provider.as_str();
            let body = match malformed_at_input_len {
                Some(len) => format!(
                    "local.get 1 i32.const {len} i32.eq \
                     if (result i64) i64.const {malformed_ret} \
                     else i64.const {valid_ret} end"
                ),
                None => format!("i64.const {valid_ret}"),
            };
            format!(
                "(func (export \"cc_lb_transform_realtime_frame_{suffix}\") \
                 (param i32 i32) (result i64) {body})"
            )
        })
        .collect();
    let mut bytes = wat::parse_str(format!(
        "(module (memory (export \"memory\") 1) \
         (data (i32.const 16) \"{data}\") \
         (data (i32.const 1024) \"\\00\") \
         (func (export \"cc_lb_alloc\") (param i32 i32) (result i32) i32.const 8192) \
         (func (export \"cc_lb_free\") (param i32 i32 i32)) {exports})"
    ))
    .expect("valid frame fixture");
    let mut hooks_json = String::from("{");
    for (index, provider) in providers.iter().enumerate() {
        if index != 0 {
            hooks_json.push(',');
        }
        let hook = HookKind::TransformRealtimeFrame.qualified_export_name(*provider);
        hooks_json.push_str(&format!(
            "\"{}\":{{\"wire_version\":2,\"description\":\"conformance realtime frame\",\"usage\":\"conformance realtime frame\",\"mode\":\"{}\"}}",
            hook.strip_prefix("cc_lb_").expect("qualified hook prefix"),
            if active { "active" } else { "noop" },
        ));
    }
    hooks_json.push('}');
    let metadata = format!(
        "{{\"name\":\"conformance-frame\",\"version\":\"1.0.0\",\"description\":\"conformance realtime frame\",\"usage\":\"conformance realtime frame\",\"hooks\":{hooks_json}}}"
    );
    custom_section(&mut bytes, "cc_lb.plugin.v2", metadata.as_bytes());
    custom_section(
        &mut bytes,
        "cc_lb.schema.transform_realtime_frame.v2",
        &<cc_lb_plugin_wire::v2::TransformRealtimeFrameRequest as WireSchema>::FINGERPRINT,
    );
    bytes
}

#[test]
fn run_sends_priority_service_tier_to_filter() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("wasmtime_filter_service_tier.wasm"))
        .expect("runtime build script produces filter service-tier fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}

#[test]
fn run_exercises_cache_aware_filter() {
    // Given
    let wasm = std::fs::read(wasm_fixture_path("cache_aware_wasmtime.wasm"))
        .expect("runtime build script produces cache-aware filter fixture");
    let suite = ConformanceSuite::for_filter(&wasm);

    // When / Then
    suite.run();
}

#[test]
fn provider_fixtures_use_native_upstreams_and_no_credentials() {
    let openai = fixtures::sample_openai_responses_shape_request();
    assert_eq!(openai.path.as_ref(), "/v1/responses");
    assert_eq!(
        openai.body.as_ref(),
        br#"{"model":"gpt-4.1","input":"probe"}"#
    );
    assert!(matches!(openai.upstream, Upstream::OpenaiDirect { .. }));
    let chat = fixtures::sample_openai_chat_completions_shape_request();
    assert_eq!(chat.path.as_ref(), "/v1/chat/completions");
    assert!(matches!(chat.upstream, Upstream::OpenaiDirect { .. }));

    let gemini = fixtures::sample_gemini_generate_content_shape_request();
    assert_eq!(
        gemini.path.as_ref(),
        "/v1beta/models/gemini-2.5-flash:generateContent"
    );
    assert!(matches!(gemini.upstream, Upstream::GeminiDirect { .. }));
    let stream = fixtures::sample_gemini_stream_generate_content_shape_request();
    assert_eq!(
        stream.path.as_ref(),
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent"
    );
    let envelope = fixtures::sample_gemini_shape_envelope_request();
    assert_eq!(envelope.upstream.provider.as_ref(), "gemini");
    assert_eq!(envelope.upstream.credential_kind.as_ref(), "gemini_oauth");
    assert_eq!(
        envelope.upstream.gemini_oauth_profile.as_deref(),
        Some("gemini_cli")
    );
    assert!(!String::from_utf8_lossy(&envelope.envelope_body).contains("token"));
}

#[test]
fn v2_filter_fixture_uses_provider_upstream_kind() {
    for (provider, expected_kind) in [
        (ProviderFamily::Anthropic, "anthropic_api_key"),
        (ProviderFamily::Openai, "openai_api_key"),
        (ProviderFamily::Gemini, "gemini_api_key"),
    ] {
        let request = fixtures::sample_filter_request_v2(provider);
        assert_eq!(request.candidates[0].kind.as_ref(), expected_kind);
    }
}

#[test]
fn v2_filter_fixture_has_no_upstream_identity() {
    let request = fixtures::sample_filter_request_v2(ProviderFamily::Openai);
    assert_eq!(request.canonical_model_id.as_ref(), "gpt-4.1");
    assert!(matches!(
        request.candidates[0].base_url.as_deref(),
        Some("https://example.test")
    ));
}

#[test]
fn v2_realtime_frame_fixture_uses_provider_native_identity_and_path() {
    for (provider, expected_path, expected_model) in [
        (ProviderFamily::Openai, "/v1/realtime", "gpt-realtime"),
        (
            ProviderFamily::Gemini,
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            "gemini-live",
        ),
    ] {
        let request = fixtures::sample_transform_realtime_frame_request_v2(provider);
        assert_eq!(request.request_path.as_ref(), expected_path);
        assert_eq!(request.canonical_model_id.as_ref(), expected_model);
        assert_eq!(
            request.upstream_identity.provider.as_ref(),
            provider.as_str()
        );
        assert_eq!(request.direction.as_ref(), "provider_to_client");
        assert_eq!(request.kind.as_ref(), "json");
    }
}

#[test]
fn frame_only_session_calls_each_typed_v2_result_variant() {
    for result in [
        cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
        cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Replace {
            kind: Box::from("text"),
            payload: Box::from(&b"replacement"[..]),
        },
        cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Drop,
    ] {
        let wasm = frame_module(&[ProviderFamily::Openai], &result, None, true);
        let decoded = ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Openai)
            .session()
            .call_transform_realtime_frame(fixtures::sample_transform_realtime_frame_request_v2(
                ProviderFamily::Openai,
            ));
        match (result, decoded) {
            (
                cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
                cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
            )
            | (
                cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Drop,
                cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Drop,
            ) => {}
            (
                cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Replace {
                    kind: expected_kind,
                    payload: expected_payload,
                },
                cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Replace { kind, payload },
            ) => {
                assert_eq!(kind, expected_kind);
                assert_eq!(payload, expected_payload);
            }
            (expected, actual) => {
                panic!("decoded unexpected frame result: {expected:?} vs {actual:?}")
            }
        }
    }
}

#[test]
fn frame_only_run_selects_openai_and_gemini_provider_exports() {
    let wasm = frame_module(
        &[ProviderFamily::Openai, ProviderFamily::Gemini],
        &cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
        None,
        true,
    );
    ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Openai).run();
    ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Gemini).run();
}

#[test]
fn frame_only_run_executes_frame_hook_after_admission_probe() {
    let request = fixtures::sample_transform_realtime_frame_request_v2(ProviderFamily::Openai);
    let request_len = rkyv::to_bytes::<RkyvError>(&request)
        .expect("encode conformance frame request")
        .len();
    let wasm = frame_module(
        &[ProviderFamily::Openai],
        &cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
        Some(request_len),
        true,
    );
    let suite = ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Openai);
    suite.assert_recognisable_by_current_host();
    let _session = suite.session();
    let panic = std::panic::catch_unwind(|| {
        suite.run();
    });
    assert!(
        panic.is_err(),
        "run must execute the post-admission frame call"
    );
}

#[test]
fn active_frame_provider_is_not_selected_for_absent_provider() {
    let wasm = frame_module(
        &[ProviderFamily::Openai],
        &cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
        None,
        true,
    );
    let panic = std::panic::catch_unwind(|| {
        ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Gemini).session();
    });
    assert!(
        panic.is_err(),
        "absent provider frame export must not be selected"
    );
}

#[test]
fn noop_frame_provider_is_not_selected_for_same_provider() {
    let wasm = frame_module(
        &[ProviderFamily::Openai],
        &cc_lb_plugin_wire::v2::TransformRealtimeFrameResult::Unchanged,
        None,
        false,
    );
    let panic = std::panic::catch_unwind(|| {
        ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Openai).session();
    });
    assert!(
        panic.is_err(),
        "noop provider frame export must not be selected"
    );
}

#[test]
fn v3_filter_fixture_round_trips_native_pricing_values() {
    let request = fixtures::sample_filter_request_v3(ProviderFamily::Openai);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&request).expect("encode V3 fixture");
    let archived =
        rkyv::access::<cc_lb_plugin_wire::v3::ArchivedFilterRequest, rkyv::rancor::Error>(&bytes)
            .expect("access V3 fixture");
    let owned: cc_lb_plugin_wire::v3::FilterRequest =
        rkyv::deserialize::<cc_lb_plugin_wire::v3::FilterRequest, rkyv::rancor::Error>(archived)
            .expect("deserialize V3 fixture");
    assert_eq!(
        owned.native_pricing.provider,
        cc_lb_plugin_wire::v3::NativeCachePricingProvider::OpenAi
    );
    assert_eq!(owned.native_pricing.rates.len(), 4);
    assert_eq!(
        owned.native_pricing.rates[2].component,
        cc_lb_plugin_wire::v3::NativeCachePricingComponent::TotalWrite
    );
    assert_eq!(
        owned.native_pricing.rates[2].lifetime,
        Some(cc_lb_plugin_wire::v3::NativeCachePricingLifetime::ThirtyMinutes)
    );
    assert_eq!(
        owned.native_pricing.rates[2].status,
        cc_lb_plugin_wire::v3::NativeCachePricingStatus::Unknown
    );
    assert_eq!(
        owned.native_pricing.rates[0].micros_per_million,
        Some(2_000_000)
    );
}

#[test]
fn prelude_exports_provider_conformance_surface() {
    use crate::prelude::*;
    let provider = ProviderFamily::Openai;
    let _ = ConformanceSuite::for_shape_provider(&[], provider);
    let _ = sample_openai_responses_shape_request();
    let _ = sample_openai_chat_completions_transform_response_request();
    let _ = sample_gemini_generate_content_transform_sse_event_request();
    let _: ShapeRequestV2 = sample_shape_request_v2(ProviderFamily::Gemini);
    let _: TransformRealtimeFrameRequestV2 =
        sample_transform_realtime_frame_request_v2(ProviderFamily::Openai);
    let _: TransformRealtimeFrameResultV2 = TransformRealtimeFrameResultV2::Unchanged;
    let _: TransformResponseResultV2 = TransformResponseResultV2::Unchanged;
    let _: TransformSseEventResultV2 = TransformSseEventResultV2::Unchanged;
}
