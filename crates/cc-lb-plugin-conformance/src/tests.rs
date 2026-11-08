use std::path::PathBuf;

use super::{ConformanceSuite, fixtures};
use cc_lb_plugin_wire::{ProviderFamily, Upstream};

fn wasm_fixture_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace root")
        .join("target/wasm32-unknown-unknown/release")
        .join(file_name)
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
fn prelude_exports_provider_conformance_surface() {
    use crate::prelude::*;
    let provider = ProviderFamily::Openai;
    let _ = ConformanceSuite::for_shape_provider(&[], provider);
    let _ = sample_openai_responses_shape_request();
    let _ = sample_openai_chat_completions_transform_response_request();
    let _ = sample_gemini_generate_content_transform_sse_event_request();
    let _: ShapeRequestV2 = sample_shape_request_v2(ProviderFamily::Gemini);
    let _: TransformResponseResultV2 = TransformResponseResultV2::Unchanged;
    let _: TransformSseEventResultV2 = TransformSseEventResultV2::Unchanged;
}
