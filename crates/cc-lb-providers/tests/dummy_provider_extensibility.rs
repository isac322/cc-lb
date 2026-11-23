use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_domain::{Provider, UpstreamKind};
use cc_lb_provider_spi::{
    AuthKindDescriptor, AuthKindId, AuthMode, ErrorFormatRequest, ProviderAdapter, ProviderId,
};
use cc_lb_providers::{build_builtin_provider_registry, builtin_provider_registry};
use http::StatusCode;
use serde_json::{Value, json};

struct DummyProviderAdapter {
    kinds: Vec<AuthKindDescriptor>,
}

impl DummyProviderAdapter {
    fn new() -> Self {
        Self {
            kinds: vec![AuthKindDescriptor {
                id: AuthKindId::from_static("dummy_api_key"),
                display_name: Cow::Borrowed("Dummy API Key"),
                auth_mode: AuthMode::ApiKey,
                metric_label: Cow::Borrowed("dummy_key"),
                default_base_url: Cow::Borrowed("https://dummy.example.com/v1"),
                default_control_base_url: Some(Cow::Borrowed("https://control.dummy.example.com/")),
                default_preflight_base_url: None,
                requires_vertex_config: false,
            }],
        }
    }
}

impl ProviderAdapter for DummyProviderAdapter {
    fn provider_id(&self) -> ProviderId {
        ProviderId::from_static("dummy")
    }

    fn display_name(&self) -> &str {
        "Dummy Provider"
    }

    fn auth_kinds(&self) -> &[AuthKindDescriptor] {
        &self.kinds
    }

    fn canonical_model_id<'a>(&self, requested: &'a str) -> &'a str {
        match requested {
            "dummy-fast" => "dummy-fast-20260101",
            other => other,
        }
    }

    fn cache_threshold_tokens(&self, canonical: &str) -> Option<usize> {
        match canonical {
            "dummy-fast-20260101" => Some(768),
            _ => None,
        }
    }

    fn format_error_body(&self, req: ErrorFormatRequest<'_>) -> Bytes {
        Bytes::from(
            json!({
                "dummy_error": {
                    "code": req.status.as_u16(),
                    "type": req.error_type,
                    "message": req.message,
                }
            })
            .to_string(),
        )
    }
}

#[test]
fn builtin_registry_covers_all_providers_and_upstream_kinds() {
    let registry = builtin_provider_registry();
    assert_eq!(registry.len(), 4);
    assert_eq!(
        registry
            .for_wire_provider(Provider::Anthropic)
            .expect("anthropic")
            .provider_id(),
        ProviderId::ANTHROPIC
    );
    assert_eq!(
        registry
            .for_wire_provider(Provider::Openai)
            .expect("openai")
            .provider_id(),
        ProviderId::OPENAI
    );
    assert_eq!(
        registry
            .for_wire_provider(Provider::Gemini)
            .expect("gemini")
            .provider_id(),
        ProviderId::GEMINI
    );

    let expected = [
        (
            UpstreamKind::AnthropicApiKey,
            "anthropic_key",
            "https://api.anthropic.com/",
            "https://api.anthropic.com/",
            "https://api.anthropic.com/",
        ),
        (
            UpstreamKind::AnthropicOauth,
            "anthropic_oauth",
            "https://api.anthropic.com/",
            "https://api.anthropic.com/",
            "https://api.anthropic.com/",
        ),
        (
            UpstreamKind::OpenaiApiKey,
            "openai_api_key",
            "https://api.openai.com/",
            "https://api.openai.com/",
            "https://api.openai.com/",
        ),
        (
            UpstreamKind::OpenaiOauth,
            "openai_oauth",
            "https://chatgpt.com/backend-api/codex",
            "https://api.openai.com/",
            "https://api.openai.com/",
        ),
        (
            UpstreamKind::GeminiApiKey,
            "gemini_api_key",
            "https://generativelanguage.googleapis.com/",
            "https://generativelanguage.googleapis.com/",
            "https://generativelanguage.googleapis.com/",
        ),
        (
            UpstreamKind::GeminiOauth,
            "gemini_oauth",
            "https://cloudcode-pa.googleapis.com/v1internal",
            "https://generativelanguage.googleapis.com/",
            "https://daily-cloudcode-pa.googleapis.com/",
        ),
        (
            UpstreamKind::VertexApiKey,
            "vertex_api_key",
            "https://aiplatform.googleapis.com/",
            "https://generativelanguage.googleapis.com/",
            "https://daily-cloudcode-pa.googleapis.com/",
        ),
        (
            UpstreamKind::VertexCredential,
            "vertex_credential",
            "https://aiplatform.googleapis.com/",
            "https://generativelanguage.googleapis.com/",
            "https://daily-cloudcode-pa.googleapis.com/",
        ),
    ];

    for (kind, metric, runtime_url, control_url, preflight_url) in expected {
        let descriptor = registry
            .auth_kind_for_upstream_kind(kind)
            .expect("descriptor registered");
        assert_eq!(descriptor.metric_label, metric);
        assert_eq!(descriptor.default_base_url, runtime_url);
        assert_eq!(descriptor.control_base_url(), control_url);
        assert_eq!(descriptor.preflight_base_url(), preflight_url);
        descriptor
            .default_base_url_parsed()
            .expect("runtime URL parses");
        descriptor
            .control_base_url_parsed()
            .expect("control URL parses");
        descriptor
            .preflight_base_url_parsed()
            .expect("preflight URL parses");
    }
}

#[test]
fn dummy_provider_registers_without_touching_spi_or_builtins() {
    let mut registry = build_builtin_provider_registry();
    registry
        .register(Arc::new(DummyProviderAdapter::new()))
        .expect("dummy provider registers cleanly");

    assert_eq!(registry.len(), 5);
    let dummy_id = ProviderId::from_static("dummy");
    let dummy_auth_id = AuthKindId::from_static("dummy_api_key");

    let adapter = registry.get(&dummy_id).expect("dummy adapter lookup");
    assert_eq!(adapter.display_name(), "Dummy Provider");
    let by_auth = registry
        .adapter_for_auth_kind(&dummy_auth_id)
        .expect("dummy adapter by auth kind");
    assert_eq!(by_auth.provider_id(), dummy_id);

    let descriptor = registry
        .auth_kind(&dummy_auth_id)
        .expect("dummy descriptor lookup");
    assert_eq!(descriptor.metric_label, "dummy_key");
    assert_eq!(
        descriptor.default_base_url_parsed().expect("url").as_str(),
        "https://dummy.example.com/v1"
    );
    assert_eq!(
        descriptor.control_base_url(),
        "https://control.dummy.example.com/"
    );
    assert_eq!(
        descriptor.preflight_base_url(),
        "https://control.dummy.example.com/"
    );

    assert_eq!(
        registry.canonical_model_id(&dummy_id, "dummy-fast"),
        "dummy-fast-20260101"
    );
    assert_eq!(
        registry.cache_threshold_tokens("dummy-fast-20260101"),
        Some(768)
    );

    let body = adapter.format_error_body(ErrorFormatRequest {
        status: StatusCode::BAD_GATEWAY,
        error_type: "dummy_upstream_error",
        message: "simulated failure",
        limit_kind: None,
    });
    let parsed: Value = serde_json::from_slice(&body).expect("dummy error json");
    assert_eq!(parsed["dummy_error"]["code"], 502);
    assert_eq!(parsed["dummy_error"]["type"], "dummy_upstream_error");
    assert_eq!(parsed["dummy_error"]["message"], "simulated failure");
}
