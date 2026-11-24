//! Isolated OpenAI provider adapter implementing [`ProviderAdapter`].

#![deny(unsafe_code)]
#![warn(missing_docs)]

use std::borrow::Cow;
use std::sync::LazyLock;

use bytes::Bytes;
use cc_lb_domain::Provider;
use cc_lb_provider_spi::{
    AuthKindDescriptor, AuthKindId, AuthMode, ErrorFormatRequest, ProviderAdapter, ProviderId,
};
use serde_json::json;

/// Default OpenAI Platform API base URL.
pub const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/";
/// Default OpenAI Codex OAuth proxy backend base URL.
pub const DEFAULT_OPENAI_CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";

static AUTH_KINDS: LazyLock<[AuthKindDescriptor; 2]> = LazyLock::new(|| {
    [
        AuthKindDescriptor {
            id: AuthKindId::OPENAI_API_KEY,
            display_name: Cow::Borrowed("OpenAI API Key"),
            auth_mode: AuthMode::ApiKey,
            metric_label: Cow::Borrowed("openai_api_key"),
            default_base_url: Cow::Borrowed(DEFAULT_OPENAI_BASE_URL),
            default_control_base_url: None,
            default_preflight_base_url: None,
            requires_vertex_config: false,
        },
        AuthKindDescriptor {
            id: AuthKindId::OPENAI_OAUTH,
            display_name: Cow::Borrowed("OpenAI OAuth"),
            auth_mode: AuthMode::Oauth,
            metric_label: Cow::Borrowed("openai_oauth"),
            default_base_url: Cow::Borrowed(DEFAULT_OPENAI_CODEX_BASE_URL),
            default_control_base_url: Some(Cow::Borrowed(DEFAULT_OPENAI_BASE_URL)),
            default_preflight_base_url: None,
            requires_vertex_config: false,
        },
    ]
});

/// Isolated [`ProviderAdapter`] implementation for OpenAI.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenaiProviderAdapter;

impl ProviderAdapter for OpenaiProviderAdapter {
    fn provider_id(&self) -> ProviderId {
        ProviderId::OPENAI
    }

    fn display_name(&self) -> &str {
        "OpenAI"
    }

    fn wire_provider(&self) -> Option<Provider> {
        Some(Provider::Openai)
    }

    fn auth_kinds(&self) -> &[AuthKindDescriptor] {
        AUTH_KINDS.as_slice()
    }

    fn format_error_body(&self, req: ErrorFormatRequest<'_>) -> Bytes {
        let mut error = json!({
            "message": req.message,
            "type": req.error_type,
            "code": req.error_type,
            "param": null,
        });
        if let Some(limit_kind) = req.limit_kind {
            error["limit_kind"] = json!(limit_kind);
        }
        Bytes::from(json!({"error": error}).to_string())
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use serde_json::Value;

    use super::*;

    #[test]
    fn preserves_codex_runtime_vs_platform_control_urls() {
        let adapter = OpenaiProviderAdapter;
        assert_eq!(adapter.provider_id(), ProviderId::OPENAI);
        assert_eq!(adapter.wire_provider(), Some(Provider::Openai));
        let api_key = &adapter.auth_kinds()[0];
        let oauth = &adapter.auth_kinds()[1];
        assert_eq!(api_key.default_base_url, DEFAULT_OPENAI_BASE_URL);
        assert_eq!(api_key.control_base_url(), DEFAULT_OPENAI_BASE_URL);
        assert_eq!(oauth.default_base_url, DEFAULT_OPENAI_CODEX_BASE_URL);
        assert_eq!(oauth.control_base_url(), DEFAULT_OPENAI_BASE_URL);
        assert_eq!(oauth.preflight_base_url(), DEFAULT_OPENAI_BASE_URL);
        assert_eq!(adapter.canonical_model_id("gpt-5"), "gpt-5");
    }

    #[test]
    fn formats_openai_error_envelope() {
        let adapter = OpenaiProviderAdapter;
        let payload: Value =
            serde_json::from_slice(&adapter.format_error_body(ErrorFormatRequest {
                status: StatusCode::TOO_MANY_REQUESTS,
                error_type: "rate_limit_exceeded",
                message: "limit hit",
                limit_kind: Some("tokens"),
            }))
            .expect("json");
        assert_eq!(payload["error"]["message"], "limit hit");
        assert_eq!(payload["error"]["type"], "rate_limit_exceeded");
        assert_eq!(payload["error"]["code"], "rate_limit_exceeded");
        assert!(payload["error"]["param"].is_null());
        assert_eq!(payload["error"]["limit_kind"], "tokens");
    }
}
