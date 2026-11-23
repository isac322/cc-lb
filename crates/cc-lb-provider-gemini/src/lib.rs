//! Isolated Google Gemini provider adapter implementing [`ProviderAdapter`].

#![deny(unsafe_code)]
#![warn(missing_docs)]

use std::borrow::Cow;
use std::sync::LazyLock;

use bytes::Bytes;
use cc_lb_domain::Provider;
use cc_lb_provider_spi::{
    AuthKindDescriptor, AuthKindId, AuthMode, ErrorFormatRequest, ProviderAdapter, ProviderId,
};
use http::StatusCode;
use serde_json::json;

/// Default Gemini Generative Language API base URL.
pub const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/";
/// Default Gemini OAuth Code Assist runtime base URL.
pub const DEFAULT_GEMINI_CODE_ASSIST_BASE_URL: &str =
    "https://cloudcode-pa.googleapis.com/v1internal";
/// Default Gemini OAuth preflight endpoint URL.
pub const DEFAULT_GEMINI_PREFLIGHT_BASE_URL: &str = "https://daily-cloudcode-pa.googleapis.com/";

static AUTH_KINDS: LazyLock<[AuthKindDescriptor; 2]> = LazyLock::new(|| {
    [
        AuthKindDescriptor {
            id: AuthKindId::GEMINI_API_KEY,
            display_name: Cow::Borrowed("Gemini API Key"),
            auth_mode: AuthMode::ApiKey,
            metric_label: Cow::Borrowed("gemini_api_key"),
            default_base_url: Cow::Borrowed(DEFAULT_GEMINI_BASE_URL),
            default_control_base_url: None,
            default_preflight_base_url: None,
            requires_vertex_config: false,
        },
        AuthKindDescriptor {
            id: AuthKindId::GEMINI_OAUTH,
            display_name: Cow::Borrowed("Gemini OAuth"),
            auth_mode: AuthMode::Oauth,
            metric_label: Cow::Borrowed("gemini_oauth"),
            default_base_url: Cow::Borrowed(DEFAULT_GEMINI_CODE_ASSIST_BASE_URL),
            default_control_base_url: Some(Cow::Borrowed(DEFAULT_GEMINI_BASE_URL)),
            default_preflight_base_url: Some(Cow::Borrowed(DEFAULT_GEMINI_PREFLIGHT_BASE_URL)),
            requires_vertex_config: false,
        },
    ]
});

/// Isolated [`ProviderAdapter`] implementation for Google Gemini.
#[derive(Clone, Copy, Debug, Default)]
pub struct GeminiProviderAdapter;

impl ProviderAdapter for GeminiProviderAdapter {
    fn provider_id(&self) -> ProviderId {
        ProviderId::GEMINI
    }

    fn display_name(&self) -> &str {
        "Gemini"
    }

    fn wire_provider(&self) -> Option<Provider> {
        Some(Provider::Gemini)
    }

    fn auth_kinds(&self) -> &[AuthKindDescriptor] {
        AUTH_KINDS.as_slice()
    }

    fn format_error_body(&self, req: ErrorFormatRequest<'_>) -> Bytes {
        let mut error = json!({
            "code": req.status.as_u16(),
            "message": req.message,
            "status": gemini_status_str(req.status),
            "type": req.error_type,
        });
        if let Some(limit_kind) = req.limit_kind {
            error["limit_kind"] = json!(limit_kind);
        }
        Bytes::from(json!({"error": error}).to_string())
    }
}

fn gemini_status_str(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "INVALID_ARGUMENT",
        StatusCode::UNAUTHORIZED => "UNAUTHENTICATED",
        StatusCode::FORBIDDEN => "PERMISSION_DENIED",
        StatusCode::NOT_FOUND => "NOT_FOUND",
        StatusCode::CONFLICT => "ABORTED",
        StatusCode::TOO_MANY_REQUESTS => "RESOURCE_EXHAUSTED",
        StatusCode::INTERNAL_SERVER_ERROR => "INTERNAL",
        StatusCode::NOT_IMPLEMENTED => "UNIMPLEMENTED",
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE => "UNAVAILABLE",
        StatusCode::GATEWAY_TIMEOUT => "DEADLINE_EXCEEDED",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn preserves_gemini_runtime_control_and_preflight_urls() {
        let adapter = GeminiProviderAdapter;
        assert_eq!(adapter.provider_id(), ProviderId::GEMINI);
        assert_eq!(adapter.wire_provider(), Some(Provider::Gemini));
        let api_key = &adapter.auth_kinds()[0];
        let oauth = &adapter.auth_kinds()[1];
        assert_eq!(api_key.default_base_url, DEFAULT_GEMINI_BASE_URL);
        assert_eq!(api_key.control_base_url(), DEFAULT_GEMINI_BASE_URL);
        assert_eq!(api_key.preflight_base_url(), DEFAULT_GEMINI_BASE_URL);
        assert_eq!(oauth.default_base_url, DEFAULT_GEMINI_CODE_ASSIST_BASE_URL);
        assert_eq!(oauth.control_base_url(), DEFAULT_GEMINI_BASE_URL);
        assert_eq!(
            oauth.preflight_base_url(),
            DEFAULT_GEMINI_PREFLIGHT_BASE_URL
        );
    }

    #[test]
    fn formats_gemini_rpc_error_envelope() {
        let adapter = GeminiProviderAdapter;
        let payload: Value =
            serde_json::from_slice(&adapter.format_error_body(ErrorFormatRequest {
                status: StatusCode::TOO_MANY_REQUESTS,
                error_type: "rate_limit_error",
                message: "exhausted",
                limit_kind: Some("rpm"),
            }))
            .expect("json");
        assert_eq!(payload["error"]["code"], 429);
        assert_eq!(payload["error"]["status"], "RESOURCE_EXHAUSTED");
        assert_eq!(payload["error"]["message"], "exhausted");
        assert_eq!(payload["error"]["type"], "rate_limit_error");
        assert_eq!(payload["error"]["limit_kind"], "rpm");
    }
}
