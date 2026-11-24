//! Isolated Google Cloud Vertex AI provider adapter implementing [`ProviderAdapter`].

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

/// Default Google Cloud Vertex AI base URL.
pub const DEFAULT_VERTEX_BASE_URL: &str = "https://aiplatform.googleapis.com/";
/// Fallback control-plane base URL when a Vertex record lacks project/location config.
pub const DEFAULT_VERTEX_CONTROL_FALLBACK_BASE_URL: &str =
    "https://generativelanguage.googleapis.com/";
/// Default Vertex preflight fallback base URL.
pub const DEFAULT_VERTEX_PREFLIGHT_BASE_URL: &str = "https://daily-cloudcode-pa.googleapis.com/";

static AUTH_KINDS: LazyLock<[AuthKindDescriptor; 2]> = LazyLock::new(|| {
    [
        AuthKindDescriptor {
            id: AuthKindId::VERTEX_API_KEY,
            display_name: Cow::Borrowed("Vertex AI API Key"),
            auth_mode: AuthMode::ApiKey,
            metric_label: Cow::Borrowed("vertex_api_key"),
            default_base_url: Cow::Borrowed(DEFAULT_VERTEX_BASE_URL),
            default_control_base_url: Some(Cow::Borrowed(DEFAULT_VERTEX_CONTROL_FALLBACK_BASE_URL)),
            default_preflight_base_url: Some(Cow::Borrowed(DEFAULT_VERTEX_PREFLIGHT_BASE_URL)),
            requires_vertex_config: true,
        },
        AuthKindDescriptor {
            id: AuthKindId::VERTEX_CREDENTIAL,
            display_name: Cow::Borrowed("Vertex AI Credential"),
            auth_mode: AuthMode::ServiceCredential,
            metric_label: Cow::Borrowed("vertex_credential"),
            default_base_url: Cow::Borrowed(DEFAULT_VERTEX_BASE_URL),
            default_control_base_url: Some(Cow::Borrowed(DEFAULT_VERTEX_CONTROL_FALLBACK_BASE_URL)),
            default_preflight_base_url: Some(Cow::Borrowed(DEFAULT_VERTEX_PREFLIGHT_BASE_URL)),
            requires_vertex_config: true,
        },
    ]
});

/// Isolated [`ProviderAdapter`] implementation for Google Cloud Vertex AI.
#[derive(Clone, Copy, Debug, Default)]
pub struct VertexProviderAdapter;

impl ProviderAdapter for VertexProviderAdapter {
    fn provider_id(&self) -> ProviderId {
        ProviderId::VERTEX
    }

    fn display_name(&self) -> &str {
        "Google Vertex AI"
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
            "status": google_rpc_status_str(req.status),
            "type": req.error_type,
        });
        if let Some(limit_kind) = req.limit_kind {
            error["limit_kind"] = json!(limit_kind);
        }
        Bytes::from(json!({"error": error}).to_string())
    }
}

fn google_rpc_status_str(status: StatusCode) -> &'static str {
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
    fn exposes_vertex_auth_kinds_with_vertex_config_flag() {
        let adapter = VertexProviderAdapter;
        assert_eq!(adapter.provider_id(), ProviderId::VERTEX);
        assert_eq!(adapter.wire_provider(), Some(Provider::Gemini));
        assert_eq!(adapter.auth_kinds().len(), 2);
        for descriptor in adapter.auth_kinds() {
            assert!(descriptor.requires_vertex_config);
            assert_eq!(descriptor.default_base_url, DEFAULT_VERTEX_BASE_URL);
            assert_eq!(
                descriptor.control_base_url(),
                DEFAULT_VERTEX_CONTROL_FALLBACK_BASE_URL
            );
            assert_eq!(
                descriptor.preflight_base_url(),
                DEFAULT_VERTEX_PREFLIGHT_BASE_URL
            );
        }
    }

    #[test]
    fn formats_google_rpc_error_envelope() {
        let adapter = VertexProviderAdapter;
        let payload: Value =
            serde_json::from_slice(&adapter.format_error_body(ErrorFormatRequest {
                status: StatusCode::SERVICE_UNAVAILABLE,
                error_type: "overloaded",
                message: "unavailable",
                limit_kind: None,
            }))
            .expect("json");
        assert_eq!(payload["error"]["code"], 503);
        assert_eq!(payload["error"]["status"], "UNAVAILABLE");
        assert_eq!(payload["error"]["message"], "unavailable");
    }
}
