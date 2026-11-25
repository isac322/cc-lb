//! Isolated Anthropic provider adapter implementing [`ProviderAdapter`].

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

/// Default Anthropic API base URL.
pub const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";

static AUTH_KINDS: LazyLock<[AuthKindDescriptor; 2]> = LazyLock::new(|| {
    [
        AuthKindDescriptor {
            id: AuthKindId::ANTHROPIC_API_KEY,
            display_name: Cow::Borrowed("Anthropic API Key"),
            auth_mode: AuthMode::ApiKey,
            metric_label: Cow::Borrowed("anthropic_key"),
            default_base_url: Cow::Borrowed(DEFAULT_ANTHROPIC_BASE_URL),
            default_control_base_url: None,
            default_preflight_base_url: None,
            requires_vertex_config: false,
        },
        AuthKindDescriptor {
            id: AuthKindId::ANTHROPIC_OAUTH,
            display_name: Cow::Borrowed("Anthropic OAuth"),
            auth_mode: AuthMode::Oauth,
            metric_label: Cow::Borrowed("anthropic_oauth"),
            default_base_url: Cow::Borrowed(DEFAULT_ANTHROPIC_BASE_URL),
            default_control_base_url: None,
            default_preflight_base_url: None,
            requires_vertex_config: false,
        },
    ]
});

/// Isolated [`ProviderAdapter`] implementation for Anthropic.
#[derive(Clone, Copy, Debug, Default)]
pub struct AnthropicProviderAdapter;

impl ProviderAdapter for AnthropicProviderAdapter {
    fn provider_id(&self) -> ProviderId {
        ProviderId::ANTHROPIC
    }

    fn display_name(&self) -> &str {
        "Anthropic"
    }

    fn wire_provider(&self) -> Option<Provider> {
        Some(Provider::Anthropic)
    }

    fn auth_kinds(&self) -> &[AuthKindDescriptor] {
        AUTH_KINDS.as_slice()
    }

    fn canonical_model_id<'a>(&self, requested: &'a str) -> &'a str {
        canonical_anthropic_model_id(requested)
    }

    fn cache_threshold_tokens(&self, canonical: &str) -> Option<usize> {
        anthropic_cache_threshold_tokens(canonical)
    }

    fn format_error_body(&self, req: ErrorFormatRequest<'_>) -> Bytes {
        let mut error = json!({
            "type": req.error_type,
            "message": req.message,
        });
        if let Some(limit_kind) = req.limit_kind {
            error["limit_kind"] = json!(limit_kind);
        }
        Bytes::from(json!({"type": "error", "error": error}).to_string())
    }
}

fn canonical_anthropic_model_id(requested: &str) -> &str {
    match requested {
        "claude-fable-5" => "claude-fable-5",
        "claude-opus-5-5" => "claude-opus-5-5",
        "claude-opus-5" => "claude-opus-5",
        "claude-mythos-5" => "claude-mythos-5",
        "claude-mythos-preview" => "claude-mythos-preview",
        "claude-sonnet-5-5" => "claude-sonnet-5-5",
        "claude-sonnet-5" => "claude-sonnet-5",
        "claude-haiku-5-5" => "claude-haiku-5-5",
        "claude-sonnet-4-6" => "claude-sonnet-4-6",
        "claude-opus-4-6" => "claude-opus-4-6",
        "claude-opus-4-7" => "claude-opus-4-7",

        // Sonnet aliases
        "claude-sonnet-4-5" => "claude-sonnet-4-5-20250929",

        // Opus aliases
        "claude-opus-4-5" => "claude-opus-4-5-20251101",
        "claude-opus-4-8" => "claude-opus-4-8-20250514",

        // Haiku aliases
        "claude-haiku-4-5" => "claude-haiku-4-5-20251001",

        // Already-dated or unknown: pass through unchanged
        other => {
            // Only warn if it looks like an alias (doesn't end with -YYYYMMDD)
            if !other.ends_with('-') && !is_dated_model(other) {
                tracing::warn!(
                    target: "cc_lb_routing_eval::model_resolution",
                    model = %other,
                    "unknown model alias, returning unchanged"
                );
            }
            other
        }
    }
}

fn is_dated_model(model: &str) -> bool {
    let parts: Vec<&str> = model.rsplitn(2, '-').collect();
    if parts.len() == 2 {
        let suffix = parts[0];
        suffix.len() == 8 && suffix.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

fn anthropic_cache_threshold_tokens(canonical: &str) -> Option<usize> {
    match canonical {
        // Claude 5.5 (Opus/Sonnet/Haiku) / Opus 5 / Fable 5 / Mythos 5: 512
        "claude-opus-5-5" | "claude-sonnet-5-5" | "claude-haiku-5-5" | "claude-opus-5"
        | "claude-fable-5" | "claude-mythos-5" => Some(512),

        // Opus 4.8 / Sonnet 5 / Sonnet 4.6 / Sonnet 4.5 / Opus 4.1 / Opus 4 / Sonnet 4: 1024
        "claude-opus-4-8-20250514"
        | "claude-opus-4-8"
        | "claude-sonnet-5"
        | "claude-sonnet-4-6"
        | "claude-sonnet-4-5-20250929"
        | "claude-sonnet-4-5"
        | "claude-opus-4-1"
        | "claude-opus-4"
        | "claude-sonnet-4" => Some(1024),

        // Mythos Preview / Opus 4.7 / Haiku 3.5: 2048
        "claude-mythos-preview"
        | "claude-opus-4-7"
        | "claude-3-5-haiku-20241022"
        | "claude-3-5-haiku-latest" => Some(2048),

        // Opus 4.5 / 4.6 / Haiku 4.5: 4096
        "claude-opus-4-5-20251101"
        | "claude-opus-4-5"
        | "claude-opus-4-6"
        | "claude-haiku-4-5-20251001"
        | "claude-haiku-4-5" => Some(4096),

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use serde_json::Value;

    use super::*;

    #[test]
    fn exposes_both_auth_kinds() {
        let adapter = AnthropicProviderAdapter;
        assert_eq!(adapter.provider_id(), ProviderId::ANTHROPIC);
        assert_eq!(adapter.wire_provider(), Some(Provider::Anthropic));
        assert_eq!(adapter.auth_kinds().len(), 2);
        assert_eq!(adapter.auth_kinds()[0].metric_label, "anthropic_key");
        assert_eq!(adapter.auth_kinds()[1].metric_label, "anthropic_oauth");
    }

    #[test]
    fn resolves_aliases_and_thresholds() {
        let adapter = AnthropicProviderAdapter;
        assert_eq!(
            adapter.canonical_model_id("claude-sonnet-4-5"),
            "claude-sonnet-4-5-20250929"
        );
        assert_eq!(
            adapter.canonical_model_id("claude-opus-4-8"),
            "claude-opus-4-8-20250514"
        );
        assert_eq!(
            adapter.canonical_model_id("claude-haiku-4-5"),
            "claude-haiku-4-5-20251001"
        );
        assert_eq!(
            adapter.canonical_model_id("claude-opus-5-5"),
            "claude-opus-5-5"
        );
        assert_eq!(
            adapter.canonical_model_id("claude-sonnet-5-5"),
            "claude-sonnet-5-5"
        );
        assert_eq!(
            adapter.canonical_model_id("claude-haiku-5-5"),
            "claude-haiku-5-5"
        );
        assert_eq!(adapter.cache_threshold_tokens("claude-opus-5"), Some(512));
        assert_eq!(adapter.cache_threshold_tokens("claude-opus-5-5"), Some(512));
        assert_eq!(
            adapter.cache_threshold_tokens("claude-sonnet-5-5"),
            Some(512)
        );
        assert_eq!(
            adapter.cache_threshold_tokens("claude-haiku-5-5"),
            Some(512)
        );
        assert_eq!(
            adapter.cache_threshold_tokens("claude-sonnet-4-5-20250929"),
            Some(1024)
        );
        assert_eq!(
            adapter.cache_threshold_tokens("claude-3-5-haiku-20241022"),
            Some(2048)
        );
        assert_eq!(
            adapter.cache_threshold_tokens("claude-opus-4-5-20251101"),
            Some(4096)
        );
        assert_eq!(adapter.cache_threshold_tokens("unknown-model"), None);
    }

    #[test]
    fn formats_anthropic_error_envelope() {
        let adapter = AnthropicProviderAdapter;
        let plain: Value = serde_json::from_slice(&adapter.format_error_body(ErrorFormatRequest {
            status: StatusCode::TOO_MANY_REQUESTS,
            error_type: "rate_limit_error",
            message: "quota reached",
            limit_kind: None,
        }))
        .expect("json");
        assert_eq!(plain["type"], "error");
        assert_eq!(plain["error"]["type"], "rate_limit_error");
        assert_eq!(plain["error"]["message"], "quota reached");
        assert!(plain["error"].get("limit_kind").is_none());

        let with_limit: Value =
            serde_json::from_slice(&adapter.format_error_body(ErrorFormatRequest {
                status: StatusCode::TOO_MANY_REQUESTS,
                error_type: "rate_limit_error",
                message: "quota reached",
                limit_kind: Some("requests_per_minute"),
            }))
            .expect("json");
        assert_eq!(with_limit["error"]["limit_kind"], "requests_per_minute");
    }
}
