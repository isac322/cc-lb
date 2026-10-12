//! One bounded probe request, plus the response readers shared by the
//! inference probes and the org Admin API probes.
//!
//! Every probe runs on [`AnthropicOrgApi::http`], which never follows
//! redirects, under a per-request deadline that also covers the body read,
//! and with the body capped at [`ProbeLimits::max_body_bytes`].
//!
//! [`ProbeLimits::max_body_bytes`]: super::client::ProbeLimits

use std::time::Duration;

use bytes::Bytes;
use http::header::{CONTENT_TYPE, HeaderMap};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use serde::Deserialize;
use url::Url;

use super::client::AnthropicOrgApi;
use super::workspace_id::validate_workspace_id;

/// `anthropic-version` sent on every probe.
pub(crate) const ANTHROPIC_VERSION: &str = "2023-06-01";
/// Request and response header naming the workspace a request acts in.
pub(crate) const WORKSPACE_HEADER: &str = "anthropic-workspace-id";
/// Longest provider text kept in `last_error`, in characters.
pub(crate) const MAX_DETAIL_CHARS: usize = 200;

/// Phrases that mark Anthropic's identity-linked 400. The live API says
/// "This API key is not scoped to a workspace, so this request must include
/// the anthropic-workspace-id header …"; older wording said
/// "anthropic-workspace-id is required". Any of them, together with the
/// header name, identifies the response.
const IDENTITY_LINKED_PHRASES: [&str; 3] = [
    "not scoped to a workspace",
    "must include the anthropic-workspace-id header",
    "anthropic-workspace-id is required",
];

/// One outbound probe.
pub(crate) struct ProbeRequest<'a> {
    pub method: Method,
    pub url: Url,
    pub key: &'a str,
    pub workspace_id: Option<&'a str>,
    /// JSON body; `None` sends an empty body.
    pub json_body: Option<Bytes>,
}

/// A fully read probe response.
#[derive(Debug)]
pub(crate) struct ProbeResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl ProbeResponse {
    /// The `anthropic-workspace-id` response header, when it is a valid
    /// workspace id that echoes no fragment of `key`. Anything else is
    /// provider metadata that must not reach responses or storage.
    pub fn resolved_workspace(&self, key: &str) -> Option<&str> {
        self.headers
            .get(WORKSPACE_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| validate_workspace_id(value) && !echoes_key(key, value))
    }
}

/// True when `text` contains `key` or a protected fragment of it.
pub(crate) fn echoes_key(key: &str, text: &str) -> bool {
    matches!(
        cc_lb_upstream::scrub_secret(key, text),
        std::borrow::Cow::Owned(_)
    )
}

/// Why a probe produced no response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeFailure {
    /// No complete response within the deadline.
    Timeout,
    /// The connection or request failed.
    Unreachable,
    /// The body exceeded the cap.
    TooLarge,
}

impl ProbeFailure {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Timeout => "timed out",
            Self::Unreachable => "connection failed",
            Self::TooLarge => "response body too large",
        }
    }
}

/// Sends `request` and reads the whole bounded body within `timeout`.
pub(crate) async fn send(
    api: &AnthropicOrgApi,
    timeout: Duration,
    request: ProbeRequest<'_>,
) -> Result<ProbeResponse, ProbeFailure> {
    if timeout.is_zero() {
        return Err(ProbeFailure::Timeout);
    }
    let mut builder = Request::builder()
        .method(request.method)
        .uri(request.url.as_str())
        .header("x-api-key", request.key)
        .header("anthropic-version", ANTHROPIC_VERSION);
    if let Some(workspace_id) = request.workspace_id {
        builder = builder.header(WORKSPACE_HEADER, workspace_id);
    }
    let body = match request.json_body {
        Some(body) => {
            builder = builder.header(CONTENT_TYPE, "application/json");
            body
        }
        None => Bytes::new(),
    };
    let request = builder
        .body(Full::new(body))
        .map_err(|_| ProbeFailure::Unreachable)?;
    let max_body_bytes = api.limits().max_body_bytes;
    let exchange = async {
        let response = api
            .http()
            .request(request)
            .await
            .map_err(|_| ProbeFailure::Unreachable)?;
        let (parts, body) = response.into_parts();
        let body = Limited::new(body, max_body_bytes)
            .collect()
            .await
            .map_err(|error| {
                if error.downcast_ref::<LengthLimitError>().is_some() {
                    ProbeFailure::TooLarge
                } else {
                    ProbeFailure::Unreachable
                }
            })?
            .to_bytes();
        Ok(ProbeResponse {
            status: parts.status,
            headers: parts.headers,
            body,
        })
    };
    tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| ProbeFailure::Timeout)?
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: ErrorDetail,
}

#[derive(Deserialize)]
struct ErrorDetail {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    message: String,
}

/// `(error.type, error.message)` of an Anthropic error body.
fn provider_error(body: &[u8]) -> Option<(String, String)> {
    serde_json::from_slice::<ErrorEnvelope>(body)
        .ok()
        .map(|envelope| (envelope.error.kind, envelope.error.message))
}

/// True for Anthropic's identity-linked 400: an `invalid_request_error`
/// that names the `anthropic-workspace-id` header and says the key needs a
/// workspace. A rejected header *value* ("must be a valid workspace ID") is
/// not identity-linked; see [`is_malformed_workspace`].
pub(crate) fn is_identity_linked(status: StatusCode, body: &[u8]) -> bool {
    status == StatusCode::BAD_REQUEST
        && provider_error(body).is_some_and(|(kind, message)| {
            kind == "invalid_request_error"
                && message.contains(WORKSPACE_HEADER)
                && !message.contains("valid workspace")
                && IDENTITY_LINKED_PHRASES
                    .iter()
                    .any(|phrase| message.contains(phrase))
        })
}

/// True for a 400 that rejects the `anthropic-workspace-id` value itself.
pub(crate) fn is_malformed_workspace(status: StatusCode, body: &[u8]) -> bool {
    status == StatusCode::BAD_REQUEST
        && provider_error(body).is_some_and(|(kind, message)| {
            kind == "invalid_request_error"
                && message.contains(WORKSPACE_HEADER)
                && message.contains("valid workspace")
        })
}

/// Operator-safe description of a response: the provider message (or the
/// status alone), with every fragment of `key` masked, at most
/// [`MAX_DETAIL_CHARS`] characters. Never the raw body or headers.
pub(crate) fn response_detail(key: &str, response: &ProbeResponse) -> String {
    let status = response.status.as_u16();
    let text = match provider_error(&response.body) {
        Some((_, message)) if !message.is_empty() => format!("HTTP {status}: {message}"),
        _ if response.status.is_redirection() => {
            format!("HTTP {status}: redirect not followed")
        }
        _ => format!("HTTP {status}"),
    };
    scrubbed(key, &text)
}

/// `text` with every fragment of `key` masked, at most
/// [`MAX_DETAIL_CHARS`] characters.
pub(crate) fn scrubbed(key: &str, text: &str) -> String {
    cc_lb_upstream::scrub_secret(key, text)
        .chars()
        .take(MAX_DETAIL_CHARS)
        .collect()
}

#[derive(Deserialize)]
struct ModelList {
    data: Vec<ModelEntry>,
}

#[derive(Deserialize)]
struct ModelEntry {
    id: String,
}

/// First `claude-*` id of a `/v1/models` page.
pub(crate) fn first_claude_model(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<ModelList>(body)
        .ok()?
        .data
        .into_iter()
        .map(|model| model.id)
        .find(|id| id.starts_with("claude-"))
}

/// True when a `count_tokens` 2xx body is the documented shape.
pub(crate) fn is_token_count(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("input_tokens").and_then(|tokens| tokens.as_u64()))
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_body(kind: &str, message: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "type": "error",
            "error": {"type": kind, "message": message}
        }))
        .unwrap_or_default()
    }

    /// WS-03: the identity-linked detector matches type plus phrase only.
    #[test]
    fn ws_03_identity_detector_robustness() {
        let reworded = error_body(
            "invalid_request_error",
            "Heads up: anthropic-workspace-id is required for identity keys (see docs).",
        );
        assert!(is_identity_linked(StatusCode::BAD_REQUEST, &reworded));

        let verbatim = error_body(
            "invalid_request_error",
            "anthropic-workspace-id is required when authenticating with an identity-linked API key: send the id of the workspace this request acts in.",
        );
        assert!(is_identity_linked(StatusCode::BAD_REQUEST, &verbatim));

        // Wording observed from api.anthropic.com on 2026-10-12 for an
        // identity-linked (`sk-ant-usr-…`) key without the header.
        let live = error_body(
            "invalid_request_error",
            "This API key is not scoped to a workspace, so this request must include the anthropic-workspace-id header with the ID of the workspace to use. Add the header, or use an API key that is scoped to a workspace.",
        );
        assert!(is_identity_linked(StatusCode::BAD_REQUEST, &live));

        let other_400 = error_body("invalid_request_error", "model: not found");
        assert!(!is_identity_linked(StatusCode::BAD_REQUEST, &other_400));

        let wrong_type = error_body("permission_error", "anthropic-workspace-id is required");
        assert!(!is_identity_linked(StatusCode::BAD_REQUEST, &wrong_type));

        assert!(!is_identity_linked(StatusCode::FORBIDDEN, &verbatim));
        assert!(!is_identity_linked(
            StatusCode::BAD_REQUEST,
            b"anthropic-workspace-id is required"
        ));
    }

    #[test]
    fn malformed_workspace_detector() {
        let body = error_body(
            "invalid_request_error",
            "anthropic-workspace-id header must be a valid workspace ID.",
        );
        assert!(is_malformed_workspace(StatusCode::BAD_REQUEST, &body));
        assert!(!is_identity_linked(StatusCode::BAD_REQUEST, &body));
    }

    #[test]
    fn detail_is_scrubbed_and_capped() {
        let key = "sk-ant-api03-SECRETSECRETSECRET-tail";
        let message = format!("bad key {key} {}", "x".repeat(400));
        let response = ProbeResponse {
            status: StatusCode::BAD_REQUEST,
            headers: HeaderMap::new(),
            body: Bytes::from(error_body("invalid_request_error", &message)),
        };
        let detail = response_detail(key, &response);
        assert!(!detail.contains("SECRETSECRET"), "{detail}");
        assert!(detail.chars().count() <= MAX_DETAIL_CHARS);
        assert!(
            detail.starts_with("HTTP 400: bad key [REDACTED]"),
            "{detail}"
        );
    }

    #[test]
    fn first_claude_model_skips_other_ids() {
        let body = br#"{"data":[{"id":"gpt-x"},{"id":"claude-live"},{"id":"claude-b"}]}"#;
        assert_eq!(first_claude_model(body).as_deref(), Some("claude-live"));
        assert_eq!(first_claude_model(br#"{"data":[{"id":"x"}]}"#), None);
        assert_eq!(first_claude_model(b"<html>"), None);
    }
}
