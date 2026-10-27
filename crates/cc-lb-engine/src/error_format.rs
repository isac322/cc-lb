use axum::body::Body;
use bytes::Bytes;
use http::header::{CONTENT_TYPE, RETRY_AFTER};
use http::{HeaderValue, Response, StatusCode};
use serde_json::json;

use crate::provider_capability::ProviderCapability;
pub fn anthropic_error_body(error_type: &str, message: &str) -> Bytes {
    Bytes::from(
        json!({
            "type": "error",
            "error": {
                "type": error_type,
                "message": message,
            }
        })
        .to_string(),
    )
}

pub fn anthropic_error_response(
    status: StatusCode,
    error_type: &str,
    message: &str,
) -> Response<Body> {
    let mut response = Response::new(Body::from(anthropic_error_body(error_type, message)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

pub fn anthropic_error_response_with_retry_after(
    status: StatusCode,
    error_type: &str,
    message: &str,
    retry_after_secs: u64,
) -> Response<Body> {
    let mut response = anthropic_error_response(status, error_type, message);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}

pub(crate) fn provider_error_body(
    capability: ProviderCapability,
    status: StatusCode,
    error_type: &str,
    message: &str,
    limit_kind: Option<&str>,
) -> Bytes {
    let error = match capability {
        ProviderCapability::AnthropicMessages => {
            let mut error = json!({
                "type": error_type,
                "message": message,
            });
            if let Some(limit_kind) = limit_kind {
                error["limit_kind"] = json!(limit_kind);
            }
            json!({"type": "error", "error": error})
        }
        ProviderCapability::OpenaiResponses => {
            let mut error = json!({
                "message": message,
                "type": error_type,
                "code": error_type,
                "param": null,
            });
            if let Some(limit_kind) = limit_kind {
                error["limit_kind"] = json!(limit_kind);
            }
            json!({"error": error})
        }
        ProviderCapability::GeminiGenerateContent | ProviderCapability::GeminiCountTokens => {
            let mut error = json!({
                "code": status.as_u16(),
                "message": message,
                "status": gemini_status_str(status),
                "type": error_type,
            });
            if let Some(limit_kind) = limit_kind {
                error["limit_kind"] = json!(limit_kind);
            }
            json!({"error": error})
        }
    };
    Bytes::from(error.to_string())
}

pub(crate) fn provider_error_response(
    capability: ProviderCapability,
    status: StatusCode,
    error_type: &str,
    message: &str,
    limit_kind: Option<&str>,
) -> Response<Body> {
    let mut response = Response::new(Body::from(provider_error_body(
        capability, status, error_type, message, limit_kind,
    )));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

pub(crate) fn provider_error_response_with_retry_after(
    capability: ProviderCapability,
    status: StatusCode,
    error_type: &str,
    message: &str,
    limit_kind: Option<&str>,
    retry_after_secs: u64,
) -> Response<Body> {
    let mut response = provider_error_response(capability, status, error_type, message, limit_kind);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}

pub(crate) fn provider_error_body_with_retry_after(
    capability: ProviderCapability,
    status: StatusCode,
    error_type: &str,
    message: &str,
    limit_kind: Option<&str>,
    retry_after_secs: Option<u64>,
) -> Bytes {
    let mut body = provider_error_body(capability, status, error_type, message, limit_kind);
    if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&body)
        && let Some(retry_after_secs) = retry_after_secs
    {
        value["error"]["retry_after_seconds"] = json!(retry_after_secs);
        body = Bytes::from(value.to_string());
    }
    body
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
