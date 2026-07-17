use std::time::Duration;

use axum::Json;
use axum::http::header::{AUTHORIZATION, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::modes::FakeMode;

use super::state::{AppState, with_fixture_headers};

pub(crate) async fn mode_response(mode: FakeMode, retry_after: u64) -> Option<Response> {
    match mode {
        FakeMode::Unauthorized => Some(error_response(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "forced fake unauthorized response",
        )),
        FakeMode::RateLimited | FakeMode::RateLimitedLong => {
            let message = if mode == FakeMode::RateLimitedLong {
                long_rate_limit_error_message()
            } else {
                "forced fake rate limit response".to_owned()
            };
            Some(with_retry_after(
                error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limit_error", &message),
                retry_after,
            ))
        }
        FakeMode::Overloaded => Some(with_retry_after(
            error_response(
                StatusCode::from_u16(529).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
                "overloaded_error",
                "forced fake overloaded response",
            ),
            retry_after,
        )),
        FakeMode::ServerError => Some(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            "forced fake server error response",
        )),
        FakeMode::Timeout => {
            sleep(Duration::from_secs(60)).await;
            Some(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "timeout_error",
                "forced fake timeout response",
            ))
        }
        FakeMode::Ok
        | FakeMode::Slow
        | FakeMode::TamperUnknownEvent
        | FakeMode::TruncateMidStream => None,
    }
}

pub(crate) fn auth_failure(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    state.record_last_request(headers);
    if header_matches(headers, "x-api-key", "")
        || header_starts_with(headers, "x-api-key", "sk-ant-")
        || header_starts_with(headers, "x-api-key", "sk-cclb-")
    {
        state.record_auth("x-api-key:accepted");
        return None;
    }
    if header_starts_with(headers, AUTHORIZATION.as_str(), "Bearer sk-ant-") {
        state.record_auth("authorization:bearer-sk-ant-*");
        return None;
    }
    Some(error_response(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "missing or invalid Anthropic API key",
    ))
}

pub(crate) fn wants_stream(headers: &HeaderMap, body_json: &Value) -> bool {
    body_json
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || headers
            .get("accept")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.contains("text/event-stream"))
            .unwrap_or(false)
}

pub(crate) fn content_length_too_large(headers: &HeaderMap, cap: usize) -> bool {
    headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .map(|length| length > cap)
        .unwrap_or(false)
}

pub(crate) fn model_json(id: &str) -> Value {
    json!({
        "id": id,
        "type": "model",
        "display_name": id,
        "created_at": "2026-05-20T00:00:00Z"
    })
}

pub(crate) fn file_json(id: &str) -> Value {
    json!({
        "id": id,
        "type": "file",
        "filename": "fixture.txt",
        "mime_type": "text/plain",
        "size_bytes": 12,
        "created_at": "2026-05-20T00:00:00Z",
        "downloadable": false
    })
}

pub(crate) fn json_response(status: StatusCode, body: Value) -> Response {
    let mut response = Json(body).into_response();
    *response.status_mut() = status;
    with_fixture_headers(response)
}

pub(crate) fn error_response(status: StatusCode, error_type: &str, message: &str) -> Response {
    json_response(
        status,
        json!({"type": "error", "error": {"type": error_type, "message": message}}),
    )
}

fn with_retry_after(mut response: Response, retry_after: u64) -> Response {
    let value = HeaderValue::from_str(&retry_after.to_string())
        .unwrap_or_else(|_| HeaderValue::from_static("1"));
    response.headers_mut().insert(RETRY_AFTER, value);
    response
}

fn header_starts_with(headers: &HeaderMap, name: &str, prefix: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.starts_with(prefix))
        .unwrap_or(false)
}

fn header_matches(headers: &HeaderMap, name: &str, expected: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value == expected)
        .unwrap_or(false)
}

fn long_rate_limit_error_message() -> String {
    let unbroken_token = "z".repeat(600);
    [
        "forced fake long rate limit response",
        "your organization exceeded its request quota for this rolling window",
        "opaque-diagnostic-token-follows-with-no-whitespace-so-the-drawer-must-wrap-one-continuous-run:",
        unbroken_token.as_str(),
        "context: this canonical error message is intentionally longer than the proxy's 1024-byte upstream_error_message cap",
        "so request-log QA can prove the storage and admin-API truncation boundary is enforced marker-inclusive",
        "and prove the admin-web drawer renders the whole message wrapped, selectable, and non-clipped",
        "across mobile 375, tablet 768, and desktop 1280 widths without overflowing its container",
        "trace: upstream=fake-anthropic mode=429-long window=organization scope=v1-messages retry=window-reset",
    ]
    .join("\n")
}
