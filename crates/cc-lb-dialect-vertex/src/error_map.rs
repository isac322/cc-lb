use bytes::Bytes;
use http::StatusCode;
use serde_json::{Value, json};

pub fn map_vertex_error_status(error_status: &str) -> &'static str {
    match error_status {
        "INVALID_ARGUMENT" => "invalid_request_error",
        "RESOURCE_EXHAUSTED" => "rate_limit_error",
        "UNAUTHENTICATED" => "authentication_error",
        "PERMISSION_DENIED" => "permission_error",
        "NOT_FOUND" => "not_found_error",
        "INTERNAL" | "UNKNOWN" | "UNAVAILABLE" => "api_error",
        _ => "api_error",
    }
}

pub fn vertex_error_to_anthropic_json(
    status: StatusCode,
    error_status: Option<&str>,
    message: Option<&str>,
) -> Bytes {
    let mapped = error_status
        .map(map_vertex_error_status)
        .unwrap_or("api_error");
    let message = message
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback_message(status));
    Bytes::from(
        json!({ "type": "error", "error": { "type": mapped, "message": message } }).to_string(),
    )
}

pub(crate) fn normalize_vertex_error(status: StatusCode, body: &Bytes) -> Option<Bytes> {
    let parsed = serde_json::from_slice::<Value>(body).ok()?;
    let error = parsed.get("error")?;
    let error_status = error.get("status").and_then(Value::as_str);
    let message = error.get("message").and_then(Value::as_str);

    error_status
        .map(|error_status| vertex_error_to_anthropic_json(status, Some(error_status), message))
}

fn fallback_message(status: StatusCode) -> &'static str {
    if status.is_server_error() {
        "upstream api error"
    } else {
        "vertex request failed"
    }
}
