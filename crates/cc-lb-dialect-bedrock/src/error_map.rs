use bytes::Bytes;
use http::StatusCode;
use serde_json::{json, Value};

pub fn map_bedrock_error_type(error_type: &str) -> &'static str {
    match short_error_type(error_type) {
        "ValidationException" => "invalid_request_error",
        "ThrottlingException" => "rate_limit_error",
        "ModelStreamErrorException" => "api_error",
        "RequestTimeTooSkewed" => "authentication_error",
        "UnrecognizedClientException" => "authentication_error",
        _ => "api_error",
    }
}

pub fn bedrock_error_to_anthropic_json(
    status: StatusCode,
    error_type: Option<&str>,
    message: Option<&str>,
) -> Bytes {
    let mapped = error_type
        .map(map_bedrock_error_type)
        .unwrap_or("api_error");
    let message = message
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback_message(status));
    Bytes::from(
        json!({ "type": "error", "error": { "type": mapped, "message": message } }).to_string(),
    )
}

pub(crate) fn normalize_bedrock_error(status: StatusCode, body: &Bytes) -> Option<Bytes> {
    let parsed = serde_json::from_slice::<Value>(body).ok();
    let error_type = parsed
        .as_ref()
        .and_then(|body| body.get("__type"))
        .and_then(Value::as_str);
    let message = parsed
        .as_ref()
        .and_then(|body| body.get("message").or_else(|| body.get("Message")))
        .and_then(Value::as_str);

    if error_type.is_some() || status.is_server_error() {
        return Some(bedrock_error_to_anthropic_json(status, error_type, message));
    }

    None
}

fn short_error_type(error_type: &str) -> &str {
    error_type.rsplit(['#', '.']).next().unwrap_or(error_type)
}

fn fallback_message(status: StatusCode) -> &'static str {
    if status.is_server_error() {
        "upstream api error"
    } else {
        "bedrock request failed"
    }
}
