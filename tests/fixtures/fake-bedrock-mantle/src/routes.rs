use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::http::header::{ACCEPT, AUTHORIZATION, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::time::sleep;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Default)]
pub struct AppConfig {}

pub fn app(_config: AppConfig) -> Router {
    Router::new()
        .route("/anthropic/v1/messages", post(messages))
        .fallback(not_found)
}

async fn messages(headers: HeaderMap, body: Bytes) -> Response {
    if let Some(response) = auth_failure(&headers) {
        return response;
    }

    if let Some(response) = mode_response(&headers).await {
        return response;
    }

    let body_json = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let model = body_json
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("claude-3-5-sonnet-20241022")
        .to_owned();

    if wants_stream(&headers, &body_json) {
        return streaming_response(model);
    }

    json_response(StatusCode::OK, message_json(&model))
}

async fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "not_found_error", "route not found")
}

fn auth_failure(headers: &HeaderMap) -> Option<Response> {
    if header_starts_with(headers, "x-api-key", "sk-ant-") {
        eprintln!("fake-bedrock-mantle auth key_label=x-api-key:sk-ant-*");
        return None;
    }

    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Some(authentication_error());
    };

    if valid_fixture_authorization(value) {
        eprintln!("fake-bedrock-mantle auth key_label=AKIATEST...");
        None
    } else {
        Some(authentication_error())
    }
}

fn valid_fixture_authorization(value: &str) -> bool {
    value.starts_with("AWS4-HMAC-SHA256 ")
        && value.contains("Credential=AKIATEST")
        && value.contains("SignedHeaders=")
        && value.contains("Signature=")
        && !value.contains("Signature=bad")
        && !value.contains("Signature=mismatch")
        && !value
            .contains("Signature=0000000000000000000000000000000000000000000000000000000000000000")
}

fn authentication_error() -> Response {
    error_response(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "missing or invalid Bedrock mantle fixture credentials",
    )
}

async fn mode_response(headers: &HeaderMap) -> Option<Response> {
    let mode = headers
        .get("x-fake-mode")
        .and_then(|value| value.to_str().ok());
    match mode {
        Some("401") => Some(error_response(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "forced fake unauthorized response",
        )),
        Some("429") => {
            let mut response = error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                "forced fake rate limit response",
            );
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
            Some(response)
        }
        Some("500") => Some(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            "forced fake server error response",
        )),
        Some("timeout") => {
            sleep(Duration::from_secs(60)).await;
            Some(error_response(
                StatusCode::GATEWAY_TIMEOUT,
                "timeout_error",
                "forced fake timeout response",
            ))
        }
        _ => None,
    }
}

fn header_starts_with(headers: &HeaderMap, name: &str, prefix: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.starts_with(prefix))
        .unwrap_or(false)
}

fn wants_stream(headers: &HeaderMap, body_json: &Value) -> bool {
    body_json
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || headers
            .get(ACCEPT)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.contains("text/event-stream"))
            .unwrap_or(false)
}

fn streaming_response(model: String) -> Response {
    let stream = async_stream::stream! {
        for (event_name, data) in stream_items(&model) {
            yield Ok::<_, Infallible>(Event::default().event(event_name).data(data));
        }
    };

    let response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    with_fixture_headers(response)
}

fn stream_items(model: &str) -> Vec<(&'static str, String)> {
    vec![
        (
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": "msg_mantle_fake_000000000000000000000000",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": { "input_tokens": 100, "output_tokens": 0 }
                }
            })
            .to_string(),
        ),
        (
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "fake mantle fixture response HELLO" }
            })
            .to_string(),
        ),
        (
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": { "stop_reason": "end_turn", "stop_sequence": null },
                "usage": { "input_tokens": 100, "output_tokens": 50 }
            })
            .to_string(),
        ),
        (
            "message_stop",
            json!({ "type": "message_stop" }).to_string(),
        ),
    ]
}

fn message_json(model: &str) -> Value {
    json!({
        "id": "msg_mantle_fake_000000000000000000000000",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{ "type": "text", "text": "fake mantle fixture response HELLO" }],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": { "input_tokens": 100, "output_tokens": 50 }
    })
}

fn json_response(status: StatusCode, body: Value) -> Response {
    let mut response = Json(body).into_response();
    *response.status_mut() = status;
    with_fixture_headers(response)
}

fn error_response(status: StatusCode, error_type: &str, message: &str) -> Response {
    json_response(
        status,
        json!({
            "type": "error",
            "error": { "type": error_type, "message": message }
        }),
    )
}

fn with_fixture_headers(mut response: Response) -> Response {
    let request_id = next_request_id();
    let request_id = match HeaderValue::from_str(&request_id) {
        Ok(value) => value,
        Err(_) => HeaderValue::from_static("req_fallback"),
    };
    response.headers_mut().insert("request-id", request_id);
    response.headers_mut().insert(
        "anthropic-organization-id",
        HeaderValue::from_static("org_test"),
    );
    response.headers_mut().insert(
        "anthropic-ratelimit-requests-remaining",
        HeaderValue::from_static("999"),
    );
    response
}

fn next_request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    format!("req_{nanos:x}{counter:x}")
}
