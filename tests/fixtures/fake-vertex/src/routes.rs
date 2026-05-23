use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::OriginalUri;
use axum::http::header::{AUTHORIZATION, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::time::sleep;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Default)]
pub struct AppConfig {}

pub fn app(_config: AppConfig) -> Router {
    Router::new()
        .route("/{*path}", post(vertex_predict))
        .fallback(not_found)
}

async fn vertex_predict(
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(response) = auth_failure(&headers) {
        return response;
    }

    if let Some(response) = mode_response(&headers).await {
        return response;
    }

    let Some((model, endpoint)) = parse_vertex_path(uri.path()) else {
        return vertex_error(StatusCode::NOT_FOUND, 404, "route not found", "NOT_FOUND");
    };

    let body_json = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    if body_json.get("anthropic_version").and_then(Value::as_str) != Some("vertex-2023-10-16") {
        return vertex_error(
            StatusCode::BAD_REQUEST,
            400,
            "anthropic_version must be vertex-2023-10-16",
            "INVALID_ARGUMENT",
        );
    }

    match endpoint {
        VertexEndpoint::RawPredict => json_response(StatusCode::OK, message_json(model)),
        VertexEndpoint::StreamRawPredict => streaming_response(model.to_owned()),
    }
}

async fn not_found() -> Response {
    vertex_error(StatusCode::NOT_FOUND, 404, "route not found", "NOT_FOUND")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VertexEndpoint {
    RawPredict,
    StreamRawPredict,
}

fn parse_vertex_path(path: &str) -> Option<(&str, VertexEndpoint)> {
    let suffix = path.strip_prefix("/v1/projects/")?;
    let mut segments = suffix.split('/');
    let _project = segments.next()?;
    if segments.next()? != "locations" {
        return None;
    }
    let _region = segments.next()?;
    if segments.next()? != "publishers"
        || segments.next()? != "anthropic"
        || segments.next()? != "models"
    {
        return None;
    }
    let model_and_method = segments.next()?;
    if segments.next().is_some() {
        return None;
    }
    if let Some(model) = model_and_method.strip_suffix(":rawPredict") {
        return Some((model, VertexEndpoint::RawPredict));
    }
    model_and_method
        .strip_suffix(":streamRawPredict")
        .map(|model| (model, VertexEndpoint::StreamRawPredict))
}

fn auth_failure(headers: &HeaderMap) -> Option<Response> {
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if value.is_some_and(|value| {
        value.starts_with("Bearer ya29.") && value.len() > "Bearer ya29.".len()
    }) {
        eprintln!("fake-vertex auth key_label=Bearer ya29.*");
        None
    } else {
        Some(vertex_error(
            StatusCode::UNAUTHORIZED,
            401,
            "missing or invalid GCP bearer token",
            "UNAUTHENTICATED",
        ))
    }
}

async fn mode_response(headers: &HeaderMap) -> Option<Response> {
    let mode = headers
        .get("x-fake-mode")
        .and_then(|value| value.to_str().ok());
    match mode {
        Some("401") => Some(vertex_error(
            StatusCode::UNAUTHORIZED,
            401,
            "forced fake unauthorized response",
            "UNAUTHENTICATED",
        )),
        Some("429") => {
            let mut response = vertex_error(
                StatusCode::TOO_MANY_REQUESTS,
                429,
                "forced fake rate limit response",
                "RESOURCE_EXHAUSTED",
            );
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
            Some(response)
        }
        Some("500") => Some(vertex_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "forced fake server error response",
            "INTERNAL",
        )),
        Some("timeout") => {
            sleep(Duration::from_secs(60)).await;
            Some(vertex_error(
                StatusCode::GATEWAY_TIMEOUT,
                504,
                "forced fake timeout response",
                "DEADLINE_EXCEEDED",
            ))
        }
        _ => None,
    }
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
                    "id": "msg_vertex_fake_000000000000000000000000",
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
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": { "type": "text", "text": "" }
            })
            .to_string(),
        ),
        (
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "fake vertex fixture response HELLO" }
            })
            .to_string(),
        ),
        (
            "content_block_stop",
            json!({ "type": "content_block_stop", "index": 0 }).to_string(),
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
        "id": "msg_vertex_fake_000000000000000000000000",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{ "type": "text", "text": "fake vertex fixture response HELLO" }],
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

fn vertex_error(status: StatusCode, code: u16, message: &str, error_status: &str) -> Response {
    json_response(
        status,
        json!({
            "error": { "code": code, "message": message, "status": error_status }
        }),
    )
}

fn with_fixture_headers(mut response: Response) -> Response {
    let request_id = next_request_id();
    let request_id = match HeaderValue::from_str(&request_id) {
        Ok(value) => value,
        Err(_) => HeaderValue::from_static("req_fallback"),
    };
    response
        .headers_mut()
        .insert("x-goog-request-id", request_id);
    response
}

fn next_request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    format!("req-{nanos:x}-{counter:x}")
}
