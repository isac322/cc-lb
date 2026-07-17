use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::modes::FakeMode;
use crate::sse::streaming_response;

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
use super::debug::response_body_with_injected_usage;
use super::helpers::{
    auth_failure, content_length_too_large, error_response, file_json, json_response,
    mode_response, model_json, wants_stream,
};
use super::state::AppState;

pub(crate) async fn last_request(State(state): State<Arc<AppState>>) -> Response {
    let x_api_key = state
        .last_x_api_key
        .lock()
        .ok()
        .and_then(|value| value.clone());
    let headers = state
        .last_selected_headers
        .lock()
        .ok()
        .map(|value| value.clone())
        .unwrap_or_default();
    json_response(
        StatusCode::OK,
        json!({"x_api_key": x_api_key, "headers": headers}),
    )
}

pub(crate) async fn messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    if let Some(script) = state.config.message_script.as_ref() {
        script.record(&headers, &body);
        if let Some(response) = script.pop_response() {
            return response.into_response().await;
        }
    }

    let weather = state.config.weather.for_headers(&headers);
    if weather.timeout() > Duration::ZERO {
        sleep(weather.timeout()).await;
        return error_response(
            StatusCode::GATEWAY_TIMEOUT,
            "timeout_error",
            "forced fake weather timeout response",
        );
    }
    let mode = FakeMode::from_headers(&headers);
    if let Some(response) = mode_response(mode, weather.retry_after()).await {
        return response;
    }

    let body_json = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let model = body_json
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("claude-3-5-sonnet-20241022")
        .to_owned();
    if wants_stream(&headers, &body_json) {
        return streaming_response(model, mode, state.config.slow_mode_bps, weather);
    }

    let response_body = json!({
        "id": "msg_fake_000000000000000000000000",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{"type": "text", "text": "fake anthropic fixture response HELLO"}],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {"input_tokens": 100, "output_tokens": 50}
    });
    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    let response_body = response_body_with_injected_usage(&state, &body_json, response_body);
    json_response(StatusCode::OK, response_body)
}

pub(crate) async fn count_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    let _body_len = body.len();
    json_response(StatusCode::OK, json!({"input_tokens": 100}))
}

pub(crate) async fn list_models(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    json_response(
        StatusCode::OK,
        json!({
            "type": "list",
            "data": [
                model_json("claude-fable-5"),
                model_json("claude-3-5-sonnet-20241022"),
                model_json("claude-3-5-haiku-20241022"),
                model_json("claude-3-opus-20240229")
            ],
            "first_id": "claude-fable-5",
            "last_id": "claude-3-opus-20240229",
            "has_more": false
        }),
    )
}

pub(crate) async fn get_model(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    json_response(StatusCode::OK, model_json(&id))
}

pub(crate) async fn create_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    if content_length_too_large(&headers, state.config.files_cap_bytes)
        || body.len() > state.config.files_cap_bytes
    {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "file body exceeds fake fixture cap",
        );
    }
    json_response(
        StatusCode::OK,
        json!({
            "id": "file_abc123",
            "type": "file",
            "filename": "fixture.txt",
            "mime_type": "text/plain",
            "size_bytes": body.len(),
            "created_at": "2026-05-20T00:00:00Z",
            "downloadable": false
        }),
    )
}

pub(crate) async fn list_files(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    json_response(
        StatusCode::OK,
        json!({
            "type": "list",
            "data": [file_json("file_abc123")],
            "first_id": "file_abc123",
            "last_id": "file_abc123",
            "has_more": false
        }),
    )
}

pub(crate) async fn get_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    json_response(StatusCode::OK, file_json(&id))
}

pub(crate) async fn delete_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }
    json_response(
        StatusCode::OK,
        json!({"id": id, "type": "file", "deleted": true}),
    )
}

pub(crate) async fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "not_found_error", "route not found")
}
