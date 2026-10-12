use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ring::digest;
use serde::Deserialize;
use serde_json::{Value, json};

use super::helpers::{error_response, json_response};
use super::key_policy::KeyPolicy;
use super::state::AppState;

/// `POST /__key_policies` body; a `null` policy removes the key's policy.
#[derive(Debug, Deserialize)]
pub(crate) struct SetKeyPolicyRequest {
    key: String,
    #[serde(default)]
    policy: Option<KeyPolicy>,
}

pub(crate) async fn set_key_policy(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SetKeyPolicyRequest>,
) -> Response {
    match request.policy {
        Some(policy) => state.config.key_policies.set(request.key, policy),
        None => state.config.key_policies.remove(&request.key),
    }
    StatusCode::NO_CONTENT.into_response()
}

pub(crate) async fn clear_key_policies(State(state): State<Arc<AppState>>) -> Response {
    state.config.key_policies.clear();
    StatusCode::NO_CONTENT.into_response()
}

pub(crate) async fn recorded_requests(State(state): State<Arc<AppState>>) -> Response {
    Json(state.config.recorder.snapshot()).into_response()
}

pub(crate) async fn clear_recorded_requests(State(state): State<Arc<AppState>>) -> Response {
    state.config.recorder.clear();
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct InjectedUsage {
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InjectCacheStatsRequest {
    match_request_signature: String,
    usage: InjectedUsage,
}

pub(crate) async fn inject_cache_stats(
    State(state): State<Arc<AppState>>,
    Json(request): Json<InjectCacheStatsRequest>,
) -> Response {
    match state.injected_usage.lock() {
        Ok(mut injected_usage) => {
            injected_usage.insert(request.match_request_signature, request.usage);
            json_response(http::StatusCode::OK, json!({"ok": true}))
        }
        Err(_) => error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            "failed to record injected cache stats",
        ),
    }
}

pub(crate) async fn set_default_cache_stats(
    State(state): State<Arc<AppState>>,
    Json(usage): Json<InjectedUsage>,
) -> Response {
    match state.default_injected_usage.lock() {
        Ok(mut slot) => {
            *slot = Some(usage);
            json_response(http::StatusCode::OK, json!({"ok": true}))
        }
        Err(_) => error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            "failed to record default cache stats",
        ),
    }
}

pub(crate) fn response_body_with_injected_usage(
    state: &AppState,
    body_json: &Value,
    mut response_body: Value,
) -> Value {
    let signature = request_signature(body_json);
    let injected_usage = state
        .injected_usage
        .lock()
        .ok()
        .and_then(|injected_usage| injected_usage.get(&signature).cloned())
        .or_else(|| {
            state
                .default_injected_usage
                .lock()
                .ok()
                .and_then(|slot| slot.clone())
        });
    if let Some(injected_usage) = injected_usage
        && let Some(usage) = response_body
            .get_mut("usage")
            .and_then(Value::as_object_mut)
    {
        if let Some(tokens) = injected_usage.cache_creation_input_tokens {
            usage.insert("cache_creation_input_tokens".to_owned(), json!(tokens));
        }
        if let Some(tokens) = injected_usage.cache_read_input_tokens {
            usage.insert("cache_read_input_tokens".to_owned(), json!(tokens));
        }
    }
    response_body
}

fn request_signature(body_json: &Value) -> String {
    let canonical = serde_json::to_string(body_json).unwrap_or_else(|_| "null".to_owned());
    let digest = digest::digest(&digest::SHA256, canonical.as_bytes());
    lowercase_hex(digest.as_ref())
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
