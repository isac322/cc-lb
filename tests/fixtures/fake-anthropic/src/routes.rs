#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
use std::collections::HashMap;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::sleep;

use crate::modes::FakeMode;
use crate::oauth::{OAuthState, authorize, refresh_history, token};
use crate::oauth_pause::OAuthRefreshPause;
use crate::sse::streaming_response;

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub slow_mode_bps: u64,
    pub files_cap_bytes: usize,
    pub tokens_expire_in: u64,
    pub oauth_refresh_pause: Option<OAuthRefreshPause>,
    pub message_script: Option<MessageScript>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            slow_mode_bps: 1024,
            files_cap_bytes: 104_857_600,
            tokens_expire_in: 3600,
            oauth_refresh_pause: None,
            message_script: None,
        }
    }
}

#[derive(Clone)]
pub struct MessageScript {
    inner: Arc<MessageScriptInner>,
}

struct MessageScriptInner {
    responses: Mutex<VecDeque<ScriptedMessageResponse>>,
    requests: Mutex<Vec<RecordedMessageRequest>>,
    notify: Notify,
}

#[derive(Clone, Debug)]
pub struct RecordedMessageRequest {
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub body_json: Value,
}

#[derive(Clone, Debug)]
pub struct ScriptedMessageResponse {
    pub status: StatusCode,
    pub headers: BTreeMap<String, String>,
    pub body: Value,
    pub delay: Duration,
}

impl Default for MessageScript {
    fn default() -> Self {
        Self {
            inner: Arc::new(MessageScriptInner {
                responses: Mutex::new(VecDeque::new()),
                requests: Mutex::new(Vec::new()),
                notify: Notify::new(),
            }),
        }
    }
}

impl MessageScript {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_response(&self, response: ScriptedMessageResponse) {
        self.inner
            .responses
            .lock()
            .expect("message script response lock")
            .push_back(response);
    }

    pub fn requests(&self) -> Vec<RecordedMessageRequest> {
        self.inner
            .requests
            .lock()
            .expect("message script request lock")
            .clone()
    }

    pub fn request_count(&self) -> usize {
        self.inner
            .requests
            .lock()
            .expect("message script request lock")
            .len()
    }

    pub async fn wait_for_requests(&self, expected: usize, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                if self.request_count() >= expected {
                    return;
                }
                self.inner.notify.notified().await;
            }
        })
        .await
        .is_ok()
    }

    fn record(&self, headers: &HeaderMap, body: &Bytes) {
        let headers = headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect::<BTreeMap<_, _>>();
        let body_json = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
        self.inner
            .requests
            .lock()
            .expect("message script request lock")
            .push(RecordedMessageRequest {
                headers,
                body: body.to_vec(),
                body_json,
            });
        self.inner.notify.notify_waiters();
    }

    fn pop_response(&self) -> Option<ScriptedMessageResponse> {
        self.inner
            .responses
            .lock()
            .expect("message script response lock")
            .pop_front()
    }
}

impl fmt::Debug for MessageScript {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageScript")
            .field("request_count", &self.request_count())
            .finish_non_exhaustive()
    }
}

impl ScriptedMessageResponse {
    pub fn ok() -> Self {
        Self {
            status: StatusCode::OK,
            headers: BTreeMap::new(),
            body: json!({
                "id": "msg_fake_warmup_000000000000000000",
                "type": "message",
                "role": "assistant",
                "model": "claude-haiku-4-5-20251001",
                "content": [{
                    "type": "text",
                    "text": "fake anthropic fixture response HELLO"
                }],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 1
                }
            }),
            delay: Duration::ZERO,
        }
    }

    pub fn error(status: StatusCode, error_type: &str, message: &str) -> Self {
        Self {
            status,
            headers: BTreeMap::new(),
            body: json!({
                "type": "error",
                "error": {
                    "type": error_type,
                    "message": message
                }
            }),
            delay: Duration::ZERO,
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    async fn into_response(self) -> Response {
        if self.delay > Duration::ZERO {
            sleep(self.delay).await;
        }
        let mut response = Json(self.body).into_response();
        *response.status_mut() = self.status;
        for (name, value) in self.headers {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(name.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                response.headers_mut().insert(name, value);
            }
        }
        with_fixture_headers(response)
    }
}

#[derive(Debug)]
pub struct AppState {
    pub(crate) config: AppConfig,
    pub(crate) oauth: OAuthState,
    request_counts: Mutex<BTreeMap<&'static str, u64>>,
    last_x_api_key: Mutex<Option<String>>,
    last_selected_headers: Mutex<BTreeMap<&'static str, Option<String>>>,
    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    injected_usage: Mutex<HashMap<String, InjectedUsage>>,
    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    default_injected_usage: Mutex<Option<InjectedUsage>>,
}

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
#[derive(Clone, Debug, Default, Deserialize)]
struct InjectedUsage {
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
}

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
#[derive(Debug, Deserialize)]
struct InjectCacheStatsRequest {
    match_request_signature: String,
    usage: InjectedUsage,
}

impl AppState {
    fn new(config: AppConfig) -> Self {
        Self {
            config,
            oauth: OAuthState::default(),
            request_counts: Mutex::new(BTreeMap::new()),
            last_x_api_key: Mutex::new(None),
            last_selected_headers: Mutex::new(BTreeMap::from([
                ("x-organization-uuid", None),
                ("x-trusted-device-token", None),
            ])),
            #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
            injected_usage: Mutex::new(HashMap::new()),
            #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
            default_injected_usage: Mutex::new(None),
        }
    }

    fn record_auth(&self, label: &'static str) {
        if let Ok(mut counts) = self.request_counts.lock() {
            let count = counts
                .entry(label)
                .and_modify(|value| *value += 1)
                .or_insert(1);
            eprintln!("fake-anthropic request_count key_label={label} count={count}");
        }
    }

    fn record_last_request(&self, headers: &HeaderMap) {
        if let Ok(mut last_x_api_key) = self.last_x_api_key.lock() {
            *last_x_api_key = headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned);
        }

        if let Ok(mut last_selected_headers) = self.last_selected_headers.lock() {
            for name in ["x-organization-uuid", "x-trusted-device-token"] {
                last_selected_headers.insert(
                    name,
                    headers
                        .get(name)
                        .and_then(|value| value.to_str().ok())
                        .map(ToOwned::to_owned),
                );
            }
        }
    }
}

pub fn app(config: AppConfig) -> Router {
    let max_body = config.files_cap_bytes;
    let state = Arc::new(AppState::new(config));

    let router = Router::new()
        .route("/v1/messages", post(messages))
        .route("/v1/messages/count_tokens", post(count_tokens))
        .route("/oauth/authorize", get(authorize))
        .route("/oauth/token", post(token))
        .route("/v1/oauth/token", post(token))
        .route("/__refresh_history", get(refresh_history))
        .route("/__last_request", get(last_request));

    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    let router = router
        .route("/__inject_cache_stats", post(inject_cache_stats))
        .route("/__set_default_cache_stats", post(set_default_cache_stats));

    router
        .route("/v1/models", get(list_models))
        .route("/v1/models/{id}", get(get_model))
        .route("/v1/files", get(list_files).post(create_file))
        .route("/v1/files/{id}", get(get_file).delete(delete_file))
        .fallback(not_found)
        .with_state(state)
        .layer(DefaultBodyLimit::max(max_body))
}

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
async fn inject_cache_stats(
    State(state): State<Arc<AppState>>,
    Json(request): Json<InjectCacheStatsRequest>,
) -> Response {
    match state.injected_usage.lock() {
        Ok(mut injected_usage) => {
            injected_usage.insert(request.match_request_signature, request.usage);
            json_response(StatusCode::OK, json!({ "ok": true }))
        }
        Err(_) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            "failed to record injected cache stats",
        ),
    }
}

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
async fn set_default_cache_stats(
    State(state): State<Arc<AppState>>,
    Json(usage): Json<InjectedUsage>,
) -> Response {
    match state.default_injected_usage.lock() {
        Ok(mut slot) => {
            *slot = Some(usage);
            json_response(StatusCode::OK, json!({ "ok": true }))
        }
        Err(_) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_error",
            "failed to record default cache stats",
        ),
    }
}

async fn last_request(State(state): State<Arc<AppState>>) -> Response {
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
        json!({ "x_api_key": x_api_key, "headers": headers }),
    )
}

async fn messages(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }

    if let Some(script) = state.config.message_script.as_ref() {
        script.record(&headers, &body);
        if let Some(response) = script.pop_response() {
            return response.into_response().await;
        }
    }

    let mode = FakeMode::from_headers(&headers);
    if let Some(response) = mode_response(mode).await {
        return response;
    }

    let body_json = serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null);
    let model = body_json
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("claude-3-5-sonnet-20241022")
        .to_owned();

    if wants_stream(&headers, &body_json) {
        return streaming_response(model, mode, state.config.slow_mode_bps);
    }

    let response_body = json!({
        "id": "msg_fake_000000000000000000000000",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{
            "type": "text",
            "text": "fake anthropic fixture response HELLO"
        }],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 100,
            "output_tokens": 50
        }
    });

    #[cfg(any(debug_assertions, feature = "debug-endpoints"))]
    let response_body = response_body_with_injected_usage(&state, &body_json, response_body);

    json_response(StatusCode::OK, response_body)
}

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
fn response_body_with_injected_usage(
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

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
fn request_signature(body_json: &Value) -> String {
    // No previous fake-anthropic request signature scheme exists. Use the plan's
    // deterministic fallback: SHA-256 over serde_json's canonical Value string.
    let canonical = serde_json::to_string(body_json).unwrap_or_else(|_| "null".to_owned());
    let digest = ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes());
    lowercase_hex(digest.as_ref())
}

#[cfg(any(debug_assertions, feature = "debug-endpoints"))]
fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

async fn count_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }

    let _body_len = body.len();
    json_response(StatusCode::OK, json!({ "input_tokens": 100 }))
}

async fn list_models(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
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

async fn get_model(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }

    json_response(StatusCode::OK, model_json(&id))
}

async fn create_file(
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

async fn list_files(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
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

async fn get_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }

    json_response(StatusCode::OK, file_json(&id))
}

async fn delete_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Some(response) = auth_failure(&state, &headers) {
        return response;
    }

    json_response(
        StatusCode::OK,
        json!({
            "id": id,
            "type": "file",
            "deleted": true
        }),
    )
}

async fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "not_found_error", "route not found")
}

async fn mode_response(mode: FakeMode) -> Option<Response> {
    match mode {
        FakeMode::Unauthorized => Some(error_response(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "forced fake unauthorized response",
        )),
        FakeMode::RateLimited => {
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

fn auth_failure(state: &AppState, headers: &HeaderMap) -> Option<Response> {
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

fn content_length_too_large(headers: &HeaderMap, cap: usize) -> bool {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .map(|length| length > cap)
        .unwrap_or(false)
}

fn model_json(id: &str) -> Value {
    json!({
        "id": id,
        "type": "model",
        "display_name": id,
        "created_at": "2026-05-20T00:00:00Z"
    })
}

fn file_json(id: &str) -> Value {
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
            "error": {
                "type": error_type,
                "message": message
            }
        }),
    )
}

pub fn with_fixture_headers(mut response: Response) -> Response {
    let request_id = next_request_id();
    let request_id = HeaderValue::from_str(&request_id)
        .unwrap_or_else(|_| HeaderValue::from_static("req_fallback"));

    response.headers_mut().insert("request-id", request_id);
    response.headers_mut().insert(
        "anthropic-organization-id",
        HeaderValue::from_static("org_test"),
    );
    response.headers_mut().insert(
        "anthropic-ratelimit-requests-remaining",
        HeaderValue::from_static("999"),
    );
    response.headers_mut().insert(
        "anthropic-ratelimit-tokens-remaining",
        HeaderValue::from_static("999000"),
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
