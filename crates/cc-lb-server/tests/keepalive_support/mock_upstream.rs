//! Loopback strict Anthropic Messages API mock used by keepalive scenarios.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use super::strict_anthropic::{
    cache_key, cache_keys, invalid_request_body, validate_messages_request,
    validate_renewal_headers,
};
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::post};
use cc_lb_clock::{ClockHandle, unix_secs};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use url::Url;

const CACHE_TTL_5M: u64 = 300;
const CACHE_TTL_1H: u64 = 3_600;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockEvent {
    pub kind: &'static str,
    pub credential: String,
    pub model: String,
    pub cached_tokens: u64,
    pub cache_key: String,
    pub synthetic: bool,
    pub hit: bool,
    pub body: Value,
    pub anthropic_beta: Option<String>,
    pub anthropic_version: Option<String>,
    pub status: u16,
    pub rejection: Option<String>,
}

impl MockEvent {
    pub fn accepted(&self) -> bool {
        self.status == 200
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RenewalFailure {
    Status { status: StatusCode, body: String },
    Hang,
}

#[derive(Clone)]
struct CacheEntry {
    last_access_ts: u64,
    ttl: u64,
}

#[derive(Clone)]
struct MockState {
    clock: ClockHandle,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    events: Arc<Mutex<Vec<MockEvent>>>,
    failures: Arc<Mutex<VecDeque<RenewalFailure>>>,
    notify: Arc<Notify>,
}

pub struct MockAnthropic {
    state: MockState,
    address: std::net::SocketAddr,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl MockAnthropic {
    pub async fn start(clock: ClockHandle) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let state = MockState {
            clock,
            cache: Arc::new(Mutex::new(HashMap::new())),
            events: Arc::new(Mutex::new(Vec::new())),
            failures: Arc::new(Mutex::new(VecDeque::new())),
            notify: Arc::new(Notify::new()),
        };
        let router = Router::new()
            .route("/v1/messages", post(messages))
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await });
        Ok(Self {
            state,
            address,
            task,
        })
    }

    pub fn base_url(&self) -> Url {
        Url::parse(&format!("http://{}/", self.address))
            .unwrap_or_else(|_| Url::parse("http://127.0.0.1/").expect("literal URL"))
    }
    pub fn closed_port_url() -> Url {
        Url::parse("http://127.0.0.1:9/").expect("literal closed-port URL")
    }
    pub fn fail_next_renewals(&self, count: usize, status: StatusCode, body: impl Into<String>) {
        let mut failures = lock(&self.state.failures);
        let body = body.into();
        for _ in 0..count {
            failures.push_back(RenewalFailure::Status {
                status,
                body: body.clone(),
            });
        }
    }
    pub fn hang_next_renewals(&self, count: usize) {
        let mut failures = lock(&self.state.failures);
        for _ in 0..count {
            failures.push_back(RenewalFailure::Hang);
        }
    }
    pub fn events(&self) -> Vec<MockEvent> {
        lock(&self.state.events).clone()
    }
    pub fn last(&self, kind: &'static str) -> Option<MockEvent> {
        lock(&self.state.events)
            .iter()
            .rev()
            .find(|event| event.kind == kind)
            .cloned()
    }

    pub async fn wait_for_event_kind(
        &self,
        kind: &'static str,
        timeout: Duration,
    ) -> Option<MockEvent> {
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.state.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(event) = self.last(kind) {
                    return event;
                }
                notified.await;
            }
        })
        .await
        .ok()
    }
}

impl Drop for MockAnthropic {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn messages(State(state): State<MockState>, request: Request<Body>) -> Response<Body> {
    let headers = request.headers().clone();
    let credential = headers
        .get("authorization")
        .or_else(|| headers.get("x-api-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let bytes = match http_body_util::BodyExt::collect(request.into_body()).await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            record_rejection(
                &state,
                &headers,
                credential,
                Value::Null,
                "proxy",
                400,
                error.to_string(),
            );
            return invalid(StatusCode::BAD_REQUEST, error.to_string());
        }
    };
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(error) => {
            record_rejection(
                &state,
                &headers,
                credential,
                Value::Null,
                "proxy",
                400,
                error.to_string(),
            );
            return invalid(StatusCode::BAD_REQUEST, error.to_string());
        }
    };
    let synthetic = value.get("max_tokens").and_then(Value::as_u64) == Some(0);
    let kind = if synthetic { "renewal" } else { "proxy" };
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if let Err(error) = validate_messages_request(&value, &headers) {
        record_rejection(
            &state,
            &headers,
            credential,
            value,
            kind,
            400,
            error.clone(),
        );
        return invalid(StatusCode::BAD_REQUEST, error);
    }
    if synthetic && let Err(error) = validate_renewal_headers(&headers) {
        record_rejection(
            &state,
            &headers,
            credential,
            value,
            kind,
            400,
            error.clone(),
        );
        return invalid(StatusCode::BAD_REQUEST, error);
    }
    if synthetic {
        let failure = {
            let mut failures = lock(&state.failures);
            failures.pop_front()
        };
        if let Some(failure) = failure {
            match failure {
                RenewalFailure::Status { status, body } => {
                    record_rejection(
                        &state,
                        &headers,
                        credential,
                        value,
                        kind,
                        status.as_u16(),
                        body.clone(),
                    );
                    return (status, body).into_response();
                }
                RenewalFailure::Hang => {
                    record_rejection(
                        &state,
                        &headers,
                        credential,
                        value,
                        kind,
                        0,
                        "hang".to_owned(),
                    );
                    return std::future::pending::<Response<Body>>().await;
                }
            }
        }
    }
    let now = unix_secs(state.clock.now());
    let keys = cache_keys(&value);
    let deepest = keys.last().cloned().unwrap_or_else(|| cache_key(&value));
    let breakpoint_ttls = breakpoint_ttls(&value);
    let mut cache = lock(&state.cache);
    let hit = cache
        .get(&deepest)
        .is_some_and(|entry| now < entry.last_access_ts.saturating_add(entry.ttl));
    for (index, key) in keys.iter().enumerate() {
        cache.insert(
            key.clone(),
            CacheEntry {
                last_access_ts: now,
                ttl: breakpoint_ttls.get(index).copied().unwrap_or(CACHE_TTL_5M),
            },
        );
    }
    drop(cache);
    let cached_tokens = prefix_tokens(&value);
    let event = MockEvent {
        kind,
        credential,
        model: model.clone(),
        cached_tokens,
        cache_key: deepest,
        synthetic,
        hit,
        body: value.clone(),
        anthropic_beta: header_value(&headers, "anthropic-beta"),
        anthropic_version: header_value(&headers, "anthropic-version"),
        status: 200,
        rejection: None,
    };
    record(&state, event);
    response(&value, &model, now, hit, cached_tokens)
}

fn record_rejection(
    state: &MockState,
    headers: &HeaderMap,
    credential: String,
    body: Value,
    kind: &'static str,
    status: u16,
    rejection: String,
) {
    let event = MockEvent {
        kind,
        credential,
        model: body
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        cached_tokens: 0,
        cache_key: String::new(),
        synthetic: body.get("max_tokens").and_then(Value::as_u64) == Some(0),
        hit: false,
        body,
        anthropic_beta: header_value(headers, "anthropic-beta"),
        anthropic_version: header_value(headers, "anthropic-version"),
        status,
        rejection: Some(rejection),
    };
    record(state, event);
}

fn record(state: &MockState, event: MockEvent) {
    lock(&state.events).push(event);
    state.notify.notify_waiters();
}
fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}
fn invalid(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    (status, Json(invalid_request_body(message))).into_response()
}

fn response(value: &Value, model: &str, now: u64, hit: bool, cached_tokens: u64) -> Response<Body> {
    let tail = value
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|messages| messages.last())
        .and_then(|message| message.get("content"));
    let directive = tail
        .and_then(Value::as_array)
        .and_then(|blocks| blocks.first())
        .and_then(|block| block.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let stop_reason = parse_tag(directive, "stop=").unwrap_or("tool_use");
    let tool = parse_tag(directive, "tool=").unwrap_or("bash");
    let synthetic = value.get("max_tokens").and_then(Value::as_u64) == Some(0);
    let content = if synthetic || stop_reason == "end_turn" {
        Vec::new()
    } else {
        vec![json!({"type":"tool_use","id":format!("tool-{now}"),"name":tool,"input":{}})]
    };
    let body = json!({
        "id": format!("msg-{now}"),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": if synthetic { "max_tokens" } else { stop_reason },
        "stop_sequence": null,
        "usage": {
            "input_tokens": 1,
            "output_tokens": 1,
            "cache_read_input_tokens": if hit { cached_tokens } else { 0 },
            "cache_creation_input_tokens": if hit { 0 } else { cached_tokens },
        },
    });
    (StatusCode::OK, Json(body)).into_response()
}

fn breakpoint_ttls(value: &Value) -> Vec<u64> {
    let mut ttls = Vec::new();
    for field in ["tools", "system"] {
        if let Some(items) = value.get(field).and_then(Value::as_array) {
            for item in items {
                if item.get("cache_control").is_some() {
                    ttls.push(cache_control_ttl(item));
                }
            }
        }
    }
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        for message in messages {
            if message.get("cache_control").is_some() {
                ttls.push(cache_control_ttl(message));
            }
            if let Some(blocks) = message.get("content").and_then(Value::as_array) {
                for block in blocks {
                    if block.get("cache_control").is_some() {
                        ttls.push(cache_control_ttl(block));
                    }
                }
            }
        }
    }
    ttls
}

fn cache_control_ttl(value: &Value) -> u64 {
    value
        .get("cache_control")
        .and_then(|control| control.get("ttl"))
        .and_then(Value::as_str)
        .map(|ttl| {
            if ttl == "1h" {
                CACHE_TTL_1H
            } else {
                CACHE_TTL_5M
            }
        })
        .unwrap_or(CACHE_TTL_5M)
}
fn prefix_tokens(value: &Value) -> u64 {
    value
        .get("system")
        .and_then(Value::as_array)
        .and_then(|blocks| blocks.first())
        .and_then(|block| block.get("text"))
        .and_then(Value::as_str)
        .and_then(|text| parse_tag(text, "tokens="))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}
fn parse_tag<'a>(input: &'a str, key: &str) -> Option<&'a str> {
    let start = input.find(key)? + key.len();
    let rest = &input[start..];
    Some(&rest[..rest.find(';').unwrap_or(rest.len())])
}
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}
