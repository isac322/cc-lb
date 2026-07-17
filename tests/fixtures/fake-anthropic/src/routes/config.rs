use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Json;
use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::sleep;

use crate::oauth_pause::OAuthRefreshPause;
use crate::weather::WeatherConfig;

use super::state::with_fixture_headers;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub slow_mode_bps: u64,
    pub files_cap_bytes: usize,
    pub tokens_expire_in: u64,
    pub oauth_refresh_pause: Option<OAuthRefreshPause>,
    pub message_script: Option<MessageScript>,
    pub weather: WeatherConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            slow_mode_bps: 1024,
            files_cap_bytes: 104_857_600,
            tokens_expire_in: 3600,
            oauth_refresh_pause: None,
            message_script: None,
            weather: WeatherConfig::default(),
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
        let mut responses = match self.inner.responses.lock() {
            Ok(responses) => responses,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(response);
    }

    pub fn requests(&self) -> Vec<RecordedMessageRequest> {
        let requests = match self.inner.requests.lock() {
            Ok(requests) => requests,
            Err(poisoned) => poisoned.into_inner(),
        };
        requests.clone()
    }

    pub fn request_count(&self) -> usize {
        let requests = match self.inner.requests.lock() {
            Ok(requests) => requests,
            Err(poisoned) => poisoned.into_inner(),
        };
        requests.len()
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

    pub(crate) fn record(&self, headers: &HeaderMap, body: &Bytes) {
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
        let mut requests = match self.inner.requests.lock() {
            Ok(requests) => requests,
            Err(poisoned) => poisoned.into_inner(),
        };
        requests.push(RecordedMessageRequest {
            headers,
            body: body.to_vec(),
            body_json,
        });
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn pop_response(&self) -> Option<ScriptedMessageResponse> {
        let mut responses = match self.inner.responses.lock() {
            Ok(responses) => responses,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.pop_front()
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
                "usage": {"input_tokens": 1, "output_tokens": 1}
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
                "error": {"type": error_type, "message": message}
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

    pub(crate) async fn into_response(self) -> Response {
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
