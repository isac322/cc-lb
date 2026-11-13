use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::any};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::common::{self, TestServer};

#[derive(Clone, Debug)]
pub struct RecordedResponseRequest {
    pub body: Value,
    pub raw_body: Vec<u8>,
    pub stream: bool,
    pub response_cache_options: Value,
    pub headers: axum::http::HeaderMap,
}

#[derive(Clone, Default)]
struct MockState {
    requests: Arc<Mutex<Vec<RecordedResponseRequest>>>,
    cached_tokens: Arc<Mutex<VecDeque<Option<u64>>>>,
    usage_payloads: Arc<Mutex<VecDeque<Value>>>,
    responses: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MockState {
    fn record(&self, request: RecordedResponseRequest) {
        self.requests
            .lock()
            .expect("OpenAI mock request lock")
            .push(request);
    }

    pub fn requests(&self) -> Vec<RecordedResponseRequest> {
        self.requests
            .lock()
            .expect("OpenAI mock request lock")
            .clone()
    }
}

pub struct StrictOpenAiMock {
    pub base_url: String,
    state: MockState,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl StrictOpenAiMock {
    pub async fn start() -> Self {
        let state = MockState::default();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind strict OpenAI mock");
        let address = listener.local_addr().expect("strict OpenAI mock address");
        let router = Router::new()
            .fallback(any(openai_handler))
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await });
        Self {
            base_url: format!("http://{address}/"),
            state,
            task,
        }
    }

    pub fn requests(&self) -> Vec<RecordedResponseRequest> {
        self.state.requests()
    }

    pub fn reply_with_cached_tokens(&self, tokens: Option<u64>) {
        self.state
            .cached_tokens
            .lock()
            .expect("mock response queue")
            .push_back(tokens);
    }

    pub fn reply_with_usage(&self, usage: Value) {
        self.state
            .usage_payloads
            .lock()
            .expect("mock usage queue")
            .push_back(usage);
    }

    pub fn response_wire(&self, index: usize) -> Vec<u8> {
        self.state.responses.lock().expect("mock response bytes")[index].clone()
    }

    pub async fn wait_for_requests(&self, count: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if self.requests().len() >= count {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {count} strict OpenAI requests; saw {}",
                self.requests().len()
            );
            tokio::task::yield_now().await;
        }
    }
}

impl Drop for StrictOpenAiMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn openai_handler(State(state): State<MockState>, request: Request) -> Response {
    if request.uri().path() != "/v1/responses" {
        return (StatusCode::NOT_FOUND, "unexpected OpenAI path").into_response();
    }
    if request.method() != axum::http::Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, "unexpected OpenAI method").into_response();
    }
    let headers = request.headers().clone();
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if authorization != Some("Bearer sk-openai-cache-test") {
        return (StatusCode::UNAUTHORIZED, "unexpected OpenAI authorization").into_response();
    }
    let body = match to_bytes(request.into_body(), 16 * 1024 * 1024).await {
        Ok(body) => body,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let stream = value
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let options = value
        .get("prompt_cache_options")
        .cloned()
        .unwrap_or(Value::Null);
    let max_output_tokens = value.get("max_output_tokens").and_then(Value::as_u64);
    let valid_model = matches!(model, "gpt-5.6" | "gpt-5.5");
    let valid_stream = value.get("stream").is_some_and(Value::is_boolean);
    let valid_options = options.is_null()
        || options.as_object().is_some_and(|options| {
            options
                .keys()
                .all(|key| matches!(key.as_str(), "ttl" | "mode"))
                && options
                    .get("ttl")
                    .is_none_or(|ttl| matches!(ttl.as_str(), Some("30m" | "12h")))
                && options
                    .get("mode")
                    .is_none_or(|mode| matches!(mode.as_str(), Some("implicit" | "explicit")))
        });
    let valid_renewal = valid_options
        && value
            .get("input")
            .is_some_and(|input| input.is_array() || input.is_string())
        && matches!(max_output_tokens, Some(16 | 32));
    if !valid_model || !valid_stream || !valid_renewal {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "strict OpenAI Responses contract violation model={model:?} stream={stream} options={options} max_output_tokens={max_output_tokens:?}"
            ),
        )
            .into_response();
    }
    let model = model.to_owned();
    state.record(RecordedResponseRequest {
        body: value,
        raw_body: body.to_vec(),
        stream,
        response_cache_options: options,
        headers,
    });
    let cached_tokens = state
        .cached_tokens
        .lock()
        .expect("mock response queue")
        .pop_front()
        .unwrap_or(Some(2048));
    let mut response = json!({
        "id": "resp_cc_lb_cache",
        "object": "response",
        "status": "completed",
        "model": model,
        "output": [{
            "type": "message",
            "id": "msg_cc_lb_cache",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "openai-cache-ok", "annotations": []}]
        }],
        "usage": {
            "input_tokens": 2048,
            "output_tokens": 16,
            "total_tokens": 2064,
            "input_tokens_details": {"cached_tokens": 2048}
        }
    });
    if let Some(tokens) = cached_tokens {
        response["usage"]["input_tokens_details"]["cached_tokens"] = json!(tokens);
    } else {
        response["usage"]
            .as_object_mut()
            .expect("mock usage")
            .remove("input_tokens_details");
    }
    if let Some(usage) = state
        .usage_payloads
        .lock()
        .expect("mock usage queue")
        .pop_front()
    {
        response["usage"] = usage;
    }
    if stream {
        let body = format!(
            "event: response.completed\ndata: {}\n\n",
            serde_json::to_string(&json!({"type":"response.completed", "response": response}))
                .expect("SSE JSON")
        );
        state
            .responses
            .lock()
            .expect("mock response bytes")
            .push(body.as_bytes().to_vec());
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/event-stream")],
            Body::from(body),
        )
            .into_response()
    } else {
        let response = serde_json::to_vec(&response).expect("JSON response");
        state
            .responses
            .lock()
            .expect("mock response bytes")
            .push(response.clone());
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Body::from(response),
        )
            .into_response()
    }
}

pub struct OpenAiFixture {
    pub server: TestServer,
    pub mock: StrictOpenAiMock,
    pub principal_id: String,
    pub upstream_id: String,
    key: String,
    pub client: reqwest::Client,
}

impl OpenAiFixture {
    pub async fn start() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = common::spawn_test_server_with_two_upstreams(
            "[price_catalog]\nurl = \"http://127.0.0.1:9/price-catalog.json\"\n",
            fake_anthropic::AppConfig::default(),
        )
        .await;
        let mock = StrictOpenAiMock::start().await;
        let client = reqwest::Client::new();
        let upstream_id = disable_seeded_and_create_openai(&client, &server, &mock.base_url).await;
        let principal_id = configure_principal(&client, &server, &upstream_id).await;
        let issued = admin_json(
            &client,
            &server,
            reqwest::Method::POST,
            &format!("/admin/v1/principals/{principal_id}/keys"),
            Some(json!({"label": "openai-cache-regression"})),
        )
        .await;
        assert_eq!(
            issued.status(),
            StatusCode::CREATED,
            "issue UUID-bound managed key"
        );
        let issued: Value = issued.json().await.expect("issued managed key JSON");
        let key = issued["plaintext_key"]
            .as_str()
            .expect("UUID-bound managed key plaintext")
            .to_owned();
        Self {
            server,
            mock,
            principal_id,
            upstream_id,
            key,
            client,
        }
    }

    pub async fn configure_keepalive(&self, enabled: bool, openai_enabled: bool, lead_secs: u32) {
        self.configure_keepalive_with_duration(enabled, openai_enabled, lead_secs, 7200)
            .await;
    }

    pub async fn configure_keepalive_with_duration(
        &self,
        enabled: bool,
        openai_enabled: bool,
        lead_secs: u32,
        max_duration_secs: u64,
    ) {
        let body = json!({
            "cache_keepalive": {
                "enabled": enabled,
                "refresh_lead_time_5m_secs": lead_secs,
                "refresh_lead_time_1h_secs": 300,
                "max_refreshes_per_session": 20,
                "max_total_duration_secs": max_duration_secs,
                "snapshot_max_bytes": 8_388_608,
                "min_cache_tokens": 1024,
                "classifier": {"openai_responses_enabled": openai_enabled}
            }
        });
        let (status, _, etag) = principal_get(&self.client, &self.server, &self.principal_id).await;
        assert_eq!(status, StatusCode::OK);
        let response = self
            .client
            .put(format!(
                "http://{}/admin/v1/principals/{}",
                self.server.admin_addr, self.principal_id
            ))
            .header("authorization", "Bearer admin-token")
            .header("if-match", etag.expect("principal ETag"))
            .json(&body)
            .send()
            .await
            .expect("update cache keepalive principal");
        assert_eq!(response.status(), StatusCode::OK, "principal update");
    }

    pub async fn post(&self, session: &str, body: Value) -> reqwest::Response {
        self.post_with_headers(session, body, &[]).await
    }

    pub async fn post_with_headers(
        &self,
        session: &str,
        body: Value,
        extra_headers: &[(&str, &str)],
    ) -> reqwest::Response {
        let mut request = self
            .client
            .post(format!("http://{}/v1/responses", self.server.proxy_addr))
            .header("x-api-key", &self.key)
            .header("x-session-id", session);
        for (name, value) in extra_headers {
            request = request.header(*name, *value);
        }
        request
            .json(&body)
            .send()
            .await
            .expect("OpenAI Responses proxy request")
    }

    pub async fn list_keepalive(&self) -> Value {
        self.client
            .get(format!(
                "http://{}/admin/v1/principals/{}/cache-keepalive",
                self.server.admin_addr, self.principal_id
            ))
            .header("authorization", "Bearer admin-token")
            .send()
            .await
            .expect("list cache keepalive")
            .json()
            .await
            .expect("cache keepalive JSON")
    }

    pub fn session_hash(&self, session: &str) -> String {
        cc_lb_engine::cache_keepalive::SessionKey::from_thread_id(
            self.principal_id.parse().expect("principal UUID"),
            session,
        )
        .to_string()
    }
}

async fn admin_json(
    client: &reqwest::Client,
    server: &TestServer,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> reqwest::Response {
    let mut request = client
        .request(method, format!("http://{}{}", server.admin_addr, path))
        .header("authorization", "Bearer admin-token");
    if let Some(body) = body {
        request = request.json(&body);
    }
    request.send().await.expect("admin request")
}

async fn disable_seeded_and_create_openai(
    client: &reqwest::Client,
    server: &TestServer,
    base_url: &str,
) -> String {
    let response = admin_json(
        client,
        server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let list: Value = response.json().await.expect("upstream list JSON");
    for entry in list["upstreams"].as_array().expect("upstreams") {
        let id = entry["id"].as_str().expect("seeded upstream id");
        let detail = admin_json(
            client,
            server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
        )
        .await;
        let etag = detail
            .headers()
            .get("etag")
            .expect("upstream ETag")
            .to_str()
            .expect("upstream ETag string")
            .to_owned();
        let disabled = client
            .post(format!(
                "http://{}/admin/v1/upstreams/{id}/disable",
                server.admin_addr
            ))
            .header("authorization", "Bearer admin-token")
            .header("if-match", etag)
            .send()
            .await
            .expect("disable seeded upstream");
        assert_eq!(disabled.status(), StatusCode::OK);
    }
    let response = admin_json(
        client,
        server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": "strict-openai-cache",
            "kind": "openai_api_key",
            "base_url": base_url,
            "api_key_value": "sk-openai-cache-test",
            "skip_preflight": true
        })),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "create OpenAI upstream"
    );
    response.json::<Value>().await.expect("created OpenAI JSON")["id"]
        .as_str()
        .expect("created OpenAI upstream id")
        .to_owned()
}

async fn principal_get(
    client: &reqwest::Client,
    server: &TestServer,
    id: &str,
) -> (StatusCode, Value, Option<String>) {
    let response = admin_json(
        client,
        server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{id}"),
        None,
    )
    .await;
    let status = response.status();
    let etag = response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body = response.json().await.expect("principal JSON");
    (status, body, etag)
}

async fn configure_principal(
    client: &reqwest::Client,
    server: &TestServer,
    upstream_id: &str,
) -> String {
    let response = admin_json(
        client,
        server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let list: Value = response.json().await.expect("principal list JSON");
    let principal = list["principals"]
        .as_array()
        .expect("principals")
        .first()
        .expect("seeded principal");
    let id = principal["id"].as_str().expect("principal id").to_owned();
    let (_, _, etag) = principal_get(client, server, &id).await;
    let response = client
        .put(format!(
            "http://{}/admin/v1/principals/{id}",
            server.admin_addr
        ))
        .header("authorization", "Bearer admin-token")
        .header("if-match", etag.expect("principal ETag"))
        .json(&json!({
            "allowed_upstreams": [upstream_id],
            "cache_keepalive": {
                "enabled": true,
                "refresh_lead_time_5m_secs": 30,
                "refresh_lead_time_1h_secs": 300,
                "max_refreshes_per_session": 20,
                "max_total_duration_secs": 7200,
                "snapshot_max_bytes": 8_388_608,
                "min_cache_tokens": 1024,
                "classifier": {"openai_responses_enabled": true}
            }
        }))
        .send()
        .await
        .expect("configure seeded principal");
    assert_eq!(response.status(), StatusCode::OK, "configure principal");
    id
}

pub fn request_body(options: Option<Value>, input: Value, stream: bool) -> Value {
    let mut body = json!({
        "model": "gpt-5.6",
        "input": input,
        "stream": stream,
        "max_output_tokens": 32
    });
    if let Some(options) = options {
        body["prompt_cache_options"] = options;
    }
    body
}

pub fn large_input(with_breakpoint_ttl: Option<&str>) -> Value {
    let text = "stable OpenAI cache prefix ".repeat(700);
    let content = if let Some(ttl) = with_breakpoint_ttl {
        json!([{"type":"input_text","text":text,"prompt_cache_breakpoint":{"ttl":ttl}}])
    } else {
        json!(text)
    };
    json!([{"role":"user","content":content}])
}
