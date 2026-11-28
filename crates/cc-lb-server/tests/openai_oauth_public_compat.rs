use crate::common;

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use reqwest::Method;
use serde_json::{Value, json};
use sqlx::{Row, sqlite::SqlitePoolOptions};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Barrier, task::JoinHandle};

/// One recorded upstream call: path, headers, body.
type Recorded = (String, Vec<(String, String)>, Vec<u8>);

#[derive(Clone, Default)]
pub(crate) struct FakeState(pub(crate) Arc<Mutex<Vec<Recorded>>>);
pub(crate) struct Fake {
    pub(crate) base: String,
    pub(crate) state: FakeState,
    task: JoinHandle<Result<(), std::io::Error>>,
}
impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake(State(state): State<FakeState>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or_default().to_owned();
    let mut headers = req
        .headers()
        .iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_owned())))
        .collect::<Vec<_>>();
    headers.push(("x-test-query".to_owned(), query.clone()));
    let body = to_bytes(req.into_body(), 2_000_000).await.unwrap().to_vec();
    state.0.lock().unwrap().push((path.clone(), headers, body));
    if path == "/codex/models"
        && !query
            .split('&')
            .any(|pair| pair.starts_with("client_version="))
        && !state
            .0
            .lock()
            .unwrap()
            .last()
            .is_some_and(|(_, headers, _)| {
                headers.iter().any(|(name, _)| name == "accept-encoding")
            })
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"detail":"client_version required"})),
        )
            .into_response();
    }
    if path == "/codex/models" {
        if query
            .split('&')
            .any(|pair| pair == "client_version=invalid_sse")
        {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(br#"{"catalog":[] }"#.as_slice()))
                .unwrap();
        }
        if query
            .split('&')
            .any(|pair| pair == "client_version=invalid")
        {
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(br#"{"catalog":[] }"#.as_slice()))
                .unwrap();
        }
        return Body::from(
            br#"{"models":[{"slug":"gpt-test","display_name":"GPT Test","description":"test","capabilities":{}},{"slug":"gpt-other","display_name":"GPT Other","description":"other","capabilities":{}}]}"#
                .as_slice(),
        )
        .into_response();
    }
    if path == "/codex/responses/compact" {
        return Body::from(b"{\"compacted\":true}".as_slice()).into_response();
    }
    if path == "/codex/alpha/search" {
        return Json(json!({"results":[{"id":"native"}]})).into_response();
    }
    if path == "/codex/realtime/calls" {
        return Body::from(b"native-sdp-response".as_slice()).into_response();
    }
    if path == "/codex/responses" {
        let body = state.0.lock().unwrap().last().unwrap().2.clone();
        let value = serde_json::from_slice::<Value>(&body).ok();
        if value
            .as_ref()
            .and_then(|value| value.get("input"))
            .is_some_and(|input| input.to_string().contains("force_stream_error"))
        {
            let s = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"too long\"}}}\n\n";
            return Body::from(s).into_response();
        }
        if value
            .as_ref()
            .and_then(|value| value.get("force_error"))
            .is_some()
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail":"Unsupported parameter: x"})),
            )
                .into_response();
        }
        let s = concat!(
            "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
            "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":1,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
            "event: response.metadata\ndata: {\"type\":\"response.metadata\",\"sequence_number\":2,\"metadata\":{\"x\":\"y\"}}\n\n",
            "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":3,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello \"}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":5,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"world\"}\n\n",
            "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello world\",\"annotations\":[]}]}}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"completed\",\"service_tier\":\"default\",\"usage\":{\"input_tokens\":2,\"output_tokens\":2,\"total_tokens\":4},\"output\":[]}}\n\n"
        );
        // Like the real Codex backend, the SSE body carries no Content-Type.
        return Body::from(s).into_response();
    }
    (StatusCode::NOT_FOUND, Json(json!({"detail":"not found"}))).into_response()
}
pub(crate) async fn spawn() -> Fake {
    let state = FakeState::default();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    let server_state = state.clone();
    let task = tokio::spawn(async move {
        axum::serve(l, Router::new().fallback(fake).with_state(server_state)).await
    });
    Fake {
        base: format!("http://{a}/codex"),
        state,
        task,
    }
}
struct FanoutFake {
    base: String,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for FanoutFake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct FanoutState {
    barrier: Arc<Barrier>,
}

async fn fanout_fake(State(state): State<FanoutState>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    if path == "/codex/models" {
        return Json(json!({"models":[{"slug":"gpt-test","display_name":"GPT Test","description":"test","capabilities":{}}]})).into_response();
    }
    if path == "/codex/responses" {
        let body = to_bytes(req.into_body(), 2_000_000).await.unwrap();
        if String::from_utf8_lossy(&body).contains("force_overlap")
            && tokio::time::timeout(Duration::from_secs(5), state.barrier.wait())
                .await
                .is_err()
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"detail":"fan-out did not overlap"})),
            )
                .into_response();
        }
        let s = concat!(
            "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":2,\"total_tokens\":4},\"output\":[]}}\n\n"
        );
        return Body::from(s).into_response();
    }
    (StatusCode::NOT_FOUND, Json(json!({"detail":"not found"}))).into_response()
}

async fn spawn_fanout() -> FanoutFake {
    let state = FanoutState {
        barrier: Arc::new(Barrier::new(3)),
    };
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(l, Router::new().fallback(fanout_fake).with_state(state)).await
    });
    FanoutFake {
        base: format!("http://{a}/codex"),
        task,
    }
}
type RecordedHeaders = Vec<(String, String)>;
type FailoverRequests = Arc<Mutex<Vec<RecordedHeaders>>>;
pub(crate) struct FailoverFake {
    pub(crate) base: String,
    pub(crate) calls: Arc<AtomicUsize>,
    pub(crate) requests: FailoverRequests,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for FailoverFake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct FailoverState {
    calls: Arc<AtomicUsize>,
    requests: FailoverRequests,
    mixed_first_success: bool,
    rate_limit_first_call: bool,
}

async fn failover_fake(State(state): State<FailoverState>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let headers = req
        .headers()
        .iter()
        .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_owned())))
        .collect::<Vec<_>>();
    state.requests.lock().unwrap().push(headers);
    let client_request = req.headers().contains_key("request-id");
    if path == "/codex/models" {
        return Json(json!({"models":[{"slug":"gpt-test","display_name":"GPT Test","description":"test","capabilities":{}}]})).into_response();
    }
    if path == "/codex/responses" {
        let _ = to_bytes(req.into_body(), 2_000_000).await.unwrap();
        let call = if client_request {
            state.calls.fetch_add(1, Ordering::SeqCst)
        } else {
            usize::MAX
        };
        if client_request
            && if state.rate_limit_first_call {
                call == 0
            } else if state.mixed_first_success {
                call == 1
            } else {
                call < 2
            }
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"detail":"rate limited"})),
            )
                .into_response();
        }
        let s = concat!(
            "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_good\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_good\",\"output_index\":0,\"content_index\":0,\"delta\":\"good\"}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_good\",\"object\":\"response\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5},\"output\":[]}}\n\n"
        );
        return Body::from(s).into_response();
    }
    (StatusCode::NOT_FOUND, Json(json!({"detail":"not found"}))).into_response()
}

pub(crate) async fn spawn_mixed_failover() -> FailoverFake {
    spawn_mixed_failover_with_first_rate_limit(false).await
}

pub(crate) async fn spawn_mixed_failover_with_first_rate_limit(
    rate_limit_first_call: bool,
) -> FailoverFake {
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = FailoverState {
        calls: calls.clone(),
        requests: requests.clone(),
        mixed_first_success: true,
        rate_limit_first_call,
    };
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(l, Router::new().fallback(failover_fake).with_state(state)).await
    });
    FailoverFake {
        base: format!("http://{a}/codex"),
        calls,
        requests,
        task,
    }
}

pub(crate) async fn admin(
    c: &reqwest::Client,
    s: &common::TestServer,
    m: Method,
    p: &str,
    b: Option<Value>,
    etag: Option<&str>,
) -> (reqwest::StatusCode, Value, Option<String>) {
    let mut r = c
        .request(m, format!("http://{}{}", s.admin_addr, p))
        .bearer_auth("admin-token");
    if let Some(e) = etag {
        r = r.header("if-match", e)
    }
    if let Some(v) = b {
        r = r.json(&v)
    }
    let x = r.send().await.unwrap();
    let st = x.status();
    let e = x
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    (st, x.json().await.unwrap(), e)
}
async fn chat_usage_rows(s: &common::TestServer) -> Vec<(Option<i64>, Option<i64>)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect(&format!("sqlite://{}", s.sqlite_path.display()))
        .await
        .expect("open request event database");
    sqlx::query(
        "SELECT input_tokens, output_tokens
         FROM request_events_v1
         WHERE upstream_name = 'oauth-test' AND list_status = 200
         ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("query chat request events")
    .into_iter()
    .map(|row| {
        (
            row.get::<Option<i64>, _>("input_tokens"),
            row.get::<Option<i64>, _>("output_tokens"),
        )
    })
    .collect()
}
async fn chat_service_tier_rows(s: &common::TestServer) -> Vec<Option<String>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect(&format!("sqlite://{}", s.sqlite_path.display()))
        .await
        .expect("open request event database");
    sqlx::query(
        "SELECT service_tier
         FROM request_events_v1
         WHERE upstream_name = 'oauth-test' AND list_status = 200
         ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("query service tier rows")
    .into_iter()
    .map(|row| row.get::<Option<String>, _>("service_tier"))
    .collect()
}

async fn wait_for_chat_usage_rows(s: &common::TestServer, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows = chat_usage_rows(s).await;
        if rows.len() >= expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} chat request events; got {}",
            rows.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
pub(crate) async fn setup(s: &common::TestServer, base: &str, c: &reqwest::Client) -> String {
    let (st, ups, _) = admin(c, s, Method::GET, "/admin/v1/upstreams", None, None).await;
    assert_eq!(st, 200);
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, d, e) = admin(
            c,
            s,
            Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        admin(
            c,
            s,
            Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            e.as_deref(),
        )
        .await;
        assert!(d.is_object());
    }
    let body = json!({"name":"oauth-test","kind":"openai_oauth","base_url":base,"oauth_tokens":{"access_token":"test-access","refresh_token":"","expires_at_unix_secs":4102444800u64,"account_id":"acct-test","scopes":[],"never_refresh":true},"skip_preflight":true});
    let (st, v, _) = admin(c, s, Method::POST, "/admin/v1/upstreams", Some(body), None).await;
    assert_eq!(st, 201, "{v}");
    v["id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn openai_oauth_public_responses_and_models() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
    let f = spawn().await;
    let c = reqwest::Client::new();
    setup(&s, &f.base, &c).await;
    let key = &s.managed_key.plaintext;
    let r = c
        .post(format!("http://{}/openai/v1/responses", s.proxy_addr))
        .bearer_auth(key)
        .json(
            &json!({"model":"gpt-test","input":"hi","stream":false,"store":true,"temperature":0.2,"service_tier":"flex"}),
        )
        .send()
        .await
        .unwrap();
    let status = r.status();
    let text = r.text().await.unwrap();
    assert_eq!(status, 200, "{text}");
    assert!(text.contains("hello world"), "{text}");
    // The fake also records the upstream readiness probe and background quota
    // polls; only the proxied client request matters here.
    let rec: Vec<_> = f
        .state
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, headers, _)| {
            path == "/codex/responses" && headers.iter().any(|(name, _)| name == "request-id")
        })
        .cloned()
        .collect();
    assert_eq!(
        rec.len(),
        1,
        "{:?}",
        rec.iter()
            .map(|r| (String::from_utf8_lossy(&r.2).into_owned(), r.1.clone()))
            .collect::<Vec<_>>()
    );
    let v: Value = serde_json::from_slice(&rec[0].2).unwrap();
    assert!(v["input"].is_array());
    assert_eq!(v["stream"], true);
    assert_eq!(v["store"], false);
    assert!(v.get("temperature").is_none());
    assert_eq!(v["service_tier"], "default");
    wait_for_chat_usage_rows(&s, 1).await;
    assert_eq!(
        chat_service_tier_rows(&s).await.last().cloned().flatten(),
        Some("default".to_owned())
    );
    let r = c
        .get(format!("http://{}/openai/v1/models", s.proxy_addr))
        .bearer_auth(key)
        // SDKs advertise compression; the converted catalog must still parse.
        .header("accept-encoding", "gzip, deflate, br")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("gpt-test"));
    let catalog_request = f
        .state
        .0
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|(path, _, _)| path == "/codex/models")
        .cloned()
        .expect("catalog request reached the backend");
    assert!(
        !catalog_request
            .1
            .iter()
            .any(|(name, _)| name == "accept-encoding"),
        "a converted response is requested uncompressed"
    );
    let r = c
        .get(format!("http://{}/openai/v1/models/gpt-test", s.proxy_addr))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = c
        .post(format!(
            "http://{}/openai/v1/memories/trace_summarize",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn openai_oauth_chat_stream_failure_before_output_is_http_error() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
    let f = spawn().await;
    let c = reqwest::Client::new();
    setup(&s, &f.base, &c).await;
    let key = &s.managed_key.plaintext;
    let response = c
        .post(format!(
            "http://{}/openai/v1/chat/completions",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({
            "model": "gpt-test",
            "messages": [{"role":"user","content":"force_stream_error"}],
            "stream": true,
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["code"], "context_length_exceeded");
}

#[tokio::test]
async fn openai_oauth_chat_fanout_retries_all_choices_after_rate_limit() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
    let f = spawn_mixed_failover().await;
    let c = reqwest::Client::new();
    setup(&s, &f.base, &c).await;
    let (status, value, _) = admin(
        &c,
        &s,
        Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name":"oauth-second",
            "kind":"openai_oauth",
            "base_url":f.base,
            "oauth_tokens":{"access_token":"test-access-2","refresh_token":"","expires_at_unix_secs":4102444800u64,"account_id":"acct-test-2","scopes":[],"never_refresh":true},
            "skip_preflight":true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201, "{value}");
    let key = &s.managed_key.plaintext;
    let response = c
        .post(format!(
            "http://{}/openai/v1/chat/completions",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({
            "model": "gpt-test",
            "messages": [{"role":"user","content":"retry_all"}],
            "n": 2,
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["choices"].as_array().map(Vec::len), Some(2));
    assert!(
        body["choices"]
            .as_array()
            .unwrap()
            .iter()
            .all(|choice| choice["message"]["content"] == "good")
    );
    assert_eq!(body["usage"]["prompt_tokens"], 4);
    assert_eq!(body["usage"]["completion_tokens"], 6);
    assert_eq!(body["usage"]["total_tokens"], 10);
}

#[tokio::test]
async fn openai_oauth_chat_fanout_n_three_runs_concurrently() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
    let f = spawn_fanout().await;
    let c = reqwest::Client::new();
    setup(&s, &f.base, &c).await;
    let key = &s.managed_key.plaintext;
    let response = c
        .post(format!(
            "http://{}/openai/v1/chat/completions",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({
            "model": "gpt-test",
            "messages": [{"role":"user","content":"force_overlap"}],
            "n": 3,
        }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let value: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["choices"].as_array().map(Vec::len), Some(3));
    assert_eq!(value["usage"]["prompt_tokens"], 6);
    assert_eq!(value["usage"]["completion_tokens"], 6);
    assert_eq!(value["usage"]["total_tokens"], 12);
}

#[tokio::test]
async fn openai_oauth_chat_completions_usage_contract() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
    let f = spawn().await;
    let c = reqwest::Client::new();
    setup(&s, &f.base, &c).await;
    let key = &s.managed_key.plaintext;
    let cases = [
        ("stream:false n=1", false, 1_u64, 2_i64, 2_i64),
        ("stream:true n=1", true, 1, 2, 2),
        ("stream:false n=2", false, 2, 4, 4),
        ("stream:true n=2", true, 2, 4, 4),
    ];
    for (name, stream, n, expected_input, expected_output) in cases {
        let before = chat_usage_rows(&s).await.len();
        let response = c
            .post(format!(
                "http://{}/openai/v1/chat/completions",
                s.proxy_addr
            ))
            .bearer_auth(key)
            .json(&json!({
                "model": "gpt-test",
                "messages": [{"role":"user","content":"hi"}],
                "stream": stream,
                "n": n,
            }))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, 200, "{name}: {body}");
        assert!(body.contains("hello"), "{name}: {body}");
        wait_for_chat_usage_rows(&s, before + 1).await;
        let rows = chat_usage_rows(&s).await;
        let (input, output) = rows.last().copied().expect("chat usage row");
        assert_eq!(input, Some(expected_input), "{name}: {rows:?}");
        assert_eq!(output, Some(expected_output), "{name}: {rows:?}");
    }
}

#[tokio::test]
async fn openai_oauth_resolves_sent_service_tier_and_pricing_across_http_and_chat_and_preserves_api_key_tier()
 {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let catalog_guard = common::lock_custom_catalog().await;

    let catalog = json!({
        "gpt-6.2-sol": {
            "litellm_provider": "openai",
            "input_cost_per_token": 0.000002,
            "output_cost_per_token": 0.000008,
            "input_cost_per_token_priority": 0.000004,
            "output_cost_per_token_priority": 0.000016
        }
    });
    let catalog_dir = tempfile::tempdir().expect("catalog dir");
    let catalog_path = catalog_dir.path().join("catalog.json");
    let catalog_json = catalog.to_string();
    std::fs::write(&catalog_path, &catalog_json).expect("write catalog");

    #[derive(Clone)]
    struct TierTestFakeState {
        rec: Arc<Mutex<Vec<(String, Value)>>>,
        seq: Arc<AtomicUsize>,
        catalog: Value,
    }

    let state_for_fake = TierTestFakeState {
        rec: Arc::new(Mutex::new(Vec::new())),
        seq: Arc::new(AtomicUsize::new(0)),
        catalog: catalog.clone(),
    };
    let app = Router::new()
        .fallback(move |State(st): State<TierTestFakeState>, req: Request| async move {
            let path = req.uri().path().to_owned();
            if path == "/catalog.json" {
                return Json(st.catalog).into_response();
            }
            let body = to_bytes(req.into_body(), 2_000_000).await.unwrap();
            let json_body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            let is_client_call = !path.ends_with("/models");
            if path.ends_with("/models") {
                return Json(json!({
                    "data": [{"id": "gpt-6.2-sol", "object": "model"}],
                    "models": [{"slug": "gpt-6.2-sol", "display_name": "GPT 6.2", "description": "test", "capabilities": {}}]
                }))
                .into_response();
            }
            if is_client_call {
                st.rec.lock().unwrap().push((path.clone(), json_body.clone()));
            }
            let resp_id = format!("resp_tier_test_{}", st.seq.fetch_add(1, Ordering::SeqCst));
            if path.ends_with("/v1/responses")
                && json_body.get("stream").and_then(Value::as_bool) != Some(true)
            {
                return Json(json!({
                    "id": resp_id,
                    "object": "response",
                    "status": "completed",
                    "service_tier": "default",
                    "usage": {"input_tokens": 1000, "output_tokens": 500, "total_tokens": 1500},
                    "output": [{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}]
                }))
                .into_response();
            }
            let created_ev = json!({
                "type": "response.created",
                "sequence_number": 0,
                "response": {
                    "id": resp_id,
                    "object": "response",
                    "status": "in_progress",
                    "service_tier": "auto",
                    "output": []
                }
            });
            let completed_ev = json!({
                "type": "response.completed",
                "sequence_number": 4,
                "response": {
                    "id": resp_id,
                    "object": "response",
                    "status": "completed",
                    "service_tier": "default",
                    "usage": {"input_tokens": 1000, "output_tokens": 500, "total_tokens": 1500},
                    "output": []
                }
            });
            let sse = format!(
                "event: response.created\ndata: {created_ev}\n\n\
event: response.output_item.added\ndata: {{\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}}}\n\n\
event: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"ok\"}}\n\n\
event: response.output_item.done\ndata: {{\"type\":\"response.output_item.done\",\"sequence_number\":3,\"output_index\":0,\"item\":{{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{{\"type\":\"output_text\",\"text\":\"ok\",\"annotations\":[]}}]}}}}\n\n\
event: response.completed\ndata: {completed_ev}\n\n"
            );
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(sse))
                .unwrap()
        })
        .with_state(state_for_fake);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fake_addr = listener.local_addr().unwrap();
    let _fake_task = tokio::spawn(async move { axum::serve(listener, app).await });

    let extra_toml = format!(
        "[price_catalog]\nurl = \"http://{fake_addr}/catalog.json\"\ncache_path = \"{}\"\n",
        catalog_path.display()
    );
    let server = common::spawn_inprocess_server_with_custom_catalog(
        &extra_toml,
        Vec::new(),
        fake_anthropic::AppConfig::default(),
        1 << 20,
        &catalog_guard,
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if cc_lb_pricing::global_catalog().current().raw_json == catalog_json.as_bytes() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "custom catalog installation timed out"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let client = reqwest::Client::new();
    let admin_get = |path: String| {
        let client = &client;
        let addr = server.admin_addr;
        let token = server.admin_token;
        async move {
            let res = client
                .get(format!("http://{addr}{path}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap();
            let status = res.status();
            let etag = res
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let json: Value = res.json().await.unwrap();
            (status, json, etag)
        }
    };

    // Disable seeded default upstreams first.
    let (st, ups, _) = admin_get("/admin/v1/upstreams".to_owned()).await;
    assert_eq!(st, 200);
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, _, etag) = admin_get(format!("/admin/v1/upstreams/{id}")).await;
        let res = client
            .patch(format!(
                "http://{}/admin/v1/upstreams/{id}",
                server.admin_addr
            ))
            .bearer_auth(server.admin_token)
            .header("if-match", etag.unwrap())
            .json(&json!({ "enabled": false }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    // Create OAuth upstream pointing at /codex.
    let oauth_res = client
        .post(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth(server.admin_token)
        .json(&json!({
            "name": "oauth-tier-test",
            "kind": "openai_oauth",
            "base_url": format!("http://{fake_addr}/codex"),
            "oauth_tokens": {
                "access_token": "test-access",
                "refresh_token": "",
                "expires_at_unix_secs": 4102444800u64,
                "account_id": "acct-tier-test",
                "scopes": [],
                "never_refresh": true
            },
            "skip_preflight": true
        }))
        .send()
        .await
        .unwrap();
    let oauth_status = oauth_res.status();
    let oauth_body: Value = oauth_res.json().await.unwrap();
    assert_eq!(oauth_status, 201, "{oauth_body}");
    let oauth_id = oauth_body["id"].as_str().unwrap().to_owned();

    let key = &server.managed_key.plaintext;
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .unwrap();

    let send_and_wait = |idx: usize, path: &str, payload: Value| {
        let client = client.clone();
        let proxy_addr = server.proxy_addr;
        let pool = pool.clone();
        let key = key.clone();
        let path = path.to_owned();
        async move {
            let res = client
                .post(format!("http://{proxy_addr}{path}"))
                .bearer_auth(&key)
                .json(&payload)
                .send()
                .await
                .unwrap();
            let status = res.status();
            let body = res.text().await.unwrap();
            assert_eq!(status, 200, "case {idx} {path}: {body}");

            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let rows = sqlx::query(
                    "SELECT event_id, service_tier, list_cost_usd_micros \
                     FROM request_events_v1 \
                     WHERE model = 'gpt-6.2-sol' AND list_status = 200 \
                     ORDER BY id ASC",
                )
                .fetch_all(&pool)
                .await
                .unwrap();
                if rows.len() > idx {
                    let row = &rows[idx];
                    return (
                        row.get::<String, _>("event_id"),
                        row.get::<Option<String>, _>("service_tier"),
                        row.get::<Option<i64>, _>("list_cost_usd_micros"),
                    );
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for row {idx}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };

    let cases: [(&str, Value, &str); 10] = [
        (
            "/openai/v1/responses",
            json!({"model":"gpt-6.2-sol","input":"hi","stream":true,"service_tier":"priority"}),
            "priority",
        ),
        (
            "/openai/v1/responses",
            json!({"model":"gpt-6.2-sol","input":"hi","stream":true,"service_tier":"ultrafast"}),
            "ultrafast",
        ),
        (
            "/openai/v1/responses",
            json!({"model":"gpt-6.2-sol","input":"hi","stream":true,"service_tier":"default"}),
            "default",
        ),
        (
            "/openai/v1/responses",
            json!({"model":"gpt-6.2-sol","input":"hi","stream":false,"service_tier":"priority"}),
            "priority",
        ),
        (
            "/openai/v1/responses",
            json!({"model":"gpt-6.2-sol","input":"hi","stream":false,"service_tier":"flex"}),
            "default",
        ),
        (
            "/openai/v1/responses",
            json!({"model":"gpt-6.2-sol","input":"hi","stream":true}),
            "default",
        ),
        (
            "/openai/v1/chat/completions",
            json!({"model":"gpt-6.2-sol","messages":[{"role":"user","content":"hi"}],"stream":false,"service_tier":"priority"}),
            "priority",
        ),
        (
            "/openai/v1/chat/completions",
            json!({"model":"gpt-6.2-sol","messages":[{"role":"user","content":"hi"}],"stream":true,"service_tier":"priority"}),
            "priority",
        ),
        (
            "/openai/v1/chat/completions",
            json!({"model":"gpt-6.2-sol","messages":[{"role":"user","content":"hi"}],"stream":false,"n":2,"service_tier":"priority"}),
            "priority",
        ),
        (
            "/openai/v1/chat/completions",
            json!({"model":"gpt-6.2-sol","messages":[{"role":"user","content":"hi"}],"stream":false,"service_tier":"flex"}),
            "default",
        ),
    ];

    let mut costs_by_idx = Vec::new();
    let mut event_ids = Vec::new();
    for (idx, (path, payload, expected_tier)) in cases.iter().enumerate() {
        let (event_id, db_tier, cost) = send_and_wait(idx, path, payload.clone()).await;
        assert_eq!(
            db_tier.as_deref(),
            Some(*expected_tier),
            "SQLite case {idx} {path}"
        );

        // Also verify Admin detail API reflects the exact resolved service_tier and cost.
        let (detail_st, detail, _) =
            admin_get(format!("/admin/v1/events/detail?id={event_id}")).await;
        assert_eq!(detail_st, 200);
        assert_eq!(
            detail["service_tier"].as_str(),
            Some(*expected_tier),
            "Admin detail case {idx} {path}: {detail}"
        );
        costs_by_idx.push(cost);
        event_ids.push((event_id, *expected_tier));
    }

    // Check Admin recent API exposes the non-default and default service_tier values accurately.
    let (recent_st, recent, _) = admin_get("/admin/v1/events/recent?limit=50".to_owned()).await;
    assert_eq!(recent_st, 200);
    let recent_items = recent["events"].as_array().unwrap();
    for (event_id, expected_tier) in &event_ids {
        let found = recent_items
            .iter()
            .find(|item| item["event_id"].as_str() == Some(event_id.as_str()))
            .unwrap_or_else(|| panic!("missing {event_id} in recent events: {recent}"));
        assert_eq!(found["service_tier"].as_str(), Some(*expected_tier));
    }

    // 1000 input + 500 output:
    // default cost = 1000 * $2/M + 500 * $8/M = 2000 + 4000 = 6000 micros
    // priority cost = 1000 * $4/M + 500 * $16/M = 4000 + 8000 = 12000 micros
    assert_eq!(costs_by_idx[2], Some(6_000), "default base cost");
    assert_eq!(costs_by_idx[0], Some(12_000), "priority cost");
    assert!(
        costs_by_idx[0].unwrap() > costs_by_idx[2].unwrap(),
        "priority pricing must exceed default pricing: {:?}",
        costs_by_idx
    );

    // Control check: switch to an openai_api_key upstream that returns service_tier:"default"
    // even when client requests "priority". Upstream response MUST remain authoritative!
    let (_, _, oauth_etag) = admin_get(format!("/admin/v1/upstreams/{oauth_id}")).await;
    let disable_res = client
        .patch(format!(
            "http://{}/admin/v1/upstreams/{oauth_id}",
            server.admin_addr
        ))
        .bearer_auth(server.admin_token)
        .header("if-match", oauth_etag.unwrap())
        .json(&json!({ "enabled": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(disable_res.status(), 200);

    let apikey_res = client
        .post(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth(server.admin_token)
        .json(&json!({
            "name": "apikey-tier-control",
            "kind": "openai_api_key",
            "base_url": format!("http://{fake_addr}"),
            "api_key_value": "sk-test-control",
            "skip_preflight": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(apikey_res.status(), 201);

    let (api_stream_id, api_key_stream_tier, api_key_stream_cost) = send_and_wait(
        cases.len(),
        "/openai/v1/responses",
        json!({"model":"gpt-6.2-sol","input":"hi","stream":true,"store":false,"service_tier":"priority"}),
    )
    .await;
    assert_eq!(
        api_key_stream_tier.as_deref(),
        Some("default"),
        "openai_api_key streaming upstream response must not be overridden by sent tier (id={api_stream_id})"
    );
    assert_eq!(api_key_stream_cost, Some(6_000));

    let (api_json_id, api_key_json_tier, api_key_json_cost) = send_and_wait(
        cases.len() + 1,
        "/openai/v1/responses",
        json!({"model":"gpt-6.2-sol","input":"hi","stream":false,"store":false,"service_tier":"priority"}),
    )
    .await;
    assert_eq!(
        api_key_json_tier.as_deref(),
        Some("default"),
        "openai_api_key non-stream upstream response must not be overridden by sent tier (id={api_json_id})"
    );
    assert_eq!(api_key_json_cost, Some(6_000));
}

#[tokio::test]
async fn openai_oauth_chat_fanout_prices_each_choice_independently_regardless_of_order() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let catalog_guard = common::lock_custom_catalog().await;

    let catalog = json!({
        "gpt-6.2-sol": {
            "litellm_provider": "openai",
            "input_cost_per_token": 0.000002,
            "output_cost_per_token": 0.000008,
            "input_cost_per_token_priority": 0.000004,
            "output_cost_per_token_priority": 0.000016
        }
    });
    let catalog_dir = tempfile::tempdir().expect("catalog dir");
    let catalog_path = catalog_dir.path().join("catalog.json");
    let catalog_json = catalog.to_string();
    std::fs::write(&catalog_path, &catalog_json).expect("write catalog");

    #[derive(Clone)]
    struct MixedFanoutFakeState {
        seq: Arc<AtomicUsize>,
        catalog: Value,
    }

    let state = MixedFanoutFakeState {
        seq: Arc::new(AtomicUsize::new(0)),
        catalog: catalog.clone(),
    };
    let app = Router::new()
        .fallback(
            move |State(st): State<MixedFanoutFakeState>, req: Request| async move {
                let path = req.uri().path().to_owned();
                if path == "/catalog.json" {
                    return Json(st.catalog).into_response();
                }
                let _body = to_bytes(req.into_body(), 2_000_000).await.unwrap();
                if path.ends_with("/models") {
                    return Json(json!({
                        "data": [{"id": "gpt-6.2-sol", "object": "model"}],
                        "models": [{"slug": "gpt-6.2-sol", "display_name": "GPT 6.2", "description": "test", "capabilities": {}}]
                    }))
                    .into_response();
                }
                let call_idx = st.seq.fetch_add(1, Ordering::SeqCst);
                let resp_id = format!("resp_fanout_mix_{call_idx}");
                // Call sequence (n=2 twice):
                // req 1: call 0 -> "default", call 1 -> "priority"
                // req 2: call 2 -> "priority", call 3 -> "default"
                let response_tier = match call_idx % 4 {
                    1 | 2 => "priority",
                    _ => "default",
                };
                let sse = format!(
                    "event: response.created\ndata: {{\"type\":\"response.created\",\"sequence_number\":0,\"response\":{{\"id\":\"{resp_id}\",\"object\":\"response\",\"status\":\"in_progress\",\"service_tier\":\"auto\",\"output\":[]}}}}\n\n\
event: response.output_item.added\ndata: {{\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}}}\n\n\
event: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"ok\"}}\n\n\
event: response.output_item.done\ndata: {{\"type\":\"response.output_item.done\",\"sequence_number\":3,\"output_index\":0,\"item\":{{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{{\"type\":\"output_text\",\"text\":\"ok\",\"annotations\":[]}}]}}}}\n\n\
event: response.completed\ndata: {{\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{{\"id\":\"{resp_id}\",\"object\":\"response\",\"status\":\"completed\",\"service_tier\":\"{response_tier}\",\"usage\":{{\"input_tokens\":1000,\"output_tokens\":500,\"total_tokens\":1500}},\"output\":[]}}}}\n\n"
                );
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap()
            },
        )
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fake_addr = listener.local_addr().unwrap();
    let _fake_task = tokio::spawn(async move { axum::serve(listener, app).await });

    let extra_toml = format!(
        "[price_catalog]\nurl = \"http://{fake_addr}/catalog.json\"\ncache_path = \"{}\"\n",
        catalog_path.display()
    );
    let server = common::spawn_inprocess_server_with_custom_catalog(
        &extra_toml,
        Vec::new(),
        fake_anthropic::AppConfig::default(),
        1 << 20,
        &catalog_guard,
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if cc_lb_pricing::global_catalog().current().raw_json == catalog_json.as_bytes() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let client = reqwest::Client::new();
    let ups: Value = client
        .get(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth(server.admin_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let detail = client
            .get(format!(
                "http://{}/admin/v1/upstreams/{id}",
                server.admin_addr
            ))
            .bearer_auth(server.admin_token)
            .send()
            .await
            .unwrap();
        let etag = detail
            .headers()
            .get("etag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let res = client
            .patch(format!(
                "http://{}/admin/v1/upstreams/{id}",
                server.admin_addr
            ))
            .bearer_auth(server.admin_token)
            .header("if-match", etag)
            .json(&json!({ "enabled": false }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    let oauth_res = client
        .post(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth(server.admin_token)
        .json(&json!({
            "name": "oauth-mixed-fanout",
            "kind": "openai_oauth",
            "base_url": format!("http://{fake_addr}/codex"),
            "oauth_tokens": {
                "access_token": "test-access",
                "refresh_token": "",
                "expires_at_unix_secs": 4102444800u64,
                "account_id": "acct-fanout",
                "scopes": [],
                "never_refresh": true
            },
            "skip_preflight": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(oauth_res.status(), 201);

    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .unwrap();

    for idx in 0..2_usize {
        let res = client
            .post(format!(
                "http://{}/openai/v1/chat/completions",
                server.proxy_addr
            ))
            .bearer_auth(&server.managed_key.plaintext)
            .json(&json!({
                "model": "gpt-6.2-sol",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": false,
                "n": 2,
                "service_tier": "default"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);

        let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let rows = sqlx::query(
                "SELECT service_tier, list_cost_usd_micros \
                 FROM request_events_v1 \
                 WHERE model = 'gpt-6.2-sol' AND list_status = 200 \
                 ORDER BY id ASC",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            if rows.len() > idx
                && rows[idx]
                    .get::<Option<i64>, _>("list_cost_usd_micros")
                    .is_some()
            {
                let tier: Option<String> = rows[idx].get("service_tier");
                let cost: Option<i64> = rows[idx].get("list_cost_usd_micros");
                assert_eq!(
                    tier.as_deref(),
                    Some("priority"),
                    "fanout request {idx} must report deterministic non-default tier"
                );
                // Choice default(1000,500) = 6000 + Choice priority(1000,500) = 12000 => 18000 micros
                assert_eq!(
                    cost,
                    Some(18_000),
                    "fanout request {idx} must price each call independently regardless of choice order"
                );
                break;
            }
            assert!(tokio::time::Instant::now() < wait_deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
