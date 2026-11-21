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
struct FakeState(Arc<Mutex<Vec<Recorded>>>);
struct Fake {
    base: String,
    state: FakeState,
    task: JoinHandle<Result<(), std::io::Error>>,
}
impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fake(State(state): State<FakeState>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let has_client_version = req.uri().query().is_some_and(|query| {
        query
            .split('&')
            .any(|pair| pair.starts_with("client_version="))
    });
    let headers = req
        .headers()
        .iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_owned())))
        .collect();
    let body = to_bytes(req.into_body(), 2_000_000).await.unwrap().to_vec();
    state.0.lock().unwrap().push((path.clone(), headers, body));
    if path == "/codex/models" {
        // The real backend rejects a catalog request without `client_version`.
        if !has_client_version {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail":"client_version required"})),
            )
                .into_response();
        }
        return Json(json!({"models":[{"slug":"gpt-test","display_name":"GPT Test","description":"test","capabilities":{}}]})).into_response();
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
async fn spawn() -> Fake {
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
struct FailoverFake {
    base: String,
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
    mixed_first_success: bool,
}

async fn failover_fake(State(state): State<FailoverState>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
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
            && if state.mixed_first_success {
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

async fn spawn_mixed_failover() -> FailoverFake {
    let state = FailoverState {
        calls: Arc::new(AtomicUsize::new(0)),
        mixed_first_success: true,
    };
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(l, Router::new().fallback(failover_fake).with_state(state)).await
    });
    FailoverFake {
        base: format!("http://{a}/codex"),
        task,
    }
}

async fn admin(
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
async fn setup(s: &common::TestServer, base: &str, c: &reqwest::Client) -> String {
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
            Method::POST,
            &format!("/admin/v1/upstreams/{id}/disable"),
            None,
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
