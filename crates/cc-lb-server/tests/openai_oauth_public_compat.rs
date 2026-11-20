#[path = "common.rs"]
mod common;

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use reqwest::Method;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};

#[derive(Clone, Default)]
struct FakeState(Arc<Mutex<Vec<(String, Vec<(String, String)>, Vec<u8>)>>>);
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
    let headers = req
        .headers()
        .iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_owned())))
        .collect();
    let body = to_bytes(req.into_body(), 2_000_000).await.unwrap().to_vec();
    state.0.lock().unwrap().push((path.clone(), headers, body));
    if path == "/codex/models" {
        if !state
            .0
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .1
            .iter()
            .any(|(k, _)| k == "client_version")
        {
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
        if serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|v| v.get("force_error").cloned())
            .is_some()
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail":"Unsupported parameter: x"})),
            )
                .into_response();
        }
        let s = concat!(
            "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"status\":\"in_progress\"}}\n\n",
            "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp-test\"}}\n\n",
            "event: response.metadata\ndata: {\"type\":\"response.metadata\",\"response\":{\"id\":\"resp-test\",\"metadata\":{\"x\":\"y\"}}}\n\n",
            "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\"}}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello \"}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"world\"}\n\n",
            "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\"}}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":2,\"total_tokens\":4},\"output\":[]}}\n\n"
        );
        return ([(header::CONTENT_TYPE, "text/event-stream")], Body::from(s)).into_response();
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
async fn setup(s: &common::TestServer, f: &Fake, c: &reqwest::Client) -> String {
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
    let body = json!({"name":"oauth-test","kind":"openai_oauth","base_url":f.base.clone(),"oauth_tokens":{"access_token":"test-access","refresh_token":"","expires_at_unix_secs":4102444800u64,"account_id":"acct-test","never_refresh":true},"skip_preflight":true});
    let (st, v, _) = admin(c, s, Method::POST, "/admin/v1/upstreams", Some(body), None).await;
    assert_eq!(st, 201, "{v}");
    v["id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn openai_oauth_public_responses_and_models() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server().await;
    let f = spawn().await;
    let c = reqwest::Client::new();
    setup(&s, &f, &c).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    let key = &s.managed_key.plaintext;
    let r = c
        .post(format!("http://{}/openai/v1/responses", s.proxy_addr))
        .bearer_auth(key)
        .json(
            &json!({"model":"gpt-test","input":"hi","stream":false,"store":true,"temperature":0.2}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let text = r.text().await.unwrap();
    assert!(text.contains("hello world"), "{text}");
    let rec = f.state.0.lock().unwrap().clone();
    assert_eq!(rec.len(), 1);
    let v: Value = serde_json::from_slice(&rec[0].2).unwrap();
    assert!(v["input"].is_array());
    assert_eq!(v["stream"], true);
    assert_eq!(v["store"], false);
    assert!(v.get("temperature").is_none());
    let r = c
        .get(format!("http://{}/openai/v1/models", s.proxy_addr))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("gpt-test"));
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
async fn openai_oauth_chat_completions_public_contract() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server().await;
    let f = spawn().await;
    let c = reqwest::Client::new();
    setup(&s, &f, &c).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    let key = &s.managed_key.plaintext;
    let r = c
        .post(format!(
            "http://{}/openai/v1/chat/completions",
            s.proxy_addr
        ))
        .bearer_auth(key)
        .json(
            &json!({"model":"gpt-test","messages":[{"role":"user","content":"hi"}],"stream":false}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert!(r.text().await.unwrap().contains("hello world"));
}
