use crate::common;

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
            "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
            "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":1,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
            "event: response.metadata\ndata: {\"type\":\"response.metadata\",\"sequence_number\":2,\"metadata\":{\"x\":\"y\"}}\n\n",
            "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":3,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello \"}\n\n",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":5,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"world\"}\n\n",
            "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello world\",\"annotations\":[]}]}}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":2,\"total_tokens\":4},\"output\":[]}}\n\n"
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
    let body = json!({"name":"oauth-test","kind":"openai_oauth","base_url":f.base.clone(),"oauth_tokens":{"access_token":"test-access","refresh_token":"","expires_at_unix_secs":4102444800u64,"account_id":"acct-test","scopes":[],"never_refresh":true},"skip_preflight":true});
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
async fn openai_oauth_chat_completions_public_contract() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
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
