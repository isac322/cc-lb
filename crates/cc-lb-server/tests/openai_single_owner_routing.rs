//! Account-owned OpenAI resources route only to the one API account that can
//! own them: a recorded owner first, otherwise the single eligible API-key
//! upstream. cc-lb never guesses between accounts.

use crate::common;

use axum::{
    Json, Router,
    extract::{Request, State},
    response::{IntoResponse, Response},
};
use reqwest::Method;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};

/// One fake OpenAI account: its name and the request paths it served.
#[derive(Clone)]
struct Account {
    name: &'static str,
    paths: Arc<Mutex<Vec<String>>>,
}

struct Fake {
    base: String,
    account: Account,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(State(account): State<Account>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    account.paths.lock().unwrap().push(path.clone());
    let response_id = format!("resp_{}", account.name);
    if path == "/v1/responses" {
        return Json(json!({
            "id": response_id,
            "object": "response",
            "status": "completed",
            "output": [],
        }))
        .into_response();
    }
    if path == format!("/v1/responses/{response_id}") {
        return Json(json!({"id": response_id, "object": "response", "owner": account.name}))
            .into_response();
    }
    if path == "/v1/files" {
        return Json(json!({"object": "list", "data": [], "owner": account.name})).into_response();
    }
    (
        reqwest::StatusCode::NOT_FOUND,
        Json(json!({"error": {"message": "not found"}})),
    )
        .into_response()
}

async fn spawn_account(name: &'static str) -> Fake {
    let account = Account {
        name,
        paths: Arc::default(),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = account.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(serve).with_state(state)).await
    });
    Fake {
        base: format!("http://{addr}/"),
        account,
        task,
    }
}

impl Fake {
    fn served(&self) -> Vec<String> {
        self.account.paths.lock().unwrap().clone()
    }
}

async fn admin(
    c: &reqwest::Client,
    s: &common::TestServer,
    method: Method,
    path: &str,
    body: Option<Value>,
    etag: Option<&str>,
) -> (reqwest::StatusCode, Value, Option<String>) {
    let mut request = c
        .request(method, format!("http://{}{}", s.admin_addr, path))
        .bearer_auth("admin-token");
    if let Some(etag) = etag {
        request = request.header("if-match", etag);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let etag = response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    (status, response.json().await.unwrap_or(Value::Null), etag)
}

async fn disable_seeded_upstreams(c: &reqwest::Client, s: &common::TestServer) {
    let (status, upstreams, _) = admin(c, s, Method::GET, "/admin/v1/upstreams", None, None).await;
    assert_eq!(status, 200);
    for upstream in upstreams["upstreams"].as_array().unwrap() {
        let id = upstream["id"].as_str().unwrap();
        let (_, _, etag) = admin(
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
            etag.as_deref(),
        )
        .await;
    }
}

async fn add_api_key_upstream(c: &reqwest::Client, s: &common::TestServer, fake: &Fake) {
    let body = json!({
        "name": format!("openai-{}", fake.account.name),
        "kind": "openai_api_key",
        "base_url": fake.base,
        "api_key_value": format!("sk-{}", fake.account.name),
        "skip_preflight": true,
    });
    let (status, value, _) =
        admin(c, s, Method::POST, "/admin/v1/upstreams", Some(body), None).await;
    assert_eq!(status, 201, "{value}");
    // The proxy picks up the new upstream on its next dynamic-view reload.
    tokio::time::sleep(Duration::from_millis(350)).await;
}

async fn proxy(
    c: &reqwest::Client,
    s: &common::TestServer,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> (reqwest::StatusCode, Value) {
    let mut request = c
        .request(method, format!("http://{}/openai{path}", s.proxy_addr))
        .bearer_auth(&s.managed_key.plaintext);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    (status, response.json().await.unwrap_or(Value::Null))
}

#[tokio::test]
async fn account_owned_resources_route_only_to_a_single_owner() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
    let c = reqwest::Client::new();
    disable_seeded_upstreams(&c, &s).await;
    let a = spawn_account("a").await;
    let b = spawn_account("b").await;

    // No OpenAI API-key upstream: account-owned operations stay unsupported.
    let (status, body) = proxy(&c, &s, Method::GET, "/v1/files", None).await;
    assert_eq!(status, 404, "{body}");

    // Exactly one eligible API account owns every account-owned resource.
    add_api_key_upstream(&c, &s, &a).await;
    let (status, body) = proxy(&c, &s, Method::GET, "/v1/files", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["owner"], "a");

    // A response created through cc-lb records its owner.
    let (status, created) = proxy(
        &c,
        &s,
        Method::POST,
        "/v1/responses",
        Some(json!({"model": "gpt-test", "input": "hi"})),
    )
    .await;
    assert_eq!(status, 200, "{created}");
    assert_eq!(created["id"], "resp_a");

    // With a second account, an owner can no longer be inferred.
    add_api_key_upstream(&c, &s, &b).await;
    let before = (a.served().len(), b.served().len());
    let (status, body) = proxy(&c, &s, Method::GET, "/v1/files", None).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], "upstream_affinity_unavailable");
    let (status, body) = proxy(&c, &s, Method::GET, "/v1/responses/resp_unknown", None).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(
        (a.served().len(), b.served().len()),
        before,
        "an ambiguous owner reaches no account"
    );

    // The recorded owner still serves its own response.
    let (status, body) = proxy(&c, &s, Method::GET, "/v1/responses/resp_a", None).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["owner"], "a");
    assert!(!b.served().iter().any(|path| path.contains("resp_a")));
}

#[tokio::test]
async fn realtime_hangup_for_an_unknown_call_reaches_no_account() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let s = common::spawn_test_server_with_openai_budget("").await;
    let c = reqwest::Client::new();
    disable_seeded_upstreams(&c, &s).await;
    let a = spawn_account("a").await;
    add_api_key_upstream(&c, &s, &a).await;

    let (status, body) = proxy(
        &c,
        &s,
        Method::POST,
        "/v1/realtime/calls/rtc_unknown/hangup",
        None,
    )
    .await;
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["error"]["code"], "realtime_call_not_found");
    assert!(a.served().is_empty());
}
