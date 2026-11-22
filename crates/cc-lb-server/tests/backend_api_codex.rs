use crate::{common, openai_oauth_public_compat};
use reqwest::Client;
use serde_json::json;

#[tokio::test]
async fn backend_codex_alias_preserves_native_requests_and_catalog_queries() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let fake = openai_oauth_public_compat::spawn().await;
    let client = Client::new();
    openai_oauth_public_compat::setup(&server, &fake.base, &client).await;
    let key = &server.managed_key.plaintext;

    let native = br#"{"model":"gpt-test","input":"raw string","store":null,"metadata":{"x":"y"},"temperature":0.2,"service_tier":"auto","stream":false,"instructions":"keep","include":["reasoning.encrypted_content"],"prompt_cache_key":"cache","tools":[{"type":"local_shell"},{"type":"apply_patch"},{"type":"shell"},{"type":"mcp"},{"type":"computer"}]}"#;
    let response = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses?client=raw",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("chatgpt-account-id", "client-account")
        .header("session_id", "session-test")
        .header("x-codex-turn-state", "turn-test")
        .body(native.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response_body = response.bytes().await.unwrap();
    assert!(response_body.starts_with(b"event: response.created"));
    assert!(
        response_body
            .windows(b"response.metadata".len())
            .any(|w| w == b"response.metadata")
    );

    let recorded = fake.state.0.lock().unwrap().clone();
    let request = recorded
        .iter()
        .rev()
        .find(|(path, headers, _)| {
            path == "/codex/responses" && headers.iter().any(|(name, _)| name == "request-id")
        })
        .expect("alias response reached OAuth backend");
    assert_eq!(request.2, native);
    assert!(
        request
            .1
            .iter()
            .any(|(name, value)| name == "session_id" && value == "session-test")
    );
    assert!(
        request
            .1
            .iter()
            .any(|(name, value)| name == "x-codex-turn-state" && value == "turn-test")
    );
    assert!(
        !request
            .1
            .iter()
            .any(|(name, value)| name == "chatgpt-account-id" && value == "client-account")
    );
    assert!(
        !request
            .1
            .iter()
            .any(|(name, value)| { name == "authorization" && value == &format!("Bearer {key}") })
    );

    let catalog = client
        .get(format!(
            "http://{}/openai/backend-api/codex/models?client_version=X&client=raw",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(catalog.status(), 200);
    let catalog_body = catalog.bytes().await.unwrap();
    assert_eq!(
        catalog_body.as_ref(),
        br#"{"models":[{"slug":"gpt-test","display_name":"GPT Test","description":"test","capabilities":{}},{"slug":"gpt-other","display_name":"GPT Other","description":"other","capabilities":{}}]}"#
    );
    let recorded = fake.state.0.lock().unwrap().clone();
    let model_request = recorded
        .iter()
        .rev()
        .find(|(path, headers, _)| {
            path == "/codex/models" && headers.iter().any(|(name, _)| name == "request-id")
        })
        .expect("alias catalog reached OAuth backend");
    assert_eq!(model_request.0, "/codex/models");
    assert!(
        model_request.1.iter().any(|(name, value)| {
            name == "x-test-query" && value == "client_version=X&client=raw"
        })
    );
    assert!(
        model_request
            .1
            .iter()
            .any(|(name, value)| name == "accept-encoding" && value == "gzip")
    );

    let compact = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses/compact",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .body(br#"{"input":"keep"}"#.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(compact.status(), 404);
    assert!(
        compact
            .text()
            .await
            .unwrap()
            .contains("operation_not_supported")
    );

    let alpha = client
        .post(format!(
            "http://{}/openai/backend-api/codex/alpha/search",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .body(br#"{"q":"native"}"#.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(alpha.status(), 200);
    assert_eq!(
        alpha.bytes().await.unwrap().as_ref(),
        br#"{"results":[{"id":"native"}]}"#
    );

    let sdp = b"v=0\r\nnative";
    let realtime = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("content-type", "application/sdp")
        .body(sdp.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(realtime.status(), 200);
    assert_eq!(
        realtime.bytes().await.unwrap().as_ref(),
        b"native-sdp-response"
    );
    let recorded = fake.state.0.lock().unwrap().clone();
    let alpha_request = recorded
        .iter()
        .find(|(path, _, _)| path == "/codex/alpha/search")
        .unwrap();
    assert_eq!(alpha_request.2, br#"{"q":"native"}"#);
    let realtime_request = recorded
        .iter()
        .find(|(path, _, _)| path == "/codex/realtime/calls")
        .unwrap();
    assert_eq!(realtime_request.2, sdp);
    assert!(
        !realtime_request
            .1
            .iter()
            .any(|(name, _)| name == "openai-alpha")
    );
}

#[tokio::test]
async fn backend_codex_zstd_body_is_decoded_before_native_forwarding() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let fake = openai_oauth_public_compat::spawn().await;
    let client = Client::new();
    openai_oauth_public_compat::setup(&server, &fake.base, &client).await;
    let body = br#"{"model":"gpt-test","input":"zstd","store":false,"stream":false}"#;
    let compressed = zstd::bulk::compress(body, 1).unwrap();
    let response = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .header("content-encoding", "zstd")
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let recorded = fake.state.0.lock().unwrap();
    let request = recorded
        .iter()
        .rev()
        .find(|(path, headers, _)| {
            path == "/codex/responses" && headers.iter().any(|(name, _)| name == "request-id")
        })
        .unwrap();
    assert_eq!(request.2, body);
}

#[tokio::test]
async fn backend_codex_catalog_filters_only_restricted_principal_models() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let fake = openai_oauth_public_compat::spawn().await;
    let client = Client::new();
    openai_oauth_public_compat::setup(&server, &fake.base, &client).await;
    let principals = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
        None,
    )
    .await
    .1;
    let id = principals["principals"][0]["id"].as_str().unwrap();
    let (_, mut detail, etag) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{id}"),
        None,
        None,
    )
    .await;
    detail["allowed_models"] = json!(["gpt-test"]);
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PATCH,
        &format!("/admin/v1/principals/{id}"),
        Some(detail),
        etag.as_deref(),
    )
    .await;
    assert_eq!(status, 200);
    let response = client
        .get(format!(
            "http://{}/openai/backend-api/codex/models?client_version=X",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let catalog: serde_json::Value = response.json().await.unwrap();
    assert_eq!(catalog["models"].as_array().unwrap().len(), 1);
    assert_eq!(catalog["models"][0]["slug"], "gpt-test");
    let denied = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .json(&json!({"model":"gpt-other","input":"denied"}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    let invalid = client
        .get(format!(
            "http://{}/openai/backend-api/codex/models?client_version=invalid",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), 502);
    let recorded = fake.state.0.lock().unwrap().clone();
    let invalid_request = recorded
        .iter()
        .rev()
        .find(|(path, headers, _)| {
            path == "/codex/models"
                && headers.iter().any(|(name, _)| name == "x-test-query")
                && headers.iter().any(|(name, value)| {
                    name == "x-test-query" && value == "client_version=invalid"
                })
        })
        .unwrap();
    assert!(
        !invalid_request
            .1
            .iter()
            .any(|(name, _)| name == "accept-encoding")
    );
    let invalid_sse = client
        .get(format!(
            "http://{}/openai/backend-api/codex/models?client_version=invalid_sse",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_sse.status(), 502);
    let hangup = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls/id/hangup",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(hangup.status(), 404);
    let hangup_body = hangup.text().await.unwrap();
    assert!(
        hangup_body.contains("realtime_call_not_found"),
        "{hangup_body}"
    );
}
#[tokio::test]
async fn backend_api_unknown_paths_authenticate_then_return_local_404() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let fake = openai_oauth_public_compat::spawn().await;
    let client = Client::new();
    openai_oauth_public_compat::setup(&server, &fake.base, &client).await;

    let unauthenticated = client
        .post(format!(
            "http://{}/openai/backend-api/wham/usage",
            server.proxy_addr
        ))
        .body("must not be read")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);
    let unauthenticated_compact = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses/compact",
            server.proxy_addr
        ))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated_compact.status(), 401);
    let unauthenticated_trace = client
        .post(format!(
            "http://{}/openai/backend-api/codex/memories/trace_summarize",
            server.proxy_addr
        ))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated_trace.status(), 401);
    let unauth_root = client
        .get(format!("http://{}/openai/backend-api", server.proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_root.status(), 401);
    let authenticated_root = client
        .get(format!("http://{}/openai/backend-api", server.proxy_addr))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(authenticated_root.status(), 404);
    let root_body = authenticated_root.text().await.unwrap();
    assert!(root_body.contains("operation_not_supported"), "{root_body}");

    let authenticated_wham = client
        .get(format!(
            "http://{}/openai/backend-api/wham/usage",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(authenticated_wham.status(), 404);
    let wham_body = authenticated_wham.text().await.unwrap();
    assert!(wham_body.contains("operation_not_supported"), "{wham_body}");

    let authenticated = client
        .post(format!(
            "http://{}/openai/backend-api/codex/memories/trace_summarize",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(authenticated.status(), 404);
    let body = authenticated.text().await.unwrap();
    assert!(body.contains("operation_not_supported"), "{body}");

    for (method, path) in [
        (reqwest::Method::POST, "/backend-api/codex/responses"),
        (reqwest::Method::GET, "/backend-api/codex/models"),
        (reqwest::Method::POST, "/backend-api/wham/usage"),
    ] {
        let unprefixed = client
            .request(method, format!("http://{}{path}", server.proxy_addr))
            .bearer_auth(&server.managed_key.plaintext)
            .send()
            .await
            .unwrap();
        assert_eq!(unprefixed.status(), 404, "{path}");
    }

    let requests = fake.state.0.lock().unwrap().clone();
    assert!(
        requests
            .iter()
            .all(|(path, _, _)| path != "/codex/wham/usage")
    );
    assert!(
        requests
            .iter()
            .all(|(path, _, _)| path != "/codex/memories/trace_summarize")
    );
    // Ignore background warmup probes on `/codex/responses`, which do not pass
    // through `request_id_middleware` and carry no `request-id` header.
    let before = fake
        .state
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, headers, _)| {
            path == "/codex/responses" && headers.iter().any(|(name, _)| name == "request-id")
        })
        .count();
    let unknown = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .json(&json!({
            "model":"gpt-test","input":"unknown","previous_response_id":"resp-unknown"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 503);
    let after = fake
        .state
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, headers, _)| {
            path == "/codex/responses" && headers.iter().any(|(name, _)| name == "request-id")
        })
        .count();
    assert_eq!(before, after);
    assert!(fake.state.0.lock().unwrap().iter().all(|(path, _, body)| {
        path != "/codex/responses" || !String::from_utf8_lossy(body).contains("resp-unknown")
    }));
}

#[tokio::test]
async fn backend_codex_mixed_pool_fails_over_only_between_oauth_upstreams() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = openai_oauth_public_compat::spawn_mixed_failover_with_first_rate_limit(true).await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    openai_oauth_public_compat::setup(&server, &fake.base, &client).await;
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name":"api-key-mixed","kind":"openai_api_key",
            "base_url":fake.base,"api_key_value":"test-api-key","skip_preflight":true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201);
    let _ = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name":"oauth-second","kind":"openai_oauth","base_url":fake.base,
            "oauth_tokens":{
                "access_token":"test-access-2","refresh_token":"",
                "expires_at_unix_secs":4102444800u64,
                "account_id":"acct-test-2","scopes":[],"never_refresh":true
            },
            "skip_preflight":true
        })),
        None,
    )
    .await;
    let principals = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
        None,
    )
    .await
    .1;
    let principal_id = principals["principals"][0]["id"].as_str().unwrap();
    let (_, _, principal_etag) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PUT,
        &format!("/admin/v1/principals/{principal_id}/router-terminal"),
        Some(json!({"strategy":"first-pick"})),
        principal_etag.as_deref(),
    )
    .await;
    assert_eq!(status, 200);
    let response = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .json(&json!({
            "model":"gpt-test","input":"native","store":true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let _ = response.bytes().await.unwrap();
    assert_eq!(fake.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    let requests = fake.requests.lock().unwrap().clone();
    assert!(requests.iter().all(|headers| {
        headers
            .iter()
            .all(|(name, value)| name != "authorization" || value != "Bearer test-api-key")
    }));
    assert!(requests.iter().any(|headers| {
        headers
            .iter()
            .any(|(name, value)| name == "chatgpt-account-id" && value == "acct-test-2")
    }));
}

#[tokio::test]
async fn backend_codex_api_key_only_principal_has_no_eligible_upstream() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let fake = openai_oauth_public_compat::spawn().await;
    let client = Client::new();
    openai_oauth_public_compat::setup(&server, &fake.base, &client).await;
    let upstreams = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await
    .1;
    for upstream in upstreams["upstreams"].as_array().unwrap() {
        let id = upstream["id"].as_str().unwrap();
        let (_, _, etag) = openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let _ = openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
    }
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name":"api-key-only",
            "kind":"openai_api_key",
            "api_key_value":"test-api-key",
            "skip_preflight":true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201);
    let response = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .json(&json!({"model":"gpt-test","input":"no oauth"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let response_body = response.text().await.unwrap();
    assert!(
        response_body.contains("route_no_upstream_after_filter")
            || response_body.contains("no upstream candidates")
    );
    assert!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|(path, headers, _)| {
                !(path == "/codex/responses"
                    && headers.iter().any(|(name, _)| name == "request-id"))
            })
    );
}

#[derive(Clone, Debug)]
pub(crate) struct RecordedCodexRequest {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) query: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

#[derive(Clone, Default)]
pub(crate) struct CodexMediaRealtimeState(
    pub(crate) std::sync::Arc<std::sync::Mutex<Vec<RecordedCodexRequest>>>,
);

pub(crate) struct CodexMediaRealtimeFake {
    pub(crate) origin: String,
    pub(crate) base: String,
    pub(crate) state: CodexMediaRealtimeState,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for CodexMediaRealtimeFake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CodexMediaRealtimeFake {
    /// Recorded upstream requests excluding background scheduler OAuth usage
    /// polls (`GET /backend-api/wham/usage` without `request-id`), which
    /// `POST /admin/v1/upstreams` seeds automatically on OAuth import.
    pub(crate) fn proxy_requests(&self) -> Vec<RecordedCodexRequest> {
        self.state
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|req| !is_background_usage_poll(req))
            .cloned()
            .collect()
    }
}

fn is_background_usage_poll(req: &RecordedCodexRequest) -> bool {
    req.method == "GET"
        && req.path == "/backend-api/wham/usage"
        && req.headers.iter().all(|(name, _)| name != "request-id")
}

async fn codex_media_realtime_ws(
    axum::extract::State(state): axum::extract::State<CodexMediaRealtimeState>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: axum::http::HeaderMap,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> axum::response::Response {
    use axum::extract::ws::{Message, WebSocket};
    let path = uri.path().to_owned();
    let query = uri.query().unwrap_or_default().to_owned();
    let captured_headers = headers
        .iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_owned())))
        .collect::<Vec<_>>();
    let alpha = headers
        .get("openai-alpha")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    state.0.lock().unwrap().push(RecordedCodexRequest {
        method: "WS".to_owned(),
        path: path.clone(),
        query: query.clone(),
        headers: captured_headers.clone(),
        body: Vec::new(),
    });
    ws.on_upgrade(move |mut socket: WebSocket| async move {
        if path == "/v1/realtime" || path == "/v1/live" || path.starts_with("/v1/live/") {
            let _ = socket
                .send(Message::Text(
                    json!({
                        "type": "session.created",
                        "path": path,
                        "query": query,
                        "alpha": alpha,
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            return;
        }
        if let Some(Ok(Message::Text(text))) = socket.recv().await {
            state.0.lock().unwrap().push(RecordedCodexRequest {
                method: "WS_FRAME".to_owned(),
                path,
                query,
                headers: captured_headers,
                body: text.as_bytes().to_vec(),
            });
            let _ = socket
                .send(Message::Text(
                    r#"{"type":"response.created","response":{"id":"resp_ws","object":"response","status":"in_progress"}}"#
                        .into(),
                ))
                .await;
            let _ = socket
                .send(Message::Text(
                    r#"{"type":"response.completed","response":{"id":"resp_ws","object":"response","status":"completed","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}"#
                        .into(),
                ))
                .await;
        }
    })
}

async fn codex_media_realtime_http(
    axum::extract::State(state): axum::extract::State<CodexMediaRealtimeState>,
    req: axum::extract::Request,
) -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;

    let method = req.method().to_string();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or_default().to_owned();
    let headers = req
        .headers()
        .iter()
        .filter_map(|(k, v)| Some((k.to_string(), v.to_str().ok()?.to_owned())))
        .collect::<Vec<_>>();
    let body = axum::body::to_bytes(req.into_body(), 10_000_000)
        .await
        .unwrap()
        .to_vec();
    state.0.lock().unwrap().push(RecordedCodexRequest {
        method,
        path: path.clone(),
        query: query.clone(),
        headers,
        body,
    });
    match path.as_str() {
        "/backend-api/codex/images/generations" => (
            StatusCode::OK,
            [("x-codex-imagegen-request-id", "img_gen_123")],
            axum::Json(json!({
                "created": 1778832973u64,
                "data": [{"b64_json": "ZmFrZQ==", "generation_id": "gen-1"}]
            })),
        )
            .into_response(),
        "/backend-api/codex/images/edits" => (
            StatusCode::OK,
            [("x-codex-imagegen-request-id", "img_edit_456")],
            axum::Json(json!({
                "created": 1778832974u64,
                "data": [{"b64_json": "ZWRpdGVk", "generation_id": "gen-2"}]
            })),
        )
            .into_response(),
        "/backend-api/codex/realtime/calls" => {
            if query.contains("status=200") {
                (
                    StatusCode::OK,
                    [(
                        header::LOCATION,
                        "/v1/realtime/calls/calls/rtc_codex_200?foo=bar",
                    )],
                    "v=0\r\nstatus200",
                )
                    .into_response()
            } else {
                (
                    StatusCode::CREATED,
                    [(header::LOCATION, "/v1/realtime/calls/rtc_codex_1")],
                    "v=0\r\ncreated",
                )
                    .into_response()
            }
        }
        "/v1/realtime/calls" => (
            StatusCode::CREATED,
            [(header::LOCATION, "/v1/realtime/calls/rtc_api_key_only")],
            "v=0\r\napikey",
        )
            .into_response(),
        "/v1/realtime/calls/rtc_codex_1/hangup"
        | "/v1/realtime/calls/rtc_codex_200/hangup"
        | "/v1/realtime/calls/rtc_api_key_only/hangup" => StatusCode::NO_CONTENT.into_response(),
        _ => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"detail": "not found"})),
        )
            .into_response(),
    }
}

pub(crate) async fn spawn_codex_media_realtime_fake() -> CodexMediaRealtimeFake {
    let state = CodexMediaRealtimeState::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = state.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route(
                    "/backend-api/codex/responses",
                    axum::routing::get(codex_media_realtime_ws),
                )
                .route("/v1/realtime", axum::routing::get(codex_media_realtime_ws))
                .route("/v1/live", axum::routing::get(codex_media_realtime_ws))
                .route(
                    "/v1/live/{call_id}",
                    axum::routing::get(codex_media_realtime_ws),
                )
                .fallback(codex_media_realtime_http)
                .with_state(server_state),
        )
        .await
    });
    CodexMediaRealtimeFake {
        origin: format!("http://{addr}/"),
        base: format!("http://{addr}/backend-api/codex"),
        state,
        task,
    }
}

async fn disable_all_seeded_upstreams(client: &Client, server: &common::TestServer) {
    let upstreams = openai_oauth_public_compat::admin(
        client,
        server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await
    .1;
    for upstream in upstreams["upstreams"].as_array().unwrap() {
        let id = upstream["id"].as_str().unwrap();
        let (_, _, etag) = openai_oauth_public_compat::admin(
            client,
            server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let (status, _, _) = openai_oauth_public_compat::admin(
            client,
            server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
        assert_eq!(status, 200);
    }
}

pub(crate) async fn add_oauth_upstream(
    client: &Client,
    server: &common::TestServer,
    name: &str,
    base: &str,
    account_id: &str,
) {
    let (status, body, _) = openai_oauth_public_compat::admin(
        client,
        server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": name,
            "kind": "openai_oauth",
            "base_url": base,
            "oauth_tokens": {
                "access_token": format!("access-{account_id}"),
                "refresh_token": "",
                "expires_at_unix_secs": 4102444800u64,
                "account_id": account_id,
                "scopes": [],
                "never_refresh": true
            },
            "warmup_enabled": false,
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201, "{body}");
}

#[tokio::test]
async fn backend_codex_images_generations_and_edits_preserve_native_contract() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let fake = spawn_codex_media_realtime_fake().await;
    let client = Client::new();
    disable_all_seeded_upstreams(&client, &server).await;
    add_oauth_upstream(
        &client,
        &server,
        "oauth-codex-images",
        &fake.base,
        "acct-img",
    )
    .await;
    let key = &server.managed_key.plaintext;

    for path in [
        "/openai/backend-api/codex/images/generations",
        "/openai/backend-api/codex/images/edits",
    ] {
        let unauth = client
            .post(format!("http://{}{path}", server.proxy_addr))
            .json(&json!({"model": "gpt-image-2", "prompt": "blocked"}))
            .send()
            .await
            .unwrap();
        assert_eq!(unauth.status(), 401, "{path}");
    }
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    let gen_payload = br#"{"model":"gpt-image-2","prompt":"a red fox","output":"png","background":"transparent","quality":"high","size":"1024x1024","stream":true}"#;
    let gen_resp = client
        .post(format!(
            "http://{}/openai/backend-api/codex/images/generations",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("x-openai-actor-authorization", "1")
        .header("x-codex-turn-state", "turn-img")
        .body(gen_payload.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(gen_resp.status(), 200);
    assert_eq!(
        gen_resp
            .headers()
            .get("x-codex-imagegen-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("img_gen_123")
    );
    let gen_json: serde_json::Value = gen_resp.json().await.unwrap();
    assert_eq!(
        gen_json,
        json!({
            "created": 1778832973u64,
            "data": [{"b64_json": "ZmFrZQ==", "generation_id": "gen-1"}]
        })
    );

    let edit_payload = br#"{"model":"gpt-image-2","images":[{"image_url":"data:image/png;base64,Zm9v"},{"file_id":"file-image"}],"prompt":"add a red hat"}"#;
    let edit_resp = client
        .post(format!(
            "http://{}/openai/backend-api/codex/images/edits",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("x-openai-actor-authorization", "1")
        .body(edit_payload.as_slice())
        .send()
        .await
        .unwrap();
    assert_eq!(edit_resp.status(), 200);
    assert_eq!(
        edit_resp
            .headers()
            .get("x-codex-imagegen-request-id")
            .and_then(|v| v.to_str().ok()),
        Some("img_edit_456")
    );
    let edit_json: serde_json::Value = edit_resp.json().await.unwrap();
    assert_eq!(
        edit_json,
        json!({
            "created": 1778832974u64,
            "data": [{"b64_json": "ZWRpdGVk", "generation_id": "gen-2"}]
        })
    );

    {
        let recorded = fake.state.0.lock().unwrap();
        let gen_req = recorded
            .iter()
            .find(|req| req.path == "/backend-api/codex/images/generations")
            .expect("generations reached upstream");
        assert_eq!(gen_req.body, gen_payload);
        assert!(
            gen_req
                .headers
                .iter()
                .all(|(name, _)| name != "x-openai-actor-authorization")
        );
        assert!(
            gen_req
                .headers
                .iter()
                .any(|(name, value)| name == "x-codex-turn-state" && value == "turn-img")
        );

        let edit_req = recorded
            .iter()
            .find(|req| req.path == "/backend-api/codex/images/edits")
            .expect("edits reached upstream");
        assert_eq!(edit_req.body, edit_payload);
        assert!(
            edit_req
                .headers
                .iter()
                .all(|(name, _)| name != "x-openai-actor-authorization")
        );
    }

    let principals = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
        None,
    )
    .await
    .1;
    let principal_id = principals["principals"][0]["id"].as_str().unwrap();
    let (_, mut detail, etag) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    detail["allowed_models"] = json!(["gpt-image-2"]);
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PATCH,
        &format!("/admin/v1/principals/{principal_id}"),
        Some(detail),
        etag.as_deref(),
    )
    .await;
    assert_eq!(status, 200);

    for (path, body) in [
        (
            "/openai/backend-api/codex/images/generations",
            json!({"model": "gpt-image-1.5", "prompt": "denied"}),
        ),
        (
            "/openai/backend-api/codex/images/edits",
            json!({"model": "gpt-image-1.5", "images": [{"file_id": "f"}], "prompt": "denied"}),
        ),
    ] {
        let denied = client
            .post(format!("http://{}{path}", server.proxy_addr))
            .bearer_auth(key)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), 403, "{path}");
    }
    let allowed = client
        .post(format!(
            "http://{}/openai/backend-api/codex/images/edits",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({"model": "gpt-image-2", "images": [{"file_id": "f"}], "prompt": "allowed"}))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), 200);
}

#[tokio::test]
async fn backend_codex_images_edits_uses_files_cap_and_rejects_api_key_only_pool() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server().await;
    let fake = spawn_codex_media_realtime_fake().await;
    let client = Client::new();
    disable_all_seeded_upstreams(&client, &server).await;
    add_oauth_upstream(&client, &server, "oauth-cap", &fake.base, "acct-cap").await;
    let key = &server.managed_key.plaintext;

    let large_prompt = "x".repeat(320);
    let gen_over_messages_cap = client
        .post(format!(
            "http://{}/openai/backend-api/codex/images/generations",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({"model": "gpt-image-2", "prompt": large_prompt}))
        .send()
        .await
        .unwrap();
    assert_eq!(gen_over_messages_cap.status(), 413);

    let edit_over_messages_cap = client
        .post(format!(
            "http://{}/openai/backend-api/codex/images/edits",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({
            "model": "gpt-image-2",
            "images": [{"image_url": format!("data:image/png;base64,{large_prompt}")}],
            "prompt": "edit"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(edit_over_messages_cap.status(), 200);

    disable_all_seeded_upstreams(&client, &server).await;
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": "api-key-only-images",
            "kind": "openai_api_key",
            "base_url": fake.base,
            "api_key_value": "test-api-key",
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201);

    let gen_count_before = fake
        .state
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.path == "/backend-api/codex/images/generations")
        .count();
    let edit_count_before = fake
        .state
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.path == "/backend-api/codex/images/edits")
        .count();

    for path in [
        "/openai/backend-api/codex/images/generations",
        "/openai/backend-api/codex/images/edits",
    ] {
        let resp = client
            .post(format!("http://{}{path}", server.proxy_addr))
            .bearer_auth(key)
            .json(&json!({"model": "gpt-image-2", "prompt": "ok"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503, "{path}");
    }

    let recorded = fake.state.0.lock().unwrap();
    assert_eq!(
        recorded
            .iter()
            .filter(|r| r.path == "/backend-api/codex/images/generations")
            .count(),
        gen_count_before
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|r| r.path == "/backend-api/codex/images/edits")
            .count(),
        edit_count_before
    );
}

#[tokio::test]
async fn backend_codex_strips_actor_authorization_header_on_responses() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let fake = openai_oauth_public_compat::spawn().await;
    let client = Client::new();
    openai_oauth_public_compat::setup(&server, &fake.base, &client).await;
    let key = &server.managed_key.plaintext;

    let response = client
        .post(format!(
            "http://{}/openai/backend-api/codex/responses",
            server.proxy_addr
        ))
        .header("x-api-key", key)
        .header("x-openai-actor-authorization", "1")
        .json(&json!({"model": "gpt-test", "input": "strip-actor"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let _ = response.bytes().await.unwrap();

    let recorded = fake.state.0.lock().unwrap();
    let request = recorded
        .iter()
        .rev()
        .find(|(path, headers, _)| {
            path == "/codex/responses" && headers.iter().any(|(name, _)| name == "request-id")
        })
        .expect("responses reached upstream");
    assert!(
        request
            .1
            .iter()
            .all(|(name, _)| name != "x-openai-actor-authorization" && name != "x-api-key")
    );
}

#[tokio::test]
async fn backend_codex_realtime_call_pins_hangup_to_owning_oauth_upstream() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget(&format!(
        "[openai_public_api]\nrealtime_origin = \"{}\"\n",
        fake.origin
    ))
    .await;
    let client = Client::new();
    disable_all_seeded_upstreams(&client, &server).await;
    add_oauth_upstream(&client, &server, "oauth-first", &fake.base, "acct-first").await;
    add_oauth_upstream(&client, &server, "oauth-second", &fake.base, "acct-second").await;

    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": "api-key-mixed",
            "kind": "openai_api_key",
            "base_url": format!("{}v1", fake.origin),
            "api_key_value": "test-api-key",
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201);

    let principals = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
        None,
    )
    .await
    .1;
    let principal_id = principals["principals"][0]["id"].as_str().unwrap();
    let (_, _, etag) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PUT,
        &format!("/admin/v1/principals/{principal_id}/router-terminal"),
        Some(json!({"strategy": "first-pick"})),
        etag.as_deref(),
    )
    .await;
    assert_eq!(status, 200);

    let key = &server.managed_key.plaintext;
    let unauth_hangup = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls/rtc_codex_1/hangup",
            server.proxy_addr
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_hangup.status(), 401);
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    let create_201 = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("x-openai-actor-authorization", "1")
        .json(&json!({"sdp": "v=offer\r\n", "session": {"type": "realtime"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(create_201.status(), 201);
    assert_eq!(
        create_201
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok()),
        Some("/v1/realtime/calls/rtc_codex_1")
    );

    let hangup_201 = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls/rtc_codex_1/hangup",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("x-openai-actor-authorization", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(hangup_201.status(), 204);

    let create_200 = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls?status=200",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .json(&json!({"sdp": "v=offer200\r\n", "session": {"type": "realtime"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(create_200.status(), 200);

    let hangup_200 = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls/rtc_codex_200/hangup",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    assert_eq!(hangup_200.status(), 204);

    let recorded = fake.state.0.lock().unwrap().clone();
    let call_201_req = recorded
        .iter()
        .find(|r| r.path == "/backend-api/codex/realtime/calls" && !r.query.contains("status=200"))
        .expect("call 201 reached upstream");
    let call_account = call_201_req
        .headers
        .iter()
        .find(|(name, _)| name == "chatgpt-account-id")
        .map(|(_, v)| v.as_str())
        .expect("oauth account header present");
    assert!(
        call_201_req
            .headers
            .iter()
            .all(|(name, _)| name != "x-openai-actor-authorization")
    );

    let hangup_201_req = recorded
        .iter()
        .find(|r| r.path == "/v1/realtime/calls/rtc_codex_1/hangup")
        .expect("hangup 201 reached realtime_origin");
    assert!(
        hangup_201_req
            .headers
            .iter()
            .any(|(name, value)| name == "chatgpt-account-id" && value == call_account)
    );
    assert!(
        hangup_201_req
            .headers
            .iter()
            .all(|(name, _)| name != "x-openai-actor-authorization")
    );

    let hangup_200_req = recorded
        .iter()
        .find(|r| r.path == "/v1/realtime/calls/rtc_codex_200/hangup")
        .expect("hangup 200 reached realtime_origin");
    assert_eq!(hangup_200_req.method, "POST");
}

#[tokio::test]
async fn backend_codex_hangup_never_routes_to_api_key_owner() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget(&format!(
        "[openai_public_api]\nrealtime_origin = \"{}\"\n",
        fake.origin
    ))
    .await;
    let client = Client::new();
    disable_all_seeded_upstreams(&client, &server).await;
    let (status, _, _) = openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": "api-key-owner",
            "kind": "openai_api_key",
            "base_url": format!("{}v1", fake.origin),
            "api_key_value": "test-api-key",
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201);

    let key = &server.managed_key.plaintext;
    let public_create = client
        .post(format!(
            "http://{}/openai/v1/realtime/calls",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .header("content-type", "application/sdp")
        .body("v=0\r\n")
        .send()
        .await
        .unwrap();
    assert_eq!(public_create.status(), 201);

    let codex_hangup = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls/rtc_api_key_only/hangup",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    assert_eq!(codex_hangup.status(), 503);
    let body = codex_hangup.text().await.unwrap();
    assert!(body.contains("upstream_affinity_unavailable"), "{body}");
    assert!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.path != "/v1/realtime/calls/rtc_api_key_only/hangup")
    );
}
