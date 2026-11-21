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
            "http://{}/backend-api/codex/responses?client=raw",
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
            "http://{}/backend-api/codex/models?client_version=X&client=raw",
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
            "http://{}/backend-api/codex/responses/compact",
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
            "http://{}/backend-api/codex/alpha/search",
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
            "http://{}/backend-api/codex/realtime/calls",
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
            "http://{}/backend-api/codex/responses",
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
        reqwest::Method::PUT,
        &format!("/admin/v1/principals/{id}"),
        Some(detail),
        etag.as_deref(),
    )
    .await;
    assert_eq!(status, 200);
    let response = client
        .get(format!(
            "http://{}/backend-api/codex/models?client_version=X",
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
            "http://{}/backend-api/codex/responses",
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
            "http://{}/backend-api/codex/models?client_version=invalid",
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
            "http://{}/backend-api/codex/models?client_version=invalid_sse",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_sse.status(), 502);
    let hangup = client
        .post(format!(
            "http://{}/backend-api/codex/realtime/calls/id/hangup",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(hangup.status(), 404);
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
            "http://{}/backend-api/wham/usage",
            server.proxy_addr
        ))
        .body("must not be read")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);
    let unauthenticated_compact = client
        .post(format!(
            "http://{}/backend-api/codex/responses/compact",
            server.proxy_addr
        ))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated_compact.status(), 401);
    let unauthenticated_trace = client
        .post(format!(
            "http://{}/backend-api/codex/memories/trace_summarize",
            server.proxy_addr
        ))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated_trace.status(), 401);
    let authenticated = client
        .post(format!(
            "http://{}/backend-api/codex/memories/trace_summarize",
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
    let before = fake
        .state
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path == "/codex/responses")
        .count();
    let unknown = client
        .post(format!(
            "http://{}/backend-api/codex/responses",
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
        .filter(|(path, _, _)| path == "/codex/responses")
        .count();
    assert_eq!(before, after);
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
            "http://{}/backend-api/codex/responses",
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
            reqwest::Method::POST,
            &format!("/admin/v1/upstreams/{id}/disable"),
            None,
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
            "http://{}/backend-api/codex/responses",
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
