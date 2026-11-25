use crate::{common, openai_oauth_public_media};
use axum::{
    Router,
    extract::{
        State,
        ws::{Message as AxumMessage, WebSocket as AxumWebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

#[tokio::test]
async fn backend_codex_websocket_preserves_frames_query_and_authentication() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = openai_oauth_public_media::spawn().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    openai_oauth_public_media::setup(&server, &fake, &client).await;
    let key = &server.managed_key.plaintext;

    let mut request = format!(
        "ws://{}/openai/backend-api/codex/responses?trace=a%2Fb&beta=1",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-test","stream_id":"lane-1","input":"native"}"#.into(),
        ))
        .await
        .unwrap();
    let first_frame = match socket.next().await.unwrap().unwrap() {
        Message::Text(frame) => frame.to_string(),
        other => panic!("expected provider text frame, got {other:?}"),
    };
    let second_frame = match socket.next().await.unwrap().unwrap() {
        Message::Text(frame) => frame.to_string(),
        other => panic!("expected provider text frame, got {other:?}"),
    };
    assert_eq!(
        first_frame,
        r#"{"type":"response.created","response":{"id":"resp_tok","object":"response","status":"in_progress"}}"#
    );
    assert_eq!(
        second_frame,
        r#"{"type":"response.completed","response":{"id":"resp_tok","object":"response","status":"completed","usage":{"input_tokens":42,"output_tokens":0,"total_tokens":42}}}"#
    );

    let (body, query) = {
        let records = fake.state.0.lock().unwrap();
        let frame = records
            .iter()
            .find(|(path, transport, _, _)| {
                path == "/backend-api/codex/responses" && transport == "WS"
            })
            .expect("native alias frame reached OAuth backend");
        (
            String::from_utf8_lossy(&frame.3).into_owned(),
            frame
                .2
                .iter()
                .find(|(name, _)| name == "x-test-query")
                .map(|(_, value)| value.clone())
                .unwrap_or_default(),
        )
    };
    assert_eq!(query, "trace=a%2Fb&beta=1");
    assert!(body.contains("stream_id"));
    assert!(!body.contains("\"store\""));

    let unauthenticated = client
        .get(format!(
            "http://{}/openai/backend-api/codex/models",
            server.proxy_addr
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), 401);
}

#[tokio::test]
async fn backend_codex_websocket_rejects_unauthenticated_upgrade() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = openai_oauth_public_media::spawn().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    openai_oauth_public_media::setup(&server, &fake, &client).await;
    let request = format!(
        "ws://{}/openai/backend-api/codex/responses?trace=a%2Fb",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio_tungstenite::connect_async(request),
    )
    .await
    .expect("WebSocket auth handshake completed")
    .expect_err("unauthenticated WebSocket upgrade must fail");
    assert!(error.to_string().contains("401"));
    assert!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|(_, _, headers, _)| { !headers.iter().any(|(name, _)| name == "request-id") })
    );
}

#[tokio::test]
async fn backend_codex_websocket_rejects_traversal_without_upstream_contact() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = openai_oauth_public_media::spawn().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    openai_oauth_public_media::setup(&server, &fake, &client).await;
    let response = client
        .get(format!(
            "http://{}/openai/backend-api/codex/..%2fresponses",
            server.proxy_addr
        ))
        .bearer_auth(&server.managed_key.plaintext)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_client_error());
    assert!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|(path, _, headers, _)| {
                path != "/backend-api/codex/responses"
                    || !headers.iter().any(|(name, _)| name == "request-id")
            })
    );
}

#[tokio::test]
async fn backend_codex_websocket_strips_actor_authorization_on_responses() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = crate::backend_api_codex::spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    let upstreams = client
        .get(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth("admin-token")
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    for u in upstreams["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let detail = client
            .get(format!(
                "http://{}/admin/v1/upstreams/{id}",
                server.admin_addr
            ))
            .bearer_auth("admin-token")
            .send()
            .await
            .unwrap();
        let etag = detail
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_owned();
        let _ = client
            .patch(format!(
                "http://{}/admin/v1/upstreams/{id}",
                server.admin_addr
            ))
            .bearer_auth("admin-token")
            .header("if-match", etag)
            .json(&json!({ "enabled": false }))
            .send()
            .await
            .unwrap();
    }
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-ws-strip",
        &fake.base,
        "acct-ws-strip",
    )
    .await;

    let mut request = format!(
        "ws://{}/openai/backend-api/codex/responses?trace=strip",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", server.managed_key.plaintext)
            .parse()
            .unwrap(),
    );
    request
        .headers_mut()
        .insert("x-openai-actor-authorization", "1".parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-test","input":"ws-strip"}"#.into(),
        ))
        .await
        .unwrap();
    let _ = socket.next().await.unwrap().unwrap();
    let _ = socket.next().await.unwrap().unwrap();

    let recorded = fake.state.0.lock().unwrap();
    let ws_req = recorded
        .iter()
        .find(|r| r.method == "WS" && r.path == "/backend-api/codex/responses")
        .expect("responses websocket handshake reached upstream");
    assert!(
        ws_req
            .headers
            .iter()
            .all(|(name, _)| name != "x-openai-actor-authorization")
    );
}

#[tokio::test]
async fn backend_codex_websocket_strips_ingress_proxy_and_cf_cookies_preserves_client_headers() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = crate::backend_api_codex::spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    let upstreams = client
        .get(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth("admin-token")
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    for u in upstreams["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let detail = client
            .get(format!(
                "http://{}/admin/v1/upstreams/{id}",
                server.admin_addr
            ))
            .bearer_auth("admin-token")
            .send()
            .await
            .unwrap();
        let etag = detail
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_owned();
        let _ = client
            .patch(format!(
                "http://{}/admin/v1/upstreams/{id}",
                server.admin_addr
            ))
            .bearer_auth("admin-token")
            .header("if-match", etag)
            .json(&json!({ "enabled": false }))
            .send()
            .await
            .unwrap();
    }
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-ws-strip-ingress",
        &fake.base,
        "acct-ws-strip-ingress",
    )
    .await;

    let mut request = format!(
        "ws://{}/openai/backend-api/codex/responses?trace=strip-ingress",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", server.managed_key.plaintext)
            .parse()
            .unwrap(),
    );
    for (name, val) in [
        ("cf-connecting-ip", "203.0.113.10"),
        ("cf-ray", "1234567890abcdef-ICN"),
        ("cf-visitor", r#"{"scheme":"https"}"#),
        ("cf-warp-tag-id", "warp-tag"),
        ("cf-ew-via", "15"),
        ("cf-access-jwt-assertion", "jwt.secret.sig"),
        ("cf-access-authenticated-user-email", "user@example.com"),
        ("cf-access-client-id", "service-client-id"),
        ("cf-access-client-secret", "service-client-secret"),
        ("cdn-loop", "cloudflare; loops=1"),
        ("forwarded", "for=203.0.113.10;proto=https"),
        ("x-forwarded-for", "203.0.113.10"),
        ("x-forwarded-proto", "https"),
        ("x-real-ip", "203.0.113.10"),
        ("true-client-ip", "203.0.113.10"),
        ("x-envoy-external-address", "203.0.113.10"),
        ("cookie", "CF_Authorization=jwt.secret.sig"),
        ("originator", "codex_cli_rs"),
        ("session_id", "sess-ws-ingress"),
    ] {
        request.headers_mut().insert(name, val.parse().unwrap());
    }
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-test","input":"ws-strip-ingress"}"#.into(),
        ))
        .await
        .unwrap();
    let _ = socket.next().await.unwrap().unwrap();
    let _ = socket.next().await.unwrap().unwrap();

    let recorded = fake.state.0.lock().unwrap();
    let ws_req = recorded
        .iter()
        .find(|r| r.method == "WS" && r.path == "/backend-api/codex/responses")
        .expect("responses websocket handshake reached upstream");
    for (name, _) in &ws_req.headers {
        assert!(
            !cc_lb_engine::is_ingress_proxy_header_name(name),
            "WS ingress header {name} leaked upstream"
        );
        assert!(
            !name.eq_ignore_ascii_case("cookie"),
            "CF_Authorization-only cookie header leaked upstream on WS"
        );
    }
    assert!(
        ws_req
            .headers
            .iter()
            .any(|(name, value)| name == "originator" && value == "codex_cli_rs")
    );
    assert!(
        ws_req
            .headers
            .iter()
            .any(|(name, value)| name == "session_id" && value == "sess-ws-ingress")
    );
}

#[tokio::test]
async fn responses_websocket_raw_api_key_fallback_strips_ingress_proxy_and_cf_cookies() {
    type RecordedHeaderPairs = Vec<(String, String)>;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let recorded_headers = Arc::new(Mutex::new(RecordedHeaderPairs::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fake_addr = listener.local_addr().unwrap();
    let captured_state = Arc::clone(&recorded_headers);
    let _fake_task = tokio::spawn(async move {
        let app = Router::new()
            .route(
                "/v1/responses",
                axum::routing::get(
                    |headers: axum::http::HeaderMap,
                     ws: WebSocketUpgrade,
                     State(state): State<Arc<Mutex<RecordedHeaderPairs>>>| async move {
                        let pairs = headers
                            .iter()
                            .map(|(k, v)| {
                                (
                                    k.as_str().to_ascii_lowercase(),
                                    v.to_str().unwrap_or("").to_owned(),
                                )
                            })
                            .collect();
                        *state.lock().unwrap() = pairs;
                        ws.on_upgrade(|mut socket: AxumWebSocket| async move {
                            if let Some(Ok(AxumMessage::Text(_))) = socket.next().await {
                                let _ = socket
                                    .send(AxumMessage::Text(
                                        r#"{"type":"response.created","response":{"id":"resp_raw","object":"response","status":"in_progress"}}"#.into(),
                                    ))
                                    .await;
                                let _ = socket
                                    .send(AxumMessage::Text(
                                        r#"{"type":"response.completed","response":{"id":"resp_raw","object":"response","status":"completed","usage":{"input_tokens":5,"output_tokens":1,"total_tokens":6}}}"#.into(),
                                    ))
                                    .await;
                            }
                        })
                    },
                ),
            )
            .with_state(captured_state);
        let _ = axum::serve(listener, app).await;
    });

    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    let upstreams = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await
    .1;
    for u in upstreams["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, _, etag) = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let _ = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
    }
    let (status, body, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": "openai-raw-ws-fallback",
            "kind": "openai_api_key",
            "base_url": format!("http://{fake_addr}"),
            "api_key_value": "test-api-key",
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201, "{body}");

    let mut request = format!(
        "ws://{}/openai/v1/responses?trace=raw-fallback",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", server.managed_key.plaintext)
            .parse()
            .unwrap(),
    );
    for (name, val) in [
        ("cf-connecting-ip", "203.0.113.10"),
        ("cf-ray", "1234567890abcdef-ICN"),
        ("cf-access-client-id", "client-spoofed"),
        ("cf-access-client-secret", "client-secret-spoofed"),
        ("cdn-loop", "cloudflare; loops=1"),
        ("x-forwarded-for", "203.0.113.10"),
        ("forwarded", "for=203.0.113.10;proto=https"),
        (
            "cookie",
            "theme=dark; CF_Authorization=jwt.secret.sig; keep=yes",
        ),
        ("originator", "codex_cli_rs"),
        ("session_id", "sess-ws-raw-fallback"),
    ] {
        request.headers_mut().insert(name, val.parse().unwrap());
    }
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-test","input":"raw-ws"}"#.into(),
        ))
        .await
        .unwrap();
    let _ = socket.next().await.unwrap().unwrap();
    let _ = socket.next().await.unwrap().unwrap();

    let headers = recorded_headers.lock().unwrap().clone();
    assert!(
        !headers.is_empty(),
        "upstream WS handshake must have occurred"
    );
    for (name, _) in &headers {
        assert!(
            !cc_lb_engine::is_ingress_proxy_header_name(name),
            "raw WS fallback leaked ingress header {name}"
        );
    }
    let cookies: Vec<&str> = headers
        .iter()
        .filter(|(name, _)| name == "cookie")
        .map(|(_, value)| value.as_str())
        .collect();
    assert_eq!(cookies, vec!["theme=dark; keep=yes"]);
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == "originator" && value == "codex_cli_rs")
    );
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == "session_id" && value == "sess-ws-raw-fallback")
    );
}

#[tokio::test]
async fn backend_codex_realtime_websocket_supports_standalone_sideband_and_pinning() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = crate::backend_api_codex::spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget(&format!(
        "[openai_public_api]\nrealtime_origin = \"{}\"\n",
        fake.origin
    ))
    .await;
    let client = Client::new();
    let (st, ups, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await;
    assert_eq!(st, 200);
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, _, etag) = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let _ = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
    }
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-rt-1",
        &fake.base,
        "acct-rt-1",
    )
    .await;
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-rt-2",
        &fake.base,
        "acct-rt-2",
    )
    .await;
    let principals = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
        None,
    )
    .await
    .1;
    let principal_id = principals["principals"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_, _, etag) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    let _ = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PUT,
        &format!("/admin/v1/principals/{principal_id}/router-terminal"),
        Some(serde_json::json!({"strategy": "first-pick"})),
        etag.as_deref(),
    )
    .await;
    let key = &server.managed_key.plaintext;

    // Unauthenticated upgrade is rejected before contacting upstream.
    let unauth_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&model=gpt-realtime",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    let unauth_err = tokio_tungstenite::connect_async(unauth_req)
        .await
        .expect_err("unauthenticated realtime websocket must fail");
    assert!(unauth_err.to_string().contains("401"), "{unauth_err}");
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    // Unknown call_id fails closed with 404 before contacting upstream.
    let mut missing_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&call_id=rtc_missing",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    missing_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let missing_err = tokio_tungstenite::connect_async(missing_req)
        .await
        .expect_err("unknown call_id must fail");
    assert!(missing_err.to_string().contains("404"), "{missing_err}");
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    // Standalone Codex realtime connect with `?intent=quicksilver` succeeds for unrestricted principal.
    let mut standalone_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    standalone_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    standalone_req
        .headers_mut()
        .insert("x-openai-actor-authorization", "1".parse().unwrap());
    let (mut standalone_ws, _) = tokio_tungstenite::connect_async(standalone_req)
        .await
        .expect("standalone codex realtime connects");
    let frame = standalone_ws
        .next()
        .await
        .expect("event arrives")
        .expect("ws ok")
        .into_text()
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["type"], "session.created");
    assert_eq!(parsed["query"], "intent=quicksilver");

    // End-to-end call create -> sideband join (default and custom OpenAI-Alpha) -> hangup on same OAuth account.
    let call_resp = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .json(&serde_json::json!({"sdp": "v=offer\r\n", "session": {"type": "realtime"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(call_resp.status(), 201);

    let mut sideband_default_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&call_id=rtc_codex_1",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    sideband_default_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    sideband_default_req
        .headers_mut()
        .insert("x-openai-actor-authorization", "1".parse().unwrap());
    let (mut sideband_ws, _) = tokio_tungstenite::connect_async(sideband_default_req)
        .await
        .expect("sideband default alpha connects");
    let sideband_frame: serde_json::Value = serde_json::from_str(
        &sideband_ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sideband_frame["alpha"], "quicksilver=v2");
    assert_eq!(
        sideband_frame["query"],
        "intent=quicksilver&call_id=rtc_codex_1"
    );

    let mut sideband_custom_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&call_id=rtc_codex_1",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    sideband_custom_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    sideband_custom_req
        .headers_mut()
        .insert("openai-alpha", "custom=v3".parse().unwrap());
    let (mut sideband_custom_ws, _) = tokio_tungstenite::connect_async(sideband_custom_req)
        .await
        .expect("sideband custom alpha connects");
    let custom_frame: serde_json::Value = serde_json::from_str(
        &sideband_custom_ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(custom_frame["alpha"], "custom=v3");

    {
        let recorded = fake.state.0.lock().unwrap();
        let call_account = recorded
            .iter()
            .find(|r| r.path == "/backend-api/codex/realtime/calls")
            .unwrap()
            .headers
            .iter()
            .find(|(k, _)| k == "chatgpt-account-id")
            .map(|(_, v)| v.as_str())
            .unwrap();
        for ws_req in recorded
            .iter()
            .filter(|r| r.method == "WS" && r.path == "/v1/realtime")
        {
            assert!(
                ws_req
                    .headers
                    .iter()
                    .all(|(k, _)| k != "x-openai-actor-authorization")
            );
            if ws_req.query.contains("call_id=rtc_codex_1") {
                assert!(
                    ws_req
                        .headers
                        .iter()
                        .any(|(k, v)| k == "chatgpt-account-id" && v == call_account)
                );
            }
        }
    }

    let hangup_resp = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls/rtc_codex_1/hangup",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .send()
        .await
        .unwrap();
    assert_eq!(hangup_resp.status(), 204);
    {
        let recorded = fake.state.0.lock().unwrap();
        let call_account = recorded
            .iter()
            .find(|r| r.path == "/backend-api/codex/realtime/calls")
            .unwrap()
            .headers
            .iter()
            .find(|(k, _)| k == "chatgpt-account-id")
            .map(|(_, v)| v.as_str())
            .unwrap();
        let hangup_req = recorded
            .iter()
            .find(|r| r.path == "/v1/realtime/calls/rtc_codex_1/hangup")
            .expect("follow-up hangup reached realtime_origin");
        assert!(
            hangup_req
                .headers
                .iter()
                .any(|(k, v)| k == "chatgpt-account-id" && v == call_account)
        );
    }

    // Restricted principal must provide an allowed `model`, even with `?intent=quicksilver`.
    let (_, mut detail, etag) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    detail["allowed_models"] = serde_json::json!(["gpt-realtime"]);
    let (status, _, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PATCH,
        &format!("/admin/v1/principals/{principal_id}"),
        Some(detail),
        etag.as_deref(),
    )
    .await;
    assert_eq!(status, 200);

    let mut modelless_restricted = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    modelless_restricted
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let modelless_err = tokio_tungstenite::connect_async(modelless_restricted)
        .await
        .expect_err("restricted principal without model must be rejected");
    assert!(modelless_err.to_string().contains("400"), "{modelless_err}");

    let mut disallowed_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&model=gpt-other",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    disallowed_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let disallowed_err = tokio_tungstenite::connect_async(disallowed_req)
        .await
        .expect_err("disallowed model must be rejected");
    assert!(
        disallowed_err.to_string().contains("403"),
        "{disallowed_err}"
    );

    let mut allowed_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&model=gpt-realtime",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    allowed_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let (mut allowed_ws, _) = tokio_tungstenite::connect_async(allowed_req)
        .await
        .expect("allowed model succeeds");
    assert!(allowed_ws.next().await.is_some());
}

#[tokio::test]
async fn backend_codex_realtime_websocket_never_routes_to_api_key_or_api_key_calls() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = crate::backend_api_codex::spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget(&format!(
        "[openai_public_api]\nrealtime_origin = \"{}\"\n",
        fake.origin
    ))
    .await;
    let client = Client::new();
    let ups = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await
    .1;
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, _, etag) = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let _ = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
    }
    let (status, _, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(serde_json::json!({
            "name": "api-key-ws-only",
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

    // Standalone Codex realtime fails 503 when only API-key upstreams exist.
    let mut standalone_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&model=gpt-realtime",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    standalone_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let err = tokio_tungstenite::connect_async(standalone_req)
        .await
        .expect_err("API-key upstream must not serve /backend-api/codex/realtime");
    assert!(err.to_string().contains("503"), "{err}");

    // Public call on API-key upstream cannot be joined on `/backend-api/codex/realtime`.
    let public_call = client
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
    assert_eq!(public_call.status(), 201);

    let mut sideband_req = format!(
        "ws://{}/openai/backend-api/codex/realtime?intent=quicksilver&call_id=rtc_api_key_only",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    sideband_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let sideband_err = tokio_tungstenite::connect_async(sideband_req)
        .await
        .expect_err("API-key call must not join on /backend-api/codex/realtime");
    assert!(sideband_err.to_string().contains("404"), "{sideband_err}");
    assert!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.method != "WS")
    );
}

#[tokio::test]
async fn backend_codex_root_supports_standalone_realtime_ws_and_rejects_http_with_authn() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = crate::backend_api_codex::spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget(&format!(
        "[openai_public_api]\nrealtime_origin = \"{}\"\n",
        fake.origin
    ))
    .await;
    let client = Client::new();
    let ups = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await
    .1;
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, _, etag) = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let _ = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
    }
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-root-ws",
        &fake.base,
        "acct-root-ws",
    )
    .await;
    let key = &server.managed_key.plaintext;

    // Unauthenticated non-WS HTTP requests to `/backend-api/codex` return 401 before upstream contact.
    for method in [reqwest::Method::GET, reqwest::Method::POST] {
        let unauth = client
            .request(
                method.clone(),
                format!("http://{}/openai/backend-api/codex", server.proxy_addr),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(unauth.status(), 401, "{method}");
    }
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    // Authenticated non-WS HTTP requests to `/backend-api/codex` return local 404 operation_not_supported.
    for method in [reqwest::Method::GET, reqwest::Method::POST] {
        let auth_http = client
            .request(
                method.clone(),
                format!("http://{}/openai/backend-api/codex", server.proxy_addr),
            )
            .bearer_auth(key)
            .send()
            .await
            .unwrap();
        assert_eq!(auth_http.status(), 404, "{method}");
        let body = auth_http.text().await.unwrap();
        assert!(body.contains("operation_not_supported"), "{body}");
    }
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    // Standalone Codex realtime WS at `/backend-api/codex?intent=quicksilver` upgrades and strips actor header.
    let mut root_ws_req = format!(
        "ws://{}/openai/backend-api/codex?intent=quicksilver",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    root_ws_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    root_ws_req
        .headers_mut()
        .insert("x-openai-actor-authorization", "1".parse().unwrap());
    let (mut root_ws, _) = tokio_tungstenite::connect_async(root_ws_req)
        .await
        .expect("root codex realtime ws connects");
    let frame: serde_json::Value =
        serde_json::from_str(&root_ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(frame["type"], "session.created");
    assert_eq!(frame["query"], "intent=quicksilver");

    let ws_count = {
        let recorded = fake.state.0.lock().unwrap();
        let ws_req = recorded
            .iter()
            .find(|r| r.method == "WS" && r.path == "/v1/realtime")
            .expect("root WS reached realtime_origin");
        assert_eq!(ws_req.query, "intent=quicksilver");
        assert!(
            ws_req
                .headers
                .iter()
                .all(|(name, _)| name != "x-openai-actor-authorization")
        );
        recorded.iter().filter(|r| r.method == "WS").count()
    };
    for path in [
        "/backend-api/codex/responses",
        "/backend-api/codex?intent=quicksilver",
    ] {
        let mut unprefixed = format!("ws://{}{path}", server.proxy_addr)
            .into_client_request()
            .unwrap();
        unprefixed
            .headers_mut()
            .insert("authorization", format!("Bearer {key}").parse().unwrap());
        let err = tokio_tungstenite::connect_async(unprefixed)
            .await
            .expect_err("unprefixed backend-api websocket must fail");
        assert!(err.to_string().contains("404"), "{path}: {err}");
    }
    assert_eq!(
        fake.state
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == "WS")
            .count(),
        ws_count
    );
}

#[tokio::test]
async fn backend_codex_realtime_v3_live_supports_standalone_sideband_and_pinning() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let fake = crate::backend_api_codex::spawn_codex_media_realtime_fake().await;
    let server = common::spawn_test_server_with_openai_budget(&format!(
        "[openai_public_api]\nrealtime_origin = \"{}\"\n",
        fake.origin
    ))
    .await;
    let client = Client::new();
    let ups = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await
    .1;
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, _, etag) = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let _ = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
    }
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-live-1",
        &fake.base,
        "acct-live-1",
    )
    .await;
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-live-2",
        &fake.base,
        "acct-live-2",
    )
    .await;
    let (status, _, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(serde_json::json!({
            "name": "api-key-live",
            "kind": "openai_api_key",
            "base_url": format!("{}v1", fake.origin),
            "api_key_value": "test-api-key",
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(status, 201);

    let principals = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
        None,
    )
    .await
    .1;
    let principal_id = principals["principals"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_, _, etag) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    let _ = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PUT,
        &format!("/admin/v1/principals/{principal_id}/router-terminal"),
        Some(serde_json::json!({"strategy": "first-pick"})),
        etag.as_deref(),
    )
    .await;
    let key = &server.managed_key.plaintext;

    // Unauthenticated `/live` and `/live/{call_id}` (including traversal paths) reject with 401 before upstream contact.
    for path in [
        "/openai/backend-api/codex/live",
        "/openai/backend-api/codex/live/rtc_codex_1",
        "/openai/backend-api/codex/live/..%2fadmin",
    ] {
        let req = format!("ws://{}{path}", server.proxy_addr)
            .into_client_request()
            .unwrap();
        let err = tokio_tungstenite::connect_async(req)
            .await
            .expect_err("unauthenticated /live websocket must fail");
        assert!(err.to_string().contains("401"), "{path}: {err}");
    }
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    // Authenticated traversal/invalid call_id returns 400 and unknown call_id returns 404 before upstream contact.
    let mut bad_call_req = format!(
        "ws://{}/openai/backend-api/codex/live/..%2fadmin",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    bad_call_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let bad_err = tokio_tungstenite::connect_async(bad_call_req)
        .await
        .expect_err("invalid call_id must fail");
    assert!(bad_err.to_string().contains("400"), "{bad_err}");

    let mut missing_call_req = format!(
        "ws://{}/openai/backend-api/codex/live/rtc_missing",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    missing_call_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let missing_err = tokio_tungstenite::connect_async(missing_call_req)
        .await
        .expect_err("unknown live call_id must fail");
    assert!(missing_err.to_string().contains("404"), "{missing_err}");
    let unexpected = fake.proxy_requests();
    assert!(
        unexpected.is_empty(),
        "unexpected upstream requests: {unexpected:?}"
    );

    // API-key call cannot be joined via `/openai/backend-api/codex/live/{call_id}`.
    let api_call = client
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
    assert_eq!(api_call.status(), 201);
    let mut api_sideband_req = format!(
        "ws://{}/openai/backend-api/codex/live/rtc_api_key_only",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    api_sideband_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let api_sideband_err = tokio_tungstenite::connect_async(api_sideband_req)
        .await
        .expect_err("API-key call must not join on codex backend /live");
    assert!(
        api_sideband_err.to_string().contains("404"),
        "{api_sideband_err}"
    );

    // Standalone V3 `/live` and root `/openai/backend-api/codex` with `OpenAI-Alpha: quicksilver=v2` reach `/v1/live`.
    for (url, alpha_hdr) in [
        (
            format!("ws://{}/openai/backend-api/codex/live", server.proxy_addr),
            None,
        ),
        (
            format!("ws://{}/openai/backend-api/codex", server.proxy_addr),
            Some("quicksilver=v2"),
        ),
    ] {
        let mut req = url.clone().into_client_request().unwrap();
        req.headers_mut()
            .insert("authorization", format!("Bearer {key}").parse().unwrap());
        req.headers_mut()
            .insert("x-openai-actor-authorization", "1".parse().unwrap());
        if let Some(alpha) = alpha_hdr {
            req.headers_mut()
                .insert("openai-alpha", alpha.parse().unwrap());
        }
        let (mut ws, _) = tokio_tungstenite::connect_async(req)
            .await
            .expect("standalone v3 live ws connects");
        let frame: serde_json::Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
        assert_eq!(frame["type"], "session.created", "{url}");
        assert_eq!(frame["path"], "/v1/live", "{url}");
        assert_eq!(frame["alpha"], "quicksilver=v2", "{url}");
    }

    // Create call -> join V3 sideband `/openai/backend-api/codex/live/rtc_codex_1` -> pinned to same OAuth account -> hangup.
    let call_resp = client
        .post(format!(
            "http://{}/openai/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas",
            server.proxy_addr
        ))
        .bearer_auth(key)
        .json(&serde_json::json!({"sdp": "v=offer\r\n", "session": {"type": "realtime"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(call_resp.status(), 201);

    let mut sideband_req = format!(
        "ws://{}/openai/backend-api/codex/live/rtc_codex_1",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    sideband_req
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    sideband_req
        .headers_mut()
        .insert("x-openai-actor-authorization", "1".parse().unwrap());
    let (mut sideband_ws, _) = tokio_tungstenite::connect_async(sideband_req)
        .await
        .expect("v3 live sideband connects");
    let sideband_frame: serde_json::Value = serde_json::from_str(
        &sideband_ws
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(sideband_frame["path"], "/v1/live/rtc_codex_1");
    assert_eq!(sideband_frame["alpha"], "quicksilver=v2");

    {
        let recorded = fake.state.0.lock().unwrap();
        let call_account = recorded
            .iter()
            .find(|r| r.path == "/backend-api/codex/realtime/calls")
            .unwrap()
            .headers
            .iter()
            .find(|(k, _)| k == "chatgpt-account-id")
            .map(|(_, v)| v.as_str())
            .unwrap();
        let sideband_ws_req = recorded
            .iter()
            .find(|r| r.method == "WS" && r.path == "/v1/live/rtc_codex_1")
            .expect("sideband live WS reached realtime_origin");
        assert!(
            sideband_ws_req
                .headers
                .iter()
                .any(|(k, v)| k == "chatgpt-account-id" && v == call_account)
        );
        assert!(
            sideband_ws_req
                .headers
                .iter()
                .all(|(k, _)| k != "x-openai-actor-authorization")
        );
    }

    // Restricted principal must supply allowed `model` even on `/live`.
    let (_, mut detail, etag) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    detail["allowed_models"] = serde_json::json!(["gpt-realtime"]);
    let (status, _, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PATCH,
        &format!("/admin/v1/principals/{principal_id}"),
        Some(detail),
        etag.as_deref(),
    )
    .await;
    assert_eq!(status, 200);

    let mut restricted_live = format!("ws://{}/openai/backend-api/codex/live", server.proxy_addr)
        .into_client_request()
        .unwrap();
    restricted_live
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let restricted_err = tokio_tungstenite::connect_async(restricted_live)
        .await
        .expect_err("restricted principal without model on /live must be rejected");
    assert!(
        restricted_err.to_string().contains("400"),
        "{restricted_err}"
    );
}

#[derive(Clone, Default)]
struct InteractiveWsState {
    connections: Arc<Mutex<Vec<Vec<String>>>>,
}

struct InteractiveWsUpstream {
    base: String,
    state: InteractiveWsState,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for InteractiveWsUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn interactive_ws_handler(
    State(state): State<InteractiveWsState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let conn_idx = {
        let mut conns = state.connections.lock().unwrap();
        conns.push(Vec::new());
        conns.len() - 1
    };
    ws.on_upgrade(move |mut socket: AxumWebSocket| async move {
        let mut active_id: Option<String> = None;
        while let Some(Ok(AxumMessage::Text(text))) = socket.recv().await {
            let raw = text.to_string();
            state.connections.lock().unwrap()[conn_idx].push(raw.clone());
            let Ok(value) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            match value.get("type").and_then(Value::as_str) {
                Some("response.create") => {
                    let is_interruptible = value
                        .get("input")
                        .map(|input| match input {
                            Value::String(s) => s == "interruptible",
                            Value::Array(items) => items.iter().any(|item| {
                                item.get("text").and_then(Value::as_str) == Some("interruptible")
                                    || item
                                        .get("content")
                                        .and_then(Value::as_array)
                                        .is_some_and(|parts| {
                                            parts.iter().any(|part| {
                                                part.get("text").and_then(Value::as_str)
                                                    == Some("interruptible")
                                            })
                                        })
                            }),
                            _ => false,
                        })
                        .unwrap_or(false);
                    if is_interruptible {
                        let resp_id = format!("resp_int_{conn_idx}");
                        active_id = Some(resp_id.clone());
                        let created = json!({
                            "type": "response.created",
                            "response": {
                                "id": resp_id,
                                "object": "response",
                                "status": "in_progress"
                            }
                        });
                        if socket
                            .send(AxumMessage::Text(created.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    } else {
                        let resp_id = format!("resp_done_{conn_idx}");
                        let created = json!({
                            "type": "response.created",
                            "response": {
                                "id": resp_id,
                                "object": "response",
                                "status": "in_progress"
                            }
                        });
                        let completed = json!({
                            "type": "response.completed",
                            "response": {
                                "id": resp_id,
                                "object": "response",
                                "status": "completed",
                                "usage": {"input_tokens": 20, "output_tokens": 5, "total_tokens": 25}
                            }
                        });
                        if socket
                            .send(AxumMessage::Text(created.to_string().into()))
                            .await
                            .is_err()
                            || socket
                                .send(AxumMessage::Text(completed.to_string().into()))
                                .await
                                .is_err()
                        {
                            break;
                        }
                    }
                }
                Some("response.interrupt") => {
                    let resp_id = value
                        .get("response_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let ack = json!({
                        "type": "response.interrupt.accepted",
                        "response_id": resp_id
                    });
                    if socket
                        .send(AxumMessage::Text(ack.to_string().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if active_id.as_deref() == Some(resp_id.as_str()) {
                        active_id = None;
                        let incomplete = json!({
                            "type": "response.incomplete",
                            "response": {
                                "id": resp_id,
                                "object": "response",
                                "status": "incomplete",
                                "error": null,
                                "incomplete_details": {"reason": "interrupted"},
                                "usage": {"input_tokens": 10, "output_tokens": 3, "total_tokens": 13}
                            }
                        });
                        if socket
                            .send(AxumMessage::Text(incomplete.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
    })
}

async fn spawn_interactive_ws_upstream() -> InteractiveWsUpstream {
    let state = InteractiveWsState::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let st = state.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(
                    "/backend-api/codex/responses",
                    axum::routing::get(interactive_ws_handler),
                )
                .with_state(st),
        )
        .await
    });
    InteractiveWsUpstream {
        base: format!("http://{addr}/backend-api/codex"),
        state,
        task,
    }
}

async fn setup_interactive_upstream(
    server: &common::TestServer,
    upstream: &InteractiveWsUpstream,
    client: &Client,
) {
    let resp = client
        .post(format!("http://{}/admin/v1/upstreams", server.admin_addr))
        .bearer_auth("admin-token")
        .json(&json!({
            "name": "oauth-ws-interrupt",
            "kind": "openai_oauth",
            "base_url": upstream.base,
            "oauth_tokens": {
                "access_token": "test-access",
                "refresh_token": "",
                "expires_at_unix_secs": 4102444800u64,
                "account_id": "acct-test",
                "scopes": [],
                "never_refresh": true
            },
            "skip_preflight": true
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "upstream creation failed: {}",
        resp.status()
    );
}

async fn recv_text(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> String {
    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
        .await
        .expect("timed out waiting for WebSocket frame")
        .expect("WebSocket stream ended unexpectedly")
        .expect("WebSocket frame error");
    match msg {
        Message::Text(text) => text.to_string(),
        other => panic!("expected text frame, got {other:?}"),
    }
}

#[tokio::test]
async fn backend_codex_websocket_forwards_response_interrupt_and_continues_session() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let upstream = spawn_interactive_ws_upstream().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    setup_interactive_upstream(&server, &upstream, &client).await;
    let key = &server.managed_key.plaintext;

    let mut request = format!(
        "ws://{}/openai/backend-api/codex/responses",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();

    // 1. Start an interruptible turn and confirm response.created arrives.
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-test","input":"interruptible"}"#.into(),
        ))
        .await
        .unwrap();
    let created: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(created["type"], "response.created");
    let response_id = created["response"]["id"].as_str().unwrap().to_owned();

    // 2. Send Codex-native response.interrupt over the same socket.
    let interrupt_payload = format!(
        r#"{{"type":"response.interrupt","response_id":"{response_id}","mode":"discard_partial_items"}}"#
    );
    socket
        .send(Message::Text(interrupt_payload.clone().into()))
        .await
        .unwrap();

    let ack_raw = recv_text(&mut socket).await;
    let ack: Value = serde_json::from_str(&ack_raw).unwrap();
    assert_eq!(ack["type"], "response.interrupt.accepted");
    assert_eq!(ack["response_id"], response_id);

    let incomplete_raw = recv_text(&mut socket).await;
    let incomplete: Value = serde_json::from_str(&incomplete_raw).unwrap();
    assert_eq!(incomplete["type"], "response.incomplete");
    assert_eq!(incomplete["response"]["id"], response_id);
    assert_eq!(
        incomplete["response"]["incomplete_details"]["reason"],
        "interrupted"
    );

    // 3. Follow up with a new response.create on the same WebSocket session.
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-test","input":"followup"}"#.into(),
        ))
        .await
        .unwrap();
    let followup_created: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(followup_created["type"], "response.created");
    let followup_completed: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(followup_completed["type"], "response.completed");

    // Verify all 3 frames went over a single upstream WebSocket connection, and interrupt stayed byte-identical.
    let recorded = upstream.state.connections.lock().unwrap();
    assert_eq!(recorded.len(), 1, "expected single owning upstream socket");
    assert_eq!(recorded[0].len(), 3);
    assert_eq!(recorded[0][1], interrupt_payload);
}

#[tokio::test]
async fn public_responses_websocket_interrupt_enforces_lane_and_reports_unknown_types() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let upstream = spawn_interactive_ws_upstream().await;
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    setup_interactive_upstream(&server, &upstream, &client).await;
    let key = &server.managed_key.plaintext;

    let mut request = format!("ws://{}/openai/v1/responses", server.proxy_addr)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();

    // 1. Start a lane-scoped turn.
    socket
        .send(Message::Text(
            r#"{"type":"response.create","stream_id":"lane-a","model":"gpt-test","store":false,"input":"interruptible"}"#.into(),
        ))
        .await
        .unwrap();
    let created: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(created["type"], "response.created");
    assert_eq!(created["stream_id"], "lane-a");
    let response_id = created["response"]["id"].as_str().unwrap().to_owned();

    // 2. Mismatched stream_id must be rejected locally without reaching upstream.
    socket
        .send(Message::Text(
            format!(
                r#"{{"type":"response.interrupt","response_id":"{response_id}","stream_id":"lane-b","mode":"discard_partial_items"}}"#
            )
            .into(),
        ))
        .await
        .unwrap();
    let mismatch_err: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(mismatch_err["type"], "error");
    assert_eq!(mismatch_err["status"], 400);
    assert_eq!(mismatch_err["error"]["code"], "response_not_found");
    assert_eq!(mismatch_err["stream_id"], "lane-b");

    // 3. Unknown response_id must be rejected locally without reaching upstream.
    socket
        .send(Message::Text(
            r#"{"type":"response.interrupt","response_id":"resp_unknown"}"#.into(),
        ))
        .await
        .unwrap();
    let unknown_err: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(unknown_err["type"], "error");
    assert_eq!(unknown_err["status"], 400);
    assert_eq!(unknown_err["error"]["code"], "response_not_found");

    // 4. Future unsupported client event must include the sanitized event type.
    socket
        .send(Message::Text(
            r#"{"type":"response.future_control"}"#.into(),
        ))
        .await
        .unwrap();
    let unsupported_err: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(unsupported_err["type"], "error");
    assert_eq!(unsupported_err["status"], 400);
    assert_eq!(unsupported_err["error"]["code"], "invalid_request_error");
    assert!(
        unsupported_err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("response.future_control")
    );

    // 5. Valid interrupt on lane-a reaches upstream and returns ack + incomplete with lane-a.
    socket
        .send(Message::Text(
            format!(
                r#"{{"type":"response.interrupt","response_id":"{response_id}","stream_id":"lane-a","mode":"discard_partial_items"}}"#
            )
            .into(),
        ))
        .await
        .unwrap();
    let ack: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(ack["type"], "response.interrupt.accepted");
    assert_eq!(ack["response_id"], response_id);
    assert_eq!(ack["stream_id"], "lane-a");

    let incomplete: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(incomplete["type"], "response.incomplete");
    assert_eq!(incomplete["stream_id"], "lane-a");

    // 6. Follow-up turn on lane-a still succeeds.
    socket
        .send(Message::Text(
            r#"{"type":"response.create","stream_id":"lane-a","model":"gpt-test","store":false,"input":"followup"}"#.into(),
        ))
        .await
        .unwrap();
    let followup_created: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(followup_created["type"], "response.created");
    let followup_completed: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(followup_completed["type"], "response.completed");

    let recorded = upstream.state.connections.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].len(),
        3,
        "only the 2 creates and 1 valid interrupt should reach upstream: {:?}",
        recorded[0]
    );
}

#[tokio::test]
async fn backend_codex_websocket_forwards_response_failed_after_in_response_error_and_recovers_after_disconnect()
 {
    use axum::routing::get;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let _ = rustls::crypto::ring::default_provider().install_default();

    // Upstream 1: turn 1 sends response.created -> in_progress -> statusless `error` (seq=3)
    // -> responsesapi.websocket_timing -> `response.failed` (seq=4), then closes WS.
    // Once `reject_after_disconnect` is armed, new handshakes return 503.
    let reject_u1 = Arc::new(AtomicBool::new(false));
    let u1_ws_calls = Arc::new(AtomicUsize::new(0));
    let reject_u1_clone = reject_u1.clone();
    let u1_calls_clone = u1_ws_calls.clone();
    let u1_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let u1_addr = u1_listener.local_addr().unwrap();
    let _u1_task = tokio::spawn(async move {
        let router = Router::new().route(
            "/backend-api/codex/responses",
            get(move |ws: WebSocketUpgrade| {
                let reject = reject_u1_clone.load(Ordering::SeqCst);
                let calls = u1_calls_clone.clone();
                async move {
                    if reject {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "overloaded")
                            .into_response();
                    }
                    ws.on_upgrade(move |mut socket: AxumWebSocket| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        while let Some(Ok(AxumMessage::Text(create))) = socket.recv().await {
                            let req: Value = serde_json::from_str(&create).unwrap();
                            if req["input"].as_str() == Some("drop-me") {
                                let created = r#"{"type":"response.created","sequence_number":0,"response":{"id":"resp_u1_drop","object":"response","status":"in_progress","store":false,"output":[]}}"#;
                                let _ = socket.send(AxumMessage::Text(created.into())).await;
                                let _ = socket.close().await;
                                return;
                            }
                            let frames = [
                                r#"{"type":"response.created","sequence_number":0,"response":{"id":"resp_u1_overload","object":"response","status":"in_progress","store":false,"output":[]}}"#,
                                r#"{"type":"response.in_progress","sequence_number":2,"response":{"id":"resp_u1_overload","object":"response","status":"in_progress","store":false,"output":[]}}"#,
                                r#"{"type":"error","sequence_number":3,"response_id":"resp_u1_overload","error":{"message":"Our servers are currently overloaded. Please try again later.","type":"service_unavailable_error","param":null,"code":"server_is_overloaded","headers":{"retry-after":"360","retry-after-ms":"360000"}}}"#,
                                r#"{"type":"responsesapi.websocket_timing","timing_metrics":{"execute_duration_ms":42}}"#,
                                r#"{"type":"response.failed","sequence_number":4,"response":{"id":"resp_u1_overload","object":"response","status":"failed","output":[],"error":{"type":"service_unavailable_error","param":null,"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}"#,
                            ];
                            for frame in frames {
                                if socket.send(AxumMessage::Text(frame.into())).await.is_err() {
                                    return;
                                }
                            }
                        }
                    })
                    .into_response()
                }
            }),
        );
        let _ = axum::serve(u1_listener, router).await;
    });

    // Upstream 2: healthy OAuth WS upstream.
    let u2_ws_calls = Arc::new(AtomicUsize::new(0));
    let u2_calls_clone = u2_ws_calls.clone();
    let u2_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let u2_addr = u2_listener.local_addr().unwrap();
    let _u2_task = tokio::spawn(async move {
        let router = Router::new().route(
            "/backend-api/codex/responses",
            get(move |ws: WebSocketUpgrade| {
                let calls = u2_calls_clone.clone();
                async move {
                    ws.on_upgrade(move |mut socket: AxumWebSocket| async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        while let Some(Ok(AxumMessage::Text(_))) = socket.recv().await {
                            let created = r#"{"type":"response.created","sequence_number":0,"response":{"id":"resp_u2_ok","object":"response","status":"in_progress","store":false,"output":[]}}"#;
                            let completed = r#"{"type":"response.completed","sequence_number":1,"response":{"id":"resp_u2_ok","object":"response","status":"completed","output":[],"usage":{"input_tokens":12,"output_tokens":4,"total_tokens":16}}}"#;
                            if socket
                                .send(AxumMessage::Text(created.into()))
                                .await
                                .is_err()
                                || socket
                                    .send(AxumMessage::Text(completed.into()))
                                    .await
                                    .is_err()
                            {
                                break;
                            }
                        }
                    })
                }
            }),
        );
        let _ = axum::serve(u2_listener, router).await;
    });

    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();
    let (_, ups, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/upstreams",
        None,
        None,
    )
    .await;
    for u in ups["upstreams"].as_array().unwrap() {
        let id = u["id"].as_str().unwrap();
        let (_, _, etag) = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::GET,
            &format!("/admin/v1/upstreams/{id}"),
            None,
            None,
        )
        .await;
        let _ = crate::openai_oauth_public_compat::admin(
            &client,
            &server,
            reqwest::Method::PATCH,
            &format!("/admin/v1/upstreams/{id}"),
            Some(json!({ "enabled": false })),
            etag.as_deref(),
        )
        .await;
    }
    crate::backend_api_codex::add_oauth_upstream(
        &client,
        &server,
        "oauth-overloaded-1",
        &format!("http://{u1_addr}/backend-api/codex"),
        "acct-u1",
    )
    .await;
    // Keep u2 disabled via PATCH during Turn 1 / Turn 1b so UUID ordering cannot route Turn 1 to u2
    // (POST ignores `enabled`).
    let (st_u2, u2_body, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::POST,
        "/admin/v1/upstreams",
        Some(json!({
            "name": "oauth-healthy-2",
            "kind": "openai_oauth",
            "base_url": format!("http://{u2_addr}/backend-api/codex"),
            "oauth_tokens": {
                "access_token": "access-acct-u2",
                "refresh_token": "",
                "expires_at_unix_secs": 4102444800u64,
                "account_id": "acct-u2",
                "scopes": [],
                "never_refresh": true
            },
            "warmup_enabled": false,
            "skip_preflight": true
        })),
        None,
    )
    .await;
    assert_eq!(st_u2, 201, "{u2_body}");
    let u2_id = u2_body["id"].as_str().unwrap().to_owned();
    let (_, _, u2_init_etag) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/upstreams/{u2_id}"),
        None,
        None,
    )
    .await;
    let (st_disable, disabled_body, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PATCH,
        &format!("/admin/v1/upstreams/{u2_id}"),
        Some(json!({ "enabled": false })),
        u2_init_etag.as_deref(),
    )
    .await;
    assert_eq!(st_disable, 200, "{disabled_body}");
    assert_eq!(disabled_body["enabled"], false);
    let principals = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        "/admin/v1/principals",
        None,
        None,
    )
    .await
    .1;
    let principal_id = principals["principals"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (_, _, etag) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/principals/{principal_id}"),
        None,
        None,
    )
    .await;
    let (st, _, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PUT,
        &format!("/admin/v1/principals/{principal_id}/router-terminal"),
        Some(json!({"strategy": "first-pick"})),
        etag.as_deref(),
    )
    .await;
    assert_eq!(st, 200);

    // Subscribe to admin events SSE before running Turn 1 so we wait deterministically
    // without sleeps for Turn 1's persisted request_event row.
    let mut sse_response = client
        .get(format!(
            "http://{}/admin/v1/events/stream",
            server.admin_addr
        ))
        .bearer_auth("admin-token")
        .send()
        .await
        .unwrap();
    assert_eq!(sse_response.status(), 200);
    let mut sse_buf = String::new();
    let sse_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !sse_buf.contains("event: cursor") {
        let chunk = tokio::time::timeout_at(sse_deadline, sse_response.chunk())
            .await
            .expect("initial cursor SSE arrives")
            .unwrap()
            .expect("SSE stream open");
        sse_buf.push_str(&String::from_utf8_lossy(&chunk));
    }

    let key = &server.managed_key.plaintext;
    let mut request = format!(
        "ws://{}/openai/backend-api/codex/responses",
        server.proxy_addr
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {key}").parse().unwrap());
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();

    // Turn 1: receives all 5 frames including BOTH `error` and `response.failed`.
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":"turn-1"}"#
                .into(),
        ))
        .await
        .unwrap();
    let expected_types = [
        "response.created",
        "response.in_progress",
        "error",
        "responsesapi.websocket_timing",
        "response.failed",
    ];
    for expected in expected_types {
        let frame: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
        assert_eq!(
            frame["type"], expected,
            "expected {expected} frame, got {frame}"
        );
    }

    // Wait for Turn 1's finalized event on SSE and inspect persisted classification.
    sse_buf.clear();
    let final_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !sse_buf.contains("upstream_stream_error") {
        let chunk = tokio::time::timeout_at(final_deadline, sse_response.chunk())
            .await
            .expect("final request event arrives on SSE")
            .unwrap()
            .expect("SSE stream open");
        sse_buf.push_str(&String::from_utf8_lossy(&chunk));
    }
    let recent = client
        .get(format!(
            "http://{}/admin/v1/events/recent?limit=10",
            server.admin_addr
        ))
        .bearer_auth("admin-token")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let turn1_event = recent["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|ev| ev["error_code"].as_str() == Some("upstream_stream_error"))
        .expect("Turn 1 event recorded as upstream_stream_error");
    assert!(
        turn1_event["internal_errors"]
            .as_array()
            .is_none_or(|errs| errs.is_empty()),
        "upstream WS error must not record InternalFailure: {turn1_event}"
    );
    assert_eq!(
        turn1_event["upstream_error_type"].as_str(),
        Some("server_is_overloaded")
    );

    // Turn 1b: verify connection_closed executes deterministically before Turn 2 by
    // having upstream 1 send response.created and immediately drop the WebSocket.
    reject_u1.store(true, Ordering::SeqCst);
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":"drop-me"}"#
                .into(),
        ))
        .await
        .unwrap();
    let drop_created: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(drop_created["type"], "response.created");
    assert_eq!(drop_created["response"]["id"], "resp_u1_drop");
    let drop_closed_err: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(drop_closed_err["type"], "error");
    assert_eq!(
        drop_closed_err["error"]["code"],
        "upstream_connection_closed"
    );

    // Confirm a stale previous_response_id still fails closed (proving `session_oauth`
    // remains true even after `session_upstream` clears).
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"previous_response_id":"resp_u1_drop","input":"stale"}"#.into(),
        ))
        .await
        .unwrap();
    let stale_err: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(stale_err["type"], "error");
    assert_eq!(stale_err["status"], 400);
    assert_eq!(stale_err["error"]["code"], "previous_response_not_found");

    // Enable healthy upstream 2 while upstream 1 stays enabled (returning 503 on new WS
    // upgrades): if `session_upstream` stayed stuck on upstream 1, Turn 2 would never reach u2.
    let (_, u2_get, u2_etag) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::GET,
        &format!("/admin/v1/upstreams/{u2_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(u2_get["enabled"], false);
    let (st_enable, enable_body, _) = crate::openai_oauth_public_compat::admin(
        &client,
        &server,
        reqwest::Method::PATCH,
        &format!("/admin/v1/upstreams/{u2_id}"),
        Some(json!({ "enabled": true })),
        u2_etag.as_deref(),
    )
    .await;
    assert_eq!(st_enable, 200, "{enable_body}");

    // Turn 2: independent full-history turn on the SAME downstream WebSocket must NOT
    // stay pinned to dead/failing upstream 1; it must route/failover to healthy upstream 2!
    socket
        .send(Message::Text(
            r#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":"turn-2"}"#
                .into(),
        ))
        .await
        .unwrap();
    let t2_created: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(
        t2_created["type"], "response.created",
        "expected Turn 2 response.created, got {t2_created}"
    );
    assert_eq!(t2_created["response"]["id"], "resp_u2_ok");
    let t2_completed: Value = serde_json::from_str(&recv_text(&mut socket).await).unwrap();
    assert_eq!(t2_completed["type"], "response.completed");
    assert_eq!(u1_ws_calls.load(Ordering::SeqCst), 1);
    assert_eq!(u2_ws_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn codex_backend_root_non_upgrade_persists_auth_failure_and_local_404_rows() {
    use sqlx::{Row as _, sqlite::SqlitePoolOptions};
    use std::time::Duration;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = common::spawn_test_server_with_openai_budget("").await;
    let client = Client::new();

    let unauth_get = client
        .get(format!(
            "http://{}/openai/backend-api/codex",
            server.proxy_addr
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_get.status(), 401);

    let unauth_post = client
        .post(format!(
            "http://{}/openai/backend-api/codex",
            server.proxy_addr
        ))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth_post.status(), 401);

    let authed_root_post = client
        .post(format!(
            "http://{}/openai/backend-api/codex",
            server.proxy_addr
        ))
        .header("x-api-key", &server.managed_key.plaintext)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(authed_root_post.status(), 404);
    let body: Value = authed_root_post.json().await.unwrap();
    assert_eq!(body["error"]["code"], "operation_not_supported");

    let authed_trailing_post = client
        .post(format!(
            "http://{}/openai/backend-api/codex/",
            server.proxy_addr
        ))
        .header("x-api-key", &server.managed_key.plaintext)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(authed_trailing_post.status(), 404);
    let body: Value = authed_trailing_post.json().await.unwrap();
    assert_eq!(body["error"]["code"], "operation_not_supported");

    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&format!("sqlite://{}", server.sqlite_path.display()))
        .await
        .expect("open sqlite db");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = sqlx::query(
            "SELECT list_status, error_code, principal_id FROM request_events_v1 ORDER BY id ASC",
        )
        .fetch_all(&pool)
        .await
        .expect("query request_events_v1");
        if rows.len() == 4 {
            assert_eq!(rows[0].get::<Option<i64>, _>("list_status"), Some(401));
            assert_eq!(
                rows[0].get::<Option<String>, _>("error_code").as_deref(),
                Some("authentication_failed")
            );
            assert_eq!(rows[1].get::<Option<i64>, _>("list_status"), Some(401));
            assert_eq!(
                rows[1].get::<Option<String>, _>("error_code").as_deref(),
                Some("authentication_failed")
            );
            for row in &rows[2..4] {
                assert_eq!(row.get::<Option<i64>, _>("list_status"), Some(404));
                assert_eq!(
                    row.get::<Option<String>, _>("error_code").as_deref(),
                    Some("operation_not_supported")
                );
                assert!(
                    row.get::<Option<String>, _>("principal_id").is_some(),
                    "authenticated 404 row should attribute principal_id"
                );
            }
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for 4 codex backend root rows; saw {}",
            rows.len()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
