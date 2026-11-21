use crate::{common, openai_oauth_public_media};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
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
            .post(format!(
                "http://{}/admin/v1/upstreams/{id}/disable",
                server.admin_addr
            ))
            .bearer_auth("admin-token")
            .header("if-match", etag)
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
            reqwest::Method::POST,
            &format!("/admin/v1/upstreams/{id}/disable"),
            None,
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
    assert!(fake.state.0.lock().unwrap().is_empty());

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
    assert!(fake.state.0.lock().unwrap().is_empty());

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
        reqwest::Method::PUT,
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
            reqwest::Method::POST,
            &format!("/admin/v1/upstreams/{id}/disable"),
            None,
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
            reqwest::Method::POST,
            &format!("/admin/v1/upstreams/{id}/disable"),
            None,
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
    assert!(fake.state.0.lock().unwrap().is_empty());

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
    assert!(fake.state.0.lock().unwrap().is_empty());

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
            reqwest::Method::POST,
            &format!("/admin/v1/upstreams/{id}/disable"),
            None,
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
    assert!(fake.state.0.lock().unwrap().is_empty());

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
    assert!(fake.state.0.lock().unwrap().is_empty());

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
        reqwest::Method::PUT,
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
