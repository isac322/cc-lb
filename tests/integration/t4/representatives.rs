use std::io;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use cc_lb_storage_api::ManagedKeyStore;
use fake_anthropic::{MessageScript, ScriptedMessageResponse};
use http::StatusCode;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use super::support::{
    ADMIN_TOKEN, AuthKind, MODEL, TestApp, TestAppOptions, TestResult, WAIT_TIMEOUT,
    expected_sse_body, happy_message_body, message_body, parse_raw_response, proxy_request,
    read_http_request, request,
};

#[tokio::test]
async fn t4__proxy_nonstream_happy() -> TestResult {
    let script = MessageScript::new();
    let mut options = TestAppOptions::fake("nonstream-happy");
    options.fake_config.message_script = Some(script.clone());
    options.downstream_api_key_auth = true;
    TestApp::run(options, move |app| async move {
        script.push_response(ScriptedMessageResponse::ok());
        script.push_response(ScriptedMessageResponse::error(
            StatusCode::from_u16(529)?,
            "overloaded_error",
            "overloaded",
        ));
        let expected_request_body = message_body(false);
        let response = proxy_request(
            &app,
            "t4-nonstream-happy",
            false,
            &[("connection", "close, x-hop"), ("x-hop", "removed")],
        )
        .await?;
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body)?,
            happy_message_body()
        );
        assert!(script.wait_for_request_arrival(1, WAIT_TIMEOUT).await);
        let upstream = script.requests();
        assert_eq!(upstream.len(), 1);
        assert_eq!(upstream[0].body, expected_request_body);
        assert!(upstream[0].headers.contains_key("x-api-key"));
        assert!(!upstream[0].headers.contains_key("connection"));
        assert!(!upstream[0].headers.contains_key("x-hop"));
        let row = app.event("t4-nonstream-happy").await;
        assert_eq!(row.status, 200);
        assert!(row.error_code.is_none());
        assert!(row.event_id.is_some());
        assert_eq!(row.model.as_deref(), Some(MODEL));

        let invalid_key = request(
            app.proxy_addr,
            "POST",
            "/v1/messages",
            &[
                ("content-type", "application/json"),
                ("request-id", "t4-authentication-failed"),
                (
                    "x-api-key",
                    "sk-cclb-invalid_bogusbogusbogusbogusbogusbogusbogusbogusbogus",
                ),
            ],
            &message_body(false),
        )
        .await?;
        assert_eq!(invalid_key.status, StatusCode::UNAUTHORIZED);
        let auth_row = app.event("t4-authentication-failed").await;
        assert_eq!(
            auth_row.error_code.as_deref(),
            Some("authentication_failed")
        );
        assert_eq!(auth_row.status, 401);
        assert!(auth_row.event_id.is_some());

        let downstream_key = app
            .downstream_api_key
            .as_deref()
            .expect("API-key-auth fixture exposes a key");
        let oversized = request_with_declared_content_length(
            app.proxy_addr,
            "t4-body-too-large",
            downstream_key,
            33 * 1024 * 1024,
        )
        .await?;
        assert_eq!(oversized.status, StatusCode::PAYLOAD_TOO_LARGE);
        let oversized_row = app.event("t4-body-too-large").await;
        assert_eq!(oversized_row.error_code.as_deref(), Some("body_too_large"));
        assert_eq!(oversized_row.status, 413);
        assert!(oversized_row.event_id.is_some());

        let upstream_5xx = proxy_request(&app, "t4-upstream-5xx", false, &[]).await?;
        assert!(
            upstream_5xx.status.is_server_error()
                || upstream_5xx.status == StatusCode::from_u16(529)?,
            "expected 5xx, got {}",
            upstream_5xx.status
        );
        let upstream_5xx_row = app.event("t4-upstream-5xx").await;
        assert_eq!(upstream_5xx_row.error_code.as_deref(), Some("upstream_5xx"));
        assert!(upstream_5xx_row.status >= 500);
        assert!(upstream_5xx_row.event_id.is_some());
        assert!(script.wait_for_request_arrival(2, WAIT_TIMEOUT).await);
        assert_eq!(script.requests().len(), 2);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_nonstream_connect_failure() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut options = TestAppOptions::fake("connect-failure");
    options.upstream_addr = Some(addr);
    TestApp::run(options, move |app| async move {
        app.track_raw_task(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let _ = read_http_request(&mut stream).await?;
            Ok::<(), io::Error>(())
        }));
        let response = proxy_request(&app, "t4-connect-failure", false, &[]).await?;
        assert_eq!(response.status, StatusCode::BAD_GATEWAY);
        let body: Value = serde_json::from_slice(&response.body)?;
        assert_eq!(body["error"]["type"], "api_error");
        let row = app.event("t4-connect-failure").await;
        assert_eq!(row.status, 502);
        assert_eq!(row.error_code.as_deref(), Some("upstream_dispatch_failed"));
        assert!(row.event_id.is_some());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_nonstream_timeout_before_headers() -> TestResult {
    let script = MessageScript::new();
    let mut options = TestAppOptions::fake("timeout-before-headers");
    options.fake_config.message_script = Some(script.clone());
    options.upstream_total_secs = 10;
    TestApp::run(options, move |app| async move {
        script.push_response(
            ScriptedMessageResponse::ok().with_delay(std::time::Duration::from_secs(30)),
        );
        let response = proxy_request(&app, "t4-timeout-before-headers", false, &[]).await?;
        assert_eq!(response.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(response.body, b"request timed out");
        assert!(script.wait_for_request_arrival(1, WAIT_TIMEOUT).await);
        let row = app.event("t4-timeout-before-headers").await;
        assert_eq!(row.status, 504);
        assert_eq!(row.error_code.as_deref(), Some("tower_timeout"));
        assert!(row.event_id.is_some());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_nonstream_401_refresh_replay() -> TestResult {
    let script = MessageScript::new();
    script.push_response(ScriptedMessageResponse::error(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "expired access token",
    ));
    script.push_response(ScriptedMessageResponse::ok());
    let mut options = TestAppOptions::fake("oauth-401-replay");
    options.fake_config.message_script = Some(script.clone());
    options.auth_kind = AuthKind::OAuth;
    TestApp::run(options, move |app| async move {
        let response = proxy_request(&app, "t4-oauth-replay", false, &[]).await?;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        assert!(script.wait_for_request_arrival(2, WAIT_TIMEOUT).await);
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        let first = requests[0]
            .headers
            .get("authorization")
            .expect("first Authorization");
        let second = requests[1]
            .headers
            .get("authorization")
            .expect("second Authorization");
        assert!(first.starts_with("Bearer sk-ant-oat01-"));
        assert!(second.starts_with("Bearer sk-ant-oat01-"));
        assert_ne!(
            first, second,
            "401 replay must use the refreshed access token"
        );
        let row = app.event("t4-oauth-replay").await;
        assert_eq!(row.status, 200);
        assert!(row.error_code.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_nonstream_downstream_cancel() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let mut options = TestAppOptions::fake("nonstream-downstream-cancel");
    options.upstream_addr = Some(addr);
    TestApp::run(options, move |app| async move {
        app.track_raw_task(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            read_http_request(&mut stream).await?;
            let _ = accepted_tx.send(());
            let mut byte = [0u8; 1];
            let read = stream.read(&mut byte).await?;
            let _ = closed_tx.send(read == 0);
            Ok::<(), io::Error>(())
        }));
        let mut client = TcpStream::connect(app.proxy_addr).await?;
        write_proxy_request(&mut client, "t4-nonstream-downstream-cancel", false).await?;
        tokio::time::timeout(WAIT_TIMEOUT, accepted_rx).await??;
        drop(client);
        assert!(tokio::time::timeout(WAIT_TIMEOUT, closed_rx).await??);
        let row = app.event("t4-nonstream-downstream-cancel").await;
        assert_eq!(row.status, 0);
        assert_eq!(row.error_code.as_deref(), Some("terminal_dropped"));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_nonstream_tls_handshake() -> TestResult {
    let mut options = TestAppOptions::fake("tls-handshake");
    options.tls = true;
    TestApp::run(options, |app| async move {
        let cert_path = app.cert_path.as_ref().expect("TLS certificate path");
        let response = tls_request(app.proxy_addr, cert_path, "GET", "/v1/models", &[]).await?;
        assert_eq!(response.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&response.body)?;
        assert_eq!(body["type"], "list");
        let admin = request(app.admin_addr, "GET", "/admin/health", &[], &[]).await?;
        assert_eq!(
            admin.status,
            StatusCode::OK,
            "admin listener stays plaintext"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_stream_happy() -> TestResult {
    let mut options = TestAppOptions::fake("stream-happy");
    options.fake_config.weather.delta_count = 1;
    TestApp::run(options, |app| async move {
        let response = proxy_request(&app, "t4-stream-happy", true, &[]).await?;
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.body, expected_sse_body().as_bytes());
        let row = app.event("t4-stream-happy").await;
        assert_eq!(row.status, 200);
        assert!(row.error_code.is_none());
        assert!(row.event_id.is_some());
        assert_eq!(row.sse_event_count, Some(6));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_stream_malformed_chunk_maps_to_upstream_stream_error() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut options = TestAppOptions::fake("stream-malformed");
    options.upstream_addr = Some(addr);
    TestApp::run(options, move |app| async move {
        app.track_raw_task(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            read_http_request(&mut stream).await?;
            let event = message_start_event();
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await?;
            write_chunk(&mut stream, event.as_bytes()).await?;
            stream.write_all(b"ZZ\r\ncorrupt\r\n").await?;
            stream.shutdown().await?;
            Ok::<(), io::Error>(())
        }));
        let response = proxy_request(&app, "t4-stream-malformed", true, &[]).await?;
        assert_eq!(response.status, StatusCode::OK);
        let body = String::from_utf8(response.body)?;
        assert!(body.contains("event: message_start"));
        assert!(body.contains("event: error"));
        let row = app.event("t4-stream-malformed").await;
        assert_eq!(row.status, 200);
        assert_eq!(
            row.error_code.as_deref(),
            Some("upstream_stream_error")
        );
        assert!(row.event_id.is_some());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_stream_upstream_rst_mid_stream() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (rst_tx, rst_rx) = oneshot::channel();
    let mut options = TestAppOptions::fake("stream-rst");
    options.upstream_addr = Some(addr);
    TestApp::run(options, move |app| async move {
        app.track_raw_task(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            read_http_request(&mut stream).await?;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
                )
                .await?;
            write_chunk(&mut stream, message_start_event().as_bytes()).await?;
            let _ = rst_rx.await;
            let std_stream = stream.into_std()?;
            let linger = libc::linger {
                l_onoff: 1,
                l_linger: 0,
            };
            // SAFETY: std_stream owns a valid TCP socket descriptor for the duration
            // of this call, and `linger` points to a correctly sized SO_LINGER value.
            let result = unsafe {
                libc::setsockopt(
                    std_stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_LINGER,
                    std::ptr::from_ref(&linger).cast(),
                    std::mem::size_of_val(&linger) as libc::socklen_t,
                )
            };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            drop(std_stream);
            Ok::<(), io::Error>(())
        }));
        let mut client = TcpStream::connect(app.proxy_addr).await?;
        write_proxy_request(&mut client, "t4-stream-rst", true).await?;
        let mut reader = BufReader::new(client);
        let mut received = read_until_contains(&mut reader, "event: message_start").await?;
        assert!(received.contains("HTTP/1.1 200"));
        let _ = rst_tx.send(());
        let mut remainder = String::new();
        tokio::time::timeout(WAIT_TIMEOUT, reader.read_to_string(&mut remainder))
            .await
            .map_err(|_| "proxy stream did not finish after upstream RST")??;
        received.push_str(&remainder);
        assert!(received.contains("event: error"));
        drop(reader);
        let row = app.event("t4-stream-rst").await;
        assert_eq!(row.status, 200);
        assert_eq!(
            row.error_code.as_deref(),
            Some("upstream_stream_error")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_stream_downstream_cancel() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (sent_tx, sent_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let mut options = TestAppOptions::fake("stream-downstream-cancel");
    options.upstream_addr = Some(addr);
    TestApp::run(options, move |app| async move {
        app.track_raw_task(tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            read_http_request(&mut stream).await?;
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await?;
            write_chunk(&mut stream, message_start_event().as_bytes()).await?;
            let _ = sent_tx.send(());
            let mut byte = [0u8; 1];
            let read = stream.read(&mut byte).await?;
            let _ = closed_tx.send(read == 0);
            Ok::<(), io::Error>(())
        }));
        let mut client = TcpStream::connect(app.proxy_addr).await?;
        write_proxy_request(&mut client, "t4-stream-downstream-cancel", true).await?;
        tokio::time::timeout(WAIT_TIMEOUT, sent_rx).await??;
        let mut reader = BufReader::new(client);
        read_until_contains(&mut reader, "event: message_start").await?;
        drop(reader);
        assert!(tokio::time::timeout(WAIT_TIMEOUT, closed_rx).await??);
        let row = app.event("t4-stream-downstream-cancel").await;
        assert_eq!(row.status, 499);
        assert_eq!(
            row.error_code.as_deref(),
            Some("client_closed_request")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_count_tokens_happy() -> TestResult {
    TestApp::run(TestAppOptions::fake("count-tokens"), |app| async move {
        let response = request(
            app.proxy_addr,
            "POST",
            "/v1/messages/count_tokens",
            &[
                ("content-type", "application/json"),
                ("request-id", "t4-count-tokens"),
            ],
            &message_body(false),
        )
        .await?;
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body)?,
            json!({"input_tokens": 100})
        );
        let row = app.event("t4-count-tokens").await;
        assert_eq!(row.status, 200);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__proxy_models_happy() -> TestResult {
    TestApp::run(TestAppOptions::fake("models"), |app| async move {
        let response = request(
            app.proxy_addr,
            "GET",
            "/v1/models",
            &[("request-id", "t4-models")],
            &[],
        )
        .await?;
        assert_eq!(response.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&response.body)?;
        assert_eq!(body["type"], "list");
        assert_eq!(body["first_id"], "claude-fable-5");
        let row = app.event("t4-models").await;
        assert_eq!(row.status, 200);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__admin_crud_on_separate_listener() -> TestResult {
    let script = MessageScript::new();
    let mut success = ScriptedMessageResponse::ok()
        .with_header("anthropic-ratelimit-requests-remaining", "999")
        .with_header("anthropic-ratelimit-requests-limit", "1000")
        .with_header("anthropic-ratelimit-requests-reset", "2026-09-10T00:00:01Z")
        .with_header("anthropic-ratelimit-tokens-remaining", "99900")
        .with_header("anthropic-ratelimit-tokens-limit", "100000")
        .with_header("anthropic-ratelimit-tokens-reset", "2026-09-10T00:00:02Z");
    success.body = json!({
        "id": "msg_t4_admin_key",
        "type": "message",
        "role": "assistant",
        "model": MODEL,
        "content": [{"type": "text", "text": "ok"}],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 10,
            "output_tokens": 1_000,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0
        }
    });
    let mut options = TestAppOptions::fake("admin-separate");
    options.fake_config.message_script = Some(script.clone());
    options.downstream_api_key_auth = true;
    options.downstream_cost_cap_micros = None;
    TestApp::run(options, move |app| async move {
        let body = serde_json::to_vec(&json!({
            "name": "t4-admin-created",
            "kind": "machine",
            "allowed_models": [],
            "default_limits": []
        }))?;
        let created = request(
            app.admin_addr,
            "POST",
            "/admin/v1/principals",
            &[
                ("authorization", &format!("Bearer {ADMIN_TOKEN}")),
                ("content-type", "application/json"),
            ],
            &body,
        )
        .await?;
        assert_eq!(
            created.status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&created.body)
        );
        let created_body: Value = serde_json::from_slice(&created.body)?;
        let id = created_body["id"].as_str().expect("created principal id");
        let fetched = request(
            app.admin_addr,
            "GET",
            &format!("/admin/v1/principals/{id}"),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
        assert_eq!(fetched.status, StatusCode::OK);
        assert_eq!(
            serde_json::from_slice::<Value>(&fetched.body)?["name"],
            "t4-admin-created"
        );
        let proxy = request(
            app.proxy_addr,
            "GET",
            &format!("/admin/v1/principals/{id}"),
            &[],
            &[],
        )
        .await?;
        assert_eq!(proxy.status, StatusCode::NOT_FOUND);

        let key_id = app
            .downstream_key_id
            .as_deref()
            .expect("API-key-auth fixture exposes a key id");
        script.push_response(success);
        let happy = proxy_request(&app, "t4-admin-key-active", false, &[]).await?;
        assert_eq!(
            happy.status,
            StatusCode::OK,
            "happy response: {}",
            String::from_utf8_lossy(&happy.body)
        );
        for header in [
            "anthropic-ratelimit-requests-remaining",
            "anthropic-ratelimit-requests-limit",
            "anthropic-ratelimit-requests-reset",
            "anthropic-ratelimit-tokens-remaining",
            "anthropic-ratelimit-tokens-limit",
            "anthropic-ratelimit-tokens-reset",
        ] {
            assert!(
                happy.headers.contains_key(header),
                "missing rate-limit header {header}"
            );
        }
        app.event("t4-admin-key-active").await;
        let usage = request(
            app.admin_addr,
            "GET",
            &format!("/admin/principals/t4-principal/keys/{key_id}/usage?range=1h&step=1h"),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
        assert_eq!(
            usage.status,
            StatusCode::OK,
            "usage response: {}",
            String::from_utf8_lossy(&usage.body)
        );
        let usage_body: Value = serde_json::from_slice(&usage.body)?;
        let first_series = usage_body["series"]
            .as_array()
            .expect("usage series array")
            .iter()
            .find(|series| series["request_count"].as_u64().unwrap_or(0) > 0)
            .expect("usage series with request count");
        assert_eq!(first_series["cost_usd_micros"].as_i64(), Some(15_030));
        assert!(first_series["request_count"].as_u64().unwrap_or(0) > 0);
        ManagedKeyStore::update(
            app.storage.as_ref(),
            "t4-principal",
            key_id,
            cc_lb_storage_api::ApiKeyMutation {
                limit_overrides: Some(vec![cc_lb_storage_api::Limit {
                    kind: cc_lb_storage_api::LimitKind::CostUsd,
                    window_secs: 60 * 60,
                    cap_micros: 1,
                }]),
                ..Default::default()
            },
        )
        .await?;

        let rejected = proxy_request(&app, "t4-admin-key-cost-cap", false, &[]).await?;
        assert_eq!(rejected.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(rejected.headers.contains_key("retry-after"));
        let rejected_body: Value = serde_json::from_slice(&rejected.body)?;
        assert_eq!(rejected_body["error"]["limit_kind"], "cost_usd");

        let disable = request(
            app.admin_addr,
            "POST",
            &format!("/admin/principals/t4-principal/keys/{key_id}/disable"),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
        assert_eq!(
            disable.status,
            StatusCode::OK,
            "disable response: {}",
            String::from_utf8_lossy(&disable.body)
        );
        let disabled = proxy_request(&app, "t4-admin-key-disabled", false, &[]).await?;
        assert_eq!(
            disabled.status,
            StatusCode::FORBIDDEN,
            "disabled response: {}",
            String::from_utf8_lossy(&disabled.body)
        );

        let enable = request(
            app.admin_addr,
            "POST",
            &format!("/admin/principals/t4-principal/keys/{key_id}/enable"),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
        assert_eq!(
            enable.status,
            StatusCode::OK,
            "enable response: {}",
            String::from_utf8_lossy(&enable.body)
        );
        let key_response = request(
            app.admin_addr,
            "GET",
            &format!("/admin/principals/t4-principal/keys/{key_id}"),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
        assert_eq!(
            key_response.status,
            StatusCode::OK,
            "get key response: {}",
            String::from_utf8_lossy(&key_response.body)
        );
        let key_body: Value = serde_json::from_slice(&key_response.body)?;
        assert_eq!(key_body["status"], "active");

        let revoke = request(
            app.admin_addr,
            "POST",
            &format!("/admin/principals/t4-principal/keys/{key_id}/revoke"),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
            &[],
        )
        .await?;
        assert_eq!(
            revoke.status,
            StatusCode::OK,
            "revoke response: {}",
            String::from_utf8_lossy(&revoke.body)
        );
        let revoked = proxy_request(&app, "t4-admin-key-revoked", false, &[]).await?;
        assert_eq!(
            revoked.status,
            StatusCode::UNAUTHORIZED,
            "revoked response: {}",
            String::from_utf8_lossy(&revoked.body)
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn t4__admin_sse_emits_final_request_event_update() -> TestResult {
    TestApp::run(TestAppOptions::fake("admin-sse"), |app| async move {
        let mut stream = TcpStream::connect(app.admin_addr).await?;
        let request_head = format!(
            "GET /admin/events/stream HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {ADMIN_TOKEN}\r\nAccept: text/event-stream\r\nConnection: keep-alive\r\n\r\n",
            app.admin_addr
        );
        stream.write_all(request_head.as_bytes()).await?;
        let mut reader = BufReader::new(stream);
        read_until_contains(&mut reader, "\r\n\r\n").await?;
        let response = proxy_request(&app, "t4-admin-sse", false, &[]).await?;
        assert_eq!(response.status, StatusCode::OK);
        let frame = next_matching_sse(&mut reader, "t4-admin-sse").await?;
        assert_eq!(frame["phase"], "final");
        assert_eq!(frame["payload"]["event"]["status"], 200);
        assert_eq!(frame["payload"]["event"]["model"], MODEL);
        assert!(
            frame["payload"]["event"]["input_tokens"]
                .as_u64()
                .unwrap_or_default()
                > 0
        );
        drop(reader);
        Ok(())
    })
    .await
}

async fn request_with_declared_content_length(
    addr: std::net::SocketAddr,
    request_id: &str,
    api_key: &str,
    content_length: usize,
) -> TestResult<super::support::TestResponse> {
    let mut stream = TcpStream::connect(addr).await?;
    let head = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nRequest-Id: {request_id}\r\nX-Api-Key: {api_key}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await?;
    let mut bytes = Vec::new();
    tokio::time::timeout(WAIT_TIMEOUT, stream.read_to_end(&mut bytes))
        .await
        .map_err(|_| "body-limit response did not complete within 5s")??;
    Ok(parse_raw_response(&bytes)?)
}

async fn write_proxy_request(
    stream: &mut TcpStream,
    request_id: &str,
    streaming: bool,
) -> io::Result<()> {
    let body = message_body(streaming);
    let accept = if streaming {
        "Accept: text/event-stream\r\n"
    } else {
        ""
    };
    let head = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nRequest-Id: {request_id}\r\n{accept}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await
}

fn message_start_event() -> String {
    format!(
        "event: message_start\ndata: {}\n\n",
        json!({"type":"message_start","message":{"id":"msg_t4","type":"message","role":"assistant","model":MODEL,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}})
    )
}

async fn write_chunk(stream: &mut TcpStream, bytes: &[u8]) -> io::Result<()> {
    stream
        .write_all(format!("{:X}\r\n", bytes.len()).as_bytes())
        .await?;
    stream.write_all(bytes).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}

async fn read_until_contains(
    reader: &mut BufReader<TcpStream>,
    needle: &str,
) -> TestResult<String> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        let mut collected = String::new();
        loop {
            let mut line = String::new();
            let read = reader.read_line(&mut line).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("stream ended before {needle}"),
                ));
            }
            collected.push_str(&line);
            if collected.contains(needle) {
                return Ok(collected);
            }
        }
    })
    .await
    .map_err(|_| format!("stream did not contain {needle} within 5s"))?
    .map_err(Into::into)
}

async fn next_matching_sse(
    reader: &mut BufReader<TcpStream>,
    request_id: &str,
) -> TestResult<Value> {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "admin SSE ended",
                ));
            }
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            let value: Value = serde_json::from_str(data.trim())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if value["phase"] == "final" && value["payload"]["event"]["request_id"] == request_id {
                return Ok(value);
            }
        }
    })
    .await
    .map_err(|_| "admin SSE final frame was not observed within 5s")?
    .map_err(Into::into)
}

async fn tls_request(
    addr: std::net::SocketAddr,
    cert_path: &std::path::Path,
    method: &str,
    path: &str,
    body: &[u8],
) -> TestResult<super::support::TestResponse> {
    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert_path)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let expected_cert = certs
        .first()
        .ok_or("TLS fixture contains no certificate")?
        .as_ref()
        .to_vec();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ExactCertificateVerifier {
            expected_cert,
            provider,
        }))
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr).await?;
    let server_name = ServerName::try_from("localhost")?.to_owned();
    let mut stream = connector.connect(server_name, tcp).await?;
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    Ok(parse_raw_response(&bytes)?)
}

#[derive(Debug)]
struct ExactCertificateVerifier {
    expected_cert: Vec<u8>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for ExactCertificateVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        if end_entity.as_ref() == self.expected_cert {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(RustlsError::InvalidCertificate(
                CertificateError::UnknownIssuer,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
