use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const RESPONSE_CREATE: &str =
    r#"{"type":"response.create","model":"gpt-4.1","store":false,"input":[]}"#;

async fn spawn_http_error_provider(
    status: u16,
    body: String,
) -> (Url, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider listener has an address");
    let requests = Arc::new(AtomicUsize::new(0));
    let requests_for_task = requests.clone();
    let reason = match status {
        401 => "Unauthorized",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
Content-Type: application/json\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\
Retry-After: 17\r\n\
X-RateLimit-Limit-Requests: 100\r\n\
X-RateLimit-Remaining-Requests: 3\r\n\
X-RateLimit-Reset-Requests: 99\r\n\
X-Provider-Secret: provider-secret-header\r\n\
Set-Cookie: provider-secret-cookie\r\n\r\n{}",
        body.len(),
        body,
    );
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            requests_for_task.fetch_add(1, Ordering::SeqCst);
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            while let Ok(read) = socket.read(&mut chunk).await {
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                if request.len() >= 64 * 1024 {
                    break;
                }
            }
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        }
    });
    (
        Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        requests,
        task,
    )
}

async fn spawn_responses_success_provider() -> (Url, Arc<AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener binds");
    let address = listener
        .local_addr()
        .expect("provider listener has an address");
    let requests = Arc::new(AtomicUsize::new(0));
    let requests_for_task = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            requests_for_task.fetch_add(1, Ordering::SeqCst);
            let Ok(mut provider) = accept_async(socket).await else {
                continue;
            };
            while let Some(Ok(message)) = provider.next().await {
                if message.is_text() {
                    provider
                        .send(UpstreamMessage::Text(
                            r#"{"type":"response.created","response":{"id":"healthy","store":false}}"#
                                .into(),
                        ))
                        .await
                        .expect("healthy provider sends response");
                    break;
                }
            }
        }
    });
    (
        Url::parse(&format!("http://{address}/")).expect("provider URL parses"),
        requests,
        task,
    )
}

async fn receive_error(client: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("WebSocket error arrives")
        .expect("WebSocket remains open for the provider error")
        .expect("WebSocket error decodes");
    serde_json::from_str(message.to_text().expect("WebSocket error is text"))
        .expect("WebSocket error is JSON")
}

#[tokio::test]
async fn websocket_handshake_responses_provider_500_opens_local_breaker_after_five_attempts() {
    let (provider_url, requests, provider_task) = spawn_http_error_provider(
        500,
        r#"{"error":{"code":"provider_server_error","message":"provider-body-secret"}}"#
            .to_owned(),
    )
    .await;
    let router = websocket_test_router(
        vec![websocket_upstream(1, "openai", provider_url)],
        TerminalStrategy::FirstPick,
    )
    .await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, response) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake succeeds");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

    for attempt in 0..6 {
        client
            .send(UpstreamMessage::Text(RESPONSE_CREATE.into()))
            .await
            .expect("response.create sends");
        let error = receive_error(&mut client).await;
        if attempt < 5 {
            assert_eq!(error["status"], 500, "provider status is preserved");
        } else {
            assert_eq!(error["status"], 502, "the sixth attempt is breaker-open");
            assert_eq!(error["error"]["code"], "upstream_unavailable");
            assert!(error["error"]["message"]
                .as_str()
                .is_some_and(|message| !message.contains("provider-body-secret")));
        }
        let serialized = error.to_string();
        assert!(!serialized.contains("provider-body-secret"));
        assert!(!serialized.contains("provider-secret-header"));
        assert!(!serialized.contains("provider-secret-cookie"));
    }

    assert_eq!(requests.load(Ordering::SeqCst), 5);
    let _ = client.close(None).await;
    proxy_task.abort();
    provider_task.abort();
}

#[tokio::test]
async fn websocket_handshake_responses_provider_401_and_429_preserve_status_and_generic_code() {
    for (status, body) in [
        (
            401,
            r#"{"error":{"code":"provider_auth_failed","message":"provider-body-secret"}}"#,
        ),
        (
            429,
            r#"{"error":{"code":"provider_rate_limited","message":"provider-body-secret"}}"#,
        ),
    ] {
        let (provider_url, requests, provider_task) =
            spawn_http_error_provider(status, body.to_owned()).await;
        let router = websocket_test_router(
            vec![websocket_upstream(1, "openai", provider_url)],
            TerminalStrategy::FirstPick,
        )
        .await;
        let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
        let (mut client, response) = connect_async(websocket_request(proxy_address))
            .await
            .expect("downstream handshake succeeds");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

        for _ in 0..6 {
            client
                .send(UpstreamMessage::Text(RESPONSE_CREATE.into()))
                .await
                .expect("response.create sends");
            let error = receive_error(&mut client).await;
            assert_eq!(error["status"], status);
            assert_eq!(error["error"]["code"], "upstream_unavailable");
            assert_ne!(error["error"]["code"], "authentication_error");
            assert_eq!(error["headers"]["retry-after"], "17");
            assert_eq!(error["headers"]["x-ratelimit-limit-requests"], "100");
            assert_eq!(error["headers"]["x-ratelimit-remaining-requests"], "3");
            assert_eq!(error["headers"]["x-ratelimit-reset-requests"], "99");
            assert!(error.get("body").is_none());
            assert!(error["headers"].get("authorization").is_none());
            assert!(error["headers"].get("cookie").is_none());
            assert!(error["headers"].get("set-cookie").is_none());
            let serialized = error.to_string();
            assert!(!serialized.contains("provider-body-secret"));
            assert!(!serialized.contains("provider-secret-header"));
            assert!(!serialized.contains("provider-secret-cookie"));
            assert!(!serialized.contains("set-cookie"));
            assert!(!serialized.contains("authorization"));
            assert!(!serialized.contains("cookie"));
        }

        assert_eq!(requests.load(Ordering::SeqCst), 6);
        let _ = client.close(None).await;
        proxy_task.abort();
        provider_task.abort();
    }
}

#[tokio::test]
async fn websocket_handshake_responses_candidate_loop_falls_back_after_provider_401_and_500() {
    for status in [401, 500] {
        let (first_url, first_requests, first_task) = spawn_http_error_provider(
            status,
            r#"{"error":{"code":"provider_rejected","message":"provider-body-secret"}}"#
                .to_owned(),
        )
        .await;
        let first_port = first_url.port().expect("first provider URL has a port");
        let second_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fallback listener binds");
        let second_address = second_listener
            .local_addr()
            .expect("fallback listener has an address");
        let second_task = tokio::spawn(async move {
            let (socket, _) = second_listener
                .accept()
                .await
                .expect("fallback connection arrives");
            let mut provider = accept_async(socket)
                .await
                .expect("fallback WebSocket handshake succeeds");
            let create = provider
                .next()
                .await
                .expect("fallback receives response.create")
                .expect("fallback response.create decodes");
            assert!(create
                .to_text()
                .expect("fallback create is text")
                .contains("response.create"));
            provider
                .send(UpstreamMessage::Text(
                    r#"{"type":"response.created","response":{"id":"fallback","store":false}}"#
                        .into(),
                ))
                .await
                .expect("fallback sends a response");
            while provider.next().await.is_some() {}
        });
        let first = websocket_upstream(
            1,
            "first-openai",
            Url::parse(&format!("http://localhost:{first_port}/"))
                .expect("first provider URL parses"),
        );
        let second = websocket_upstream(
            2,
            "second-openai",
            Url::parse(&format!("http://127.0.0.1:{}/", second_address.port()))
                .expect("fallback provider URL parses"),
        );
        let router = websocket_test_router(vec![first, second], TerminalStrategy::FirstPick).await;
        let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
        let (mut client, response) = connect_async(websocket_request(proxy_address))
            .await
            .expect("downstream handshake succeeds");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        client
            .send(UpstreamMessage::Text(RESPONSE_CREATE.into()))
            .await
            .expect("response.create sends");
        let message = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("fallback response arrives")
            .expect("fallback keeps downstream open")
            .expect("fallback response decodes");
        assert_eq!(
            message,
            UpstreamMessage::Text(
                r#"{"type":"response.created","response":{"id":"fallback","store":false}}"#
                    .into(),
            )
        );
        assert_eq!(first_requests.load(Ordering::SeqCst), 1);
        let _ = client.close(None).await;
        proxy_task.abort();
        first_task.abort();
        let _ = tokio::time::timeout(Duration::from_secs(5), second_task).await;
    }
}

#[tokio::test]
async fn websocket_handshake_responses_same_host_breakers_are_scoped_to_upstream() {
    let (failing_url, failing_requests, failing_task) = spawn_http_error_provider(
        500,
        r#"{"error":{"code":"provider_server_error","message":"provider-body-secret"}}"#
            .to_owned(),
    )
    .await;
    let (healthy_url, healthy_requests, healthy_task) = spawn_responses_success_provider().await;
    let failing = websocket_upstream(1, "failing-credential", failing_url);
    let healthy = websocket_upstream(2, "healthy-credential", healthy_url);
    let state = websocket_test_state(
        vec![failing],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let router = proxy_router(state.clone(), 30);
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;

    for _ in 0..5 {
        let (mut client, response) = connect_async(websocket_request(proxy_address))
            .await
            .expect("downstream handshake succeeds");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        client
            .send(UpstreamMessage::Text(RESPONSE_CREATE.into()))
            .await
            .expect("response.create sends");
        let error = receive_error(&mut client).await;
        assert_eq!(error["status"], 500);
        let _ = client.close(None).await;
    }
    assert_eq!(failing_requests.load(Ordering::SeqCst), 5);

    replace_websocket_upstreams(&state, vec![healthy]);
    for _ in 0..10 {
        let (mut client, response) = connect_async(websocket_request(proxy_address))
            .await
            .expect("healthy downstream handshake succeeds");
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        client
            .send(UpstreamMessage::Text(RESPONSE_CREATE.into()))
            .await
            .expect("response.create sends");
        let message = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("healthy response arrives")
            .expect("healthy downstream remains open")
            .expect("healthy response decodes");
        assert_eq!(
            message,
            UpstreamMessage::Text(
                r#"{"type":"response.created","response":{"id":"healthy","store":false}}"#
                    .into(),
            )
        );
        let _ = client.close(None).await;
    }
    assert_eq!(healthy_requests.load(Ordering::SeqCst), 10);
    proxy_task.abort();
    failing_task.abort();
    healthy_task.abort();
}
