use super::*;

struct RealtimeKeySignerFactory;

impl ApiKeyAwareSignerFactory for RealtimeKeySignerFactory {
    fn with_router_choice(&self, _name: String) -> Arc<dyn SignerFactory> { Arc::new(Self) }
}

#[async_trait]
impl SignerFactory for RealtimeKeySignerFactory {
    async fn build(&self, upstream: &cc_lb_domain::Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let kind = match upstream {
            cc_lb_domain::Upstream::GeminiDirect { .. } => cc_lb_signer_anthropic_key::ProviderKeyKind::Gemini,
            _ => cc_lb_signer_anthropic_key::ProviderKeyKind::Openai,
        };
        cc_lb_signer_anthropic_key::ProviderKeySignerFactory::new(kind, "provider-test-credential")?
            .build(upstream).await
    }
}

struct HandshakeCapture {
    sender: Option<tokio::sync::oneshot::Sender<(String, HeaderMap)>>,
}

impl tokio_tungstenite::tungstenite::handshake::server::Callback for HandshakeCapture {
    fn on_request(
        mut self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        self.sender
            .take()
            .expect("handshake sender")
            .send((request.uri().to_string(), request.headers().clone()))
            .expect("handshake receiver");
        Ok(response)
    }
}

fn realtime_request(address: SocketAddr, path: &str) -> http::Request<()> {
    let mut request = websocket_request(address);
    *request.uri_mut() = format!("ws://{address}{path}").parse().expect("target URI");
    request
}

#[tokio::test]
async fn realtime_and_live_reject_authentication_before_upgrade_or_connect() {
    for (path, provider) in [
        (
            "/openai/v1/realtime?model=gpt-realtime",
            cc_lb_domain::Provider::Openai,
        ),
        (
            "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            cc_lb_domain::Provider::Gemini,
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("provider listener");
        let address = listener.local_addr().unwrap();
        let mut record = websocket_upstream(
            1,
            "provider",
            Url::parse(&format!("http://{address}/")).unwrap(),
        );
        if provider == cc_lb_domain::Provider::Gemini {
            record.kind = UpstreamKind::GeminiApiKey;
        }
        let router = websocket_test_router(vec![record], TerminalStrategy::FirstPick).await;
        let (proxy, task) = spawn_proxy_router(router).await;
        let mut request = realtime_request(proxy, path);
        request.headers_mut().remove("x-api-key");
        let error = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
            .await
            .unwrap()
            .unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("HTTP auth rejection expected")
        };
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value =
            serde_json::from_slice(response.body().as_ref().expect("error body"))
                .expect("auth response is JSON");
        match provider {
            cc_lb_domain::Provider::Openai => {
                assert!(body["error"].is_object());
                assert!(body.get("type").is_none());
            }
            cc_lb_domain::Provider::Gemini => {
                assert!(body["error"]["code"].is_u64());
                assert!(body["error"]["status"].is_string());
            }
            cc_lb_domain::Provider::Anthropic => unreachable!("fixture covers both routes"),
        }
        assert!(
            listener.accept().now_or_never().is_none(),
            "unauthenticated caller never connects to provider"
        );
        task.abort();
    }
}

#[tokio::test]
async fn realtime_requires_an_allowed_model_before_provider_selection() {
    let router = websocket_test_router_with_allowed_models(
        vec![websocket_upstream(
            1,
            "openai",
            Url::parse("http://127.0.0.1:1/").expect("provider URL parses"),
        )],
        TerminalStrategy::FirstPick,
        vec!["allowed-model".to_owned()],
    )
    .await;
    let (proxy, task) = spawn_proxy_router(router).await;

    for (path, expected_status, expected_code) in [
        (
            "/openai/v1/realtime",
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
        ),
        (
            "/openai/v1/realtime?model=blocked-model",
            StatusCode::FORBIDDEN,
            "model_not_allowed",
        ),
    ] {
        let error = tokio::time::timeout(Duration::from_secs(5), connect_async(realtime_request(proxy, path)))
            .await
            .expect("model admission response arrives")
            .expect_err("model admission rejects before upgrade");
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("HTTP model admission rejection expected");
        };
        assert_eq!(response.status(), expected_status);
        let body: serde_json::Value =
            serde_json::from_slice(response.body().as_ref().expect("error body")).unwrap();
        assert_eq!(body["error"]["code"], expected_code);
    }

    task.abort();
}

#[tokio::test]
async fn gemini_live_rejects_unsupported_operation_before_upgrade() {
    let router = websocket_test_router(Vec::new(), TerminalStrategy::FirstPick).await;
    let (proxy, task) = spawn_proxy_router(router).await;
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(realtime_request(proxy, "/gemini/ws/unsupported")),
    )
    .await
    .unwrap()
    .unwrap_err();
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("HTTP operation rejection expected")
    };
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = response.body().as_ref().expect("error body");
    let body: serde_json::Value = serde_json::from_slice(body).expect("JSON error body");
    assert_eq!(body["error"]["type"], "operation_not_supported");
    assert_eq!(body["error"]["status"], "NOT_FOUND");
    task.abort();
}

#[tokio::test]
async fn gemini_live_rejects_without_eligible_api_key_upstream_before_upgrade() {
    let base_url = Url::parse("http://127.0.0.1:1/").unwrap();
    let mut oauth = websocket_upstream(1, "oauth-only", base_url.clone());
    oauth.kind = UpstreamKind::GeminiOauth;
    let mut disabled = websocket_upstream(2, "disabled-key", base_url.clone());
    disabled.kind = UpstreamKind::GeminiApiKey;
    disabled.enabled = false;
    let mut deleted = websocket_upstream(3, "deleted-key", base_url.clone());
    deleted.kind = UpstreamKind::GeminiApiKey;
    deleted.deleted_at_unix_secs = Some(1);
    let other_provider = websocket_upstream(4, "openai-key", base_url);
    for records in [
        Vec::new(),
        vec![oauth],
        vec![disabled],
        vec![deleted],
        vec![other_provider],
    ] {
        let router = websocket_test_router(records, TerminalStrategy::FirstPick).await;
        let (proxy, task) = spawn_proxy_router(router).await;
        let result = connect_async(realtime_request(
            proxy,
            "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
        ))
        .await;
        task.abort();
        let error = result.expect_err("ineligible Live request must not upgrade");
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("HTTP routing rejection expected")
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value =
            serde_json::from_slice(response.body().as_ref().expect("error body")).unwrap();
        assert_eq!(body["error"]["code"], "route_no_upstream_after_filter");
        assert_eq!(body["error"]["message"], "no eligible provider API-key upstream");
    }
}

#[tokio::test]
async fn realtime_and_live_pin_relay_binary_and_propagate_provider_close() {
    for gemini in [false, true] {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let (handshake_tx, handshake_rx) = tokio::sync::oneshot::channel();
        let first_task = tokio::spawn(async move {
            let (stream, _) = first_listener.accept().await.unwrap();
            let mut provider = tokio_tungstenite::accept_hdr_async(
                stream,
                HandshakeCapture {
                    sender: Some(handshake_tx),
                },
            )
            .await
            .unwrap();
            if gemini {
                let setup = provider.next().await.unwrap().unwrap();
                assert_eq!(setup.to_text().unwrap(), r#"{"setup":{"model":"models/gemini-live"}}"#);
            }
            for _ in 0..2 {
                let message = provider.next().await.unwrap().unwrap();
                provider.send(message).await.unwrap();
            }
            provider.send(UpstreamMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away,
                reason: "provider session ended".into(),
            }))).await.unwrap();
        });
        let kind = if gemini { UpstreamKind::GeminiApiKey } else { UpstreamKind::OpenaiApiKey };
        let mut first = websocket_upstream(1, "first-provider", Url::parse(&format!("http://{first_address}/")).unwrap());
        first.kind = kind;
        let mut second = websocket_upstream(2, "second-provider", Url::parse(&format!("http://{second_address}/")).unwrap());
        second.kind = kind;
        let mut oauth = second.clone();
        oauth.id = Uuid::from_u128(3);
        oauth.name = "ineligible-oauth".to_owned();
        oauth.kind = if gemini { UpstreamKind::GeminiOauth } else { UpstreamKind::OpenaiOauth };
        let drain = DrainController::new();
        let router = websocket_test_router_with_signer(vec![oauth, first, second], TerminalStrategy::FirstPick, drain.clone(), Arc::new(RealtimeKeySignerFactory)).await;
        let (proxy, task) = spawn_proxy_router(router).await;
        let path = if gemini {
            "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=caller-secret&alt=json"
        } else { "/openai/v1/realtime?model=gpt-realtime&key=caller-secret" };
        let mut request = realtime_request(proxy, path);
        request.headers_mut().insert("openai-beta", HeaderValue::from_static("realtime=v1"));
        let (mut client, response) = tokio::time::timeout(Duration::from_secs(5), connect_async(request)).await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        if gemini { client.send(UpstreamMessage::Text(r#"{"setup":{"model":"models/gemini-live"}}"#.into())).await.unwrap(); }
        let (target, headers) = tokio::time::timeout(Duration::from_secs(5), handshake_rx).await.unwrap().unwrap();
        assert!(!target.contains("caller-secret"));
        assert!(!target.contains("key="));
        if gemini {
            assert_eq!(target, "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?alt=json");
            assert_eq!(headers["x-goog-api-key"], "provider-test-credential");
        } else {
            assert_eq!(target, "/v1/realtime?model=gpt-realtime");
            assert_eq!(headers["authorization"], "Bearer provider-test-credential");
            assert_eq!(headers["openai-beta"], "realtime=v1");
        }
        for message in [UpstreamMessage::Text("unchanged text".into()), UpstreamMessage::Binary(Bytes::from_static(&[0, 1, 255]))] {
            client.send(message.clone()).await.unwrap();
            assert_eq!(tokio::time::timeout(Duration::from_secs(5), client.next()).await.unwrap().unwrap().unwrap(), message);
        }
        let close = tokio::time::timeout(Duration::from_secs(5), client.next()).await.unwrap().unwrap().unwrap();
        let UpstreamMessage::Close(Some(close)) = close else { panic!("provider close frame expected") };
        assert_eq!(u16::from(close.code), 1001);
        assert_eq!(close.reason.as_str(), "provider session ended");
        first_task.await.unwrap();
        assert!(!drain.await_drained(Duration::from_secs(5)).await);
        assert!(second_listener.accept().now_or_never().is_none(), "never reconnect or fail over mid-session");
        task.abort();
    }
}

#[tokio::test]
async fn realtime_client_close_reaches_pinned_provider() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let close = socket.next().await.unwrap().unwrap();
        let UpstreamMessage::Close(Some(close)) = close else { panic!("client close must propagate") };
        assert_eq!(u16::from(close.code), 1000);
        assert_eq!(close.reason.as_str(), "client finished");
    });
    let router = websocket_test_router(vec![websocket_upstream(1, "provider", Url::parse(&format!("http://{address}/")).unwrap())], TerminalStrategy::FirstPick).await;
    let (proxy, task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(realtime_request(proxy, "/openai/v1/realtime?model=gpt-realtime")).await.unwrap();
    client.send(UpstreamMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal, reason: "client finished".into(),
    }))).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), provider).await.unwrap().unwrap();
    task.abort();
}

#[tokio::test]
async fn live_waiting_for_setup_and_realtime_active_connections_respect_drain() {
    let drain = DrainController::new();
    let mut record = websocket_upstream(1, "live-key", Url::parse("http://127.0.0.1:1/").unwrap());
    record.kind = UpstreamKind::GeminiApiKey;
    let router = websocket_test_router_with_drain(vec![record], TerminalStrategy::FirstPick, drain.clone()).await;
    let (proxy, task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(realtime_request(proxy, "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent")).await.unwrap();
    drain.trigger();
    let _ = tokio::time::timeout(Duration::from_secs(5), client.next()).await.unwrap();
    assert!(!drain.await_drained(Duration::from_secs(5)).await);
    task.abort();
}

const PROVIDER_BODY_SENTINEL: &str = "provider-body-sentinel-must-not-escape";

struct RejectingHandshake {
    status: StatusCode,
}

impl tokio_tungstenite::tungstenite::handshake::server::Callback for RejectingHandshake {
    fn on_request(
        self,
        _request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        _response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        // Keep the provider rejection a valid RFC 6455 HTTP response so the
        // proxy can preserve its provider status for the downstream handshake.
        // QA-04: an invalid wire value such as [0xff] cannot be represented by
        // a contract-faithful mock response; tungstenite rejects that response
        // before the proxy's header filtering path can be exercised.
        let response = http::Response::builder()
            .status(self.status)
            .header("retry-after", "17")
            .header("x-ratelimit-limit", "100")
            .header("x-ratelimit-remaining", "99")
            .header("x-ratelimit-reset", "1700000000")
            .header("cookie", "provider-cookie-secret")
            .header("authorization", "Bearer provider-auth-secret")
            .body(Some(PROVIDER_BODY_SENTINEL.to_owned()))
            .expect("provider rejection response");
        Err(response)
    }
}

async fn rejecting_provider(
    status: StatusCode,
) -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener");
    let address = listener.local_addr().expect("provider address");
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_attempts = attempts.clone();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("provider accepts TCP");
            observed_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = tokio_tungstenite::accept_hdr_async(
                stream,
                RejectingHandshake { status },
            )
            .await;
        }
    });
    (address, attempts, task)
}

async fn accepting_provider() -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("provider listener");
    let address = listener.local_addr().expect("provider address");
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed_attempts = attempts.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            observed_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let Ok(mut provider) = tokio_tungstenite::accept_async(stream).await else {
                continue;
            };
            provider
                .send(UpstreamMessage::Text(
                    r#"{"type":"provider_ready"}"#.into(),
                ))
                .await
                .expect("provider ready frame sends");
            while provider.next().await.is_some() {}
        }
    });
    (address, attempts, task)
}

fn assert_same_websocket_host(failing: &Url, healthy: &Url) {
    assert_eq!(
        failing.host_str(),
        healthy.host_str(),
        "regression requires same-host provider URLs"
    );
}

fn assert_distinct_websocket_upstreams(failing: &UpstreamRecord, healthy: &UpstreamRecord) {
    assert_ne!(
        failing.id, healthy.id,
        "regression requires distinct upstream identities"
    );
}

fn provider_ready_message() -> UpstreamMessage {
    UpstreamMessage::Text(r#"{"type":"provider_ready"}"#.into())
}

fn assert_safe_http_rejection(response: &http::Response<Option<Vec<u8>>>, status: StatusCode) {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()["retry-after"], "17");
    assert_eq!(response.headers()["x-ratelimit-limit"], "100");
    assert_eq!(response.headers()["x-ratelimit-remaining"], "99");
    assert_eq!(response.headers()["x-ratelimit-reset"], "1700000000");
    assert!(!response.headers().contains_key("cookie"));
    assert!(!response.headers().contains_key("authorization"));
    let body = response.body().as_ref().expect("provider rejection body");
    let value: serde_json::Value = serde_json::from_slice(body).expect("proxy error JSON");
    assert_eq!(value["error"]["code"], "upstream_unavailable");
    assert_ne!(value["error"]["code"], "authentication_error");
    assert!(
        !String::from_utf8_lossy(body).contains(PROVIDER_BODY_SENTINEL),
        "provider body must not escape the proxy"
    );
}

fn assert_safe_json_rejection(
    value: &serde_json::Value,
    status: StatusCode,
    expect_headers: bool,
) {
    assert_eq!(value["status"], status.as_u16());
    assert_eq!(value["error"]["code"], "upstream_unavailable");
    assert_ne!(value["error"]["code"], "authentication_error");
    assert!(
        !value.to_string().contains(PROVIDER_BODY_SENTINEL),
        "provider body must not escape the proxy"
    );
    let Some(headers) = value.get("headers").and_then(serde_json::Value::as_object) else {
        assert!(!expect_headers, "safe provider headers should be retained");
        return;
    };
    assert!(expect_headers);
    assert_eq!(headers["retry-after"], "17");
    assert_eq!(headers["x-ratelimit-limit"], "100");
    assert_eq!(headers["x-ratelimit-remaining"], "99");
    assert_eq!(headers["x-ratelimit-reset"], "1700000000");
    assert!(!headers.contains_key("cookie"));
    assert!(!headers.contains_key("authorization"));
    assert!(headers.len() <= 32);
    for value in headers.values() {
        let value = value.as_str().expect("serialized safe headers are strings");
        assert!(value.len() <= 1024);
    }
}

async fn openai_rejection_attempt(
    proxy: SocketAddr,
    expected_status: StatusCode,
) -> http::Response<Option<Vec<u8>>> {
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(realtime_request(proxy, "/openai/v1/realtime?model=gpt-realtime")),
    )
    .await
    .expect("OpenAI handshake response arrives")
    .expect_err("provider rejection must not upgrade");
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("OpenAI provider rejection must be an HTTP response");
    };
    let response = *response;
    assert_safe_http_rejection(&response, expected_status);
    response
}

async fn gemini_rejection_attempt(
    proxy: SocketAddr,
    expected_status: StatusCode,
    expect_headers: bool,
) {
    let (mut client, response) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(realtime_request(
            proxy,
            "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
        )),
    )
    .await
    .expect("Gemini downstream handshake arrives")
    .expect("Gemini downstream handshake succeeds before provider connect");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    client
        .send(UpstreamMessage::Text(
            r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
        ))
        .await
        .expect("Gemini setup sends");
    let message = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("Gemini rejection arrives")
        .expect("Gemini rejection message exists")
        .expect("Gemini rejection message decodes");
    let UpstreamMessage::Text(message) = message else {
        panic!("Gemini provider rejection must be a JSON text message");
    };
    let value: serde_json::Value =
        serde_json::from_slice(message.as_bytes()).expect("Gemini rejection is JSON");
    if expected_status == StatusCode::UNAUTHORIZED {
        assert_ne!(value["error"]["code"], "authentication_error");
    }
    assert_safe_json_rejection(&value, expected_status, expect_headers);
}

#[tokio::test]
async fn websocket_handshake_openai_provider_500_opens_local_breaker() {
    let (address, attempts, provider_task) = rejecting_provider(StatusCode::INTERNAL_SERVER_ERROR).await;
    let mut record = websocket_upstream(
        1,
        "openai-rejecting-provider",
        Url::parse(&format!("http://{address}/")).expect("provider URL"),
    );
    record.kind = UpstreamKind::OpenaiApiKey;
    let router = websocket_test_router_with_signer(
        vec![record],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(RealtimeKeySignerFactory),
    )
    .await;
    let (proxy, task) = spawn_proxy_router(router).await;

    for _ in 0..5 {
        openai_rejection_attempt(proxy, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(realtime_request(proxy, "/openai/v1/realtime?model=gpt-realtime")),
    )
    .await
    .expect("local breaker response arrives")
    .expect_err("open circuit must reject before upgrade");
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("local breaker rejection must be an HTTP response");
    };
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response.body().as_ref().expect("local breaker body");
    let value: serde_json::Value =
        serde_json::from_slice(body).expect("local breaker body is JSON");
    assert_safe_json_rejection(&value, StatusCode::BAD_GATEWAY, false);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 5);
    task.abort();
    provider_task.abort();
}

#[tokio::test]
async fn websocket_handshake_gemini_provider_500_opens_local_breaker() {
    let (address, attempts, provider_task) = rejecting_provider(StatusCode::INTERNAL_SERVER_ERROR).await;
    let mut record = websocket_upstream(
        1,
        "gemini-rejecting-provider",
        Url::parse(&format!("http://{address}/")).expect("provider URL"),
    );
    record.kind = UpstreamKind::GeminiApiKey;
    let router = websocket_test_router_with_signer(
        vec![record],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(RealtimeKeySignerFactory),
    )
    .await;
    let (proxy, task) = spawn_proxy_router(router).await;

    for _ in 0..5 {
        gemini_rejection_attempt(proxy, StatusCode::INTERNAL_SERVER_ERROR, true).await;
    }
    gemini_rejection_attempt(proxy, StatusCode::BAD_GATEWAY, false).await;
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 5);
    task.abort();
    provider_task.abort();
}

#[tokio::test]
async fn websocket_handshake_realtime_and_gemini_same_host_breakers_are_scoped_to_upstream() {
    for gemini in [false, true] {
        let (failing_address, failing_attempts, failing_task) =
            rejecting_provider(StatusCode::INTERNAL_SERVER_ERROR).await;
        let (healthy_address, healthy_attempts, healthy_task) = accepting_provider().await;
        let failing_url =
            Url::parse(&format!("http://{failing_address}/")).expect("failing provider URL");
        let healthy_url =
            Url::parse(&format!("http://{healthy_address}/")).expect("healthy provider URL");
        assert_same_websocket_host(&failing_url, &healthy_url);
        let mut failing = websocket_upstream(1, "failing-credential", failing_url);
        let mut healthy = websocket_upstream(2, "healthy-credential", healthy_url);
        assert_distinct_websocket_upstreams(&failing, &healthy);
        failing.kind = if gemini {
            UpstreamKind::GeminiApiKey
        } else {
            UpstreamKind::OpenaiApiKey
        };
        healthy.kind = failing.kind;
        let state = websocket_test_state(
            vec![failing],
            TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(RealtimeKeySignerFactory),
            Arc::new(FixtureKeyStore),
            vec!["*".to_owned()],
        )
        .await;
        let router = proxy_router(state.clone(), 30);
        let (proxy, task) = spawn_proxy_router(router).await;

        for _ in 0..5 {
            if gemini {
                gemini_rejection_attempt(proxy, StatusCode::INTERNAL_SERVER_ERROR, true).await;
            } else {
                openai_rejection_attempt(proxy, StatusCode::INTERNAL_SERVER_ERROR).await;
            }
        }
        assert_eq!(failing_attempts.load(std::sync::atomic::Ordering::SeqCst), 5);

        replace_websocket_upstreams(&state, vec![healthy]);
        let mut client = if gemini {
            let (client, response) = tokio::time::timeout(
                Duration::from_secs(5),
                connect_async(realtime_request(
                    proxy,
                    "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
                )),
            )
            .await
            .expect("healthy Gemini handshake response arrives")
            .expect("healthy Gemini handshake succeeds");
            assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
            client
        } else {
            let (client, response) = tokio::time::timeout(
                Duration::from_secs(5),
                connect_async(realtime_request(proxy, "/openai/v1/realtime?model=gpt-realtime")),
            )
            .await
            .expect("healthy OpenAI handshake response arrives")
            .expect("healthy OpenAI handshake succeeds");
            assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
            client
        };
        if gemini {
            client
                .send(UpstreamMessage::Text(
                    r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
                ))
                .await
                .expect("healthy Gemini setup sends");
        }
        let ready = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("healthy provider connection arrives")
            .expect("healthy provider remains connected")
            .expect("healthy provider frame decodes");
        assert_eq!(ready, provider_ready_message());
        let _ = client.close(None).await;
        assert_eq!(
            healthy_attempts.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        task.abort();
        failing_task.abort();
        healthy_task.abort();
    }
}

#[tokio::test]
async fn websocket_handshake_openai_provider_401_and_429_preserve_status() {
    for status in [StatusCode::UNAUTHORIZED, StatusCode::TOO_MANY_REQUESTS] {
        let (address, attempts, provider_task) = rejecting_provider(status).await;
        let mut record = websocket_upstream(
            1,
            "openai-rejecting-provider",
            Url::parse(&format!("http://{address}/")).expect("provider URL"),
        );
        record.kind = UpstreamKind::OpenaiApiKey;
        let router = websocket_test_router_with_signer(
            vec![record],
            TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(RealtimeKeySignerFactory),
        )
        .await;
        let (proxy, task) = spawn_proxy_router(router).await;

        for _ in 0..7 {
            let response = openai_rejection_attempt(proxy, status).await;
            if status == StatusCode::UNAUTHORIZED {
                let body = response.body().as_ref().expect("error body");
                let value: serde_json::Value =
                    serde_json::from_slice(body).expect("proxy error JSON");
                assert_ne!(value["error"]["code"], "authentication_error");
            }
        }
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 7);
        task.abort();
        provider_task.abort();
    }
}

#[tokio::test]
async fn websocket_handshake_gemini_provider_401_and_429_preserve_status() {
    for status in [StatusCode::UNAUTHORIZED, StatusCode::TOO_MANY_REQUESTS] {
        let (address, attempts, provider_task) = rejecting_provider(status).await;
        let mut record = websocket_upstream(
            1,
            "gemini-rejecting-provider",
            Url::parse(&format!("http://{address}/")).expect("provider URL"),
        );
        record.kind = UpstreamKind::GeminiApiKey;
        let router = websocket_test_router_with_signer(
            vec![record],
            TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(RealtimeKeySignerFactory),
        )
        .await;
        let (proxy, task) = spawn_proxy_router(router).await;

        for _ in 0..7 {
            gemini_rejection_attempt(proxy, status, true).await;
        }
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 7);
        task.abort();
        provider_task.abort();
    }
}
