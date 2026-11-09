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

fn native_websocket_path(gemini: bool) -> &'static str {
    if gemini {
        "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
    } else {
        "/openai/v1/realtime?model=gpt-realtime"
    }
}

async fn native_websocket_observed_state(
    address: SocketAddr,
    gemini: bool,
    bus: Arc<cc_lb_control::InMemoryBus>,
    concurrent_limit: Option<i64>,
) -> ProxyState {
    let mut upstream = websocket_upstream(
        1,
        "native-provider",
        Url::parse(&format!("http://{address}/")).unwrap(),
    );
    upstream.kind = if gemini {
        UpstreamKind::GeminiApiKey
    } else {
        UpstreamKind::OpenaiApiKey
    };
    let mut state = websocket_test_state(
        vec![upstream],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(RealtimeKeySignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.storage.initialize().await
        .expect("initialize the complete runtime SQLite schema");
    let lifecycle = Arc::try_unwrap(state.lifecycle)
        .unwrap_or_else(|_| panic!("private fixture lifecycle has one owner"));
    let mut lifecycle = lifecycle.with_event_bus(bus);
    if let Some(cap) = concurrent_limit {
        let view = state.dynamic_view.load();
        let principal_view = Arc::new(PrincipalView::for_tests(
            TEST_PRINCIPAL,
            true,
            vec!["*".to_owned()],
            vec![cc_lb_storage_api::principal::Limit {
                kind: cc_lb_storage_api::principal::LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: cap,
            }],
            HashMap::from([(
                TEST_PRINCIPAL.to_owned(),
                (
                    Some(Arc::new(RouterPipelineCache {
                        user_filters: Vec::new(),
                        terminal: TerminalStrategy::FirstPick,
                        instantiation_error: None,
                        provider_filters: None,
                    })),
                    DialectCache::Inherit,
                ),
            )]),
        ));
        state.dynamic_view.store(
            cc_lb_control::DynamicViewBuilder::from_view(&view)
                .principal_view(principal_view)
                .build(),
        );
        lifecycle = lifecycle.with_static_limit_subject(
            LimitEngine::new(
                Arc::new(KeyConcurrencyManager::new()),
                state.clock.clone(),
            ),
            TEST_PRINCIPAL.to_owned(),
            TEST_KEY.key_id.clone(),
            TEST_KEY.record.clone(),
        );
    }
    state.lifecycle = Arc::new(lifecycle);
    state
}

#[tokio::test(flavor = "current_thread")]
async fn realtime_and_live_principal_concurrent_limit_rejects_without_second_provider_contact() {
    for (gemini, http_control) in [(false, true), (false, false), (true, false)] {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics_handle = recorder.handle();
        let _metrics_guard = metrics::set_default_local_recorder(&recorder);
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
        let address = listener.local_addr().unwrap();
        let provider_listener = listener.clone();
        let provider = tokio::spawn(async move {
            let (stream, _) = provider_listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            if gemini {
                assert_eq!(
                    socket.next().await.unwrap().unwrap().to_text().unwrap(),
                    r#"{"setup":{"model":"models/gemini-live"}}"#,
                );
            }
            socket.send(provider_ready_message()).await.unwrap();
            while socket.next().await.is_some() {}
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let event_logger =
            cc_lb_engine::spawn_lifecycle_event_logger(bus.attach_lifecycle_writer(128));
        let state = native_websocket_observed_state(address, gemini, bus.clone(), Some(1)).await;
        let storage = state.storage.clone();
        let (audit_sink, audit_writer) =
            cc_lb_control::spawn_audit_writer(storage.clone(), 16);
        let audit_subscriber = cc_lb_engine::spawn_lifecycle_limit_rejection_audit_subscriber(
            bus.attach_lifecycle_limit_rejection_audit(128),
            Arc::new(audit_sink),
        );
        let (proxy, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
        let (mut first, _) = connect_async(realtime_request(proxy, native_websocket_path(gemini)))
            .await
            .expect("first session is admitted");
        if gemini {
            first
                .send(UpstreamMessage::Text(
                    r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
                ))
                .await
                .unwrap();
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), first.next())
                .await.unwrap().unwrap().unwrap(),
            provider_ready_message(),
        );
        if http_control {
            let response = reqwest::Client::new()
                .post(format!("http://{proxy}/openai/v1/responses"))
                .header("x-api-key", TEST_KEY.plaintext.as_str())
                .header("content-type", "application/json")
                .body(r#"{"model":"gpt-4.1","input":"concurrent admission control","max_output_tokens":100,"store":false}"#)
                .send()
                .await
                .expect("HTTP Responses control receives an admission response");
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            let body = response.bytes().await.expect("HTTP control rejection body");
            let value: serde_json::Value = serde_json::from_slice(&body)
                .expect("HTTP control rejection is JSON");
            assert!(value["error"].is_object());
        } else if gemini {
            let (mut second, response) =
                connect_async(realtime_request(proxy, native_websocket_path(true)))
                    .await
                    .expect("Live upgrades before model-bearing setup admission");
            assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
            second
                .send(UpstreamMessage::Text(
                    r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
                ))
                .await
                .unwrap();
            let message = tokio::time::timeout(Duration::from_secs(5), second.next())
                .await.unwrap().unwrap().unwrap();
            let value: serde_json::Value =
                serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_eq!(value["status"], 429);
            assert_eq!(value["error"]["type"], "rate_limit_error");
            assert_eq!(value["error"]["code"], "limit_rejected");
        } else {
            let error = connect_async(realtime_request(proxy, native_websocket_path(false)))
                .await
                .expect_err("second Realtime session is rejected before upgrade");
            let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
                panic!("OpenAI concurrent rejection must be HTTP");
            };
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            let value: serde_json::Value =
                serde_json::from_slice(response.body().as_ref().unwrap()).unwrap();
            assert_eq!(value["error"]["type"], "rate_limit_error");
            assert_eq!(value["error"]["code"], "limit_rejected");
        }
        let mut observed = Vec::new();
        let rejected_id = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = events.recv().await.unwrap();
                let rejected = matches!(
                    event,
                    LifecycleEvent::RequestTerminated { client_status: 429, .. }
                );
                let event_id = event.event_id().clone();
                observed.push(event);
                if rejected {
                    break event_id;
                }
            }
        })
        .await
        .expect("denied session terminates");
        assert!(
            listener.accept().now_or_never().is_none(),
            "principal denial must happen before a second provider TCP connection",
        );
        first.close(None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), provider)
            .await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = events.recv().await.unwrap();
                let first_terminated = matches!(
                    event,
                    LifecycleEvent::RequestTerminated { .. }
                ) && event.event_id() != &rejected_id;
                observed.push(event);
                if first_terminated {
                    break;
                }
            }
        }).await.expect("admitted session terminates after client close");
        observed.extend(std::iter::from_fn(|| events.try_recv().ok()));
        let denied: Vec<_> = observed.iter()
            .filter(|event| event.event_id() == &rejected_id)
            .collect();
        assert_eq!(denied.iter().filter(|event| matches!(
            event,
            LifecycleEvent::LimitDecision {
                decision: cc_lb_lifecycle::LimitDecisionKind::Rejected { .. }, ..
            }
        )).count(), 1, "one principal denial publishes exactly one rejected decision");
        assert_eq!(denied.iter().filter(|event| matches!(
            event, LifecycleEvent::RequestTerminated { .. }
        )).count(), 1, "one denied session produces exactly one terminal event");
        assert!(!denied.iter().any(|event| matches!(
            event, LifecycleEvent::UpstreamAttempt { .. }
        )), "rejected admission never attempts a provider handshake");
        audit_subscriber.shutdown().await;
        event_logger.shutdown().await;
        tokio::time::timeout(Duration::from_secs(5), audit_writer)
            .await.unwrap().unwrap();
        let audits = storage.query_recent_audit(
            cc_lb_storage_api::AuditQueryScope::All,
            0,
            cc_lb_clock::unix_secs(std::time::SystemTime::now()).saturating_add(1),
            10,
            false,
        ).await.unwrap();
        assert_eq!(audits.len(), 1, "one principal rejection produces one durable audit row");
        assert_eq!(audits[0].request_id, rejected_id);
        assert_eq!(audits[0].principal_id, TEST_PRINCIPAL);
        assert_eq!(audits[0].api_key_id.as_deref(), Some(TEST_KEY.key_id.as_str()));
        assert_eq!(audits[0].status, 429);
        assert_eq!(audits[0].limit_violation.as_deref(), Some("Concurrent"));
        let rendered = metrics_handle.render();
        for metric in ["cclb_limit_hits_total", "cclb_concurrent_rejects_total"] {
            let samples: Vec<_> = rendered.lines()
                .filter(|line| line.starts_with(&format!("{metric}{{")))
                .filter(|line| line.contains(&format!("key_id=\"{}\"", TEST_KEY.key_id)))
                .collect();
            assert_eq!(samples.len(), 1, "{metric} has one key-attributed sample");
            assert_eq!(samples[0].split_whitespace().last(), Some("1"), "{metric} increments once");
        }
        let counter_sum = |metric: &str, labels: &[&str]| -> u64 {
            rendered.lines()
                .filter(|line| line.starts_with(&format!("{metric}{{")))
                .filter(|line| labels.iter().all(|label| line.contains(label)))
                .map(|line| line.split_whitespace().last().unwrap().parse::<u64>().unwrap())
                .sum()
        };
        assert_eq!(counter_sum(
            "cc_lb_request_decisions_total",
            &["stage=\"limit\"", "outcome=\"rejected\""],
        ), 1, "one denied request records one rejected limit decision");
        assert_eq!(counter_sum(
            "cc_lb_lifecycle_limit_rejection_audit_events_total",
            &["outcome=\"enqueued\""],
        ), 1, "one denied request enqueues one audit entry");
        assert_eq!(counter_sum(
            "cc_lb_lifecycle_limit_rejection_audit_events_total",
            &["outcome=\"dropped\""],
        ), 0, "the rejection audit is never dropped");
        proxy_task.abort();
    }
}

struct NativeResponseHeaders;

impl tokio_tungstenite::tungstenite::handshake::server::Callback for NativeResponseHeaders {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        mut response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        let request_id_header = if request.uri().path().starts_with("/ws/") {
            assert_eq!(request.headers()["x-goog-api-key"], "provider-test-credential");
            "x-goog-request-id"
        } else {
            assert_eq!(request.headers()["authorization"], "Bearer provider-test-credential");
            "x-request-id"
        };
        response.headers_mut().insert(
            request_id_header,
            HeaderValue::from_static("native-provider-request-id"),
        );
        Ok(response)
    }
}

#[tokio::test]
async fn realtime_and_live_publish_active_cumulative_usage_then_preserve_provider_close() {
    use cc_lb_request_log::RequestEventUpdate;
    use cc_lb_storage_api::RequestEventStreamFilters;

    for gemini in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<UpstreamMessage>(1);
        let provider = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket =
                tokio_tungstenite::accept_hdr_async(stream, NativeResponseHeaders)
                    .await.unwrap();
            if gemini {
                assert_eq!(
                    socket.next().await.unwrap().unwrap().to_text().unwrap(),
                    r#"{"setup":{"model":"models/gemini-live"}}"#,
                );
            }
            while let Some(frame) = frame_rx.recv().await {
                let closing = frame.is_close();
                socket.send(frame).await.unwrap();
                if closing {
                    return;
                }
            }
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut updates = bus.subscribe();
        let mut lifecycle_events = bus.subscribe_lifecycle();
        let state = native_websocket_observed_state(address, gemini, bus.clone(), None).await;
        let storage = state.storage.clone();
        let assembler = cc_lb_engine::spawn_request_event_assembler(
            bus.attach_lifecycle_assembler(128),
            storage.clone(),
            Some(bus),
            Arc::new(cc_lb_observability::NoopMetricsHook),
        );
        let (proxy, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
        let (mut client, response) =
            connect_async(realtime_request(proxy, native_websocket_path(gemini)))
                .await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        if gemini {
            client.send(UpstreamMessage::Text(
                r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
            )).await.unwrap();
        }
        let event_id = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match updates.recv().await.unwrap() {
                    RequestEventUpdate::Partial(partial)
                        if partial.upstream_response_status == Some(101) => break partial.event_id,
                    RequestEventUpdate::Final(_) => panic!("session finalized before provider close"),
                    _ => {}
                }
            }
        }).await.expect("active dashboard sees the provider upgrade");
        let frames = if gemini {
            [
                r#"{"usageMetadata":{"promptTokenCount":10,"responseTokenCount":2,"thoughtsTokenCount":1,"cachedContentTokenCount":0,"toolUsePromptTokenCount":0}}"#,
                r#"{"usageMetadata":{"promptTokenCount":25,"responseTokenCount":5,"thoughtsTokenCount":2,"cachedContentTokenCount":5,"toolUsePromptTokenCount":9}}"#,
            ]
        } else {
            [
                r#"{"type":"response.done","response":{"id":"first","usage":{"input_tokens":10,"output_tokens":3}}}"#,
                r#"{"type":"response.done","response":{"id":"second","usage":{"input_tokens":10,"output_tokens":4}}}"#,
            ]
        };
        let mut observed_before_close = Vec::new();
        for (frame, input, output) in [(frames[0], 10, 3), (frames[1], 20, 7)] {
            frame_tx.send(UpstreamMessage::Text(frame.into())).await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), client.next())
                    .await.unwrap().unwrap().unwrap().to_text().unwrap(),
                frame,
            );
            if input == 10 {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        match updates.recv().await.unwrap() {
                            RequestEventUpdate::Partial(partial)
                                if partial.event_id == event_id
                                    && partial.input_tokens == Some(input)
                                    && partial.output_tokens == Some(output) => {
                                        assert_eq!(partial.upstream_response_status, Some(101));
                                        if gemini {
                                            assert_eq!(partial.tool_use_prompt_tokens, Some(0));
                                            assert_eq!(partial.thinking_tokens, Some(1));
                                        }
                                        break;
                                    }
                            RequestEventUpdate::Final(_) => panic!("usage must be visible before close"),
                            _ => {}
                        }
                    }
                }).await.expect("dashboard receives first usage while session remains open");
            } else {
                // Later dashboard partials are intentionally throttled using std::Instant.
                // The consumer lifecycle stream still exposes the newest cumulative snapshot.
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let event = lifecycle_events.recv().await.unwrap();
                        assert!(!matches!(event, LifecycleEvent::RequestTerminated { .. }));
                        let latest = matches!(
                            &event,
                            LifecycleEvent::UsageObserved { usage, .. }
                                if usage.input_tokens == input && usage.output_tokens == output
                        );
                        observed_before_close.push(event);
                        if latest {
                            break;
                        }
                    }
                }).await.expect("latest cumulative usage is published before provider close");
            }
        }
        if gemini {
            let completed = r#"{"serverContent":{"turnComplete":true}}"#;
            frame_tx.send(UpstreamMessage::Text(completed.into())).await.unwrap();
            assert_eq!(client.next().await.unwrap().unwrap().to_text().unwrap(), completed);
        }
        frame_tx.send(UpstreamMessage::Close(Some(
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away,
                reason: "provider maintenance".into(),
            },
        ))).await.unwrap();
        let UpstreamMessage::Close(Some(close)) =
            tokio::time::timeout(Duration::from_secs(5), client.next())
                .await.unwrap().unwrap().unwrap()
        else {
            panic!("native provider close frame must reach the client");
        };
        assert_eq!(u16::from(close.code), 1001);
        assert_eq!(close.reason.as_str(), "provider maintenance");
        let final_row = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let RequestEventUpdate::Final(final_update) = updates.recv().await.unwrap() {
                    assert_eq!(final_update.event.event_id.as_deref(), Some(event_id.as_str()));
                    break final_update.event;
                }
            }
        }).await.expect("closed session produces a durable final row");
        assert_eq!((final_row.input_tokens, final_row.output_tokens), (Some(20), Some(7)));
        assert_eq!(final_row.status, 101);
        let mut response_started = 0;
        let mut usage_snapshots = Vec::new();
        let mut stream_completed = 0;
        let mut request_terminated = 0;
        observed_before_close.extend(std::iter::from_fn(|| lifecycle_events.try_recv().ok()));
        for event in observed_before_close {
            assert_eq!(event.event_id(), &event_id);
            match event {
                LifecycleEvent::UpstreamResponseStarted { status, .. } => {
                    assert_eq!(status, 101);
                    response_started += 1;
                }
                LifecycleEvent::UsageObserved { usage, .. } => {
                    usage_snapshots.push((usage.input_tokens, usage.output_tokens));
                }
                LifecycleEvent::StreamCompleted { result, .. } => {
                    assert!(result.is_ok(), "completed turns retain successful stream outcome");
                    stream_completed += 1;
                }
                LifecycleEvent::RequestTerminated { client_status, .. } => {
                    assert_eq!(client_status, 101);
                    request_terminated += 1;
                }
                _ => {}
            }
        }
        assert_eq!(response_started, 1, "the close must not re-emit the handshake");
        assert!(usage_snapshots.contains(&(10, 3)));
        assert_eq!(usage_snapshots.last(), Some(&(20, 7)));
        assert_eq!(stream_completed, 1, "one provider close completes the stream once");
        assert_eq!(request_terminated, 1, "one provider close terminates the request once");
        assembler.shutdown().await;
        let cursor = storage.current_request_event_cursor().await.unwrap();
        let rows = storage.query_request_events_between_cursors(
            0, cursor, 10, &RequestEventStreamFilters::default(),
        ).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.event_id.as_deref(), Some(event_id.as_str()));
        assert_eq!((rows[0].1.input_tokens, rows[0].1.output_tokens), (Some(20), Some(7)));
        let stored = &rows[0].1;
        assert_eq!(stored.status, 101);
        if gemini {
            assert_eq!(stored.tool_use_prompt_tokens, Some(9));
            assert_eq!(stored.thinking_tokens, Some(2));
            assert_eq!(stored.cache_read_input_tokens, Some(5));
        }
        assert_eq!(stored.upstream_request_id.as_deref(), Some("native-provider-request-id"));
        assert_eq!(stored.upstream_stream_warning_type.as_deref(), Some("websocket_close"));
        assert_native_close_details(
            stored.upstream_stream_warning_message.as_deref()
                .expect("successful native close details are durable"),
            1001,
            "provider maintenance",
        );
        assert_eq!(stored.upstream_error_message, None, "completed turns do not become stream errors");
        assert_eq!(stored.upstream_body_error_h2_reason, None);
        assert_eq!(stored.upstream_http_version, None);
        assert_eq!(stored.upstream_body_error_io_kind, None);
        tokio::time::timeout(Duration::from_secs(5), provider)
            .await.unwrap().unwrap();
        proxy_task.abort();
    }
}
#[tokio::test]
async fn realtime_and_live_normal_downstream_close_after_completion_is_success() {
    use cc_lb_request_log::RequestEventUpdate;
    use cc_lb_storage_api::RequestEventStreamFilters;

    for gemini in [false, true] {
        let expected_output_tokens = if gemini { 4 } else { 3 };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket =
                tokio_tungstenite::accept_hdr_async(stream, NativeResponseHeaders)
                    .await.unwrap();
            if gemini {
                assert_eq!(
                    socket.next().await.unwrap().unwrap().to_text().unwrap(),
                    r#"{"setup":{"model":"models/gemini-live"}}"#,
                );
            }
            let completed = if gemini {
                r#"{"serverContent":{"modelTurn":{"parts":[{"text":"done"}]},"turnComplete":true},"usageMetadata":{"promptTokenCount":10,"responseTokenCount":3,"thoughtsTokenCount":1,"cachedContentTokenCount":0,"toolUsePromptTokenCount":0}}"#
            } else {
                r#"{"type":"response.done","response":{"id":"completed","usage":{"input_tokens":10,"output_tokens":3}}}"#
            };
            socket.send(UpstreamMessage::Text(completed.into())).await.unwrap();
            let UpstreamMessage::Close(Some(close)) =
                tokio::time::timeout(Duration::from_secs(5), socket.next())
                    .await.unwrap().unwrap().unwrap()
            else {
                panic!("normal downstream close must reach the provider");
            };
            assert_eq!(u16::from(close.code), 1000);
            assert_eq!(close.reason.as_str(), "client finished");
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut updates = bus.subscribe();
        let mut lifecycle_events = bus.subscribe_lifecycle();
        let state = native_websocket_observed_state(address, gemini, bus.clone(), Some(1)).await;
        let storage = state.storage.clone();
        let assembler = cc_lb_engine::spawn_request_event_assembler(
            bus.attach_lifecycle_assembler(128),
            storage.clone(),
            Some(bus),
            Arc::new(cc_lb_observability::NoopMetricsHook),
        );
        let (proxy, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
        let (mut client, response) =
            connect_async(realtime_request(proxy, native_websocket_path(gemini)))
                .await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        if gemini {
            client.send(UpstreamMessage::Text(
                r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
            )).await.unwrap();
        }
        let event_id = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match updates.recv().await.unwrap() {
                    RequestEventUpdate::Partial(partial)
                        if partial.upstream_response_status == Some(101) => break partial.event_id,
                    RequestEventUpdate::Final(_) => panic!("session finalized before completion"),
                    _ => {}
                }
            }
        }).await.expect("active dashboard sees the provider upgrade");
        let completed = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await.unwrap().unwrap().unwrap();
        assert!(completed.is_text());
        client.send(UpstreamMessage::Close(Some(
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: "client finished".into(),
            },
        ))).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), provider)
            .await.unwrap().unwrap();
        let final_row = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let RequestEventUpdate::Final(update) = updates.recv().await.unwrap() {
                    assert_eq!(update.event.event_id.as_deref(), Some(event_id.as_str()));
                    break update.event;
                }
            }
        }).await.expect("normal downstream close produces a durable final row");
        assert_eq!(final_row.status, 101);
        assert_eq!(final_row.error_code, None);
        assert_eq!(final_row.upstream_error_message, None);
        assert_eq!(
            (final_row.input_tokens, final_row.output_tokens),
            (Some(10), Some(expected_output_tokens)),
        );
        let mut reserved = 0;
        let mut stream_completed = 0;
        let mut request_terminated = 0;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = lifecycle_events.recv().await.unwrap();
                assert_eq!(event.event_id(), &event_id);
                match event {
                    LifecycleEvent::LimitDecision {
                        decision: cc_lb_lifecycle::LimitDecisionKind::Reserved { .. }, ..
                    } => reserved += 1,
                    LifecycleEvent::UpstreamStreamDiagnosticsObserved { diagnostics, .. } => {
                        assert_eq!(diagnostics.warning_type.as_deref(), Some("websocket_close"));
                        assert_native_close_details(
                            diagnostics.warning_message.as_deref().unwrap(),
                            1000,
                            "client finished",
                        );
                    }
                    LifecycleEvent::StreamCompleted { result, .. } => {
                        assert!(result.is_ok(), "completed downstream close is successful");
                        stream_completed += 1;
                    }
                    LifecycleEvent::RequestTerminated { reason, client_status, .. } => {
                        assert_eq!(reason, cc_lb_lifecycle::TerminationReason::Success);
                        assert_eq!(client_status, 101);
                        request_terminated += 1;
                        break;
                    }
                    _ => {}
                }
            }
        }).await.expect("successful lifecycle termination arrives");
        assert_eq!(reserved, 1, "one reservation is handed to the response lifecycle");
        assert_eq!(stream_completed, 1);
        assert_eq!(request_terminated, 1);
        assembler.shutdown().await;
        let cursor = storage.current_request_event_cursor().await.unwrap();
        let rows = storage.query_request_events_between_cursors(
            0, cursor, 10, &RequestEventStreamFilters::default(),
        ).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.event_id.as_deref(), Some(event_id.as_str()));
        assert_eq!(rows[0].1.status, 101);
        assert_eq!(rows[0].1.error_code, None);
        assert_eq!(
            (rows[0].1.input_tokens, rows[0].1.output_tokens),
            (Some(10), Some(expected_output_tokens)),
        );
        assert_native_close_details(
            rows[0].1.upstream_stream_warning_message.as_deref().unwrap(),
            1000,
            "client finished",
        );
        proxy_task.abort();
    }
}

#[tokio::test]
async fn realtime_and_live_persist_native_close_details_and_request_id() {
    use cc_lb_request_log::RequestEventUpdate;
    use cc_lb_storage_api::RequestEventStreamFilters;

    for gemini in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket =
                tokio_tungstenite::accept_hdr_async(stream, NativeResponseHeaders)
                    .await.unwrap();
            if gemini {
                assert_eq!(
                    socket.next().await.unwrap().unwrap().to_text().unwrap(),
                    r#"{"setup":{"model":"models/gemini-live"}}"#,
                );
            }
            socket.send(UpstreamMessage::Close(Some(
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy,
                    reason: "provider policy changed".into(),
                },
            ))).await.unwrap();
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut updates = bus.subscribe();
        let mut events = bus.subscribe_lifecycle();
        let state = native_websocket_observed_state(address, gemini, bus.clone(), None).await;
        let storage = state.storage.clone();
        let assembler = cc_lb_engine::spawn_request_event_assembler(
            bus.attach_lifecycle_assembler(128),
            storage.clone(),
            Some(bus),
            Arc::new(cc_lb_observability::NoopMetricsHook),
        );
        let (proxy, proxy_task) = spawn_proxy_router(proxy_router(state, 30)).await;
        let (mut client, response) =
            connect_async(realtime_request(proxy, native_websocket_path(gemini)))
                .await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        if gemini {
            client.send(UpstreamMessage::Text(
                r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
            )).await.unwrap();
        }
        let UpstreamMessage::Close(Some(close)) =
            tokio::time::timeout(Duration::from_secs(5), client.next())
                .await.unwrap().unwrap().unwrap()
        else {
            panic!("provider policy close must reach the client unchanged");
        };
        assert_eq!(u16::from(close.code), 1008);
        assert_eq!(close.reason.as_str(), "provider policy changed");
        let row = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let RequestEventUpdate::Final(update) = updates.recv().await.unwrap() {
                    break update.event;
                }
            }
        }).await.expect("provider close finalizes the request");
        let recorded = row.upstream_error_message.as_deref()
            .expect("provider close details reach the final request row");
        assert_native_close_details(recorded, 1008, "provider policy changed");
        assert_eq!(row.status, 101, "upgraded sessions never invent an HTTP failure status");
        let mut request_terminated = 0;
        let mut errors = Vec::new();
        while let Ok(event) = events.try_recv() {
            assert_eq!(Some(event.event_id().as_str()), row.event_id.as_deref());
            match event {
                LifecycleEvent::StreamCompleted { result, .. } => {
                    errors.push(result.expect_err("policy close before completion remains a stream error"));
                }
                LifecycleEvent::RequestTerminated { client_status, .. } => {
                    assert_eq!(client_status, 101);
                    request_terminated += 1;
                }
                _ => {}
            }
        }
        assert_eq!(errors.len(), 1);
        assert_native_close_details(&errors[0].error_message, 1008, "provider policy changed");
        assert_eq!(recorded, errors[0].error_message, "the row preserves the observed stream error");
        assert_eq!(request_terminated, 1, "one policy close terminates the request once");
        assembler.shutdown().await;
        let cursor = storage.current_request_event_cursor().await.unwrap();
        let rows = storage.query_request_events_between_cursors(
            0, cursor, 10, &RequestEventStreamFilters::default(),
        ).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.event_id, row.event_id);
        assert_eq!(rows[0].1.upstream_error_message.as_deref(), Some(recorded));
        assert_eq!(rows[0].1.upstream_request_id.as_deref(), Some("native-provider-request-id"));
        assert_eq!(rows[0].1.upstream_http_version, None);
        assert_eq!(rows[0].1.upstream_body_error_io_kind, None);
        assert_eq!(rows[0].1.upstream_body_error_h2_reason, None);
        tokio::time::timeout(Duration::from_secs(5), provider)
            .await.unwrap().unwrap();
        proxy_task.abort();
    }
}

fn assert_native_close_details(message: &str, code: u16, reason: &str) {
    assert!(message.contains(&code.to_string()), "close code must remain observable");
    assert!(message.contains(reason), "close reason must remain observable");
    assert!(
        message.len() <= cc_lb_observability::ROUTING_REASON_MAX_BYTES,
        "close details must be bounded",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn realtime_and_live_handshakes_propagate_child_trace_without_replacing_signer_credentials() {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::prelude::*;
    use tracing::instrument::WithSubscriber as _;

    // Two live dispatchers make tracing-core register callsites against every subscriber,
    // rather than caching `never` from an untraced sibling's thread-local dispatcher.
    let _second_dispatcher = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());

    const TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
    const PARENT_ID: &str = "b7ad6b7169203331";
    for gemini in [false, true] {
        let exporter = InMemorySpanExporter::default();
        let tracer_provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer_provider.tracer("native-handshake-test")),
        );
        let dispatch = tracing::Dispatch::new(subscriber);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (capture_tx, capture_rx) = tokio::sync::oneshot::channel();
        let provider = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                stream,
                HandshakeCapture { sender: Some(capture_tx) },
            ).await.unwrap();
            if gemini {
                assert_eq!(
                    socket.next().await.unwrap().unwrap().to_text().unwrap(),
                    r#"{"setup":{"model":"models/gemini-live"}}"#,
                );
            }
            socket.send(provider_ready_message()).await.unwrap();
            while socket.next().await.is_some() {}
        });
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let state = native_websocket_observed_state(address, gemini, bus, None).await;
        let drain = state.drain_controller.clone();
        let router = proxy_router(state, 30).layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| {
                async move {
                    next.run(request).await
                }.with_subscriber(dispatch.clone())
            },
        ));
        let (proxy, proxy_task) = spawn_proxy_router(router).await;
        let mut request = realtime_request(proxy, native_websocket_path(gemini));
        request.headers_mut().insert(
            "traceparent",
            HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        );
        request.headers_mut().insert("tracestate", HeaderValue::from_static("vendor=value"));
        let (mut client, _) = connect_async(request).await.unwrap();
        if gemini {
            client.send(UpstreamMessage::Text(
                r#"{"setup":{"model":"models/gemini-live"}}"#.into(),
            )).await.unwrap();
        }
        let (_, headers) = tokio::time::timeout(Duration::from_secs(5), capture_rx)
            .await.unwrap().unwrap();
        if gemini {
            assert_eq!(headers["x-goog-api-key"], "provider-test-credential");
            assert!(!headers.contains_key("authorization"));
        } else {
            assert_eq!(headers["authorization"], "Bearer provider-test-credential");
        }
        let traceparent = headers["traceparent"].to_str().unwrap().to_owned();
        let parts: Vec<_> = traceparent.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1], TRACE_ID, "the provider remains on the caller's trace");
        assert_ne!(parts[2], PARENT_ID, "the handshake injects a local child, not the caller header");
        assert_ne!(parts[2], "0000000000000000");
        assert_eq!(parts[3], "01");
        assert_eq!(headers["tracestate"], "vendor=value", "the proxy child retains incoming trace state");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), client.next())
                .await.unwrap().unwrap().unwrap(),
            provider_ready_message(),
        );
        client.close(None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), provider)
            .await.unwrap().unwrap();
        assert!(!drain.await_drained(Duration::from_secs(5)).await);
        tracer_provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let injected = spans.iter().find(|span|
            span.span_context.span_id().to_string() == parts[2]
        ).expect("provider traceparent identifies an actual exported local span");
        assert_eq!(injected.name.as_ref(), "proxy.realtime_stream");
        assert_eq!(injected.span_context.trace_id().to_string(), TRACE_ID);
        assert_eq!(injected.span_context.trace_state().get("vendor"), Some("value"));
        assert_ne!(injected.parent_span_id.to_string(), "0000000000000000");
        proxy_task.abort();
        tracer_provider.shutdown().unwrap();
    }
}
