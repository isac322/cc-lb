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
    for path in ["/openai/v1/realtime?model=gpt-realtime", "/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("provider listener");
        let address = listener.local_addr().unwrap();
        let mut record = websocket_upstream(1, "provider", Url::parse(&format!("http://{address}/")).unwrap());
        if path.starts_with("/gemini") { record.kind = UpstreamKind::GeminiApiKey; }
        let router = websocket_test_router(vec![record], TerminalStrategy::FirstPick).await;
        let (proxy, task) = spawn_proxy_router(router).await;
        let mut request = realtime_request(proxy, path);
        request.headers_mut().remove("x-api-key");
        let error = tokio::time::timeout(Duration::from_secs(5), connect_async(request)).await.unwrap().unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else { panic!("HTTP auth rejection expected") };
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(listener.accept().now_or_never().is_none(), "unauthenticated caller never connects to provider");
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
