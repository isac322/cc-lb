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

#[tokio::test]
async fn websocket_handshake_candidate_loop_caps_attempts_at_three_and_skips_open_breakers() {
    let mut failing_tasks = Vec::new();
    let mut failing_requests = Vec::new();
    let mut upstreams = Vec::new();
    for idx in 1..=5_u128 {
        let (url, reqs, task) = spawn_http_error_provider(
            500,
            r#"{"error":{"code":"provider_server_error","message":"provider-body-secret"}}"#
                .to_owned(),
        )
        .await;
        failing_requests.push(reqs);
        failing_tasks.push(task);
        upstreams.push(websocket_upstream(
            idx,
            &format!("failing-{idx}"),
            url,
        ));
    }

    let router = websocket_test_router(upstreams.clone(), TerminalStrategy::FirstPick).await;
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
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
    assert_eq!(error["error"]["code"], "upstream_unavailable");

    let counts: Vec<usize> = failing_requests
        .iter()
        .map(|reqs| reqs.load(Ordering::SeqCst))
        .collect();
    assert_eq!(
        counts,
        vec![1, 1, 1, 0, 0],
        "single dispatch must attempt at most 3 distinct upstreams"
    );

    let _ = client.close(None).await;
    proxy_task.abort();
    for task in failing_tasks {
        task.abort();
    }

    // Now test open-breaker skipping: trip local breakers on upstreams 1..=3, keep 4th healthy.
    let (healthy_url, healthy_requests, healthy_task) = spawn_responses_success_provider().await;
    let (dead_url, dead_requests, dead_task) = spawn_http_error_provider(
        500,
        r#"{"error":{"code":"provider_server_error","message":"provider-body-secret"}}"#
            .to_owned(),
    )
    .await;
    let state = websocket_test_state(
        vec![
            websocket_upstream(101, "tripped-1", dead_url.clone()),
            websocket_upstream(102, "tripped-2", dead_url.clone()),
            websocket_upstream(103, "tripped-3", dead_url),
            websocket_upstream(104, "healthy-4", healthy_url),
        ],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    for id in [101_u128, 102, 103] {
        let breaker = state.breaker_registry.breaker(
            responses_websocket::websocket_breaker_key(Uuid::from_u128(id)),
            state.breaker_config,
            state.clock.clone(),
        );
        for _ in 0..state.breaker_config.failures_to_open {
            if let Ok(permit) = breaker.permit() {
                permit.record_failure();
            }
        }
    }
    let router = proxy_router(state, 30);
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let (mut client, _) = connect_async(websocket_request(proxy_address))
        .await
        .expect("downstream handshake succeeds");
    client
        .send(UpstreamMessage::Text(RESPONSE_CREATE.into()))
        .await
        .expect("response.create sends");
    let message = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("healthy 4th candidate responds after skipping open breakers")
        .expect("downstream stays open")
        .expect("response decodes");
    assert_eq!(
        message,
        UpstreamMessage::Text(
            r#"{"type":"response.created","response":{"id":"healthy","store":false}}"#.into(),
        )
    );
    assert_eq!(
        dead_requests.load(Ordering::SeqCst),
        0,
        "open-breaker candidates must be skipped without contacting upstream"
    );
    assert_eq!(healthy_requests.load(Ordering::SeqCst), 1);

    let _ = client.close(None).await;
    proxy_task.abort();
    dead_task.abort();
    healthy_task.abort();
}

#[tokio::test]
async fn websocket_handshake_candidate_loop_enforces_total_connect_budget() {
    let mut listeners = Vec::new();
    let mut upstreams = Vec::new();
    let accepted = Arc::new(AtomicUsize::new(0));
    let mut accept_tasks = Vec::new();
    for idx in 1..=3_u128 {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("hanging listener binds");
        let addr = listener.local_addr().expect("addr");
        let accepted_clone = accepted.clone();
        let (tx_hold, rx_hold) = tokio::sync::oneshot::channel::<()>();
        listeners.push(tx_hold);
        accept_tasks.push(tokio::spawn(async move {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            accepted_clone.fetch_add(1, Ordering::SeqCst);
            let _ = rx_hold.await;
            drop(socket);
        }));
        upstreams.push(websocket_upstream(
            idx,
            &format!("hanging-{idx}"),
            Url::parse(&format!("http://{addr}/")).expect("url"),
        ));
    }

    let state = websocket_test_state(
        upstreams,
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let headers = {
        let mut h = HeaderMap::new();
        h.insert(
            "x-api-key",
            HeaderValue::from_str(TEST_KEY.plaintext.as_str())
                .expect("fixture key is a valid header value"),
        );
        h
    };
    let auth = state
        .lifecycle
        .authenticate(&headers)
        .await
        .expect("fixture key authenticates");
    let mut gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
        state.lifecycle.clone(),
        &auth,
        headers,
        state.body_caps.messages,
        8,
    );
    let dispatch = gateway
        .client_message(auth, Bytes::from_static(RESPONSE_CREATE.as_bytes()))
        .await
        .expect("client message admits")
        .expect("dispatch returned");
    assert_eq!(dispatch.candidates.len(), 3);

    let (events, _event_rx) = tokio::sync::mpsc::channel(8);
    let mut connections = HashMap::new();
    let mut next_connection_id = 1;
    let started = std::time::Instant::now();
    let (_ready, errors) = responses_websocket::dispatch_responses_websocket_with_limits(
        &mut gateway,
        dispatch,
        &state,
        &mut connections,
        &events,
        &mut next_connection_id,
        8,
        Duration::from_millis(250),
        Duration::from_millis(450),
        3,
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].status, 504);
    assert_eq!(errors[0].code, "upstream_unavailable");
    assert!(
        elapsed < Duration::from_secs(3),
        "total connect budget must bound aggregate latency, took {elapsed:?}"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        2,
        "third candidate must not be attempted once total connect budget expires"
    );

    drop(listeners);
    for task in accept_tasks {
        task.abort();
    }
}

async fn subscription_websocket_state(
    records: Vec<UpstreamRecord>,
    quota_cache: Arc<crate::subscription_quota_cache::SubscriptionQuotaCache>,
) -> ProxyState {
    let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
    let sqlite = Arc::new(
        cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
            .await
            .expect("in-memory storage opens"),
    );
    let storage: Arc<dyn Storage> = sqlite;
    let builtin_authn = Arc::new(BuiltinAuthn::new(
        Arc::new(KeyStore::new(Arc::new(FixtureKeyStore))),
        clock.clone(),
    ));
    let filter: Arc<dyn cc_lb_routing::FilterPlugin> = Arc::new(
        cc_lb_routing_eval::builtin_filters::subscription_preference::SubscriptionPreferenceFilter::new(
        ),
    );
    let principal_view = Arc::new(PrincipalView::for_tests(
        TEST_PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::from([(
            TEST_PRINCIPAL.to_owned(),
            (
                Some(Arc::new(RouterPipelineCache {
                    user_filters: vec![filter],
                    terminal: TerminalStrategy::FirstPick,
                    instantiation_error: None,
                    provider_filters: None,
                })),
                DialectCache::Inherit,
            ),
        )]),
    ));
    let dynamic_view = Arc::new(DynamicViewHolder::new(
        cc_lb_control::dynamic_view::DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(WebSocketTestSignerFactory))
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache)
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(records)
            .build(),
    ));
    let lifecycle = Arc::new(Lifecycle::new_with_dynamic_view(
        builtin_authn.clone(),
        dynamic_view.clone(),
        make_default_dispatcher(50),
        LifecycleConfig::default(),
        clock.clone(),
    ));
    ProxyState {
        lifecycle,
        body_caps: RequestBodyCaps {
            messages: 1024 * 1024,
            files: 1024 * 1024,
        },
        server_state: Arc::new(ServerStateHandle::new_starting()),
        start_time: Instant::now(),
        drain_controller: DrainController::new(),
        aead: Arc::new(AeadService::from_master_key([0; 32])),
        storage,
        dynamic_view,
        builtin_authn,
        clock,
        breaker_registry: Arc::new(BreakerRegistry::new()),
        breaker_config: BreakerRuntimeConfig::default(),
        websocket_bulkheads: Arc::new(BulkheadRegistry::new()),
        websocket_bulkhead_config: BulkheadRuntimeConfig {
            max_conns_per_upstream: 50,
            semaphore_permits: 100,
            acquire_timeout: Duration::from_secs(1),
        },
    }
}

fn seed_subscription_quota(
    quota_cache: &crate::subscription_quota_cache::SubscriptionQuotaCache,
    upstream_id: Uuid,
    utilization_5h: f64,
    utilization_7d: f64,
    exhausted: bool,
) {
    use cc_lb_storage_api::{
        SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
        SubscriptionQuotaStatus, SubscriptionQuotaWindow,
    };
    let now_secs = cc_lb_clock::unix_secs(std::time::SystemTime::now());
    let now_ms = now_secs * 1_000;
    for (window, utilization, reset_offset) in [
        (SubscriptionQuotaWindow::FiveHour, utilization_5h, 1_800),
        (SubscriptionQuotaWindow::SevenDay, utilization_7d, 86_400),
    ] {
        quota_cache.upsert_observation(
            upstream_id,
            &SubscriptionQuotaSample {
                upstream_id,
                window,
                source: SubscriptionQuotaSource::Header,
                sample_kind: SubscriptionQuotaSampleKind::Sample,
                observed_at_unix_millis: now_ms,
                sample_id: Uuid::new_v4(),
                utilization: Some(utilization),
                status: Some(if exhausted && window == SubscriptionQuotaWindow::SevenDay {
                    SubscriptionQuotaStatus::Rejected
                } else {
                    SubscriptionQuotaStatus::Allowed
                }),
                resets_at_unix_secs: Some(now_secs + reset_offset),
                surpassed_threshold: None,
                representative_claim: None,
                fallback_percentage: None,
                fallback_available: None,
                overage_in_use: None,
                overage_period_monthly_utilization: None,
                upgrade_paths: None,
                disabled_reason: None,
                extra_usage_enabled: None,
                extra_usage_monthly_limit: None,
                extra_usage_used_credits: None,
                credits: None,
                ingested_at_unix_millis: now_ms,
            },
        );
    }
}

#[tokio::test]
async fn websocket_handshake_public_reserves_final_attempt_for_api_key_and_codex_stays_oauth_only()
 {
    let mut failing_tasks = Vec::new();
    let mut failing_requests = Vec::new();
    let mut records = Vec::new();
    let quota_cache = Arc::new(crate::subscription_quota_cache::SubscriptionQuotaCache::new());

    for (idx, util) in [(1_u128, 0.05), (2, 0.10), (3, 0.20), (4, 0.30)] {
        let (url, reqs, task) = spawn_http_error_provider(
            429,
            r#"{"error":{"code":"rate_limit_exceeded","message":"rate limited"}}"#.to_owned(),
        )
        .await;
        let mut up = websocket_upstream(idx, &format!("oauth-{idx}"), url);
        up.kind = UpstreamKind::OpenaiOauth;
        seed_subscription_quota(&quota_cache, up.id, util, util, false);
        failing_requests.push(reqs);
        failing_tasks.push(task);
        records.push(up);
    }
    let (healthy_url, healthy_requests, healthy_task) = spawn_responses_success_provider().await;
    let api_key = websocket_upstream(10, "openai-api-key", healthy_url);
    records.push(api_key);

    // 1. Public `/v1/responses`: open breaker on oauth-1 is skipped; oauth-2 and oauth-3 fail 429;
    //    attempt 3 is reserved for the API key instead of trying oauth-4.
    let state = subscription_websocket_state(records.clone(), quota_cache.clone()).await;
    let breaker = state.breaker_registry.breaker(
        responses_websocket::websocket_breaker_key(Uuid::from_u128(1)),
        state.breaker_config,
        state.clock.clone(),
    );
    for _ in 0..state.breaker_config.failures_to_open {
        if let Ok(permit) = breaker.permit() {
            permit.record_failure();
        }
    }
    let router = proxy_router(state, 30);
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let mut req = websocket_request(proxy_address);
    *req.uri_mut() = format!("ws://{proxy_address}/openai/v1/responses")
        .parse()
        .expect("public WS URI parses");
    let (mut client, _) = connect_async(req).await.expect("public WS connects");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("create sends");
    let message = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("public API-key fallback responds")
        .expect("stream stays open")
        .expect("response decodes");
    let created: Value =
        serde_json::from_str(message.to_text().expect("text frame")).expect("json");
    assert_eq!(created["response"]["id"], "healthy");
    assert_eq!(failing_requests[0].load(Ordering::SeqCst), 0);
    assert_eq!(failing_requests[1].load(Ordering::SeqCst), 1);
    assert_eq!(failing_requests[2].load(Ordering::SeqCst), 1);
    assert_eq!(failing_requests[3].load(Ordering::SeqCst), 0);
    assert_eq!(healthy_requests.load(Ordering::SeqCst), 1);
    let _ = client.close(None).await;
    proxy_task.abort();

    // 2. Native Codex `/openai/backend-api/codex/responses` stays OAuth-only.
    let codex_state = subscription_websocket_state(records, quota_cache).await;
    let codex_router = proxy_router(codex_state, 30);
    let (codex_proxy, codex_task) = spawn_proxy_router(codex_router).await;
    let mut codex_req = websocket_request(codex_proxy);
    *codex_req.uri_mut() = format!("ws://{codex_proxy}/openai/backend-api/codex/responses")
        .parse()
        .expect("codex WS URI parses");
    let (mut codex_client, _) = connect_async(codex_req).await.expect("codex WS connects");
    codex_client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("codex create sends");
    let err = receive_error(&mut codex_client).await;
    assert_eq!(err["status"], 429);
    assert_eq!(
        healthy_requests.load(Ordering::SeqCst),
        1,
        "native Codex must never fall back to API-key upstream"
    );
    let _ = codex_client.close(None).await;
    codex_task.abort();

    for task in failing_tasks {
        task.abort();
    }
    healthy_task.abort();
}

#[tokio::test]
async fn websocket_handshake_fallback_skips_freshly_exhausted_oauth_on_connect_and_reuse() {
    // Part A: New-connect fallback on native Codex skips an OAuth candidate whose quota becomes
    // exhausted after turn admission without burning an attempt slot.
    {
        let quota_cache = Arc::new(crate::subscription_quota_cache::SubscriptionQuotaCache::new());

        let accepted_first = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let accepted_first_task = accepted_first.clone();
        let release_first_task = release_first.clone();
        let reqs1 = Arc::new(AtomicUsize::new(0));
        let reqs1_task = reqs1.clone();
        let task1 = tokio::spawn(async move {
            while let Ok((socket, _)) = listener1.accept().await {
                reqs1_task.fetch_add(1, Ordering::SeqCst);
                accepted_first_task.notify_one();
                release_first_task.notified().await;
                drop(socket);
            }
        });

        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();
        let reqs2 = Arc::new(AtomicUsize::new(0));
        let reqs2_task = reqs2.clone();
        let task2 = tokio::spawn(async move {
            while let Ok((socket, _)) = listener2.accept().await {
                reqs2_task.fetch_add(1, Ordering::SeqCst);
                drop(socket);
            }
        });

        let (url3, reqs3, task3) = spawn_responses_success_provider().await;

        let mut up1 =
            websocket_upstream(1, "oauth-1", Url::parse(&format!("http://{addr1}/")).unwrap());
        up1.kind = UpstreamKind::OpenaiOauth;
        let mut up2 =
            websocket_upstream(2, "oauth-2", Url::parse(&format!("http://{addr2}/")).unwrap());
        up2.kind = UpstreamKind::OpenaiOauth;
        let mut up3 = websocket_upstream(3, "oauth-3", url3);
        up3.kind = UpstreamKind::OpenaiOauth;

        seed_subscription_quota(&quota_cache, up1.id, 0.05, 0.05, false);
        seed_subscription_quota(&quota_cache, up2.id, 0.10, 0.10, false);
        seed_subscription_quota(&quota_cache, up3.id, 0.30, 0.30, false);

        let state = subscription_websocket_state(vec![up1, up2.clone(), up3], quota_cache.clone()).await;
        let headers = {
            let mut h = HeaderMap::new();
            h.insert("x-api-key", HeaderValue::from_str(TEST_KEY.plaintext.as_str()).unwrap());
            h
        };
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            8,
        )
        .with_codex_backend(true);

        let d1 = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-6.2-sol","stream_id":"main","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d1.candidates.len(), 3);

        let quota_clone = quota_cache.clone();
        let up2_id = up2.id;
        let coordinator = tokio::spawn(async move {
            accepted_first.notified().await;
            seed_subscription_quota(&quota_clone, up2_id, 0.0, 1.0, true);
            release_first.notify_one();
        });

        let (events, _event_rx) = tokio::sync::mpsc::channel(16);
        let mut connections = HashMap::new();
        let mut next_conn_id = 1;
        let (_ready, errs) = responses_websocket::dispatch_responses_websocket_with_limits(
            &mut gateway,
            d1,
            &state,
            &mut connections,
            &events,
            &mut next_conn_id,
            8,
            Duration::from_secs(5),
            Duration::from_secs(5),
            2,
        )
        .await;
        coordinator.await.unwrap();
        assert!(
            errs.is_empty(),
            "fallback must skip freshly exhausted oauth-2 and succeed on oauth-3 within max_attempts=2: {errs:?}"
        );
        assert_eq!(reqs1.load(Ordering::SeqCst), 1);
        assert_eq!(
            reqs2.load(Ordering::SeqCst),
            0,
            "freshly exhausted oauth-2 fallback must never be contacted"
        );
        assert_eq!(reqs3.load(Ordering::SeqCst), 1);

        task1.abort();
        task2.abort();
        task3.abort();
    }

    // Part B: Reused-socket fallback on public WS revalidates eligibility before reusing an open
    // socket and skips to the next candidate when that upstream becomes ineligible mid-flight.
    {
        let quota_cache = Arc::new(crate::subscription_quota_cache::SubscriptionQuotaCache::new());

        let accepted_b = Arc::new(tokio::sync::Notify::new());
        let release_b = Arc::new(tokio::sync::Notify::new());
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let accepted_b_task = accepted_b.clone();
        let release_b_task = release_b.clone();
        let reqs1 = Arc::new(AtomicUsize::new(0));
        let reqs1_task = reqs1.clone();
        let task1 = tokio::spawn(async move {
            while let Ok((socket, _)) = listener1.accept().await {
                reqs1_task.fetch_add(1, Ordering::SeqCst);
                accepted_b_task.notify_one();
                release_b_task.notified().await;
                drop(socket);
            }
        });

        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();
        let creates2 = Arc::new(AtomicUsize::new(0));
        let creates2_task = creates2.clone();
        let task2 = tokio::spawn(async move {
            let (socket, _) = listener2.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if msg.is_text() {
                    creates2_task.fetch_add(1, Ordering::SeqCst);
                    ws.send(UpstreamMessage::Text(
                        r#"{"type":"response.created","response":{"id":"api2-turn1","store":false}}"#
                            .into(),
                    ))
                    .await
                    .unwrap();
                    ws.send(UpstreamMessage::Text(
                        r#"{"type":"response.completed","response":{"id":"api2-turn1","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#
                            .into(),
                    ))
                    .await
                    .unwrap();
                }
            }
        });

        let (url3, reqs3, task3) = spawn_responses_success_provider().await;

        let mut up1 =
            websocket_upstream(1, "oauth-1", Url::parse(&format!("http://{addr1}/")).unwrap());
        up1.kind = UpstreamKind::OpenaiOauth;
        up1.enabled = false;
        let mut up2 = websocket_upstream(
            2,
            "api-key-2",
            Url::parse(&format!("http://{addr2}/")).unwrap(),
        );
        up2.kind = UpstreamKind::OpenaiApiKey;
        let mut up3 = websocket_upstream(3, "api-key-3", url3);
        up3.kind = UpstreamKind::OpenaiApiKey;
        up3.enabled = false;

        seed_subscription_quota(&quota_cache, up1.id, 0.05, 0.05, false);

        let state = subscription_websocket_state(
            vec![up1.clone(), up2.clone(), up3.clone()],
            quota_cache,
        )
        .await;
        let headers = {
            let mut h = HeaderMap::new();
            h.insert("x-api-key", HeaderValue::from_str(TEST_KEY.plaintext.as_str()).unwrap());
            h
        };
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            8,
        );

        let (events, mut event_rx) = tokio::sync::mpsc::channel(16);
        let mut connections = HashMap::new();
        let mut next_conn_id = 1;

        let d1 = gateway
            .client_message(
                auth.clone(),
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let (ready1, errs1) = responses_websocket::dispatch_responses_websocket_with_limits(
            &mut gateway,
            d1,
            &state,
            &mut connections,
            &events,
            &mut next_conn_id,
            8,
            Duration::from_secs(5),
            Duration::from_secs(5),
            2,
        )
        .await;
        assert!(errs1.is_empty() && ready1.is_empty());
        while let Some(ev) = event_rx.recv().await {
            if let responses_websocket::WsProviderEvent::Frame {
                connection_id,
                bytes,
                ..
            } = ev
            {
                let _ = gateway.provider_message(connection_id, &bytes).await.unwrap();
                if bytes.windows(18).any(|w| w == b"response.completed") {
                    break;
                }
            }
        }
        assert_eq!(creates2.load(Ordering::SeqCst), 1);

        up1.enabled = true;
        up3.enabled = true;
        replace_websocket_upstreams(&state, vec![up1.clone(), up2.clone(), up3.clone()]);

        let d2 = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d2.candidates.len(), 3);

        let state_clone = state.clone();
        let coordinator = tokio::spawn(async move {
            accepted_b.notified().await;
            let mut disabled_up2 = up2;
            disabled_up2.enabled = false;
            replace_websocket_upstreams(&state_clone, vec![up1, disabled_up2, up3]);
            release_b.notify_one();
        });

        let (_ready2, errs2) = responses_websocket::dispatch_responses_websocket_with_limits(
            &mut gateway,
            d2,
            &state,
            &mut connections,
            &events,
            &mut next_conn_id,
            8,
            Duration::from_secs(5),
            Duration::from_secs(5),
            2,
        )
        .await;
        coordinator.await.unwrap();
        assert!(
            errs2.is_empty(),
            "fallback must skip ineligible reused api-key-2 socket and connect to api-key-3: {errs2:?}"
        );
        assert_eq!(reqs1.load(Ordering::SeqCst), 1);
        assert_eq!(
            creates2.load(Ordering::SeqCst),
            1,
            "reused api-key-2 socket must not receive Turn 2 after becoming ineligible"
        );
        assert_eq!(
            reqs3.load(Ordering::SeqCst),
            1,
            "healthy api-key-3 must serve Turn 2 after skipping reused api-key-2 socket"
        );

        task1.abort();
        task2.abort();
        task3.abort();
    }
}

#[tokio::test]
async fn websocket_dispatch_deadline_bounds_bulkhead_wait_and_connection_prepare() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut up = websocket_upstream(1, "stalled", Url::parse(&format!("http://{addr}/")).unwrap());
    up.kind = UpstreamKind::OpenaiApiKey;

    let mut state = websocket_test_state(
        vec![up],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    state.websocket_bulkhead_config = BulkheadRuntimeConfig {
        max_conns_per_upstream: 1,
        semaphore_permits: 1,
        acquire_timeout: Duration::from_secs(30),
    };

    // Hold the sole bulkhead permit so the next acquire stalls for up to 30s unless bounded by deadline.
    let mut cfg = state.websocket_bulkhead_config;
    cfg.semaphore_permits = cfg.max_conns_per_upstream;
    let held_guard = state
        .websocket_bulkheads
        .bulkhead("stalled".to_owned(), cfg)
        .acquire_owned()
        .await
        .expect("sole bulkhead permit acquired");

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_str(TEST_KEY.plaintext.as_str()).unwrap());
    let auth = state.lifecycle.authenticate(&headers).await.unwrap();
    let mut gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
        state.lifecycle.clone(),
        &auth,
        headers,
        state.body_caps.messages,
        8,
    );
    let dispatch = gateway
        .client_message(auth, Bytes::from_static(RESPONSE_CREATE.as_bytes()))
        .await
        .unwrap()
        .unwrap();

    let (events, _event_rx) = tokio::sync::mpsc::channel(8);
    let mut connections = HashMap::new();
    let mut next_conn_id = 1;
    let started = std::time::Instant::now();
    let (_ready, errors) = responses_websocket::dispatch_responses_websocket_with_limits(
        &mut gateway,
        dispatch,
        &state,
        &mut connections,
        &events,
        &mut next_conn_id,
        8,
        Duration::from_millis(80),
        Duration::from_millis(120),
        3,
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].status, 504);
    assert_eq!(errors[0].code, "upstream_unavailable");
    assert!(
        elapsed < Duration::from_secs(2),
        "bulkhead wait must be bounded by connect deadline, took {elapsed:?}"
    );
    drop(held_guard);
}

#[test]
fn realtime_websocket_preserves_30s_connect_timeout() {
    assert_eq!(
        responses_websocket::DEFAULT_WS_PROVIDER_CONNECT_TIMEOUT,
        Duration::from_secs(30)
    );
    assert_eq!(
        responses_websocket::WS_PROVIDER_CONNECT_TIMEOUT,
        Duration::from_secs(15)
    );
}

#[tokio::test]
async fn websocket_handshake_public_skips_open_breaker_on_first_api_key_and_reaches_second_behind_16_oauth()
 {
    let mut failing_tasks = Vec::new();
    let mut records = Vec::new();
    let quota_cache = Arc::new(crate::subscription_quota_cache::SubscriptionQuotaCache::new());

    for idx in 1..=16_u128 {
        let (url, _reqs, task) = spawn_http_error_provider(
            429,
            r#"{"error":{"code":"rate_limit_exceeded","message":"rate limited"}}"#.to_owned(),
        )
        .await;
        let mut up = websocket_upstream(idx, &format!("oauth-{idx}"), url);
        up.kind = UpstreamKind::OpenaiOauth;
        seed_subscription_quota(&quota_cache, up.id, 0.01 * idx as f64, 0.01, false);
        failing_tasks.push(task);
        records.push(up);
    }
    let (api1_url, api1_reqs, api1_task) = spawn_http_error_provider(
        500,
        r#"{"error":{"code":"server_error","message":"api1 down"}}"#.to_owned(),
    )
    .await;
    let api1 = websocket_upstream(101, "openai-api-key-1", api1_url);
    records.push(api1.clone());
    let (api2_url, api2_reqs, api2_task) = spawn_responses_success_provider().await;
    let api2 = websocket_upstream(102, "openai-api-key-2", api2_url);
    records.push(api2);

    let state = subscription_websocket_state(records, quota_cache).await;
    // Open breakers on the first 14 OAuth upstreams (so 15 & 16 fail 429) AND on the first API key.
    for id in (1..=14_u128).chain(std::iter::once(101_u128)) {
        let breaker = state.breaker_registry.breaker(
            responses_websocket::websocket_breaker_key(Uuid::from_u128(id)),
            state.breaker_config,
            state.clock.clone(),
        );
        for _ in 0..state.breaker_config.failures_to_open {
            if let Ok(permit) = breaker.permit() {
                permit.record_failure();
            }
        }
    }

    let router = proxy_router(state, 30);
    let (proxy_address, proxy_task) = spawn_proxy_router(router).await;
    let mut req = websocket_request(proxy_address);
    *req.uri_mut() = format!("ws://{proxy_address}/openai/v1/responses")
        .parse()
        .expect("public WS URI parses");
    let (mut client, _) = connect_async(req).await.expect("public WS connects");
    client
        .send(UpstreamMessage::Text(
            r#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":[]}"#.into(),
        ))
        .await
        .expect("create sends");
    let message = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("second API key fallback responds")
        .expect("stream stays open")
        .expect("response decodes");
    let created: Value =
        serde_json::from_str(message.to_text().expect("text frame")).expect("json");
    assert_eq!(created["response"]["id"], "healthy");
    assert_eq!(api1_reqs.load(Ordering::SeqCst), 0);
    assert_eq!(api2_reqs.load(Ordering::SeqCst), 1);

    let _ = client.close(None).await;
    proxy_task.abort();
    api1_task.abort();
    api2_task.abort();
    for task in failing_tasks {
        task.abort();
    }
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn websocket_handshake_post_upgrade_quota_exhaustion_skips_pin_and_continues_and_respects_deadline()
 {
    // Part A: Exhausting an OAuth candidate DURING its WebSocket upgrade drops the uncommitted
    // socket, avoids pinning `session_upstream`/`session_oauth`, and fails over cleanly.
    {
        let quota_cache = Arc::new(crate::subscription_quota_cache::SubscriptionQuotaCache::new());
        let up1_id = Uuid::from_u128(1);
        let upgrade_count_1 = Arc::new(AtomicUsize::new(0));
        let text_msgs_1 = Arc::new(AtomicUsize::new(0));

        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let quota_for_up1 = quota_cache.clone();
        let upgrade_1_task = upgrade_count_1.clone();
        let text_1_task = text_msgs_1.clone();
        let task1 = tokio::spawn(async move {
            while let Ok((socket, _)) = listener1.accept().await {
                upgrade_1_task.fetch_add(1, Ordering::SeqCst);
                let quota = quota_for_up1.clone();
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(
                    socket,
                    |_req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        seed_subscription_quota(&quota, up1_id, 0.0, 1.0, true);
                        Ok(resp)
                    },
                )
                .await
                else {
                    continue;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    if msg.is_text() {
                        text_1_task.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();
        let reqs2 = Arc::new(AtomicUsize::new(0));
        let reqs2_task = reqs2.clone();
        let task2 = tokio::spawn(async move {
            let (socket, _) = listener2.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if msg.is_text() {
                    let count = reqs2_task.fetch_add(1, Ordering::SeqCst) + 1;
                    ws.send(UpstreamMessage::Text(
                        format!(
                            r#"{{"type":"response.created","response":{{"id":"healthy-{count}","store":false}}}}"#
                        )
                        .into(),
                    ))
                    .await
                    .unwrap();
                    ws.send(UpstreamMessage::Text(
                        format!(
                            r#"{{"type":"response.completed","response":{{"id":"healthy-{count}","status":"completed","usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
                        )
                        .into(),
                    ))
                    .await
                    .unwrap();
                }
            }
        });

        let mut up1 =
            websocket_upstream(1, "oauth-1", Url::parse(&format!("http://{addr1}/")).unwrap());
        up1.kind = UpstreamKind::OpenaiOauth;
        let mut up2 =
            websocket_upstream(2, "oauth-2", Url::parse(&format!("http://{addr2}/")).unwrap());
        up2.kind = UpstreamKind::OpenaiOauth;
        seed_subscription_quota(&quota_cache, up1.id, 0.05, 0.05, false);
        seed_subscription_quota(&quota_cache, up2.id, 0.20, 0.20, false);

        let state = subscription_websocket_state(vec![up1, up2], quota_cache).await;
        let headers = {
            let mut h = HeaderMap::new();
            h.insert("x-api-key", HeaderValue::from_str(TEST_KEY.plaintext.as_str()).unwrap());
            h
        };
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            8,
        )
        .with_codex_backend(true);

        let d1 = gateway
            .client_message(
                auth.clone(),
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        let (events, mut event_rx) = tokio::sync::mpsc::channel(16);
        let mut connections = HashMap::new();
        let mut next_conn_id = 1;
        let (_ready1, errs1) = responses_websocket::dispatch_responses_websocket_with_limits(
            &mut gateway,
            d1,
            &state,
            &mut connections,
            &events,
            &mut next_conn_id,
            8,
            Duration::from_secs(5),
            Duration::from_secs(5),
            3,
        )
        .await;
        assert!(errs1.is_empty());
        assert_eq!(upgrade_count_1.load(Ordering::SeqCst), 1);
        assert_eq!(
            text_msgs_1.load(Ordering::SeqCst),
            0,
            "post-upgrade exhausted oauth-1 must be dropped before any response.create write"
        );
        assert_eq!(
            connections.len(),
            1,
            "only committed oauth-2 socket must remain registered"
        );
        while let Some(ev) = event_rx.recv().await {
            if let responses_websocket::WsProviderEvent::Frame {
                connection_id,
                bytes,
                ..
            } = ev
            {
                let _ = gateway.provider_message(connection_id, &bytes).await.unwrap();
                if bytes.windows(18).any(|w| w == b"response.completed") {
                    break;
                }
            }
        }
        assert_eq!(reqs2.load(Ordering::SeqCst), 1);

        // Turn 2 must route directly via healthy oauth-2 (never pinned to rejected oauth-1).
        let d2 = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","model":"gpt-6.2-sol","store":false,"input":[]}"#,
                ),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d2.candidates.len(), 1);
        assert_eq!(d2.candidates[0].upstream_name, "oauth-2");
        let (_ready2, errs2) = responses_websocket::dispatch_responses_websocket_with_limits(
            &mut gateway,
            d2,
            &state,
            &mut connections,
            &events,
            &mut next_conn_id,
            8,
            Duration::from_secs(5),
            Duration::from_secs(5),
            3,
        )
        .await;
        assert!(errs2.is_empty());
        while let Some(ev) = event_rx.recv().await {
            if let responses_websocket::WsProviderEvent::Frame {
                connection_id,
                bytes,
                ..
            } = ev
            {
                let _ = gateway.provider_message(connection_id, &bytes).await.unwrap();
                if bytes.windows(18).any(|w| w == b"response.completed") {
                    break;
                }
            }
        }
        assert_eq!(upgrade_count_1.load(Ordering::SeqCst), 1);
        assert_eq!(reqs2.load(Ordering::SeqCst), 2);

        task1.abort();
        task2.abort();
    }

    // Part B: Post-upgrade `validate_candidate_for_send` stalling past deadline cleans up the
    // uncommitted socket and returns 504 without writing to the upstream.
    {
        use std::sync::atomic::AtomicBool;
        struct ArmableStallKeyStore {
            armed: Arc<AtomicBool>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl ManagedKeyStore for ArmableStallKeyStore {
            async fn issue(
                &self,
                principal_id: &str,
                key_id: &str,
                params: IssueParams,
            ) -> StorageResult<StoredApiKeyRecord> {
                FixtureKeyStore.issue(principal_id, key_id, params).await
            }

            async fn get(
                &self,
                principal_id: &str,
                key_id: &str,
            ) -> StorageResult<Option<StoredApiKeyRecord>> {
                FixtureKeyStore.get(principal_id, key_id).await
            }

            async fn lookup_by_index_hash(
                &self,
                index_hash: &[u8; 32],
            ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
                if self.armed.load(Ordering::SeqCst) {
                    self.release.notified().await;
                }
                FixtureKeyStore.lookup_by_index_hash(index_hash).await
            }

            async fn list_by_principal(
                &self,
                principal_id: &str,
            ) -> StorageResult<Vec<StoredApiKeyRecord>> {
                FixtureKeyStore.list_by_principal(principal_id).await
            }

            async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
                FixtureKeyStore.list_all().await
            }

            async fn update(
                &self,
                principal_id: &str,
                key_id: &str,
                mutation: ApiKeyMutation,
            ) -> StorageResult<()> {
                FixtureKeyStore.update(principal_id, key_id, mutation).await
            }

            async fn revoke_zero_secrets(&self, principal_id: &str, key_id: &str) -> StorageResult<()> {
                FixtureKeyStore.revoke_zero_secrets(principal_id, key_id).await
            }
        }

        let armed = Arc::new(AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());
        let text_msgs = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let armed_task = armed.clone();
        let text_task = text_msgs.clone();
        let server_task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(
                socket,
                |_req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                 resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    // Stall the very next `authenticate()` inside post-upgrade validation.
                    armed_task.store(true, Ordering::SeqCst);
                    Ok(resp)
                },
            )
            .await
            else {
                return;
            };
            while let Some(Ok(msg)) = ws.next().await {
                if msg.is_text() {
                    text_task.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        let up = websocket_upstream(1, "slow-validate", Url::parse(&format!("http://{addr}/")).unwrap());
        let state = websocket_test_state(
            vec![up],
            TerminalStrategy::FirstPick,
            DrainController::new(),
            Arc::new(WebSocketTestSignerFactory),
            Arc::new(ArmableStallKeyStore {
                armed,
                release: release.clone(),
            }),
            vec!["*".to_owned()],
        )
        .await;
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_str(TEST_KEY.plaintext.as_str()).unwrap());
        let auth = state.lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
            state.lifecycle.clone(),
            &auth,
            headers,
            state.body_caps.messages,
            8,
        );
        let dispatch = gateway
            .client_message(auth, Bytes::from_static(RESPONSE_CREATE.as_bytes()))
            .await
            .unwrap()
            .unwrap();
        let (events, _event_rx) = tokio::sync::mpsc::channel(8);
        let mut connections = HashMap::new();
        let mut next_conn_id = 1;
        let (_ready, errors) = responses_websocket::dispatch_responses_websocket_with_limits(
            &mut gateway,
            dispatch,
            &state,
            &mut connections,
            &events,
            &mut next_conn_id,
            8,
            Duration::from_millis(80),
            Duration::from_millis(120),
            1,
        )
        .await;
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].status, 504);
        assert_eq!(errors[0].code, "upstream_unavailable");
        assert!(connections.is_empty(), "timed-out uncommitted socket must be dropped");
        assert_eq!(text_msgs.load(Ordering::SeqCst), 0);

        release.notify_waiters();
        server_task.abort();
    }
}

#[tokio::test]
async fn websocket_relay_delivers_buffered_response_failed_before_pending_expiry() {
    use futures_util::StreamExt as _;

    // Obtain a real axum WebSocket `SplitSink` by upgrading a loopback socket.
    let (sink_tx, sink_rx) = tokio::sync::oneshot::channel();
    let sink_tx = Arc::new(std::sync::Mutex::new(Some(sink_tx)));
    let app = axum::Router::new().route(
        "/ws",
        axum::routing::get(move |ws: axum::extract::ws::WebSocketUpgrade| {
            let sink_tx = sink_tx.clone();
            async move {
                ws.on_upgrade(move |socket| async move {
                    let (sink, _stream) = socket.split();
                    if let Some(tx) = sink_tx.lock().unwrap().take() {
                        let _ = tx.send(sink);
                    }
                    std::future::pending::<()>().await;
                })
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let (mut client_ws, _) = connect_async(format!("ws://{addr}/ws")).await.unwrap();
    let mut downstream_sink = sink_rx.await.unwrap();

    // Stand up a real proxy gateway turn using `websocket_test_state` + open provider socket
    // that keeps its WebSocket open after `response.created` so no premature `Closed` event
    // arrives ahead of our synthetic `response.failed`.
    let up_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_addr = up_listener.local_addr().unwrap();
    let up_url = Url::parse(&format!("http://{up_addr}/")).unwrap();
    let up_task = tokio::spawn(async move {
        let Ok((socket, _)) = up_listener.accept().await else {
            return;
        };
        let Ok(mut provider) = accept_async(socket).await else {
            return;
        };
        while let Some(Ok(message)) = provider.next().await {
            if message.is_text() {
                let _ = provider
                    .send(UpstreamMessage::Text(
                        r#"{"type":"response.created","response":{"id":"healthy","store":false}}"#
                            .into(),
                    ))
                    .await;
                break;
            }
        }
        while let Some(Ok(_)) = provider.next().await {}
    });
    let up = websocket_upstream(1, "provider-a", up_url);
    let relay_state = websocket_test_state(
        vec![up.clone()],
        TerminalStrategy::FirstPick,
        DrainController::new(),
        Arc::new(WebSocketTestSignerFactory),
        Arc::new(FixtureKeyStore),
        vec!["*".to_owned()],
    )
    .await;
    let mut r_headers = HeaderMap::new();
    r_headers.insert("x-api-key", HeaderValue::from_str(TEST_KEY.plaintext.as_str()).unwrap());
    let r_auth = relay_state.lifecycle.authenticate(&r_headers).await.unwrap();
    let mut r_gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
        relay_state.lifecycle.clone(),
        &r_auth,
        r_headers,
        relay_state.body_caps.messages,
        8,
    );
    let d1 = r_gateway
        .client_message(
            r_auth,
            Bytes::from_static(
                br#"{"type":"response.create","model":"gpt-test","store":false,"input":[]}"#,
            ),
        )
        .await
        .unwrap()
        .unwrap();
    let (events, mut event_rx) = tokio::sync::mpsc::channel(16);
    let mut connections = HashMap::new();
    let mut next_conn_id = 1;
    let (_ready, errs) = responses_websocket::dispatch_responses_websocket_with_limits(
        &mut r_gateway,
        d1,
        &relay_state,
        &mut connections,
        &events,
        &mut next_conn_id,
        8,
        Duration::from_secs(5),
        Duration::from_secs(5),
        1,
    )
    .await;
    assert!(errs.is_empty());
    // Consume the mock provider's initial `response.created` (`healthy`), park on a statusless
    // `error` (`healthy`), buffer a timely `response.failed` behind a slow dispatch past the
    // deadline, and prove `drain_buffered_ws_events_before_expiry` forwards `response.failed`
    // rather than synthesizing an expiry terminal.
    let conn_id = *connections.keys().next().unwrap();
    let first_created = loop {
        match event_rx.recv().await.unwrap() {
            responses_websocket::WsProviderEvent::Frame { bytes, .. } => break bytes,
            _ => continue,
        }
    };
    assert!(r_gateway.provider_message(conn_id, &first_created).await.unwrap().forward);
    let statusless_err = Bytes::from_static(
        br#"{"type":"error","response_id":"healthy","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded"}}"#,
    );
    assert!(r_gateway.provider_message(conn_id, &statusless_err).await.unwrap().forward);
    let past_deadline = r_gateway
        .next_pending_error_deadline()
        .expect("pending terminal scheduled")
        + Duration::from_secs(1);

    events
        .send(responses_websocket::WsProviderEvent::Frame {
            connection_id: conn_id,
            bytes: Bytes::from_static(
                br#"{"type":"response.failed","response":{"id":"healthy","status":"failed","error":{"code":"server_is_overloaded","message":"Overloaded"}}}"#,
            ),
            text: true,
        })
        .await
        .unwrap();

    let mut dispatches = VecDeque::new();
    assert!(
        responses_websocket::drain_buffered_ws_events_before_expiry(
            &mut event_rx,
            &mut r_gateway,
            &mut connections,
            &mut dispatches,
            &mut downstream_sink,
            past_deadline,
        )
        .await
    );
    assert!(
        r_gateway.next_pending_error_deadline().is_none(),
        "buffered response.failed must drain and retire the pending terminal"
    );
    let forwarded = tokio::time::timeout(Duration::from_secs(5), client_ws.next())
        .await
        .expect("forwarded frame arrives")
        .unwrap()
        .unwrap();
    let forwarded_json: Value = serde_json::from_str(forwarded.to_text().unwrap()).unwrap();
    assert_eq!(forwarded_json["type"], "response.failed");

    let _ = client_ws.close(None).await;
    server_task.abort();
    up_task.abort();
}
