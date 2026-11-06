use super::*;
use cc_lb_lifecycle::{StreamError, StreamSuccess};
use tower::ServiceExt as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const START: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1027\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"usage\":{\"input_tokens\":13,\"output_tokens\":0}}}\n\n";
const CACHE_START: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1027\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"usage\":{\"input_tokens\":13,\"output_tokens\":0,\"cache_read_input_tokens\":12000}}}\n\n";
const CONTENT: &str = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n";
const AGENT_CONTENT: &str = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_agent\",\"name\":\"Bash\",\"input\":{}}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n";
const AGENT_DELTA: &str = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":7}}\n\n";
const DELTA: &str = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n";
const STOP: &str = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
const REQUEST: &[u8] = br#"{"model":"claude-test","messages":[],"stream":true}"#;

struct Observation {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    row: cc_lb_storage_api::RequestEvent,
    events: Vec<LifecycleEvent>,
}

impl Observation {
    fn completion(&self) -> &Result<StreamSuccess, StreamError> {
        let completed = self
            .events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::StreamCompleted { result, .. } => Some(result),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(completed.len(), 1, "exactly one stream completion");
        completed[0]
    }

    fn assert_terminal(&self, error_code: Option<&str>) {
        let terminal = self
            .events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::RequestTerminated {
                    reason,
                    client_status,
                    ..
                } => Some((reason, client_status)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let expected = error_code.map_or(TerminationReason::Success, |code| {
            TerminationReason::ErrorCode(code.to_owned())
        });
        assert_eq!(terminal.len(), 1, "exactly one request termination");
        assert_eq!(terminal[0].0, &expected);
        assert_eq!(*terminal[0].1, self.status.as_u16());
        assert_eq!(self.row.status, self.status.as_u16());
        assert_eq!(self.row.error_code.as_deref(), error_code);
        assert_eq!(self.row.upstream_stream_warning_type, None);
        assert_eq!(self.row.upstream_stream_warning_message, None);
    }

    fn assert_incomplete_outcome(&self) {
        assert_eq!(self.status, StatusCode::OK);
        let error = self
            .completion()
            .as_ref()
            .expect_err("missing semantic stop is incomplete");
        assert_eq!(error.error_type, "upstream_response_incomplete");
        self.assert_terminal(Some("upstream_stream_error"));
        assert_eq!(
            self.row.upstream_error_type.as_deref(),
            Some("upstream_response_incomplete")
        );
        assert_eq!(
            self.row.upstream_error_message.as_deref(),
            Some(error.error_message.as_str())
        );
    }

    fn assert_incomplete(&self, input: u64, output: u64) {
        self.assert_incomplete_outcome();
        assert_eq!(
            (self.row.input_tokens, self.row.output_tokens),
            (Some(input), Some(output))
        );
        let completion_index = self
            .events
            .iter()
            .position(|event| matches!(event, LifecycleEvent::StreamCompleted { .. }))
            .expect("stream completion observed");
        let usage = self.events[..completion_index]
            .iter()
            .rev()
            .find_map(|event| match event {
                LifecycleEvent::UsageObserved { usage, .. } => Some(usage),
                _ => None,
            })
            .expect("incomplete EOF emits accumulated usage before completion");
        assert_eq!((usage.input_tokens, usage.output_tokens), (input, output));
    }

    fn assert_success(&self, input: u64, output: u64, stopped: bool) {
        let success = self
            .completion()
            .as_ref()
            .expect("stream completes successfully");
        self.assert_terminal(None);
        assert_eq!(self.row.upstream_error_type, None);
        assert_eq!(self.row.upstream_error_message, None);
        assert_eq!(
            (success.usage.input_tokens, success.usage.output_tokens),
            (input, output)
        );
        assert_eq!(
            (self.row.input_tokens, self.row.output_tokens),
            (Some(input), Some(output))
        );
        assert_eq!(success.stream_message_stop_ms.is_some(), stopped);
        assert_eq!(self.row.stream_message_stop_ms.is_some(), stopped);
    }

    fn assert_semantic_success(&self, input: u64, output: u64, stopped: bool) {
        self.assert_terminal(None);
        assert_eq!(self.row.upstream_error_type, None);
        assert_eq!(self.row.upstream_error_message, None);
        assert_eq!(
            (self.row.input_tokens, self.row.output_tokens),
            (Some(input), Some(output))
        );
        assert_eq!(self.row.stream_message_stop_ms.is_some(), stopped);
        assert!(
            self.events
                .iter()
                .filter(|event| matches!(event, LifecycleEvent::StreamCompleted { .. }))
                .count()
                <= 1,
            "semantic close must not emit duplicate stream completion"
        );
    }
}

async fn observe(
    dispatcher: Arc<dyn UpstreamDispatch>,
    transform: bool,
    record: UpstreamRecord,
    request: http::Request<Bytes>,
) -> TestResult<Observation> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "semantic-sse-eof.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut rx = test_bus.bus.subscribe_lifecycle();
    let hook = transform
        .then(|| Arc::new(UnchangedSseTransform::default()) as Arc<dyn SseEventTransformHook>);
    let lifecycle = with_test_limit_subject(lifecycle_with_record_and_config(
        None,
        hook,
        dispatcher,
        LifecycleConfig::default(),
        record,
    ))
    .with_event_bus(test_bus.bus_arc());
    let response = handle_authenticated(&lifecycle, request).await?;
    let (status, headers, body) = collect_body(response).await;
    let events = tokio::time::timeout(Duration::from_secs(3), async {
        let mut events = Vec::new();
        loop {
            let event = rx.recv().await.expect("lifecycle event delivered");
            let terminated = matches!(event, LifecycleEvent::RequestTerminated { .. });
            events.push(event);
            if terminated {
                break events;
            }
        }
    })
    .await
    .expect("request terminates after downstream EOF");
    let mut rows = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(rows.len(), 1);
    Ok(Observation {
        status,
        headers,
        body,
        row: rows.remove(0),
        events,
    })
}
async fn observe_eof_with_keepalive(
    dispatcher: Arc<dyn UpstreamDispatch>,
    record: UpstreamRecord,
    request: http::Request<Bytes>,
    enqueuer: Arc<dyn CacheKeepaliveEnqueuer>,
) -> TestResult<Observation> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "semantic-sse-eof-keepalive.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = with_test_limit_subject(
        lifecycle_with_record_config_and_keepalive(
            None,
            None,
            dispatcher,
            LifecycleConfig::default(),
            record,
            Some(CacheKeepaliveConfig {
                enabled: true,
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
                min_cache_tokens: 10_000,
                classifier: ClassifierConfig::default(),
            }),
        )
        .with_cache_keepalive_enqueuer(enqueuer),
    )
    .with_event_bus(test_bus.bus_arc());
    let response = handle_authenticated(&lifecycle, request).await?;
    let (status, headers, body) = collect_body(response).await;
    let events = tokio::time::timeout(Duration::from_secs(3), async {
        let mut events = Vec::new();
        loop {
            let event = rx.recv().await.expect("lifecycle event delivered");
            let terminated = matches!(event, LifecycleEvent::RequestTerminated { .. });
            events.push(event);
            if terminated {
                break events;
            }
        }
    })
    .await
    .expect("request terminates after downstream EOF");
    let mut rows = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(rows.len(), 1);
    Ok(Observation {
        status,
        headers,
        body,
        row: rows.remove(0),
        events,
    })
}

async fn observe_drop_after_terminal(
    dispatcher: Arc<dyn UpstreamDispatch>,
    record: UpstreamRecord,
    request: http::Request<Bytes>,
    terminal_marker: &'static [u8],
    transform: Option<Arc<dyn SseEventTransformHook>>,
) -> TestResult<Observation> {
    observe_drop_after_terminal_with_keepalive(
        dispatcher,
        record,
        request,
        terminal_marker,
        transform,
        None,
    )
    .await
}

async fn observe_drop_after_terminal_with_keepalive(
    dispatcher: Arc<dyn UpstreamDispatch>,
    record: UpstreamRecord,
    request: http::Request<Bytes>,
    terminal_marker: &'static [u8],
    transform: Option<Arc<dyn SseEventTransformHook>>,
    keepalive: Option<(CacheKeepaliveConfig, Arc<dyn CacheKeepaliveEnqueuer>)>,
) -> TestResult<Observation> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "semantic-sse-drop.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = if let Some((config, enqueuer)) = keepalive {
        lifecycle_with_record_config_and_keepalive(
            None,
            transform,
            dispatcher,
            LifecycleConfig::default(),
            record,
            Some(config),
        )
        .with_cache_keepalive_enqueuer(enqueuer)
    } else {
        lifecycle_with_record_and_config(
            None,
            transform,
            dispatcher,
            LifecycleConfig::default(),
            record,
        )
    }
    .with_event_bus(test_bus.bus_arc());
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        handle_authenticated(&lifecycle, request),
    )
    .await
    .expect("stream headers arrive before upstream EOF")?;
    let status = response.status();
    let headers = response.headers().clone();
    let mut body = response.into_body();
    let mut received = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), body.frame())
            .await?
            .expect("response frame")
            .expect("response frame succeeds")
            .into_data()
            .expect("response frame is data");
        received.extend_from_slice(&frame);
        if received
            .windows(terminal_marker.len())
            .any(|window| window == terminal_marker)
        {
            break;
        }
    }
    drop(body);
    let events = tokio::time::timeout(Duration::from_secs(3), async {
        let mut events = Vec::new();
        loop {
            let event = rx.recv().await.expect("lifecycle event delivered");
            let terminated = matches!(event, LifecycleEvent::RequestTerminated { .. });
            events.push(event);
            if terminated {
                break events;
            }
        }
    })
    .await
    .expect("request terminates after downstream drop");
    let mut rows = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(rows.len(), 1);
    Ok(Observation {
        status,
        headers,
        body: Bytes::from(received),
        row: rows.remove(0),
        events,
    })
}

struct PendingDispatch {
    headers: HeaderMap,
    body: Bytes,
}

#[async_trait]
impl UpstreamDispatch for PendingDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let body = self.body.clone();
        let stream = async_stream::stream! {
            yield Ok::<Bytes, std::convert::Infallible>(body);
            std::future::pending::<()>().await;
        };
        let mut response = Response::new(Body::from_stream(stream));
        *response.headers_mut() = self.headers.clone();
        Ok(response)
    }
}

async fn anthropic(
    body: Bytes,
    status: StatusCode,
    gzip: bool,
    transform: bool,
) -> TestResult<Observation> {
    let wire = if gzip {
        gzip_bytes(&body)
    } else {
        body.clone()
    };
    let mut headers = sse_headers();
    if gzip {
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    let observed = observe(
        Arc::new(FixedDispatch {
            status,
            headers,
            body: wire.clone(),
        }),
        transform,
        default_upstream_record(),
        messages_request(Bytes::from_static(REQUEST)),
    )
    .await?;
    let expected = if gzip && !transform { &wire } else { &body };
    assert_eq!(
        &observed.body, expected,
        "no replay or synthetic terminal/error bytes"
    );
    assert_eq!(
        observed.headers.get(CONTENT_ENCODING).is_some(),
        gzip && !transform
    );
    Ok(observed)
}

#[tokio::test]
async fn semantic_sse_eof_before_delta_matrix() -> TestResult {
    for gzip in [false, true] {
        for transform in [false, true] {
            let observed = anthropic(
                Bytes::from(format!("{START}{CONTENT}")),
                StatusCode::OK,
                gzip,
                transform,
            )
            .await?;
            observed.assert_incomplete(13, 0);
            assert_eq!(observed.row.stream_message_stop_ms, None);
        }
    }
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_after_delta_matrix() -> TestResult {
    for gzip in [false, true] {
        for transform in [false, true] {
            let observed = anthropic(
                Bytes::from(format!("{START}{CONTENT}{DELTA}")),
                StatusCode::OK,
                gzip,
                transform,
            )
            .await?;
            observed.assert_incomplete(13, 7);
            assert_eq!(observed.row.stream_message_stop_ms, None);
        }
    }
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_complete_matrix() -> TestResult {
    for gzip in [false, true] {
        for transform in [false, true] {
            let observed = anthropic(
                Bytes::from(format!("{START}{CONTENT}{DELTA}{STOP}")),
                StatusCode::OK,
                gzip,
                transform,
            )
            .await?;
            observed.assert_success(13, 7, true);
        }
    }
    Ok(())
}
#[tokio::test]
async fn anthropic_terminal_client_close_is_success() -> TestResult {
    let body = Bytes::from(format!("{START}{CONTENT}{DELTA}{STOP}"));
    let observed = observe_drop_after_terminal(
        Arc::new(PendingDispatch {
            headers: sse_headers(),
            body: body.clone(),
        }),
        default_upstream_record(),
        messages_request(Bytes::from_static(REQUEST)),
        b"event: message_stop",
        None,
    )
    .await?;

    assert_eq!(observed.status, StatusCode::OK);
    assert_eq!(observed.row.error_code, None);
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (Some(13), Some(7))
    );
    Ok(())
}

#[tokio::test]
async fn anthropic_terminal_client_close_enqueues_keepalive_once() -> TestResult {
    let enqueuer = Arc::new(TransformedQuestionEnqueuer::default());
    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"tools":[{"name":"Bash","description":"run a command","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"hello"}],"stream":true}"#,
    ));
    request.headers_mut().insert(
        "x-claude-code-session-id",
        HeaderValue::from_static("semantic-close-keepalive"),
    );
    let observed = observe_drop_after_terminal_with_keepalive(
        Arc::new(PendingDispatch {
            headers: sse_headers(),
            body: Bytes::from(format!("{CACHE_START}{AGENT_CONTENT}{AGENT_DELTA}{STOP}")),
        }),
        default_upstream_record(),
        request,
        b"event: message_stop",
        None,
        Some((
            CacheKeepaliveConfig {
                enabled: true,
                refresh_lead_time_5m_secs: 30,
                refresh_lead_time_1h_secs: 300,
                max_refreshes_per_session: 12,
                max_total_duration_secs: 14_400,
                snapshot_max_bytes: 524_288,
                min_cache_tokens: 10_000,
                classifier: ClassifierConfig::default(),
            },
            Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
        )),
    )
    .await?;

    observed.assert_semantic_success(13, 7, false);
    assert!(
        observed
            .body
            .windows(b"event: message_stop".len())
            .any(|window| window == b"event: message_stop"),
        "semantic terminal bytes must reach the client before close"
    );
    assert_eq!(
        observed.row.cache_read_input_tokens,
        Some(12_000),
        "semantic close must preserve provider cache-read usage"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !enqueuer.enqueued.lock().expect("enqueue lock").is_empty() {
                break;
            }
            enqueuer.completed.notified().await;
        }
    })
    .await
    .expect("semantic close completes keepalive classification");
    assert_eq!(
        enqueuer.enqueued.lock().expect("enqueue lock").len(),
        1,
        "semantic close must enqueue one fresh agent-in-turn generation"
    );
    Ok(())
}

#[tokio::test]
async fn anthropic_normal_eof_enqueues_keepalive_once() -> TestResult {
    let enqueuer = Arc::new(TransformedQuestionEnqueuer::default());
    let mut request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":32,"system":[{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}}],"tools":[{"name":"Bash","description":"run a command","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"hello"}],"stream":true}"#,
    ));
    request.headers_mut().insert(
        "x-claude-code-session-id",
        HeaderValue::from_static("normal-eof-keepalive"),
    );
    let observed = observe_eof_with_keepalive(
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: sse_headers(),
            body: Bytes::from(format!("{CACHE_START}{AGENT_CONTENT}{AGENT_DELTA}{STOP}")),
        }),
        default_upstream_record(),
        request,
        Arc::clone(&enqueuer) as Arc<dyn CacheKeepaliveEnqueuer>,
    )
    .await?;

    observed.assert_success(13, 7, true);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !enqueuer.enqueued.lock().expect("enqueue lock").is_empty() {
                break;
            }
            enqueuer.completed.notified().await;
        }
    })
    .await
    .expect("normal EOF completes keepalive classification");
    assert_eq!(
        enqueuer.enqueued.lock().expect("enqueue lock").len(),
        1,
        "normal EOF must enqueue one keepalive completion"
    );
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_terminal_is_json_type_not_event_name() -> TestResult {
    for transform in [false, true] {
        for payload in [
            "event: message_stop\ndata: {}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n\n",
        ] {
            let output = if payload.contains("message_delta") {
                7
            } else {
                0
            };
            let observed = anthropic(
                Bytes::from(format!("{START}{payload}")),
                StatusCode::OK,
                false,
                transform,
            )
            .await?;
            observed.assert_incomplete(13, output);
        }
        let observed = anthropic(
            Bytes::from(format!(
                "{START}event: ping\ndata: {{\"type\":\"message_stop\"}}\n\n"
            )),
            StatusCode::OK,
            false,
            transform,
        )
        .await?;
        observed.assert_success(13, 0, true);
    }
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_empty_and_ping_only_are_incomplete() -> TestResult {
    for body in [
        Bytes::new(),
        Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n"),
    ] {
        for transform in [false, true] {
            let observed = anthropic(body.clone(), StatusCode::OK, false, transform).await?;
            observed.assert_incomplete_outcome();
            assert_eq!(observed.row.stream_message_stop_ms, None);
        }
    }
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_existing_error_precedence() -> TestResult {
    for transform in [false, true] {
        let provider = anthropic(
            Bytes::from(format!("{START}event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}}}\n\n")),
            StatusCode::OK, false, transform,
        ).await?;
        assert_eq!(
            provider.row.upstream_error_type.as_deref(),
            Some("overloaded_error")
        );
        assert_eq!(
            provider.row.upstream_error_message.as_deref(),
            Some("Overloaded")
        );
        provider.assert_terminal(Some("upstream_stream_error"));
        let provider_errors = provider
            .events
            .iter()
            .filter_map(|event| match event {
                LifecycleEvent::StreamCompleted {
                    result: Err(error), ..
                } => Some(error.error_type.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(provider_errors, vec!["overloaded_error"]);

        let http_error = anthropic(
            Bytes::from_static(START.as_bytes()),
            StatusCode::TOO_MANY_REQUESTS,
            false,
            transform,
        )
        .await?;
        http_error.assert_terminal(Some("upstream_4xx"));
        assert!(
            http_error.completion().is_ok(),
            "clean HTTP error EOF is not an incomplete-response error"
        );
        assert_ne!(
            http_error.row.upstream_error_type.as_deref(),
            Some("upstream_response_incomplete")
        );

        for (stop, code) in [
            ("refusal", "upstream_refusal"),
            (
                "model_context_window_exceeded",
                "upstream_context_window_exceeded",
            ),
        ] {
            let observed = anthropic(
                Bytes::from(format!("{START}event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{stop}\"}},\"usage\":{{\"output_tokens\":7}}}}\n\n")),
                StatusCode::OK, false, transform,
            ).await?;
            observed.assert_terminal(Some(code));
            assert_eq!(observed.row.upstream_error_type.as_deref(), Some(stop));
            assert!(
                observed.completion().is_ok(),
                "abnormal stop retains existing completion behavior"
            );
            assert_eq!(observed.row.stream_message_stop_ms, None);
        }
    }
    Ok(())
}

struct FixtureDispatch {
    app: axum::Router,
    mode: &'static str,
    delivered: Arc<Mutex<Vec<u8>>>,
}

#[async_trait]
impl UpstreamDispatch for FixtureDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let (url, method, mut headers, body) = request.into_parts();
        headers.insert("x-fake-mode", HeaderValue::from_static(self.mode));
        let mut request = http::Request::builder()
            .method(method)
            .uri(url.as_str())
            .body(Body::from(body))
            .expect("fixture request builds");
        *request.headers_mut() = headers;
        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("fixture router is infallible");
        let (parts, mut body) = response.into_parts();
        let delivered = Arc::clone(&self.delivered);
        let stream = async_stream::stream! {
            while let Some(frame) = body.frame().await {
                let frame = frame.expect("fake upstream body frame succeeds");
                if let Ok(bytes) = frame.into_data() {
                    delivered.lock().expect("fixture capture lock").extend_from_slice(&bytes);
                    yield Ok::<Bytes, std::convert::Infallible>(bytes);
                }
            }
        };
        Ok(Response::from_parts(parts, Body::from_stream(stream)))
    }
}

#[tokio::test]
async fn semantic_sse_eof_fake_anthropic_truncate_vs_ok() -> TestResult {
    for mode in ["ok", "truncate-mid-stream"] {
        for transform in [false, true] {
            let delivered = Arc::new(Mutex::new(Vec::new()));
            let observed = observe(
                Arc::new(FixtureDispatch {
                    app: fake_anthropic::app(fake_anthropic::AppConfig::default()),
                    mode,
                    delivered: Arc::clone(&delivered),
                }),
                transform,
                default_upstream_record(),
                messages_request(Bytes::from_static(REQUEST)),
            )
            .await?;
            assert_eq!(
                observed.body.as_ref(),
                delivered.lock().expect("fixture capture lock").as_slice(),
                "fixture bytes are relayed exactly"
            );
            if mode == "truncate-mid-stream" {
                observed.assert_incomplete(100, 0);
                assert_eq!(observed.row.stream_message_stop_ms, None);
                assert!(!std::str::from_utf8(&observed.body)?.contains("event: message_stop"));
            } else {
                observed.assert_success(100, 50, true);
                assert_eq!(
                    std::str::from_utf8(&observed.body)?
                        .matches("event: message_stop")
                        .count(),
                    1
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_unsupported_stacked_encoding_preserves_success() -> TestResult {
    let plaintext = Bytes::from(format!("{START}{CONTENT}{DELTA}{STOP}"));
    let wire = brotli_bytes(&gzip_bytes(&plaintext));
    for transform in [false, true] {
        let mut headers = sse_headers();
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip, br"));
        let observed = observe(
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers,
                body: wire.clone(),
            }),
            transform,
            default_upstream_record(),
            messages_request(Bytes::from_static(REQUEST)),
        )
        .await?;
        assert_eq!(observed.status, StatusCode::OK);
        assert_eq!(observed.body, wire);
        assert_eq!(
            observed.headers.get(CONTENT_ENCODING),
            Some(&HeaderValue::from_static("gzip, br"))
        );
        observed.assert_terminal(None);
        assert_eq!(observed.row.upstream_error_type, None);
        assert_eq!(observed.row.upstream_error_message, None);
        observed
            .completion()
            .as_ref()
            .expect("unsupported encoding remains successful raw passthrough");
    }
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_non_anthropic_and_non_sse_controls() -> TestResult {
    let cases = [
        (
            default_upstream_record(),
            "/v1/messages",
            br#"{"model":"claude-test","messages":[]}"#.as_slice(),
            br#"{"type":"message","usage":{"input_tokens":13,"output_tokens":7}}"#.as_slice(),
            false,
            13,
            7,
        ),
        (
            upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
            "/v1/responses",
            br#"{"model":"gpt-test","input":"hello","stream":true}"#.as_slice(),
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":13,\"output_tokens\":7}}}\n\n".as_slice(),
            true,
            13,
            7,
        ),
        (
            upstream_record(StorageUpstreamKind::GeminiApiKey, "https://generativelanguage.googleapis.com/"),
            "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
            br#"{"contents":[]}"#.as_slice(),
            b"data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"hello\"}]}}],\"usageMetadata\":{\"promptTokenCount\":13,\"candidatesTokenCount\":7}}\n\n".as_slice(),
            true,
            13,
            7,
        ),
    ];
    for (record, path, request_body, body, sse, input, output) in cases {
        let body = Bytes::copy_from_slice(body);
        let headers = if sse {
            sse_headers()
        } else {
            json_headers(body.len(), false)
        };
        let request = http::Request::builder()
            .method("POST")
            .uri(path)
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::copy_from_slice(request_body))?;
        let observed = observe(
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers,
                body: body.clone(),
            }),
            false,
            record,
            request,
        )
        .await?;
        assert_eq!(observed.body, body);
        observed.assert_success(input, output, false);
    }
    Ok(())
}

#[tokio::test]
async fn semantic_sse_eof_gemini_json_array_stream_records_usage_and_preserves_body() -> TestResult
{
    let cases = [
        (
            upstream_record(
                StorageUpstreamKind::GeminiApiKey,
                "https://generativelanguage.googleapis.com/",
            ),
            "/v1beta/models/gemini-test:streamGenerateContent?alt=json",
            br#"[{"candidates":[{"content":{"parts":[{"text":"partial"}]}}]},{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":110,"candidatesTokenCount":4}}]"#.as_slice(),
        ),
        (
            upstream_record(
                StorageUpstreamKind::GeminiOauth,
                "https://cloudcode-pa.googleapis.com/v1internal",
            ),
            "/v1beta/models/gemini-test:streamGenerateContent?alt=json",
            br#"[{"response":{"candidates":[{"content":{"parts":[{"text":"partial"}]}}]}},{"response":{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":110,"candidatesTokenCount":4}}}]"#.as_slice(),
        ),
    ];

    for (record, path, raw_body) in cases {
        let body = Bytes::copy_from_slice(raw_body);
        let observed = observe(
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers: json_headers(body.len(), false),
                body: body.clone(),
            }),
            false,
            record,
            http::Request::builder()
                .method("POST")
                .uri(path)
                .header("x-api-key", common::managed_api_key())
                .body(Bytes::from_static(br#"{"contents":[]}"#))?,
        )
        .await?;

        assert_eq!(observed.body, body);
        observed.assert_success(110, 4, false);
    }
    Ok(())
}

#[tokio::test]
async fn codex_event_name_completed_accepts_transport_close() -> TestResult {
    let body = Bytes::from_static(
        b"event: response.created\n\
data: {\"response\":{\"status\":\"in_progress\",\"usage\":null}}\n\n\
event: response.output_text.delta\n\
data: {\"item_id\":\"msg_1\",\"delta\":\"hello\"}\n\n\
event: response.completed\n\
data: {\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":13,\"output_tokens\":7}}}\n\n",
    );
    let observed = observe(
        Arc::new(ScriptedDispatch::identity_sse(
            vec![body.clone()],
            Some(std::io::ErrorKind::ConnectionReset),
        )),
        false,
        upstream_record(
            StorageUpstreamKind::OpenaiOauth,
            "https://chatgpt.com/backend-api/",
        ),
        http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-5-codex","input":"hello","stream":true}"#,
            ))?,
    )
    .await?;

    assert_eq!(observed.body, body);
    assert_eq!(observed.status, StatusCode::OK);
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (Some(13), Some(7))
    );
    assert_eq!(observed.row.error_code, None);
    assert_eq!(observed.row.upstream_error_type, None);
    assert_eq!(
        observed.row.upstream_stream_warning_type.as_deref(),
        Some("upstream_response_body_error")
    );
    assert!(observed.completion().is_ok());
    Ok(())
}

#[tokio::test]
async fn codex_missing_content_type_still_accepts_terminal_transport_close() -> TestResult {
    let body = Bytes::from_static(
        b"event: response.created\n\
data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\",\"usage\":null}}\n\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"hello\"}\n\n\
event: response.completed\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null,\"usage\":{\"input_tokens\":13,\"output_tokens\":7}}}\n\n",
    );
    let observed = observe(
        Arc::new(ScriptedDispatch {
            version: http::Version::HTTP_2,
            headers: HeaderMap::new(),
            chunks: vec![body.clone()],
            trailing_error: Some(std::io::ErrorKind::ConnectionReset),
        }),
        false,
        upstream_record(
            StorageUpstreamKind::OpenaiOauth,
            "https://chatgpt.com/backend-api/",
        ),
        http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-5-codex","input":"hello","stream":true}"#,
            ))?,
    )
    .await?;

    assert_eq!(observed.body, body);
    assert_eq!(observed.status, StatusCode::OK);
    assert_eq!(observed.row.error_code, None);
    assert_eq!(observed.row.upstream_error_type, None);
    assert_eq!(
        observed.row.upstream_stream_warning_type.as_deref(),
        Some("upstream_response_body_error")
    );
    assert!(observed.completion().is_ok());
    Ok(())
}

#[tokio::test]
async fn openai_terminal_client_close_without_content_type_is_success() -> TestResult {
    let body = Bytes::from_static(
        b"event: response.created\n\
data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\",\"usage\":null}}\n\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"hello\"}\n\n\
event: response.completed\n\
data: {\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":13,\"output_tokens\":7}}}\n\n",
    );
    let observed = observe_drop_after_terminal(
        Arc::new(PendingDispatch {
            headers: HeaderMap::new(),
            body: body.clone(),
        }),
        upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
        http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-test","input":"hello","stream":true}"#,
            ))?,
        b"event: response.completed",
        None,
    )
    .await?;

    assert_eq!(observed.status, StatusCode::OK);
    assert_eq!(observed.row.error_code, None);
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (Some(13), Some(7))
    );
    Ok(())
}

#[tokio::test]
async fn non_stream_without_content_type_preserves_usage() -> TestResult {
    let body =
        Bytes::from_static(br#"{"type":"message","usage":{"input_tokens":13,"output_tokens":7}}"#);
    for content_type in [
        None,
        Some("text/json"),
        Some("application/problem+json"),
        Some("text/plain"),
    ] {
        let mut headers = HeaderMap::new();
        if let Some(value) = content_type {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(value));
        }
        let observed = observe(
            Arc::new(FixedDispatch {
                status: StatusCode::OK,
                headers,
                body: body.clone(),
            }),
            false,
            default_upstream_record(),
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","messages":[],"stream":false}"#,
            )),
        )
        .await?;
        assert_eq!(observed.body, body);
        observed.assert_success(13, 7, false);
    }
    Ok(())
}

#[tokio::test]
async fn openai_stream_json_error_is_not_sse() -> TestResult {
    let body = Bytes::from_static(
        br#"{"error":{"type":"invalid_request_error","message":"invalid input"}}"#,
    );
    let observed = observe(
        Arc::new(FixedDispatch {
            status: StatusCode::BAD_REQUEST,
            headers: json_headers(body.len(), false),
            body: body.clone(),
        }),
        true,
        upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
        http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-test","input":"hello","stream":true}"#,
            ))?,
    )
    .await?;
    assert_eq!(
        observed.body, body,
        "JSON error is relayed without synthetic SSE frames"
    );
    assert_eq!(
        observed.row.upstream_error_type.as_deref(),
        Some("invalid_request_error")
    );
    observed.assert_terminal(Some("upstream_4xx"));
    assert!(observed.completion().is_ok());
    Ok(())
}

struct WithholdTerminalTransform;

impl SseEventTransformHook for WithholdTerminalTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if request.event.event == "message_stop" {
            Ok(TransformSseEventResult::Drop)
        } else {
            Ok(TransformSseEventResult::Unchanged)
        }
    }
}

#[tokio::test]
async fn client_close_while_terminal_is_withheld_remains_cancelled() -> TestResult {
    let body = Bytes::from(format!(
        "{START}{DELTA}{STOP}event: ping\ndata: {{\"type\":\"ping\"}}\n\n"
    ));
    let observed = observe_drop_after_terminal(
        Arc::new(PendingDispatch {
            headers: sse_headers(),
            body,
        }),
        default_upstream_record(),
        messages_request(Bytes::from_static(REQUEST)),
        b"event: ping",
        Some(Arc::new(WithholdTerminalTransform)),
    )
    .await?;
    assert!(!String::from_utf8_lossy(&observed.body).contains("message_stop"));
    assert_eq!(
        observed.row.status, 499,
        "unexposed terminal event is not client completion"
    );
    assert_eq!(
        observed.row.error_code.as_deref(),
        Some("client_closed_request")
    );
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (Some(13), Some(7))
    );
    Ok(())
}

struct HeldAffinityStore {
    binding_started: tokio::sync::Notify,
}

#[async_trait]
impl cc_lb_storage_api::UpstreamAffinityStore for HeldAffinityStore {
    async fn resolve_upstream_affinities(
        &self,
        _keys: &[cc_lb_storage_api::UpstreamAffinityKey],
        _now_unix_secs: u64,
        _ttl_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<Vec<cc_lb_storage_api::UpstreamAffinityBinding>> {
        Ok(Vec::new())
    }

    async fn bind_upstream_affinities(
        &self,
        bindings: &[cc_lb_storage_api::UpstreamAffinityBinding],
        _now_unix_secs: u64,
        _ttl_secs: u64,
    ) -> cc_lb_storage_api::StorageResult<()> {
        assert!(!bindings.is_empty());
        self.binding_started.notify_one();
        std::future::pending().await
    }

    async fn purge_expired_upstream_affinities(
        &self,
        _now_unix_secs: u64,
        _ttl_secs: u64,
        _batch_size: usize,
    ) -> cc_lb_storage_api::StorageResult<u64> {
        Ok(0)
    }
}

#[tokio::test]
async fn client_close_while_terminal_chunk_awaits_affinity_is_cancelled() -> TestResult {
    let test_bus = TestLifecycleBus::new();
    let mut events = test_bus.bus.subscribe_lifecycle();
    let store = Arc::new(HeldAffinityStore {
        binding_started: tokio::sync::Notify::new(),
    });
    // A trailing state event makes the relay await persistence after parsing
    // message_stop, while all bytes from this compressed chunk remain held.
    let plaintext = Bytes::from(format!(
        "{START}{DELTA}{STOP}event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"web_search_tool_result\",\"content\":[{{\"type\":\"web_search_result\",\"encrypted_content\":\"held-state\"}}]}}}}\n\n"
    ));
    let mut headers = sse_headers();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    let lifecycle = lifecycle_with_transforms(
        None,
        None,
        Arc::new(PendingDispatch {
            headers,
            body: gzip_bytes(&plaintext),
        }),
    )
    .with_upstream_affinity_store(store.clone())
    .with_event_bus(test_bus.bus_arc());
    let response =
        handle_authenticated(&lifecycle, messages_request(Bytes::from_static(REQUEST))).await?;
    let mut body = response.into_body();
    {
        let frame = body.frame();
        tokio::pin!(frame);
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = store.binding_started.notified() => {}
                _ = &mut frame => panic!("terminal chunk must remain held before affinity binding"),
            }
        })
        .await?;
    }
    drop(body);
    let (reason, client_status) = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let LifecycleEvent::RequestTerminated {
                reason,
                client_status,
                ..
            } = events.recv().await.expect("lifecycle event delivered")
            {
                break (reason, client_status);
            }
        }
    })
    .await?;
    assert_eq!(client_status, 499);
    assert_eq!(
        reason,
        TerminationReason::ErrorCode("client_closed_request".to_owned())
    );
    Ok(())
}

#[tokio::test]
async fn openai_missing_content_type_uses_sse_hook_without_buffering() -> TestResult {
    let buffered = Arc::new(UnchangedBufferedTransform::default());
    let sse = Arc::new(UnchangedSseTransform::default());
    let terminal = Bytes::from_static(
        b"event: response.completed\ndata: {\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":13,\"output_tokens\":7}}}\n\n",
    );
    let lifecycle = lifecycle_with_record_and_config(
        Some(buffered.clone()),
        Some(sse.clone()),
        Arc::new(PendingDispatch {
            headers: HeaderMap::new(),
            body: terminal.clone(),
        }),
        LifecycleConfig::default(),
        upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
    );
    let request = http::Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(
            br#"{"model":"gpt-test","input":"hello","stream":true}"#,
        ))?;
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        handle_authenticated(&lifecycle, request),
    )
    .await
    .expect("omitted Content-Type SSE must not wait for upstream EOF")?;
    let mut body = response.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(3), body.frame())
        .await?
        .expect("terminal frame")
        .expect("terminal frame succeeds")
        .into_data()
        .expect("terminal frame is data");
    assert_eq!(frame, terminal);
    assert_eq!(sse.seen_events(), vec!["response.completed".to_owned()]);
    assert!(buffered.seen_bodies().is_empty());
    drop(body);
    Ok(())
}

struct OpenaiChatToAnthropicSseTransform;

impl SseEventTransformHook for OpenaiChatToAnthropicSseTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        let data = request.event.data.as_ref();
        if data
            .windows(b"\"finish_reason\":\"stop\"".len())
            .any(|window| window == b"\"finish_reason\":\"stop\"")
        {
            return Ok(TransformSseEventResult::Replace {
                events: vec![
                    SseEvent {
                        event: "message_delta".to_owned(),
                        data: Bytes::from_static(
                            br#"{"type":"message_delta","delta":{"stop_reason":"end_turn"}}"#,
                        ),
                    },
                    SseEvent {
                        event: "message_stop".to_owned(),
                        data: Bytes::from_static(br#"{"type":"message_stop"}"#),
                    },
                ],
            });
        }
        if data
            .windows(b"\"content\":\"hello\"".len())
            .any(|window| window == b"\"content\":\"hello\"")
        {
            return Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: "content_block_delta".to_owned(),
                    data: Bytes::from_static(
                        br#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}"#,
                    ),
                }],
            });
        }
        Ok(TransformSseEventResult::Unchanged)
    }
}

#[tokio::test]
async fn openai_chat_finish_reason_without_usage_refunds_pending_close() -> TestResult {
    assert_chat_finish_reason_without_usage_refunds_pending_close(false, false).await
}

#[tokio::test]
async fn native_openai_chat_finish_reason_without_usage_refunds_pending_close() -> TestResult {
    assert_chat_finish_reason_without_usage_refunds_pending_close(true, false).await
}

#[tokio::test]
async fn thinking_progress_without_usage_refunds_pending_close() -> TestResult {
    assert_chat_finish_reason_without_usage_refunds_pending_close(false, true).await
}

async fn assert_chat_finish_reason_without_usage_refunds_pending_close(
    native_openai: bool,
    thinking_progress: bool,
) -> TestResult {
    use cc_lb_storage_api::principal::{Limit, LimitKind};

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "openai-chat-finish.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut events = test_bus.bus.subscribe_lifecycle();
    let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(64);
    let concurrency = Arc::new(KeyConcurrencyManager::new());
    let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
    let reconcile =
        cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, engine.clone());
    struct SplitPendingDispatch {
        chunks: [Bytes; 3],
        finish_ready: Arc<tokio::sync::Notify>,
        usage_ready: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl UpstreamDispatch for SplitPendingDispatch {
        async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
            let [content, finish, usage] = self.chunks.clone();
            let finish_ready = self.finish_ready.clone();
            let usage_ready = self.usage_ready.clone();
            let stream = async_stream::stream! {
                yield Ok::<Bytes, std::convert::Infallible>(content);
                finish_ready.notified().await;
                yield Ok::<Bytes, std::convert::Infallible>(finish);
                usage_ready.notified().await;
                yield Ok::<Bytes, std::convert::Infallible>(usage);
                std::future::pending::<()>().await;
            };
            let mut response = Response::new(Body::from_stream(stream));
            *response.headers_mut() = sse_headers();
            Ok(response)
        }
    }

    let upstream_content = Bytes::from_static(
        b"data: {\"id\":\"chatcmpl-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n",
    );
    let upstream_finish = if native_openai {
        Bytes::from_static(
            b"data: {\"id\":\"chatcmpl-test\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        )
    } else {
        // The transformed /v1/messages path uses Anthropic terminal bookkeeping.
        Bytes::from_static(
            b"data: {\"id\":\"chatcmpl-test\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"type\":\"message_stop\"}\n\n",
        )
    };
    let (content, finish) = if native_openai {
        (upstream_content.clone(), upstream_finish.clone())
    } else if thinking_progress {
        (
            Bytes::from_static(
                b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"estimated_tokens\":9}}\n\n",
            ),
            Bytes::from_static(
                b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            ),
        )
    } else {
        (
            Bytes::from_static(
                b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
            ),
            Bytes::from_static(
                b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
            ),
        )
    };
    let usage = Bytes::from_static(
        b"data: {\"id\":\"chatcmpl-test\",\"choices\":[],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":7,\"total_tokens\":20}}\n\n",
    );
    let finish_ready = Arc::new(tokio::sync::Notify::new());
    let lifecycle = lifecycle_with_options(
        None,
        (!native_openai)
            .then(|| Arc::new(OpenaiChatToAnthropicSseTransform) as Arc<dyn SseEventTransformHook>),
        Arc::new(SplitPendingDispatch {
            chunks: [content.clone(), finish.clone(), usage],
            finish_ready: finish_ready.clone(),
            // Never released: usage cannot race the downstream close.
            usage_ready: Arc::new(tokio::sync::Notify::new()),
        }),
        LifecycleConfig::default(),
        upstream_record(
            if native_openai {
                StorageUpstreamKind::OpenaiApiKey
            } else {
                StorageUpstreamKind::AnthropicApiKey
            },
            "https://api.openai.com/",
        ),
        true,
        vec![
            Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 10,
            },
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 1_000_000,
            },
            Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 2,
            },
        ],
    )
    .with_static_limit_subject(
        engine.clone(),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_event_bus(test_bus.bus_arc());
    let request = if native_openai {
        let mut request = http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("x-api-key", common::managed_api_key())
            .header(CONTENT_TYPE, "application/json")
            .body(Bytes::from_static(
                br#"{"model":"gpt-test","messages":[{"role":"user","content":"hello"}],"stream":true,"stream_options":{"include_usage":true}}"#,
            ))?;
        request
            .extensions_mut()
            .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
        request
    } else {
        messages_request(Bytes::from_static(
            br#"{"model":"claude-test","messages":[{"role":"user","content":"hello"}],"stream":true}"#,
        ))
    };
    let response = handle_authenticated(&lifecycle, request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(3), body.frame())
        .await?
        .expect("content frame")
        .expect("content frame succeeds")
        .into_data()
        .expect("content frame carries data");
    assert_eq!(frame, content);
    // Only expose finish_reason after the first downstream frame was consumed.
    // This forces the lifecycle to parse it in a separate upstream chunk.
    finish_ready.notify_one();
    if native_openai {
        let frame = tokio::time::timeout(Duration::from_secs(3), body.frame())
            .await?
            .expect("finish_reason frame")
            .expect("finish_reason frame succeeds")
            .into_data()
            .expect("finish_reason frame carries data");
        assert_eq!(frame, finish);
    } else {
        let first = tokio::time::timeout(Duration::from_secs(3), body.frame())
            .await?
            .expect("message_delta frame")
            .expect("message_delta frame succeeds")
            .into_data()
            .expect("message_delta frame carries data");
        let second = tokio::time::timeout(Duration::from_secs(3), body.frame())
            .await?
            .expect("message_stop frame")
            .expect("message_stop frame succeeds")
            .into_data()
            .expect("message_stop frame carries data");
        let mut transformed_finish = Vec::with_capacity(first.len() + second.len());
        transformed_finish.extend_from_slice(&first);
        transformed_finish.extend_from_slice(&second);
        assert_eq!(transformed_finish.as_slice(), finish.as_ref());
    }
    drop(body);

    let observed_events = tokio::time::timeout(Duration::from_secs(3), async {
        let mut observed = Vec::new();
        loop {
            let event = events.recv().await.expect("lifecycle event delivered");
            let terminated = matches!(event, LifecycleEvent::RequestTerminated { .. });
            observed.push(event);
            if terminated {
                break observed;
            }
        }
    })
    .await
    .expect("request terminates after downstream close");
    let mut rows = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(rows.len(), 1);
    let row = rows.remove(0);
    assert_eq!(row.status, StatusCode::OK.as_u16());
    assert_eq!(row.error_code, None);
    assert_eq!(row.upstream_error_type, None);
    assert_eq!(row.upstream_error_message, None);
    let terminations = observed_events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::RequestTerminated {
                reason,
                client_status,
                ..
            } => Some((reason, *client_status)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        terminations,
        vec![(&TerminationReason::Success, StatusCode::OK.as_u16())]
    );
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, LifecycleEvent::UsageObserved { .. })),
        "finish_reason without upstream usage must not fabricate measured zero usage"
    );
    assert!(
        !observed_events
            .iter()
            .any(|event| matches!(event, LifecycleEvent::Priced { .. })),
        "unavailable upstream usage must not be priced"
    );

    assert_eq!(
        (row.input_tokens, row.output_tokens, row.cost_usd_micros),
        (None, None, None),
        "missing upstream usage remains unknown, not measured zero"
    );

    reconcile.shutdown().await;
    let headers = engine.headers_for("key-test", "principal-test");
    let remaining = |name: &str| -> i64 {
        headers
            .iter()
            .find(|(header, _)| header == name)
            .and_then(|(_, value)| value.parse().ok())
            .expect("remaining limit header")
    };
    assert_eq!(
        remaining("anthropic-ratelimit-requests-remaining"),
        10,
        "request reservation is refunded exactly once"
    );
    assert_eq!(
        remaining("anthropic-ratelimit-tokens-remaining"),
        1_000_000,
        "token reservation is refunded exactly once"
    );
    assert_eq!(concurrency.current("key-test"), 0);
    let reservation_id = observed_events
        .iter()
        .find_map(|event| match event {
            LifecycleEvent::LimitDecision {
                decision: cc_lb_lifecycle::LimitDecisionKind::Reserved { reservation_id, .. },
                ..
            } => Some(reservation_id.clone()),
            _ => None,
        })
        .expect("reservation event");
    assert!(
        !engine.refund_by_id(&reservation_id),
        "the reservation was already refunded"
    );
    Ok(())
}

#[tokio::test]
async fn semantic_usage_before_terminal_is_measured_on_client_close() -> TestResult {
    let body = Bytes::from(format!("{START}{CONTENT}{DELTA}{STOP}"));
    let observed = observe_drop_after_terminal(
        Arc::new(PendingDispatch {
            headers: sse_headers(),
            body: body.clone(),
        }),
        default_upstream_record(),
        messages_request(Bytes::from_static(REQUEST)),
        b"event: message_stop",
        None,
    )
    .await?;

    assert_eq!(observed.status, StatusCode::OK);
    assert_eq!(observed.body, body);
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (Some(13), Some(7))
    );
    observed.assert_terminal(None);
    let usage_events = observed
        .events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::UsageObserved { usage, .. } => Some(usage),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!usage_events.is_empty());
    assert!(
        usage_events
            .iter()
            .any(|usage| (usage.input_tokens, usage.output_tokens) == (13, 7))
    );
    Ok(())
}

#[tokio::test]
async fn semantic_explicit_zero_usage_remains_measured() -> TestResult {
    let body = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1027\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\n\
event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\n\
event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    let observed = observe_drop_after_terminal(
        Arc::new(PendingDispatch {
            headers: sse_headers(),
            body: body.clone(),
        }),
        default_upstream_record(),
        messages_request(Bytes::from_static(REQUEST)),
        b"event: message_stop",
        None,
    )
    .await?;

    assert_eq!(observed.status, StatusCode::OK);
    assert_eq!(observed.body, body);
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (Some(0), Some(0))
    );
    observed.assert_terminal(None);
    let usage_events = observed
        .events
        .iter()
        .filter_map(|event| match event {
            LifecycleEvent::UsageObserved { usage, .. } => Some(usage),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!usage_events.is_empty(), "usage observation is present");
    assert!(
        usage_events
            .iter()
            .any(|usage| (usage.input_tokens, usage.output_tokens) == (0, 0)),
        "explicit zero usage is measured"
    );
    Ok(())
}

#[tokio::test]
async fn semantic_client_close_reconciles_limits_and_preterminal_cancel_refunds() -> TestResult {
    use cc_lb_storage_api::principal::{Limit, LimitKind};

    for completed in [false, true] {
        let test_bus = TestLifecycleBus::new();
        let mut events = test_bus.bus.subscribe_lifecycle();
        let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(64);
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
        let reconcile =
            cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, engine.clone());
        let upstream = if completed {
            Bytes::from(format!("{START}{DELTA}{STOP}"))
        } else {
            Bytes::from_static(START.as_bytes())
        };
        let lifecycle = lifecycle_with_options(
            None,
            None,
            Arc::new(PendingDispatch {
                headers: sse_headers(),
                body: upstream,
            }),
            LifecycleConfig::default(),
            default_upstream_record(),
            true,
            vec![
                Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 10,
                },
                Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 60,
                    cap_micros: 1_000_000,
                },
                Limit {
                    kind: LimitKind::Concurrent,
                    window_secs: 0,
                    cap_micros: 2,
                },
            ],
        )
        .with_static_limit_subject(
            engine.clone(),
            "principal-test".to_owned(),
            "key-test".to_owned(),
            StoredApiKeyRecord {
                key_hash_b64: "key-test".to_owned(),
                status: KeyStatus::Active,
                ..StoredApiKeyRecord::default()
            },
        )
        .with_event_bus(test_bus.bus_arc());
        let response = handle_authenticated(
            &lifecycle,
            messages_request(Bytes::from_static(
                br#"{"model":"claude-test","max_tokens":16,"messages":[],"stream":true}"#,
            )),
        )
        .await?;
        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(3), body.frame())
            .await?
            .expect("upstream frame")
            .expect("upstream frame succeeds")
            .into_data()
            .expect("upstream frame carries data");
        assert_eq!(
            String::from_utf8_lossy(&frame).contains("message_stop"),
            completed
        );
        drop(body);
        let (reason, client_status) = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let LifecycleEvent::RequestTerminated {
                    reason,
                    client_status,
                    ..
                } = events.recv().await.expect("lifecycle event delivered")
                {
                    break (reason, client_status);
                }
            }
        })
        .await?;
        reconcile.shutdown().await;
        assert_eq!(client_status, if completed { 200 } else { 499 });
        assert_eq!(
            reason,
            if completed {
                TerminationReason::Success
            } else {
                TerminationReason::ErrorCode("client_closed_request".to_owned())
            }
        );
        let headers = engine.headers_for("key-test", "principal-test");
        let remaining = |name: &str| -> i64 {
            headers
                .iter()
                .find(|(header, _)| header == name)
                .and_then(|(_, value)| value.parse().ok())
                .expect("remaining limit header")
        };
        assert_eq!(
            remaining("anthropic-ratelimit-requests-remaining"),
            if completed { 9 } else { 10 }
        );
        assert_eq!(
            remaining("anthropic-ratelimit-tokens-remaining"),
            if completed { 999_980 } else { 1_000_000 }
        );
        assert_eq!(concurrency.current("key-test"), 0);
    }
    Ok(())
}

#[tokio::test]
async fn semantic_client_close_preserves_abnormal_stop() -> TestResult {
    for (stop, code) in [
        ("refusal", "upstream_refusal"),
        (
            "model_context_window_exceeded",
            "upstream_context_window_exceeded",
        ),
    ] {
        let body = Bytes::from(format!(
            "{START}event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{stop}\"}},\"usage\":{{\"output_tokens\":7}}}}\n\n{STOP}"
        ));
        let observed = observe_drop_after_terminal(
            Arc::new(PendingDispatch {
                headers: sse_headers(),
                body,
            }),
            default_upstream_record(),
            messages_request(Bytes::from_static(REQUEST)),
            b"event: message_stop",
            None,
        )
        .await?;
        observed.assert_terminal(Some(code));
        assert_eq!(observed.row.upstream_error_type.as_deref(), Some(stop));
        assert_eq!(
            (observed.row.input_tokens, observed.row.output_tokens),
            (Some(13), Some(7))
        );
    }
    Ok(())
}

struct FailOnPingTransform;

impl SseEventTransformHook for FailOnPingTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if request.event.event == "ping" {
            Err(ResponseTransformError::Runtime {
                reason: "terminal follow-up transform failure".to_owned(),
            })
        } else {
            Ok(TransformSseEventResult::Unchanged)
        }
    }
}

#[tokio::test]
async fn semantic_client_close_abnormal_stop_preserves_error_precedence() -> TestResult {
    let refusal = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\"},\"usage\":{\"output_tokens\":7}}\n\n";
    for proxy_error in [false, true] {
        let trailing = if proxy_error {
            "event: ping\ndata: {\"type\":\"ping\"}\n\n"
        } else {
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
        };
        let observed = observe_drop_after_terminal(
            Arc::new(PendingDispatch {
                headers: sse_headers(),
                body: Bytes::from(format!("{START}{refusal}{STOP}{trailing}")),
            }),
            default_upstream_record(),
            messages_request(Bytes::from_static(REQUEST)),
            b"event: error",
            proxy_error.then(|| Arc::new(FailOnPingTransform) as Arc<dyn SseEventTransformHook>),
        )
        .await?;
        observed.assert_terminal(Some("upstream_stream_error"));
        assert_ne!(observed.row.upstream_error_type.as_deref(), Some("refusal"));
        if !proxy_error {
            assert_eq!(
                observed.row.upstream_error_type.as_deref(),
                Some("overloaded_error")
            );
        }
    }
    Ok(())
}
