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
    let hook = transform
        .then(|| Arc::new(UnchangedSseTransform::default()) as Arc<dyn SseEventTransformHook>);
    observe_with_sse_hook(dispatcher, hook, record, request).await
}

async fn observe_with_sse_hook(
    dispatcher: Arc<dyn UpstreamDispatch>,
    hook: Option<Arc<dyn SseEventTransformHook>>,
    record: UpstreamRecord,
    request: http::Request<Bytes>,
) -> TestResult<Observation> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "semantic-sse-eof.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut rx = test_bus.bus.subscribe_lifecycle();
    let lifecycle = with_test_limit_subject(lifecycle_with_record_and_config(
        None,
        hook,
        dispatcher,
        LifecycleConfig::default(),
        record,
    ))
    .with_upstream_affinity_store(Arc::new(
        crate::upstream_affinity::MemoryAffinityStore::default(),
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

struct TerminalThenPendingDispatch {
    headers: HeaderMap,
    chunks: Vec<Bytes>,
}

#[async_trait]
impl UpstreamDispatch for TerminalThenPendingDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let chunks = self.chunks.clone();
        let stream = async_stream::stream! {
            for chunk in chunks {
                yield Ok::<Bytes, std::convert::Infallible>(chunk);
            }
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

const RESPONSES_CREATED: &str = "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_eof\",\"object\":\"response\",\"status\":\"in_progress\",\"usage\":null}}\n\n";
const RESPONSES_DELTA: &str = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":7,\"delta\":\"private-partial-output\"}\n\n";
const RESPONSES_ITEM_DONE: &str = "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":8,\"output_index\":0,\"item\":{\"id\":\"msg_eof\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[]}}\n\n";
const RESPONSES_USAGE: &str = "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":9,\"response\":{\"status\":\"in_progress\",\"usage\":{\"input_tokens\":13,\"output_tokens\":7,\"total_tokens\":20}}}\n\n";

fn responses_eof_request(path: &str) -> TestResult<http::Request<Bytes>> {
    let mut request = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(
            br#"{"model":"gpt-test","input":"hello","max_output_tokens":16,"stream":true}"#,
        ))?;
    request
        .extensions_mut()
        .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
    Ok(request)
}

fn responses_eof_record(oauth: bool) -> UpstreamRecord {
    if oauth {
        upstream_record(
            StorageUpstreamKind::OpenaiOauth,
            "https://chatgpt.com/backend-api/",
        )
    } else {
        upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/")
    }
}

fn assert_responses_eof_frame(
    body: &[u8],
    prefix: &[u8],
    response_id: Option<&str>,
    sequence_number: u64,
) {
    assert!(body.starts_with(prefix), "upstream progress is preserved");
    let tail = std::str::from_utf8(&body[prefix.len()..]).expect("native failure is UTF-8");
    assert_eq!(
        tail.lines()
            .filter_map(|line| line.strip_prefix("event:"))
            .map(str::trim)
            .collect::<Vec<_>>(),
        ["response.failed"],
        "one native terminal event and no Anthropic wrapper"
    );
    let data = tail
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .collect::<Vec<_>>();
    assert_eq!(data.len(), 1, "exactly one appended failure payload");
    let failure: serde_json::Value = serde_json::from_str(data[0]).expect("native failure JSON");
    assert_eq!(failure["type"], "response.failed");
    assert_eq!(failure["sequence_number"].as_u64(), Some(sequence_number));
    assert_eq!(
        failure["response"]["id"].as_str(),
        response_id,
        "reuse only a bounded observed identity"
    );
    assert_eq!(failure["response"]["object"], "response");
    assert_eq!(failure["response"]["status"], "failed");
    assert_eq!(failure["response"]["error"]["code"], "api_error");
    let message = failure["response"]["error"]["message"]
        .as_str()
        .expect("safe public error message");
    assert!(!message.is_empty());
    assert!(!message.contains("private-partial-output"));
    assert!(!message.contains("gpt-test"));
    assert_eq!(failure["response"]["usage"], serde_json::Value::Null);
    assert_eq!(failure["response"]["output"], serde_json::json!([]));
    assert!(tail.ends_with("\n\n"), "complete SSE frame");
}

#[tokio::test]
async fn responses_semantic_sse_eof_missing_terminal_matrix() -> TestResult {
    let progress = [
        String::new(),
        RESPONSES_CREATED.to_owned(),
        format!("{RESPONSES_CREATED}{RESPONSES_DELTA}"),
        format!("{RESPONSES_CREATED}{RESPONSES_DELTA}{RESPONSES_ITEM_DONE}\r\n\n"),
    ];
    // Eight cases cover each progress state with and without an unchanged
    // transform; rotate credentials, encodings, and transport boundaries.
    for (state, plaintext) in progress.iter().enumerate() {
        for transform in [false, true] {
            let case = state * 2 + usize::from(transform);
            let plaintext = Bytes::copy_from_slice(plaintext.as_bytes());
            let encoding = ["identity", "gzip", "br"][case % 3];
            let wire = match encoding {
                "gzip" => gzip_bytes(&plaintext),
                "br" => brotli_bytes(&plaintext),
                _ => plaintext.clone(),
            };
            let mut headers = sse_headers();
            if encoding != "identity" {
                headers.insert(CONTENT_ENCODING, HeaderValue::from_static(encoding));
            }
            let chunk_size = match case % 3 {
                0 => wire.len().max(1),
                1 => 1,
                _ => 7,
            };
            let observed = observe(
                Arc::new(ScriptedDispatch {
                    version: http::Version::HTTP_2,
                    headers,
                    chunks: wire
                        .chunks(chunk_size)
                        .map(Bytes::copy_from_slice)
                        .collect(),
                    trailing_error: None,
                }),
                transform,
                responses_eof_record(matches!(case, 1 | 2 | 5 | 6)),
                responses_eof_request("/v1/responses")?,
            )
            .await?;
            observed.assert_incomplete_outcome();
            assert_responses_eof_frame(
                &observed.body,
                if state == 3 {
                    &plaintext[..plaintext.len() - 3]
                } else {
                    &plaintext
                },
                (state > 0).then_some("resp_eof"),
                [0, 1, 8, 9][state],
            );
            assert_eq!(
                (
                    observed.row.input_tokens,
                    observed.row.output_tokens,
                    observed.row.cost_usd_micros,
                ),
                (None, None, None),
                "missing usage remains unknown in case {case}"
            );
            assert!(observed.events.iter().all(|event| !matches!(
                event,
                LifecycleEvent::UsageObserved { .. } | LifecycleEvent::Priced { .. }
            )));
            assert!(observed.headers.get(CONTENT_ENCODING).is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn responses_semantic_sse_eof_preserves_bounded_identity_and_sequence() -> TestResult {
    for (id, expected_id, sequence) in [
        ("r".repeat(512), true, 7),
        ("r".repeat(513), false, u64::MAX),
        (String::new(), false, 0),
    ] {
        let prefix = Bytes::from(format!(
            "event: response.created\ndata: {}\n\n",
            serde_json::json!({
                "type": "response.created",
                "sequence_number": sequence,
                "response": {"id": id, "status": "in_progress", "usage": null},
            })
        ));
        let observed = observe(
            Arc::new(ScriptedDispatch::identity_sse(vec![prefix.clone()], None)),
            false,
            responses_eof_record(false),
            responses_eof_request("/v1/responses")?,
        )
        .await?;
        observed.assert_incomplete_outcome();
        assert_responses_eof_frame(
            &observed.body,
            &prefix,
            expected_id.then_some(id.as_str()),
            sequence.saturating_add(1),
        );
    }
    Ok(())
}

#[tokio::test]
async fn responses_semantic_sse_eof_compact_retains_provider_usage() -> TestResult {
    let prefix = Bytes::from(format!("{RESPONSES_CREATED}{RESPONSES_USAGE}"));
    let observed = observe(
        Arc::new(ScriptedDispatch::identity_sse(vec![prefix.clone()], None)),
        true,
        responses_eof_record(false),
        responses_eof_request("/v1/responses/compact")?,
    )
    .await?;
    observed.assert_incomplete(13, 7);
    assert_responses_eof_frame(&observed.body, &prefix, Some("resp_eof"), 10);
    Ok(())
}

#[tokio::test]
async fn responses_semantic_sse_eof_terminal_controls() -> TestResult {
    for terminal in ["completed", "incomplete"] {
        for type_only in [false, true] {
            let event = if type_only {
                "message".to_owned()
            } else {
                format!("response.{terminal}")
            };
            let mut value = serde_json::json!({
                "sequence_number": 8,
                "response": {
                    "status": terminal,
                    "usage": {"input_tokens": 13, "output_tokens": 7, "total_tokens": 20},
                },
            });
            if type_only {
                value["type"] = serde_json::json!(format!("response.{terminal}"));
            }
            let prefix = Bytes::from(format!(
                "{RESPONSES_CREATED}{RESPONSES_DELTA}event: {event}\ndata: {value}\n\n"
            ));
            let observed = observe(
                Arc::new(ScriptedDispatch::identity_sse(vec![prefix.clone()], None)),
                type_only,
                responses_eof_record(type_only),
                responses_eof_request("/v1/responses")?,
            )
            .await?;
            assert_eq!(observed.body, prefix, "no extra event after {terminal}");
            observed.assert_success(13, 7, false);
        }
    }
    Ok(())
}

struct ResponsesNativeFailureTransform;

impl SseEventTransformHook for ResponsesNativeFailureTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if request.event.event == "response.output_text.delta" {
            Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: "response.failed".to_owned(),
                    data: Bytes::from_static(
                        br#"{"type":"response.failed","sequence_number":8,"response":{"status":"failed","error":{"code":"server_error","message":"transformed failure"}}}"#,
                    ),
                }],
            })
        } else {
            Ok(TransformSseEventResult::Unchanged)
        }
    }
}

#[tokio::test]
async fn responses_semantic_sse_eof_forwarded_transform_failure_is_not_duplicated() -> TestResult {
    let observed = observe_with_sse_hook(
        Arc::new(ScriptedDispatch::identity_sse(
            vec![Bytes::from(format!("{RESPONSES_CREATED}{RESPONSES_DELTA}"))],
            None,
        )),
        Some(Arc::new(ResponsesNativeFailureTransform)),
        responses_eof_record(false),
        responses_eof_request("/v1/responses")?,
    )
    .await?;
    let events = std::str::from_utf8(&observed.body)?
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let failures = events
        .iter()
        .filter(|event| event["type"] == "response.failed")
        .collect::<Vec<_>>();
    assert_eq!(failures.len(), 1, "forward native transform failure once");
    assert_eq!(failures[0]["response"]["error"]["code"], "server_error");
    assert_eq!(
        failures[0]["response"]["error"]["message"],
        "transformed failure"
    );
    assert_ne!(
        observed.row.upstream_error_type.as_deref(),
        Some("upstream_response_incomplete")
    );
    assert!(
        observed.completion().is_ok(),
        "retain existing transform outcome"
    );
    observed.assert_terminal(None);
    Ok(())
}

struct DropResponsesTerminalTransform;

impl SseEventTransformHook for DropResponsesTerminalTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if matches!(
            request.event.event.as_str(),
            "response.completed" | "response.incomplete"
        ) {
            Ok(TransformSseEventResult::Drop)
        } else {
            Ok(TransformSseEventResult::Unchanged)
        }
    }
}

#[tokio::test]
async fn responses_semantic_sse_eof_dropped_terminal_becomes_native_failure() -> TestResult {
    for (event, status) in [
        ("response.completed", "completed"),
        ("response.incomplete", "incomplete"),
    ] {
        let terminal = format!(
            "event: {event}\ndata: {{\"type\":\"{event}\",\"sequence_number\":10,\"response\":{{\"status\":\"{status}\",\"usage\":{{\"input_tokens\":13,\"output_tokens\":7,\"total_tokens\":20}}}}}}\n\n"
        );
        let observed = observe_with_sse_hook(
            Arc::new(ScriptedDispatch::identity_sse(
                vec![Bytes::from(format!(
                    "{RESPONSES_CREATED}{RESPONSES_DELTA}{RESPONSES_USAGE}{terminal}"
                ))],
                None,
            )),
            Some(Arc::new(DropResponsesTerminalTransform)),
            responses_eof_record(false),
            responses_eof_request("/v1/responses")?,
        )
        .await?;

        observed.assert_incomplete_outcome();
        let body = std::str::from_utf8(&observed.body)?;
        let failures = body
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .filter_map(|data| serde_json::from_str::<serde_json::Value>(data).ok())
            .filter(|data| data["type"] == "response.failed")
            .collect::<Vec<_>>();
        assert_eq!(
            failures.len(),
            1,
            "clean EOF appends one native failure after dropped {event}"
        );
        assert_eq!(failures[0]["response"]["status"], "failed");
        assert!(
            !body.contains(event),
            "dropped upstream terminal is not forwarded: {event}"
        );
        assert!(
            !body.contains("event: response.completed"),
            "dropped terminal must not become a successful completion"
        );
    }
    Ok(())
}

struct ReplaceResponsesDeltaWithTerminal {
    event: &'static str,
    status: &'static str,
}

impl SseEventTransformHook for ReplaceResponsesDeltaWithTerminal {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        if request.event.event == "response.output_text.delta" {
            Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: self.event.to_owned(),
                    data: Bytes::from(format!(
                        r#"{{"type":"{}","sequence_number":10,"response":{{"status":"{}","usage":{{"input_tokens":13,"output_tokens":7,"total_tokens":20}}}}}}"#,
                        self.event, self.status
                    )),
                }],
            })
        } else {
            Ok(TransformSseEventResult::Unchanged)
        }
    }
}

#[tokio::test]
async fn responses_semantic_sse_eof_replaced_delta_is_terminal_success() -> TestResult {
    for (event, status) in [
        ("response.completed", "completed"),
        ("response.incomplete", "incomplete"),
    ] {
        let observed = observe_with_sse_hook(
            Arc::new(ScriptedDispatch::identity_sse(
                vec![Bytes::from(format!(
                    "{RESPONSES_CREATED}{RESPONSES_USAGE}{RESPONSES_DELTA}"
                ))],
                None,
            )),
            Some(Arc::new(ReplaceResponsesDeltaWithTerminal {
                event,
                status,
            })),
            responses_eof_record(false),
            responses_eof_request("/v1/responses")?,
        )
        .await?;

        observed.assert_success(13, 7, false);
        let body = std::str::from_utf8(&observed.body)?;
        assert!(
            body.contains(&format!("event: {event}\n")),
            "transformed terminal is forwarded: {event}"
        );
        assert_eq!(
            body.matches("event: response.failed").count(),
            0,
            "transformed terminal must not receive a synthetic failure"
        );
        assert_eq!(
            observed.row.upstream_error_type, None,
            "transformed {event} remains successful"
        );
    }
    Ok(())
}

fn responses_terminal_faults(
    plaintext: &Bytes,
) -> [(&'static str, &'static str, ScriptedDispatch); 3] {
    let compressed = gzip_bytes(plaintext);
    [
        (
            "upstream_response_body_error",
            "transport_error",
            ScriptedDispatch::identity_sse(
                vec![plaintext.clone()],
                Some(std::io::ErrorKind::ConnectionReset),
            ),
        ),
        (
            "upstream_response_decode_error",
            "decode_error_mid_body",
            // Sync-flush all terminal-containing plaintext before a separate
            // DEFLATE block with reserved BTYPE=3 makes decoder.push fail.
            ScriptedDispatch::gzip_sse(
                vec![
                    incomplete_gzip_prefix(plaintext),
                    Bytes::from_static(b"\x07"),
                ],
                None,
            ),
        ),
        (
            "upstream_response_decode_error",
            "decode_error_after_clean_end",
            // Keep the complete DEFLATE payload so every SSE event is emitted;
            // only the CRC32/ISIZE trailer is missing when decoder.finish runs.
            ScriptedDispatch::gzip_sse(vec![compressed.slice(..compressed.len() - 8)], None),
        ),
    ]
}

#[tokio::test]
async fn responses_semantic_sse_eof_dropped_terminal_later_fault_stays_fatal() -> TestResult {
    for (event, status) in [
        ("response.completed", "completed"),
        ("response.incomplete", "incomplete"),
    ] {
        let prefix = Bytes::from(format!("{RESPONSES_CREATED}{RESPONSES_DELTA}"));
        // Usage occurs only in the dropped upstream terminal. Persisting it and
        // advancing the failure sequence prove the terminal decoded before fault.
        let plaintext = Bytes::from(format!(
            "{RESPONSES_CREATED}{RESPONSES_DELTA}event: {event}\ndata: {{\"type\":\"{event}\",\"sequence_number\":10,\"response\":{{\"status\":\"{status}\",\"usage\":{{\"input_tokens\":13,\"output_tokens\":7,\"total_tokens\":20}}}}}}\n\n"
        ));
        for (error_type, body_end, dispatcher) in responses_terminal_faults(&plaintext) {
            let observed = observe_with_sse_hook(
                Arc::new(dispatcher),
                Some(Arc::new(DropResponsesTerminalTransform)),
                responses_eof_record(false),
                responses_eof_request("/v1/responses")?,
            )
            .await?;

            assert_eq!(observed.status, StatusCode::OK);
            let error = observed
                .completion()
                .as_ref()
                .expect_err("a dropped terminal cannot make a later fault acceptable");
            assert_eq!(error.error_type, error_type, "{event}: {body_end}");
            observed.assert_terminal(Some("upstream_stream_error"));
            assert_eq!(
                observed.row.upstream_error_type.as_deref(),
                Some(error_type),
                "{event}: {body_end}"
            );
            assert_eq!(
                observed.row.upstream_error_message.as_deref(),
                Some(error.error_message.as_str())
            );
            assert_eq!(
                observed.row.upstream_body_end.as_deref(),
                Some(body_end),
                "{event}: {body_end}"
            );
            assert_eq!(
                (observed.row.input_tokens, observed.row.output_tokens),
                (Some(13), Some(7)),
                "retain upstream terminal usage even when dropped: {body_end}"
            );
            assert_responses_eof_frame(&observed.body, &prefix, Some("resp_eof"), 11);
            let body = std::str::from_utf8(&observed.body)?;
            assert!(!body.contains(event), "never forward dropped {event}");
            assert!(
                !body.contains("event: response.completed"),
                "a later fault must not become a synthetic success"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn responses_semantic_sse_eof_replaced_terminal_later_fault_is_warning() -> TestResult {
    for (event, status) in [
        ("response.completed", "completed"),
        ("response.incomplete", "incomplete"),
    ] {
        for measured in [false, true] {
            let usage = if measured { RESPONSES_USAGE } else { "" };
            let plaintext = Bytes::from(format!("{RESPONSES_CREATED}{usage}{RESPONSES_DELTA}"));
            for (warning_type, body_end, dispatcher) in responses_terminal_faults(&plaintext) {
                let observed = observe_with_sse_hook(
                    Arc::new(dispatcher),
                    Some(Arc::new(ReplaceResponsesDeltaWithTerminal {
                        event,
                        status,
                    })),
                    responses_eof_record(false),
                    responses_eof_request("/v1/responses")?,
                )
                .await?;

                assert_eq!(observed.status, StatusCode::OK);
                let success = observed
                    .completion()
                    .as_ref()
                    .expect("an emitted replacement terminal makes the later fault a warning");
                assert_eq!(observed.row.error_code, None, "{event}: {body_end}");
                assert_eq!(observed.row.upstream_error_type, None);
                assert_eq!(observed.row.upstream_error_message, None);
                assert_eq!(
                    observed.row.upstream_stream_warning_type.as_deref(),
                    Some(warning_type),
                    "{event}: {body_end}"
                );
                assert!(
                    observed
                        .row
                        .upstream_stream_warning_message
                        .as_deref()
                        .is_some_and(|message| !message.is_empty())
                );
                assert_eq!(
                    observed.row.upstream_body_end.as_deref(),
                    Some(body_end),
                    "{event}: {body_end}"
                );
                let terminations = observed
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
                assert_eq!(terminations.len(), 1);
                assert_eq!(terminations[0].0, &TerminationReason::Success);
                assert_eq!(*terminations[0].1, StatusCode::OK.as_u16());
                assert_eq!(observed.row.status, StatusCode::OK.as_u16());
                let expected_usage = if measured {
                    (Some(13), Some(7))
                } else {
                    (None, None)
                };
                assert_eq!(
                    (observed.row.input_tokens, observed.row.output_tokens),
                    expected_usage,
                    "replacement terminal usage must not become provider usage: {body_end}"
                );
                assert_eq!(
                    (success.usage.input_tokens, success.usage.output_tokens),
                    if measured { (13, 7) } else { (0, 0) }
                );
                if !measured {
                    assert_eq!(observed.row.cost_usd_micros, None);
                    assert!(observed.events.iter().all(|event| !matches!(
                        event,
                        LifecycleEvent::UsageObserved { .. } | LifecycleEvent::Priced { .. }
                    )));
                }
                assert_eq!(success.stream_message_stop_ms, None);
                assert_eq!(observed.row.stream_message_stop_ms, None);
                let body = std::str::from_utf8(&observed.body)?;
                assert_eq!(
                    body.matches(&format!("event: {event}\n")).count(),
                    1,
                    "forward one replacement terminal before {body_end}"
                );
                assert!(!body.contains("event: response.output_text.delta"));
                assert!(!body.contains("event: response.failed"));
                assert!(!body.contains("event: error"));
                assert!(body.ends_with("\n\n"));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn responses_semantic_sse_eof_out_of_scope_controls() -> TestResult {
    let json = Bytes::from_static(br#"{"object":"response","status":"in_progress","usage":null}"#);
    let stacked = brotli_bytes(&gzip_bytes(&Bytes::from_static(
        RESPONSES_CREATED.as_bytes(),
    )));
    let mut stacked_headers = sse_headers();
    stacked_headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip, br"));
    for (record, path, request_body, body, headers) in [
        (
            upstream_record(
                StorageUpstreamKind::GeminiApiKey,
                "https://generativelanguage.googleapis.com/",
            ),
            "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
            br#"{"contents":[]}"#.as_slice(),
            Bytes::new(),
            sse_headers(),
        ),
        (
            responses_eof_record(false),
            "/v1/responses",
            br#"{"model":"gpt-test","input":"hello","stream":true}"#.as_slice(),
            json.clone(),
            json_headers(json.len(), false),
        ),
    ] {
        let mut request = http::Request::builder()
            .method("POST")
            .uri(path)
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::copy_from_slice(request_body))?;
        if path.starts_with("/v1/") {
            request
                .extensions_mut()
                .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
        }
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
        assert_eq!(observed.body, body, "{path} policy is unchanged");
        observed.assert_terminal(None);
        assert!(observed.completion().is_ok());
        assert_eq!(observed.row.upstream_error_type, None);
    }
    let observed = observe(
        Arc::new(FixedDispatch {
            status: StatusCode::OK,
            headers: stacked_headers,
            body: stacked.clone(),
        }),
        false,
        responses_eof_record(false),
        responses_eof_request("/v1/responses")?,
    )
    .await?;
    assert_eq!(observed.status, StatusCode::OK);
    assert_eq!(
        observed.headers.get(CONTENT_ENCODING),
        Some(&HeaderValue::from_static("gzip, br"))
    );
    assert_responses_eof_frame(&observed.body, b"", None, 0);
    let error = observed
        .completion()
        .as_ref()
        .expect_err("unsupported stacked encoding uses the existing safe failure path");
    assert_eq!(error.error_type, "upstream_response_decode_error");
    assert_eq!(
        error.error_message,
        "upstream SSE response could not be safely processed"
    );
    observed.assert_terminal(Some("upstream_stream_error"));
    assert_eq!(
        observed.row.upstream_error_type.as_deref(),
        Some("upstream_response_decode_error")
    );
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (None, None)
    );
    Ok(())
}

#[tokio::test]
async fn responses_semantic_sse_eof_failure_refunds_limits_with_or_without_usage() -> TestResult {
    use cc_lb_storage_api::principal::{Limit, LimitKind};

    for measured in [false, true] {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(sqlite_storage(&dir, "responses-eof-limits.sqlite").await?);
        let test_bus =
            TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
        let mut rx = test_bus.bus.subscribe_lifecycle();
        let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(64);
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
        let reconcile =
            cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, engine.clone());
        let prefix = Bytes::from(format!(
            "{RESPONSES_CREATED}{RESPONSES_DELTA}{}",
            if measured { RESPONSES_USAGE } else { "" }
        ));
        let lifecycle = lifecycle_with_options(
            None,
            None,
            Arc::new(ScriptedDispatch::identity_sse(vec![prefix.clone()], None)),
            LifecycleConfig::default(),
            responses_eof_record(measured),
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
        .with_upstream_affinity_store(Arc::new(
            crate::upstream_affinity::MemoryAffinityStore::default(),
        ))
        .with_event_bus(test_bus.bus_arc());
        let response =
            handle_authenticated(&lifecycle, responses_eof_request("/v1/responses")?).await?;
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
        .await?;
        reconcile.shutdown().await;
        let mut rows = wait_for_events(storage.as_ref(), 1).await?;
        assert_eq!(rows.len(), 1);
        let observed = Observation {
            status,
            headers,
            body,
            row: rows.remove(0),
            events,
        };
        observed.assert_incomplete_outcome();
        assert_responses_eof_frame(
            &observed.body,
            &prefix,
            Some("resp_eof"),
            if measured { 10 } else { 8 },
        );
        if measured {
            observed.assert_incomplete(13, 7);
        } else {
            assert_eq!(
                (
                    observed.row.input_tokens,
                    observed.row.output_tokens,
                    observed.row.cost_usd_micros,
                ),
                (None, None, None)
            );
            assert!(observed.events.iter().all(|event| !matches!(
                event,
                LifecycleEvent::UsageObserved { .. } | LifecycleEvent::Priced { .. }
            )));
        }
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
            "incomplete failure retains the existing full request refund policy"
        );
        assert_eq!(
            remaining("anthropic-ratelimit-tokens-remaining"),
            1_000_000,
            "measured usage is retained without changing failure refund policy"
        );
        assert_eq!(concurrency.current("key-test"), 0);
        let reservation_id = observed
            .events
            .iter()
            .find_map(|event| match event {
                LifecycleEvent::LimitDecision {
                    decision: cc_lb_lifecycle::LimitDecisionKind::Reserved { reservation_id, .. },
                    ..
                } => Some(reservation_id),
                _ => None,
            })
            .expect("request had a limit reservation");
        assert!(
            !engine.refund_by_id(reservation_id),
            "the failed reservation is already settled exactly once"
        );
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
async fn openai_incomplete_terminal_client_close_preserves_usage() -> TestResult {
    for transform in [false, true] {
        let first = Bytes::from_static(
            b"event: response.created\n\
data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\",\"usage\":null}}\n\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"hello\"}\n\n",
        );
        let terminal = Bytes::from_static(
            b"event: response.incomplete\n\
data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7,\"total_tokens\":19}}}\n\n",
        );
        let observed = observe_drop_after_terminal(
            Arc::new(TerminalThenPendingDispatch {
                headers: HeaderMap::new(),
                chunks: vec![first, terminal],
            }),
            upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
            http::Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("x-api-key", common::managed_api_key())
                .body(Bytes::from_static(
                    br#"{"model":"gpt-test","input":"hello","stream":true}"#,
                ))?,
            b"event: response.incomplete",
            transform.then(|| {
                Arc::new(UnchangedSseTransform::default()) as Arc<dyn SseEventTransformHook>
            }),
        )
        .await?;

        observed.assert_semantic_success(12, 7, false);
    }
    Ok(())
}

#[tokio::test]
async fn openai_responses_preterminal_client_close_refunds_without_usage() -> TestResult {
    let first = Bytes::from_static(
        b"event: response.created\n\
data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\",\"usage\":null}}\n\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
    );
    let observed = observe_drop_after_terminal(
        Arc::new(PendingDispatch {
            headers: sse_headers(),
            body: first,
        }),
        upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
        http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-test","input":"hello","stream":true}"#,
            ))?,
        b"event: response.output_text.delta",
        None,
    )
    .await?;
    assert_eq!(observed.row.status, 499);
    assert_eq!(
        observed.row.error_code.as_deref(),
        Some("client_closed_request")
    );
    assert!(
        observed.events.iter().all(|event| !matches!(
            event,
            LifecycleEvent::UsageObserved { usage, .. }
                if (usage.input_tokens, usage.output_tokens) == (12, 7)
        )),
        "pre-terminal close must not observe terminal usage from the undispatched terminal"
    );
    assert!(
        observed
            .events
            .iter()
            .all(|event| !matches!(event, LifecycleEvent::StreamCompleted { .. })),
        "pre-terminal close must not settle the stream"
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

    async fn read_upstream_owner_epoch(
        &self,
        _upstream_id: cc_lb_storage_api::UpstreamRecordId,
        _expected_spec_revision: u64,
    ) -> cc_lb_storage_api::StorageResult<Option<u64>> {
        Ok(Some(0))
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
async fn openai_responses_incomplete_terminal_close_reconciles_limits() -> TestResult {
    use cc_lb_storage_api::principal::{Limit, LimitKind};

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir, "responses-incomplete-limits.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let mut events = test_bus.bus.subscribe_lifecycle();
    let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(64);
    let concurrency = Arc::new(KeyConcurrencyManager::new());
    let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
    let reconcile =
        cc_lb_engine::spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, engine.clone());
    let upstream = Bytes::from_static(
        b"event: response.created\n\
data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\",\"usage\":null}}\n\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n\
event: response.incomplete\n\
data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"usage\":{\"input_tokens\":12,\"output_tokens\":7,\"total_tokens\":19}}}\n\n",
    );
    let lifecycle = lifecycle_with_options(
        None,
        None,
        Arc::new(TerminalThenPendingDispatch {
            headers: sse_headers(),
            chunks: vec![
                Bytes::from_static(
                    b"event: response.created\n\
data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\",\"usage\":null}}\n\n\
event: response.output_text.delta\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
                ),
                upstream,
            ],
        }),
        LifecycleConfig::default(),
        upstream_record(StorageUpstreamKind::OpenaiApiKey, "https://api.openai.com/"),
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
        http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("x-api-key", common::managed_api_key())
            .body(Bytes::from_static(
                br#"{"model":"gpt-test","input":"hello","stream":true}"#,
            ))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let mut received = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), body.frame())
            .await?
            .expect("upstream frame")
            .expect("upstream frame succeeds")
            .into_data()
            .expect("upstream frame carries data");
        received.extend_from_slice(&frame);
        if received
            .windows(b"response.incomplete".len())
            .any(|window| window == b"response.incomplete")
        {
            break;
        }
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
    reconcile.shutdown().await;
    assert_eq!(client_status, 200);
    assert_eq!(reason, TerminationReason::Success);

    let mut rows = wait_for_events(storage.as_ref(), 1).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].input_tokens, rows[0].output_tokens),
        (Some(12), Some(7))
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
        9,
        "terminal incomplete response consumes its request reservation"
    );
    assert_eq!(
        remaining("anthropic-ratelimit-tokens-remaining"),
        999_981,
        "terminal incomplete response settles measured 12+7 tokens"
    );
    assert_eq!(concurrency.current("key-test"), 0);
    rows.clear();
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

const CHAT_PARTIAL: &str = "data: {\"id\":\"chatcmpl-terminal\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n";
const CHAT_FINISH: &str = "data: {\"id\":\"chatcmpl-terminal\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
const CHAT_USAGE: &str = "data: {\"id\":\"chatcmpl-terminal\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":7,\"total_tokens\":20}}\n\n";
const CHAT_DONE: &str = "data: [DONE]\n\n";

fn chat_terminal_request() -> TestResult<http::Request<Bytes>> {
    let mut request = http::Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("x-api-key", common::managed_api_key())
        .body(Bytes::from_static(
            br#"{"model":"gpt-test","messages":[],"stream":true,"stream_options":{"include_usage":true}}"#,
        ))?;
    request
        .extensions_mut()
        .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
    Ok(request)
}

async fn observe_chat_terminal(
    dispatcher: ScriptedDispatch,
    transform: bool,
) -> TestResult<Observation> {
    observe(
        Arc::new(dispatcher),
        transform,
        responses_eof_record(false),
        chat_terminal_request()?,
    )
    .await
}

fn assert_chat_unknown_usage(observed: &Observation) {
    assert_eq!(
        (
            observed.row.input_tokens,
            observed.row.output_tokens,
            observed.row.cost_usd_micros,
        ),
        (None, None, None)
    );
    assert!(observed.events.iter().all(|event| !matches!(
        event,
        LifecycleEvent::UsageObserved { .. } | LifecycleEvent::Priced { .. }
    )));
}

fn chat_json_events(body: &[u8]) -> Vec<serde_json::Value> {
    std::str::from_utf8(body)
        .expect("Chat SSE is UTF-8")
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter(|data| data.trim() != "[DONE]")
        .map(|data| serde_json::from_str(data).expect("Chat data is JSON"))
        .collect()
}

fn assert_chat_native_incomplete(observed: &Observation, prefix: &[u8]) {
    observed.assert_incomplete_outcome();
    assert!(
        observed.body.starts_with(prefix),
        "preserve streamed content"
    );
    let events = chat_json_events(&observed.body);
    let errors = events
        .iter()
        .filter_map(|event| event.get("error"))
        .collect::<Vec<_>>();
    assert_eq!(errors.len(), 1, "one native Chat error frame");
    assert_eq!(errors[0]["type"], "api_error");
    assert_eq!(errors[0]["code"], "api_error");
    assert!(errors[0]["param"].is_null());
    assert!(events.iter().all(|event| event.get("type").is_none()));
    assert!(errors[0]["message"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(!events.iter().any(|event| event.get("usage").is_some()));
    assert!(
        !std::str::from_utf8(&observed.body)
            .unwrap()
            .lines()
            .any(|line| line == "data: [DONE]")
    );
    assert!(observed.body.ends_with(b"\n\n"));
    assert_chat_unknown_usage(observed);
}

fn assert_chat_terminal_success(
    observed: &Observation,
    warning_type: Option<&str>,
    measured: bool,
) {
    assert_eq!(observed.status, StatusCode::OK);
    let success = observed
        .completion()
        .as_ref()
        .expect("Chat terminal succeeds");
    let terminations = observed
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
    assert_eq!(terminations.len(), 1);
    assert_eq!(terminations[0].0, &TerminationReason::Success);
    assert_eq!(*terminations[0].1, 200);
    assert_eq!(observed.row.status, 200);
    assert_eq!(observed.row.error_code, None);
    assert_eq!(observed.row.upstream_error_type, None);
    assert_eq!(observed.row.upstream_error_message, None);
    assert_eq!(
        observed.row.upstream_stream_warning_type.as_deref(),
        warning_type
    );
    assert_eq!(
        observed
            .row
            .upstream_stream_warning_message
            .as_deref()
            .is_some_and(|message| !message.is_empty()),
        warning_type.is_some()
    );
    if measured {
        assert_eq!(
            (observed.row.input_tokens, observed.row.output_tokens),
            (Some(13), Some(7))
        );
        assert_eq!(
            (success.usage.input_tokens, success.usage.output_tokens),
            (13, 7)
        );
    } else {
        assert_chat_unknown_usage(observed);
    }
    assert!(
        chat_json_events(&observed.body)
            .iter()
            .all(|event| event.get("error").is_none())
    );
}

#[tokio::test]
async fn chat_terminal_empty_and_partial_eof_emit_native_incomplete() -> TestResult {
    // QA-02: two progress states and both identity emission paths, not a
    // Cartesian product of providers, encodings, and chunk sizes.
    for (body, transform) in [("", false), (CHAT_PARTIAL, false), (CHAT_PARTIAL, true)] {
        let observed = observe_chat_terminal(
            ScriptedDispatch::identity_sse(vec![Bytes::copy_from_slice(body.as_bytes())], None),
            transform,
        )
        .await?;
        assert_chat_native_incomplete(&observed, body.as_bytes());
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_incomplete_error_immediate_client_close_keeps_failure() -> TestResult {
    // QA-03: drop as soon as the consumer receives the complete native frame.
    let observed = observe_drop_after_terminal(
        Arc::new(ScriptedDispatch::identity_sse(
            vec![Bytes::from_static(CHAT_PARTIAL.as_bytes())],
            None,
        )),
        responses_eof_record(false),
        chat_terminal_request()?,
        b"event: error\n",
        None,
    )
    .await?;
    assert_chat_native_incomplete(&observed, CHAT_PARTIAL.as_bytes());
    Ok(())
}

#[tokio::test]
async fn chat_terminal_finish_reason_clean_but_not_fault_is_success() -> TestResult {
    // QA-04: a finished choice accepts normal EOF, never a transport fault.
    let body = Bytes::from(format!("{CHAT_PARTIAL}{CHAT_FINISH}"));
    for fault in [false, true] {
        let observed = observe_chat_terminal(
            ScriptedDispatch::identity_sse(
                vec![body.clone()],
                fault.then_some(std::io::ErrorKind::ConnectionReset),
            ),
            false,
        )
        .await?;
        if fault {
            assert_eq!(
                observed.completion().as_ref().unwrap_err().error_type,
                "upstream_response_body_error"
            );
            observed.assert_terminal(Some("upstream_stream_error"));
        } else {
            assert_eq!(observed.body, body);
            assert_chat_terminal_success(&observed, None, false);
        }
        assert_chat_unknown_usage(&observed);
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_done_clean_and_transport_fault_preserve_usage() -> TestResult {
    // QA-01, QA-04, QA-05: usage-only final chunks must not erase completion.
    let full = format!("{CHAT_PARTIAL}{CHAT_FINISH}{CHAT_USAGE}");
    for (prefix, fault, transform, measured) in [
        (CHAT_PARTIAL, false, false, false),
        (CHAT_PARTIAL, true, false, false),
        (CHAT_PARTIAL, true, true, false),
        (CHAT_USAGE, false, false, true),
        (CHAT_USAGE, true, true, true),
        (full.as_str(), false, false, true),
        (full.as_str(), true, true, true),
    ] {
        let body = Bytes::from(format!("{prefix}{CHAT_DONE}"));
        let observed = observe_chat_terminal(
            ScriptedDispatch::identity_sse(
                vec![body.clone()],
                fault.then_some(std::io::ErrorKind::ConnectionReset),
            ),
            transform,
        )
        .await?;
        assert_eq!(observed.body, body);
        assert_chat_terminal_success(
            &observed,
            fault.then_some("upstream_response_body_error"),
            measured,
        );
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_split_done_requires_complete_delimiter_before_fault() -> TestResult {
    // QA-06: seeing the sentinel bytes without a full SSE delimiter is unsafe.
    for complete in [false, true] {
        let mut chunks = vec![
            Bytes::from_static(CHAT_PARTIAL.as_bytes()),
            Bytes::from_static(b"data: [DO"),
            Bytes::from_static(b"NE]\n"),
        ];
        if complete {
            chunks.push(Bytes::from_static(b"\n"));
        }
        let observed = observe_chat_terminal(
            ScriptedDispatch::identity_sse(chunks, Some(std::io::ErrorKind::ConnectionReset)),
            false,
        )
        .await?;
        if complete {
            assert_eq!(
                observed.body,
                Bytes::from(format!("{CHAT_PARTIAL}{CHAT_DONE}"))
            );
            assert_chat_terminal_success(&observed, Some("upstream_response_body_error"), false);
        } else {
            assert_eq!(
                observed.completion().as_ref().unwrap_err().error_type,
                "upstream_response_body_error"
            );
            observed.assert_terminal(Some("upstream_stream_error"));
            assert_chat_unknown_usage(&observed);
        }
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_provider_error_then_done_preserves_single_error() -> TestResult {
    // QA-07: terminal evidence cannot overwrite a provider-declared failure.
    let body = Bytes::from(format!(
        "{CHAT_PARTIAL}data: {{\"error\":{{\"type\":\"server_error\",\"code\":\"server_error\",\"message\":\"provider failure\"}}}}\n\n{CHAT_DONE}"
    ));
    let observed = observe_chat_terminal(
        ScriptedDispatch::identity_sse(vec![body.clone()], None),
        false,
    )
    .await?;
    assert_eq!(observed.body, body);
    assert_eq!(
        observed.completion().as_ref().unwrap_err().error_type,
        "server_error"
    );
    observed.assert_terminal(Some("upstream_stream_error"));
    assert_eq!(
        observed.row.upstream_error_type.as_deref(),
        Some("server_error")
    );
    assert_eq!(
        chat_json_events(&observed.body)
            .iter()
            .filter(|event| event.get("error").is_some())
            .count(),
        1
    );
    assert_chat_unknown_usage(&observed);
    Ok(())
}

#[tokio::test]
async fn chat_terminal_provider_error_retains_final_cumulative_usage() -> TestResult {
    assert_chat_failed_end_usage(true).await
}

#[tokio::test]
async fn chat_terminal_premature_eof_retains_final_cumulative_usage() -> TestResult {
    assert_chat_failed_end_usage(false).await
}

async fn assert_chat_failed_end_usage(provider_error: bool) -> TestResult {
    // All events share one upstream chunk, so the second usage snapshot is
    // throttled until finalization. A failed completion must still publish it.
    let mut prefix = format!(
        "{CHAT_PARTIAL}data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":7,\"completion_tokens\":1}}}}\n\ndata: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":7,\"completion_tokens\":2}}}}\n\n"
    );
    if provider_error {
        prefix.push_str("data: {\"error\":{\"type\":\"server_error\",\"code\":\"server_error\",\"message\":\"provider failure\"}}\n\n");
    }
    let body = Bytes::from(prefix);
    let observed = observe_chat_terminal(
        ScriptedDispatch::identity_sse(vec![body.clone()], None),
        false,
    )
    .await?;
    assert!(observed.body.starts_with(&body));
    if provider_error {
        assert_eq!(observed.body, body);
    } else {
        let errors = chat_json_events(&observed.body)
            .into_iter()
            .filter(|event| event.get("error").is_some())
            .collect::<Vec<_>>();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["error"]["type"], "api_error");
    }
    assert_eq!(
        observed.completion().as_ref().unwrap_err().error_type,
        if provider_error {
            "server_error"
        } else {
            "upstream_response_incomplete"
        }
    );
    observed.assert_terminal(Some("upstream_stream_error"));
    assert_eq!(
        (observed.row.input_tokens, observed.row.output_tokens),
        (Some(7), Some(2))
    );
    let final_usage = observed.events.iter().rev().find_map(|event| match event {
        LifecycleEvent::UsageObserved { usage, .. } => {
            Some((usage.input_tokens, usage.output_tokens))
        }
        _ => None,
    });
    assert_eq!(final_usage, Some((7, 2)));

    // Replay the actual lifecycle snapshots into a saturated durable channel.
    // Progress may drop; the final snapshot must reach the assembler consumer.
    let snapshots = observed
        .events
        .iter()
        .filter(|event| matches!(event, LifecycleEvent::UsageObserved { .. }))
        .cloned()
        .collect::<Vec<_>>();
    let bus = cc_lb_control::InMemoryBus::new();
    let mut assembler_rx = bus.attach_lifecycle_assembler(1);
    bus.publish_lifecycle(snapshots[0].clone());
    let mut progress = snapshots[0].clone();
    if let LifecycleEvent::UsageObserved { usage, .. } = &mut progress {
        usage.output_tokens = 99;
    }
    bus.publish_lifecycle(progress);
    bus.publish_lifecycle(snapshots.last().expect("final usage snapshot").clone());
    drop(bus);
    let delivered = tokio::time::timeout(Duration::from_secs(3), async {
        let mut delivered = Vec::new();
        while let Some(event) = assembler_rx.recv().await {
            if let LifecycleEvent::UsageObserved { usage, .. } = event {
                delivered.push((usage.input_tokens, usage.output_tokens));
            }
        }
        delivered
    })
    .await?;
    assert_eq!(delivered, vec![(7, 1), (7, 2)]);
    Ok(())
}

#[derive(Clone, Copy)]
enum ChatTerminalTransform {
    Drop,
    ReplaceWithDelta,
    Emit,
    EmitError,
}

impl SseEventTransformHook for ChatTerminalTransform {
    fn transform_sse_event(
        &self,
        request: TransformSseEventRequest,
    ) -> Result<TransformSseEventResult, ResponseTransformError> {
        let done = request.event.data.as_ref() == b"[DONE]";
        match (*self, done) {
            (Self::Drop, true) => Ok(TransformSseEventResult::Drop),
            (Self::ReplaceWithDelta, true) => Ok(TransformSseEventResult::Replace {
                events: vec![SseEvent {
                    event: String::new(),
                    data: Bytes::from_static(
                        br#"{"choices":[{"index":0,"delta":{"content":"replacement"},"finish_reason":null}]}"#,
                    ),
                }],
            }),
            (Self::Emit, false) => Ok(TransformSseEventResult::Replace {
                events: vec![
                    request.event,
                    SseEvent {
                        event: String::new(),
                        data: Bytes::from_static(b"[DONE]"),
                    },
                ],
            }),
            (Self::EmitError, false) => Ok(TransformSseEventResult::Replace {
                events: vec![
                    SseEvent {
                        event: "error".to_owned(),
                        data: Bytes::from_static(
                            br#"{"error":{"type":"server_error","code":"server_error","message":"transformed failure"}}"#,
                        ),
                    },
                    SseEvent {
                        event: String::new(),
                        data: Bytes::from_static(b"[DONE]"),
                    },
                ],
            }),
            _ => Ok(TransformSseEventResult::Unchanged),
        }
    }
}

#[tokio::test]
async fn chat_terminal_transformed_error_then_done_fault_is_not_warning() -> TestResult {
    // QA-07: emitted provider errors cannot be downgraded by an emitted DONE.
    let observed = observe_with_sse_hook(
        Arc::new(ScriptedDispatch::identity_sse(
            vec![Bytes::from_static(CHAT_PARTIAL.as_bytes())],
            Some(std::io::ErrorKind::ConnectionReset),
        )),
        Some(Arc::new(ChatTerminalTransform::EmitError)),
        responses_eof_record(false),
        chat_terminal_request()?,
    )
    .await?;
    let error = observed
        .completion()
        .as_ref()
        .expect_err("provider error remains fatal");
    assert_ne!(error.error_type, "upstream_response_incomplete");
    observed.assert_terminal(Some("upstream_stream_error"));
    assert_eq!(
        chat_json_events(&observed.body)
            .iter()
            .filter(|event| event.get("error").is_some())
            .count(),
        1
    );
    assert_eq!(
        std::str::from_utf8(&observed.body)?
            .matches("[DONE]")
            .count(),
        1
    );
    assert_chat_unknown_usage(&observed);
    Ok(())
}

#[tokio::test]
async fn chat_terminal_transformed_done_uses_emitted_not_upstream_evidence() -> TestResult {
    // QA-08: dropping/replacing upstream DONE removes its protection; a newly
    // emitted DONE supplies protection even when the upstream never sent one.
    for (transform, fault) in [
        (ChatTerminalTransform::Drop, false),
        (ChatTerminalTransform::Drop, true),
        (ChatTerminalTransform::ReplaceWithDelta, false),
        (ChatTerminalTransform::ReplaceWithDelta, true),
        (ChatTerminalTransform::Emit, true),
    ] {
        let emitted_done = matches!(transform, ChatTerminalTransform::Emit);
        let body = if emitted_done {
            CHAT_PARTIAL.to_owned()
        } else {
            format!("{CHAT_PARTIAL}{CHAT_DONE}")
        };
        let observed = observe_with_sse_hook(
            Arc::new(ScriptedDispatch::identity_sse(
                vec![Bytes::from(body)],
                fault.then_some(std::io::ErrorKind::ConnectionReset),
            )),
            Some(Arc::new(transform)),
            responses_eof_record(false),
            chat_terminal_request()?,
        )
        .await?;
        let downstream = std::str::from_utf8(&observed.body)?;
        assert!(downstream.contains("\"content\":\"partial\""));
        if emitted_done {
            assert_eq!(downstream.matches("data: [DONE]").count(), 1);
            assert_chat_terminal_success(&observed, Some("upstream_response_body_error"), false);
        } else {
            assert!(!downstream.lines().any(|line| line == "data: [DONE]"));
            if fault {
                assert_eq!(
                    observed.completion().as_ref().unwrap_err().error_type,
                    "upstream_response_body_error"
                );
                observed.assert_terminal(Some("upstream_stream_error"));
            } else {
                assert_chat_native_incomplete(&observed, CHAT_PARTIAL.as_bytes());
            }
            if matches!(transform, ChatTerminalTransform::ReplaceWithDelta) {
                assert!(downstream.contains("\"content\":\"replacement\""));
            }
        }
        assert_chat_unknown_usage(&observed);
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_decoded_gzip_transform_emits_native_incomplete() -> TestResult {
    // QA-02, QA-09: decoded transform output permits a plaintext native error.
    let observed = observe_chat_terminal(
        ScriptedDispatch::gzip_sse(
            vec![gzip_bytes(&Bytes::from_static(CHAT_PARTIAL.as_bytes()))],
            None,
        ),
        true,
    )
    .await?;
    assert!(observed.headers.get(CONTENT_ENCODING).is_none());
    assert_chat_native_incomplete(&observed, CHAT_PARTIAL.as_bytes());
    Ok(())
}

#[tokio::test]
async fn chat_terminal_raw_gzip_preserves_wire_without_plaintext_error() -> TestResult {
    // QA-02, QA-09: observe decoded completion without corrupting passthrough.
    for fault in [false, true] {
        let plaintext = if fault {
            format!("{CHAT_PARTIAL}{CHAT_DONE}")
        } else {
            CHAT_PARTIAL.to_owned()
        };
        let wire = gzip_bytes(&Bytes::from(plaintext));
        let observed = observe_chat_terminal(
            ScriptedDispatch::gzip_sse(
                vec![wire.clone()],
                fault.then_some(std::io::ErrorKind::ConnectionReset),
            ),
            false,
        )
        .await?;
        assert_eq!(observed.body, wire);
        assert_eq!(
            observed.headers.get(CONTENT_ENCODING),
            Some(&HeaderValue::from_static("gzip"))
        );
        if fault {
            assert_eq!(
                observed.completion().as_ref().unwrap_err().error_type,
                "upstream_response_body_error"
            );
            observed.assert_terminal(Some("upstream_stream_error"));
        } else {
            observed.assert_incomplete_outcome();
        }
        assert_chat_unknown_usage(&observed);
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_framing_and_decoder_failures_keep_precedence() -> TestResult {
    // QA-07: native incomplete must never replace existing processing errors.
    let partial = Bytes::from_static(CHAT_PARTIAL.as_bytes());
    for (error_type, dispatcher, transform) in [
        (
            "upstream_response_framing_error",
            ScriptedDispatch::identity_sse(
                vec![Bytes::from(format!(
                    "{CHAT_PARTIAL}data: {{\"choices\":[]}}\n"
                ))],
                None,
            ),
            true,
        ),
        (
            "upstream_response_decode_error",
            ScriptedDispatch::gzip_sse(
                vec![
                    incomplete_gzip_prefix(&partial),
                    Bytes::from_static(b"\x07"),
                ],
                None,
            ),
            true,
        ),
        (
            "upstream_response_decode_error",
            ScriptedDispatch::gzip_sse(vec![gzip_without_trailer(&partial)], None),
            false,
        ),
    ] {
        let observed = observe_chat_terminal(dispatcher, transform).await?;
        assert_eq!(
            observed.completion().as_ref().unwrap_err().error_type,
            error_type
        );
        observed.assert_terminal(Some("upstream_stream_error"));
        assert_eq!(
            observed.row.upstream_error_type.as_deref(),
            Some(error_type)
        );
        assert!(
            !observed
                .body
                .windows(b"upstream_response_incomplete".len())
                .any(|part| part == b"upstream_response_incomplete")
        );
        assert_chat_unknown_usage(&observed);
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_done_before_decoder_fault_is_warning() -> TestResult {
    // QA-01, QA-05: both decoder push and finish faults after emitted DONE are
    // warnings, preserving the usage-only final chunk and exact plaintext.
    let body = Bytes::from(format!("{CHAT_PARTIAL}{CHAT_USAGE}{CHAT_DONE}"));
    for (warning_type, body_end, dispatcher) in responses_terminal_faults(&body) {
        let observed = observe_chat_terminal(dispatcher, true).await?;
        assert_eq!(observed.body, body);
        assert_chat_terminal_success(&observed, Some(warning_type), true);
        assert_eq!(observed.row.upstream_body_end.as_deref(), Some(body_end));
    }
    Ok(())
}

#[tokio::test]
async fn chat_terminal_gemini_compatible_and_legacy_remain_out_of_scope() -> TestResult {
    // QA-09: these Chat-shaped protocols retain partial-EOF success and fatal
    // post-DONE body faults. Neither may inherit native OpenAI Chat policy.
    for gemini in [false, true] {
        for fault in [false, true] {
            let body = Bytes::from(if fault {
                format!("{CHAT_PARTIAL}{CHAT_DONE}")
            } else {
                CHAT_PARTIAL.to_owned()
            });
            let (record, path, provider, request_body) = if gemini {
                (
                    upstream_record(
                        StorageUpstreamKind::GeminiApiKey,
                        "https://generativelanguage.googleapis.com/",
                    ),
                    "/v1beta/openai/chat/completions",
                    cc_lb_domain::Provider::Gemini,
                    br#"{"model":"gemini-test","messages":[],"stream":true}"#.as_slice(),
                )
            } else {
                (
                    responses_eof_record(false),
                    "/v1/completions",
                    cc_lb_domain::Provider::Openai,
                    br#"{"model":"gpt-test","prompt":"hello","stream":true}"#.as_slice(),
                )
            };
            let mut request = http::Request::builder()
                .method("POST")
                .uri(path)
                .header("x-api-key", common::managed_api_key())
                .body(Bytes::copy_from_slice(request_body))?;
            request
                .extensions_mut()
                .insert(cc_lb_engine::ProviderScope(provider));
            let observed = observe(
                Arc::new(ScriptedDispatch::identity_sse(
                    vec![body.clone()],
                    fault.then_some(std::io::ErrorKind::ConnectionReset),
                )),
                false,
                record,
                request,
            )
            .await?;
            if fault {
                assert_eq!(
                    observed.completion().as_ref().unwrap_err().error_type,
                    "upstream_response_body_error"
                );
                observed.assert_terminal(Some("upstream_stream_error"));
                assert!(observed.body.starts_with(&body));
                let events = chat_json_events(&observed.body);
                let errors = events
                    .iter()
                    .filter_map(|event| event.get("error"))
                    .collect::<Vec<_>>();
                assert_eq!(
                    errors.len(),
                    1,
                    "existing local transport error is preserved"
                );
                assert_eq!(errors[0]["type"], "api_error");
                assert!(
                    errors[0]["message"]
                        .as_str()
                        .is_some_and(|message| !message.is_empty())
                );
                assert!(observed.body.ends_with(b"\n\n"));
            } else {
                observed.assert_terminal(None);
                assert!(observed.completion().is_ok());
                assert_eq!(observed.body, body);
            }
            assert_chat_unknown_usage(&observed);
        }
    }
    Ok(())
}
