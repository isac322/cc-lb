use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::{DialectCache, PrincipalView, RouterPipelineCache};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder, RequestEventBus};
use cc_lb_domain::{Principal, TerminalStrategy, UpstreamCandidate};
use cc_lb_engine::realtime_websocket::{
    RealtimeStreamMetrics, RealtimeUsage, RealtimeWebSocketDiagnostics, RealtimeWebSocketKind,
    RealtimeWebSocketTarget, RealtimeWebSocketTermination,
};
use cc_lb_engine::responses_websocket::{ResponsesWebSocketGateway, WebSocketError};
use cc_lb_engine::{DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin};
use cc_lb_storage_api::upstream::{
    UpstreamCreate, UpstreamKind as StorageUpstreamKind, UpstreamRecord,
};
use cc_lb_storage_api::{MetaStore, Storage as StorageTrait, UpstreamStore};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::SignedRequest;
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use http_body_util::BodyExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::{Layer, Registry};
use url::Url;
use uuid::Uuid;

use crate::common::{
    TestAuthn, TestLifecycleBus, TestState, lifecycle_with_parts, managed_api_key, messages_request,
};

const UNKNOWN_ALIAS_MESSAGE: &str = "unknown model alias, returning unchanged";

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapturedWarning {
    message: String,
    model: Option<String>,
}

#[derive(Default)]
struct EventFields {
    message: Option<String>,
    model: Option<String>,
}

impl tracing::field::Visit for EventFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn Debug) {
        let value = format!("{value:?}");
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .map_or(value.clone(), str::to_owned);
        self.record(field, value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record(field, value.to_owned());
    }
}

impl EventFields {
    fn record(&mut self, field: &tracing::field::Field, value: String) {
        match field.name() {
            "message" => self.message = Some(value),
            "model" => self.model = Some(value),
            _ => {}
        }
    }
}

#[derive(Clone)]
struct WarningLayer {
    warnings: Arc<Mutex<Vec<CapturedWarning>>>,
}

impl<S> Layer<S> for WarningLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != Level::WARN {
            return;
        }
        let mut fields = EventFields::default();
        event.record(&mut fields);
        let Some(message) = fields.message else {
            return;
        };
        self.warnings
            .lock()
            .expect("warning capture lock")
            .push(CapturedWarning {
                message,
                model: fields.model,
            });
    }
}

async fn capture_warnings<F, Fut, R>(operation: F) -> (R, Vec<CapturedWarning>)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = R>,
{
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let layer = WarningLayer {
        warnings: Arc::clone(&warnings),
    };
    let dispatch = tracing::Dispatch::new(Registry::default().with(layer));
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let result = operation().await;
    let warnings = warnings.lock().expect("warning capture lock").clone();
    (result, warnings)
}

#[tokio::test]
async fn gemini_model_identity_is_canonicalized_without_rewriting_wire_body()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let authn = TestAuthn::new(TestState::default());
    let upstream_id = uuid::Uuid::from_u128(1_204_001);
    let upstream = storage
        .create(UpstreamCreate {
            id: upstream_id,
            name: "gemini-identity-events".to_owned(),
            kind: StorageUpstreamKind::GeminiApiKey,
            base_url: Some("https://generativelanguage.googleapis.com/".parse()?),
            api_key_ciphertext: Some(Vec::new()),
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        })
        .await?;
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![upstream])
        .build();
    let dispatcher = GeminiCapturingDispatch::default();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(dispatcher.clone()),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc())
    .with_upstream_affinity_store(storage.clone());

    let requests = [
        (
            "/v1beta/cachedContents",
            Bytes::from_static(
                br#"{"model":"models/gemini-2.5-flash","contents":[]}"#,
            ),
        ),
        (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            Bytes::from_static(
                br#"{"model":"models/gemini-2.5-flash","cachedContent":"cachedContents/example","contents":[]}"#,
            ),
        ),
        (
            "/v1beta/models/gemini-2.5-flash:generateContent",
            Bytes::from_static(br#"{"contents":[]}"#),
        ),
        (
            "/v1beta/cachedContents",
            Bytes::from_static(
                br#"{"model":"models/gemini-2.5-flash","contents":[]}"#,
            ),
        ),
    ];
    for (path, body) in &requests {
        let mut request = http::Request::builder()
            .method("POST")
            .uri(*path)
            .header("x-api-key", managed_api_key())
            .body(body.clone())?;
        request
            .extensions_mut()
            .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Gemini));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("Gemini request authenticates");
        let response = lifecycle.handle(request, &auth).await?;
        assert_eq!(response.status(), StatusCode::OK, "{path} response");
        response.into_body().collect().await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    update_rx.recv().await.expect("request update delivered"),
                    cc_lb_request_log::RequestEventUpdate::Final(_)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("Gemini final request event arrives");
    }

    assert_eq!(
        dispatcher
            .bodies
            .lock()
            .expect("captured bodies lock")
            .as_slice(),
        requests
            .iter()
            .map(|(_, body)| body.clone())
            .collect::<Vec<_>>()
            .as_slice()
    );
    let events = crate::common::stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 4);
    assert!(
        events
            .iter()
            .all(|event| event.model.as_deref() == Some("gemini-2.5-flash")),
        "cached-content creation and URL-only requests share one canonical model identity"
    );
    Ok(())
}

#[tokio::test]
async fn openai_image_and_transcription_usage_persist_in_request_events()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let state = TestState::default();
    let authn = TestAuthn::new(state);
    let upstream_id = uuid::Uuid::from_u128(1_093_001);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![UpstreamRecord {
            id: upstream_id,
            name: "openai-usage-events".to_owned(),
            kind: StorageUpstreamKind::OpenaiApiKey,
            base_url: Some("https://api.openai.com/".parse()?),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            ..UpstreamRecord::default()
        }])
        .build();
    let dispatcher = OpenAiUsageDispatch {
        responses: Arc::new(Mutex::new(VecDeque::from([
            Bytes::from_static(
                br#"{"created":1,"data":[],"usage":{"input_tokens":11,"output_tokens":3,"total_tokens":14}}"#,
            ),
            Bytes::from_static(
                br#"{"text":"transcribed","usage":{"type":"tokens","input_tokens":17,"output_tokens":5,"total_tokens":22}}"#,
            ),
        ]))),
    };
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(dispatcher),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc());

    for (path, body) in [
        (
            "/v1/images/generations",
            br#"{"model":"gpt-image-1"}"#.as_slice(),
        ),
        (
            "/v1/audio/transcriptions",
            br#"{"model":"whisper-1"}"#.as_slice(),
        ),
    ] {
        let mut request = http::Request::builder()
            .method("POST")
            .uri(path)
            .header("x-api-key", managed_api_key())
            .header("content-type", "application/json")
            .body(Bytes::from_static(body))?;
        request
            .extensions_mut()
            .insert(cc_lb_engine::ProviderScope(cc_lb_domain::Provider::Openai));
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("OpenAI request authenticates");
        let response = lifecycle.handle(request, &auth).await?;
        assert_eq!(response.status(), StatusCode::OK, "{path} response");
        response.into_body().collect().await?;

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    update_rx.recv().await.expect("request update delivered"),
                    cc_lb_request_log::RequestEventUpdate::Final(_)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("OpenAI final request event arrives");
    }

    let events = crate::common::stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .any(|event| event.input_tokens == Some(11) && event.output_tokens == Some(3)),
        "image usage persists in the request event"
    );
    assert!(
        events
            .iter()
            .any(|event| event.input_tokens == Some(17) && event.output_tokens == Some(5)),
        "transcription usage persists in the request event"
    );

    Ok(())
}

#[tokio::test]
async fn concurrent_limit_denial_emits_one_rejected_decision_without_reserved_or_dispatch() {
    use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
    use cc_lb_control::api_keys::limit_engine::LimitEngine;
    use cc_lb_control::api_keys::principal_view::PrincipalView;
    use cc_lb_storage_api::principal::{Limit, LimitKind};
    use cc_lb_storage_api::{KeyStatus, StoredApiKeyRecord};

    let state = TestState::default();
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        vec![Limit {
            kind: LimitKind::Concurrent,
            window_secs: 0,
            cap_micros: 1,
        }],
        std::collections::HashMap::new(),
    ));
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(64);
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(principal_view)
        .upstream_records(vec![UpstreamRecord {
            id: uuid::Uuid::from_u128(1_167),
            name: "concurrent-limit-upstream".to_owned(),
            kind: StorageUpstreamKind::AnthropicApiKey,
            base_url: Some("http://upstream.local/".parse().expect("test URL")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            ..UpstreamRecord::default()
        }])
        .build();
    let concurrency = Arc::new(KeyConcurrencyManager::new());
    let held = concurrency
        .try_acquire("key-test", 1)
        .expect("fixture occupies the only concurrent slot");
    let limit_engine = LimitEngine::new(concurrency, Arc::new(cc_lb_clock::SystemClock));
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_static_limit_subject(
        limit_engine,
        "principal-test".to_owned(),
        "key-test".to_owned(),
        StoredApiKeyRecord {
            key_hash_b64: "key-test".to_owned(),
            status: KeyStatus::Active,
            ..StoredApiKeyRecord::default()
        },
    )
    .with_event_bus(test_bus.bus_arc());
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","max_tokens":1,"messages":[]}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("request authenticates");
    let response = lifecycle
        .handle(request, &auth)
        .await
        .expect("limit rejection returns HTTP response");
    let (status, headers, body) = crate::common::collect_body(response).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers
            .get("x-should-retry")
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
    let body: serde_json::Value = serde_json::from_slice(&body).expect("limit error JSON");
    assert_eq!(body["error"]["type"], "rate_limit_error");
    assert_eq!(body["error"]["limit_kind"], "concurrent");
    assert_eq!(body["error"]["retry_after_seconds"], 1);
    assert_eq!(
        state
            .upstream_calls
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    let mut rejected = 0;
    let mut reserved = 0;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
        .await
        .expect("lifecycle event arrives")
    {
        match event {
            cc_lb_lifecycle::LifecycleEvent::LimitDecision { decision, .. } => match decision {
                cc_lb_lifecycle::LimitDecisionKind::Rejected {
                    reason,
                    subject,
                    request_summary,
                    route_summary,
                    limit_violation,
                    ..
                } => {
                    rejected += 1;
                    assert_eq!(reason, "limit_rejected");
                    assert_eq!(subject.principal_id, "principal-test");
                    assert_eq!(subject.key_id, "key-test");
                    assert_eq!(request_summary.model, "claude-test");
                    assert_eq!(request_summary.path, "/v1/messages");
                    assert_eq!(request_summary.method, "POST");
                    assert_eq!(route_summary.upstream_name, "concurrent-limit-upstream");
                    assert_eq!(limit_violation.as_deref(), Some("Concurrent"));
                }
                cc_lb_lifecycle::LimitDecisionKind::Reserved { .. } => reserved += 1,
                unexpected => panic!("unexpected limit decision: {unexpected:?}"),
            },
            cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. } => break,
            _ => {}
        }
    }
    assert_eq!(rejected, 1);
    assert_eq!(reserved, 0);
    drop(held);
}

#[tokio::test]
async fn realtime_websocket_observers_classify_attempted_routes() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());
    let auth = lifecycle
        .authenticate(messages_request(Bytes::new()).headers())
        .await
        .expect("test request authenticates");

    for (path, expected) in [
        (
            "/v1/realtime",
            cc_lb_request_log::RequestEventKind::OpenaiResponses,
        ),
        (
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            cc_lb_request_log::RequestEventKind::GeminiGenerateContent,
        ),
    ] {
        lifecycle
            .begin_realtime_websocket(&auth, path)
            .finish(StatusCode::SWITCHING_PROTOCOLS);
        let mut saw_started = false;
        let mut saw_terminated = false;
        while !saw_terminated {
            match tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
                .await
                .expect("lifecycle event arrives")
                .expect("lifecycle event stream remains open")
            {
                cc_lb_lifecycle::LifecycleEvent::RequestStarted { event_kind, .. } => {
                    assert_eq!(event_kind, Some(expected));
                    saw_started = true;
                }
                cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. } => {
                    saw_terminated = true;
                }
                _ => {}
            }
        }
        assert!(saw_started);
    }
}

#[tokio::test]
async fn early_gemini_websocket_cancellation_uses_upgrade_status() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());
    let auth = lifecycle
        .authenticate(messages_request(Bytes::new()).headers())
        .await
        .expect("test request authenticates");

    lifecycle
        .begin_realtime_websocket(
            &auth,
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
        )
        .finish_client_closed();

    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
            .await
            .expect("cancellation lifecycle event arrives")
            .expect("lifecycle event stream remains open");
        if let cc_lb_lifecycle::LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            ..
        } = event
        {
            assert_eq!(client_status, StatusCode::SWITCHING_PROTOCOLS.as_u16());
            assert!(matches!(
                reason,
                cc_lb_lifecycle::TerminationReason::ErrorCode(code)
                    if code == "client_closed_request"
            ));
            break;
        }
    }
}

#[tokio::test]
async fn realtime_setup_identity_is_authenticated_and_persisted_before_routing() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.subscribe_lifecycle();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());
    let request = messages_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("websocket setup authenticates before parsing");
    let mut headers = request.headers().clone();
    headers.insert(
        "x-claude-code-session-id",
        HeaderValue::from_static("realtime-session"),
    );
    headers.insert(
        "x-session-affinity",
        HeaderValue::from_static("lower-priority-session"),
    );

    for (kind, path, model) in [
        (
            RealtimeWebSocketKind::Openai,
            "/v1/realtime",
            "gpt-realtime",
        ),
        (
            RealtimeWebSocketKind::Gemini,
            "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent",
            "gemini-live",
        ),
    ] {
        let observation = lifecycle.begin_realtime_websocket(&auth, path);
        let result = lifecycle
            .prepare_realtime_websocket(
                &auth,
                observation,
                kind,
                &headers,
                RealtimeWebSocketTarget {
                    path,
                    query: None,
                    model: Some(model),
                },
                Bytes::from_static(br#"{"setup":{"not_session_metadata":true}}"#),
            )
            .await;
        assert!(
            result.is_err(),
            "the default fixture has no eligible WebSocket candidate"
        );

        let parse = loop {
            let event = tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
                .await
                .expect("setup lifecycle event arrives")
                .expect("lifecycle event stream remains open");
            if let cc_lb_lifecycle::LifecycleEvent::ParseCompleted {
                result: Ok(info), ..
            } = event
            {
                break info;
            }
        };
        assert_eq!(parse.thread_id.as_deref(), Some("realtime-session"));
        assert_eq!(
            parse.observed_session_id.as_deref(),
            Some("realtime-session")
        );
        assert_eq!(
            parse.session_id_source.as_deref(),
            Some("x-claude-code-session-id")
        );
        assert_eq!(parse.claude_agent_id, None);
        assert_eq!(parse.parent_session_id, None);
    }
}

#[tokio::test]
async fn realtime_relay_finish_publishes_typed_io_diagnostics_and_single_terminal() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_assembler(128);
    let authn = TestAuthn::new(TestState::default());
    let upstream = UpstreamRecord {
        id: uuid::Uuid::from_u128(1_132),
        name: "realtime-diagnostics-upstream".to_owned(),
        kind: StorageUpstreamKind::OpenaiApiKey,
        base_url: Some("http://upstream.local/".parse().expect("test URL")),
        enabled: true,
        api_key_ciphertext: Some(Vec::new()),
        ..UpstreamRecord::default()
    };
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(authn.principal_view.clone())
        .upstream_records(vec![upstream])
        .build();
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
    .with_event_bus(test_bus.bus_arc());
    let request = messages_request(Bytes::new());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("realtime request authenticates");
    let observation = lifecycle.begin_realtime_websocket(&auth, "/v1/realtime");
    let mut session = lifecycle
        .prepare_realtime_websocket(
            &auth,
            observation,
            RealtimeWebSocketKind::Openai,
            request.headers(),
            RealtimeWebSocketTarget {
                path: "/v1/realtime",
                query: None,
                model: Some("gpt-realtime"),
            },
            Bytes::from_static(br#"{"setup":{"type":"realtime"}}"#),
        )
        .await
        .expect("realtime session prepares");
    session.connected(&HeaderMap::from_iter([(
        http::header::HeaderName::from_static("x-request-id"),
        HeaderValue::from_static("provider-request-1132"),
    )]));
    session.finish(
        RealtimeUsage::default(),
        RealtimeStreamMetrics::default(),
        RealtimeWebSocketTermination::ProxyRelayError,
        RealtimeWebSocketDiagnostics::default().with_relay_error(&std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        )),
    );

    let mut response_started = 0;
    let mut diagnostics = 0;
    let mut terminated = 0;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
            .await
            .expect("relay terminal lifecycle event arrives")
            .expect("lifecycle event stream remains open");
        match event {
            cc_lb_lifecycle::LifecycleEvent::UpstreamResponseStarted { status, .. } => {
                response_started += 1;
                assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS.as_u16());
            }
            cc_lb_lifecycle::LifecycleEvent::UpstreamStreamDiagnosticsObserved {
                diagnostics: observed,
                ..
            } => {
                diagnostics += 1;
                assert_eq!(observed.http_version, None);
                assert_eq!(
                    observed.request_id.as_deref(),
                    Some("provider-request-1132")
                );
                assert_eq!(observed.body_error_cause.as_deref(), Some("io_reset"));
                assert_eq!(
                    observed.body_error_io_kind.as_deref(),
                    Some("ConnectionReset")
                );
                assert_eq!(observed.body_error_h2_reason, None);
            }
            cc_lb_lifecycle::LifecycleEvent::RequestTerminated { client_status, .. } => {
                terminated += 1;
                assert_eq!(client_status, StatusCode::SWITCHING_PROTOCOLS.as_u16());
                break;
            }
            _ => {}
        }
    }
    assert_eq!(response_started, 1);
    assert_eq!(diagnostics, 1);
    assert_eq!(terminated, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn responses_websocket_admission_records_canonical_model_without_alias_warning() {
    let upstream_id = Uuid::from_u128(1196);
    let canonical_models = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![Arc::new(ResponsesRecordingModelFilter {
            canonical_models: Arc::clone(&canonical_models),
            kept_upstream_ids: vec![upstream_id],
        })],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
        provider_filters: None,
    });
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    );
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ));
    let authn = TestAuthn::with_principal_view(TestState::default(), principal_view.clone());
    let test_bus = TestLifecycleBus::new();
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .principal_view(principal_view)
        .upstream_records(vec![UpstreamRecord {
            id: upstream_id,
            name: "responses-admission-upstream".to_owned(),
            kind: StorageUpstreamKind::OpenaiApiKey,
            base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            ..UpstreamRecord::default()
        }])
        .build();
    let lifecycle = Arc::new(
        Lifecycle::new_with_dynamic_view(
            authn.authn.clone(),
            Arc::new(DynamicViewHolder::new(view)),
            Arc::new(CapturingDispatch::default()),
            LifecycleConfig::default(),
            Arc::new(cc_lb_clock::SystemClock),
        )
        .with_event_bus(test_bus.bus_arc()),
    );
    let headers = messages_request(Bytes::new()).headers().clone();
    let auth = lifecycle
        .authenticate(&headers)
        .await
        .expect("Responses WebSocket authenticates");
    let mut gateway =
        ResponsesWebSocketGateway::new(Arc::clone(&lifecycle), &auth, headers, 1 << 20, 8);
    let (dispatch, warnings) = capture_warnings(|| async {
        gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","stream_id":"main","model":"gpt-test","store":false,"input":"canonical model fixture"}"#,
                ),
            )
            .await
    })
    .await;
    let dispatch = dispatch
        .expect("response.create admission succeeds")
        .expect("first response.create dispatches");
    assert_eq!(dispatch.candidates.len(), 1);
    assert_eq!(canonical_models.lock().unwrap().as_slice(), ["gpt-test"]);
    assert_eq!(
        warnings
            .iter()
            .filter(|warning| {
                warning.message == UNKNOWN_ALIAS_MESSAGE
                    && warning.model.as_deref() == Some("gpt-test")
            })
            .count(),
        0,
        "native OpenAI admission model emitted alias warnings: {warnings:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn responses_websocket_terminal_matrix_settles_once_and_labels_every_outcome() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);
    let cases = [
        ("completed", "completed"),
        ("provider_closed", "upstream_error"),
        ("proxy_error", "proxy_error"),
        ("downstream_closed", "client_cancelled"),
    ];
    let expected_outcomes = cases
        .iter()
        .map(|(_, outcome)| *outcome)
        .collect::<std::collections::BTreeSet<_>>();

    for (case, _) in cases {
        let test_bus = TestLifecycleBus::new();
        let mut lifecycle_events = test_bus.bus.subscribe_lifecycle();
        let authn = TestAuthn::new(TestState::default());
        let upstream = UpstreamRecord {
            id: uuid::Uuid::from_u128(1131),
            name: "responses-matrix-upstream".to_owned(),
            kind: StorageUpstreamKind::OpenaiApiKey,
            base_url: Some("http://upstream.local/".parse().expect("test URL")),
            enabled: true,
            api_key_ciphertext: Some(Vec::new()),
            ..UpstreamRecord::default()
        };
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(authn.clone()))
            .principal_view(authn.principal_view.clone())
            .upstream_records(vec![upstream])
            .build();
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.authn.clone(),
                Arc::new(DynamicViewHolder::new(view)),
                Arc::new(CapturingDispatch::default()),
                LifecycleConfig::default(),
                Arc::new(cc_lb_clock::SystemClock),
            )
            .with_event_bus(test_bus.bus_arc()),
        );
        let request = messages_request(Bytes::new());
        let headers = request.headers().clone();
        let auth = lifecycle
            .authenticate(&headers)
            .await
            .expect("Responses WebSocket authenticates");
        let mut gateway =
            ResponsesWebSocketGateway::new(Arc::clone(&lifecycle), &auth, headers, 1 << 20, 8);
        let dispatch = gateway
            .client_message(
                auth,
                Bytes::from_static(
                    br#"{"type":"response.create","stream_id":"main","model":"gpt-test","store":false,"input":"responses matrix fixture"}"#,
                ),
            )
            .await
            .expect("response.create is admitted")
            .expect("first response.create dispatches");
        let candidate = dispatch
            .candidates
            .first()
            .cloned()
            .expect("admitted turn has one candidate");
        gateway.assign_connection(
            dispatch.turn_id,
            1131,
            candidate.upstream_id,
            &candidate.upstream_name,
        );
        gateway
            .validate_send(dispatch.turn_id)
            .await
            .expect("admitted turn validates before send");
        gateway.sent(dispatch.turn_id);

        match case {
            "completed" => {
                gateway
                    .provider_message(
                        1131,
                        &Bytes::from_static(
                            br#"{"type":"response.created","stream_id":"main","response":{"id":"resp-1131","store":false}}"#,
                        ),
                    )
                    .await
                    .expect("response.created is accepted");
                gateway
                    .provider_message(
                        1131,
                        &Bytes::from_static(
                            br#"{"type":"response.completed","stream_id":"main","response":{"id":"resp-1131","usage":{"input_tokens":2,"output_tokens":3}}}"#,
                        ),
                    )
                    .await
                    .expect("response.completed is accepted");
            }
            "provider_closed" => {
                gateway.connection_closed(1131, "provider closed");
            }
            "proxy_error" => {
                gateway
                    .fail_dispatch(
                        dispatch.turn_id,
                        &WebSocketError::new(502, "proxy_relay_error", "relay failed"),
                    )
                    .await;
            }
            "downstream_closed" => gateway.close(),
            _ => unreachable!("matrix covers all terminal paths"),
        }

        let mut terminal_count = 0;
        let mut stream_completed_count = 0;
        while terminal_count == 0 {
            let event = tokio::time::timeout(Duration::from_secs(1), lifecycle_events.recv())
                .await
                .expect("terminal lifecycle event arrives")
                .expect("lifecycle event stream remains open");
            match event {
                cc_lb_lifecycle::LifecycleEvent::StreamCompleted { .. } => {
                    stream_completed_count += 1;
                }
                cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. } => {
                    terminal_count += 1;
                }
                _ => {}
            }
        }
        assert_eq!(stream_completed_count, 1, "{case} stream settles once");
        assert_eq!(terminal_count, 1, "{case} request settles once");
        loop {
            match lifecycle_events.try_recv() {
                Ok(event) => assert!(
                    !matches!(
                        event,
                        cc_lb_lifecycle::LifecycleEvent::RequestTerminated { .. }
                    ),
                    "{case} close/task-drop race must not duplicate RequestTerminated"
                ),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
                | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            }
        }
    }

    let samples = snapshotter.snapshot().into_vec();
    let terminations = samples
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == "cc_lb_stream_terminations_total")
        .collect::<Vec<_>>();
    assert_eq!(
        terminations.len(),
        cases.len(),
        "each terminal path has one metric series"
    );
    for (key, _, _, value) in terminations {
        let labels = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            labels
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            ["cause", "model", "outcome", "upstream"]
                .into_iter()
                .collect()
        );
        let outcome = labels.get("outcome").copied().expect("outcome label");
        assert!(expected_outcomes.contains(outcome));
        let upstream = labels.get("upstream").copied().expect("upstream label");
        assert!(
            matches!(upstream, "responses-matrix-upstream" | "unknown"),
            "upstream label must be a bounded valid value, got {upstream}"
        );
        assert_eq!(labels.get("model"), Some(&"gpt-test"));
        assert!(matches!(value, DebugValue::Counter(1)));
    }
}

#[tokio::test]
async fn hermes_root_identity_crosses_proxy_and_persists_without_body_rewrite()
-> Result<(), Box<dyn std::error::Error>> {
    let original_body = Bytes::from_static(
        br#"{"model":"claude-test","session_id":"hermes-root-session","tags":["product=hermes-agent","client=hermes-client-v0.19.0","conversation=hermes-root-session"],"system":[{"type":"text","text":"You are a focused subagent working on a specific delegated task.\n\nYOUR TASK:\nReview the auth change"}],"messages":[{"role":"user","content":"Review the auth change"}],"tools":[{"name":"terminal"}]}"#,
    );
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let dispatcher = CapturingDispatch::default();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(dispatcher.clone()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(original_body.clone());
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                update_rx.recv().await.expect("request update delivered"),
                cc_lb_request_log::RequestEventUpdate::Final(_)
            ) {
                break;
            }
        }
    })
    .await
    .expect("final request event arrives");

    assert_eq!(
        dispatcher
            .bodies
            .lock()
            .expect("captured bodies lock")
            .as_slice(),
        &[original_body]
    );
    let events = crate::common::stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.thread_id.as_deref(), Some("hermes-root-session"));
    assert_eq!(
        event.observed_session_id.as_deref(),
        Some("hermes-root-session")
    );
    assert_eq!(event.session_id_source.as_deref(), Some("session_id"));
    assert_eq!(event.request_kind.as_deref(), Some("subagent"));
    assert_eq!(event.parent_session_id, None);
    assert_eq!(event.client_app, None);

    Ok(())
}

#[tokio::test]
async fn aside_main_worker_and_title_identities_persist() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&dir).await?);
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let lifecycle = lifecycle_with_parts(
        TestAuthn::new(TestState::default()),
        Arc::new(CapturingDispatch::default()),
        LifecycleConfig::default(),
    )
    .with_event_bus(test_bus.bus_arc());

    for (session_id, system, request_kind, parent_session_id) in [
        (
            "aside-main",
            "You are Aside, a helpful, proactive, and smart AI browser.",
            "main",
            None,
        ),
        (
            "aside:worker:aside-main:default",
            "You are Aside, a helpful, proactive, and smart AI browser.",
            "subagent",
            Some("aside-main"),
        ),
        (
            "aside-main",
            "Generate a brief title that would help the user find this conversation later.",
            "session_title",
            None,
        ),
    ] {
        let body = Bytes::from(serde_json::to_vec(&serde_json::json!({
            "model": "claude-test",
            "system": system,
            "messages": [{"role": "user", "content": "hello"}],
        }))?);
        let mut request = messages_request(body);
        request.headers_mut().insert(
            http::HeaderName::from_static("x-aside-provider-session-id"),
            http::HeaderValue::from_static(session_id),
        );
        let auth = lifecycle
            .authenticate(request.headers())
            .await
            .expect("Aside request authenticates");
        let response = lifecycle.handle(request, &auth).await?;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if matches!(
                    update_rx.recv().await.expect("request update delivered"),
                    cc_lb_request_log::RequestEventUpdate::Final(_)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("Aside final request event arrives");

        let events = crate::common::stored_request_events(storage.as_ref()).await?;
        assert!(
            events.iter().any(|event| {
                event.observed_session_id.as_deref() == Some(session_id)
                    && event.thread_id.as_deref() == Some(session_id)
                    && event.parent_session_id.as_deref() == parent_session_id
                    && event.session_id_source.as_deref() == Some("x-aside-provider-session-id")
                    && event.request_kind.as_deref() == Some(request_kind)
            }),
            "{request_kind} identity persists"
        );
    }
    assert_eq!(
        crate::common::stored_request_events(storage.as_ref())
            .await?
            .len(),
        3
    );
    Ok(())
}

#[derive(Clone, Default)]
struct CapturingDispatch {
    bodies: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl UpstreamDispatch for CapturingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.bodies
            .lock()
            .expect("captured bodies lock")
            .push(request.body().clone());

        let mut response = Response::new(Body::from(Bytes::from_static(
            br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#,
        )));
        *response.status_mut() = StatusCode::OK;
        Ok(response)
    }
}

#[derive(Clone, Default)]
struct GeminiCapturingDispatch {
    bodies: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl UpstreamDispatch for GeminiCapturingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.bodies
            .lock()
            .expect("captured Gemini bodies lock")
            .push(request.body().clone());
        let response_body = if request.url().path() == "/v1beta/cachedContents" {
            Bytes::from_static(
                br#"{"name":"cachedContents/example","expireTime":"2100-01-01T00:00:00Z"}"#,
            )
        } else {
            Bytes::from_static(
                br#"{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1}}"#,
            )
        };
        let mut response = Response::new(Body::from(response_body));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}

#[derive(Clone)]
struct OpenAiUsageDispatch {
    responses: Arc<Mutex<VecDeque<Bytes>>>,
}

#[async_trait]
impl UpstreamDispatch for OpenAiUsageDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let body = self
            .responses
            .lock()
            .expect("OpenAI response queue lock")
            .pop_front()
            .expect("one mocked response per request");
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}

struct ResponsesRecordingModelFilter {
    canonical_models: Arc<Mutex<Vec<String>>>,
    kept_upstream_ids: Vec<Uuid>,
}

impl FilterPlugin for ResponsesRecordingModelFilter {
    fn filter(
        &self,
        ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.canonical_models
            .lock()
            .expect("canonical model capture lock")
            .push(ctx.canonical_model_id.clone());
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: "record:responses-model".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "record-responses-model"
    }
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("hermes-request-events.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await?;
    storage.initialize().await?;
    Ok(storage)
}
