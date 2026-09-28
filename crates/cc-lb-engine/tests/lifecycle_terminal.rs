use crate::common;

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::RequestEventBus;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::api_keys::principal_view::{
    DialectCache, PrincipalRoutingArtifacts, PrincipalView, RouterPipelineCache,
};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, TerminalStrategy, Upstream, UpstreamCandidate};
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin};
use cc_lb_storage_api::principal::{PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::upstream::{UpstreamKind as StorageUpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{KeyStatus, StoredApiKeyRecord};
use cc_lb_storage_api::{MetaStore, RequestEventStore, Storage as StorageTrait};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability,
};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Response, StatusCode};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::BodyExt;
use tokio::sync::broadcast;
use url::Url;
use uuid::Uuid;

use common::{
    DispatchMode, MockDispatch, TestAuthn, TestLifecycleBus, TestState, collect_body,
    messages_request,
};

#[test]
fn terminal_strategy_exhaustive_match_covers_supported_variants() {
    let labels = [TerminalStrategy::FirstPick, TerminalStrategy::Random]
        .map(terminal_strategy_label_for_exhaustive_test);

    assert_eq!(labels, ["first-pick", "random"]);
}

fn terminal_strategy_label_for_exhaustive_test(strategy: TerminalStrategy) -> &'static str {
    match strategy {
        TerminalStrategy::FirstPick => "first-pick",
        TerminalStrategy::Random => "random",
    }
}

#[tokio::test]
async fn first_pick_selects_first_candidate_after_filters() -> Result<(), Box<dyn std::error::Error>>
{
    let first = upstream_id(1);
    let second = upstream_id(2);
    let third = upstream_id(3);
    let choices = Arc::new(Mutex::new(Vec::new()));
    let filter_calls = Arc::new(Mutex::new(Vec::new()));
    let _dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&_dir, "lifecycle-terminal.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_terminal(
        TerminalStrategy::FirstPick,
        vec![Arc::new(KeepFilter {
            kept_upstream_ids: vec![second, third],
            calls: Arc::clone(&filter_calls),
        })],
        vec![
            upstream_record(first, "first"),
            upstream_record(second, "second"),
            upstream_record(third, "third"),
        ],
        Arc::clone(&choices),
    )
    .with_event_bus(test_bus.bus_arc())
    .with_static_limit_subject(
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        ),
        "principal-test".to_owned(),
        "key-test".to_owned(),
        active_record(),
    );

    send_message(&lifecycle).await?;

    assert_eq!(
        filter_calls.lock().expect("filter calls lock").as_slice(),
        &[vec![first, second, third]]
    );
    assert_eq!(
        choices.lock().expect("choices lock").as_slice(),
        &["second".to_owned()]
    );
    let events = wait_for_events(storage.as_ref(), 1).await?;
    let trace = events
        .first()
        .and_then(|event| event.routing_trace.as_ref())
        .expect("routing trace is recorded");
    assert_eq!(trace.stages.len(), 1);
    assert_eq!(trace.stages[0].stage_name, "keep-terminal-candidates");
    assert_eq!(
        trace
            .terminal_decision
            .as_ref()
            .map(|decision| (decision.upstream_id, decision.strategy.clone(),)),
        Some((Some(second), TerminalStrategy::FirstPick))
    );
    assert_request_setup_timings(events.first().expect("successful event"));
    Ok(())
}

#[tokio::test]
async fn signer_build_failure_preserves_request_setup_timings()
-> Result<(), Box<dyn std::error::Error>> {
    let choices = Arc::new(Mutex::new(Vec::new()));
    let _dir = tempfile::tempdir()?;
    let storage = Arc::new(sqlite_storage(&_dir, "lifecycle-signer-failure.sqlite").await?);
    let test_bus =
        TestLifecycleBus::new().with_assembler(Arc::clone(&storage) as Arc<dyn StorageTrait>);
    let lifecycle = lifecycle_with_terminal_and_signer_failure(
        TerminalStrategy::FirstPick,
        Vec::new(),
        vec![upstream_record(upstream_id(1), "first")],
        Arc::clone(&choices),
        true,
    )
    .with_event_bus(test_bus.bus_arc());

    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    let events = wait_for_events(storage.as_ref(), 1).await?;
    let event = events.first().expect("signer failure event");
    assert_eq!(event.error_code.as_deref(), Some("signer_failed"));
    assert_request_setup_timings(event);
    Ok(())
}

#[tokio::test]
async fn eof_tail_error_yield_preserves_terminal_timings_on_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let incomplete_event = Bytes::from_static(
        b"event: message_stop\n\
          data: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}",
    );

    let cancelled_bus = TestLifecycleBus::new();
    let mut cancelled_rx = lifecycle_receiver(&cancelled_bus);
    let cancelled_exhausted = Arc::new(AtomicBool::new(false));
    let cancelled_lifecycle = lifecycle_with_terminal_and_upstream_dispatch(
        TerminalStrategy::FirstPick,
        Vec::new(),
        vec![upstream_record(upstream_id(1), "first")],
        Arc::new(Mutex::new(Vec::new())),
        false,
        TestState::default(),
        Arc::new(TailFrameDispatch {
            body: incomplete_event.clone(),
            exhausted: Arc::clone(&cancelled_exhausted),
        }),
    )
    .with_event_bus(cancelled_bus.bus_arc());
    let cancelled_request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","stream":true,"messages":[],"max_tokens":16}"#,
    ));
    let cancelled_auth = cancelled_lifecycle
        .authenticate(cancelled_request.headers())
        .await
        .expect("test request authenticates");
    let cancelled_response = cancelled_lifecycle
        .handle(cancelled_request, &cancelled_auth)
        .await?;
    let mut cancelled_body = cancelled_response.into_body();
    let original_frame = cancelled_body
        .frame()
        .await
        .expect("original incomplete frame exists")
        .expect("original incomplete frame succeeds")
        .into_data()
        .expect("original incomplete frame is data");
    assert_eq!(original_frame, incomplete_event);
    assert!(!cancelled_exhausted.load(Ordering::SeqCst));

    let tail_frame = cancelled_body
        .frame()
        .await
        .expect("EOF tail error frame exists")
        .expect("EOF tail error frame succeeds")
        .into_data()
        .expect("EOF tail error frame is data");
    assert!(
        cancelled_exhausted.load(Ordering::SeqCst),
        "tail error frame must be produced after the upstream body reaches EOS"
    );
    assert_tail_error_frame(&tail_frame);
    drop(cancelled_body);

    let (cancelled_terminal, stream_completed_before_cancel) =
        wait_for_terminal(&mut cancelled_rx).await;
    assert!(
        !stream_completed_before_cancel,
        "dropping at the EOF tail yield must not emit StreamCompleted"
    );
    let LifecycleEvent::RequestTerminated {
        reason,
        client_status,
        upstream_body_ms,
        finalize_ms,
        io_timings,
        ..
    } = cancelled_terminal
    else {
        unreachable!("wait_for_terminal returns RequestTerminated")
    };
    assert_eq!(client_status, StatusCode::OK.as_u16());
    assert_eq!(
        reason,
        TerminationReason::ErrorCode("upstream_stream_error".to_owned())
    );
    assert!(upstream_body_ms.is_some());
    assert!(finalize_ms.is_some());
    assert!(io_timings.response_body_process_ms.is_some());

    let completed_bus = TestLifecycleBus::new();
    let mut completed_rx = lifecycle_receiver(&completed_bus);
    let completed_exhausted = Arc::new(AtomicBool::new(false));
    let completed_lifecycle = lifecycle_with_terminal_and_upstream_dispatch(
        TerminalStrategy::FirstPick,
        Vec::new(),
        vec![upstream_record(upstream_id(2), "second")],
        Arc::new(Mutex::new(Vec::new())),
        false,
        TestState::default(),
        Arc::new(TailFrameDispatch {
            body: incomplete_event.clone(),
            exhausted: Arc::clone(&completed_exhausted),
        }),
    )
    .with_event_bus(completed_bus.bus_arc());
    let completed_request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","stream":true,"messages":[],"max_tokens":16}"#,
    ));
    let completed_auth = completed_lifecycle
        .authenticate(completed_request.headers())
        .await
        .expect("test request authenticates");
    let completed_response = completed_lifecycle
        .handle(completed_request, &completed_auth)
        .await?;
    let completed_body = completed_response
        .into_body()
        .collect()
        .await
        .expect("completed body collects")
        .to_bytes();
    assert!(completed_exhausted.load(Ordering::SeqCst));
    assert!(completed_body.starts_with(&incomplete_event));
    assert_tail_error_frame(&completed_body.slice(incomplete_event.len()..));

    let (completed_terminal, stream_completed_before_terminal) =
        wait_for_terminal(&mut completed_rx).await;
    assert!(
        stream_completed_before_terminal,
        "full tail consumption must emit StreamCompleted before RequestTerminated"
    );
    let LifecycleEvent::RequestTerminated {
        reason,
        client_status,
        upstream_body_ms,
        finalize_ms,
        ..
    } = completed_terminal
    else {
        unreachable!("wait_for_terminal returns RequestTerminated")
    };
    assert_eq!(client_status, StatusCode::OK.as_u16());
    assert_eq!(
        reason,
        TerminationReason::ErrorCode("upstream_stream_error".to_owned())
    );
    assert!(upstream_body_ms.is_some());
    assert!(finalize_ms.is_some());
    Ok(())
}

fn assert_tail_error_frame(frame: &Bytes) {
    let text = std::str::from_utf8(frame).expect("tail error frame is UTF-8");
    let event = text.lines().find_map(|line| line.strip_prefix("event:"));
    assert_eq!(event.map(str::trim), Some("error"));
    let data = text
        .lines()
        .find_map(|line| line.strip_prefix("data:"))
        .expect("tail error event has data");
    let error: serde_json::Value = serde_json::from_str(data).expect("tail error data is JSON");
    assert_eq!(error["type"], "error");
    assert_eq!(error["error"]["type"], "api_error");
}

fn lifecycle_receiver(test_bus: &TestLifecycleBus) -> broadcast::Receiver<LifecycleEvent> {
    test_bus.bus.subscribe_lifecycle()
}

async fn wait_for_terminal(rx: &mut broadcast::Receiver<LifecycleEvent>) -> (LifecycleEvent, bool) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut stream_completed = false;
        loop {
            let event = rx.recv().await.expect("lifecycle event delivered");
            if matches!(event, LifecycleEvent::StreamCompleted { .. }) {
                stream_completed = true;
            }
            if matches!(event, LifecycleEvent::RequestTerminated { .. }) {
                break (event, stream_completed);
            }
        }
    })
    .await
    .expect("request terminates")
}

fn assert_request_setup_timings(event: &cc_lb_storage_api::RequestEvent) {
    for timing in [event.json_parse_ms, event.prepare_signer_ms] {
        assert!(timing.is_some_and(|value| value.is_finite() && value >= 0.0));
    }
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
    file_name: &str,
) -> Result<SqliteStorage, Box<dyn std::error::Error>> {
    let database_url = format!("sqlite://{}", dir.path().join(file_name).display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await?;
    storage.initialize().await?;
    Ok(storage)
}

async fn wait_for_events(
    storage: &dyn RequestEventStore,
    expected: usize,
) -> Result<Vec<cc_lb_storage_api::RequestEvent>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let events = RequestEventStore::query_request_events(storage, 0, u64::MAX, 10).await?;
        if events.len() >= expected {
            return Ok(events);
        }
        if std::time::Instant::now() >= deadline {
            panic!("expected {expected} request event(s), got {}", events.len());
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn random_terminal_uses_seeded_rng_and_distributes_choices()
-> Result<(), Box<dyn std::error::Error>> {
    let first = upstream_id(1);
    let second = upstream_id(2);
    let third = upstream_id(3);
    let records = vec![
        upstream_record(first, "first"),
        upstream_record(second, "second"),
        upstream_record(third, "third"),
    ];

    let first_run = random_sequence([7; 32], records.clone()).await?;
    let second_run = random_sequence([7; 32], records).await?;

    assert_eq!(first_run, second_run);
    assert!(
        first_run.contains(&"first".to_owned()),
        "choices: {first_run:?}"
    );
    assert!(
        first_run.contains(&"second".to_owned()),
        "choices: {first_run:?}"
    );
    assert!(
        first_run.contains(&"third".to_owned()),
        "choices: {first_run:?}"
    );
    Ok(())
}

async fn random_sequence(
    seed: [u8; 32],
    records: Vec<UpstreamRecord>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let choices = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = lifecycle_with_terminal(
        TerminalStrategy::Random,
        Vec::new(),
        records,
        Arc::clone(&choices),
    )
    .with_terminal_rng_seed(seed);

    for _ in 0..24 {
        send_message(&lifecycle).await?;
    }

    Ok(choices.lock().expect("choices lock").clone())
}

async fn send_message(lifecycle: &Lifecycle) -> Result<(), Box<dyn std::error::Error>> {
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[],"max_tokens":16}"#,
    ));
    let auth = lifecycle
        .authenticate(request.headers())
        .await
        .expect("test request authenticates");
    let response = lifecycle.handle(request, &auth).await?;
    let (status, _headers, _body) = collect_body(response).await;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

fn lifecycle_with_terminal(
    terminal: TerminalStrategy,
    filters: Vec<Arc<dyn FilterPlugin>>,
    records: Vec<UpstreamRecord>,
    choices: Arc<Mutex<Vec<String>>>,
) -> Lifecycle {
    lifecycle_with_terminal_and_signer_failure(terminal, filters, records, choices, false)
}

fn lifecycle_with_terminal_and_signer_failure(
    terminal: TerminalStrategy,
    filters: Vec<Arc<dyn FilterPlugin>>,
    records: Vec<UpstreamRecord>,
    choices: Arc<Mutex<Vec<String>>>,
    signer_fails: bool,
) -> Lifecycle {
    let state = TestState::default();
    let dispatcher = Arc::new(MockDispatch {
        state: state.clone(),
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![StatusCode::OK].into()))),
    });
    lifecycle_with_terminal_and_upstream_dispatch(
        terminal,
        filters,
        records,
        choices,
        signer_fails,
        state,
        dispatcher,
    )
}

fn lifecycle_with_terminal_and_upstream_dispatch(
    terminal: TerminalStrategy,
    filters: Vec<Arc<dyn FilterPlugin>>,
    records: Vec<UpstreamRecord>,
    choices: Arc<Mutex<Vec<String>>>,
    signer_fails: bool,
    state: TestState,
    dispatcher: Arc<dyn UpstreamDispatch>,
) -> Lifecycle {
    let principal_view = principal_view(terminal, filters);
    let authn = TestAuthn::with_principal_view(state, Arc::clone(&principal_view));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(RecordingSignerFactory {
            choices,
            signer_fails,
        }))
        .principal_view(principal_view)
        .upstream_records(records)
        .build();

    Lifecycle::new_with_dynamic_view(
        authn.authn.clone(),
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(cc_lb_clock::SystemClock),
    )
}

struct TailFrameDispatch {
    body: Bytes,
    exhausted: Arc<AtomicBool>,
}

#[async_trait]
impl UpstreamDispatch for TailFrameDispatch {
    async fn dispatch(&self, _request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let mut response = Response::new(Body::new(TailFrameBody {
            body: Some(self.body.clone()),
            exhausted: Arc::clone(&self.exhausted),
        }));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
        Ok(response)
    }
}

struct TailFrameBody {
    body: Option<Bytes>,
    exhausted: Arc<AtomicBool>,
}

impl HttpBody for TailFrameBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(body) = self.body.take() {
            return Poll::Ready(Some(Ok(Frame::data(body))));
        }
        self.exhausted.store(true, Ordering::SeqCst);
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        false
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

fn principal_view(
    terminal: TerminalStrategy,
    filters: Vec<Arc<dyn FilterPlugin>>,
) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: filters,
        terminal,
        instantiation_error: None,
    });
    let mut chains: HashMap<String, PrincipalRoutingArtifacts> = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (Some(pipeline), DialectCache::Inherit),
    );
    Arc::new(PrincipalView::from_db(&[principal()], chains))
}

fn principal() -> PrincipalRecord {
    PrincipalRecord {
        id: Uuid::new_v4(),
        name: "principal-test".to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: TerminalStrategy::FirstPick,
        cache_keepalive: None,
    }
}

fn active_record() -> StoredApiKeyRecord {
    StoredApiKeyRecord {
        key_hash_b64: "key-test".to_owned(),
        status: KeyStatus::Active,
        ..StoredApiKeyRecord::default()
    }
}

fn upstream_record(id: Uuid, name: &str) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: StorageUpstreamKind::AnthropicApiKey,
        base_url: Some(Url::parse("http://upstream.local/").expect("test URL parses")),
        enabled: true,
        oauth_credentials: None,
        oauth_never_refresh: false,
        api_key_ciphertext: Some(Vec::new()),
        last_apply_error: None,
        last_apply_at_unix_secs: None,
        deleted_at_unix_secs: None,
        revision: 1,
        oauth_token_generation: 0,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        warmup_enabled: false,
        warmup_dialect_plugin: None,
        last_warmup_at_unix_secs: None,
    }
}

fn upstream_id(index: u128) -> Uuid {
    Uuid::from_u128(index)
}

struct RecordingSignerFactory {
    choices: Arc<Mutex<Vec<String>>>,
    signer_fails: bool,
}

impl ApiKeyAwareSignerFactory for RecordingSignerFactory {
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        self.choices
            .lock()
            .expect("choices lock")
            .push(router_chosen_upstream_name);
        Arc::new(RecordingSignerFactoryInner {
            signer_fails: self.signer_fails,
        })
    }
}

struct RecordingSignerFactoryInner {
    signer_fails: bool,
}

#[async_trait]
impl SignerFactory for RecordingSignerFactoryInner {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        if self.signer_fails {
            return Err(SignerError::MissingCredentials {
                reason: "forced signer build failure".to_owned(),
            });
        }
        Ok(Arc::new(RecordingSigner))
    }
}

struct RecordingSigner;

#[async_trait]
impl Signer for RecordingSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &cc_lb_upstream::UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

struct KeepFilter {
    kept_upstream_ids: Vec<Uuid>,
    calls: Arc<Mutex<Vec<Vec<Uuid>>>>,
}

impl FilterPlugin for KeepFilter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls.lock().expect("filter calls lock").push(
            candidates
                .iter()
                .map(|candidate| candidate.upstream_id)
                .collect(),
        );
        Ok(FilterOutput {
            kept_upstream_ids: self.kept_upstream_ids.clone(),
            reason: "kept terminal candidates".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }

    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }

    fn plugin_name(&self) -> &str {
        "keep-terminal-candidates"
    }
}
