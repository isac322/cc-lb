use crate::common::{
    TestAuthn, TestLifecycleBus, TestState, collect_body, messages_request, stored_request_events,
};

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_clock::TestClock;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::api_keys::principal_view::{DialectCache, PrincipalView, RouterPipelineCache};
use cc_lb_control::{
    DynamicViewBuilder, DynamicViewHolder, RequestEventBus, SubscriptionQuotaCacheLike,
};
use cc_lb_domain::{
    ANTHROPIC_WORKSPACE_ID_HEADER, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState,
    TerminalStrategy, Upstream,
};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, Body, DispatchError, Lifecycle, LifecycleConfig, LimitCostEstimator,
    UpstreamDispatch, spawn_lifecycle_limit_reconcile_subscriber,
    spawn_lifecycle_subscription_quota_subscriber,
};
use cc_lb_lifecycle::{LifecycleEvent, LimitDecisionKind};
use cc_lb_signer_anthropic_key::AnthropicKeySignerFactory;
use cc_lb_storage_api::principal::{Limit, LimitKind};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{
    KeyStatus, MetaStore, Storage as StorageTrait, StoredApiKeyRecord, SubscriptionQuotaSample,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use cc_lb_storage_sqlite::SqliteStorage;
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError,
};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use url::Url;
use uuid::Uuid;

const PRINCIPAL: &str = "principal-test";
const NOW: u64 = 1_700_000_000;
const REQUEST_BODY: &[u8] = br#"{"model":"claude-test","max_tokens":16,"messages":[]}"#;
const RATE_LIMIT_BODY: &[u8] =
    br#"{"type":"error","error":{"type":"rate_limit_error","message":"quota exhausted"}}"#;

/// How the scripted signer for one upstream behaves.
#[derive(Clone, Copy, Default)]
enum Credential {
    #[default]
    Healthy,
    BuildFails,
    SignExpired,
    RefreshOnUnauthorized,
    /// A body-sensitive custom signer: refreshes only when the 401 body it is
    /// handed contains the needle.
    RefreshIfBodyContains(&'static str),
    /// Mirrors the OAuth signer's header handling: bearer in, `x-api-key`
    /// and workspace out.
    Bearer,
    /// The production API-key signer.
    ApiKey {
        key: &'static str,
        workspace: Option<&'static str>,
    },
}

#[derive(Clone)]
enum Reply {
    Status(StatusCode),
    StatusWithHeaders(StatusCode, HeaderMap),
    Body {
        status: StatusCode,
        content_type: &'static str,
        body: Bytes,
    },
    Transport,
    /// An SSE 200 that yields `first`, then the upstream connection resets.
    SseThenDisconnect(Bytes),
    /// A response whose body is never-ending: `chunks_before_stall` 64 KiB
    /// chunks, then it stalls forever.
    Unending {
        status: StatusCode,
        chunks_before_stall: usize,
        probe: BodyProbe,
    },
}

/// What the lifecycle did with an [`Reply::Unending`] body.
#[derive(Clone, Default)]
struct BodyProbe {
    /// Chunks the body produced.
    polled: Arc<AtomicUsize>,
    /// Set once the body is dropped.
    dropped: Arc<AtomicBool>,
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn unending_body(chunks_before_stall: usize, probe: BodyProbe) -> Body {
    let guard = DropFlag(probe.dropped.clone());
    let stream = async_stream::stream! {
        let _guard = guard;
        for _ in 0..chunks_before_stall {
            probe.polled.fetch_add(1, Ordering::SeqCst);
            yield Ok::<Bytes, std::io::Error>(Bytes::from(vec![b'x'; 64 * 1024]));
        }
        std::future::pending::<()>().await;
    };
    Body::from_stream(stream)
}

struct TestUpstream {
    record: UpstreamRecord,
    credential: Credential,
    replies: Vec<Reply>,
    quota: Vec<SubscriptionQuotaCandidateSnapshot>,
}

impl TestUpstream {
    fn new(id: u128, name: &str, kind: UpstreamKind) -> Self {
        Self {
            record: UpstreamRecord {
                id: Uuid::from_u128(id),
                name: name.to_owned(),
                kind,
                base_url: Some(
                    Url::parse(&format!("http://{name}.invalid/")).expect("test URL parses"),
                ),
                enabled: true,
                revision: 1,
                ..UpstreamRecord::default()
            },
            credential: Credential::Healthy,
            replies: Vec::new(),
            quota: Vec::new(),
        }
    }

    fn replies(mut self, replies: Vec<Reply>) -> Self {
        self.replies = replies;
        self
    }

    fn credential(mut self, credential: Credential) -> Self {
        self.credential = credential;
        self
    }

    fn quota(mut self, quota: Vec<SubscriptionQuotaCandidateSnapshot>) -> Self {
        self.quota = quota;
        self
    }

    fn last_apply_error(mut self, error: &str) -> Self {
        self.record.last_apply_error = Some(error.to_owned());
        self
    }
}

fn oauth(id: u128, name: &str) -> TestUpstream {
    TestUpstream::new(id, name, UpstreamKind::AnthropicOauth)
}

fn api_key(id: u128, name: &str) -> TestUpstream {
    TestUpstream::new(id, name, UpstreamKind::AnthropicApiKey)
}

fn host(name: &str) -> String {
    format!("{name}.invalid")
}

struct Fixture {
    lifecycle: Lifecycle,
    dispatch: Arc<ScriptedDispatch>,
    quota_cache: Arc<RecordingQuotaCache>,
    dynamic_view: Arc<DynamicViewHolder>,
}

impl Fixture {
    fn new(upstreams: Vec<TestUpstream>) -> Self {
        Self::with_limits(upstreams, Vec::new())
    }

    fn with_limits(upstreams: Vec<TestUpstream>, limits: Vec<Limit>) -> Self {
        let mut records = Vec::new();
        let mut credentials = HashMap::new();
        let mut replies = HashMap::new();
        let mut quotas = HashMap::new();
        for upstream in upstreams {
            credentials.insert(upstream.record.name.clone(), upstream.credential);
            replies.insert(
                host(&upstream.record.name),
                VecDeque::from(upstream.replies),
            );
            quotas.insert(upstream.record.id, upstream.quota);
            records.push(upstream.record);
        }
        let quota_cache = Arc::new(RecordingQuotaCache {
            snapshots: Mutex::new(quotas),
            ..RecordingQuotaCache::default()
        });
        let dispatch = Arc::new(ScriptedDispatch {
            replies: Mutex::new(replies),
            hosts: Mutex::new(Vec::new()),
            headers: Mutex::new(Vec::new()),
        });
        let principal_view = principal_view(limits);
        let authn =
            TestAuthn::with_principal_view(TestState::default(), Arc::clone(&principal_view));
        let view = DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(ScriptedSignerFactory {
                credentials: Arc::new(credentials),
            }))
            .principal_view(principal_view)
            .subscription_quota_cache(quota_cache.clone())
            .subscription_quota_routing_max_staleness_secs(60)
            .upstream_records(records)
            .build();
        let dynamic_view = Arc::new(DynamicViewHolder::new(view));
        let lifecycle = Lifecycle::new_with_dynamic_view(
            authn.authn,
            dynamic_view.clone(),
            dispatch.clone(),
            LifecycleConfig::default(),
            Arc::new(TestClock::new_at_secs(NOW)),
        );
        Self {
            lifecycle,
            dispatch,
            quota_cache,
            dynamic_view,
        }
    }

    fn with_lifecycle(mut self, configure: impl FnOnce(Lifecycle) -> Lifecycle) -> Self {
        self.lifecycle = configure(self.lifecycle);
        self
    }

    async fn send(&self) -> (StatusCode, Bytes) {
        self.send_request(messages_request(Bytes::from_static(REQUEST_BODY)))
            .await
    }

    async fn send_request(&self, request: Request<Bytes>) -> (StatusCode, Bytes) {
        let auth = self
            .lifecycle
            .authenticate(request.headers())
            .await
            .expect("test request authenticates");
        let response = self
            .lifecycle
            .handle(request, &auth)
            .await
            .expect("lifecycle handles request");
        let (status, _headers, body) = collect_body(response).await;
        (status, body)
    }

    fn hosts(&self) -> Vec<String> {
        self.dispatch
            .hosts
            .lock()
            .expect("dispatch hosts lock")
            .clone()
    }

    /// Outbound headers of every dispatched attempt, in dispatch order.
    fn dispatched_headers(&self) -> Vec<HeaderMap> {
        self.dispatch
            .headers
            .lock()
            .expect("dispatch headers lock")
            .clone()
    }
}

fn principal_view(limits: Vec<Limit>) -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![Arc::new(SubscriptionPreferenceFilter::new())],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    Arc::new(PrincipalView::for_tests(
        PRINCIPAL,
        true,
        vec!["*".to_owned()],
        limits,
        HashMap::from([(
            PRINCIPAL.to_owned(),
            (Some(pipeline), DialectCache::Inherit),
        )]),
    ))
}

fn known_base_quota() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![
        quota_snapshot(
            SubscriptionQuotaWindow::FiveHour,
            0.1,
            "allowed",
            NOW + 1_800,
        ),
        quota_snapshot(
            SubscriptionQuotaWindow::SevenDay,
            0.1,
            "allowed",
            NOW + 60_480,
        ),
    ]
}

fn quota_snapshot(
    window: SubscriptionQuotaWindow,
    utilization: f64,
    status: &str,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.as_str().to_owned(),
        state: SubscriptionQuotaDataState::Fresh,
        source: Some("header".to_owned()),
        utilization: Some(utilization),
        status: Some(status.to_owned()),
        resets_at_unix_secs: Some(resets_at_unix_secs),
        observed_at_unix_millis: Some(NOW * 1_000),
        max_staleness_secs: 60,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}

/// Stand-in for the server quota cache: each ingested header sample becomes
/// the fresh snapshot for its window, which is what routing reads next.
#[derive(Default)]
struct RecordingQuotaCache {
    snapshots: Mutex<HashMap<Uuid, Vec<SubscriptionQuotaCandidateSnapshot>>>,
    samples: Mutex<Vec<SubscriptionQuotaSample>>,
    ingested: tokio::sync::Notify,
}

impl SubscriptionQuotaCacheLike for RecordingQuotaCache {
    fn upsert_observation(&self, record: &SubscriptionQuotaSample) {
        let snapshot = SubscriptionQuotaCandidateSnapshot {
            utilization: record.utilization,
            status: record.status.map(|status| status.as_str().to_owned()),
            resets_at_unix_secs: record.resets_at_unix_secs,
            ..quota_snapshot(record.window, 0.0, "allowed", 0)
        };
        let mut snapshots = self.snapshots.lock().expect("quota snapshots lock");
        let windows = snapshots.entry(record.upstream_id).or_default();
        windows.retain(|existing| existing.window != snapshot.window);
        windows.push(snapshot);
        self.samples
            .lock()
            .expect("quota samples lock")
            .push(record.clone());
        self.ingested.notify_one();
    }

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        _now_unix_millis: u64,
        _max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        self.snapshots
            .lock()
            .expect("quota snapshots lock")
            .get(&upstream_id)
            .cloned()
            .unwrap_or_default()
    }
}

struct ScriptedSignerFactory {
    credentials: Arc<HashMap<String, Credential>>,
}

impl ApiKeyAwareSignerFactory for ScriptedSignerFactory {
    fn with_router_choice(&self, router_chosen_upstream_name: String) -> Arc<dyn SignerFactory> {
        Arc::new(ScriptedSignerBuilder {
            credential: self
                .credentials
                .get(&router_chosen_upstream_name)
                .copied()
                .unwrap_or_default(),
        })
    }
}

struct ScriptedSignerBuilder {
    credential: Credential,
}

#[async_trait]
impl SignerFactory for ScriptedSignerBuilder {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        match self.credential {
            Credential::BuildFails => Err(SignerError::MissingCredentials {
                reason: "oauth credentials unavailable".to_owned(),
            }),
            Credential::ApiKey { key, workspace } => {
                AnthropicKeySignerFactory::new(key, workspace)?
                    .build(upstream)
                    .await
            }
            credential => Ok(Arc::new(ScriptedSigner { credential })),
        }
    }
}

struct ScriptedSigner {
    credential: Credential,
}

#[async_trait]
impl Signer for ScriptedSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        match self.credential {
            Credential::SignExpired => Err(SignerError::ExpiredToken {
                reason: "access token expired".to_owned(),
            }),
            Credential::Bearer => {
                let headers = shaped.headers_mut();
                headers.remove("x-api-key");
                headers.remove(ANTHROPIC_WORKSPACE_ID_HEADER);
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_static("Bearer oauth-token"),
                );
                Ok(SignedRequest::from_shaped(shaped, capability))
            }
            _ => Ok(SignedRequest::from_shaped(shaped, capability)),
        }
    }

    async fn on_unauthorized(&self, err: &UpstreamError) -> RetryDecision {
        let refresh = match self.credential {
            Credential::RefreshOnUnauthorized => true,
            Credential::RefreshIfBodyContains(needle) => matches!(
                err,
                UpstreamError::Unauthorized { body: Some(body), .. }
                    if body.windows(needle.len()).any(|window| window == needle.as_bytes())
            ),
            _ => false,
        };
        if refresh {
            RetryDecision::Refresh {
                new_signer: Arc::new(ScriptedSigner {
                    credential: Credential::Healthy,
                }),
            }
        } else {
            RetryDecision::Fail
        }
    }
}

struct ScriptedDispatch {
    replies: Mutex<HashMap<String, VecDeque<Reply>>>,
    hosts: Mutex<Vec<String>>,
    headers: Mutex<Vec<HeaderMap>>,
}

#[async_trait]
impl UpstreamDispatch for ScriptedDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let host = request
            .url()
            .host_str()
            .expect("test request has host")
            .to_owned();
        self.hosts
            .lock()
            .expect("dispatch hosts lock")
            .push(host.clone());
        self.headers
            .lock()
            .expect("dispatch headers lock")
            .push(request.headers().clone());
        let reply = self
            .replies
            .lock()
            .expect("dispatch replies lock")
            .get_mut(&host)
            .and_then(VecDeque::pop_front)
            .unwrap_or(Reply::Status(StatusCode::OK));
        let (status, headers) = match reply {
            Reply::Status(status) => (status, HeaderMap::new()),
            Reply::StatusWithHeaders(status, headers) => (status, headers),
            Reply::Body {
                status,
                content_type,
                body,
            } => {
                let mut response = Response::new(Body::from(body));
                *response.status_mut() = status;
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
                return Ok(response);
            }
            Reply::Transport => {
                return Err(DispatchError::Transport {
                    source: Box::new(std::io::Error::other("connection reset")),
                });
            }
            Reply::Unending {
                status,
                chunks_before_stall,
                probe,
            } => {
                let mut response = Response::new(unending_body(chunks_before_stall, probe));
                *response.status_mut() = status;
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                return Ok(response);
            }
            Reply::SseThenDisconnect(first) => {
                let stream = async_stream::stream! {
                    yield Ok::<Bytes, std::io::Error>(first);
                    yield Err(std::io::Error::other("upstream connection reset"));
                };
                let mut response = Response::new(Body::from_stream(stream));
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
                return Ok(response);
            }
        };
        let body: &'static [u8] = match status.as_u16() {
            200 => br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#,
            429 => RATE_LIMIT_BODY,
            401 => br#"{"type":"error","error":{"type":"authentication_error","message":"invalid token"}}"#,
            400 => br#"{"type":"error","error":{"type":"invalid_request_error","message":"bad request"}}"#,
            529 => br#"{"type":"error","error":{"type":"overloaded_error","message":"overloaded"}}"#,
            _ => br#"{"type":"error","error":{"type":"api_error","message":"internal error"}}"#,
        };
        let mut response = Response::new(Body::from(Bytes::from_static(body)));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        Ok(response)
    }
}

async fn sqlite_storage(
    dir: &tempfile::TempDir,
) -> Result<Arc<SqliteStorage>, Box<dyn std::error::Error>> {
    let database_url = format!(
        "sqlite://{}",
        dir.path().join("failover-request-events.sqlite").display()
    );
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock))
            .await?;
    storage.initialize().await?;
    Ok(Arc::new(storage))
}

async fn wait_for_final_request_event(
    update_rx: &mut tokio::sync::broadcast::Receiver<cc_lb_request_log::RequestEventUpdate>,
) {
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
}

/// Collect `(attempt_num, upstream_id)` for every `UpstreamAttempt` until the
/// request terminates.
async fn attempts_until_terminated(
    events: &mut tokio::sync::mpsc::Receiver<LifecycleEvent>,
) -> Vec<(u32, Uuid)> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut attempts = Vec::new();
        loop {
            match events
                .recv()
                .await
                .expect("lifecycle event channel remains open")
            {
                LifecycleEvent::UpstreamAttempt {
                    attempt_num,
                    upstream_id,
                    ..
                } => attempts.push((attempt_num, upstream_id)),
                LifecycleEvent::RequestTerminated { .. } => break attempts,
                _ => {}
            }
        }
    })
    .await
    .expect("terminal lifecycle event arrives")
}

fn rejected_five_hour_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "anthropic-ratelimit-unified-5h-status",
        HeaderValue::from_static("rejected"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-5h-utilization",
        HeaderValue::from_static("1.0"),
    );
    headers.insert(
        "anthropic-ratelimit-unified-5h-reset",
        HeaderValue::from_str(&(NOW + 3_600).to_string()).expect("reset header value"),
    );
    headers
}

#[tokio::test]
async fn oauth_429_fails_over_to_api_key_same_request() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = sqlite_storage(&dir).await?;
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        api_key(2, "key"),
    ])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(test_bus.bus_arc()));

    let (status, _body) = fixture.send().await;
    wait_for_final_request_event(&mut update_rx).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
    let events = stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.upstream_id, Some(Uuid::from_u128(2)));
    assert_eq!(event.upstream_name.as_deref(), Some("key"));
    assert_eq!(event.status, StatusCode::OK.as_u16());
    assert_eq!(event.error_code, None);
    assert_eq!(event.upstream_error_type, None);
    assert_eq!(event.upstream_error_message, None);
    Ok(())
}

/// SEC-FAILOVER-DOS-03: a failed-over 429 body is never read. A stalled or
/// never-ending body cannot hold the fallback dispatch, and no byte of it is
/// buffered. Time is paused: without the fix the idle runtime auto-advances
/// to the guard deadline and the test fails instead of hanging.
#[tokio::test(start_paused = true)]
async fn oauth_429_unending_body_fails_over_without_reading_it() {
    for chunks_before_stall in [0, 256] {
        let probe = BodyProbe::default();
        let fixture = Fixture::new(vec![
            oauth(1, "oauth").replies(vec![Reply::Unending {
                status: StatusCode::TOO_MANY_REQUESTS,
                chunks_before_stall,
                probe: probe.clone(),
            }]),
            api_key(2, "key"),
        ]);

        let (status, _body) = tokio::time::timeout(Duration::from_secs(600), fixture.send())
            .await
            .expect("the failed body never holds the fallback");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
        assert_eq!(probe.polled.load(Ordering::SeqCst), 0);
        assert!(probe.dropped.load(Ordering::SeqCst));
    }
}

/// SEC-FAILOVER-DOS-03: the refreshed-401 failover discards its body the
/// same way.
#[tokio::test(start_paused = true)]
async fn oauth_refreshed_401_unending_body_fails_over_without_reading_it() {
    for chunks_before_stall in [0, 256] {
        let probe = BodyProbe::default();
        let fixture = Fixture::new(vec![
            oauth(1, "oauth")
                .credential(Credential::RefreshOnUnauthorized)
                .replies(vec![
                    Reply::Status(StatusCode::UNAUTHORIZED),
                    Reply::Unending {
                        status: StatusCode::UNAUTHORIZED,
                        chunks_before_stall,
                        probe: probe.clone(),
                    },
                ]),
            api_key(2, "key"),
        ]);

        let (status, _body) = tokio::time::timeout(Duration::from_secs(600), fixture.send())
            .await
            .expect("the failed body never holds the fallback");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            fixture.hosts(),
            vec![host("oauth"), host("oauth"), host("key")]
        );
        assert_eq!(probe.polled.load(Ordering::SeqCst), 0);
        assert!(probe.dropped.load(Ordering::SeqCst));
    }
}

/// Review r3: the first 401's body is read before the refresh/failover
/// decision, but only within a short deadline and the error-body byte cap.
/// A stalled (`0` chunks) or oversized (`1` × 64 KiB chunk) body must not
/// spend the request timeout before the API-key alternative is tried, and the
/// body-sensitive signer sees no body instead of a truncated one.
#[tokio::test(start_paused = true)]
async fn initial_401_stalled_body_fails_over_promptly() {
    for chunks_before_stall in [0, 1] {
        let probe = BodyProbe::default();
        let fixture = Fixture::new(vec![
            oauth(1, "oauth")
                .credential(Credential::RefreshIfBodyContains("x"))
                .replies(vec![Reply::Unending {
                    status: StatusCode::UNAUTHORIZED,
                    chunks_before_stall,
                    probe: probe.clone(),
                }]),
            api_key(2, "key"),
        ]);

        let started = tokio::time::Instant::now();
        let (status, _body) = tokio::time::timeout(Duration::from_secs(600), fixture.send())
            .await
            .expect("the stalled 401 body never holds the fallback");

        assert!(
            started.elapsed() <= Duration::from_secs(10),
            "chunks {chunks_before_stall}: fallback waited {:?}",
            started.elapsed()
        );
        assert_eq!(status, StatusCode::OK);
        // No refresh: the signer never saw the partial `x` bytes.
        assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
        assert_eq!(probe.polled.load(Ordering::SeqCst), chunks_before_stall);
        assert!(probe.dropped.load(Ordering::SeqCst));
    }
}

/// Without an alternative, an unreadable 401 body is not relayed in part:
/// the client gets a cc-lb-generated 502 instead of a 401 with cut bytes.
#[tokio::test(start_paused = true)]
async fn initial_401_stalled_body_without_alternative_relays_502() {
    let probe = BodyProbe::default();
    let fixture = Fixture::new(vec![oauth(1, "oauth").replies(vec![Reply::Unending {
        status: StatusCode::UNAUTHORIZED,
        chunks_before_stall: 0,
        probe: probe.clone(),
    }])]);

    let (status, body) = tokio::time::timeout(Duration::from_secs(600), fixture.send())
        .await
        .expect("the stalled 401 body is bounded");

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("generated JSON error");
    assert_eq!(body["error"]["type"], "api_error");
    assert_eq!(fixture.hosts(), vec![host("oauth")]);
    assert!(probe.dropped.load(Ordering::SeqCst));
}

/// A normal 401 with no alternative is still relayed byte-for-byte.
#[tokio::test]
async fn initial_401_normal_body_relays_unchanged() {
    const BODY: &[u8] =
        br#"{"type":"error","error":{"type":"authentication_error","message":"bad key"}}"#;
    let fixture = Fixture::new(vec![oauth(1, "oauth").replies(vec![Reply::Body {
        status: StatusCode::UNAUTHORIZED,
        content_type: "application/json",
        body: Bytes::from_static(BODY),
    }])]);

    let (status, body) = fixture.send().await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body.as_ref(), BODY);
    assert_eq!(fixture.hosts(), vec![host("oauth")]);
}

/// A body-sensitive custom signer still receives a normal 401 body and can
/// refresh on it.
#[tokio::test]
async fn initial_401_body_reaches_body_sensitive_signer() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth")
            .credential(Credential::RefreshIfBodyContains("invalid token"))
            .replies(vec![
                Reply::Status(StatusCode::UNAUTHORIZED),
                Reply::Status(StatusCode::OK),
            ]),
        api_key(2, "key"),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("oauth")]);
}

#[tokio::test]
async fn oauth_429_headers_still_ingested_on_failover() {
    let test_bus = TestLifecycleBus::new();
    let quota_rx = test_bus.bus.attach_lifecycle_subscription_quota(64);
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::StatusWithHeaders(
            StatusCode::TOO_MANY_REQUESTS,
            rejected_five_hour_headers(),
        )]),
        api_key(2, "key"),
    ])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(test_bus.bus_arc()));
    let subscriber = spawn_lifecycle_subscription_quota_subscriber(
        quota_rx,
        Some(fixture.quota_cache.clone() as Arc<dyn SubscriptionQuotaCacheLike>),
        None,
    );

    let (first_status, _body) = fixture.send().await;
    tokio::time::timeout(
        Duration::from_secs(3),
        fixture.quota_cache.ingested.notified(),
    )
    .await
    .expect("failed attempt headers are ingested");

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
    let oauth_rejected = fixture
        .quota_cache
        .samples
        .lock()
        .expect("quota samples lock")
        .iter()
        .any(|sample| {
            sample.upstream_id == Uuid::from_u128(1)
                && sample.window == SubscriptionQuotaWindow::FiveHour
                && sample.status == Some(SubscriptionQuotaStatus::Rejected)
        });
    assert!(
        oauth_rejected,
        "the OAuth 429 quota headers are attributed to the OAuth upstream"
    );

    let (second_status, _body) = fixture.send().await;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(
        fixture.hosts(),
        vec![host("oauth"), host("key"), host("key")],
        "the next request routes straight to the API key"
    );
    subscriber.shutdown().await;
}

#[tokio::test]
async fn oauth_401_refresh_fail_fails_over_to_api_key() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::UNAUTHORIZED)]),
        api_key(2, "key"),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
}

#[tokio::test]
async fn oauth_401_after_refresh_retry_fails_over() {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(64);
    let fixture = Fixture::new(vec![
        oauth(1, "oauth")
            .credential(Credential::RefreshOnUnauthorized)
            .replies(vec![
                Reply::Status(StatusCode::UNAUTHORIZED),
                Reply::Status(StatusCode::UNAUTHORIZED),
            ]),
        api_key(2, "key"),
    ])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(test_bus.bus_arc()));

    let (status, _body) = fixture.send().await;
    let attempts = attempts_until_terminated(&mut lifecycle_events).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        fixture.hosts(),
        vec![host("oauth"), host("oauth"), host("key")]
    );
    assert_eq!(
        attempts,
        vec![
            (1, Uuid::from_u128(1)),
            (2, Uuid::from_u128(1)),
            (3, Uuid::from_u128(2)),
        ]
    );
}

#[tokio::test]
async fn oauth_sign_expired_fails_over_without_dispatch() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").credential(Credential::SignExpired),
        api_key(2, "key"),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("key")]);
}

#[tokio::test]
async fn oauth_signer_build_failure_fails_over() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").credential(Credential::BuildFails),
        api_key(2, "key"),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("key")]);
}

#[tokio::test]
async fn second_oauth_tried_before_api_key() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth-a")
            .quota(known_base_quota())
            .replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        oauth(2, "oauth-b"),
        api_key(3, "key"),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth-a"), host("oauth-b")]);
}

#[tokio::test]
async fn last_attempt_reserved_for_api_key() {
    let rate_limited = || vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)];
    let fixture = Fixture::new(vec![
        oauth(1, "oauth-1").replies(rate_limited()),
        oauth(2, "oauth-2").replies(rate_limited()),
        oauth(3, "oauth-3").replies(rate_limited()),
        api_key(4, "key"),
    ]);

    let (status, _body) = fixture.send().await;
    let hosts = fixture.hosts();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        hosts.len(),
        3,
        "three distinct upstreams at most: {hosts:?}"
    );
    assert!(hosts[0].starts_with("oauth-"));
    assert!(hosts[1].starts_with("oauth-"));
    assert_ne!(hosts[0], hosts[1]);
    assert_eq!(hosts[2], host("key"));
}

#[tokio::test]
async fn single_oauth_429_without_alternative_relays_original_429()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = sqlite_storage(&dir).await?;
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
    ])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(test_bus.bus_arc()));

    let (status, body) = fixture.send().await;
    wait_for_final_request_event(&mut update_rx).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body.as_ref(), RATE_LIMIT_BODY);
    assert_eq!(fixture.hosts(), vec![host("oauth")]);
    let events = stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, StatusCode::TOO_MANY_REQUESTS.as_u16());
    assert_eq!(events[0].upstream_id, Some(Uuid::from_u128(1)));
    Ok(())
}

#[tokio::test]
async fn all_failover_targets_fail_relays_last_failure() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        api_key(2, "key").replies(vec![Reply::Status(StatusCode::UNAUTHORIZED)]),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
}

#[tokio::test]
async fn failover_signer_build_failure_does_not_inherit_prior_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = sqlite_storage(&dir).await?;
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        api_key(2, "key").credential(Credential::BuildFails),
    ])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(test_bus.bus_arc()));

    let (status, _body) = fixture.send().await;
    let partials = tokio::time::timeout(Duration::from_secs(3), async {
        let mut partials = Vec::new();
        loop {
            match update_rx.recv().await.expect("request update delivered") {
                cc_lb_request_log::RequestEventUpdate::Partial(partial) => partials.push(partial),
                cc_lb_request_log::RequestEventUpdate::Final(_) => break partials,
            }
        }
    })
    .await
    .expect("final request event arrives");

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(fixture.hosts(), vec![host("oauth")]);
    let oauth_id = Uuid::from_u128(1);
    let key_id = Uuid::from_u128(2);
    assert!(
        partials
            .iter()
            .any(|partial| partial.upstream_id == Some(oauth_id)
                && partial.upstream_response_status == Some(429)),
        "the OAuth 429 is observed live before failover: {partials:?}"
    );
    let key_partials = partials
        .iter()
        .filter(|partial| partial.upstream_id == Some(key_id))
        .collect::<Vec<_>>();
    assert!(
        !key_partials.is_empty(),
        "the API-key route is published live"
    );
    assert!(
        key_partials
            .iter()
            .all(|partial| partial.upstream_response_status.is_none()),
        "the API-key route never carries the OAuth response status: {key_partials:?}"
    );
    let events = stored_request_events(storage.as_ref()).await?;
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.upstream_id, Some(key_id));
    assert_eq!(event.status, StatusCode::BAD_GATEWAY.as_u16());
    assert_eq!(event.error_code.as_deref(), Some("signer_failed"));
    assert_eq!(event.shape_ms, None);
    assert_eq!(event.sign_ms, None);
    assert_eq!(event.upstream_ttfb_ms, None);
    assert_eq!(event.dns_ms, None);
    assert_eq!(event.connect_ms, None);
    assert_eq!(event.upstream_error_type, None);
    Ok(())
}

#[tokio::test]
async fn no_failover_on_400_5xx_529_transport() {
    let cases = [
        (
            Reply::Status(StatusCode::BAD_REQUEST),
            StatusCode::BAD_REQUEST,
        ),
        (
            Reply::Status(StatusCode::INTERNAL_SERVER_ERROR),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            Reply::Status(StatusCode::from_u16(529).expect("529 is a valid status")),
            StatusCode::from_u16(529).expect("529 is a valid status"),
        ),
        (Reply::Transport, StatusCode::BAD_GATEWAY),
    ];
    for (reply, expected_status) in cases {
        let fixture = Fixture::new(vec![
            oauth(1, "oauth").replies(vec![reply]),
            api_key(2, "key"),
        ]);

        let (status, _body) = fixture.send().await;

        assert_eq!(status, expected_status);
        assert_eq!(fixture.hosts(), vec![host("oauth")]);
    }
}

#[derive(Default)]
struct CountingLimitCostEstimator {
    calls: Mutex<u32>,
}

impl LimitCostEstimator for CountingLimitCostEstimator {
    fn estimate_max(
        &self,
        _model: &str,
        _max_input: u64,
        _max_output: u64,
        _service_tier: Option<&str>,
    ) -> Option<i64> {
        *self.calls.lock().expect("estimator calls lock") += 1;
        Some(1)
    }
}

const LIMIT_KEY: &str = "key-test";
const REQUEST_CAP: i64 = 10;
const TOKEN_CAP: i64 = 1_000_000;

fn failover_limits() -> Vec<Limit> {
    vec![
        Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: REQUEST_CAP,
        },
        Limit {
            kind: LimitKind::TotalTokens,
            window_secs: 60,
            cap_micros: TOKEN_CAP,
        },
        Limit {
            kind: LimitKind::Concurrent,
            window_secs: 0,
            cap_micros: 2,
        },
    ]
}

fn remaining(headers: &[(String, String)], name: &str) -> i64 {
    headers
        .iter()
        .find(|(header, _)| header == name)
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or_else(|| panic!("{name} header present in {headers:?}"))
}

struct LimitOutcome {
    status: StatusCode,
    reserved_decisions: u32,
    estimator_calls: u32,
    requests_remaining: i64,
    tokens_remaining: i64,
    concurrent_in_flight: u32,
}

/// Drive one request through the real limit engine and the production
/// reconcile subscriber. Shutting the subscriber down after the terminal event
/// drains its channel and force-finalizes pending reconciliations, so the
/// engine state read afterwards is final without waiting on a timer.
async fn run_with_limits(upstreams: Vec<TestUpstream>) -> (Fixture, LimitOutcome) {
    run_with_limits_request(
        upstreams,
        messages_request(Bytes::from_static(REQUEST_BODY)),
        ClientRead::Full,
    )
    .await
}

/// How the client consumes the response in [`run_with_limits_request`].
#[derive(Clone, Copy)]
enum ClientRead {
    Full,
    /// The client hangs up after the response head, before reading the body.
    CancelBeforeBody,
}

async fn run_with_limits_request(
    upstreams: Vec<TestUpstream>,
    request: Request<Bytes>,
    client_read: ClientRead,
) -> (Fixture, LimitOutcome) {
    let test_bus = TestLifecycleBus::new();
    let mut lifecycle_events = test_bus.bus.attach_lifecycle_writer(64);
    let reconcile_rx = test_bus.bus.attach_lifecycle_limit_reconcile(64);
    let estimator = Arc::new(CountingLimitCostEstimator::default());
    let concurrency = Arc::new(KeyConcurrencyManager::new());
    let limit_engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
    let reconcile = spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, limit_engine.clone());
    let fixture = Fixture::with_limits(upstreams, failover_limits()).with_lifecycle(|lifecycle| {
        lifecycle
            .with_static_limit_subject(
                limit_engine.clone(),
                PRINCIPAL.to_owned(),
                LIMIT_KEY.to_owned(),
                StoredApiKeyRecord {
                    key_hash_b64: LIMIT_KEY.to_owned(),
                    status: KeyStatus::Active,
                    ..StoredApiKeyRecord::default()
                },
            )
            .with_limit_cost_estimator(estimator.clone())
            .with_event_bus(test_bus.bus_arc())
    });

    let status = match client_read {
        ClientRead::Full => fixture.send_request(request).await.0,
        ClientRead::CancelBeforeBody => {
            let auth = fixture
                .lifecycle
                .authenticate(request.headers())
                .await
                .expect("test request authenticates");
            let response = fixture
                .lifecycle
                .handle(request, &auth)
                .await
                .expect("lifecycle handles request");
            let status = response.status();
            drop(response);
            status
        }
    };
    let reserved_decisions = tokio::time::timeout(Duration::from_secs(3), async {
        let mut reserved = 0_u32;
        loop {
            match lifecycle_events
                .recv()
                .await
                .expect("lifecycle event channel remains open")
            {
                LifecycleEvent::LimitDecision {
                    decision: LimitDecisionKind::Reserved { .. },
                    ..
                } => reserved += 1,
                LifecycleEvent::RequestTerminated { .. } => break reserved,
                _ => {}
            }
        }
    })
    .await
    .expect("terminal lifecycle event arrives");
    reconcile.shutdown().await;

    let headers = limit_engine.headers_for(LIMIT_KEY, PRINCIPAL);
    let outcome = LimitOutcome {
        status,
        reserved_decisions,
        estimator_calls: *estimator.calls.lock().expect("estimator calls lock"),
        requests_remaining: remaining(&headers, "anthropic-ratelimit-requests-remaining"),
        tokens_remaining: remaining(&headers, "anthropic-ratelimit-tokens-remaining"),
        concurrent_in_flight: concurrency.current(LIMIT_KEY),
    };
    (fixture, outcome)
}

#[tokio::test]
async fn limit_reservation_reconciled_once_across_failover() {
    let (fixture, outcome) = run_with_limits(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        api_key(2, "key"),
    ])
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
    assert_eq!(outcome.reserved_decisions, 1);
    assert_eq!(outcome.estimator_calls, 1);
    assert_eq!(
        outcome.requests_remaining,
        REQUEST_CAP - 1,
        "the failed-over request counts once"
    );
    assert_eq!(
        outcome.tokens_remaining,
        TOKEN_CAP - 2,
        "the reservation is reconciled to the API key's actual usage"
    );
    assert_eq!(outcome.concurrent_in_flight, 0);
}

#[tokio::test]
async fn limit_reservation_matches_single_upstream_relay_when_failover_exhausted() {
    // A relayed upstream error refunds its reservation on the terminal event.
    // An exhausted failover must leave exactly the state a single relayed 429
    // leaves: the reservation refunded once, not held per tried upstream.
    let (baseline_fixture, baseline) = run_with_limits(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
    ])
    .await;
    let (fixture, outcome) = run_with_limits(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        api_key(2, "key").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
    ])
    .await;

    assert_eq!(baseline.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(baseline_fixture.hosts(), vec![host("oauth")]);
    assert_eq!(outcome.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
    assert_eq!(outcome.reserved_decisions, 1);
    assert_eq!(outcome.estimator_calls, 1);
    assert_eq!(outcome.requests_remaining, REQUEST_CAP);
    assert_eq!(outcome.requests_remaining, baseline.requests_remaining);
    assert_eq!(outcome.tokens_remaining, TOKEN_CAP);
    assert_eq!(outcome.tokens_remaining, baseline.tokens_remaining);
    assert_eq!(outcome.concurrent_in_flight, 0);
    assert_eq!(baseline.concurrent_in_flight, 0);
}

/// Asserts the reservation was refunded in full: no request, token, or
/// concurrent slot stays held after the terminal event.
fn assert_refunded(outcome: &LimitOutcome, label: &str) {
    assert_eq!(outcome.reserved_decisions, 1, "{label}: one reservation");
    assert_eq!(
        outcome.requests_remaining, REQUEST_CAP,
        "{label}: request refunded"
    );
    assert_eq!(
        outcome.tokens_remaining, TOKEN_CAP,
        "{label}: tokens refunded"
    );
    assert_eq!(
        outcome.concurrent_in_flight, 0,
        "{label}: concurrent slot released"
    );
}

#[tokio::test]
async fn limit_reservation_refunded_after_buffered_upstream_500() {
    let (fixture, outcome) = run_with_limits(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::INTERNAL_SERVER_ERROR)]),
    ])
    .await;

    assert_eq!(outcome.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(fixture.hosts(), vec![host("oauth")]);
    assert_refunded(&outcome, "buffered 500");
}

#[tokio::test]
async fn limit_reservation_refunded_after_buffered_upstream_error_body() {
    let (_fixture, outcome) = run_with_limits(vec![oauth(1, "oauth").replies(vec![Reply::Body {
        status: StatusCode::BAD_REQUEST,
        content_type: "application/json",
        body: Bytes::from_static(
            br#"{"type":"error","error":{"type":"invalid_request_error","message":"bad request"}}"#,
        ),
    }])])
    .await;

    assert_eq!(outcome.status, StatusCode::BAD_REQUEST);
    assert_refunded(&outcome, "buffered 400");
}

#[tokio::test]
async fn limit_reservation_refunded_after_streaming_upstream_500() {
    let (_fixture, outcome) = run_with_limits_request(
        vec![oauth(1, "oauth").replies(vec![stream_error_reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            br#"{"type":"error","error":{"type":"api_error","message":"internal error"}}"#.to_vec(),
        )])],
        stream_request(),
        ClientRead::Full,
    )
    .await;

    assert_eq!(outcome.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_refunded(&outcome, "streaming 500");
}

#[tokio::test]
async fn limit_reservation_refunded_after_truncated_sse() {
    let truncated = format!(
        "{SSE_MESSAGE_START}event: content_block_delta\ndata: {{\"type\":\"content_block_delta\""
    );
    let (_fixture, outcome) = run_with_limits_request(
        vec![oauth(1, "oauth").replies(vec![stream_error_reply(
            StatusCode::OK,
            truncated.into_bytes(),
        )])],
        stream_request(),
        ClientRead::Full,
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_refunded(&outcome, "truncated SSE");
}

#[tokio::test]
async fn limit_reservation_refunded_after_mid_stream_provider_error() {
    let (_fixture, outcome) = run_with_limits_request(
        vec![oauth(1, "oauth").replies(vec![sse(SSE_MID_STREAM_ERROR)])],
        stream_request(),
        ClientRead::Full,
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_refunded(&outcome, "mid-stream provider error");
}

#[tokio::test]
async fn limit_reservation_refunded_after_upstream_disconnect_mid_stream() {
    let (_fixture, outcome) = run_with_limits_request(
        vec![
            oauth(1, "oauth").replies(vec![Reply::SseThenDisconnect(Bytes::from_static(
                SSE_MESSAGE_START.as_bytes(),
            ))]),
        ],
        stream_request(),
        ClientRead::Full,
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_refunded(&outcome, "upstream disconnect mid-stream");
}

#[tokio::test]
async fn limit_reservation_refunded_after_transport_error() {
    let (_fixture, outcome) =
        run_with_limits(vec![oauth(1, "oauth").replies(vec![Reply::Transport])]).await;

    assert_eq!(outcome.status, StatusCode::BAD_GATEWAY);
    assert_refunded(&outcome, "transport error");
}

#[tokio::test]
async fn limit_reservation_reconciled_after_healthy_stream() {
    let (_fixture, outcome) = run_with_limits_request(
        vec![oauth(1, "oauth").replies(vec![sse(SSE_OK)])],
        stream_request(),
        ClientRead::Full,
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_eq!(outcome.reserved_decisions, 1);
    assert_eq!(
        outcome.requests_remaining,
        REQUEST_CAP - 1,
        "success counts"
    );
    assert_eq!(
        outcome.tokens_remaining,
        TOKEN_CAP - 2,
        "success settles to actual usage"
    );
    assert_eq!(outcome.concurrent_in_flight, 0);
}

#[tokio::test]
async fn limit_reservation_refunded_when_client_cancels_stream() {
    let (_fixture, outcome) = run_with_limits_request(
        vec![oauth(1, "oauth").replies(vec![sse(SSE_OK)])],
        stream_request(),
        ClientRead::CancelBeforeBody,
    )
    .await;

    assert_eq!(outcome.status, StatusCode::OK);
    assert_refunded(&outcome, "client cancel");
}

#[tokio::test]
async fn healthy_oauth_never_touches_api_key() {
    let fixture = Fixture::new(vec![oauth(1, "oauth"), api_key(2, "key")]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth")]);
}

#[tokio::test]
async fn reconnect_required_oauth_routes_to_api_key_end_to_end() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").last_apply_error("status_401"),
        api_key(2, "key"),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("key")]);
}

const KEY_1: &str = "sk-ant-api03-FAILOVERkey1-0123456789abcdef-AAAA";
const KEY_2: &str = "sk-ant-api03-FAILOVERkey2-0123456789abcdef-BBBB";
const WORKSPACE: &str = "wrkspc_cfg";
const STREAM_REQUEST_BODY: &[u8] =
    br#"{"model":"claude-test","max_tokens":16,"stream":true,"messages":[]}"#;
const SSE_MESSAGE_START: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
const SSE_OK: &[u8] = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
const SSE_MID_STREAM_ERROR: &[u8] = b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-test\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n";

/// An API-key upstream signed by the production key signer.
fn keyed(id: u128, name: &str, key: &'static str, workspace: Option<&'static str>) -> TestUpstream {
    api_key(id, name).credential(Credential::ApiKey { key, workspace })
}

fn sse(body: &'static [u8]) -> Reply {
    Reply::Body {
        status: StatusCode::OK,
        content_type: "text/event-stream",
        body: Bytes::from_static(body),
    }
}

fn values(headers: &HeaderMap, name: &str) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .map(|value| value.to_str().expect("ascii header").to_owned())
        .collect()
}

fn stream_request() -> Request<Bytes> {
    messages_request(Bytes::from_static(STREAM_REQUEST_BODY))
}

/// One request through a storage-backed lifecycle, with the request-event
/// rows read back after the final event lands.
struct Recorded {
    fixture: Fixture,
    status: StatusCode,
    body: Bytes,
    events: Vec<cc_lb_storage_api::RequestEvent>,
}

async fn send_recorded(
    upstreams: Vec<TestUpstream>,
    request: Request<Bytes>,
) -> Result<Recorded, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let storage = sqlite_storage(&dir).await?;
    let test_bus = TestLifecycleBus::new().with_assembler(storage.clone() as Arc<dyn StorageTrait>);
    let mut update_rx = test_bus.bus.subscribe();
    let fixture = Fixture::new(upstreams)
        .with_lifecycle(|lifecycle| lifecycle.with_event_bus(test_bus.bus_arc()));
    let (status, body) = fixture.send_request(request).await;
    wait_for_final_request_event(&mut update_rx).await;
    let events = stored_request_events(storage.as_ref()).await?;
    Ok(Recorded {
        fixture,
        status,
        body,
        events,
    })
}

/// FO-02: OAuth 429 fails over to a workspace API key in the same request,
/// and each attempt carries only its own credential headers.
#[tokio::test]
async fn fo_02_oauth_429_fails_over_to_workspace_key_with_isolated_headers() {
    let fixture = Fixture::new(vec![
        oauth(1, "oauth")
            .credential(Credential::Bearer)
            .replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        keyed(2, "key", KEY_1, Some(WORKSPACE)),
    ]);
    let mut request = messages_request(Bytes::from_static(REQUEST_BODY));
    request.headers_mut().insert(
        ANTHROPIC_WORKSPACE_ID_HEADER,
        HeaderValue::from_static("wrkspc_EVIL"),
    );

    let (status, _body) = fixture.send_request(request).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("oauth"), host("key")]);
    let attempts = fixture.dispatched_headers();
    let [oauth_attempt, key_attempt] = attempts.as_slice() else {
        panic!("two attempts expected, got {}", attempts.len());
    };
    assert_eq!(
        values(oauth_attempt, AUTHORIZATION.as_str()),
        vec!["Bearer oauth-token"]
    );
    assert!(values(oauth_attempt, "x-api-key").is_empty());
    assert!(values(oauth_attempt, ANTHROPIC_WORKSPACE_ID_HEADER).is_empty());
    assert_eq!(values(key_attempt, "x-api-key"), vec![KEY_1]);
    assert_eq!(
        values(key_attempt, ANTHROPIC_WORKSPACE_ID_HEADER),
        vec![WORKSPACE]
    );
    assert!(values(key_attempt, AUTHORIZATION.as_str()).is_empty());
}

/// FO-03: an API-key 401 fails over to the next API key; the key signer never
/// asks for a same-upstream refresh retry.
#[tokio::test]
async fn fo_03_api_key_401_fails_over_to_next_key_without_refresh() {
    let fixture = Fixture::new(vec![
        keyed(1, "key-a", KEY_1, None).replies(vec![Reply::Status(StatusCode::UNAUTHORIZED)]),
        keyed(2, "key-b", KEY_2, None),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("key-a"), host("key-b")]);
    let keys: Vec<Vec<String>> = fixture
        .dispatched_headers()
        .iter()
        .map(|headers| values(headers, "x-api-key"))
        .collect();
    assert_eq!(keys, vec![vec![KEY_1.to_owned()], vec![KEY_2.to_owned()]]);
}

/// FO-05: a streaming request that fails over from OAuth to a key relays the
/// key's SSE and records one clean request event for the key.
#[tokio::test]
async fn fo_05_streaming_failover_records_one_clean_event() -> Result<(), Box<dyn std::error::Error>>
{
    let recorded = send_recorded(
        vec![
            oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
            keyed(2, "key", KEY_1, None).replies(vec![sse(SSE_OK)]),
        ],
        stream_request(),
    )
    .await?;

    assert_eq!(recorded.status, StatusCode::OK);
    assert_eq!(recorded.body.as_ref(), SSE_OK);
    assert_eq!(recorded.fixture.hosts(), vec![host("oauth"), host("key")]);
    let [event] = recorded.events.as_slice() else {
        panic!("one request event expected: {:?}", recorded.events);
    };
    assert_eq!(event.upstream_id, Some(Uuid::from_u128(2)));
    assert_eq!(event.status, StatusCode::OK.as_u16());
    assert_eq!(event.error_code, None);
    assert_eq!(event.upstream_error_type, None);
    assert_eq!(event.upstream_error_message, None);
    Ok(())
}

/// FO-06: once a key's 200 stream is committed, a mid-stream error never
/// fails over; the error is recorded once on the single event.
#[tokio::test]
async fn fo_06_mid_stream_error_after_commit_never_fails_over()
-> Result<(), Box<dyn std::error::Error>> {
    let recorded = send_recorded(
        vec![
            keyed(1, "key-a", KEY_1, None).replies(vec![sse(SSE_MID_STREAM_ERROR)]),
            keyed(2, "key-b", KEY_2, None),
        ],
        stream_request(),
    )
    .await?;

    assert_eq!(recorded.status, StatusCode::OK);
    assert!(
        recorded.body.starts_with(SSE_MESSAGE_START.as_bytes()),
        "the committed stream is relayed"
    );
    assert_eq!(recorded.fixture.hosts(), vec![host("key-a")]);
    let [event] = recorded.events.as_slice() else {
        panic!("one request event expected: {:?}", recorded.events);
    };
    assert_eq!(event.upstream_id, Some(Uuid::from_u128(1)));
    assert_eq!(event.error_code.as_deref(), Some("upstream_stream_error"));
    assert_eq!(
        event.upstream_error_type.as_deref(),
        Some("overloaded_error")
    );
    assert_eq!(event.upstream_error_message.as_deref(), Some("Overloaded"));
    Ok(())
}

/// FO-09: an OAuth-to-key failover increments the failover counter once with
/// bounded reason and kind labels.
#[tokio::test]
async fn fo_09_failover_metric_names_reason_and_kinds() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _recorder_guard = metrics::set_default_local_recorder(&recorder);
    let fixture = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
        keyed(2, "key", KEY_1, None),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    let failovers: Vec<(HashMap<String, String>, u64)> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == "cc_lb_upstream_failover_total")
        .map(|(key, _, _, value)| {
            let labels = key
                .key()
                .labels()
                .map(|label| (label.key().to_owned(), label.value().to_owned()))
                .collect();
            let metrics_util::debugging::DebugValue::Counter(count) = value else {
                panic!("failover metric is a counter");
            };
            (labels, count)
        })
        .collect();
    assert_eq!(
        failovers,
        vec![(
            HashMap::from([
                ("reason".to_owned(), "upstream_429".to_owned()),
                ("from_kind".to_owned(), "anthropic_oauth".to_owned()),
                ("to_kind".to_owned(), "anthropic_key".to_owned()),
            ]),
            1,
        )]
    );
}

/// FO-10: every failover path, streaming or not, leaves exactly one request
/// event, owned by the key, with none of the failed attempt's errors.
#[tokio::test]
async fn fo_10_every_failover_path_records_one_event() -> Result<(), Box<dyn std::error::Error>> {
    for path in ["429", "401 refresh 401", "sign expired", "build failed"] {
        let failing = || match path {
            "429" => oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::TOO_MANY_REQUESTS)]),
            "401 refresh 401" => oauth(1, "oauth")
                .credential(Credential::RefreshOnUnauthorized)
                .replies(vec![
                    Reply::Status(StatusCode::UNAUTHORIZED),
                    Reply::Status(StatusCode::UNAUTHORIZED),
                ]),
            "sign expired" => oauth(1, "oauth").credential(Credential::SignExpired),
            _ => oauth(1, "oauth").credential(Credential::BuildFails),
        };
        for stream in [false, true] {
            let (key, request) = if stream {
                (
                    keyed(2, "key", KEY_1, None).replies(vec![sse(SSE_OK)]),
                    stream_request(),
                )
            } else {
                (
                    keyed(2, "key", KEY_1, None),
                    messages_request(Bytes::from_static(REQUEST_BODY)),
                )
            };

            let recorded = send_recorded(vec![failing(), key], request).await?;

            assert_eq!(recorded.status, StatusCode::OK, "{path} stream={stream}");
            let [event] = recorded.events.as_slice() else {
                panic!(
                    "{path} stream={stream}: one request event expected, got {:?}",
                    recorded.events
                );
            };
            assert_eq!(
                event.upstream_id,
                Some(Uuid::from_u128(2)),
                "{path} stream={stream}"
            );
            assert_eq!(event.status, 200, "{path} stream={stream}");
            assert_eq!(event.error_code, None, "{path} stream={stream}");
            assert_eq!(event.upstream_error_type, None, "{path} stream={stream}");
            assert_eq!(event.upstream_error_message, None, "{path} stream={stream}");
        }
    }
    Ok(())
}

/// FO-11: an upstream 403 is relayed without failover, and the key stays a
/// routing candidate for the next request.
#[tokio::test]
async fn fo_11_forbidden_key_relays_403_and_stays_routable() {
    let fixture = Fixture::new(vec![
        keyed(1, "key-a", KEY_1, None).replies(vec![Reply::Status(StatusCode::FORBIDDEN)]),
        keyed(2, "key-b", KEY_2, None),
    ]);

    let (first, _body) = fixture.send().await;
    let (second, _body) = fixture.send().await;

    assert_eq!(first, StatusCode::FORBIDDEN);
    assert_eq!(second, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("key-a"), host("key-a")]);
}

/// FO-14: a long-lived OAuth 401 fails over to the API key in the same
/// request; once the 401 is persisted as reconnect-required, traffic goes
/// straight to the key.
#[tokio::test]
async fn fo_14_long_lived_oauth_401_moves_traffic_to_api_key() {
    let failing = Fixture::new(vec![
        oauth(1, "oauth").replies(vec![Reply::Status(StatusCode::UNAUTHORIZED)]),
        keyed(2, "key", KEY_1, None),
    ]);
    let (status, _body) = failing.send().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(failing.hosts(), vec![host("oauth"), host("key")]);

    let reconciled = Fixture::new(vec![
        oauth(1, "oauth").last_apply_error("status_401"),
        keyed(2, "key", KEY_1, None),
    ]);
    let (status, _body) = reconciled.send().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reconciled.hosts(), vec![host("key")]);
}

/// FO-15: `status_401` on an API-key upstream is not the OAuth
/// reconnect-required predicate; the key keeps serving.
#[tokio::test]
async fn fo_15_api_key_status_401_is_not_reconnect_required() {
    let fixture = Fixture::new(vec![
        keyed(1, "key", KEY_1, None).last_apply_error("status_401"),
    ]);

    let (status, _body) = fixture.send().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fixture.hosts(), vec![host("key")]);
}

/// Request-log store cap on upstream error text.
const STORE_CAP: usize = 8 * 1024;
const TRUNCATION_MARKER: &str = "\n...[truncated]";

/// No 12-byte window of the key's secret body (after the public family
/// prefix a hint may show) appears in `sink`.
fn assert_clean(sink: &str, label: &str) {
    let secret_body = KEY_1.strip_prefix("sk-ant-api03-").expect("api03 key");
    for window in secret_body.as_bytes().windows(12) {
        let window = std::str::from_utf8(window).expect("ascii key");
        assert!(
            !sink.contains(window),
            "{label} keeps key fragment {window:?}"
        );
    }
}

fn stream_error_reply(status: StatusCode, body: Vec<u8>) -> Reply {
    Reply::Body {
        status,
        content_type: "text/event-stream",
        body: Bytes::from(body),
    }
}

/// SEC-05 (streaming relay): a streamed 400 echoing the key is relayed
/// byte-identical while the stored message is masked.
#[tokio::test]
async fn sec_05_streamed_error_echo_is_masked_in_stored_sinks_only()
-> Result<(), Box<dyn std::error::Error>> {
    let body = format!(
        r#"{{"type":"error","error":{{"type":"invalid_request_error","message":"bad key {KEY_1}"}}}}"#
    );
    let recorded = send_recorded(
        vec![
            keyed(1, "key", KEY_1, None).replies(vec![stream_error_reply(
                StatusCode::BAD_REQUEST,
                body.clone().into_bytes(),
            )]),
        ],
        stream_request(),
    )
    .await?;

    assert_eq!(recorded.status, StatusCode::BAD_REQUEST);
    assert_eq!(recorded.body.as_ref(), body.as_bytes());
    let [event] = recorded.events.as_slice() else {
        panic!("one request event expected: {:?}", recorded.events);
    };
    assert_eq!(
        event.upstream_error_message.as_deref(),
        Some("bad key [REDACTED]")
    );
    assert_clean(&serde_json::to_string(&recorded.events)?, "request event");
    Ok(())
}

/// SEC-05 (mid-stream): a provider `event: error` inside a 200 stream that
/// echoes the key is relayed unchanged and stored masked.
#[tokio::test]
async fn sec_05_mid_stream_error_echo_is_masked_in_stored_sinks_only()
-> Result<(), Box<dyn std::error::Error>> {
    let body = format!(
        "{SSE_MESSAGE_START}event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"overloaded_error\",\"message\":\"retry with {KEY_1}\"}}}}\n\n"
    );
    let recorded = send_recorded(
        vec![
            keyed(1, "key", KEY_1, None).replies(vec![stream_error_reply(
                StatusCode::OK,
                body.clone().into_bytes(),
            )]),
        ],
        stream_request(),
    )
    .await?;

    assert_eq!(recorded.status, StatusCode::OK);
    assert_eq!(recorded.body.as_ref(), body.as_bytes());
    let [event] = recorded.events.as_slice() else {
        panic!("one request event expected: {:?}", recorded.events);
    };
    assert_eq!(
        event.upstream_error_message.as_deref(),
        Some("retry with [REDACTED]")
    );
    assert_clean(&serde_json::to_string(&recorded.events)?, "request event");
    Ok(())
}

/// SEC-13 (streaming relay): a raw streamed error body with the key at the
/// start, middle, or straddling the store cap leaves no fragment in the
/// stored text, which stays within the cap.
#[tokio::test]
async fn sec_13_streamed_key_straddling_store_cap_is_masked()
-> Result<(), Box<dyn std::error::Error>> {
    for offset in [
        0,
        STORE_CAP / 2,
        STORE_CAP - TRUNCATION_MARKER.len() - 30,
        STORE_CAP - 1,
    ] {
        let mut body = vec![b'x'; offset];
        body.extend_from_slice(KEY_1.as_bytes());
        body.resize(STORE_CAP * 2, b'y');
        let recorded = send_recorded(
            vec![
                keyed(1, "key", KEY_1, None)
                    .replies(vec![stream_error_reply(StatusCode::BAD_REQUEST, body)]),
            ],
            stream_request(),
        )
        .await?;

        assert_eq!(recorded.status, StatusCode::BAD_REQUEST, "offset {offset}");
        let [event] = recorded.events.as_slice() else {
            panic!("offset {offset}: one request event expected");
        };
        let stored = event
            .upstream_error_message
            .as_deref()
            .expect("raw error body stored");
        assert!(stored.len() <= STORE_CAP, "offset {offset}");
        assert!(stored.ends_with(TRUNCATION_MARKER), "offset {offset}");
        assert_clean(stored, &format!("offset {offset}"));
    }
    Ok(())
}

async fn websocket_gateway_fixture(
    fixture: Fixture,
    bytes: usize,
    slots: usize,
) -> (
    cc_lb_engine::responses_websocket::ResponsesWebSocketGateway,
    Arc<ScriptedDispatch>,
) {
    let lifecycle = Arc::new(fixture.lifecycle);
    let headers = messages_request(Bytes::new()).headers().clone();
    let auth = lifecycle
        .authenticate(&headers)
        .await
        .expect("fixture authenticates");
    let gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
        lifecycle, &auth, headers, bytes, slots,
    );
    (gateway, fixture.dispatch)
}

fn websocket_create(lane: &str, previous: Option<&str>) -> Bytes {
    let mut value = serde_json::json!({
        "type": "response.create",
        "stream_id": lane,
        "model": "gpt-5-codex",
        "max_output_tokens": 4,
        "store": false,
        "input": "gateway regression",
    });
    if let Some(previous) = previous {
        value["previous_response_id"] = serde_json::json!(previous);
    }
    Bytes::from(serde_json::to_vec(&value).expect("fixture serializes"))
}

fn websocket_created(lane: &str, id: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "type": "response.created",
            "stream_id": lane,
            "response": {"id": id, "model": "gpt-5-codex", "store": false, "usage": null},
        }))
        .expect("fixture serializes"),
    )
}

fn websocket_completed(lane: &str, id: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "type": "response.completed",
            "stream_id": lane,
            "response": {"id": id, "model": "gpt-5-codex", "store": false, "usage": null},
        }))
        .expect("fixture serializes"),
    )
}

async fn websocket_admit(
    gateway: &mut cc_lb_engine::responses_websocket::ResponsesWebSocketGateway,
    frame: Bytes,
) -> Option<cc_lb_engine::responses_websocket::WebSocketDispatch> {
    let auth = gateway
        .authenticate()
        .await
        .expect("fresh client check succeeds");
    gateway
        .client_message(auth, frame)
        .await
        .expect("create is admitted")
}

async fn websocket_send(
    gateway: &mut cc_lb_engine::responses_websocket::ResponsesWebSocketGateway,
    dispatch: &cc_lb_engine::responses_websocket::WebSocketDispatch,
    connection_id: u64,
) {
    let candidate = dispatch.candidates.first().expect("route has a candidate");
    gateway.assign_connection(
        dispatch.turn_id,
        connection_id,
        candidate.upstream_id,
        &candidate.upstream_name,
    );
    gateway
        .validate_send(dispatch.turn_id)
        .await
        .expect("current route remains eligible");
    assert!(gateway.sent(dispatch.turn_id).is_empty());
}

#[tokio::test]
async fn responses_websocket_terminal_status_matches_success_failure_and_cancellation() {
    use cc_lb_lifecycle::TerminationReason;

    for (kind, expected_status, expected_reason) in [
        (
            "response.failed",
            502,
            TerminationReason::ErrorCode("upstream_stream_error".to_owned()),
        ),
        (
            "client.close",
            499,
            TerminationReason::ErrorCode("client_closed_request".to_owned()),
        ),
        ("response.completed", 200, TerminationReason::Success),
    ] {
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let fixture = Fixture::new(vec![TestUpstream::new(
            50_001,
            "openai",
            UpstreamKind::OpenaiApiKey,
        )])
        .with_lifecycle(|lifecycle| lifecycle.with_event_bus(bus));
        let (mut gateway, dispatch) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
        let turn = websocket_admit(&mut gateway, websocket_create("main", None))
            .await
            .unwrap();
        websocket_send(&mut gateway, &turn, 1).await;
        assert!(
            gateway
                .provider_message(1, &websocket_created("main", "resp_terminal_status"))
                .await
                .unwrap()
                .forward
        );
        if kind != "client.close" {
            let terminal = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "type": kind,
                    "stream_id": "main",
                    "response": {
                        "id": "resp_terminal_status",
                        "store": false,
                        "usage": {
                            "input_tokens": 10,
                            "output_tokens": 6,
                            "total_tokens": 16,
                            "input_tokens_details": {"cached_tokens": 4},
                            "output_tokens_details": {"reasoning_tokens": 2},
                        },
                    },
                }))
                .unwrap(),
            );
            assert!(
                gateway
                    .provider_message(1, &terminal)
                    .await
                    .unwrap()
                    .forward
            );
        }
        gateway.close();
        drop(gateway);

        let mut starts = Vec::new();
        let mut authenticated = Vec::new();
        let mut terminated = Vec::new();
        let mut usages = Vec::new();
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::RequestStarted { event_id, .. } => starts.push(event_id),
                LifecycleEvent::AuthCompleted {
                    event_id, result, ..
                } => {
                    assert!(result.is_ok(), "{kind} uses the authenticated client rail");
                    authenticated.push(event_id);
                }
                LifecycleEvent::RequestTerminated {
                    event_id,
                    reason,
                    client_status,
                    ..
                } => terminated.push((event_id, reason, client_status)),
                LifecycleEvent::UsageObserved {
                    event_id, usage, ..
                } => usages.push((
                    event_id,
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_read_input_tokens,
                    usage.thinking_tokens,
                )),
                _ => {}
            }
        }
        assert_eq!(starts.len(), 1, "{kind} starts exactly one logical turn");
        assert_eq!(authenticated, starts, "{kind} authenticates that same turn");
        assert_eq!(terminated.len(), 1, "{kind} terminates exactly once");
        let (event_id, reason, status) = terminated.pop().unwrap();
        assert_eq!(
            starts,
            vec![event_id.clone()],
            "{kind} preserves turn identity"
        );
        if kind == "client.close" {
            assert!(usages.is_empty(), "cancellation must not fabricate usage");
        } else {
            assert_eq!(usages, vec![(event_id, 6, 6, 4, 2)]);
        }
        assert_eq!(reason, expected_reason, "{kind} keeps its terminal reason");
        assert_eq!(
            status, expected_status,
            "{kind} records the logical turn status"
        );
        assert!(dispatch.hosts.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn responses_websocket_three_create_fifo_and_parallel_lane_preserve_payloads() {
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_001,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )]);
    let (mut gateway, dispatch) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    let second_frame = websocket_create("main", None);
    let third_frame = websocket_create("main", None);
    assert!(
        websocket_admit(&mut gateway, second_frame.clone())
            .await
            .is_none()
    );
    assert!(
        websocket_admit(&mut gateway, third_frame.clone())
            .await
            .is_none()
    );
    let parallel = websocket_admit(&mut gateway, websocket_create("parallel", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &parallel, 1).await;

    assert!(
        gateway
            .provider_message(1, &websocket_created("main", "resp_fifo_1"))
            .await
            .unwrap()
            .forward
    );
    assert!(
        gateway
            .provider_message(1, &websocket_created("parallel", "resp_parallel"))
            .await
            .unwrap()
            .forward
    );
    let delta = Bytes::from_static(
        br#"{"type":"response.output_text.delta","stream_id":"parallel","delta":"opaque"}"#,
    );
    assert!(gateway.provider_message(1, &delta).await.unwrap().forward);
    let completed = gateway
        .provider_message(1, &websocket_completed("parallel", "resp_parallel"))
        .await
        .unwrap();
    assert!(completed.dispatches.is_empty());

    let mut completed = gateway
        .provider_message(1, &websocket_completed("main", "resp_fifo_1"))
        .await
        .unwrap();
    assert_eq!(completed.dispatches.len(), 1);
    let second = completed.dispatches.pop().unwrap();
    assert_eq!(second.frame, second_frame);
    websocket_send(&mut gateway, &second, 1).await;
    assert!(
        gateway
            .provider_message(1, &websocket_created("main", "resp_fifo_2"))
            .await
            .unwrap()
            .forward
    );
    let mut completed = gateway
        .provider_message(1, &websocket_completed("main", "resp_fifo_2"))
        .await
        .unwrap();
    assert_eq!(completed.dispatches.len(), 1);
    let third = completed.dispatches.pop().unwrap();
    assert_eq!(third.frame, third_frame);
    websocket_send(&mut gateway, &third, 1).await;
    assert!(
        gateway
            .provider_message(1, &websocket_created("main", "resp_fifo_3"))
            .await
            .unwrap()
            .forward
    );
    assert!(
        gateway
            .provider_message(1, &websocket_completed("main", "resp_fifo_3"))
            .await
            .unwrap()
            .dispatches
            .is_empty()
    );
    assert!(
        dispatch.hosts.lock().expect("dispatch log lock").is_empty(),
        "WS never dispatches through the HTTP body lifecycle"
    );
}

#[tokio::test]
async fn responses_websocket_mismatched_lane_and_connection_never_expose_ownership() {
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_002,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )]);
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    let error = gateway
        .provider_message(1, &websocket_created("wrong-lane", "resp_owned"))
        .await
        .err()
        .expect("mismatched lane rejects before expose");
    assert_eq!(error.code, "invalid_upstream_response");
    assert!(
        gateway
            .provider_message(1, &websocket_created("main", "resp_owned"))
            .await
            .unwrap()
            .forward
    );
    assert!(
        gateway
            .provider_message(1, &websocket_completed("main", "resp_owned"))
            .await
            .unwrap()
            .forward
    );

    let fork = websocket_admit(&mut gateway, websocket_create("fork", Some("resp_owned")))
        .await
        .unwrap();
    assert_eq!(fork.connection_id, Some(1));
    let candidate = fork.candidates.first().unwrap();
    gateway.assign_connection(
        fork.turn_id,
        2,
        candidate.upstream_id,
        &candidate.upstream_name,
    );
    let error = gateway.validate_send(fork.turn_id).await.unwrap_err();
    assert_eq!(error.code, "upstream_affinity_unavailable");
    gateway.fail_dispatch(fork.turn_id, &error).await;
    let duplicate = gateway
        .provider_message(1, &websocket_completed("main", "resp_owned"))
        .await
        .unwrap();
    assert!(
        !duplicate.forward,
        "a known terminal replay is neither exposed nor settled again"
    );
    assert!(duplicate.dispatches.is_empty());
    assert!(duplicate.errors.is_empty());
}

#[tokio::test]
async fn responses_websocket_zero_slots_and_oversize_fail_before_dispatch() {
    for (byte_cap, slots, expected) in [
        (1, 64, "request_too_large"),
        (1_048_576, 0, "overloaded_error"),
    ] {
        let fixture = Fixture::new(vec![TestUpstream::new(
            50_003,
            "openai",
            UpstreamKind::OpenaiApiKey,
        )]);
        let (mut gateway, dispatch) = websocket_gateway_fixture(fixture, byte_cap, slots).await;
        let auth = gateway.authenticate().await.unwrap();
        let error = gateway
            .client_message(auth, websocket_create("main", None))
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert!(dispatch.hosts.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn responses_websocket_close_releases_inflight_queued_and_steer_reservations_once() {
    let concurrency = Arc::new(KeyConcurrencyManager::new());
    let limits = vec![
        Limit {
            kind: LimitKind::Concurrent,
            window_secs: 0,
            cap_micros: 8,
        },
        Limit {
            kind: LimitKind::TotalTokens,
            window_secs: 60,
            cap_micros: 100_000,
        },
    ];
    let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
    let fixture = Fixture::with_limits(
        vec![TestUpstream::new(
            50_004,
            "openai",
            UpstreamKind::OpenaiApiKey,
        )],
        limits,
    )
    .with_lifecycle(|lifecycle| {
        lifecycle.with_static_limit_subject(
            engine,
            PRINCIPAL.to_owned(),
            "ws-limit-key".to_owned(),
            StoredApiKeyRecord {
                key_hash_b64: "ws-limit-key".to_owned(),
                status: KeyStatus::Active,
                ..Default::default()
            },
        )
    });
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    assert!(
        gateway
            .provider_message(1, &websocket_created("main", "resp_parent"))
            .await
            .unwrap()
            .forward
    );
    assert!(
        websocket_admit(&mut gateway, websocket_create("main", None))
            .await
            .is_none()
    );
    let steer = Bytes::from_static(br#"{"type":"response.steer","previous_response_id":"resp_parent","input":"keep the scope small"}"#);
    let successor = websocket_admit(&mut gateway, steer).await.unwrap();
    websocket_send(&mut gateway, &successor, 1).await;
    assert_eq!(concurrency.current("ws-limit-key"), 3);
    gateway.close();
    assert_eq!(concurrency.current("ws-limit-key"), 0);
    gateway.close();
    assert_eq!(concurrency.current("ws-limit-key"), 0);
}

#[tokio::test]
async fn responses_websocket_idle_key_expiry_or_revocation_rejects_queued_and_new_dispatches() {
    use cc_lb_control::api_keys::builtin_authn::{BuiltinAuthError, BuiltinAuthn};
    use cc_lb_control::api_keys::key_store::KeyStore;
    use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind, PrincipalStore};
    use cc_lb_storage_api::{IssueParams, ManagedKeyStore};

    for revoke in [false, true] {
        let clock = Arc::new(TestClock::new_at_secs(NOW));
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite("sqlite::memory:", clock.clone())
                .await
                .unwrap(),
        );
        storage.initialize().await.unwrap();
        let limits = vec![Limit {
            kind: LimitKind::Concurrent,
            window_secs: 0,
            cap_micros: 4,
        }];
        let principal = PrincipalStore::create(
            storage.as_ref(),
            PrincipalCreate {
                name: "websocket-key-recheck".to_owned(),
                kind: PrincipalKind::Human,
                allowed_models: vec!["*".to_owned()],
                allowed_upstreams: Vec::new(),
                default_limits: limits.clone(),
                cache_keepalive: None,
            },
            NOW,
        )
        .await
        .unwrap();
        let principal_id = principal.id.to_string();
        let generated = cc_lb_control::api_keys::secret::generate_new();
        let issued = ManagedKeyStore::issue(
            storage.as_ref(),
            &principal_id,
            &generated.key_id,
            IssueParams {
                label: "websocket-key-recheck".to_owned(),
                description: None,
                expires_at_unix_secs: Some(NOW + 1),
                limit_overrides: Vec::new(),
                secret_salt: generated.secret_salt,
                verify_hash: generated.verify_hash,
                last_4: generated.last_4,
                index_hash: generated.index_hash,
            },
        )
        .await
        .unwrap();
        let key_storage: Arc<dyn ManagedKeyStore> = storage;
        let keys = Arc::new(KeyStore::new(key_storage));
        let authn = Arc::new(BuiltinAuthn::new(keys.clone(), clock.clone()));
        let view = DynamicViewBuilder::new(0)
            .principal_view(Arc::new(PrincipalView::for_tests(
                &principal_id,
                true,
                vec!["*".to_owned()],
                limits,
                HashMap::new(),
            )))
            .signer_factory(Arc::new(ScriptedSignerFactory {
                credentials: Arc::new(HashMap::from([("openai".to_owned(), Credential::Healthy)])),
            }))
            .upstream_records(vec![
                TestUpstream::new(50_005, "openai", UpstreamKind::OpenaiApiKey).record,
            ])
            .build();
        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(concurrency.clone(), clock.clone());
        let bus = Arc::new(cc_lb_control::InMemoryBus::new());
        let mut events = bus.subscribe_lifecycle();
        let reconcile = spawn_lifecycle_limit_reconcile_subscriber(
            bus.attach_lifecycle_limit_reconcile(128),
            engine.clone(),
        );
        let dispatch = Arc::new(ScriptedDispatch {
            replies: Mutex::new(HashMap::new()),
            hosts: Mutex::new(Vec::new()),
            headers: Mutex::new(Vec::new()),
        });
        let lifecycle = Arc::new(
            Lifecycle::new_with_dynamic_view(
                authn.clone(),
                Arc::new(DynamicViewHolder::new(view)),
                dispatch.clone(),
                LifecycleConfig::default(),
                clock.clone(),
            )
            .with_limit_engine(engine, authn)
            .with_event_bus(bus),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", generated.plaintext.expose())).unwrap(),
        );
        let handshake = lifecycle.authenticate(&headers).await.unwrap();
        let mut gateway = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
            lifecycle, &handshake, headers, 1_048_576, 64,
        );
        assert!(
            matches!(
                events.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "successful upgrade authentication creates no orphan request lifecycle"
        );
        let first = websocket_admit(&mut gateway, websocket_create("main", None))
            .await
            .unwrap();
        websocket_send(&mut gateway, &first, 1).await;
        gateway
            .provider_message(1, &websocket_created("main", "resp_before_revoke"))
            .await
            .unwrap();
        assert!(
            websocket_admit(&mut gateway, websocket_create("main", None))
                .await
                .is_none()
        );
        if revoke {
            keys.revoke(&principal_id, &generated.key_id).await.unwrap();
        } else {
            clock.advance_secs(2);
        }
        let delta = Bytes::from_static(br#"{"type":"response.output_text.delta","stream_id":"main","delta":"already admitted"}"#);
        assert!(
            gateway.provider_message(1, &delta).await.unwrap().forward,
            "key state does not reauthenticate or interrupt provider deltas"
        );
        let mut result = gateway
            .provider_message(1, &websocket_completed("main", "resp_before_revoke"))
            .await
            .unwrap();
        let queued = result.dispatches.pop().unwrap();
        let candidate = queued.candidates.first().unwrap();
        gateway.assign_connection(
            queued.turn_id,
            1,
            candidate.upstream_id,
            &candidate.upstream_name,
        );
        let queued_error = gateway.validate_send(queued.turn_id).await.unwrap_err();
        assert_eq!(queued_error.code, "authentication_error");
        assert!(
            gateway
                .fail_dispatch(queued.turn_id, &queued_error)
                .await
                .is_empty()
        );
        let auth_error = match gateway.authenticate().await {
            Ok(_) => panic!("changed key must reject the next create before parse"),
            Err(error) => error,
        };
        assert!(matches!(
            (&auth_error, revoke),
            (BuiltinAuthError::NotFound, true) | (BuiltinAuthError::Expired, false)
        ));
        assert_eq!(
            gateway.reject_authentication(&auth_error).code,
            "authentication_error"
        );
        gateway.close();
        reconcile.shutdown().await;
        assert_eq!(concurrency.current(&issued.key_hash_b64), 0);
        let mut started = Vec::new();
        let mut terminated = Vec::new();
        while let Ok(event) = events.try_recv() {
            match event {
                LifecycleEvent::RequestStarted { event_id, .. } => started.push(event_id),
                LifecycleEvent::RequestTerminated { event_id, .. } => terminated.push(event_id),
                _ => {}
            }
        }
        assert_eq!(
            started.len(),
            3,
            "two admitted turns and one rejected new create"
        );
        assert_eq!(terminated.len(), 3);
        started.sort();
        terminated.sort();
        assert_eq!(started, terminated);
        started.dedup();
        assert_eq!(
            started.len(),
            3,
            "reauthentication does not allocate duplicate turn lifecycles"
        );
        assert!(dispatch.hosts.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn responses_websocket_midturn_steering_reserves_one_successor_and_keeps_usage_separate() {
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_006,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(bus));
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    gateway
        .provider_message(1, &websocket_created("main", "resp_steer_parent"))
        .await
        .unwrap();
    let queued_frame = websocket_create("main", None);
    assert!(
        websocket_admit(&mut gateway, queued_frame.clone())
            .await
            .is_none()
    );
    let first_steer = Bytes::from_static(br#"{"type":"response.steer","previous_response_id":"resp_steer_parent","input":"first update"}"#);
    let second_steer = Bytes::from_static(br#"{"type":"response.steer","previous_response_id":"resp_steer_parent","input":"second update"}"#);
    let successor = websocket_admit(&mut gateway, first_steer)
        .await
        .expect("mid-turn steering sends before the parent ends");
    assert!(
        websocket_admit(&mut gateway, second_steer.clone())
            .await
            .is_none(),
        "a later control cannot overtake its not-yet-sent first control"
    );
    let candidate = successor.candidates.first().unwrap();
    gateway.assign_connection(
        successor.turn_id,
        1,
        candidate.upstream_id,
        &candidate.upstream_name,
    );
    gateway.validate_send(successor.turn_id).await.unwrap();
    let mut controls = gateway.sent(successor.turn_id);
    assert_eq!(controls.len(), 1);
    let control = controls.pop().unwrap();
    assert_eq!(
        control.turn_id, successor.turn_id,
        "the second steer does not allocate another response reservation"
    );
    assert_eq!(control.frame, second_steer);
    assert!(gateway.sent(control.turn_id).is_empty());
    for id in ["steer_first", "steer_second"] {
        let accepted = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.steer.accepted", "stream_id": "main",
                "steer": {"id": id, "previous_response_id": "resp_steer_parent"},
            }))
            .unwrap(),
        );
        assert!(
            gateway
                .provider_message(1, &accepted)
                .await
                .unwrap()
                .forward
        );
    }
    let parent_terminal = Bytes::from_static(br#"{"type":"response.incomplete","stream_id":"main","response":{"id":"resp_steer_parent","incomplete_details":{"reason":"steered"},"usage":{"input_tokens":7,"output_tokens":11}}}"#);
    let terminal = gateway.provider_message(1, &parent_terminal).await.unwrap();
    assert!(
        terminal.dispatches.is_empty(),
        "a sent steering successor owns the lane before queued creates"
    );
    let created = Bytes::from_static(br#"{"type":"response.created","stream_id":"main","response":{"id":"resp_steer_successor","previous_response_id":"resp_steer_parent","store":false}}"#);
    assert!(gateway.provider_message(1, &created).await.unwrap().forward);
    let completed = Bytes::from_static(br#"{"type":"response.completed","stream_id":"main","response":{"id":"resp_steer_successor","usage":{"input_tokens":3,"output_tokens":5}}}"#);
    let mut terminal = gateway.provider_message(1, &completed).await.unwrap();
    assert_eq!(terminal.dispatches.len(), 1);
    assert_eq!(terminal.dispatches.pop().unwrap().frame, queued_frame);
    gateway.close();

    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut usages = HashMap::new();
    while let Ok(event) = events.try_recv() {
        match event {
            LifecycleEvent::RequestStarted { event_id, .. } => starts.push(event_id),
            LifecycleEvent::RequestTerminated { event_id, .. } => ends.push(event_id),
            LifecycleEvent::UsageObserved {
                event_id, usage, ..
            } => {
                assert!(
                    usages
                        .insert(event_id, (usage.input_tokens, usage.output_tokens))
                        .is_none()
                );
            }
            _ => {}
        }
    }
    assert_eq!(
        starts.len(),
        3,
        "parent, queued create, and one steering successor only"
    );
    starts.sort();
    ends.sort();
    assert_eq!(starts, ends);
    let mut snapshots: Vec<_> = usages.into_values().collect();
    snapshots.sort();
    assert_eq!(
        snapshots,
        vec![(3, 5), (7, 11)],
        "steering never merges parent and successor usage"
    );
}

#[tokio::test]
async fn responses_websocket_completed_parent_steering_owns_idle_lane_before_next_create() {
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_007,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )]);
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    gateway
        .provider_message(1, &websocket_created("main", "resp_completed_parent"))
        .await
        .unwrap();
    gateway
        .provider_message(1, &websocket_completed("main", "resp_completed_parent"))
        .await
        .unwrap();
    let steer = Bytes::from_static(br#"{"type":"response.steer","previous_response_id":"resp_completed_parent","input":"continue this response"}"#);
    let successor = websocket_admit(&mut gateway, steer).await.unwrap();
    websocket_send(&mut gateway, &successor, 1).await;
    assert!(
        websocket_admit(&mut gateway, websocket_create("main", None))
            .await
            .is_none()
    );
    let created = Bytes::from_static(br#"{"type":"response.created","stream_id":"main","response":{"id":"resp_after_completed","previous_response_id":"resp_completed_parent","store":false}}"#);
    assert!(gateway.provider_message(1, &created).await.unwrap().forward);
    let mut terminal = gateway
        .provider_message(1, &websocket_completed("main", "resp_after_completed"))
        .await
        .unwrap();
    assert_eq!(
        terminal.dispatches.len(),
        1,
        "the next ordinary create starts only after the successor ends"
    );
    let next = terminal.dispatches.pop().unwrap();
    websocket_send(&mut gateway, &next, 1).await;
}

#[tokio::test]
async fn responses_websocket_recent_store_false_ids_are_per_provider_connection() {
    let fixture = Fixture::new(vec![
        TestUpstream::new(50_008, "openai-first", UpstreamKind::OpenaiApiKey),
        TestUpstream::new(50_009, "openai-second", UpstreamKind::OpenaiApiKey),
    ]);
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    let first_upstream = first.candidates[0].upstream_id;
    websocket_send(&mut gateway, &first, 1).await;
    gateway
        .provider_message(1, &websocket_created("main", "resp_connection_first"))
        .await
        .unwrap();
    gateway
        .provider_message(1, &websocket_completed("main", "resp_connection_first"))
        .await
        .unwrap();
    let second = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    let fallback = second
        .candidates
        .iter()
        .find(|candidate| candidate.upstream_id != first_upstream)
        .unwrap();
    gateway.assign_connection(
        second.turn_id,
        2,
        fallback.upstream_id,
        &fallback.upstream_name,
    );
    gateway.validate_send(second.turn_id).await.unwrap();
    assert!(gateway.sent(second.turn_id).is_empty());
    gateway
        .provider_message(2, &websocket_created("main", "resp_connection_second"))
        .await
        .unwrap();
    gateway
        .provider_message(2, &websocket_completed("main", "resp_connection_second"))
        .await
        .unwrap();
    let fork = websocket_admit(
        &mut gateway,
        websocket_create("fork", Some("resp_connection_first")),
    )
    .await
    .unwrap();
    assert_eq!(
        fork.connection_id,
        Some(1),
        "another provider connection does not erase this connection's session cache"
    );
    assert_eq!(fork.candidates.len(), 1);
    assert_eq!(fork.candidates[0].upstream_id, first_upstream);
}

#[tokio::test]
async fn responses_websocket_terminal_releases_concurrency_before_background_settlement() {
    let concurrency = Arc::new(KeyConcurrencyManager::new());
    let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let reconcile_rx = bus.attach_lifecycle_limit_reconcile(128);
    let fixture = Fixture::with_limits(
        vec![TestUpstream::new(
            50_010,
            "openai",
            UpstreamKind::OpenaiApiKey,
        )],
        vec![Limit {
            kind: LimitKind::Concurrent,
            window_secs: 0,
            cap_micros: 1,
        }],
    )
    .with_lifecycle(|lifecycle| {
        lifecycle
            .with_static_limit_subject(
                engine.clone(),
                PRINCIPAL.to_owned(),
                "ws-serial-key".to_owned(),
                StoredApiKeyRecord {
                    key_hash_b64: "ws-serial-key".to_owned(),
                    status: KeyStatus::Active,
                    ..Default::default()
                },
            )
            .with_event_bus(bus)
    });
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    gateway
        .provider_message(1, &websocket_created("main", "resp_serial_first"))
        .await
        .unwrap();
    assert_eq!(concurrency.current("ws-serial-key"), 1);
    gateway
        .provider_message(1, &websocket_completed("main", "resp_serial_first"))
        .await
        .unwrap();
    assert_eq!(
        concurrency.current("ws-serial-key"),
        0,
        "response completion, not pricing lag, ends concurrency"
    );
    let next = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &next, 1).await;
    assert_eq!(concurrency.current("ws-serial-key"), 1);
    gateway.close();
    assert_eq!(concurrency.current("ws-serial-key"), 0);
    let reconcile = spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, engine);
    reconcile.shutdown().await;
    assert_eq!(
        concurrency.current("ws-serial-key"),
        0,
        "late settlement never releases a slot twice"
    );
}

#[tokio::test]
async fn responses_websocket_early_steering_successor_preserves_parent_and_fifo() {
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_011,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )]);
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let parent = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &parent, 1).await;
    gateway
        .provider_message(1, &websocket_created("main", "resp_early_parent"))
        .await
        .unwrap();
    let queued = websocket_create("main", None);
    assert!(
        websocket_admit(&mut gateway, queued.clone())
            .await
            .is_none()
    );
    let steer = Bytes::from_static(br#"{"type":"response.steer","previous_response_id":"resp_early_parent","input":"steer before the parent terminal arrives"}"#);
    let successor = websocket_admit(&mut gateway, steer).await.unwrap();
    websocket_send(&mut gateway, &successor, 1).await;
    let early_created = Bytes::from_static(br#"{"type":"response.created","stream_id":"main","response":{"id":"resp_early_successor","previous_response_id":"resp_early_parent","store":false}}"#);
    assert!(
        gateway
            .provider_message(1, &early_created)
            .await
            .unwrap()
            .forward
    );
    let repeat = Bytes::from_static(br#"{"type":"response.steer","previous_response_id":"resp_early_parent","input":"repeat in the parent protection window"}"#);
    let repeat_dispatch = websocket_admit(&mut gateway, repeat).await.unwrap();
    assert_eq!(
        repeat_dispatch.turn_id, successor.turn_id,
        "an early id does not allocate a second successor"
    );
    assert!(gateway.sent(repeat_dispatch.turn_id).is_empty());
    let failed = Bytes::from_static(br#"{"type":"response.steer.failed","stream_id":"main","steer":{"id":"late_failed_steer","previous_response_id":"resp_early_parent"},"error":{"code":"response_not_found","message":"parent already completed"}}"#);
    assert!(
        gateway.provider_message(1, &failed).await.unwrap().forward,
        "a late steering failure cannot cancel a committed successor"
    );
    assert!(
        gateway
            .provider_message(1, &websocket_completed("main", "resp_early_parent"))
            .await
            .unwrap()
            .dispatches
            .is_empty()
    );
    let mut terminal = gateway
        .provider_message(1, &websocket_completed("main", "resp_early_successor"))
        .await
        .unwrap();
    assert_eq!(terminal.dispatches.len(), 1);
    assert_eq!(terminal.dispatches.pop().unwrap().frame, queued);
}

#[tokio::test]
async fn responses_websocket_durable_bind_failure_never_exposes_response_id() {
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_012,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(bus));
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let frame = Bytes::from_static(br#"{"type":"response.create","stream_id":"main","model":"gpt-5-codex","store":true,"input":"durable owner is required"}"#);
    let turn = websocket_admit(&mut gateway, frame).await.unwrap();
    websocket_send(&mut gateway, &turn, 1).await;
    let created = Bytes::from_static(br#"{"type":"response.created","stream_id":"main","response":{"id":"resp_must_not_be_exposed","store":true}}"#);
    let error = gateway
        .provider_message(1, &created)
        .await
        .err()
        .expect("missing durable storage fails before exposure");
    assert_eq!(error.code, "upstream_affinity_unavailable");
    assert!(
        !std::str::from_utf8(&error.to_bytes())
            .unwrap()
            .contains("resp_must_not_be_exposed")
    );
    let closed = gateway.connection_closed(1, &error.message);
    assert_eq!(closed.errors.len(), 1);
    assert!(closed.dispatches.is_empty());
    gateway.close();
    let mut started = Vec::new();
    let mut terminated = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            LifecycleEvent::RequestStarted { event_id, .. } => started.push(event_id),
            LifecycleEvent::RequestTerminated { event_id, .. } => terminated.push(event_id),
            _ => {}
        }
    }
    assert_eq!(started.len(), 1);
    assert_eq!(
        started, terminated,
        "failed binding still terminates its single turn once"
    );
}

#[tokio::test]
async fn responses_websocket_malformed_terminal_and_connection_error_are_fail_closed() {
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_013,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )]);
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let turn = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &turn, 1).await;
    gateway
        .provider_message(1, &websocket_created("main", "resp_terminal_owner"))
        .await
        .unwrap();
    let missing_id = Bytes::from_static(br#"{"type":"response.completed","stream_id":"main","response":{"usage":{"input_tokens":7,"output_tokens":11}}}"#);
    let error = gateway
        .provider_message(1, &missing_id)
        .await
        .err()
        .expect("a malformed terminal cannot silently finish an arbitrary turn");
    assert_eq!(error.code, "invalid_upstream_response");
    let native = Bytes::from_static(br#"{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"websocket_connection_limit_reached","message":"create a new connection"}}"#);
    let error = gateway
        .provider_message(1, &native)
        .await
        .err()
        .expect("connection errors must close the owning provider connection");
    assert_eq!(error.code, "websocket_connection_limit_reached");
    let closed = gateway.connection_closed(1, &error.message);
    assert_eq!(closed.errors.len(), 1);
    assert!(closed.dispatches.is_empty());
}

#[tokio::test]
async fn responses_websocket_reconnect_separates_durable_store_true_from_local_store_false() {
    use cc_lb_storage_api::{UpstreamCreate, UpstreamStore};

    // Change real eligibility under explicit FirstPick, not incidental record order.
    for stored in [false, true] {
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(
                "sqlite::memory:",
                Arc::new(TestClock::new_at_secs(NOW)),
            )
            .await
            .unwrap(),
        );
        storage.initialize().await.unwrap();
        let mut upstreams = Vec::new();
        for (id, name) in [(50_014, "openai-first"), (50_013, "openai-second")] {
            let mut record = UpstreamStore::create(
                storage.as_ref(),
                UpstreamCreate {
                    id: Uuid::from_u128(id),
                    name: name.to_owned(),
                    kind: UpstreamKind::OpenaiApiKey,
                    base_url: Some(Url::parse(&format!("http://{name}.invalid/")).unwrap()),
                    api_key_ciphertext: None,
                    oauth_tokens: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                },
            )
            .await
            .unwrap();
            record.enabled = name == "openai-first";
            upstreams.push(TestUpstream {
                record,
                credential: Credential::Healthy,
                replies: Vec::new(),
                quota: Vec::new(),
            });
        }
        let fixture = Fixture::new(upstreams)
            .with_lifecycle(|lifecycle| lifecycle.with_upstream_affinity_store(storage.clone()));
        let holder = fixture.dynamic_view.clone();
        assert_eq!(
            holder
                .load()
                .principal_view
                .get(PRINCIPAL)
                .unwrap()
                .resolved_pipeline(None)
                .terminal,
            TerminalStrategy::FirstPick,
            "this fixture intentionally selects the first eligible candidate",
        );
        let dispatch = fixture.dispatch.clone();
        let lifecycle = Arc::new(fixture.lifecycle);
        let headers = messages_request(Bytes::new()).headers().clone();
        let handshake = lifecycle.authenticate(&headers).await.unwrap();
        let mut first = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
            lifecycle.clone(),
            &handshake,
            headers.clone(),
            1_048_576,
            64,
        );
        let create = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.create", "stream_id": "main", "model": "gpt-5-codex",
                "store": stored, "input": "create a response before reconnect",
            }))
            .unwrap(),
        );
        let turn = websocket_admit(&mut first, create).await.unwrap();
        assert_eq!(
            turn.candidates.len(),
            1,
            "only the initial owner is enabled"
        );
        let owner = turn.candidates[0].upstream_id;
        websocket_send(&mut first, &turn, 1).await;
        let created = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "type": "response.created", "stream_id": "main",
                "response": {"id":"resp_before_reconnect", "store": stored},
            }))
            .unwrap(),
        );
        assert!(first.provider_message(1, &created).await.unwrap().forward);
        first
            .provider_message(1, &websocket_completed("main", "resp_before_reconnect"))
            .await
            .unwrap();
        first.close();

        let current = holder.load();
        let mut records = current.upstreams_snapshot().to_vec();
        let preferred = records
            .iter_mut()
            .find(|record| record.id != owner)
            .map(|record| {
                assert!(!record.enabled, "the alternative starts ineligible");
                record.enabled = true;
                record.id
            })
            .expect("a second upstream exists");
        assert!(
            records.iter().all(|record| record.enabled),
            "the durable owner remains eligible"
        );
        assert_ne!(preferred, owner);
        holder.store(
            DynamicViewBuilder::new(current.generation)
                .principal_view(current.principal_view.clone())
                .signer_factory(current.signer_factory.clone())
                .upstream_records(records)
                .build(),
        );
        let handshake = lifecycle.authenticate(&headers).await.unwrap();
        let mut reconnected = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
            lifecycle, &handshake, headers, 1_048_576, 64,
        );
        let independent = websocket_admit(&mut reconnected, websocket_create("independent", None))
            .await
            .unwrap();
        assert_eq!(
            independent.candidates[0].upstream_id, preferred,
            "independent full history follows current routing"
        );
        assert_ne!(independent.candidates[0].upstream_id, owner);
        let (signed, _) = reconnected
            .prepare_connection(&independent, &independent.candidates[0])
            .await
            .expect("the newly selected upstream can be signed");
        assert_eq!(signed.host_str(), Some("openai-second.invalid"));
        websocket_send(&mut reconnected, &independent, 2).await;
        let auth = reconnected.authenticate().await.unwrap();
        let continuation = reconnected
            .client_message(
                auth,
                websocket_create("continued", Some("resp_before_reconnect")),
            )
            .await;
        if stored {
            let continuation = continuation.unwrap().unwrap();
            assert_eq!(continuation.candidates.len(), 1);
            assert_eq!(continuation.candidates[0].upstream_id, owner);
            assert_eq!(
                continuation.connection_id, None,
                "durable ownership selects the same upstream, not a dead socket"
            );
            websocket_send(&mut reconnected, &continuation, 3).await;
            let other_principal = "principal-other";
            let other_lifecycle = Arc::new(
                Lifecycle::new_with_dynamic_view(
                    crate::common::managed_authn(other_principal),
                    Arc::new(DynamicViewHolder::new(
                        DynamicViewBuilder::new(0)
                            .principal_view(Arc::new(PrincipalView::for_tests(
                                other_principal,
                                true,
                                vec!["*".to_owned()],
                                Vec::new(),
                                HashMap::new(),
                            )))
                            .signer_factory(current.signer_factory.clone())
                            .upstream_records(current.upstreams_snapshot().to_vec())
                            .build(),
                    )),
                    dispatch.clone(),
                    LifecycleConfig::default(),
                    Arc::new(TestClock::new_at_secs(NOW)),
                )
                .with_upstream_affinity_store(storage.clone()),
            );
            let other_headers = messages_request(Bytes::new()).headers().clone();
            let other_auth = other_lifecycle.authenticate(&other_headers).await.unwrap();
            assert_eq!(other_auth.principal_id(), other_principal);
            let mut other = cc_lb_engine::responses_websocket::ResponsesWebSocketGateway::new(
                other_lifecycle,
                &other_auth,
                other_headers,
                1_048_576,
                64,
            );
            let auth = other.authenticate().await.unwrap();
            let error = other
                .client_message(
                    auth,
                    websocket_create("cross-principal", Some("resp_before_reconnect")),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.code, "upstream_affinity_unavailable",
                "another principal cannot resolve a stored response through the same upstream"
            );
        } else {
            let error = continuation.unwrap_err();
            assert_eq!(
                error.code, "upstream_affinity_unavailable",
                "connection-local state does not become durable or silently reroute"
            );
        }
    }
}

#[tokio::test]
async fn responses_websocket_expired_queued_reservation_rejects_before_sign_or_send() {
    let concurrency = Arc::new(KeyConcurrencyManager::new());
    let engine = LimitEngine::new(concurrency.clone(), Arc::new(cc_lb_clock::SystemClock));
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let reconcile_rx = bus.attach_lifecycle_limit_reconcile(128);
    let fixture = Fixture::with_limits(
        vec![TestUpstream::new(
            50_016,
            "openai",
            UpstreamKind::OpenaiApiKey,
        )],
        vec![Limit {
            kind: LimitKind::Concurrent,
            window_secs: 0,
            cap_micros: 4,
        }],
    )
    .with_lifecycle(|lifecycle| {
        lifecycle
            .with_static_limit_subject(
                engine.clone(),
                PRINCIPAL.to_owned(),
                "ws-expired-hold".to_owned(),
                StoredApiKeyRecord {
                    key_hash_b64: "ws-expired-hold".to_owned(),
                    status: KeyStatus::Active,
                    ..Default::default()
                },
            )
            .with_event_bus(bus)
    });
    let (mut gateway, dispatch_log) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    gateway
        .provider_message(1, &websocket_created("main", "resp_before_hold_expiry"))
        .await
        .unwrap();
    while events.try_recv().is_ok() {}
    assert!(
        websocket_admit(&mut gateway, websocket_create("main", None))
            .await
            .is_none()
    );
    let mut ids = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let LifecycleEvent::LimitDecision {
            decision: LimitDecisionKind::Reserved { reservation_id, .. },
            ..
        } = event
        {
            ids.push(reservation_id);
        }
    }
    assert_eq!(ids.len(), 1);
    assert!(!ids[0].is_empty());
    // The production TTL sweeper retires a reservation through this same API.
    assert!(engine.refund_by_id(&ids[0]));
    let mut complete = gateway
        .provider_message(1, &websocket_completed("main", "resp_before_hold_expiry"))
        .await
        .unwrap();
    let queued = complete.dispatches.pop().unwrap();
    let candidate = queued.candidates.first().unwrap();
    let error = gateway
        .prepare_connection(&queued, candidate)
        .await
        .expect_err("an expired hold cannot sign a new connection");
    assert_eq!(error.code, "reservation_expired");
    gateway.assign_connection(
        queued.turn_id,
        1,
        candidate.upstream_id,
        &candidate.upstream_name,
    );
    let error = gateway.validate_send(queued.turn_id).await.unwrap_err();
    assert_eq!(error.code, "reservation_expired");
    assert!(
        gateway
            .fail_dispatch(queued.turn_id, &error)
            .await
            .is_empty()
    );
    gateway.close();
    let reconcile = spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, engine.clone());
    reconcile.shutdown().await;
    assert_eq!(concurrency.current("ws-expired-hold"), 0);
    assert!(!engine.refund_by_id(&ids[0]));
    assert!(dispatch_log.hosts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn responses_websocket_generate_false_chains_id_without_inventing_usage() {
    let bus = Arc::new(cc_lb_control::InMemoryBus::new());
    let mut events = bus.subscribe_lifecycle();
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_017,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )])
    .with_lifecycle(|lifecycle| lifecycle.with_event_bus(bus));
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let warm = Bytes::from_static(br#"{"type":"response.create","stream_id":"main","model":"gpt-5-codex","store":false,"generate":false,"input":[]}"#);
    let warm_dispatch = websocket_admit(&mut gateway, warm.clone()).await.unwrap();
    assert_eq!(warm_dispatch.frame, warm);
    websocket_send(&mut gateway, &warm_dispatch, 1).await;
    assert!(
        gateway
            .provider_message(1, &websocket_created("main", "resp_warm"))
            .await
            .unwrap()
            .forward
    );
    let completed = gateway
        .provider_message(1, &websocket_completed("main", "resp_warm"))
        .await
        .unwrap();
    assert!(completed.forward);
    assert!(completed.dispatches.is_empty());
    let continuation = websocket_admit(&mut gateway, websocket_create("main", Some("resp_warm")))
        .await
        .unwrap();
    assert_eq!(continuation.connection_id, Some(1));
    assert_eq!(continuation.candidates.len(), 1);
    gateway.close();
    let mut starts = 0;
    let mut ends = 0;
    while let Ok(event) = events.try_recv() {
        match event {
            LifecycleEvent::UsageObserved { .. } => {
                panic!("missing provider usage must not become fabricated zero usage")
            }
            LifecycleEvent::RequestStarted { .. } => starts += 1,
            LifecycleEvent::RequestTerminated { .. } => ends += 1,
            _ => {}
        }
    }
    assert_eq!((starts, ends), (2, 2));
}

#[tokio::test]
async fn responses_websocket_queued_response_id_is_bound_before_creation_or_exposure() {
    let fixture = Fixture::new(vec![TestUpstream::new(
        50_018,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )]);
    let (mut gateway, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let first = websocket_admit(&mut gateway, websocket_create("main", None))
        .await
        .unwrap();
    websocket_send(&mut gateway, &first, 1).await;
    let queued = Bytes::from_static(br#"{"type":"response.queued","stream_id":"main","response":{"id":"resp_provider_queued","status":"queued","store":false,"usage":null}}"#);
    assert!(gateway.provider_message(1, &queued).await.unwrap().forward);
    let fork = websocket_admit(
        &mut gateway,
        websocket_create("fork", Some("resp_provider_queued")),
    )
    .await
    .unwrap();
    assert_eq!(
        fork.connection_id,
        Some(1),
        "a queued response id is already owned before the caller can reference it"
    );
    assert_eq!(fork.candidates.len(), 1);
    assert!(
        gateway
            .provider_message(1, &websocket_created("main", "resp_provider_queued"))
            .await
            .unwrap()
            .forward
    );

    let fixture = Fixture::new(vec![TestUpstream::new(
        50_019,
        "openai",
        UpstreamKind::OpenaiApiKey,
    )]);
    let (mut without_store, _) = websocket_gateway_fixture(fixture, 1_048_576, 64).await;
    let create = Bytes::from_static(br#"{"type":"response.create","stream_id":"main","model":"gpt-5-codex","store":true,"input":"stored queue ownership"}"#);
    let first = websocket_admit(&mut without_store, create).await.unwrap();
    websocket_send(&mut without_store, &first, 1).await;
    let queued = Bytes::from_static(br#"{"type":"response.queued","stream_id":"main","response":{"id":"resp_queued_must_not_escape","status":"queued","store":true,"usage":null}}"#);
    let error = without_store
        .provider_message(1, &queued)
        .await
        .err()
        .expect("queued ids must fail closed when durable binding fails");
    assert_eq!(error.code, "upstream_affinity_unavailable");
    assert!(
        !std::str::from_utf8(&error.to_bytes())
            .unwrap()
            .contains("resp_queued_must_not_escape")
    );
}
