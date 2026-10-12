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
        let lifecycle = Lifecycle::new_with_dynamic_view(
            authn.authn,
            Arc::new(DynamicViewHolder::new(view)),
            dispatch.clone(),
            LifecycleConfig::default(),
            Arc::new(TestClock::new_at_secs(NOW)),
        );
        Self {
            lifecycle,
            dispatch,
            quota_cache,
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

    let (status, _body) = fixture.send().await;
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
    // A relayed upstream error hands its reservation to the engine's TTL
    // sweeper rather than the success reconcile path. An exhausted failover
    // must leave exactly the state a single relayed 429 leaves: one held
    // reservation, not one per tried upstream.
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
    assert_eq!(outcome.requests_remaining, REQUEST_CAP - 1);
    assert_eq!(outcome.requests_remaining, baseline.requests_remaining);
    assert_eq!(outcome.tokens_remaining, baseline.tokens_remaining);
    assert_eq!(outcome.concurrent_in_flight, baseline.concurrent_in_flight);
    assert!(outcome.concurrent_in_flight <= 1);
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
