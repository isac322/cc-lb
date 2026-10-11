//! Scripted dispatch only: these tests never contact a provider.
use crate::common::{TestAuthn, TestLifecycleBus, TestState, collect_body, messages_request};
use async_trait::async_trait;
use axum::body::Body;
use bytes::Bytes;
use cc_lb_control::api_keys::principal_view::{DialectCache, PrincipalView, RouterPipelineCache};
use cc_lb_control::{DynamicViewBuilder, DynamicViewHolder};
use cc_lb_domain::{Principal, TerminalStrategy, UpstreamCandidate};
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DispatchError, Lifecycle, LifecycleConfig, UpstreamDispatch,
};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_storage_api::{
    PromptCacheObservationStore, PromptCacheProtection, StorageError, StorageResult,
};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError,
};
use http::{Response, StatusCode};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Default)]
struct Calls {
    urls: Mutex<Vec<String>>,
    signers: Mutex<Vec<String>>,
    filters: Mutex<Vec<Vec<Uuid>>>,
    protection: Mutex<Option<(Uuid, u64)>>,
    clock: Arc<cc_lb_clock::TestClock>,
    streaming: std::sync::atomic::AtomicBool,
    sign_delay: std::sync::atomic::AtomicBool,
    block_refresh: std::sync::atomic::AtomicBool,
    decline_refresh: std::sync::atomic::AtomicBool,
    prefer_second: std::sync::atomic::AtomicBool,
    refresh_started: tokio::sync::Notify,
    refresh_release: tokio::sync::Notify,
    records: Mutex<Vec<cc_lb_storage_api::PromptCacheObservationRecord>>,
}
struct Factory {
    authn: TestAuthn,
    calls: Arc<Calls>,
}
impl ApiKeyAwareSignerFactory for Factory {
    fn with_router_choice(&self, name: String) -> Arc<dyn SignerFactory> {
        self.calls.signers.lock().unwrap().push(name.clone());
        Arc::new(ClockedFactory {
            inner: self.authn.with_router_choice(name),
            calls: self.calls.clone(),
        })
    }
}
struct ClockedFactory {
    inner: Arc<dyn SignerFactory>,
    calls: Arc<Calls>,
}
#[async_trait]
impl SignerFactory for ClockedFactory {
    async fn build(
        &self,
        upstream: &cc_lb_domain::Upstream,
    ) -> Result<Arc<dyn Signer>, SignerError> {
        Ok(Arc::new(ClockedSigner {
            inner: self.inner.build(upstream).await?,
            calls: self.calls.clone(),
        }))
    }
}
struct ClockedSigner {
    inner: Arc<dyn Signer>,
    calls: Arc<Calls>,
}
#[async_trait]
impl Signer for ClockedSigner {
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        if self
            .calls
            .sign_delay
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.calls.clock.advance_secs(100);
        }
        self.inner.sign(shaped, capability).await
    }
    async fn on_unauthorized(&self, error: &UpstreamError) -> RetryDecision {
        if self
            .calls
            .block_refresh
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.calls.refresh_started.notify_one();
            self.calls.refresh_release.notified().await;
        }
        if self
            .calls
            .decline_refresh
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return RetryDecision::Fail;
        }
        self.inner.on_unauthorized(error).await
    }
}
struct DroppedSink {
    calls: Arc<Calls>,
}
impl cc_lb_control::PromptCacheObservationSinkLike for DroppedSink {
    fn enqueue(
        &self,
        record: cc_lb_storage_api::PromptCacheObservationRecord,
    ) -> Result<(), cc_lb_control::PromptCacheObservationEnqueueError> {
        self.calls.records.lock().unwrap().push(record);
        Err(cc_lb_control::PromptCacheObservationEnqueueError::ChannelFull)
    }
}
struct Dispatch {
    statuses: Mutex<VecDeque<StatusCode>>,
    calls: Arc<Calls>,
}
#[async_trait]
impl UpstreamDispatch for Dispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        self.calls
            .urls
            .lock()
            .unwrap()
            .push(request.url().to_string());
        let status = self
            .statuses
            .lock()
            .unwrap()
            .pop_front()
            .expect("bounded scripted calls");
        if status.is_success()
            && self
                .calls
                .streaming
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            let stream = async_stream::stream! {
                yield Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\n"));
                std::future::pending::<()>().await;
            };
            return Ok(Response::builder()
                .status(status)
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream))
                .unwrap());
        }
        Ok(Response::builder()
            .status(status)
            .header("x-scripted-account", request.url().host_str().unwrap())
            .body(Body::from(if status.is_success() {
                r#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":4096}}"#
            } else {
                "original upstream rejection"
            }))
            .unwrap())
    }
}
struct Filter {
    hard_pin: bool,
    calls: Arc<Calls>,
}
impl FilterPlugin for Filter {
    fn filter(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        self.calls
            .filters
            .lock()
            .unwrap()
            .push(candidates.iter().map(|c| c.upstream_id).collect());
        let prefer_second = self
            .calls
            .prefer_second
            .load(std::sync::atomic::Ordering::SeqCst)
            && candidates
                .iter()
                .any(|candidate| candidate.upstream_id == Uuid::from_u128(2));
        Ok(FilterOutput {
            kept_upstream_ids: candidates
                .iter()
                .filter(|candidate| !self.hard_pin || candidate.upstream_id == Uuid::from_u128(1))
                .filter(|candidate| !prefer_second || candidate.upstream_id == Uuid::from_u128(2))
                .map(|candidate| candidate.upstream_id)
                .collect(),
            reason: "test policy".to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        })
    }
    fn plugin_id(&self) -> Uuid {
        Uuid::nil()
    }
    fn plugin_name(&self) -> &str {
        "failover-test-policy"
    }
}
struct Store {
    unavailable: bool,
    historical: bool,
    calls: Arc<Calls>,
}
#[async_trait]
impl PromptCacheObservationStore for Store {
    async fn list_active_protection(
        &self,
        ids: &[Uuid],
        model: &str,
        now: u64,
    ) -> StorageResult<Vec<PromptCacheProtection>> {
        if self.unavailable {
            return Err(StorageError::Unavailable {
                message: "scripted read failure".to_owned(),
            });
        }
        if let Some((id, deadline)) = *self.calls.protection.lock().unwrap() {
            return Ok(if ids.contains(&id) && deadline > now {
                vec![PromptCacheProtection {
                    upstream_id: id,
                    canonical_model_id: Some(model.to_owned()),
                    protection_expires_at_unix_secs: deadline,
                }]
            } else {
                Vec::new()
            });
        }
        Ok(if self.historical {
            ids.iter()
                .map(|id| PromptCacheProtection {
                    upstream_id: *id,
                    canonical_model_id: Some(model.to_owned()),
                    protection_expires_at_unix_secs: now + 3600,
                })
                .collect()
        } else {
            Vec::new()
        })
    }
}
fn setup(
    enabled: bool,
    hard_pin: bool,
    unavailable: bool,
    historical: bool,
    statuses: &[StatusCode],
) -> (Lifecycle, Arc<Calls>, TestState) {
    let calls = Arc::new(Calls::default());
    calls.clock.set_unix_secs(1000);
    let mut chains = HashMap::new();
    chains.insert(
        "principal-test".to_owned(),
        (
            Some(Arc::new(RouterPipelineCache {
                user_filters: vec![Arc::new(Filter {
                    hard_pin,
                    calls: calls.clone(),
                })],
                terminal: TerminalStrategy::FirstPick,
                instantiation_error: None,
            })),
            DialectCache::Inherit,
        ),
    );
    let principal_view = Arc::new(PrincipalView::for_tests(
        "principal-test",
        true,
        vec!["*".to_owned()],
        Vec::new(),
        chains,
    ));
    let state = TestState::default();
    let authn = TestAuthn::with_principal_view(state.clone(), principal_view.clone());
    let records = (1..=3)
        .map(|id| UpstreamRecord {
            id: Uuid::from_u128(id),
            name: format!("account-{id}"),
            kind: UpstreamKind::AnthropicOauth,
            enabled: true,
            base_url: Some(format!("http://account-{id}.test/").parse().unwrap()),
            ..UpstreamRecord::default()
        })
        .collect();
    let view = DynamicViewBuilder::new(0)
        .principal_view(principal_view)
        .upstream_records(records)
        .signer_factory(Arc::new(Factory {
            authn: authn.clone(),
            calls: calls.clone(),
        }))
        .prompt_cache_observation_sink(Arc::new(DroppedSink {
            calls: calls.clone(),
        }))
        .prompt_cache_observation_store(Arc::new(Store {
            unavailable,
            historical,
            calls: calls.clone(),
        }))
        .build();
    let mut config = LifecycleConfig::default();
    config.prompt_cache_shadow.subscription_failover = enabled;
    let lifecycle = Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        Arc::new(Dispatch {
            statuses: Mutex::new(statuses.iter().copied().collect()),
            calls: calls.clone(),
        }),
        config,
        calls.clock.clone(),
    );
    (lifecycle, calls, state)
}
async fn request(lifecycle: &Lifecycle) -> Response<Body> {
    let request = messages_request(Bytes::from_static(
        br#"{"model":"claude-test","messages":[]}"#,
    ));
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    lifecycle.handle(request, &auth).await.unwrap()
}

#[tokio::test]
async fn subscription_429_selects_one_distinct_account_with_final_route_and_attempt_identity() {
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[StatusCode::TOO_MANY_REQUESTS, StatusCode::OK],
    );
    let bus = TestLifecycleBus::new();
    let mut events = bus.bus.attach_lifecycle_writer(64);
    let lifecycle = lifecycle.with_event_bus(bus.bus_arc());
    let (status, headers, _) = collect_body(request(&lifecycle).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-scripted-account"], "account-2.test");
    assert_eq!(*calls.signers.lock().unwrap(), ["account-1", "account-2"]);
    assert_eq!(
        *calls.urls.lock().unwrap(),
        [
            "http://account-1.test/v1/messages",
            "http://account-2.test/v1/messages"
        ]
    );
    assert_eq!(
        *calls.filters.lock().unwrap(),
        [
            vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)],
            vec![Uuid::from_u128(2), Uuid::from_u128(3)]
        ]
    );
    let mut attempts = Vec::new();
    let mut routes = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match events.recv().await.unwrap() {
                LifecycleEvent::UpstreamAttempt {
                    attempt_num,
                    upstream_id,
                    ..
                } => attempts.push((attempt_num, upstream_id)),
                LifecycleEvent::RouteCompleted {
                    result: Ok(route), ..
                } => routes.push(route),
                LifecycleEvent::RequestTerminated { .. } => break,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(attempts, [(1, Uuid::from_u128(1)), (2, Uuid::from_u128(2))]);
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].upstream_id, Uuid::from_u128(2));
}

#[tokio::test]
async fn default_off_hard_filter_unavailable_and_historical_unknown_preserve_original_429() {
    for (enabled, hard, unavailable, historical) in [
        (false, false, false, false),
        (true, true, false, false),
        (true, false, true, false),
        (true, false, false, true),
    ] {
        let (lifecycle, calls, _) = setup(
            enabled,
            hard,
            unavailable,
            historical,
            &[StatusCode::TOO_MANY_REQUESTS],
        );
        let (status, headers, body) = collect_body(request(&lifecycle).await).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(headers["x-scripted-account"], "account-1.test");
        assert_eq!(body, Bytes::from_static(b"original upstream rejection"));
        assert_eq!(calls.urls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn repeated_429_never_dispatches_a_third_distinct_account() {
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[StatusCode::TOO_MANY_REQUESTS, StatusCode::TOO_MANY_REQUESTS],
    );
    assert_eq!(
        request(&lifecycle).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(calls.urls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn primary_and_alternate_preserve_same_account_401_refresh() {
    for statuses in [
        vec![
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::OK,
        ],
        vec![
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::UNAUTHORIZED,
            StatusCode::OK,
        ],
    ] {
        let (lifecycle, calls, state) = setup(true, false, false, false, &statuses);
        assert_eq!(request(&lifecycle).await.status(), StatusCode::OK);
        assert_eq!(
            state
                .refresh_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        let urls = calls.urls.lock().unwrap();
        assert_eq!(urls.len(), 3);
        assert!(urls[0] == urls[1] || urls[1] == urls[2]);
    }
}

#[tokio::test]
async fn unrelated_one_hour_warmth_at_fifty_minutes_is_removed_before_filter_and_third_account_wins()
 {
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[StatusCode::TOO_MANY_REQUESTS, StatusCode::OK],
    );
    *calls.protection.lock().unwrap() = Some((Uuid::from_u128(2), 4600));
    calls.clock.set_unix_secs(4000);
    let (status, headers, _) = collect_body(request(&lifecycle).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-scripted-account"], "account-3.test");
    assert_eq!(*calls.signers.lock().unwrap(), ["account-1", "account-3"]);
    assert_eq!(calls.filters.lock().unwrap()[1], [Uuid::from_u128(3)]);
}

#[tokio::test]
async fn fallback_stream_claim_survives_unpolled_body_and_releases_on_drop() {
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::OK,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::OK,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::OK,
        ],
    );
    calls
        .streaming
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let first = request(&lifecycle).await;
    assert_eq!(calls.signers.lock().unwrap()[1], "account-2");
    let second = request(&lifecycle).await;
    assert_eq!(calls.signers.lock().unwrap()[3], "account-3");
    drop(first);
    drop(second);
    let third = request(&lifecycle).await;
    assert_eq!(calls.signers.lock().unwrap()[5], "account-2");
    drop(third);
}

#[tokio::test]
async fn alternate_storage_and_dropped_write_protection_use_actual_dispatch_after_delayed_signing()
{
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[StatusCode::TOO_MANY_REQUESTS, StatusCode::OK],
    );
    calls
        .sign_delay
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let body = serde_json::json!({"model":"claude-test", "max_tokens":1,
        "system":[{"type":"text","text":"distinct cache text ".repeat(8000), "cache_control":{"type":"ephemeral","ttl":"1h"}}], "messages":[]});
    let request = messages_request(Bytes::from(serde_json::to_vec(&body).unwrap()));
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let records = calls.records.lock().unwrap();
    assert!(
        !records.is_empty(),
        "decoded usage produces cacheable observation despite dropped writer"
    );
    assert!(
        records
            .iter()
            .all(|record| record.upstream_id == Uuid::from_u128(2)
                && record.protection_expires_at_unix_secs == Some(4800))
    );
    drop(records);
    // The registry retains protection even though no storage writer received these rows.
    assert_eq!(
        cc_lb_clock::unix_secs(cc_lb_clock::Clock::now(calls.clock.as_ref())),
        1200
    );
}

struct PinnedAffinity;
#[async_trait]
impl cc_lb_storage_api::UpstreamAffinityStore for PinnedAffinity {
    async fn resolve_upstream_affinities(
        &self,
        keys: &[cc_lb_storage_api::UpstreamAffinityKey],
        now: u64,
        _ttl: u64,
    ) -> StorageResult<Vec<cc_lb_storage_api::UpstreamAffinityBinding>> {
        Ok(keys
            .iter()
            .map(|key| cc_lb_storage_api::UpstreamAffinityBinding {
                key: key.clone(),
                upstream_id: Uuid::from_u128(1),
                observed_at_unix_secs: now,
                expires_at_unix_secs: Some(now + 3600),
            })
            .collect())
    }
    async fn bind_upstream_affinities(
        &self,
        _bindings: &[cc_lb_storage_api::UpstreamAffinityBinding],
        _now: u64,
        _ttl: u64,
    ) -> StorageResult<()> {
        Ok(())
    }
    async fn purge_expired_upstream_affinities(
        &self,
        _now: u64,
        _ttl: u64,
        _batch: usize,
    ) -> StorageResult<u64> {
        Ok(0)
    }
}

#[tokio::test]
async fn encrypted_web_tool_affinity_forbids_switching_subscription() {
    let (lifecycle, calls, _) = setup(true, false, false, false, &[StatusCode::TOO_MANY_REQUESTS]);
    let lifecycle = lifecycle.with_upstream_affinity_store(Arc::new(PinnedAffinity));
    let body = serde_json::json!({"model":"claude-test", "messages":[{"role":"user","content":[{"type":"web_search_tool_result","content":[{"encrypted_content":"opaque-continuation"}]}]}]});
    let request = messages_request(Bytes::from(serde_json::to_vec(&body).unwrap()));
    let auth = lifecycle.authenticate(request.headers()).await.unwrap();
    let response = lifecycle.handle(request, &auth).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(calls.urls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn keepalive_refresh_protects_one_hour_idle_conversation_across_view_reload() {
    use cc_lb_engine::cache_keepalive::{
        AnthropicKeepaliveDispatcher, DispatchOutcome, KeepaliveDispatchContext,
        KeepaliveDispatcher, RequestSnapshot,
    };
    use cc_lb_storage_api::upstream::UpstreamCreate;
    use cc_lb_storage_api::{CacheTtl, MetaStore, UpstreamStore};
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[StatusCode::TOO_MANY_REQUESTS, StatusCode::OK],
    );
    let registry = Arc::new(cc_lb_engine::upstream_activity::UpstreamActivity::new(
        calls.clock.clone(),
    ));
    let lifecycle = lifecycle.with_upstream_activity(registry.clone());
    let holder = lifecycle.dynamic_view();
    let temp = tempfile::tempdir().unwrap();
    let storage = cc_lb_storage_sqlite::open_sqlite(
        &format!(
            "sqlite://{}",
            temp.path().join("keepalive.sqlite").display()
        ),
        calls.clock.clone(),
    )
    .await
    .unwrap();
    storage.initialize().await.unwrap();
    storage
        .create(UpstreamCreate {
            id: Uuid::from_u128(2),
            name: "account-2".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            base_url: Some("http://account-2.test/".parse().unwrap()),
            api_key_ciphertext: None,
            oauth_tokens: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        })
        .await
        .unwrap();
    let dispatcher = AnthropicKeepaliveDispatcher::new(
        holder.clone(),
        Arc::new(storage),
        Arc::new(Dispatch {
            statuses: Mutex::new(VecDeque::from([StatusCode::OK])),
            calls: calls.clone(),
        }),
    )
    .with_upstream_activity(registry);
    let body = serde_json::json!({"model":"claude-test", "max_tokens":1, "system":[{"type":"text", "text":"prefix", "cache_control":{"type":"ephemeral","ttl":"1h"}}], "messages":[]});
    let snapshot = RequestSnapshot {
        url: "http://account-2.test/v1/messages".parse().unwrap(),
        method: http::Method::POST,
        headers: http::HeaderMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        upstream_id: Uuid::from_u128(2),
        ttl: CacheTtl::Ttl1h,
    };
    assert!(matches!(
        dispatcher
            .dispatch(
                &snapshot,
                KeepaliveDispatchContext::new(None, "test-renewal".to_owned())
                    .with_principal("principal-test".to_owned())
            )
            .await,
        DispatchOutcome::CacheMiss { .. }
    ));
    calls.clock.set_unix_secs(4000);
    holder.store(DynamicViewBuilder::from_view(&holder.load()).build());
    let (status, headers, _) = collect_body(request(&lifecycle).await).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-scripted-account"], "account-3.test");
    assert_eq!(
        *calls.signers.lock().unwrap(),
        ["account-2", "account-1", "account-3"]
    );
}

#[tokio::test]
async fn primary_refresh_losing_admission_finishes_original_401_without_phantom_attempt() {
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::OK,
        ],
    );
    let bus = TestLifecycleBus::new();
    let mut events = bus.bus.attach_lifecycle_assembler(128);
    let lifecycle = Arc::new(lifecycle.with_event_bus(bus.bus_arc()));
    calls
        .block_refresh
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let pending = tokio::spawn({
        let lifecycle = lifecycle.clone();
        async move { request(&lifecycle).await }
    });
    calls.refresh_started.notified().await;
    // During credential refresh another conversation takes account1 as its fallback.
    calls
        .prefer_second
        .store(true, std::sync::atomic::Ordering::SeqCst);
    calls
        .streaming
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let protected = request(&lifecycle).await;
    assert_eq!(protected.status(), StatusCode::OK);
    assert_eq!(
        *calls.urls.lock().unwrap(),
        [
            "http://account-1.test/v1/messages",
            "http://account-2.test/v1/messages",
            "http://account-1.test/v1/messages"
        ]
    );
    calls.refresh_release.notify_one();
    let (status, headers, body) = collect_body(pending.await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers["x-scripted-account"], "account-1.test");
    assert_eq!(body, "original upstream rejection");
    let observed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut observed = Vec::new();
        loop {
            let event = events.recv().await.unwrap();
            let done = matches!(
                &event,
                LifecycleEvent::RequestTerminated {
                    client_status: 401,
                    ..
                }
            );
            observed.push(event);
            if done {
                break observed;
            }
        }
    })
    .await
    .unwrap();
    let first_id = observed
        .iter()
        .find_map(|event| match event {
            LifecycleEvent::UpstreamAttempt {
                event_id,
                upstream_id,
                ..
            } if *upstream_id == Uuid::from_u128(1) => Some(event_id),
            _ => None,
        })
        .unwrap();
    let original: Vec<_> = observed
        .iter()
        .filter(|event| event.event_id() == first_id)
        .collect();
    assert_eq!(
        original
            .iter()
            .filter(|event| matches!(event, LifecycleEvent::UpstreamAttempt { .. }))
            .count(),
        1
    );
    assert_eq!(
        original
            .iter()
            .filter(|event| matches!(
                event,
                LifecycleEvent::UpstreamResponseStarted { status: 401, .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        original
            .iter()
            .filter(|event| matches!(event, LifecycleEvent::RouteCompleted { .. }))
            .count(),
        1
    );
    assert_eq!(
        original
            .iter()
            .filter(|event| matches!(
                event,
                LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        original
            .iter()
            .filter(|event| matches!(
                event,
                LifecycleEvent::RequestTerminated {
                    client_status: 401,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(
        original.iter().any(|event| matches!(event,
            LifecycleEvent::RequestTerminated {
                reason: cc_lb_lifecycle::TerminationReason::ErrorCode(code), client_status: 401, ..
            } if code == "upstream_4xx"
        )),
        "original rejection must finish explicitly rather than report cancellation"
    );
    assert_eq!(
        calls.urls.lock().unwrap().len(),
        3,
        "denied refresh must never dispatch"
    );
    drop(protected);
}

#[tokio::test]
async fn alternate_declining_401_refresh_emits_one_response_event_per_actual_attempt() {
    let (lifecycle, calls, _) = setup(
        true,
        false,
        false,
        false,
        &[StatusCode::TOO_MANY_REQUESTS, StatusCode::UNAUTHORIZED],
    );
    calls
        .decline_refresh
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let bus = TestLifecycleBus::new();
    let mut events = bus.bus.attach_lifecycle_writer(64);
    let lifecycle = lifecycle.with_event_bus(bus.bus_arc());
    assert_eq!(request(&lifecycle).await.status(), StatusCode::UNAUTHORIZED);
    let statuses = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut statuses = Vec::new();
        loop {
            match events.recv().await.unwrap() {
                LifecycleEvent::UpstreamResponseStarted { status, .. } => statuses.push(status),
                LifecycleEvent::RequestTerminated {
                    client_status: 401, ..
                } => break statuses,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(statuses, [429, 401]);
    assert_eq!(calls.urls.lock().unwrap().len(), 2);
}
