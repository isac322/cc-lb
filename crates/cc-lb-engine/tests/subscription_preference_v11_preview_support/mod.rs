use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_clock::TestClock;
use cc_lb_domain::{
    Principal, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState, TerminalStrategy,
    UpstreamCandidate,
};
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_engine::lifecycle::{PreviewRouteInput, PreviewRouteOutcome};
use cc_lb_engine::{
    Body, DispatchError, DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig,
    SubscriptionQuotaCacheLike, UpstreamDispatch,
};
use cc_lb_routing::{RouteDecision, RouteError, RouterPlugin};
use cc_lb_storage_api::SubscriptionQuotaSample;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use cc_lb_upstream::SignedRequest;
use http::{HeaderMap, Response, StatusCode};
use parking_lot::Mutex as ParkingMutex;
use url::Url;
use uuid::Uuid;

use super::common::{RecordingHook, TestAuthn, TestState, collect_body, messages_request};

const PRINCIPAL: &str = "principal-test";
const NOW: u64 = 1_700_000_000;

pub struct PreviewFixture {
    lifecycle: Lifecycle,
    cache: Arc<MutableQuotaCache>,
    dispatch: Arc<RecordingDispatch>,
    pub urgent_id: Uuid,
    pub steady_id: Uuid,
}

impl PreviewFixture {
    pub fn new(
        urgent: Vec<SubscriptionQuotaCandidateSnapshot>,
        steady: Vec<SubscriptionQuotaCandidateSnapshot>,
    ) -> Self {
        Self::with_statuses(urgent, steady, StatusCode::OK, StatusCode::OK)
    }

    pub fn with_statuses(
        urgent: Vec<SubscriptionQuotaCandidateSnapshot>,
        steady: Vec<SubscriptionQuotaCandidateSnapshot>,
        urgent_status: StatusCode,
        steady_status: StatusCode,
    ) -> Self {
        let urgent_id = Uuid::from_u128(1);
        let steady_id = Uuid::from_u128(2);
        let cache = Arc::new(MutableQuotaCache::new(HashMap::from([
            (urgent_id, urgent),
            (steady_id, steady),
        ])));
        let dispatch = Arc::new(RecordingDispatch::new(urgent_status, steady_status));
        let lifecycle = lifecycle(Arc::clone(&cache), [urgent_id, steady_id], dispatch.clone());
        Self {
            lifecycle,
            cache,
            dispatch,
            urgent_id,
            steady_id,
        }
    }

    pub async fn preview(&self, request_id: &str) -> PreviewRouteOutcome {
        self.preview_model(request_id, "claude-test").await
    }

    pub async fn preview_model(&self, request_id: &str, model: &str) -> PreviewRouteOutcome {
        let body = format!(r#"{{"model":"{model}","messages":[]}}"#);
        self.lifecycle
            .preview_route(PreviewRouteInput {
                principal_id: PRINCIPAL.to_owned(),
                request_id: Some(request_id.to_owned()),
                headers: HeaderMap::new(),
                body_bytes: Bytes::from(body),
            })
            .await
            .expect("preview succeeds")
    }

    pub async fn handle_model(&self, model: &str) -> StatusCode {
        let body = format!(r#"{{"model":"{model}","messages":[]}}"#);
        let request = messages_request(Bytes::from(body));
        let auth = self
            .lifecycle
            .authenticate(request.headers())
            .await
            .expect("test request authenticates");
        let response = self
            .lifecycle
            .handle(request, &auth)
            .await
            .expect("handle succeeds");
        collect_body(response).await.0
    }

    pub fn dispatch_hosts(&self) -> Vec<String> {
        self.dispatch.calls.lock().clone()
    }

    pub fn set_quota(&self, upstream_id: Uuid, quotas: Vec<SubscriptionQuotaCandidateSnapshot>) {
        self.cache
            .snapshots
            .lock()
            .expect("quota cache lock")
            .insert(upstream_id, quotas);
    }

    pub fn swap_quota_states(&self) {
        let mut snapshots = self.cache.snapshots.lock().expect("quota cache lock");
        *snapshots = HashMap::from([
            (self.urgent_id, on_pace_quota()),
            (self.steady_id, urgent_quota()),
        ]);
    }
}

struct MutableQuotaCache {
    snapshots: Mutex<HashMap<Uuid, Vec<SubscriptionQuotaCandidateSnapshot>>>,
}

impl MutableQuotaCache {
    fn new(snapshots: HashMap<Uuid, Vec<SubscriptionQuotaCandidateSnapshot>>) -> Self {
        Self {
            snapshots: Mutex::new(snapshots),
        }
    }
}

impl SubscriptionQuotaCacheLike for MutableQuotaCache {
    fn upsert_observation(&self, _record: &SubscriptionQuotaSample) {}

    fn snapshot_for_upstream(
        &self,
        upstream_id: Uuid,
        _now_unix_millis: u64,
        _max_staleness_secs: u64,
    ) -> Vec<SubscriptionQuotaCandidateSnapshot> {
        self.snapshots
            .lock()
            .expect("quota cache lock")
            .get(&upstream_id)
            .cloned()
            .unwrap_or_default()
    }
}

fn lifecycle(
    cache: Arc<MutableQuotaCache>,
    ids: [Uuid; 2],
    dispatcher: Arc<dyn UpstreamDispatch>,
) -> Lifecycle {
    let state = TestState::default();
    let principal_view = principal_view();
    let authn = TestAuthn::with_principal_view(state, Arc::clone(&principal_view));
    let view = DynamicViewBuilder::new(0)
        .signer_factory(Arc::new(authn.clone()))
        .global_router(Arc::new(NoopRouter))
        .global_observability_hooks(vec![Arc::new(RecordingHook::default())])
        .principal_view(principal_view)
        .subscription_quota_cache(cache)
        .subscription_quota_routing_max_staleness_secs(60)
        .upstream_records(vec![upstream(ids[0], "urgent"), upstream(ids[1], "steady")])
        .build();
    Lifecycle::new_with_dynamic_view(
        authn.authn,
        Arc::new(DynamicViewHolder::new(view)),
        dispatcher,
        LifecycleConfig::default(),
        Arc::new(TestClock::new_at_secs(NOW)),
    )
}

fn principal_view() -> Arc<PrincipalView> {
    let pipeline = Arc::new(RouterPipelineCache {
        user_filters: vec![Arc::new(SubscriptionPreferenceFilter::new())],
        terminal: TerminalStrategy::FirstPick,
        instantiation_error: None,
    });
    Arc::new(PrincipalView::for_tests(
        PRINCIPAL,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::from([(
            PRINCIPAL.to_owned(),
            (
                Some(pipeline),
                ObservabilityHooksCache::Inherit,
                DialectCache::Inherit,
            ),
        )]),
    ))
}

fn upstream(id: Uuid, name: &str) -> UpstreamRecord {
    UpstreamRecord {
        id,
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        base_url: Some(
            Url::parse(&format!("http://{name}.invalid/")).expect("test upstream URL parses"),
        ),
        enabled: true,
        revision: 1,
        ..UpstreamRecord::default()
    }
}

pub fn urgent_quota() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![quota("5h", 0.0, NOW + 1), quota("7d", 0.2, NOW + 60_480)]
}

pub fn on_pace_quota() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![
        quota("5h", 0.9, NOW + 1_800),
        quota("7d", 0.95, NOW + 60_480),
    ]
}

pub fn exhausted_shared_quota() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    let mut weekly = quota("7d", 1.0, NOW + 60_480);
    weekly.status = Some("rejected".to_owned());
    vec![quota("5h", 0.0, NOW + 1_800), weekly]
}

pub fn sonnet_quota(scoped_utilization: Option<f64>) -> Vec<SubscriptionQuotaCandidateSnapshot> {
    let mut snapshots = vec![
        quota("5h", 0.2, NOW + 1_800),
        quota("7d", 0.2, NOW + 60_480),
    ];
    let scoped = match scoped_utilization {
        Some(utilization) => quota("7d_sonnet", utilization, NOW + 60_480),
        None => {
            let mut snapshot = quota("7d_sonnet", 0.0, NOW + 60_480);
            snapshot.state = SubscriptionQuotaDataState::Unobserved;
            snapshot.utilization = None;
            snapshot.status = None;
            snapshot.resets_at_unix_secs = None;
            snapshot
        }
    };
    snapshots.push(scoped);
    snapshots
}

pub fn stale_exhausted_shared_quota() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    let mut weekly = quota("7d", 1.0, NOW + 60_480);
    weekly.state = SubscriptionQuotaDataState::Stale;
    vec![quota("5h", 0.2, NOW + 1_800), weekly]
}

pub fn stale_overage_positive_quota() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    let mut overage = quota("overage", 0.2, NOW + 60_480);
    overage.state = SubscriptionQuotaDataState::Stale;
    vec![exhausted_shared_quota()[1].clone(), overage]
}

pub fn fable_quota(fable_utilization: f64) -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![
        quota("5h", 0.2, NOW + 1_800),
        quota("7d", 0.2, NOW + 60_480),
        quota("7d_fable", fable_utilization, NOW + 60_480),
    ]
}
pub fn fable_quota_without_shared_seven_day() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    let mut missing_shared = quota("7d", 0.0, NOW + 60_480);
    missing_shared.state = SubscriptionQuotaDataState::Absent;
    missing_shared.utilization = None;
    missing_shared.status = None;
    missing_shared.resets_at_unix_secs = None;
    vec![
        quota("5h", 0.2, NOW + 1_800),
        missing_shared,
        quota("7d_fable", 0.2, NOW + 60_480),
    ]
}
pub fn fable_quota_with_unobserved_shared_seven_day() -> Vec<SubscriptionQuotaCandidateSnapshot> {
    let mut shared = quota("7d", 0.0, NOW + 60_480);
    shared.state = SubscriptionQuotaDataState::Unobserved;
    shared.utilization = None;
    shared.status = None;
    shared.resets_at_unix_secs = None;
    vec![
        quota("5h", 0.2, NOW + 1_800),
        shared,
        quota("7d_fable", 0.2, NOW + 60_480),
    ]
}

fn quota(window: &str, utilization: f64, reset: u64) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
        state: SubscriptionQuotaDataState::Fresh,
        source: Some("v11-preview-fixture".to_owned()),
        utilization: Some(utilization),
        status: Some("allowed".to_owned()),
        resets_at_unix_secs: Some(reset),
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

struct RecordingDispatch {
    calls: ParkingMutex<Vec<String>>,
    statuses: HashMap<String, StatusCode>,
}

impl RecordingDispatch {
    fn new(urgent_status: StatusCode, steady_status: StatusCode) -> Self {
        Self {
            calls: ParkingMutex::new(Vec::new()),
            statuses: HashMap::from([
                ("urgent.invalid".to_owned(), urgent_status),
                ("steady.invalid".to_owned(), steady_status),
            ]),
        }
    }
}

#[async_trait]
impl UpstreamDispatch for RecordingDispatch {
    async fn dispatch(&self, request: SignedRequest) -> Result<Response<Body>, DispatchError> {
        let host = request
            .url()
            .host_str()
            .expect("test request has host")
            .to_owned();
        self.calls.lock().push(host.clone());
        let status = self.statuses[&host];
        let body = if status.is_success() {
            Bytes::from_static(
                br#"{"type":"message","usage":{"input_tokens":1,"output_tokens":1}}"#,
            )
        } else {
            Bytes::from_static(
                br#"{"type":"error","error":{"type":"rate_limit_error","message":"quota exhausted"}}"#,
            )
        };
        Ok(Response::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .expect("test response builds"))
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "preview uses terminal strategy".to_owned(),
        })
    }
}
