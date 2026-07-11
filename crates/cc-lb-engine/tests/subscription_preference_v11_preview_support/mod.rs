use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cc_lb_clock::TestClock;
use cc_lb_engine::api_keys::principal_view::{
    DialectCache, ObservabilityHooksCache, PrincipalView, RouterPipelineCache,
};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_engine::lifecycle::{PreviewRouteInput, PreviewRouteOutcome};
use cc_lb_engine::{
    DynamicViewBuilder, DynamicViewHolder, Lifecycle, LifecycleConfig, SubscriptionQuotaCacheLike,
};
use cc_lb_plugin_api::{
    Principal, RouteDecision, RouteError, RouterPlugin, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, TerminalStrategy, UpstreamCandidate,
};
use cc_lb_storage_api::SubscriptionQuotaSample;
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamRecord};
use http::HeaderMap;
use uuid::Uuid;

use crate::common::{DispatchMode, MockDispatch, RecordingHook, TestAuthn, TestState};

const PRINCIPAL: &str = "principal-test";
const NOW: u64 = 1_700_000_000;

pub struct PreviewFixture {
    lifecycle: Lifecycle,
    cache: Arc<MutableQuotaCache>,
    pub urgent_id: Uuid,
    pub steady_id: Uuid,
}

impl PreviewFixture {
    pub fn new(
        urgent: Vec<SubscriptionQuotaCandidateSnapshot>,
        steady: Vec<SubscriptionQuotaCandidateSnapshot>,
    ) -> Self {
        let urgent_id = Uuid::from_u128(1);
        let steady_id = Uuid::from_u128(2);
        let cache = Arc::new(MutableQuotaCache::new(HashMap::from([
            (urgent_id, urgent),
            (steady_id, steady),
        ])));
        let lifecycle = lifecycle(Arc::clone(&cache), [urgent_id, steady_id]);
        Self {
            lifecycle,
            cache,
            urgent_id,
            steady_id,
        }
    }

    pub fn preview(&self, request_id: &str) -> PreviewRouteOutcome {
        self.lifecycle
            .preview_route(PreviewRouteInput {
                principal_id: PRINCIPAL.to_owned(),
                request_id: Some(request_id.to_owned()),
                headers: HeaderMap::new(),
                body_bytes: Bytes::from_static(br#"{"model":"claude-test","messages":[]}"#),
            })
            .expect("preview succeeds")
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

fn lifecycle(cache: Arc<MutableQuotaCache>, ids: [Uuid; 2]) -> Lifecycle {
    let state = TestState::default();
    let principal_view = principal_view();
    let authn = TestAuthn::with_principal_view(state.clone(), Arc::clone(&principal_view));
    let dispatcher = Arc::new(MockDispatch {
        state,
        mode: DispatchMode::Statuses(Arc::new(Mutex::new(vec![http::StatusCode::OK].into()))),
    });
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
