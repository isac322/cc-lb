use bytes::Bytes;
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_plugin_api::types::CacheScore;
use cc_lb_plugin_api::{
    FilterPlugin, Principal, PrincipalKind, RequestContext, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, SubscriptionTier, UpstreamCandidate, UpstreamKind,
};
use http::Method;
use uuid::Uuid;

const MODEL_AGNOSTIC: &str = "claude-3-5-haiku-default";
const T0_SECS: u64 = 1_700_000_000;
const WINDOW_FIVE_HOUR: &str = "5h";
const WINDOW_SEVEN_DAY: &str = "7d";
const WINDOW_OVERAGE: &str = "overage";

#[test]
fn recorded_isac_personal_warning_snapshot_loses_to_clean_known_base_peer() {
    // Given: aggregate-only production evidence shape from thread
    // thread-prod-warning-redacted: 411 events, 310 requests, 4 upstreams,
    // with isac-personal reporting 7d allowed_warning at util 0.91/0.92 over
    // surpassed_threshold 0.75 while overage was rejected.
    let isac_personal = recorded_isac_personal_warning_snapshot();
    let clean_peer = clean_known_base("clean-known-base", 2);

    // When: subscription-preference assesses both candidates.
    let output = filter(&[isac_personal.clone(), clean_peer.clone()]);

    // Then: warning-demoted isac-personal is PartialBase and loses to the clean
    // KnownBase peer without being hard-blocked.
    let trace = output.subscription_preference.expect("trace present");
    assert_eq!(output.kept_upstream_ids, vec![clean_peer.upstream_id]);
    assert_eq!(trace.chosen_tier, SubscriptionTier::KnownBase);
    assert_eq!(
        candidate_tier(&trace, isac_personal.upstream_id),
        SubscriptionTier::PartialBase
    );
    assert_eq!(
        candidate_tier(&trace, clean_peer.upstream_id),
        SubscriptionTier::KnownBase
    );
}

#[test]
fn allowed_utilization_at_surpassed_threshold_demotes_to_partial_base() {
    // Given: a base window still says allowed but its utilization has crossed
    // the provider-supplied surpassed_threshold.
    let threshold_crossed = oauth_at_t0(
        "threshold-crossed",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed")
                .util(0.12)
                .reset_at(T0_SECS + 3_600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(0.91)
                .surpassed_threshold(0.75)
                .reset_at(T0_SECS + 604_800)
                .build(),
        ],
    );
    let clean_peer = clean_known_base("clean-known-base", 2);

    // When: subscription-preference assesses both candidates.
    let output = filter(&[threshold_crossed.clone(), clean_peer.clone()]);

    // Then: the threshold-crossed candidate remains positive but is demoted
    // below the clean KnownBase peer.
    let trace = output.subscription_preference.expect("trace present");
    assert_eq!(output.kept_upstream_ids, vec![clean_peer.upstream_id]);
    assert_eq!(
        candidate_tier(&trace, threshold_crossed.upstream_id),
        SubscriptionTier::PartialBase
    );
}

#[test]
fn warning_only_candidate_remains_selectable_without_clean_peer() {
    // Given: the only OAuth candidate has warning-positive base windows.
    let warning_only = oauth_at_t0(
        "warning-only",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed_warning")
                .util(0.82)
                .reset_at(T0_SECS + 3_600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(0.92)
                .surpassed_threshold(0.75)
                .reset_at(T0_SECS + 604_800)
                .build(),
        ],
    );

    // When: subscription-preference has no clean peer available.
    let output = filter(std::slice::from_ref(&warning_only));

    // Then: warning-positive base remains selectable as PartialBase instead of
    // failing open or falling to API key routing.
    let trace = output.subscription_preference.expect("trace present");
    assert_eq!(output.kept_upstream_ids, vec![warning_only.upstream_id]);
    assert_eq!(trace.chosen_tier, SubscriptionTier::PartialBase);
    assert_eq!(
        candidate_tier(&trace, warning_only.upstream_id),
        SubscriptionTier::PartialBase
    );
}

#[test]
fn near_full_allowed_quota_remains_smooth_positive_weight() {
    // Given: a cache-warm candidate with high but still allowed 7d utilization.
    let cache_warm_near_full_7d = with_live_cache(
        oauth_at_t0(
            "cache-warm-near-full-7d",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .status("allowed")
                    .util(0.10)
                    .reset_at(T0_SECS + 3_600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .status("allowed")
                    .util(0.995)
                    .reset_at(T0_SECS + 604_800)
                    .build(),
            ],
        ),
        250_000,
    );
    let clean_peer = with_live_cache(clean_known_base("clean-known-base", 2), 15_000);

    // When: both candidates are assessed in KnownBase.
    let output = filter(&[cache_warm_near_full_7d.clone(), clean_peer]);

    // Then: local policy does not hard-zero an allowed candidate below 100%.
    let trace = output.subscription_preference.expect("trace present");
    let near_full = candidate_urgency(&trace, cache_warm_near_full_7d.upstream_id);
    assert_eq!(near_full.tier, SubscriptionTier::KnownBase);
    assert!(
        near_full.quota_urgency > 0.0,
        "allowed high-utilization quota must remain a smooth positive weight"
    );
}

#[test]
fn same_thread_retains_owner_until_successor_converges() {
    // Given: one stable thread first warms owner-a, then owner-b becomes the
    // formula winner for two consecutive turns.
    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let thread_id = "thread-owner-stability";
    let owner_a = with_live_cache(clean_known_base("owner-a", 1), 250_000);
    let owner_b = with_live_cache(clean_known_base("owner-b", 2), 15_000);
    let first = filter
        .filter(
            &ctx_with_thread_id("req-1", thread_id),
            &principal,
            &[owner_a.clone(), owner_b.clone()],
        )
        .expect("filter succeeds");
    assert_eq!(first.kept_upstream_ids, vec![owner_a.upstream_id]);

    let challenger_a = with_live_cache(clean_known_base("owner-a", 1), 15_000);
    let challenger_b = with_live_cache(clean_known_base("owner-b", 2), 250_000);

    // When: the challenger wins formula scoring once.
    let second = filter
        .filter(
            &ctx_with_thread_id("req-2", thread_id),
            &principal,
            &[challenger_a.clone(), challenger_b.clone()],
        )
        .expect("filter succeeds");

    // Then: the incumbent owner is retained and no second cache is seeded yet.
    assert_eq!(second.kept_upstream_ids, vec![owner_a.upstream_id]);

    // When: the same challenger wins formula scoring again.
    let third = filter
        .filter(
            &ctx_with_thread_id("req-3", thread_id),
            &principal,
            &[challenger_a, challenger_b.clone()],
        )
        .expect("filter succeeds");

    // Then: the thread converges to that single deterministic successor.
    assert_eq!(third.kept_upstream_ids, vec![challenger_b.upstream_id]);
}

fn filter(candidates: &[UpstreamCandidate]) -> cc_lb_plugin_api::FilterOutput {
    SubscriptionPreferenceFilter::new()
        .filter(&ctx(), &principal(), candidates)
        .expect("builtin filter cannot fail")
}

fn candidate_tier(
    trace: &cc_lb_plugin_api::SubscriptionPreferenceTrace,
    upstream_id: Uuid,
) -> SubscriptionTier {
    candidate_urgency(trace, upstream_id).tier
}

fn candidate_urgency(
    trace: &cc_lb_plugin_api::SubscriptionPreferenceTrace,
    upstream_id: Uuid,
) -> &cc_lb_plugin_api::CandidateUrgency {
    trace
        .candidates
        .iter()
        .find(|candidate| candidate.upstream_id == upstream_id)
        .expect("candidate must be present in trace")
}

fn with_live_cache(mut candidate: UpstreamCandidate, read_tokens: u32) -> UpstreamCandidate {
    candidate.cache_score = Some(CacheScore {
        predicted_cache_read_tokens: read_tokens,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: None,
        matched_breakpoint_index: Some(0),
        confidence: 1.0,
        ambiguity_reason: None,
    });
    candidate
}

fn recorded_isac_personal_warning_snapshot() -> UpstreamCandidate {
    oauth_at_t0(
        "isac-personal",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed")
                .util(0.13)
                .reset_at(T0_SECS + 3_600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed_warning")
                .util(0.91)
                .surpassed_threshold(0.75)
                .reset_at(T0_SECS + 604_800)
                .build(),
            fresh(WINDOW_OVERAGE).status("rejected").util(1.0).build(),
        ],
    )
}

fn clean_known_base(name: &str, id_seed: u8) -> UpstreamCandidate {
    oauth_at_t0(
        name,
        id_seed,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed")
                .util(0.10)
                .reset_at(T0_SECS + 3_600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(0.20)
                .reset_at(T0_SECS + 604_800)
                .build(),
        ],
    )
}

fn ctx() -> RequestContext {
    RequestContext {
        request_id: "req-regression".to_owned(),
        thread_id: None,
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: MODEL_AGNOSTIC.to_owned(),
    }
}

fn ctx_with_thread_id(request_id: &str, thread_id: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.to_owned(),
        thread_id: Some(thread_id.to_owned()),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: MODEL_AGNOSTIC.to_owned(),
    }
}

fn principal() -> Principal {
    Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::Map::new(),
    }
}

fn oauth_at_t0(
    name: &str,
    id_seed: u8,
    quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: upstream_id(id_seed),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: Vec::new(),
        subscription_quotas: quotas,
        observed_at_unix_secs: T0_SECS,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    }
}

fn upstream_id(seed: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = seed;
    Uuid::from_bytes(bytes)
}

#[derive(Clone, Debug)]
struct SnapBuilder {
    inner: SubscriptionQuotaCandidateSnapshot,
}

impl SnapBuilder {
    fn status(mut self, value: &str) -> Self {
        self.inner.status = Some(value.to_owned());
        self
    }

    fn util(mut self, value: f64) -> Self {
        self.inner.utilization = Some(value);
        self
    }

    fn surpassed_threshold(mut self, value: f64) -> Self {
        self.inner.surpassed_threshold = Some(value);
        self
    }

    fn reset_at(mut self, value: u64) -> Self {
        self.inner.resets_at_unix_secs = Some(value);
        self
    }

    fn build(self) -> SubscriptionQuotaCandidateSnapshot {
        self.inner
    }
}

fn fresh(window: &str) -> SnapBuilder {
    SnapBuilder {
        inner: SubscriptionQuotaCandidateSnapshot {
            window: window.to_owned(),
            state: SubscriptionQuotaDataState::Fresh,
            source: Some("recorded-aggregate-fixture".to_owned()),
            utilization: None,
            status: None,
            resets_at_unix_secs: None,
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            extra_usage_enabled: None,
            extra_usage_monthly_limit: None,
            extra_usage_used_credits: None,
            observed_at_unix_millis: Some(T0_SECS * 1_000),
            max_staleness_secs: 60,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
        },
    }
}
