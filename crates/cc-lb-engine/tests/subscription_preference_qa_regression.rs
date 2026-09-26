use bytes::Bytes;
use cc_lb_domain::{
    CachePricingSummary, CacheScore, CandidateUrgency, Principal, PrincipalKind,
    SubscriptionPreferenceTrace, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState,
    SubscriptionTier, UpstreamCandidate, UpstreamKind,
};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_routing::{FilterPlugin, RoutingContext};
use http::{HeaderMap, Method};
use uuid::Uuid;

const MODEL: &str = "claude-opus-4-8";
const T0_SECS: u64 = 1_700_000_000;
const WINDOW_FIVE_HOUR: &str = "5h";
const WINDOW_SEVEN_DAY: &str = "7d";
const SHARED_V3_KEY: &str = "shared-v3-cache-key";

#[test]
fn cache_positive_hash_affinity_keeps_single_winner_across_request_ids() {
    let owner = with_cache_score(
        oauth_candidate("warm-owner", 1, base_quota(0.50, 18_000)),
        cache_score(SHARED_V3_KEY, 200_000, 0),
    );
    let peer = with_cache_score(
        oauth_candidate("creation-peer", 2, base_quota(0.10, 3_600)),
        cache_score(SHARED_V3_KEY, 0, 200_000),
    );

    let first = route("req-cache-a", &[owner.clone(), peer.clone()]);
    let second = route("req-cache-b", &[owner.clone(), peer]);

    for result in [first, second] {
        assert_eq!(result.kept_upstream_id, owner.upstream_id);
        assert_eq!(
            result.trace.bucket_v3_cache_key.as_deref(),
            Some(SHARED_V3_KEY)
        );
    }
}

#[test]
fn non_positive_cache_value_uses_request_id_despite_matched_key() {
    let first = with_cache_score(
        oauth_candidate("creation-a", 1, base_quota(0.40, 9_000)),
        cache_score(SHARED_V3_KEY, 0, 100_000),
    );
    let second = with_cache_score(
        oauth_candidate("creation-b", 2, base_quota(0.40, 9_000)),
        cache_score(SHARED_V3_KEY, 0, 100_000),
    );

    let result = route("req-creation-only", &[first, second]);

    assert_eq!(result.trace.bucket_v3_cache_key, None);
}

#[test]
fn reset_recent_has_lower_urgency_when_only_reset_time_differs() {
    let fresh_reset = oauth_candidate("fresh-reset", 1, base_quota(0.10, 18_000));
    let soon_reset = oauth_candidate("soon-reset", 2, base_quota(0.10, 1_800));

    let result = route("req-reset-only", &[fresh_reset.clone(), soon_reset.clone()]);
    let fresh = trace_candidate(&result.trace, fresh_reset.upstream_id);
    let soon = trace_candidate(&result.trace, soon_reset.upstream_id);

    assert!(
        fresh.quota_urgency < soon.quota_urgency,
        "a just-reset 5h window must not gain priority from reset time alone"
    );
}

#[test]
fn v11_trace_contains_distinct_winner_and_loser_pressure() {
    let first_base = oauth_candidate("first-base", 1, base_quota(0.25, 9_000));
    let second_base = oauth_candidate("second-base", 2, base_quota(0.75, 9_000));
    let partial_base = partial_base_candidate("partial-base", 3, base_quota(0.25, 9_000));
    let overage = overage_candidate("overage", 4, 0.40);
    let unknown = unknown_candidate("unknown", 5);

    let result = route(
        "req-v11-all-candidate-trace",
        &[
            first_base.clone(),
            second_base.clone(),
            partial_base.clone(),
            overage.clone(),
            unknown.clone(),
        ],
    );
    let winner_id = result
        .trace
        .formula_winner_upstream_id
        .expect("formula winner is traced");
    let loser_id = if winner_id == first_base.upstream_id {
        second_base.upstream_id
    } else {
        first_base.upstream_id
    };
    let winner = trace_candidate(&result.trace, winner_id);
    let loser = trace_candidate(&result.trace, loser_id);
    let partial = trace_candidate(&result.trace, partial_base.upstream_id);
    let overage = trace_candidate(&result.trace, overage.upstream_id);
    let unknown = trace_candidate(&result.trace, unknown.upstream_id);

    assert_eq!(
        result.trace.formula_version.as_deref(),
        Some("cost-first-v2")
    );
    assert_ne!(winner.quota_urgency_combined, loser.quota_urgency_combined);
    for base in [winner, loser, partial] {
        assert!(base.quota_urgency_5h.is_some());
        assert!(base.quota_urgency_7d.is_some());
        assert_eq!(base.quota_urgency_combined, Some(base.quota_urgency));
    }
    assert_eq!(partial.tier, SubscriptionTier::PartialBase);
    assert_eq!(partial.quota_urgency_7d, Some(0.0));
    assert_eq!(overage.quota_urgency_5h, None);
    assert_eq!(overage.quota_urgency_7d, None);
    assert_eq!(overage.quota_urgency_combined, None);
    assert!(overage.quota_urgency > 0.0);
    assert_eq!(unknown.quota_urgency_5h, None);
    assert_eq!(unknown.quota_urgency_7d, None);
    assert_eq!(unknown.quota_urgency_combined, None);
    assert_eq!(unknown.quota_urgency, 0.0);
}

struct RouteResult {
    kept_upstream_id: Uuid,
    trace: SubscriptionPreferenceTrace,
}

#[derive(Clone, Copy)]
struct BaseQuota {
    utilization: f64,
    five_hour_reset_offset_secs: u64,
}

fn base_quota(utilization: f64, five_hour_reset_offset_secs: u64) -> BaseQuota {
    BaseQuota {
        utilization,
        five_hour_reset_offset_secs,
    }
}

fn route(request_id: &str, candidates: &[UpstreamCandidate]) -> RouteResult {
    let output = SubscriptionPreferenceFilter::new()
        .filter(&ctx(request_id), &principal(), candidates)
        .expect("subscription preference filter succeeds");
    let trace = output
        .subscription_preference
        .expect("subscription preference trace present");
    RouteResult {
        kept_upstream_id: output.kept_upstream_ids[0],
        trace,
    }
}

fn trace_candidate(trace: &SubscriptionPreferenceTrace, upstream_id: Uuid) -> &CandidateUrgency {
    trace
        .candidates
        .iter()
        .find(|candidate| candidate.upstream_id == upstream_id)
        .expect("candidate appears in trace")
}

fn oauth_candidate(name: &str, seed: u8, quota: BaseQuota) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: upstream_id(seed),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicOauth,
        observed_rate_limits: Vec::new(),
        subscription_quotas: quota_snapshots(quota),
        observed_at_unix_secs: T0_SECS,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    }
}

fn partial_base_candidate(name: &str, seed: u8, quota: BaseQuota) -> UpstreamCandidate {
    let mut candidate = oauth_candidate(name, seed, quota);
    let seven_day = candidate
        .subscription_quotas
        .iter_mut()
        .find(|snapshot| snapshot.window == WINDOW_SEVEN_DAY)
        .expect("test fixture contains seven-day quota");
    seven_day.state = SubscriptionQuotaDataState::Stale;
    candidate
}

fn overage_candidate(name: &str, seed: u8, utilization: f64) -> UpstreamCandidate {
    let mut candidate = oauth_candidate(name, seed, base_quota(1.0, 9_000));
    candidate
        .subscription_quotas
        .push(quota_snapshot("overage", utilization, T0_SECS + 2_592_000));
    candidate
}

fn unknown_candidate(name: &str, seed: u8) -> UpstreamCandidate {
    let mut candidate = oauth_candidate(name, seed, base_quota(0.0, 9_000));
    candidate.subscription_quotas.clear();
    candidate
}

fn quota_snapshots(quota: BaseQuota) -> Vec<SubscriptionQuotaCandidateSnapshot> {
    vec![
        quota_snapshot(
            WINDOW_FIVE_HOUR,
            quota.utilization,
            T0_SECS + quota.five_hour_reset_offset_secs,
        ),
        // An untouched weekly quota a full week before reset stays behind
        // linear pace, so the weekly pace gate keeps the 5h pressure under test.
        quota_snapshot(WINDOW_SEVEN_DAY, 0.0, T0_SECS + 604_800),
    ]
}

fn quota_snapshot(
    window: &str,
    utilization: f64,
    resets_at_unix_secs: u64,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
        state: SubscriptionQuotaDataState::Fresh,
        source: Some("qa-regression".to_owned()),
        utilization: Some(utilization),
        status: Some("allowed".to_owned()),
        resets_at_unix_secs: Some(resets_at_unix_secs),
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
    }
}

fn with_cache_score(mut candidate: UpstreamCandidate, score: CacheScore) -> UpstreamCandidate {
    candidate.cache_score = Some(score);
    candidate
}

fn cache_score(key: &str, read_tokens: u32, create_5m_tokens: u32) -> CacheScore {
    CacheScore {
        predicted_cache_read_tokens: read_tokens,
        predicted_cache_creation_tokens_5m: create_5m_tokens,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: None,
        matched_breakpoint_index: Some(0),
        confidence: 1.0,
        ambiguity_reason: None,
        matched_v3_cache_key: Some(key.to_owned()),
        breakpoint_content_block_index: Some(0),
        matched_content_block_index: Some(0),
        lookback_distance: Some(0),
        token_estimate_source: Some("qa-regression".to_owned()),
    }
}

fn ctx(request_id: &str) -> RoutingContext {
    RoutingContext {
        request_id: request_id.to_owned(),
        thread_id: Some("qa-regression-thread".to_owned()),
        requested_service_tier: None,
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        canonical_model_id: MODEL.to_owned(),
        cache_pricing: cache_pricing(),
    }
}

fn cache_pricing() -> CachePricingSummary {
    CachePricingSummary {
        status: "known".to_owned(),
        input_micros_per_million: Some(5_000_000),
        cache_creation_5m_micros_per_million: Some(6_250_000),
        cache_creation_1h_micros_per_million: Some(10_000_000),
        cache_read_micros_per_million: Some(500_000),
    }
}

fn principal() -> Principal {
    Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::Map::new(),
    }
}

fn upstream_id(seed: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = seed;
    Uuid::from_bytes(bytes)
}
