use bytes::Bytes;
use cc_lb_domain::{
    CachePricingSummary, CacheScore, Principal, PrincipalKind, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, SubscriptionTier, UpstreamCandidate, UpstreamKind,
};
use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
use cc_lb_pricing::{
    CatalogSnapshot, CatalogStatus, PriceCatalog, Pricing, UsdPerMillion, global_catalog,
    init_global_catalog,
};
use cc_lb_routing::{FilterPlugin, RoutingContext};
use http::Method;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

const MODEL_AGNOSTIC: &str = "claude-3-5-haiku-default";
const T0_SECS: u64 = 1_700_000_000;
const WINDOW_FIVE_HOUR: &str = "5h";
const WINDOW_SEVEN_DAY: &str = "7d";
const WINDOW_OVERAGE: &str = "overage";

static PRICE_CATALOG_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn example_warning_transition_preserves_base_selection() {
    // Given: a deterministic warning-positive quota fixture and the same
    // candidate before the provider changes its weekly status.
    let warning = example_warning_snapshot();
    let mut allowed = warning.clone();
    allowed.subscription_quotas[1].status = Some("allowed".to_owned());
    allowed.subscription_quotas[1].surpassed_threshold = None;
    let mut clean_peer = clean_known_base("clean-known-base", 2);
    clean_peer
        .subscription_quotas
        .iter_mut()
        .find(|snapshot| snapshot.window == WINDOW_FIVE_HOUR)
        .expect("fixture has 5h quota")
        .resets_at_unix_secs = Some(T0_SECS + 9_000);

    // When: both states compete with the same KnownBase peer.
    let allowed_output = filter(&[allowed, clean_peer.clone()]);
    let warning_output = filter(&[warning.clone(), clean_peer]);
    let trace = warning_output
        .subscription_preference
        .expect("trace present");

    // Then: the provider warning remains base-usable and observable without
    // changing the selected upstream.
    assert_eq!(
        warning_output.kept_upstream_ids,
        allowed_output.kept_upstream_ids
    );
    assert_eq!(trace.chosen_tier, SubscriptionTier::KnownBase);
    assert_eq!(
        candidate_tier(&trace, warning.upstream_id),
        SubscriptionTier::KnownBase
    );
    assert_eq!(
        candidate_urgency(&trace, warning.upstream_id).warning_multiplier,
        1.0
    );
}

#[test]
fn surpassed_threshold_transition_preserves_base_selection() {
    // Given: a base window before and after crossing the provider-supplied
    // surpassed_threshold without changing utilization or reset state.
    let allowed = oauth_at_t0(
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
                .reset_at(T0_SECS + 604_800)
                .build(),
        ],
    );
    let mut threshold_crossed = allowed.clone();
    threshold_crossed.subscription_quotas[1].surpassed_threshold = Some(0.75);
    let mut clean_peer = clean_known_base("clean-known-base", 2);
    clean_peer
        .subscription_quotas
        .iter_mut()
        .find(|snapshot| snapshot.window == WINDOW_FIVE_HOUR)
        .expect("fixture has 5h quota")
        .resets_at_unix_secs = Some(T0_SECS + 9_000);

    // When: both states compete with the same KnownBase peer.
    let allowed_output = filter(&[allowed, clean_peer.clone()]);
    let warning_output = filter(&[threshold_crossed.clone(), clean_peer]);
    let trace = warning_output
        .subscription_preference
        .expect("trace present");

    // Then: threshold metadata cannot alter base-tier ranking or eligibility.
    assert_eq!(
        warning_output.kept_upstream_ids,
        allowed_output.kept_upstream_ids
    );
    assert_eq!(
        candidate_tier(&trace, threshold_crossed.upstream_id),
        SubscriptionTier::KnownBase
    );
    assert_eq!(
        candidate_urgency(&trace, threshold_crossed.upstream_id).warning_multiplier,
        1.0
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

    // Then: warning-positive base remains selectable as KnownBase instead of
    // failing open or falling to API key routing.
    let trace = output.subscription_preference.expect("trace present");
    assert_eq!(output.kept_upstream_ids, vec![warning_only.upstream_id]);
    assert_eq!(trace.chosen_tier, SubscriptionTier::KnownBase);
    assert_eq!(
        candidate_tier(&trace, warning_only.upstream_id),
        SubscriptionTier::KnownBase
    );
}

#[test]
fn same_thread_uses_formula_winner_without_cache_loss_gate() {
    let _guard = PRICE_CATALOG_TEST_LOCK
        .lock()
        .expect("price catalog test lock");
    install_test_pricing();

    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let thread_id = "example-session-id";
    let example_peer = with_cache_score(
        example_peer_warning_snapshot(),
        CacheScore {
            predicted_cache_read_tokens: 516_000,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            predicted_expires_at_unix_secs: Some(T0_SECS + 300),
            matched_breakpoint_index: Some(0),
            confidence: 1.0,
            ambiguity_reason: None,
            matched_v3_cache_key: Some("example-v3-cache-key".to_owned()),
            breakpoint_content_block_index: Some(0),
            matched_content_block_index: Some(0),
            lookback_distance: Some(0),
            token_estimate_source: Some("test".to_owned()),
        },
    );
    let example_secondary = with_cache_score(
        example_secondary_clean_snapshot(),
        CacheScore {
            predicted_cache_read_tokens: 0,
            predicted_cache_creation_tokens_5m: 516_000,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            predicted_expires_at_unix_secs: None,
            matched_breakpoint_index: None,
            confidence: 1.0,
            ambiguity_reason: None,
            matched_v3_cache_key: None,
            breakpoint_content_block_index: None,
            matched_content_block_index: None,
            lookback_distance: None,
            token_estimate_source: None,
        },
    );

    let first = filter
        .filter(
            &ctx_with_thread_id("req-1", thread_id),
            &principal,
            std::slice::from_ref(&example_peer),
        )
        .expect("filter succeeds");
    assert_eq!(first.kept_upstream_ids, vec![example_peer.upstream_id]);

    let risky_peer = with_cache_score(
        example_peer_high_risk_warning_snapshot(),
        warm_cache_score(516_000),
    );

    let second = filter
        .filter(
            &ctx_with_thread_id("req-2", thread_id),
            &principal,
            &[risky_peer.clone(), example_secondary.clone()],
        )
        .expect("filter succeeds");
    let trace = second.subscription_preference.expect("trace present");

    assert_eq!(
        second.kept_upstream_ids,
        vec![trace.formula_winner_upstream_id.unwrap()]
    );
    assert_eq!(trace.kept_upstream_id, trace.formula_winner_upstream_id);
    assert_eq!(trace.incumbent_upstream_id, None);
    assert_eq!(trace.estimated_switch_cache_loss_micros, None);
    assert_eq!(trace.cache_loss_status, None);
    assert_eq!(trace.switch_gate_reason.as_deref(), Some("formula_winner"));
}

#[test]
fn hard_rejected_incumbent_switches_despite_high_reprime_cost() {
    let _guard = PRICE_CATALOG_TEST_LOCK
        .lock()
        .expect("price catalog test lock");
    install_test_pricing();

    let filter = SubscriptionPreferenceFilter::new();
    let principal = principal();
    let thread_id = "thread-hard-reject-bypass";
    let warm_owner = with_cache_score(clean_known_base("warm-owner", 1), warm_cache_score(516_000));
    let first = filter
        .filter(
            &ctx_with_thread_id("req-1", thread_id),
            &principal,
            std::slice::from_ref(&warm_owner),
        )
        .expect("filter succeeds");
    assert_eq!(first.kept_upstream_ids, vec![warm_owner.upstream_id]);

    let rejected_owner = with_cache_score(
        oauth_at_t0(
            "warm-owner",
            1,
            vec![
                fresh(WINDOW_FIVE_HOUR)
                    .status("rejected")
                    .util(1.0)
                    .reset_at(T0_SECS + 3_600)
                    .build(),
                fresh(WINDOW_SEVEN_DAY)
                    .status("allowed")
                    .util(0.20)
                    .reset_at(T0_SECS + 604_800)
                    .build(),
            ],
        ),
        warm_cache_score(516_000),
    );
    let cold_peer = with_cache_score(example_secondary_clean_snapshot(), cold_reprime_score(516_000));

    let second = filter
        .filter(
            &ctx_with_thread_id("req-2", thread_id),
            &principal,
            &[rejected_owner, cold_peer.clone()],
        )
        .expect("filter succeeds");
    let trace = second.subscription_preference.expect("trace present");

    assert_eq!(second.kept_upstream_ids, vec![cold_peer.upstream_id]);
    assert_eq!(trace.kept_upstream_id, Some(cold_peer.upstream_id));
    assert_eq!(trace.switch_gate_reason.as_deref(), Some("formula_winner"));
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
fn same_thread_recomputes_without_successor_convergence() {
    // Given: one stable thread first routes to owner-a, then cache facts change.
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
    let first_trace = first.subscription_preference.expect("first trace present");
    assert_eq!(
        first.kept_upstream_ids,
        vec![first_trace.formula_winner_upstream_id.unwrap()]
    );

    let challenger_a = with_live_cache(clean_known_base("owner-a", 1), 15_000);
    let challenger_b = with_live_cache(clean_known_base("owner-b", 2), 250_000);

    // When: the formula is evaluated after cache facts change.
    let second = filter
        .filter(
            &ctx_with_thread_id("req-2", thread_id),
            &principal,
            &[challenger_a.clone(), challenger_b.clone()],
        )
        .expect("filter succeeds");
    let second_trace = second
        .subscription_preference
        .expect("second trace present");

    // Then: there is no owner latch or successor-pending delay.
    assert_eq!(
        second.kept_upstream_ids,
        vec![second_trace.formula_winner_upstream_id.unwrap()]
    );
    assert_eq!(
        second_trace.switch_gate_reason.as_deref(),
        Some("formula_winner")
    );

    // When: the same facts are evaluated again.
    let third = filter
        .filter(
            &ctx_with_thread_id("req-3", thread_id),
            &principal,
            &[challenger_a, challenger_b.clone()],
        )
        .expect("filter succeeds");
    let third_trace = third.subscription_preference.expect("third trace present");

    // Then: the same formula winner remains the selected upstream.
    assert_eq!(
        third.kept_upstream_ids,
        vec![third_trace.formula_winner_upstream_id.unwrap()]
    );
}

fn filter(candidates: &[UpstreamCandidate]) -> cc_lb_routing::FilterOutput {
    SubscriptionPreferenceFilter::new()
        .filter(&ctx(), &principal(), candidates)
        .expect("builtin filter cannot fail")
}

fn candidate_tier(
    trace: &cc_lb_domain::SubscriptionPreferenceTrace,
    upstream_id: Uuid,
) -> SubscriptionTier {
    candidate_urgency(trace, upstream_id).tier
}

fn candidate_urgency(
    trace: &cc_lb_domain::SubscriptionPreferenceTrace,
    upstream_id: Uuid,
) -> &cc_lb_domain::CandidateUrgency {
    trace
        .candidates
        .iter()
        .find(|candidate| candidate.upstream_id == upstream_id)
        .expect("candidate must be present in trace")
}

fn with_live_cache(candidate: UpstreamCandidate, read_tokens: u32) -> UpstreamCandidate {
    let matched_v3_cache_key = format!("v3-cache-{}", candidate.upstream_id);
    with_cache_score(
        candidate,
        CacheScore {
            predicted_cache_read_tokens: read_tokens,
            predicted_cache_creation_tokens_5m: 0,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 0,
            predicted_expires_at_unix_secs: None,
            matched_breakpoint_index: Some(0),
            confidence: 1.0,
            ambiguity_reason: None,
            matched_v3_cache_key: Some(matched_v3_cache_key),
            breakpoint_content_block_index: Some(0),
            matched_content_block_index: Some(0),
            lookback_distance: Some(0),
            token_estimate_source: Some("test".to_owned()),
        },
    )
}

fn with_cache_score(mut candidate: UpstreamCandidate, score: CacheScore) -> UpstreamCandidate {
    candidate.cache_score = Some(score);
    candidate
}

fn warm_cache_score(read_tokens: u32) -> CacheScore {
    CacheScore {
        predicted_cache_read_tokens: read_tokens,
        predicted_cache_creation_tokens_5m: 0,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: Some(T0_SECS + 300),
        matched_breakpoint_index: Some(0),
        confidence: 1.0,
        ambiguity_reason: None,
        matched_v3_cache_key: Some(format!("warm-v3-cache-{read_tokens}")),
        breakpoint_content_block_index: Some(0),
        matched_content_block_index: Some(0),
        lookback_distance: Some(0),
        token_estimate_source: Some("test".to_owned()),
    }
}

fn cold_reprime_score(tokens: u32) -> CacheScore {
    CacheScore {
        predicted_cache_read_tokens: 0,
        predicted_cache_creation_tokens_5m: tokens,
        predicted_cache_creation_tokens_1h: 0,
        predicted_uncached_input_tokens: 0,
        predicted_expires_at_unix_secs: None,
        matched_breakpoint_index: None,
        confidence: 1.0,
        ambiguity_reason: None,
        matched_v3_cache_key: None,
        breakpoint_content_block_index: None,
        matched_content_block_index: None,
        lookback_distance: None,
        token_estimate_source: None,
    }
}

fn install_test_pricing() {
    let model = MODEL_AGNOSTIC;
    let pricing = Pricing {
        model: model.to_owned(),
        input_per_million_usd: UsdPerMillion::from_whole_usd(5),
        output_per_million_usd: UsdPerMillion::from_whole_usd(25),
        by_tier: Default::default(),
    };
    let mut models = HashMap::new();
    models.insert(model.to_owned(), pricing);
    let mut cache_creation_per_million_usd = HashMap::new();
    cache_creation_per_million_usd
        .insert(model.to_owned(), UsdPerMillion::from_micros_usd(6_250_000));
    let mut cache_read_per_million_usd = HashMap::new();
    cache_read_per_million_usd.insert(model.to_owned(), UsdPerMillion::from_micros_usd(500_000));
    let snapshot = CatalogSnapshot {
        payload_hash: "test-fixture-hash".to_owned(),
        fetched_at_ms: 1,
        models,
        raw_json: b"{}".to_vec(),
        cache_creation_per_million_usd,
        cache_read_per_million_usd,
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    };
    let catalog = PriceCatalog::new_empty();
    catalog.install_snapshot(snapshot.clone());
    if init_global_catalog(catalog).is_err() {
        global_catalog().install_snapshot(snapshot);
    }
}

fn recorded_example_peer_warning_snapshot() -> UpstreamCandidate {
    oauth_at_t0(
        "example-peer",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed")
                .util(0.10)
                .reset_at(T0_SECS + 3_600)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed_warning")
                .util(0.90)
                .surpassed_threshold(0.75)
                .reset_at(T0_SECS + 604_800)
                .build(),
            fresh(WINDOW_OVERAGE).status("rejected").util(1.0).build(),
        ],
    )
}

fn example_peer_warning_snapshot() -> UpstreamCandidate {
    oauth_at_t0(
        "example-peer",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed_warning")
                .util(0.75)
                .reset_at(T0_SECS + 2_000)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(0.60)
                .reset_at(T0_SECS + 260_000)
                .build(),
        ],
    )
}

fn example_peer_high_risk_warning_snapshot() -> UpstreamCandidate {
    oauth_at_t0(
        "example-peer",
        1,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed_warning")
                .util(0.999)
                .reset_at(T0_SECS + 15_000)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(0.98)
                .reset_at(T0_SECS + 604_800)
                .build(),
        ],
    )
}

fn example_secondary_clean_snapshot() -> UpstreamCandidate {
    oauth_at_t0(
        "example-secondary",
        2,
        vec![
            fresh(WINDOW_FIVE_HOUR)
                .status("allowed")
                .util(0.50)
                .reset_at(T0_SECS + 15_000)
                .build(),
            fresh(WINDOW_SEVEN_DAY)
                .status("allowed")
                .util(0.25)
                .reset_at(T0_SECS + 450_000)
                .build(),
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

fn ctx() -> RoutingContext {
    RoutingContext {
        request_id: "req-regression".to_owned(),
        thread_id: None,
        requested_service_tier: None,
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        canonical_model_id: MODEL_AGNOSTIC.to_owned(),
        cache_pricing: test_cache_pricing(),
    }
}

fn ctx_with_thread_id(request_id: &str, thread_id: &str) -> RoutingContext {
    RoutingContext {
        request_id: request_id.to_owned(),
        thread_id: Some(thread_id.to_owned()),
        requested_service_tier: None,
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        canonical_model_id: MODEL_AGNOSTIC.to_owned(),
        cache_pricing: test_cache_pricing(),
    }
}

fn test_cache_pricing() -> CachePricingSummary {
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
