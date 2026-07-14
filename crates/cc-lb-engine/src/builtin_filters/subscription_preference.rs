//! Subscription-preference router filter.
//!
//! Selects at most one OAuth upstream per request via:
//!
//! 1. A strict tier ordering (KnownBase > PartialBase > Overage > UnknownProbe)
//!    that keeps the Anthropic base plan strictly higher priority than the
//!    overage bucket.
//! 2. Within the winning tier, a Weighted Rendezvous Hash (WRH, Vilkonis) using
//!    a tier-specific quota factor multiplied by cache and warning factors.
//!    Base pressure is the weighted smoothmax of 5h and effective weekly
//!    use-it-or-lose-it pressure. For exact Fable, effective weekly pressure is
//!    the smoothmax of shared `7d` and `7d_fable`. Overage urgency retains its
//!    fixed nominal 30-day denominator.
//! 3. A deterministic tiebreak if two candidates produce numerically identical
//!    WRH scores (rendezvous_hash DESC, upstream_id ASC).
//!
//! ## Design rationale
//!
//! ADR 0008 base pressure compares utilization with the target burn pace for
//! the time remaining before reset. Positive pressure raises the base quota
//! factor above its neutral `1.0`; on-pace buckets use `1.0` uniformly. The
//! candidate `plan_capacity_ratio` remains available as metadata for analytics
//! but does not affect pressure, tiering, WRH weight, or distribution.
//!
//! ## Windows
//!
//! Base: shared `5h` and `7d` windows for every model, plus `7d_fable` only for
//! the canonical model ID `claude-fable-5`. Older model-specific
//! `7d_sonnet` and `7d_opus` labels remain deliberately ignored because they
//! are not stable enough to drive routing. `unified` is not itself an
//! exhaustion window; its top-level flags (`overage_in_use`,
//! `fallback_available`, `extra_usage_*`) enrich the overage assessment.
//!
//! ## Reset semantics
//!
//! - Fresh + `resets_at` in the future: window contributes to urgency.
//! - Fresh without `resets_at`: window excluded from urgency (Q4).
//! - Stale + `rejected` + future reset: hard negative (rejection still live).
//! - Stale + `rejected` + past reset: unknown (rejection expired).

use cc_lb_domain::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, CachePricingSummary,
    Principal, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState, UpstreamCandidate,
    UpstreamKind, WrhKeySource,
};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, RoutingContext};
use std::cmp::Ordering;
use uuid::Uuid;

// -- Reason strings surfaced on FilterOutput.reason (log/audit surface). -----

pub(crate) const SUBSCRIPTION_ALIVE_REASON: &str = "keep:best_subscription_candidate";
pub(crate) const API_KEY_FALLBACK_REASON: &str = "keep:api_key_subscription_exhausted";
pub(crate) const NO_API_KEY_REASON: &str = "keep:subscription_exhausted_no_api_key";
pub(crate) const NO_SUBSCRIPTION_REASON: &str = "keep:no_subscription_candidates";

// -- Window labels. ----------------------------------------------------------

pub(crate) const WINDOW_FIVE_HOUR: &str = "5h";
pub(crate) const WINDOW_SEVEN_DAY: &str = "7d";
pub(crate) const WINDOW_SEVEN_DAY_FABLE: &str = "7d_fable";
pub(crate) const WINDOW_OVERAGE: &str = "overage";
pub(crate) const WINDOW_UNIFIED: &str = "unified";
pub(crate) const FABLE_MODEL: &str = "claude-fable-5";

// -- Algorithm constants. ---------------------------------------------------

/// Exponent retained by the v10 overage urgency formula.
pub(crate) const HEADROOM_EXPONENT: i32 = 2;

const FIVE_HOUR_WINDOW_LEN_SECS: u64 = 18_000;
const SEVEN_DAY_WINDOW_LEN_SECS: u64 = 604_800;
const FIVE_HOUR_GAMMA: f64 = 1.0;
const SEVEN_DAY_GAMMA: f64 = 1.3;
const FIVE_HOUR_TARGET_FLOOR: f64 = 0.01;
const SEVEN_DAY_TARGET_FLOOR: f64 = 0.01;
const SMOOTHMAX_P: f64 = 6.0;
const SEVEN_DAY_SMOOTHMAX_WEIGHT: f64 = 1.0;

/// Guard below which the aggregate WRH weight is treated as zero and the
/// filter falls back to a uniform distribution.
pub(crate) const EPSILON: f64 = 1e-12;

/// Nominal remaining-seconds denominator for overage-tier urgency. Anthropic
/// overage windows do not carry a reliable `resets_at`; billing rolls over on
/// the monthly boundary, so we use a fixed 30-day nominal.
pub(crate) const OVERAGE_REMAINING_NOMINAL_SECS: u64 = 30 * 86_400;

/// Baseline WRH weight for overage candidates whose utilization we cannot
/// read.
pub(crate) const OVERAGE_UNKNOWN_WEIGHT: f64 = 0.5;

/// Short version tag identifying the WRH salt/algorithm shape. Embedded in
/// [`RENDEZVOUS_SALT`] and surfaced on `SubscriptionPreferenceTrace.rendezvous_salt_version`
/// so trace queries can distinguish upstream-mix shifts caused by an
/// algorithm/salt change from those caused by upstream or quota state
/// changes. Bumped in lockstep with `RENDEZVOUS_SALT` — a `debug_assert`
/// in `tests::rendezvous_salt_embeds_version` guards the invariant.
pub const SALT_VERSION: &str = "v11";
pub(crate) const FABLE_SALT_VERSION: &str = "v11-fable";

/// v11 salt for base and unknown-probe weighted-rendezvous selection. The
/// overage tier deliberately retains its v10 salt so ADR 0008 does not move
/// overage winners; traces still report the current v11 semantic era.
///
/// v11-fable (2026-07-11): exact `claude-fable-5` uses scoped weekly pressure,
/// salt, and trace version without remapping non-Fable or overage traffic.
///
/// v11 (2026-07-10): exposes use-it-or-lose-it base pressure components and the
/// tier-specific quota factor in all-candidate traces.
///
/// v10 (2026-07-07): removes hidden threaded owner memory and cache-loss switch
/// gates. Cache influence is folded into each candidate's WRH weight as
/// `read_savings - missing_incremental_create_cost`, normalized within the
/// selected tier. Thread IDs are used as the WRH key only when the current tier
/// has a positive priced cache value; all-cold routing falls back to request ID.
///
/// v9 (2026-07-06): warning-positive base windows remain KnownBase with a
/// same-tier multiplier, and threaded owner handoff is additionally gated by
/// estimated in-memory cache re-prime cost derived from plugin-visible pricing
/// and cache-score inputs.
///
/// v8 (2026-07-06): WRH is keyed on non-empty `thread_id`, falling back to
/// `request_id` only for stateless requests. Fresh base windows with
/// `allowed_warning`, or with `allowed` plus finite utilization at/above a
/// finite `surpassed_threshold`, remain selectable but demote the candidate
/// from KnownBase to PartialBase. Threaded requests keep a per-thread owner and
/// require a converged challenger before non-forced handoff. See
/// docs/adr/0005-thread-keyed-subscription-preference-with-warning-demotion.md.
///
/// v7 (2026-07-06): WRH is keyed on `request_id` and the per-candidate
/// weight is `quota_urgency * exp(CACHE_LOG_BOOST * cache_ratio)`, where
/// `cache_ratio` is `predicted_cache_read_tokens / max_in_tier_bucket`.
/// The exponential multiplier makes a deeply cached upstream keep winning
/// through ~95% utilisation and hand off probabilistically around 99%
/// (`CACHE_LOG_BOOST` is calibrated for a 99% crossover in the observed
/// example-org/Example Org scenario). This replaces the v6 cache_affinity
/// max-ranker filter, which dropped every non-max cache candidate before
/// subscription-preference could see it — when the max-cache upstream
/// became tier-blocked (util ≥ 1.0), there was no fallback candidate left
/// to route to and the request hit 429. v7 keeps every candidate in the
/// bucket and lets tier assessment plus the cache-weighted WRH handle both
/// warm-session pinning and hard tier-eviction spill. See
/// docs/adr/0004-cache-weighted-subscription-preference.md and
/// docs/rfc/0003-cache-weighted-subscription-preference.md.
///
/// v6 (2026-07-06): WRH keyed on `request_id`, cache handling delegated to
/// a separate cache_affinity max-ranker filter. Rejected once the
/// tier-eviction spill bug was found: dropping non-max candidates before
/// subscription's tier assessment meant a saturated cache-holder had no
/// peer left to spill to.
///
/// v5 (rejected, never deployed): would have made subscription-preference
/// inspect candidate `cache_score` to switch between thread-id and
/// request-id keying. Rejected for violating filter separation.
///
/// v4 (2026-07-05): keyed WRH on `thread_id` so multi-turn sessions pinned
/// while the prompt cache stayed warm. Deployed by PR #322 after the
/// 2026-07-05 06:24 UTC scatter incident.
const RENDEZVOUS_SALT: &str =
    "cc-lb:subscription-preference:v11:use-it-or-lose-it-quota-pressure:2026-07-10";
const FABLE_RENDEZVOUS_SALT: &str =
    "cc-lb:subscription-preference:v11-fable:use-it-or-lose-it-fable-quota-pressure:2026-07-11";
const OVERAGE_RENDEZVOUS_SALT: &str =
    "cc-lb:subscription-preference:v10:symmetric-cache-value:2026-07-07";

pub(crate) const CACHE_COST_BASIS_VERSION: &str = "v1";
const WARNING_MULTIPLIER: f64 = 0.20;

/// Exponent coefficient on the cache-weighted WRH multiplier:
/// `cache_weight_multiplier = exp(CACHE_LOG_BOOST * cache_ratio)`.
///
/// The retained ADR 0004 calibration is `ln(8100) / 0.94`, so cache ratios
/// `1.0` and `0.06` differ by a multiplier of exactly 8100. ADR 0008 composes
/// this independently with the current quota factor and warning multiplier;
/// plan capacity and the deleted v10 base-headroom formula are not inputs.
pub const CACHE_LOG_BOOST: f64 = 9.574_063_128_362_267;

// -- Config knobs (compiled defaults today; expose per-principal later). ----

struct FilterConfig {
    unknown_probe_enabled: bool,
    stale_rejected_without_reset_blocks: bool,
    hard_overage_block_wins: bool,
    rendezvous_hash_salt: &'static str,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            unknown_probe_enabled: true,
            stale_rejected_without_reset_blocks: true,
            hard_overage_block_wins: true,
            rendezvous_hash_salt: RENDEZVOUS_SALT,
        }
    }
}

// -- Public plugin type. ----------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
pub struct SubscriptionPreferenceFilter;

impl SubscriptionPreferenceFilter {
    pub fn new() -> Self {
        Self
    }
}

impl FilterPlugin for SubscriptionPreferenceFilter {
    fn filter(
        &self,
        ctx: &RoutingContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let mut config = FilterConfig::default();
        if ctx.canonical_model_id == FABLE_MODEL {
            config.rendezvous_hash_salt = FABLE_RENDEZVOUS_SALT;
        }
        Ok(evaluate(ctx, candidates, &config))
    }

    fn plugin_id(&self) -> Uuid {
        BUILTIN_SUBSCRIPTION_PREFERENCE_ID
    }

    fn plugin_name(&self) -> &str {
        BUILTIN_SUBSCRIPTION_PREFERENCE_NAME
    }
}

// -- Evaluation entry point. ------------------------------------------------

fn evaluate(
    ctx: &RoutingContext,
    candidates: &[UpstreamCandidate],
    config: &FilterConfig,
) -> FilterOutput {
    let canonical_model = ctx.canonical_model_id.as_str();
    let base_windows = relevant_base_windows(canonical_model);

    let has_subscription = candidates
        .iter()
        .any(|c| c.kind == UpstreamKind::AnthropicOauth);
    let has_api_key = candidates
        .iter()
        .any(|c| c.kind == UpstreamKind::AnthropicApiKey);

    if !has_subscription {
        return FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            reason: NO_SUBSCRIPTION_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        };
    }

    let mut buckets: [Vec<Assessment<'_>>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.kind != UpstreamKind::AnthropicOauth {
            continue;
        }
        if let Some(assessment) = assess_candidate(candidate, index, &base_windows, config) {
            buckets[assessment.tier as usize].push(assessment);
        }
    }

    let all_assessments: Vec<cc_lb_domain::CandidateUrgency> = buckets
        .iter()
        .flat_map(|bucket| {
            let max_cache_value_micros =
                max_positive_cache_value_micros(bucket, &ctx.cache_pricing);
            let total_urgency_in_bucket: f64 = bucket.iter().map(Assessment::tier_urgency).sum();
            bucket.iter().map(move |a| {
                let cache_tokens = candidate_cache_read_tokens(a);
                let cache_value_micros =
                    candidate_cache_value_micros(a.candidate, &ctx.cache_pricing);
                let cache_ratio =
                    cache_value_ratio_within_bucket(cache_value_micros, max_cache_value_micros);
                let cache_weight_multiplier = cache_weight_multiplier(cache_ratio);
                let tier_urgency = a.tier_urgency();
                let quota_weight =
                    quota_weight_factor(a.tier, tier_urgency, total_urgency_in_bucket);
                let quota_uniform_fallback = total_urgency_in_bucket < EPSILON;
                let effective_weight =
                    quota_weight * cache_weight_multiplier * a.warning_multiplier;
                let estimated_input_cost_micros =
                    estimate_candidate_input_cost_micros(a.candidate, &ctx.cache_pricing)
                        .unwrap_or(0);
                let cache_savings_ratio = cache_read_savings_ratio(a.candidate, &ctx.cache_pricing);
                cc_lb_domain::CandidateUrgency {
                    upstream_id: a.candidate.upstream_id,
                    tier: tier_to_plugin_api(a.tier),
                    urgency: effective_weight,
                    quota_urgency: tier_urgency,
                    quota_urgency_5h: matches!(a.tier, Tier::KnownBase | Tier::PartialBase)
                        .then_some(a.quota_urgency_5h),
                    quota_urgency_7d: matches!(a.tier, Tier::KnownBase | Tier::PartialBase)
                        .then_some(a.quota_urgency_7d),
                    quota_urgency_combined: matches!(a.tier, Tier::KnownBase | Tier::PartialBase)
                        .then_some(a.quota_urgency_combined),
                    quota_weight_factor: quota_weight,
                    quota_uniform_fallback,
                    predicted_cache_read_tokens: cache_tokens,
                    predicted_cache_creation_tokens_5m: candidate_cache_creation_tokens_5m(a),
                    predicted_cache_creation_tokens_1h: candidate_cache_creation_tokens_1h(a),
                    predicted_uncached_input_tokens: candidate_uncached_input_tokens(a),
                    cache_ratio,
                    cache_weight_multiplier,
                    warning_multiplier: a.warning_multiplier,
                    cache_savings_ratio,
                    estimated_input_cost_micros,
                    effective_weight,
                    cache_value_micros,
                    matched_v3_cache_key: a
                        .candidate
                        .cache_score
                        .as_ref()
                        .and_then(|score| score.matched_v3_cache_key.clone()),
                    matched_content_block_index: a
                        .candidate
                        .cache_score
                        .as_ref()
                        .and_then(|score| score.matched_content_block_index),
                    breakpoint_content_block_index: a
                        .candidate
                        .cache_score
                        .as_ref()
                        .and_then(|score| score.breakpoint_content_block_index),
                    lookback_distance: a
                        .candidate
                        .cache_score
                        .as_ref()
                        .and_then(|score| score.lookback_distance),
                    token_estimate_source: a
                        .candidate
                        .cache_score
                        .as_ref()
                        .and_then(|score| score.token_estimate_source.clone()),
                }
            })
        })
        .collect();

    for bucket in buckets.iter() {
        if bucket.is_empty() {
            continue;
        }
        let max_cache_value_micros = max_positive_cache_value_micros(bucket, &ctx.cache_pricing);
        let bucket_v3_cache_affinity_key = bucket_v3_cache_affinity_key(bucket, &ctx.cache_pricing);
        let (routing_key, wrh_key_source) =
            routing_key_for_bucket(ctx.request_id.as_str(), bucket_v3_cache_affinity_key);
        let selection = pick_within_tier(
            bucket,
            routing_key,
            config,
            &ctx.cache_pricing,
            max_cache_value_micros,
        );
        let formula_winner = selection.winner;
        let chosen_tier = tier_to_plugin_api(formula_winner.tier);
        let kept_upstream_id = formula_winner.candidate.upstream_id;

        let trace = cc_lb_domain::SubscriptionPreferenceTrace {
            chosen_tier,
            candidates: all_assessments,
            wrh_key_source,
            previous_tier: None,
            rendezvous_salt_version: Some(
                if canonical_model == FABLE_MODEL {
                    FABLE_SALT_VERSION
                } else {
                    SALT_VERSION
                }
                .to_owned(),
            ),
            cache_cost_basis_version: Some(CACHE_COST_BASIS_VERSION.to_owned()),
            formula_winner_upstream_id: Some(formula_winner.candidate.upstream_id),
            kept_upstream_id: Some(kept_upstream_id),
            incumbent_upstream_id: None,
            estimated_switch_cache_loss_micros: None,
            cache_loss_status: None,
            switch_gate_reason: Some("formula_winner".to_owned()),
            bucket_v3_cache_affinity_key: bucket_v3_cache_affinity_key.map(str::to_owned),
            lineage_would_have_predicted_read_tokens: None,
            lineage_would_have_picked_upstream_id: None,
        };
        return FilterOutput {
            kept_upstream_ids: vec![kept_upstream_id],
            reason: SUBSCRIPTION_ALIVE_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: Some(trace),
            cache_affinity: None,
        };
    }

    if has_api_key {
        FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            reason: API_KEY_FALLBACK_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        }
    } else {
        FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicOauth),
            reason: NO_API_KEY_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
            cache_affinity: None,
        }
    }
}

fn relevant_base_windows(canonical_model: &str) -> Vec<&'static str> {
    let mut windows = vec![WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY];
    if canonical_model == FABLE_MODEL {
        windows.push(WINDOW_SEVEN_DAY_FABLE);
    }
    windows
}

fn routing_key_for_bucket<'a>(
    request_id: &'a str,
    bucket_v3_cache_affinity_key: Option<&'a str>,
) -> (&'a str, WrhKeySource) {
    if let Some(cache_key) = bucket_v3_cache_affinity_key {
        (cache_key, WrhKeySource::CacheHash)
    } else {
        (request_id, WrhKeySource::RequestId)
    }
}

fn collect_kind(candidates: &[UpstreamCandidate], kind: UpstreamKind) -> Vec<Uuid> {
    candidates
        .iter()
        .filter(|c| c.kind == kind)
        .map(|c| c.upstream_id)
        .collect()
}

// -- Tier & signal types. ---------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Tier {
    KnownBase = 0,
    PartialBase = 1,
    Overage = 2,
    UnknownProbe = 3,
}

const fn rendezvous_salt_for_tier(tier: Tier, current_salt: &'static str) -> &'static str {
    match tier {
        Tier::KnownBase | Tier::PartialBase | Tier::UnknownProbe => current_salt,
        Tier::Overage => OVERAGE_RENDEZVOUS_SALT,
    }
}

fn tier_to_plugin_api(tier: Tier) -> cc_lb_domain::SubscriptionTier {
    match tier {
        Tier::KnownBase => cc_lb_domain::SubscriptionTier::KnownBase,
        Tier::PartialBase => cc_lb_domain::SubscriptionTier::PartialBase,
        Tier::Overage => cc_lb_domain::SubscriptionTier::Overage,
        Tier::UnknownProbe => cc_lb_domain::SubscriptionTier::UnknownProbe,
    }
}

const fn quota_weight_factor(tier: Tier, urgency: f64, bucket_total_urgency: f64) -> f64 {
    match tier {
        Tier::KnownBase | Tier::PartialBase => {
            if bucket_total_urgency < EPSILON {
                1.0
            } else {
                1.0 + urgency
            }
        }
        Tier::Overage => {
            if bucket_total_urgency < EPSILON {
                1.0
            } else {
                urgency
            }
        }
        Tier::UnknownProbe => 1.0,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BaseSignal {
    CurrentPositive,
    WarningPositive,
    HardNegative,
    Unknown,
}

struct OverageAssessment {
    ok: bool,
    base_exhausted_hint: bool,
    /// Fresh, finite utilization on the overage window (if present). Used
    /// for overage-tier urgency; `None` means "use OVERAGE_UNKNOWN_WEIGHT".
    fresh_overage_util: Option<f64>,
}

struct Assessment<'a> {
    candidate: &'a UpstreamCandidate,
    original_index: usize,
    tier: Tier,
    quota_urgency_5h: f64,
    quota_urgency_7d: f64,
    quota_urgency_combined: f64,
    overage_urgency: f64,
    warning_multiplier: f64,
}

impl Assessment<'_> {
    const fn tier_urgency(&self) -> f64 {
        match self.tier {
            Tier::KnownBase | Tier::PartialBase => {
                debug_assert!(self.quota_urgency_combined + EPSILON >= self.quota_urgency_5h);
                debug_assert!(self.quota_urgency_combined + EPSILON >= self.quota_urgency_7d);
                self.quota_urgency_combined
            }
            Tier::Overage => self.overage_urgency,
            Tier::UnknownProbe => 0.0,
        }
    }
}

#[derive(Clone, Copy)]
struct BaseWindowPressureConfig {
    window_len_secs: u64,
    gamma: f64,
    target_floor: f64,
}

// -- Candidate assessment. --------------------------------------------------

fn assess_candidate<'a>(
    candidate: &'a UpstreamCandidate,
    original_index: usize,
    base_windows: &[&'static str],
    config: &FilterConfig,
) -> Option<Assessment<'a>> {
    let now_secs = candidate_estimated_now(candidate);

    let mut positive_count = 0u32;
    let mut warning_positive_count = 0u32;
    let mut hard_negative_count = 0u32;
    let mut five_hour_pressure = 0.0f64;
    let mut seven_day_pressure = 0.0f64;
    let mut fable_weekly_pressure = 0.0f64;
    for &window in base_windows {
        let snapshot = find_snapshot(candidate, window);
        let signal = if window == WINDOW_SEVEN_DAY_FABLE {
            classify_fable_base_snapshot(snapshot, now_secs, config)
        } else {
            classify_base_snapshot(snapshot, now_secs, config)
        };
        match signal {
            BaseSignal::CurrentPositive => {
                positive_count += 1;
                match window {
                    WINDOW_FIVE_HOUR => {
                        five_hour_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    WINDOW_SEVEN_DAY => {
                        seven_day_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    WINDOW_SEVEN_DAY_FABLE => {
                        fable_weekly_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    _ => {}
                }
            }
            BaseSignal::WarningPositive => {
                positive_count += 1;
                warning_positive_count += 1;
                match window {
                    WINDOW_FIVE_HOUR => {
                        five_hour_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    WINDOW_SEVEN_DAY => {
                        seven_day_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    WINDOW_SEVEN_DAY_FABLE => {
                        fable_weekly_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    _ => {}
                }
            }
            BaseSignal::HardNegative => hard_negative_count += 1,
            BaseSignal::Unknown => {}
        }
    }
    let total = base_windows.len() as u32;
    let effective_weekly_pressure = weekly_smoothmax(seven_day_pressure, fable_weekly_pressure);
    let base_urgency = weighted_smoothmax(five_hour_pressure, effective_weekly_pressure);

    let overage = assess_overage(candidate, config);
    let base_proven_blocked = hard_negative_count > 0 || overage.base_exhausted_hint;

    let (tier, quota_urgency_5h, quota_urgency_7d, quota_urgency_combined, overage_urgency) =
        if base_proven_blocked {
            if overage.ok {
                (
                    Tier::Overage,
                    0.0,
                    0.0,
                    0.0,
                    overage_urgency(overage.fresh_overage_util),
                )
            } else {
                return None;
            }
        } else if positive_count == total && total > 0 {
            (
                Tier::KnownBase,
                five_hour_pressure,
                effective_weekly_pressure,
                base_urgency,
                0.0,
            )
        } else if positive_count > 0 {
            (
                Tier::PartialBase,
                five_hour_pressure,
                effective_weekly_pressure,
                base_urgency,
                0.0,
            )
        } else if config.unknown_probe_enabled {
            (Tier::UnknownProbe, 0.0, 0.0, 0.0, 0.0)
        } else {
            return None;
        };

    Some(Assessment {
        candidate,
        original_index,
        tier,
        quota_urgency_5h,
        quota_urgency_7d,
        quota_urgency_combined,
        overage_urgency,
        warning_multiplier: if warning_positive_count > 0 {
            WARNING_MULTIPLIER
        } else {
            1.0
        },
    })
}

fn base_window_pressure(
    snapshot: Option<&SubscriptionQuotaCandidateSnapshot>,
    now_secs: u64,
) -> f64 {
    let Some(snap) = snapshot else {
        return 0.0;
    };
    if snap.state != SubscriptionQuotaDataState::Fresh {
        return 0.0;
    }
    let Some(util) = snap.utilization else {
        return 0.0;
    };
    if !util.is_finite() {
        return 0.0;
    }
    let Some(resets_at) = snap.resets_at_unix_secs else {
        return 0.0;
    };
    if resets_at <= now_secs {
        return 0.0;
    }
    let Some(pressure_config) = base_window_pressure_config(snap.window.as_str()) else {
        return 0.0;
    };

    let remaining_ratio = 1.0 - util.clamp(0.0, 1.0);
    let time_ratio =
        ((resets_at - now_secs) as f64 / pressure_config.window_len_secs as f64).clamp(0.0, 1.0);
    pressure_from_ratios(remaining_ratio, time_ratio, pressure_config)
}

fn pressure_from_ratios(
    remaining_ratio: f64,
    time_ratio: f64,
    pressure_config: BaseWindowPressureConfig,
) -> f64 {
    let target = time_ratio
        .powf(pressure_config.gamma)
        .max(pressure_config.target_floor);
    (remaining_ratio / target).ln().max(0.0)
}

fn base_window_pressure_config(window: &str) -> Option<BaseWindowPressureConfig> {
    match window {
        WINDOW_FIVE_HOUR => Some(BaseWindowPressureConfig {
            window_len_secs: FIVE_HOUR_WINDOW_LEN_SECS,
            gamma: FIVE_HOUR_GAMMA,
            target_floor: FIVE_HOUR_TARGET_FLOOR,
        }),
        WINDOW_SEVEN_DAY | WINDOW_SEVEN_DAY_FABLE => Some(BaseWindowPressureConfig {
            window_len_secs: SEVEN_DAY_WINDOW_LEN_SECS,
            gamma: SEVEN_DAY_GAMMA,
            target_floor: SEVEN_DAY_TARGET_FLOOR,
        }),
        _ => None,
    }
}

fn weekly_smoothmax(shared_weekly_pressure: f64, fable_weekly_pressure: f64) -> f64 {
    (shared_weekly_pressure.powf(SMOOTHMAX_P) + fable_weekly_pressure.powf(SMOOTHMAX_P))
        .powf(1.0 / SMOOTHMAX_P)
}

fn weighted_smoothmax(five_hour_pressure: f64, seven_day_pressure: f64) -> f64 {
    (five_hour_pressure.powf(SMOOTHMAX_P)
        + (SEVEN_DAY_SMOOTHMAX_WEIGHT * seven_day_pressure).powf(SMOOTHMAX_P))
    .powf(1.0 / SMOOTHMAX_P)
}

fn overage_urgency(fresh_util: Option<f64>) -> f64 {
    match fresh_util {
        Some(u) if u.is_finite() => {
            let clamped = u.clamp(0.0, 1.0);
            let headroom = (1.0 - clamped).powi(HEADROOM_EXPONENT);
            headroom / (OVERAGE_REMAINING_NOMINAL_SECS as f64)
        }
        _ => OVERAGE_UNKNOWN_WEIGHT,
    }
}

fn classify_base_snapshot(
    snapshot: Option<&SubscriptionQuotaCandidateSnapshot>,
    now_secs: u64,
    config: &FilterConfig,
) -> BaseSignal {
    let Some(snap) = snapshot else {
        return BaseSignal::Unknown;
    };
    match snap.state {
        SubscriptionQuotaDataState::Missing => BaseSignal::Unknown,
        SubscriptionQuotaDataState::Fresh => {
            if snap.disabled_reason.is_some() {
                return BaseSignal::HardNegative;
            }
            if let Some(util) = snap.utilization
                && util.is_finite()
                && util >= 1.0
            {
                return BaseSignal::HardNegative;
            }
            let allowed_surpassed_threshold = match (snap.utilization, snap.surpassed_threshold) {
                (Some(util), Some(threshold)) => {
                    util.is_finite() && threshold.is_finite() && util >= threshold
                }
                (Some(_), None) | (None, Some(_)) | (None, None) => false,
            };
            match snap.status.as_deref() {
                Some("rejected") => BaseSignal::HardNegative,
                Some("allowed_warning") => BaseSignal::WarningPositive,
                Some("allowed") if allowed_surpassed_threshold => BaseSignal::WarningPositive,
                Some("allowed") => BaseSignal::CurrentPositive,
                Some(_) | None => utilization_signal(snap.utilization),
            }
        }
        SubscriptionQuotaDataState::Stale => {
            if snap.status.as_deref() == Some("rejected") {
                match snap.resets_at_unix_secs {
                    Some(resets_at) if resets_at > now_secs => BaseSignal::HardNegative,
                    Some(_) => BaseSignal::Unknown,
                    None => {
                        if config.stale_rejected_without_reset_blocks {
                            BaseSignal::HardNegative
                        } else {
                            BaseSignal::Unknown
                        }
                    }
                }
            } else {
                BaseSignal::Unknown
            }
        }
    }
}

fn classify_fable_base_snapshot(
    snapshot: Option<&SubscriptionQuotaCandidateSnapshot>,
    now_secs: u64,
    config: &FilterConfig,
) -> BaseSignal {
    let Some(snapshot) = snapshot else {
        return BaseSignal::Unknown;
    };
    if snapshot.state != SubscriptionQuotaDataState::Fresh {
        return BaseSignal::Unknown;
    }
    if !matches!(snapshot.utilization, Some(value) if value.is_finite()) {
        return BaseSignal::Unknown;
    }
    if !matches!(snapshot.resets_at_unix_secs, Some(value) if value > now_secs) {
        return BaseSignal::Unknown;
    }
    classify_base_snapshot(Some(snapshot), now_secs, config)
}

fn utilization_signal(util: Option<f64>) -> BaseSignal {
    match util {
        Some(u) if !u.is_finite() => BaseSignal::Unknown,
        Some(u) if u >= 1.0 => BaseSignal::HardNegative,
        Some(u) if (0.0..1.0).contains(&u) => BaseSignal::CurrentPositive,
        _ => BaseSignal::Unknown,
    }
}

fn assess_overage(candidate: &UpstreamCandidate, config: &FilterConfig) -> OverageAssessment {
    let overage_snap = find_snapshot(candidate, WINDOW_OVERAGE);
    let unified_snap = find_snapshot(candidate, WINDOW_UNIFIED);

    let overage_in_use = unified_snap
        .and_then(|s| s.overage_in_use)
        .or_else(|| overage_snap.and_then(|s| s.overage_in_use))
        .unwrap_or(false);
    let fallback_available = unified_snap
        .and_then(|s| s.fallback_available)
        .or_else(|| overage_snap.and_then(|s| s.fallback_available));
    let extra_usage_enabled = overage_snap
        .and_then(|s| s.extra_usage_enabled)
        .or_else(|| unified_snap.and_then(|s| s.extra_usage_enabled));
    let extra_usage_remaining = overage_snap
        .and_then(snapshot_extra_usage_remaining)
        .or_else(|| unified_snap.and_then(snapshot_extra_usage_remaining));

    let is_overage_fresh = overage_snap
        .map(|s| s.state == SubscriptionQuotaDataState::Fresh)
        .unwrap_or(false);
    let overage_status = overage_snap.and_then(|s| s.status.as_deref());
    let overage_util = overage_snap
        .and_then(|s| s.utilization)
        .filter(|u| u.is_finite());

    let mut positive_count = 0u32;
    if matches!(overage_status, Some("allowed") | Some("allowed_warning")) {
        positive_count += 1;
    }
    if let Some(u) = overage_util
        && u < 1.0
    {
        positive_count += 1;
    }
    if fallback_available == Some(true) {
        positive_count += 1;
    }
    if overage_in_use {
        positive_count += 1;
    }
    if extra_usage_enabled == Some(true) && extra_usage_remaining.map(|r| r > 0.0).unwrap_or(false)
    {
        positive_count += 1;
    }
    let positive = positive_count > 0;

    let overage_status_blocked = is_overage_fresh && overage_status == Some("rejected");
    let overage_util_blocked = is_overage_fresh && overage_util.map(|u| u >= 1.0).unwrap_or(false);
    let extra_usage_disabled = extra_usage_enabled == Some(false);
    let extra_usage_exhausted = extra_usage_enabled == Some(true)
        && extra_usage_remaining.map(|r| r <= 0.0).unwrap_or(false);
    let blocked = fallback_available == Some(false)
        || overage_status_blocked
        || overage_util_blocked
        || extra_usage_disabled
        || extra_usage_exhausted;

    let ok = if config.hard_overage_block_wins {
        positive && !blocked
    } else {
        positive
    };

    let fresh_overage_util = if is_overage_fresh { overage_util } else { None };

    OverageAssessment {
        ok,
        base_exhausted_hint: overage_in_use,
        fresh_overage_util,
    }
}

fn snapshot_extra_usage_remaining(s: &SubscriptionQuotaCandidateSnapshot) -> Option<f64> {
    match (s.extra_usage_monthly_limit, s.extra_usage_used_credits) {
        (Some(limit), Some(used)) if limit.is_finite() && used.is_finite() => Some(limit - used),
        (Some(limit), None) if limit.is_finite() => Some(limit),
        _ => None,
    }
}

fn find_snapshot<'a>(
    candidate: &'a UpstreamCandidate,
    window: &str,
) -> Option<&'a SubscriptionQuotaCandidateSnapshot> {
    candidate
        .subscription_quotas
        .iter()
        .find(|s| s.window == window)
}

/// Estimate the wall clock for reset-comparison purposes. Both the candidate
/// observation and the freshest snapshot observation are lower bounds on the
/// real clock; taking the max minimises the window in which a long-since-
/// reset quota still appears rejected.
fn candidate_estimated_now(candidate: &UpstreamCandidate) -> u64 {
    let snap_max = candidate
        .subscription_quotas
        .iter()
        .filter_map(|s| s.observed_at_unix_millis.map(|m| m / 1_000))
        .max()
        .unwrap_or(0);
    candidate.observed_at_unix_secs.max(snap_max)
}

// -- Weighted-rendezvous selection. -----------------------------------------

struct TierSelection<'a, 'b> {
    winner: &'b Assessment<'a>,
}

fn pick_within_tier<'a, 'b>(
    bucket: &'b [Assessment<'a>],
    routing_key: &str,
    config: &FilterConfig,
    pricing: &CachePricingSummary,
    max_cache_value_micros: i64,
) -> TierSelection<'a, 'b> {
    debug_assert!(!bucket.is_empty());
    let total_urgency: f64 = bucket.iter().map(Assessment::tier_urgency).sum();

    if bucket.len() == 1 {
        return TierSelection { winner: &bucket[0] };
    }

    let mut best_index = 0usize;
    let mut best_key = wrh_key(
        &bucket[0],
        routing_key,
        config,
        total_urgency,
        pricing,
        max_cache_value_micros,
    );
    for (i, assessment) in bucket.iter().enumerate().skip(1) {
        let key = wrh_key(
            assessment,
            routing_key,
            config,
            total_urgency,
            pricing,
            max_cache_value_micros,
        );
        if compare_wrh_key(&key, &best_key) == Ordering::Less {
            best_index = i;
            best_key = key;
        }
    }
    TierSelection {
        winner: &bucket[best_index],
    }
}

/// Read-token count on a bucket assessment. Collapses `None` cache_score and
/// `Some(0)` into zero because both mean "no cache-read benefit for this
/// request"; they tie under the max-in-bucket comparison and both produce
/// `cache_ratio = 0` when they are the only signal.
fn candidate_cache_read_tokens(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_cache_read_tokens)
}

fn candidate_cache_creation_tokens_5m(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_cache_creation_tokens_5m)
}

fn candidate_cache_creation_tokens_1h(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_cache_creation_tokens_1h)
}

fn candidate_uncached_input_tokens(assessment: &Assessment<'_>) -> u32 {
    assessment
        .candidate
        .cache_score
        .as_ref()
        .map_or(0, |score| score.predicted_uncached_input_tokens)
}

fn max_positive_cache_value_micros(
    bucket: &[Assessment<'_>],
    pricing: &CachePricingSummary,
) -> i64 {
    bucket
        .iter()
        .filter_map(|assessment| candidate_cache_value_micros(assessment.candidate, pricing))
        .filter(|value| *value > 0)
        .max()
        .unwrap_or(0)
}

fn bucket_v3_cache_affinity_key<'a>(
    bucket: &'a [Assessment<'_>],
    pricing: &CachePricingSummary,
) -> Option<&'a str> {
    bucket
        .iter()
        .filter_map(|assessment| {
            let cache_value_micros = candidate_cache_value_micros(assessment.candidate, pricing)?;
            if cache_value_micros <= 0 {
                return None;
            }
            let score = assessment.candidate.cache_score.as_ref()?;
            let cache_key = score.matched_v3_cache_key.as_deref()?;
            Some((
                cache_value_micros,
                score.predicted_cache_read_tokens,
                score.lookback_distance.unwrap_or(u32::MAX),
                assessment.candidate.upstream_id,
                cache_key,
            ))
        })
        .max_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| right.2.cmp(&left.2))
                .then_with(|| right.3.cmp(&left.3))
        })
        .map(|(_, _, _, _, cache_key)| cache_key)
}

fn cache_value_ratio_within_bucket(
    candidate_cache_value: Option<i64>,
    max_cache_value: i64,
) -> f64 {
    let Some(value) = candidate_cache_value else {
        return 0.0;
    };
    if max_cache_value <= 0 {
        0.0
    } else {
        ((value as f64) / (max_cache_value as f64)).clamp(-1.0, 1.0)
    }
}

fn cache_weight_multiplier(cache_value_ratio: f64) -> f64 {
    (CACHE_LOG_BOOST * cache_value_ratio).exp()
}

fn estimate_candidate_input_cost_micros(
    candidate: &UpstreamCandidate,
    pricing: &CachePricingSummary,
) -> Option<u64> {
    let score = candidate.cache_score.as_ref()?;
    let input_price = pricing.input_micros_per_million?;
    let cache_creation_5m_price = pricing.cache_creation_5m_micros_per_million?;
    let cache_creation_1h_price = pricing.cache_creation_1h_micros_per_million?;
    let cache_read_price = pricing.cache_read_micros_per_million?;
    Some(
        micros_for_tokens(score.predicted_uncached_input_tokens, input_price)
            .saturating_add(micros_for_tokens(
                score.predicted_cache_creation_tokens_5m,
                cache_creation_5m_price,
            ))
            .saturating_add(micros_for_tokens(
                score.predicted_cache_creation_tokens_1h,
                cache_creation_1h_price,
            ))
            .saturating_add(micros_for_tokens(
                score.predicted_cache_read_tokens,
                cache_read_price,
            )),
    )
}

fn cache_read_savings_ratio(candidate: &UpstreamCandidate, pricing: &CachePricingSummary) -> f64 {
    let Some(score) = candidate.cache_score.as_ref() else {
        return 0.0;
    };
    let Some(input_price) = pricing.input_micros_per_million else {
        return 0.0;
    };
    let Some(read_price) = pricing.cache_read_micros_per_million else {
        return 0.0;
    };
    let cold_cost = micros_for_tokens(score.predicted_cache_read_tokens, input_price);
    if cold_cost == 0 {
        return 0.0;
    }
    let read_cost = micros_for_tokens(score.predicted_cache_read_tokens, read_price);
    cold_cost.saturating_sub(read_cost) as f64 / cold_cost as f64
}

fn candidate_cache_value_micros(
    candidate: &UpstreamCandidate,
    pricing: &CachePricingSummary,
) -> Option<i64> {
    let score = candidate.cache_score.as_ref()?;
    let input_price = pricing.input_micros_per_million?;
    let cache_creation_5m_price = pricing.cache_creation_5m_micros_per_million?;
    let cache_creation_1h_price = pricing.cache_creation_1h_micros_per_million?;
    let cache_read_price = pricing.cache_read_micros_per_million?;

    let cold_read_cost = micros_for_tokens(score.predicted_cache_read_tokens, input_price);
    let cached_read_cost = micros_for_tokens(score.predicted_cache_read_tokens, cache_read_price);
    let read_savings = cold_read_cost.saturating_sub(cached_read_cost);
    let create_cost = micros_for_tokens(
        score.predicted_cache_creation_tokens_5m,
        cache_creation_5m_price,
    )
    .saturating_add(micros_for_tokens(
        score.predicted_cache_creation_tokens_1h,
        cache_creation_1h_price,
    ));
    Some(saturating_i128_to_i64(
        i128::from(read_savings) - i128::from(create_cost),
    ))
}

fn saturating_i128_to_i64(value: i128) -> i64 {
    i64::try_from(value).unwrap_or(if value.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

fn micros_for_tokens(tokens: u32, micros_per_million: u64) -> u64 {
    (u128::from(tokens) * u128::from(micros_per_million) / 1_000_000)
        .try_into()
        .unwrap_or(u64::MAX)
}

struct WrhKey {
    /// `-ln(u) / weight`. Lower key wins.
    score: f64,
    /// Negated raw hash so that higher hash wins during score ties.
    rendezvous_neg: u64,
    upstream_id: Uuid,
    original_index: usize,
}

fn wrh_key(
    assessment: &Assessment<'_>,
    routing_key: &str,
    config: &FilterConfig,
    bucket_total_urgency: f64,
    pricing: &CachePricingSummary,
    max_cache_value_micros: i64,
) -> WrhKey {
    let hash = rendezvous_hash(
        rendezvous_salt_for_tier(assessment.tier, config.rendezvous_hash_salt),
        routing_key,
        assessment.candidate.upstream_id,
    );
    let quota_weight = quota_weight_factor(
        assessment.tier,
        assessment.tier_urgency(),
        bucket_total_urgency,
    );
    let cache_ratio = cache_value_ratio_within_bucket(
        candidate_cache_value_micros(assessment.candidate, pricing),
        max_cache_value_micros,
    );
    let effective_weight =
        quota_weight * cache_weight_multiplier(cache_ratio) * assessment.warning_multiplier;
    let u = hash_to_open_unit(hash);
    let score = if effective_weight <= 0.0 {
        f64::INFINITY
    } else {
        -u.ln() / effective_weight
    };
    WrhKey {
        score,
        rendezvous_neg: u64::MAX - hash,
        upstream_id: assessment.candidate.upstream_id,
        original_index: assessment.original_index,
    }
}

fn compare_wrh_key(a: &WrhKey, b: &WrhKey) -> Ordering {
    a.score
        .total_cmp(&b.score)
        .then_with(|| a.rendezvous_neg.cmp(&b.rendezvous_neg))
        .then_with(|| a.upstream_id.cmp(&b.upstream_id))
        .then_with(|| a.original_index.cmp(&b.original_index))
}

/// Map a 64-bit hash to `u ∈ (0, 1)` using the top 53 bits so the division
/// is exact in f64. Guarantees `-ln(u)` is a finite positive number so WRH
/// scoring is numerically well-defined.
fn hash_to_open_unit(hash: u64) -> f64 {
    let top53 = hash >> 11;
    ((top53 as f64) + 0.5) / ((1u64 << 53) as f64)
}

// -- Rendezvous hash (FNV-1a 64 over salt || routing key || upstream_id,
// with a Murmur3 fmix64 avalanche finalizer). ------------------------------
//
// FNV-1a alone has weak avalanche: two inputs differing in one trailing byte
// produce hash values that differ by only a small fixed delta * fnv_prime.
// That is fatal for WRH — near-identical `u` values across candidates cause
// the highest-weight candidate to win every request. The Murmur3 fmix64
// step spreads any local input change across all 64 output bits, restoring
// the "independent uniforms per candidate" property WRH requires.

fn rendezvous_hash(salt: &str, routing_key: &str, upstream_id: Uuid) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let fnv_prime: u64 = 0x100_0000_01b3;
    let mix = |h: &mut u64, byte: u8| {
        *h ^= byte as u64;
        *h = h.wrapping_mul(fnv_prime);
    };
    for &b in salt.as_bytes() {
        mix(&mut h, b);
    }
    mix(&mut h, 0);
    for &b in routing_key.as_bytes() {
        mix(&mut h, b);
    }
    mix(&mut h, 0);
    for &b in upstream_id.as_bytes() {
        mix(&mut h, b);
    }
    fmix64(h)
}

fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[cfg(test)]
mod tests;
