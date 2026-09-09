//! Subscription-preference router filter.
//!
//! Selects at most one OAuth upstream per request via:
//!
//! 1. A strict tier ordering (KnownBase > PartialBase > Overage > UnknownProbe)
//!    that keeps the Anthropic base plan strictly higher priority than the
//!    overage bucket.
//! 2. Within the winning tier, deterministic cost-first-v1 selection. Candidates
//!    within five percent of the lowest known input cost use urgency, the
//!    overage-only warning multiplier, upstream ID, and original position as
//!    stable tiebreaks. Base-tier warning signals remain observable but do not
//!    affect ranking.
//!
//! ## Design rationale
//!
//! ADR 0008 base pressure compares utilization with the target burn pace for
//! the time remaining before reset. Within the near-cost candidate set,
//! positive pressure ranks ahead of zero-pressure, on-pace candidates.
//! The candidate `plan_capacity_ratio` remains available as metadata for
//! analytics but does not affect pressure, tiering, or winner selection.
//!
//! ## Windows
//!
//! Base: every non-missing shared quota window exposed by the upstream, plus
//! the matching model-scoped weekly window when present. `7d_fable` applies
//! only to the canonical model ID `claude-fable-5`; `7d_sonnet` and `7d_opus`
//! apply only to their matching model families. Absent quota kinds are not
//! synthesized into requirements. `unified` is not itself an exhaustion window; its
//! top-level flags enrich the overage assessment.
//!
//! ## Reset semantics
//!
//! - `7d_fable` uses the same `classify_base_snapshot` tier classification as
//!   `7d`.
//! - Its use-it-or-lose-it urgency contribution is zero unless the snapshot is
//!   fresh, has finite utilization, and has a future `resets_at`; the
//!   pressure-side reset gate enforces this independently of tier membership.
//! - Stale + `rejected` + future reset: hard negative (rejection still live).
//! - Stale + `rejected` + past reset: unknown (rejection expired).

use cc_lb_domain::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, CachePricingSummary,
    Principal, SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState, UpstreamCandidate,
    UpstreamKind,
};
use cc_lb_routing::{FilterError, FilterOutput, FilterPlugin, RoutingContext};
use std::cmp::Ordering;
use uuid::Uuid;

// -- Reason strings surfaced on FilterOutput.reason (log/audit surface). -----

pub(crate) const SUBSCRIPTION_ALIVE_REASON: &str = "keep:best_subscription_candidate";
pub(crate) const API_KEY_FALLBACK_REASON: &str = "keep:api_key_subscription_exhausted";
pub(crate) const NO_API_KEY_REASON: &str = "drop:subscription_exhausted_no_api_key";
pub(crate) const NO_SUBSCRIPTION_REASON: &str = "keep:no_subscription_candidates";

// -- Window labels. ----------------------------------------------------------

pub(crate) const WINDOW_FIVE_HOUR: &str = "5h";
pub(crate) const WINDOW_SEVEN_DAY: &str = "7d";
pub(crate) const WINDOW_SEVEN_DAY_FABLE: &str = "7d_fable";
pub(crate) const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
pub(crate) const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";
pub(crate) const WINDOW_OVERAGE: &str = "overage";
pub(crate) const WINDOW_UNIFIED: &str = "unified";
pub(crate) const FABLE_MODEL: &str = "claude-fable-5";
const SONNET_MODEL_PREFIX: &str = "claude-sonnet-";
const OPUS_MODEL_PREFIX: &str = "claude-opus-";

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

/// Guard below which aggregate urgency uses its neutral quota factor.
pub(crate) const EPSILON: f64 = 1e-12;

/// Nominal remaining-seconds denominator for overage-tier urgency. Anthropic
/// overage windows do not carry a reliable `resets_at`; billing rolls over on
/// the monthly boundary, so we use a fixed 30-day nominal.
pub(crate) const OVERAGE_REMAINING_NOMINAL_SECS: u64 = 30 * 86_400;

/// Baseline overage urgency for candidates whose utilization we cannot read.
pub(crate) const OVERAGE_UNKNOWN_WEIGHT: f64 = 0.5;

pub(crate) const CACHE_COST_BASIS_VERSION: &str = "v1";
const OVERAGE_WARNING_MULTIPLIER: f64 = 0.20;

const COST_FIRST_FORMULA_VERSION: &str = "cost-first-v1";

// -- Config knobs (compiled defaults today; expose per-principal later). ----

struct FilterConfig {
    unknown_probe_enabled: bool,
    stale_rejected_without_reset_blocks: bool,
    hard_overage_block_wins: bool,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            unknown_probe_enabled: true,
            stale_rejected_without_reset_blocks: true,
            hard_overage_block_wins: true,
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
        let config = FilterConfig::default();
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
            bucket.iter().map(move |a| {
                let cache_tokens = candidate_cache_read_tokens(a);
                let cache_value_micros =
                    candidate_cache_value_micros(a.candidate, &ctx.cache_pricing);
                let cache_ratio =
                    cache_value_ratio_within_bucket(cache_value_micros, max_cache_value_micros);
                let tier_urgency = a.tier_urgency();
                let estimated_input_cost_micros =
                    estimate_candidate_input_cost_micros(a.candidate, &ctx.cache_pricing)
                        .unwrap_or(0);
                let cache_savings_ratio = cache_read_savings_ratio(a.candidate, &ctx.cache_pricing);
                cc_lb_domain::CandidateUrgency {
                    upstream_id: a.candidate.upstream_id,
                    tier: tier_to_plugin_api(a.tier),
                    quota_urgency: tier_urgency,
                    quota_urgency_5h: matches!(a.tier, Tier::KnownBase | Tier::PartialBase)
                        .then_some(a.quota_urgency_5h),
                    quota_urgency_7d: matches!(a.tier, Tier::KnownBase | Tier::PartialBase)
                        .then_some(a.quota_urgency_7d),
                    quota_urgency_combined: matches!(a.tier, Tier::KnownBase | Tier::PartialBase)
                        .then_some(a.quota_urgency_combined),
                    predicted_cache_read_tokens: cache_tokens,
                    predicted_cache_creation_tokens_5m: candidate_cache_creation_tokens_5m(a),
                    predicted_cache_creation_tokens_1h: candidate_cache_creation_tokens_1h(a),
                    predicted_uncached_input_tokens: candidate_uncached_input_tokens(a),
                    cache_ratio,
                    warning_multiplier: a.warning_multiplier,
                    cache_savings_ratio,
                    estimated_input_cost_micros,
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
        let bucket_v3_cache_key = bucket_v3_cache_key(bucket, &ctx.cache_pricing);
        let selection = pick_within_tier(bucket, &ctx.cache_pricing);
        let formula_winner = selection.winner;
        let chosen_tier = tier_to_plugin_api(formula_winner.tier);
        let kept_upstream_id = formula_winner.candidate.upstream_id;

        let trace = cc_lb_domain::SubscriptionPreferenceTrace {
            chosen_tier,
            candidates: all_assessments,
            previous_tier: None,
            formula_version: Some(COST_FIRST_FORMULA_VERSION.to_owned()),
            cache_cost_basis_version: Some(CACHE_COST_BASIS_VERSION.to_owned()),
            formula_winner_upstream_id: Some(formula_winner.candidate.upstream_id),
            kept_upstream_id: Some(kept_upstream_id),
            incumbent_upstream_id: None,
            estimated_switch_cache_loss_micros: None,
            cache_loss_status: None,
            switch_gate_reason: Some("formula_winner".to_owned()),
            bucket_v3_cache_key: bucket_v3_cache_key.map(str::to_owned),
            lineage_would_have_predicted_read_tokens: None,
            lineage_would_have_picked_upstream_id: None,
        };
        return FilterOutput {
            kept_upstream_ids: vec![kept_upstream_id],
            reason: SUBSCRIPTION_ALIVE_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: Some(trace),
        };
    }

    if has_api_key {
        FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            reason: API_KEY_FALLBACK_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        }
    } else {
        FilterOutput {
            kept_upstream_ids: Vec::new(),
            reason: NO_API_KEY_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
            subscription_preference: None,
        }
    }
}

fn relevant_base_windows(canonical_model: &str) -> Vec<&'static str> {
    let mut windows = vec![WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY];
    if canonical_model == FABLE_MODEL {
        windows.push(WINDOW_SEVEN_DAY_FABLE);
    } else if canonical_model.starts_with(SONNET_MODEL_PREFIX) {
        windows.push(WINDOW_SEVEN_DAY_SONNET);
    } else if canonical_model.starts_with(OPUS_MODEL_PREFIX) {
        windows.push(WINDOW_SEVEN_DAY_OPUS);
    }
    windows
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

fn tier_to_plugin_api(tier: Tier) -> cc_lb_domain::SubscriptionTier {
    match tier {
        Tier::KnownBase => cc_lb_domain::SubscriptionTier::KnownBase,
        Tier::PartialBase => cc_lb_domain::SubscriptionTier::PartialBase,
        Tier::Overage => cc_lb_domain::SubscriptionTier::Overage,
        Tier::UnknownProbe => cc_lb_domain::SubscriptionTier::UnknownProbe,
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
    let mut total = 0u32;
    let mut five_hour_pressure = 0.0f64;
    let mut shared_weekly_pressure = 0.0f64;
    let mut model_scoped_weekly_pressure = 0.0f64;
    for window in base_windows.iter().copied() {
        let snapshot = find_snapshot(candidate, window);
        if snapshot.is_some_and(|snapshot| snapshot.state == SubscriptionQuotaDataState::Absent) {
            continue;
        }
        total += 1;
        let signal = classify_base_snapshot(snapshot, now_secs, config);
        match signal {
            BaseSignal::CurrentPositive => {
                positive_count += 1;
                match window {
                    WINDOW_FIVE_HOUR => {
                        five_hour_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    WINDOW_SEVEN_DAY => {
                        shared_weekly_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    WINDOW_SEVEN_DAY_FABLE | WINDOW_SEVEN_DAY_SONNET | WINDOW_SEVEN_DAY_OPUS => {
                        model_scoped_weekly_pressure = base_window_pressure(snapshot, now_secs);
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
                        shared_weekly_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    WINDOW_SEVEN_DAY_FABLE | WINDOW_SEVEN_DAY_SONNET | WINDOW_SEVEN_DAY_OPUS => {
                        model_scoped_weekly_pressure = base_window_pressure(snapshot, now_secs);
                    }
                    _ => {}
                }
            }
            BaseSignal::HardNegative => hard_negative_count += 1,
            BaseSignal::Unknown => {}
        }
    }
    let effective_weekly_pressure =
        weekly_smoothmax(shared_weekly_pressure, model_scoped_weekly_pressure);
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
        warning_multiplier: if tier == Tier::Overage && warning_positive_count > 0 {
            OVERAGE_WARNING_MULTIPLIER
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
        WINDOW_SEVEN_DAY
        | WINDOW_SEVEN_DAY_FABLE
        | WINDOW_SEVEN_DAY_SONNET
        | WINDOW_SEVEN_DAY_OPUS => Some(BaseWindowPressureConfig {
            window_len_secs: SEVEN_DAY_WINDOW_LEN_SECS,
            gamma: SEVEN_DAY_GAMMA,
            target_floor: SEVEN_DAY_TARGET_FLOOR,
        }),
        _ => None,
    }
}

fn weekly_smoothmax(shared_weekly_pressure: f64, model_scoped_weekly_pressure: f64) -> f64 {
    (shared_weekly_pressure.powf(SMOOTHMAX_P) + model_scoped_weekly_pressure.powf(SMOOTHMAX_P))
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
        SubscriptionQuotaDataState::Absent | SubscriptionQuotaDataState::Unobserved => {
            BaseSignal::Unknown
        }
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
            let hard_negative = snap.disabled_reason.is_some()
                || snap
                    .utilization
                    .is_some_and(|util| util.is_finite() && util >= 1.0)
                || snap.status.as_deref() == Some("rejected");
            if !hard_negative {
                return BaseSignal::Unknown;
            }
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
        }
    }
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
    let fresh_overage_snap =
        overage_snap.filter(|snapshot| snapshot.state == SubscriptionQuotaDataState::Fresh);
    let fresh_unified_snap =
        unified_snap.filter(|snapshot| snapshot.state == SubscriptionQuotaDataState::Fresh);
    let fresh_snapshots = [fresh_overage_snap, fresh_unified_snap];
    let all_snapshots = [overage_snap, unified_snap];

    let overage_status = fresh_overage_snap.and_then(|snapshot| snapshot.status.as_deref());
    let overage_util = fresh_overage_snap
        .and_then(|snapshot| snapshot.utilization)
        .filter(|utilization| utilization.is_finite());
    let overage_in_use = fresh_snapshots
        .iter()
        .flatten()
        .any(|snapshot| snapshot.overage_in_use == Some(true));
    let fallback_available = fresh_snapshots
        .iter()
        .flatten()
        .any(|snapshot| snapshot.fallback_available == Some(true));
    let extra_usage_available = fresh_snapshots.iter().flatten().any(|snapshot| {
        snapshot.extra_usage_enabled == Some(true)
            && snapshot_extra_usage_remaining(snapshot).is_some_and(|remaining| remaining > 0.0)
    });

    let positive = matches!(overage_status, Some("allowed") | Some("allowed_warning"))
        || overage_util.is_some_and(|utilization| utilization < 1.0)
        || fallback_available
        || overage_in_use
        || extra_usage_available;

    let fallback_unavailable = all_snapshots
        .iter()
        .flatten()
        .any(|snapshot| snapshot.fallback_available == Some(false));
    let overage_status_blocked = overage_status == Some("rejected");
    let overage_util_blocked = overage_util.is_some_and(|utilization| utilization >= 1.0);
    let extra_usage_disabled = all_snapshots
        .iter()
        .flatten()
        .any(|snapshot| snapshot.extra_usage_enabled == Some(false));
    let extra_usage_exhausted = all_snapshots.iter().flatten().any(|snapshot| {
        snapshot.extra_usage_enabled == Some(true)
            && snapshot_extra_usage_remaining(snapshot).is_some_and(|remaining| remaining <= 0.0)
    });
    let blocked = fallback_unavailable
        || overage_status_blocked
        || overage_util_blocked
        || extra_usage_disabled
        || extra_usage_exhausted;

    let ok = if config.hard_overage_block_wins {
        positive && !blocked
    } else {
        positive
    };

    OverageAssessment {
        ok,
        base_exhausted_hint: overage_in_use,
        fresh_overage_util: overage_util,
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

// -- Cost-first selection. ---------------------------------------------------

struct TierSelection<'a, 'b> {
    winner: &'b Assessment<'a>,
}

fn pick_within_tier<'a, 'b>(
    bucket: &'b [Assessment<'a>],
    pricing: &CachePricingSummary,
) -> TierSelection<'a, 'b> {
    debug_assert!(!bucket.is_empty());

    if bucket.len() == 1 {
        return TierSelection { winner: &bucket[0] };
    }

    let min_cost = bucket
        .iter()
        .filter_map(|assessment| {
            estimate_candidate_input_cost_micros(assessment.candidate, pricing)
        })
        .min();

    let mut winner = CostFirstCandidate {
        assessment: &bucket[0],
        cost: None,
    };

    match min_cost {
        Some(min_cost) => {
            let mut found_near_cost_candidate = false;
            for assessment in bucket {
                let Some(cost) =
                    estimate_candidate_input_cost_micros(assessment.candidate, pricing)
                else {
                    continue;
                };
                if u128::from(cost) * 100 > u128::from(min_cost) * 105 {
                    continue;
                }

                let candidate = CostFirstCandidate {
                    assessment,
                    cost: Some(cost),
                };
                if !found_near_cost_candidate
                    || candidate.compare_for_winner(winner) == Ordering::Greater
                {
                    winner = candidate;
                    found_near_cost_candidate = true;
                }
            }
            debug_assert!(found_near_cost_candidate);
        }
        None => {
            for assessment in &bucket[1..] {
                let candidate = CostFirstCandidate {
                    assessment,
                    cost: None,
                };
                if candidate.compare_for_winner(winner) == Ordering::Greater {
                    winner = candidate;
                }
            }
        }
    }

    TierSelection {
        winner: winner.assessment,
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

fn bucket_v3_cache_key<'a>(
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

#[derive(Clone, Copy)]
struct CostFirstCandidate<'candidate, 'assessment> {
    assessment: &'assessment Assessment<'candidate>,
    cost: Option<u64>,
}

impl CostFirstCandidate<'_, '_> {
    fn compare_for_winner(self, other: Self) -> Ordering {
        // Cheapest warm owners are selected deterministically; only near-cost peers use urgency.
        let effective_urgency = self.assessment.tier_urgency() * self.assessment.warning_multiplier;
        let other_effective_urgency =
            other.assessment.tier_urgency() * other.assessment.warning_multiplier;
        effective_urgency
            .total_cmp(&other_effective_urgency)
            .then_with(|| {
                self.assessment
                    .warning_multiplier
                    .total_cmp(&other.assessment.warning_multiplier)
            })
            .then_with(|| match (self.cost, other.cost) {
                (Some(cost), Some(other_cost)) => other_cost.cmp(&cost),
                (None, None) => Ordering::Equal,
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
            })
            .then_with(|| {
                other
                    .assessment
                    .candidate
                    .upstream_id
                    .cmp(&self.assessment.candidate.upstream_id)
            })
            .then_with(|| {
                other
                    .assessment
                    .original_index
                    .cmp(&self.assessment.original_index)
            })
    }
}

#[cfg(test)]
mod tests;
