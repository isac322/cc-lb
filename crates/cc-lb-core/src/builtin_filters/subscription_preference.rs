//! Subscription-preference router filter.
//!
//! Selects at most one OAuth upstream per request via a strict lexicographic
//! **tier** ordering that keeps the Anthropic base plan strictly higher
//! priority than the overage bucket. Base plan (5h + 7d, plus 7d_sonnet on
//! sonnet requests) always wins over overage: an upstream whose base is
//! healthy is preferred even when its overage has been auto-topup-rejected.
//!
//! ## Tiers (strict lexicographic order — highest tier wins as a whole)
//!
//! 1. **KnownBase** — every relevant base window is currently positive
//!    (fresh + allowed/warning, or fresh + no status + utilization < 1).
//! 2. **PartialBase** — at least one relevant base window is currently
//!    positive, no window is a hard negative.
//! 3. **Overage** — base is proven blocked (some window is a hard negative
//!    OR upstream reports `overage_in_use=true`) AND overage is usable.
//! 4. **UnknownProbe** — no positive AND no hard negative base signals; the
//!    upstream may or may not be healthy. Policy-toggleable.
//!
//! Candidates that fit no tier are Dead and dropped. If every OAuth candidate
//! is Dead, an API-key upstream is preferred when available; otherwise the
//! filter passes the exhausted OAuth candidates through so the upstream's
//! authoritative response reaches the caller.
//!
//! Within a tier, the winner is selected by:
//!
//! 1. Tier-specific score (higher wins). The score for a base tier reads
//!    **only** base windows; the overage tier score reads **only** the
//!    overage snapshot; the probe tier score reads **only** base window
//!    unknown signals. This isolates tiers from each other's noise.
//! 2. Rendezvous hash of `(salt, request_id, upstream_id)` (higher wins).
//!    Provides load spread + per-request affinity without hot-spotting the
//!    lowest UUID.
//! 3. `upstream_id` lexical order (lowest wins). Deterministic final
//!    tie-break.
//!
//! ## Design notes
//!
//! - `7d_opus` is deliberately ignored. Anthropic ships the label with no
//!   real quota attached, so opus traffic uses the shared `7d` counter.
//! - `unified` is not read as an exhaustion window itself; the per-window
//!   signals (`5h`, `7d`, `overage`) are already independent. `unified`'s
//!   top-level flags (`overage_in_use`, `fallback_available`) are used only
//!   to enrich the overage assessment.
//! - Reset semantics are strict: a reset scheduled 30s from now does **not**
//!   unblock a currently-rejected candidate for the request in flight.
//! - Stale evidence: a stale `rejected` snapshot with `resets_at` in the
//!   future is still a hard negative. Stale evidence whose reset has already
//!   passed downgrades to unknown so the candidate can recover naturally.

use cc_lb_plugin_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, FilterError,
    FilterOutput, FilterPlugin, Principal, RequestContext, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, UpstreamCandidate, UpstreamKind,
};
use std::cmp::Ordering;
use uuid::Uuid;

// -- Reason strings surfaced on FilterOutput.reason (part of the log/audit surface). ------------

pub(crate) const SUBSCRIPTION_ALIVE_REASON: &str = "keep:best_subscription_candidate";
pub(crate) const API_KEY_FALLBACK_REASON: &str = "keep:api_key_subscription_exhausted";
pub(crate) const NO_API_KEY_REASON: &str = "keep:subscription_exhausted_no_api_key";
pub(crate) const NO_SUBSCRIPTION_REASON: &str = "keep:no_subscription_candidates";

// -- Window labels. -----------------------------------------------------------------------------

pub(crate) const WINDOW_FIVE_HOUR: &str = "5h";
pub(crate) const WINDOW_SEVEN_DAY: &str = "7d";
pub(crate) const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
#[allow(dead_code)]
pub(crate) const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";
pub(crate) const WINDOW_OVERAGE: &str = "overage";
pub(crate) const WINDOW_UNIFIED: &str = "unified";

// -- Intra-tier score constants. ---------------------------------------------------------------

/// Penalty applied per warning-status base window inside the base tier
/// score. Keeps warnings below cleanly-allowed candidates while remaining
/// small enough not to dominate the base headroom term.
const WARNING_PENALTY: f64 = 0.05;

/// Salt for the rendezvous hash. Kept versioned so bucket assignment can be
/// rotated by bumping the salt without changing per-window semantics.
const RENDEZVOUS_SALT: &str = "cclb-subscription-preference-v1";

// -- Config knobs. -----------------------------------------------------------------------------
//
// Currently hard-coded to sane defaults. The filter registers as a built-in
// with no per-principal config plumbed in; when routing config surfaces
// need to expose these, promote `FilterConfig` to a public struct threaded
// through `SubscriptionPreferenceFilter::with_config` at instantiation
// time in `dynamic_view_builder`.

struct FilterConfig {
    /// When true, candidates with no positive base signal AND no hard
    /// negative base signal are eligible via the `UnknownProbe` tier. When
    /// false, unknown-only candidates are Dead. Default: `true`.
    unknown_probe_enabled: bool,
    /// Stale `rejected` snapshots without a `resets_at` are treated as hard
    /// negatives when this is `true`; unknown when `false`. Default: `true`.
    stale_rejected_without_reset_blocks: bool,
    /// Overage `blocked` evidence overrides overage `positive` evidence when
    /// this is `true` (default). Set `false` only to allow explicit
    /// `overage_in_use=true` to defeat conflicting header-parsed rejection.
    hard_overage_block_wins: bool,
    /// Salt for rendezvous-hash spread + affinity.
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

// -- Public plugin type. -----------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct SubscriptionPreferenceFilter;

impl SubscriptionPreferenceFilter {
    pub fn new() -> Self {
        Self
    }
}

impl FilterPlugin for SubscriptionPreferenceFilter {
    fn filter(
        &self,
        ctx: &RequestContext,
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

// -- Core evaluation. --------------------------------------------------------------------------

fn evaluate(
    ctx: &RequestContext,
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
        };
    }

    // Classify every OAuth candidate into a tier bucket.
    let mut buckets: [Vec<Assessment<'_>>; 4] =
        [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.kind != UpstreamKind::AnthropicOauth {
            continue;
        }
        if let Some(assessment) = assess_candidate(candidate, index, &base_windows, config) {
            buckets[assessment.tier as usize].push(assessment);
        }
    }

    // Pick winner from the highest non-empty bucket.
    for bucket in buckets.iter() {
        if bucket.is_empty() {
            continue;
        }
        let winner = pick_within_tier(bucket, ctx, config);
        return FilterOutput {
            kept_upstream_ids: vec![winner.candidate.upstream_id],
            reason: SUBSCRIPTION_ALIVE_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
        };
    }

    // Every OAuth candidate was Dead.
    if has_api_key {
        FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            reason: API_KEY_FALLBACK_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
        }
    } else {
        FilterOutput {
            kept_upstream_ids: collect_kind(candidates, UpstreamKind::AnthropicOauth),
            reason: NO_API_KEY_REASON.to_owned(),
            per_candidate_reasons: Vec::new(),
        }
    }
}

fn relevant_base_windows(canonical_model: &str) -> Vec<&'static str> {
    let mut windows = vec![WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY];
    if canonical_model.contains("sonnet") {
        windows.push(WINDOW_SEVEN_DAY_SONNET);
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

// -- Tier & signal types. ----------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Tier {
    KnownBase = 0,
    PartialBase = 1,
    Overage = 2,
    UnknownProbe = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BaseSignal {
    CurrentPositive { warning: bool },
    HardNegative,
    Unknown,
}

struct BaseSignalEntry {
    signal: BaseSignal,
    utilization: Option<f64>,
}

struct OverageAssessment {
    ok: bool,
    base_exhausted_hint: bool,
    remaining_headroom: Option<f64>,
    positive_count: u32,
    fresh_positive: bool,
}

struct Assessment<'a> {
    candidate: &'a UpstreamCandidate,
    original_index: usize,
    tier: Tier,
    base_signals: Vec<BaseSignalEntry>,
    overage: OverageAssessment,
}

// -- Candidate assessment. --------------------------------------------------------------------

fn assess_candidate<'a>(
    candidate: &'a UpstreamCandidate,
    original_index: usize,
    base_windows: &[&'static str],
    config: &FilterConfig,
) -> Option<Assessment<'a>> {
    let now_secs = candidate_estimated_now(candidate);

    let mut base_signals = Vec::with_capacity(base_windows.len());
    let mut positive_count = 0u32;
    let mut hard_negative_count = 0u32;
    for &window in base_windows {
        let snapshot = find_snapshot(candidate, window);
        let signal = classify_base_snapshot(snapshot, now_secs, config);
        match signal {
            BaseSignal::CurrentPositive { .. } => positive_count += 1,
            BaseSignal::HardNegative => hard_negative_count += 1,
            BaseSignal::Unknown => {}
        }
        base_signals.push(BaseSignalEntry {
            signal,
            utilization: snapshot.and_then(|s| s.utilization),
        });
    }
    let total = base_windows.len() as u32;

    let overage = assess_overage(candidate, config);
    let base_proven_blocked = hard_negative_count > 0 || overage.base_exhausted_hint;

    let tier = if base_proven_blocked {
        if overage.ok { Tier::Overage } else { return None }
    } else if positive_count == total && total > 0 {
        Tier::KnownBase
    } else if positive_count > 0 {
        Tier::PartialBase
    } else if config.unknown_probe_enabled {
        Tier::UnknownProbe
    } else {
        return None;
    };

    Some(Assessment {
        candidate,
        original_index,
        tier,
        base_signals,
        overage,
    })
}

/// Classify one base-window snapshot. Encodes the state machine from the
/// filter design doc:
///
/// - Missing / no snapshot → Unknown
/// - Fresh + disabled_reason → HardNegative
/// - Fresh + status="rejected" → HardNegative
/// - Fresh + status="allowed"/"allowed_warning" → CurrentPositive
/// - Fresh + no known status → utilization decides:
///     util ≥ 1 → HardNegative, util < 1 → CurrentPositive, util=None → Unknown
/// - Stale + status="rejected":
///     resets_at > now → HardNegative (rejection still live)
///     resets_at ≤ now → Unknown (rejection expired, allow recovery)
///     resets_at=None → HardNegative if config, else Unknown
/// - Stale + anything else → Unknown
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
            match snap.status.as_deref() {
                Some("rejected") => BaseSignal::HardNegative,
                Some("allowed") => BaseSignal::CurrentPositive { warning: false },
                Some("allowed_warning") => BaseSignal::CurrentPositive { warning: true },
                // Unknown status label — fall through to utilization gate.
                Some(_) | None => utilization_signal(snap.utilization),
            }
        }
        SubscriptionQuotaDataState::Stale => {
            if snap.status.as_deref() == Some("rejected") {
                match snap.resets_at_unix_secs {
                    Some(resets_at) if resets_at > now_secs => BaseSignal::HardNegative,
                    Some(_) => BaseSignal::Unknown, // rejection expired
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

fn utilization_signal(util: Option<f64>) -> BaseSignal {
    match util {
        Some(u) if !u.is_finite() => BaseSignal::Unknown,
        Some(u) if u >= 1.0 => BaseSignal::HardNegative,
        Some(u) if (0.0..1.0).contains(&u) => BaseSignal::CurrentPositive { warning: false },
        _ => BaseSignal::Unknown,
    }
}

/// Assess overage headroom for a candidate. Reads the `overage` snapshot for
/// per-window signals and the `unified` snapshot for top-level flags
/// (`overage_in_use`, `fallback_available`), plus `extra_usage_*` fields
/// wherever they were surfaced.
///
/// A `positive` signal alone does not make overage usable; a `blocked`
/// signal wins by default (`hard_overage_block_wins=true`).
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

    // Positive signals — any evidence overage is usable.
    let mut positive_count = 0u32;
    let mut fresh_positive = false;
    if matches!(overage_status, Some("allowed") | Some("allowed_warning")) {
        positive_count += 1;
        if is_overage_fresh {
            fresh_positive = true;
        }
    }
    if let Some(u) = overage_util
        && u < 1.0
    {
        positive_count += 1;
        if is_overage_fresh {
            fresh_positive = true;
        }
    }
    if fallback_available == Some(true) {
        positive_count += 1;
    }
    if overage_in_use {
        // overage_in_use=true simultaneously proves base is spent AND
        // demonstrates overage is currently servicing traffic.
        positive_count += 1;
    }
    if extra_usage_enabled == Some(true)
        && extra_usage_remaining.map(|r| r > 0.0).unwrap_or(false)
    {
        positive_count += 1;
    }
    let positive = positive_count > 0;

    // Blocked signals — only fresh evidence counts against overage
    // (an expired 4-hour-old header rejection should not permanently kill
    // overage routing).
    let overage_status_blocked = is_overage_fresh && overage_status == Some("rejected");
    let overage_util_blocked = is_overage_fresh
        && overage_util.map(|u| u >= 1.0).unwrap_or(false);
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

    let remaining_headroom = overage_util.map(|u| (1.0 - u).clamp(0.0, 1.0));

    OverageAssessment {
        ok,
        base_exhausted_hint: overage_in_use,
        remaining_headroom,
        positive_count,
        fresh_positive,
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

// -- Intra-tier winner selection. --------------------------------------------------------------

fn pick_within_tier<'a, 'b>(
    bucket: &'b [Assessment<'a>],
    ctx: &RequestContext,
    config: &FilterConfig,
) -> &'b Assessment<'a> {
    debug_assert!(!bucket.is_empty());
    let mut best_index = 0usize;
    let mut best_key = tiebreak_key(&bucket[0], ctx, config);
    for i in 1..bucket.len() {
        let key = tiebreak_key(&bucket[i], ctx, config);
        if compare_tiebreak_key(&key, &best_key) == Ordering::Less {
            best_index = i;
            best_key = key;
        }
    }
    &bucket[best_index]
}

struct TiebreakKey {
    /// Negated score so that lower key = higher score = better candidate.
    score_neg: f64,
    /// Negated hash so higher hash sorts first.
    rendezvous_neg: u64,
    upstream_id: Uuid,
    original_index: usize,
}

fn tiebreak_key(
    assessment: &Assessment<'_>,
    ctx: &RequestContext,
    config: &FilterConfig,
) -> TiebreakKey {
    let score = intra_tier_score(assessment);
    let rendezvous = rendezvous_hash(
        config.rendezvous_hash_salt,
        &ctx.request_id,
        assessment.candidate.upstream_id,
    );
    TiebreakKey {
        score_neg: -score,
        rendezvous_neg: u64::MAX - rendezvous,
        upstream_id: assessment.candidate.upstream_id,
        original_index: assessment.original_index,
    }
}

fn compare_tiebreak_key(a: &TiebreakKey, b: &TiebreakKey) -> Ordering {
    a.score_neg
        .total_cmp(&b.score_neg)
        .then_with(|| a.rendezvous_neg.cmp(&b.rendezvous_neg))
        .then_with(|| a.upstream_id.cmp(&b.upstream_id))
        .then_with(|| a.original_index.cmp(&b.original_index))
}

fn intra_tier_score(a: &Assessment<'_>) -> f64 {
    match a.tier {
        Tier::KnownBase | Tier::PartialBase => score_base_tier(a),
        Tier::Overage => score_overage_tier(a),
        Tier::UnknownProbe => score_probe_tier(a),
    }
}

/// Base-tier score. Reads only base-window signals; overage/unified
/// utilization must never leak in.
///
/// Components (higher wins):
/// - min headroom across positive base windows in `[0, 1]`
/// - positive ratio in `[0, 1]`
/// - warning penalty (small negative per warning window)
fn score_base_tier(a: &Assessment<'_>) -> f64 {
    let mut min_headroom: f64 = 1.0;
    let mut has_positive_headroom = false;
    let mut positive_count = 0.0;
    let mut warning_count = 0.0;
    let mut total = 0.0;
    for entry in &a.base_signals {
        total += 1.0;
        if let BaseSignal::CurrentPositive { warning } = entry.signal {
            positive_count += 1.0;
            if warning {
                warning_count += 1.0;
            }
            if let Some(u) = entry.utilization
                && u.is_finite()
                && (0.0..=1.0).contains(&u)
            {
                min_headroom = min_headroom.min(1.0 - u);
                has_positive_headroom = true;
            }
        }
    }
    let headroom_component = if has_positive_headroom {
        min_headroom
    } else {
        0.5
    };
    let positive_ratio = if total > 0.0 {
        positive_count / total
    } else {
        0.0
    };
    let warning_penalty = warning_count * WARNING_PENALTY;

    headroom_component + positive_ratio - warning_penalty
}

/// Overage-tier score. Reads only the overage snapshot.
fn score_overage_tier(a: &Assessment<'_>) -> f64 {
    let ov = &a.overage;
    let remaining = ov.remaining_headroom.unwrap_or(0.5);
    let positive_rank = (ov.positive_count as f64).min(3.0) * 0.1;
    let fresh_bonus = if ov.fresh_positive { 0.05 } else { 0.0 };
    remaining + positive_rank + fresh_bonus
}

/// Probe-tier score. Reads only the unknown-base signals — an unknown
/// candidate whose stale reads suggest lower utilization is preferred over
/// one whose stale reads look near-full.
fn score_probe_tier(a: &Assessment<'_>) -> f64 {
    let mut snapshots_present = 0.0;
    let mut lowest_util: Option<f64> = None;
    for entry in &a.base_signals {
        if !matches!(entry.signal, BaseSignal::Unknown) {
            continue;
        }
        if let Some(u) = entry.utilization
            && u.is_finite()
            && (0.0..=1.0).contains(&u)
        {
            snapshots_present += 1.0;
            lowest_util = Some(lowest_util.map(|x| x.min(u)).unwrap_or(u));
        }
    }
    let headroom_estimate = lowest_util.map(|u| 1.0 - u).unwrap_or(0.5);
    headroom_estimate + snapshots_present * 0.05
}

// -- Rendezvous hash (FNV-1a 64 over salt || request_id || upstream_id). ----------------------

fn rendezvous_hash(salt: &str, request_id: &str, upstream_id: Uuid) -> u64 {
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
    for &b in request_id.as_bytes() {
        mix(&mut h, b);
    }
    mix(&mut h, 0);
    for &b in upstream_id.as_bytes() {
        mix(&mut h, b);
    }
    h
}

#[cfg(test)]
mod tests;
