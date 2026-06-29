use cc_lb_plugin_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, FilterError,
    FilterOutput, FilterPlugin, Principal, RequestContext, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, UpstreamCandidate, UpstreamKind,
};
use cc_lb_storage_api::PluginMetadata;
use std::cmp::Ordering;
use uuid::Uuid;

pub const PURPOSE: &str = "Prefer a single OAuth upstream selected by a continuous quota-headroom score that combines max-applicable-utilization with drain-before-lockout and use-before-reset-waste urgency; fall back to API-key upstreams only when every OAuth candidate is exhausted on a quota window that applies to the request model.";
pub const KEEPS: &str = "Exactly one OAuth candidate — the one whose combined headroom + reset-urgency score is best — when any OAuth candidate is alive; otherwise API-key candidates when every OAuth candidate is exhausted.";
pub const DROPS: &str = "API-key candidates while any OAuth candidate is alive; all OAuth candidates except the chosen best one when subscription routing is active; OAuth candidates whose subscription quota is exhausted on any window that applies to the request model when API-key fallback is available.";
pub const EMPTY_BEHAVIOR: &str = "Never drops everything. If no API-key fallback exists, exhausted OAuth candidates pass through so the upstream/provider returns the authoritative result.";

const SUBSCRIPTION_ALIVE_REASON: &str = "keep:best_subscription_candidate";
const API_KEY_FALLBACK_REASON: &str = "keep:api_key_subscription_exhausted";
const NO_API_KEY_REASON: &str = "keep:subscription_exhausted_no_api_key";
const NO_SUBSCRIPTION_REASON: &str = "keep:no_subscription_candidates";

const WINDOW_FIVE_HOUR: &str = "5h";
const WINDOW_SEVEN_DAY: &str = "7d";
const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";
const WINDOW_OVERAGE: &str = "overage";
const WINDOW_UNIFIED: &str = "unified";

/// Score-only utilization baseline for candidates whose applicable windows
/// carry no trusted utilization signal. Pinned strictly between fully
/// healthy and lockout so unknown candidates beat near-lockout known
/// candidates but never beat known healthy ones.
const UNKNOWN_UTIL: f64 = 0.5;

/// Utilization at which short-window drain pressure starts rising. Below
/// this threshold a candidate contributes zero drain credit; above it the
/// smoothstep curve raises the credit continuously up to 1.0 at u=1.0.
const DRAIN_START: f64 = 0.9;

/// Weight on Fresh short-window (5h / overage / unified) lockout pressure.
/// Calibrated so that a candidate at u ≈ 0.99 with no other urgency can
/// outscore a candidate at u ≈ 0.2 with no urgency.
const SHORT_DRAIN_WEIGHT: f64 = 1.6;

/// Weight on the joint "fresh near-lockout AND fresh near-reset" signal.
/// Acts as a tie-breaker between drain candidates whose reset windows
/// differ.
const RESET_DRAIN_WEIGHT: f64 = 0.8;

/// Weight on "use it before the window resets and the unused capacity is
/// wasted". Multiplied by squared headroom so credit is concentrated on
/// candidates with meaningful unused capacity rather than nearly-full ones.
const WASTE_WEIGHT: f64 = 1.0;

/// Hard cap on a candidate's total urgency credit. Prevents urgency from
/// completely flattening base-utilization differences when multiple signals
/// stack onto a single window.
const MAX_URGENCY_CREDIT: f64 = 2.0;

/// Reset-urgency horizon for the short 5h window. Resets within the next
/// two hours generate non-zero urgency; resets farther out generate none.
const RESET_HORIZON_5H_SECS: u64 = 2 * 3_600;

/// Reset-urgency horizon for long / weekly / aggregate windows (7d,
/// 7d_sonnet, overage, unified). 24-hour budget captures "use today before
/// reset" without distorting balancing days ahead of the reset.
const RESET_HORIZON_LONG_SECS: u64 = 24 * 3_600;

pub fn metadata() -> PluginMetadata {
    PluginMetadata {
        purpose: PURPOSE.to_owned(),
        keeps: KEEPS.to_owned(),
        drops: DROPS.to_owned(),
        empty_behavior: EMPTY_BEHAVIOR.to_owned(),
        examples: vec![
            "Two alive OAuth A (5h=0.2 reset far) and B (5h=0.95 fresh, resets in 30m): B's lockout pressure + reset urgency flip the choice from A to B."
                .to_owned(),
            "Two alive OAuth with low utilization but one's 5h resets in 5 minutes with large headroom: pick that candidate so the imminently-wasted capacity gets used."
                .to_owned(),
            "OAuth 7d exhausted (or 7d_sonnet exhausted on a sonnet request) and an API-key alternative: keep API-key candidates only."
                .to_owned(),
            "Only API-key candidates: keep API-key candidates.".to_owned(),
        ],
    }
}

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
        let canonical_model = ctx.canonical_model_id.as_str();
        let has_subscription = candidates
            .iter()
            .any(|candidate| candidate.kind == UpstreamKind::AnthropicOauth);
        let has_api_key = candidates
            .iter()
            .any(|candidate| candidate.kind == UpstreamKind::AnthropicApiKey);
        let best_subscription =
            pick_best_subscription_candidate(candidates, canonical_model).map(|c| c.upstream_id);

        let (reason, kept_upstream_ids) = match (best_subscription, has_subscription, has_api_key) {
            (Some(oauth_id), _, _) => (SUBSCRIPTION_ALIVE_REASON, vec![oauth_id]),
            (None, true, true) => (
                API_KEY_FALLBACK_REASON,
                collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            ),
            (None, true, false) => (
                NO_API_KEY_REASON,
                collect_kind(candidates, UpstreamKind::AnthropicOauth),
            ),
            (None, false, _) => (
                NO_SUBSCRIPTION_REASON,
                collect_kind(candidates, UpstreamKind::AnthropicApiKey),
            ),
        };

        Ok(FilterOutput {
            kept_upstream_ids,
            reason: reason.to_owned(),
            per_candidate_reasons: Vec::new(),
        })
    }

    fn plugin_id(&self) -> Uuid {
        BUILTIN_SUBSCRIPTION_PREFERENCE_ID
    }

    fn plugin_name(&self) -> &str {
        BUILTIN_SUBSCRIPTION_PREFERENCE_NAME
    }
}

fn collect_kind(candidates: &[UpstreamCandidate], kind: UpstreamKind) -> Vec<Uuid> {
    candidates
        .iter()
        .filter(|candidate| candidate.kind == kind)
        .map(|candidate| candidate.upstream_id)
        .collect()
}

/// Among alive OAuth candidates, pick the one with the lowest score, with a
/// deterministic total-order tie-break chain that always yields exactly one
/// winner.
fn pick_best_subscription_candidate<'a>(
    candidates: &'a [UpstreamCandidate],
    canonical_model: &str,
) -> Option<&'a UpstreamCandidate> {
    candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| is_alive_subscription_candidate(candidate, canonical_model))
        .map(|(index, candidate)| {
            (
                candidate,
                score_candidate(candidate, canonical_model, index),
            )
        })
        .min_by(|left, right| compare_score_records(&left.1, &right.1))
        .map(|(candidate, _)| candidate)
}

#[derive(Clone, Copy, Debug)]
struct ScoreRecord {
    score: f64,
    has_trusted_signal: bool,
    freshness_secs: u64,
    u_max: f64,
    upstream_id: Uuid,
    original_index: usize,
}

fn compare_score_records(left: &ScoreRecord, right: &ScoreRecord) -> Ordering {
    left.score
        .total_cmp(&right.score)
        .then_with(|| right.has_trusted_signal.cmp(&left.has_trusted_signal))
        .then_with(|| right.freshness_secs.cmp(&left.freshness_secs))
        .then_with(|| left.u_max.total_cmp(&right.u_max))
        .then_with(|| left.upstream_id.cmp(&right.upstream_id))
        .then_with(|| left.original_index.cmp(&right.original_index))
}

fn score_candidate(
    candidate: &UpstreamCandidate,
    canonical_model: &str,
    original_index: usize,
) -> ScoreRecord {
    let mut u_max_trusted: Option<f64> = None;
    let mut max_credit: f64 = 0.0;
    let mut freshness_secs = candidate.observed_at_unix_secs;

    for snapshot in &candidate.subscription_quotas {
        if !snapshot_applies_to_model(&snapshot.window, canonical_model) {
            continue;
        }
        if let Some(snapshot_secs) = snapshot
            .observed_at_unix_millis
            .map(|millis| millis / 1_000)
            .filter(|secs| *secs > 0)
        {
            freshness_secs = freshness_secs.max(snapshot_secs);
        }

        let snap_now = estimated_now_secs(candidate, snapshot);
        let Some(u) = normalize_utilization_for_scoring(snapshot.utilization) else {
            continue;
        };
        if !scoring_signal_trusted(snapshot, snap_now) {
            continue;
        }

        u_max_trusted = Some(match u_max_trusted {
            Some(current) => current.max(u),
            None => u,
        });

        if snapshot.state == SubscriptionQuotaDataState::Fresh {
            let credit = window_urgency_credit(snapshot, u, snap_now);
            if credit > max_credit {
                max_credit = credit;
            }
        }
    }

    let (u_max, has_trusted_signal) = match u_max_trusted {
        Some(value) => (value, true),
        None => (UNKNOWN_UTIL, false),
    };
    let urgency = max_credit.min(MAX_URGENCY_CREDIT);
    let score = u_max - urgency;

    ScoreRecord {
        score,
        has_trusted_signal,
        freshness_secs,
        u_max,
        upstream_id: candidate.upstream_id,
        original_index,
    }
}

fn normalize_utilization_for_scoring(value: Option<f64>) -> Option<f64> {
    let u = value?;
    if !u.is_finite() || !(0.0..=1.0).contains(&u) {
        return None;
    }
    Some(u)
}

fn scoring_signal_trusted(snapshot: &SubscriptionQuotaCandidateSnapshot, now_secs: u64) -> bool {
    match snapshot.state {
        SubscriptionQuotaDataState::Fresh => true,
        SubscriptionQuotaDataState::Stale => {
            if !is_long_window(&snapshot.window) {
                return false;
            }
            match snapshot.resets_at_unix_secs {
                Some(resets_at) => resets_at > now_secs,
                None => true,
            }
        }
        SubscriptionQuotaDataState::Missing => false,
    }
}

fn window_urgency_credit(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    u: f64,
    now_secs: u64,
) -> f64 {
    let headroom = 1.0 - u;
    let r = reset_urgency(snapshot, now_secs);
    let lockout = lockout_pressure(u);
    let short_drain = match snapshot.window.as_str() {
        WINDOW_FIVE_HOUR | WINDOW_OVERAGE | WINDOW_UNIFIED => SHORT_DRAIN_WEIGHT * lockout,
        _ => 0.0,
    };
    let reset_drain = RESET_DRAIN_WEIGHT * r * lockout;
    let waste = WASTE_WEIGHT * r * headroom * headroom;
    short_drain + reset_drain + waste
}

/// Smoothstep curve `x² (3 - 2x)` clamped to `[0, 1]`. Provides C¹ continuity
/// between zero and full urgency credit instead of a hard step boundary.
fn smoothstep(x: f64) -> f64 {
    let x = x.clamp(0.0, 1.0);
    x * x * (3.0 - 2.0 * x)
}

/// Zero below `DRAIN_START`, smoothly rises to 1 by `u = 1.0`. Encodes the
/// "this window is about to lock out, drain it" preference.
fn lockout_pressure(u: f64) -> f64 {
    if u <= DRAIN_START {
        return 0.0;
    }
    if u >= 1.0 {
        return 1.0;
    }
    let x = (u - DRAIN_START) / (1.0 - DRAIN_START);
    smoothstep(x)
}

fn reset_horizon_secs(window: &str) -> u64 {
    match window {
        WINDOW_FIVE_HOUR => RESET_HORIZON_5H_SECS,
        _ => RESET_HORIZON_LONG_SECS,
    }
}

fn reset_urgency(snapshot: &SubscriptionQuotaCandidateSnapshot, now_secs: u64) -> f64 {
    let Some(resets_at) = snapshot.resets_at_unix_secs else {
        return 0.0;
    };
    if resets_at <= now_secs {
        return 0.0;
    }
    let tau = resets_at - now_secs;
    let horizon = reset_horizon_secs(&snapshot.window);
    if tau >= horizon {
        return 0.0;
    }
    let x = 1.0 - (tau as f64) / (horizon as f64);
    smoothstep(x)
}

fn is_alive_subscription_candidate(candidate: &UpstreamCandidate, canonical_model: &str) -> bool {
    candidate.kind == UpstreamKind::AnthropicOauth
        && !subscription_quota_exhausted(candidate, canonical_model)
}

fn subscription_quota_exhausted(candidate: &UpstreamCandidate, canonical_model: &str) -> bool {
    candidate.subscription_quotas.iter().any(|snapshot| {
        snapshot_applies_to_model(&snapshot.window, canonical_model)
            && snapshot_is_exhausted(snapshot, estimated_now_secs(candidate, snapshot))
    })
}

/// Whether the given window's signal applies to the current request model.
///
/// The 5h, 7d, overage, and unified windows are model-agnostic shared
/// counters; their exhaustion or utilization affects every request. The
/// 7d_sonnet window is model-scoped to sonnet traffic. The 7d_opus window
/// is currently shipped as a label with no real quota attached upstream
/// — opus traffic falls back to the shared 7d counter — so it is ignored.
fn snapshot_applies_to_model(window: &str, canonical_model: &str) -> bool {
    match window {
        WINDOW_FIVE_HOUR | WINDOW_SEVEN_DAY | WINDOW_OVERAGE | WINDOW_UNIFIED => true,
        WINDOW_SEVEN_DAY_SONNET => model_is_sonnet(canonical_model),
        WINDOW_SEVEN_DAY_OPUS => false,
        _ => false,
    }
}

fn model_is_sonnet(canonical_model: &str) -> bool {
    canonical_model.contains("sonnet")
}

fn is_long_window(window: &str) -> bool {
    matches!(window, WINDOW_SEVEN_DAY | WINDOW_SEVEN_DAY_SONNET)
}

/// Best-effort current-time estimate used to invalidate `resets_at` gates
/// and rank candidate freshness. Both inputs are lower bounds on the true
/// wall clock, so taking the max minimises the window in which a
/// long-since-reset quota would still look exhausted.
fn estimated_now_secs(
    candidate: &UpstreamCandidate,
    snapshot: &SubscriptionQuotaCandidateSnapshot,
) -> u64 {
    let snapshot_secs = snapshot
        .observed_at_unix_millis
        .map(|millis| millis / 1_000)
        .unwrap_or(0);
    candidate.observed_at_unix_secs.max(snapshot_secs)
}

fn snapshot_is_exhausted(snapshot: &SubscriptionQuotaCandidateSnapshot, now_secs: u64) -> bool {
    if snapshot.state == SubscriptionQuotaDataState::Missing {
        return false;
    }
    if !is_long_window(&snapshot.window) && snapshot.state != SubscriptionQuotaDataState::Fresh {
        return false;
    }
    if now_secs > 0
        && let Some(resets_at) = snapshot.resets_at_unix_secs
        && resets_at <= now_secs
    {
        return false;
    }

    snapshot.disabled_reason.is_some()
        || snapshot
            .status
            .as_deref()
            .is_some_and(|status| matches!(status, "rejected" | "exceeded" | "exceeded_overage"))
        || snapshot
            .utilization
            .is_some_and(|utilization| utilization >= 1.0)
}

#[cfg(test)]
mod tests;
