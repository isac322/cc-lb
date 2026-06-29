use cc_lb_plugin_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, FilterError,
    FilterOutput, FilterPlugin, Principal, RequestContext, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, UpstreamCandidate, UpstreamKind,
};
use cc_lb_storage_api::PluginMetadata;
use uuid::Uuid;

pub const PURPOSE: &str = "Prefer subscription/OAuth upstreams while quota appears alive; use API-key upstreams only when subscription candidates are exhausted on any quota window that applies to the request model.";
pub const KEEPS: &str = "OAuth candidates whose 5h, 7d, 7d_sonnet (sonnet requests only), overage, and unified subscription quotas are not exhausted; or API-key candidates when every OAuth candidate is exhausted.";
pub const DROPS: &str = "API-key candidates while at least one OAuth candidate appears alive; OAuth candidates whose subscription quota is exhausted on any window that applies to the request model when API-key fallback is available.";
pub const EMPTY_BEHAVIOR: &str = "Never drops everything. If no API-key fallback exists, exhausted OAuth candidates pass through so the upstream/provider returns the authoritative result.";

const SUBSCRIPTION_ALIVE_REASON: &str = "keep:subscription_quota_alive";
const API_KEY_FALLBACK_REASON: &str = "keep:api_key_subscription_exhausted";
const NO_API_KEY_REASON: &str = "keep:subscription_exhausted_no_api_key";
const NO_SUBSCRIPTION_REASON: &str = "keep:no_subscription_candidates";

const WINDOW_FIVE_HOUR: &str = "5h";
const WINDOW_SEVEN_DAY: &str = "7d";
const WINDOW_SEVEN_DAY_SONNET: &str = "7d_sonnet";
const WINDOW_SEVEN_DAY_OPUS: &str = "7d_opus";
const WINDOW_OVERAGE: &str = "overage";
const WINDOW_UNIFIED: &str = "unified";

pub fn metadata() -> PluginMetadata {
    PluginMetadata {
        purpose: PURPOSE.to_owned(),
        keeps: KEEPS.to_owned(),
        drops: DROPS.to_owned(),
        empty_behavior: EMPTY_BEHAVIOR.to_owned(),
        examples: vec![
            "OAuth and API-key candidates, OAuth 5h/7d quotas alive → keep OAuth candidates only."
                .to_owned(),
            "OAuth and API-key candidates, OAuth 7d exhausted (or 7d_sonnet exhausted on a sonnet request) → keep API-key candidates only."
                .to_owned(),
            "OAuth and API-key candidates, every OAuth 5h quota exhausted → keep API-key candidates only."
                .to_owned(),
            "Only API-key candidates → keep API-key candidates.".to_owned(),
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
        let has_alive_subscription = candidates
            .iter()
            .any(|candidate| is_alive_subscription_candidate(candidate, canonical_model));
        let has_api_key = candidates
            .iter()
            .any(|candidate| candidate.kind == UpstreamKind::AnthropicApiKey);

        let reason = if has_alive_subscription {
            SUBSCRIPTION_ALIVE_REASON
        } else if has_subscription && has_api_key {
            API_KEY_FALLBACK_REASON
        } else if has_subscription {
            NO_API_KEY_REASON
        } else {
            NO_SUBSCRIPTION_REASON
        };
        let kept_upstream_ids = candidates
            .iter()
            .filter(|candidate| {
                should_keep_candidate(
                    candidate,
                    canonical_model,
                    has_alive_subscription,
                    has_api_key,
                )
            })
            .map(|candidate| candidate.upstream_id)
            .collect::<Vec<_>>();

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

fn should_keep_candidate(
    candidate: &UpstreamCandidate,
    canonical_model: &str,
    has_alive_subscription: bool,
    has_api_key: bool,
) -> bool {
    match candidate.kind {
        UpstreamKind::AnthropicOauth => {
            has_alive_subscription && is_alive_subscription_candidate(candidate, canonical_model)
                || !has_api_key
        }
        UpstreamKind::AnthropicApiKey => !has_alive_subscription,
    }
}

fn is_alive_subscription_candidate(
    candidate: &UpstreamCandidate,
    canonical_model: &str,
) -> bool {
    candidate.kind == UpstreamKind::AnthropicOauth
        && !subscription_quota_exhausted(candidate, canonical_model)
}

fn subscription_quota_exhausted(
    candidate: &UpstreamCandidate,
    canonical_model: &str,
) -> bool {
    candidate.subscription_quotas.iter().any(|snapshot| {
        snapshot_applies_to_model(&snapshot.window, canonical_model)
            && snapshot_is_exhausted(snapshot, estimated_now_secs(candidate, snapshot))
    })
}

/// Whether the given window's exhaustion signal should affect routing for the
/// current request model.
///
/// The 5h, 7d, overage, and unified windows are model-agnostic: a single
/// counter is shared across every model and so exhaustion on any of them
/// blocks every request. The 7d_sonnet window is model-scoped and only
/// constrains sonnet traffic. The 7d_opus window is currently shipped as a
/// label with no real quota attached upstream, so it is intentionally
/// ignored — opus traffic falls back to the shared 7d counter.
fn snapshot_applies_to_model(window: &str, canonical_model: &str) -> bool {
    match window {
        WINDOW_FIVE_HOUR | WINDOW_SEVEN_DAY | WINDOW_OVERAGE | WINDOW_UNIFIED => true,
        WINDOW_SEVEN_DAY_SONNET => model_is_sonnet(canonical_model),
        // WINDOW_SEVEN_DAY_OPUS: upstream does not populate this window with
        // a real quota; opus models rely on the shared 7d counter instead.
        WINDOW_SEVEN_DAY_OPUS => false,
        // Unknown windows: ignore to avoid surprise filtering.
        _ => false,
    }
}

fn model_is_sonnet(canonical_model: &str) -> bool {
    canonical_model.contains("sonnet")
}

fn is_long_window(window: &str) -> bool {
    matches!(window, WINDOW_SEVEN_DAY | WINDOW_SEVEN_DAY_SONNET)
}

/// Best-effort current-time estimate used to invalidate `resets_at` gates.
/// Both `candidate.observed_at_unix_secs` and `snapshot.observed_at_unix_millis`
/// are LOWER bounds on the true wall clock, so taking the max minimises the
/// window in which a long-since-reset quota would still look exhausted.
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

fn snapshot_is_exhausted(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    now_secs: u64,
) -> bool {
    // Missing data carries no usable signal.
    if snapshot.state == SubscriptionQuotaDataState::Missing {
        return false;
    }

    // Short-lived windows (5h, overage, unified) churn quickly: stale data
    // is unreliable and is treated as alive to avoid false negatives.
    // Long-lived 7d-class windows reset only weekly, so the last observed
    // exhaustion remains the best available signal even after the metadata
    // cache freshness window has expired. This is the main fix: without it,
    // a candidate whose 7d quota is exhausted but whose 5h quota is fresh
    // and healthy would still be routed to, immediately drawing a 429.
    if !is_long_window(&snapshot.window) && snapshot.state != SubscriptionQuotaDataState::Fresh {
        return false;
    }

    // If the provider-reported reset time has demonstrably passed relative to
    // the best `now` estimate we have, the recorded exhaustion has expired and
    // we cannot rely on it any longer.
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
