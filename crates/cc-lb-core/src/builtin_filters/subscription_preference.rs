use cc_lb_plugin_api::{
    BUILTIN_SUBSCRIPTION_PREFERENCE_ID, BUILTIN_SUBSCRIPTION_PREFERENCE_NAME, FilterError,
    FilterOutput, FilterPlugin, Principal, RequestContext, SubscriptionQuotaCandidateSnapshot,
    SubscriptionQuotaDataState, UpstreamCandidate, UpstreamKind,
};
use cc_lb_storage_api::PluginMetadata;
use uuid::Uuid;

pub const PURPOSE: &str = "Prefer subscription/OAuth upstreams while quota appears alive; use API-key upstreams only when subscription candidates are exhausted.";
pub const KEEPS: &str = "OAuth candidates whose subscription quota is not clearly exhausted, or API-key candidates when every OAuth candidate is exhausted.";
pub const DROPS: &str = "API-key candidates while at least one OAuth candidate appears alive; exhausted OAuth candidates when API-key fallback is available.";
pub const EMPTY_BEHAVIOR: &str = "Never drops everything. If no API-key fallback exists, exhausted OAuth candidates pass through so the upstream/provider returns the authoritative result.";

const SUBSCRIPTION_ALIVE_REASON: &str = "keep:subscription_quota_alive";
const API_KEY_FALLBACK_REASON: &str = "keep:api_key_subscription_exhausted";
const NO_API_KEY_REASON: &str = "keep:subscription_exhausted_no_api_key";
const NO_SUBSCRIPTION_REASON: &str = "keep:no_subscription_candidates";

pub fn metadata() -> PluginMetadata {
    PluginMetadata {
        purpose: PURPOSE.to_owned(),
        keeps: KEEPS.to_owned(),
        drops: DROPS.to_owned(),
        empty_behavior: EMPTY_BEHAVIOR.to_owned(),
        examples: vec![
            "OAuth and API-key candidates, OAuth quota alive → keep OAuth candidates only."
                .to_owned(),
            "OAuth and API-key candidates, every OAuth quota exhausted → keep API-key candidates only."
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
        _ctx: &RequestContext,
        _principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError> {
        let has_subscription = candidates
            .iter()
            .any(|candidate| candidate.kind == UpstreamKind::AnthropicOauth);
        let has_alive_subscription = candidates.iter().any(is_alive_subscription_candidate);
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
                should_keep_candidate(candidate, has_alive_subscription, has_api_key)
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
    has_alive_subscription: bool,
    has_api_key: bool,
) -> bool {
    match candidate.kind {
        UpstreamKind::AnthropicOauth => {
            has_alive_subscription && is_alive_subscription_candidate(candidate) || !has_api_key
        }
        UpstreamKind::AnthropicApiKey => !has_alive_subscription,
    }
}

fn is_alive_subscription_candidate(candidate: &UpstreamCandidate) -> bool {
    candidate.kind == UpstreamKind::AnthropicOauth && !subscription_quota_exhausted(candidate)
}

fn subscription_quota_exhausted(candidate: &UpstreamCandidate) -> bool {
    candidate
        .subscription_quotas
        .iter()
        .any(snapshot_is_exhausted)
}

fn snapshot_is_exhausted(snapshot: &SubscriptionQuotaCandidateSnapshot) -> bool {
    snapshot.state == SubscriptionQuotaDataState::Fresh
        && (snapshot.disabled_reason.is_some()
            || snapshot.status.as_deref().is_some_and(|status| {
                matches!(status, "rejected" | "exceeded" | "exceeded_overage")
            })
            || snapshot
                .utilization
                .is_some_and(|utilization| utilization >= 1.0))
}

#[cfg(test)]
mod tests;
