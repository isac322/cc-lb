use cc_lb_storage_api::{
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
};
use http::HeaderMap;
use uuid::Uuid;

use crate::rate_limit_headers::{UnifiedQuotaObservation, parse_anthropic_unified_headers};

pub fn build_subscription_quota_samples(
    headers: &HeaderMap,
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
) -> Vec<SubscriptionQuotaSample> {
    parse_anthropic_unified_headers(headers)
        .into_iter()
        .map(|observation| {
            unified_observation_to_sample(upstream_id, observation, observed_at_unix_millis)
        })
        .collect()
}

pub fn unified_observation_to_sample(
    upstream_id: Uuid,
    observation: UnifiedQuotaObservation,
    observed_at_unix_millis: u64,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window: observation.window,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization: observation.utilization,
        status: observation.status,
        resets_at_unix_secs: observation.resets_at_unix_secs,
        surpassed_threshold: observation.surpassed_threshold,
        representative_claim: observation.representative_claim,
        fallback_percentage: observation.fallback_percentage,
        fallback_available: observation.fallback_available,
        overage_in_use: observation.overage_in_use,
        overage_period_monthly_utilization: observation.overage_period_monthly_utilization,
        upgrade_paths: observation.upgrade_paths,
        disabled_reason: observation.disabled_reason,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}
