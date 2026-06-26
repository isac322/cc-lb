use bytes::Bytes;
use cc_lb_plugin_api::{PrincipalKind, SubscriptionQuotaDataState};
use http::Method;

use super::*;

#[test]
fn keeps_subscription_when_quota_appears_alive() {
    let oauth = oauth_candidate("oauth", quota("allowed", Some(0.7), None));
    let api_key = api_key_candidate("api-key");
    let output = filter(&[oauth.clone(), api_key]);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_api_key_when_subscription_quota_is_exhausted() {
    let oauth = oauth_candidate("oauth", quota("rejected", Some(1.0), None));
    let api_key = api_key_candidate("api-key");
    let output = filter(&[oauth, api_key.clone()]);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_api_key_when_one_host_window_is_exhausted_and_others_missing() {
    let oauth = UpstreamCandidate {
        subscription_quotas: vec![
            quota("rejected", Some(1.0), None),
            missing_quota("7d"),
            missing_quota("overage"),
        ],
        ..oauth_candidate_without_quota("oauth")
    };
    let api_key = api_key_candidate("api-key");
    let output = filter(&[oauth, api_key.clone()]);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn treats_unknown_subscription_quota_as_alive() {
    let oauth = oauth_candidate_without_quota("oauth");
    let api_key = api_key_candidate("api-key");
    let output = filter(&[oauth.clone(), api_key]);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_exhausted_subscription_when_no_api_key_exists() {
    let oauth = oauth_candidate(
        "oauth",
        quota("rejected", Some(1.0), Some("quota exhausted")),
    );
    let output = filter(std::slice::from_ref(&oauth));

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, NO_API_KEY_REASON);
}

#[test]
fn keeps_api_keys_when_no_subscription_candidates_exist() {
    let first = api_key_candidate("first");
    let second = api_key_candidate("second");
    let output = filter(&[first.clone(), second.clone()]);

    assert_eq!(
        output.kept_upstream_ids,
        vec![first.upstream_id, second.upstream_id]
    );
    assert_eq!(output.reason, NO_SUBSCRIPTION_REASON);
}

fn filter(candidates: &[UpstreamCandidate]) -> FilterOutput {
    SubscriptionPreferenceFilter::new()
        .filter(&ctx(), &principal(), candidates)
        .expect("builtin filter cannot fail")
}

fn ctx() -> RequestContext {
    RequestContext {
        request_id: "req".to_owned(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: "claude".to_owned(),
    }
}

fn principal() -> Principal {
    Principal {
        id: "principal".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::Map::new(),
    }
}

fn oauth_candidate(name: &str, quota: SubscriptionQuotaCandidateSnapshot) -> UpstreamCandidate {
    UpstreamCandidate {
        subscription_quotas: vec![quota],
        ..oauth_candidate_without_quota(name)
    }
}

fn oauth_candidate_without_quota(name: &str) -> UpstreamCandidate {
    UpstreamCandidate {
        kind: UpstreamKind::AnthropicOauth,
        ..candidate(name)
    }
}

fn api_key_candidate(name: &str) -> UpstreamCandidate {
    UpstreamCandidate {
        kind: UpstreamKind::AnthropicApiKey,
        ..candidate(name)
    }
}

fn candidate(name: &str) -> UpstreamCandidate {
    UpstreamCandidate {
        upstream_id: Uuid::new_v4(),
        name: name.to_owned(),
        kind: UpstreamKind::AnthropicApiKey,
        observed_rate_limits: Vec::new(),
        subscription_quotas: Vec::new(),
        observed_at_unix_secs: 0,
        cache_score: None,
        base_url: None,
    }
}

fn quota(
    status: &str,
    utilization: Option<f64>,
    disabled_reason: Option<&str>,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: "5h".to_owned(),
        state: SubscriptionQuotaDataState::Fresh,
        source: Some("header".to_owned()),
        utilization,
        status: Some(status.to_owned()),
        resets_at_unix_secs: None,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: disabled_reason.map(str::to_owned),
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: Some(0),
        max_staleness_secs: 60,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}

fn missing_quota(window: &str) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
        state: SubscriptionQuotaDataState::Missing,
        source: None,
        utilization: None,
        status: None,
        resets_at_unix_secs: None,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis: None,
        max_staleness_secs: 60,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}
