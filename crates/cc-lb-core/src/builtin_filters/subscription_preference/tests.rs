use bytes::Bytes;
use cc_lb_plugin_api::{PrincipalKind, SubscriptionQuotaDataState};
use http::Method;

use super::*;

const SONNET_MODEL: &str = "claude-sonnet-4-5-20250929";
const OPUS_MODEL: &str = "claude-opus-4-8-20250514";
const HAIKU_MODEL: &str = "claude-haiku-4-5-20251001";

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

// --- 7d / 7d_sonnet / 7d_opus coverage --------------------------------------

#[test]
fn keeps_api_key_when_seven_day_window_is_fresh_exhausted_even_if_five_hour_is_alive() {
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.3), None, None),
            fresh_window_quota("7d", "rejected", Some(1.0), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth, api_key.clone()], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_api_key_when_seven_day_window_is_stale_exhausted() {
    // This is the production bug: 7d quota was last observed exhausted but
    // the metadata cache is now past its freshness window. Without the fix
    // the filter would treat the candidate as alive because only the fresh
    // 5h window is inspected, immediately drawing a 429 on routing.
    let observed_at_millis = 1_000_000_000_u64 * 1_000;
    let resets_at_secs = observed_at_millis / 1_000 + 3 * 24 * 3_600; // 3 days in the future
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.2), None, Some(observed_at_millis)),
            stale_window_quota(
                "7d",
                Some("rejected"),
                Some(1.0),
                None,
                Some(resets_at_secs),
                Some(observed_at_millis),
            ),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth, api_key.clone()], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn ignores_stale_seven_day_exhaustion_when_reset_has_already_passed() {
    // The cached exhaustion is from a previous cycle; the window has since
    // reset, so the candidate should be routable again.
    let observed_at_millis = 1_000_000_000_u64 * 1_000;
    let candidate_observed_secs = observed_at_millis / 1_000 + 10 * 24 * 3_600; // 10 days later
    let resets_at_secs = observed_at_millis / 1_000 + 5 * 24 * 3_600; // 5 days after observation
    let oauth = UpstreamCandidate {
        observed_at_unix_secs: candidate_observed_secs,
        ..oauth_candidate_with_quotas(
            "oauth",
            vec![
                fresh_window_quota("5h", "allowed", Some(0.2), None, Some(observed_at_millis)),
                stale_window_quota(
                    "7d",
                    Some("rejected"),
                    Some(1.0),
                    None,
                    Some(resets_at_secs),
                    Some(observed_at_millis),
                ),
            ],
        )
    };
    let api_key = api_key_candidate("api-key");
    let oauth_id = oauth.upstream_id;
    let output = filter_for_model(&[oauth, api_key], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_api_key_when_seven_day_sonnet_window_is_exhausted_for_sonnet_request() {
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.3), None, None),
            fresh_window_quota("7d", "allowed", Some(0.4), None, None),
            fresh_window_quota("7d_sonnet", "exceeded", Some(1.0), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth, api_key.clone()], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_subscription_when_seven_day_sonnet_is_exhausted_for_opus_request() {
    // Opus traffic should not be punished for sonnet's per-model quota.
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.3), None, None),
            fresh_window_quota("7d", "allowed", Some(0.4), None, None),
            fresh_window_quota("7d_sonnet", "exceeded", Some(1.0), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth.clone(), api_key], OPUS_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn ignores_seven_day_opus_window_even_for_opus_request() {
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.3), None, None),
            fresh_window_quota("7d", "allowed", Some(0.4), None, None),
            fresh_window_quota("7d_opus", "exceeded", Some(1.0), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth.clone(), api_key], OPUS_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn keeps_api_key_when_shared_seven_day_is_exhausted_for_opus_request() {
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.3), None, None),
            fresh_window_quota("7d", "rejected", Some(1.0), None, None),
            fresh_window_quota("7d_opus", "allowed", Some(0.1), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth, api_key.clone()], OPUS_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![api_key.upstream_id]);
    assert_eq!(output.reason, API_KEY_FALLBACK_REASON);
}

#[test]
fn keeps_subscription_when_only_seven_day_sonnet_is_exhausted_for_haiku_request() {
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.3), None, None),
            fresh_window_quota("7d", "allowed", Some(0.4), None, None),
            fresh_window_quota("7d_sonnet", "exceeded", Some(1.0), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth.clone(), api_key], HAIKU_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn ignores_unknown_window_labels() {
    let oauth = oauth_candidate_with_quotas(
        "oauth",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.3), None, None),
            fresh_window_quota("unknown_made_up_window", "rejected", Some(1.0), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(&[oauth.clone(), api_key], SONNET_MODEL);

    assert_eq!(output.kept_upstream_ids, vec![oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

#[test]
fn five_hour_and_shared_seven_day_exhaustion_gates_routing_for_every_model() {
    for model in [SONNET_MODEL, OPUS_MODEL, HAIKU_MODEL] {
        for exhausted_window in ["5h", "7d"] {
            let oauth = oauth_candidate_with_quotas(
                "oauth",
                vec![fresh_window_quota(
                    exhausted_window,
                    "rejected",
                    Some(1.0),
                    None,
                    None,
                )],
            );
            let api_key = api_key_candidate("api-key");
            let output = filter_for_model(&[oauth, api_key.clone()], model);

            assert_eq!(
                output.kept_upstream_ids,
                vec![api_key.upstream_id],
                "model={model} exhausted_window={exhausted_window} should drop OAuth"
            );
            assert_eq!(
                output.reason, API_KEY_FALLBACK_REASON,
                "model={model} exhausted_window={exhausted_window} expected API key fallback reason"
            );
        }
    }
}

#[test]
fn pre_filters_seven_day_exhaustion_independently_per_candidate() {
    // Two OAuth upstreams: one has 7d exhausted, the other is fully alive.
    // The filter should keep the alive OAuth and drop the exhausted one.
    let exhausted_oauth = oauth_candidate_with_quotas(
        "oauth-exhausted",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.2), None, None),
            fresh_window_quota("7d", "rejected", Some(1.0), None, None),
        ],
    );
    let alive_oauth = oauth_candidate_with_quotas(
        "oauth-alive",
        vec![
            fresh_window_quota("5h", "allowed", Some(0.2), None, None),
            fresh_window_quota("7d", "allowed", Some(0.5), None, None),
        ],
    );
    let api_key = api_key_candidate("api-key");
    let output = filter_for_model(
        &[exhausted_oauth, alive_oauth.clone(), api_key],
        SONNET_MODEL,
    );

    assert_eq!(output.kept_upstream_ids, vec![alive_oauth.upstream_id]);
    assert_eq!(output.reason, SUBSCRIPTION_ALIVE_REASON);
}

// --- helpers ---------------------------------------------------------------

fn filter(candidates: &[UpstreamCandidate]) -> FilterOutput {
    filter_for_model(candidates, "claude")
}

fn filter_for_model(candidates: &[UpstreamCandidate], canonical_model: &str) -> FilterOutput {
    SubscriptionPreferenceFilter::new()
        .filter(&ctx(canonical_model), &principal(), candidates)
        .expect("builtin filter cannot fail")
}

fn ctx(canonical_model: &str) -> RequestContext {
    RequestContext {
        request_id: "req".to_owned(),
        downstream_headers: http::HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        cache_breakpoints: Vec::new(),
        canonical_model_id: canonical_model.to_owned(),
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

fn oauth_candidate_with_quotas(
    name: &str,
    quotas: Vec<SubscriptionQuotaCandidateSnapshot>,
) -> UpstreamCandidate {
    UpstreamCandidate {
        subscription_quotas: quotas,
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
    fresh_window_quota("5h", status, utilization, disabled_reason, None)
}

fn fresh_window_quota(
    window: &str,
    status: &str,
    utilization: Option<f64>,
    disabled_reason: Option<&str>,
    observed_at_unix_millis: Option<u64>,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
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
        observed_at_unix_millis: observed_at_unix_millis.or(Some(0)),
        max_staleness_secs: 60,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
    }
}

fn stale_window_quota(
    window: &str,
    status: Option<&str>,
    utilization: Option<f64>,
    disabled_reason: Option<&str>,
    resets_at_unix_secs: Option<u64>,
    observed_at_unix_millis: Option<u64>,
) -> SubscriptionQuotaCandidateSnapshot {
    SubscriptionQuotaCandidateSnapshot {
        window: window.to_owned(),
        state: SubscriptionQuotaDataState::Stale,
        source: Some("header".to_owned()),
        utilization,
        status: status.map(str::to_owned),
        resets_at_unix_secs,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: disabled_reason.map(str::to_owned),
        extra_usage_enabled: None,
        extra_usage_monthly_limit: None,
        extra_usage_used_credits: None,
        observed_at_unix_millis,
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
