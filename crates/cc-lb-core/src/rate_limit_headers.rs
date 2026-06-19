use std::collections::BTreeMap;

use cc_lb_plugin_api::{Principal, RateLimitKind, RateLimitObservation};
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};
use http::HeaderMap;
use serde_json::Value;

const HEADER_PREFIX: &str = "anthropic-ratelimit-";
const DEFAULT_WINDOW: &str = "default";

#[derive(Clone, Debug, PartialEq)]
pub struct UnifiedQuotaObservation {
    pub window: SubscriptionQuotaWindow,
    pub utilization: Option<f64>,
    pub status: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs: Option<u64>,
    pub surpassed_threshold: Option<f64>,
    pub representative_claim: Option<String>,
    pub fallback_percentage: Option<f64>,
    pub fallback_available: Option<bool>,
    pub overage_in_use: Option<bool>,
    pub overage_period_monthly_utilization: Option<f64>,
    pub upgrade_paths: Option<Vec<String>>,
    pub disabled_reason: Option<String>,
}

impl Default for UnifiedQuotaObservation {
    fn default() -> Self {
        Self {
            window: SubscriptionQuotaWindow::FiveHour,
            utilization: None,
            status: None,
            resets_at_unix_secs: None,
            surpassed_threshold: None,
            representative_claim: None,
            fallback_percentage: None,
            fallback_available: None,
            overage_in_use: None,
            overage_period_monthly_utilization: None,
            upgrade_paths: None,
            disabled_reason: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LimitIdentity {
    Account(String),
    Credential(String),
    Unobserved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RateLimitField {
    Limit,
    Remaining,
    Reset,
}

#[derive(Default)]
struct PartialSnapshot {
    limit: Option<u64>,
    remaining: Option<u64>,
    reset: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnifiedQuotaField {
    Utilization,
    Reset,
    Status,
    SurpassedThreshold,
    RepresentativeClaim,
    FallbackPercentage,
    FallbackAvailable,
    OverageInUse,
    OverageMonthlyUtilization,
    UpgradePaths,
    DisabledReason,
}

#[derive(Default)]
struct PartialUnifiedQuotaObservation {
    utilization: Option<f64>,
    status: Option<SubscriptionQuotaStatus>,
    resets_at_unix_secs: Option<u64>,
    surpassed_threshold: Option<f64>,
    representative_claim: Option<String>,
    fallback_percentage: Option<f64>,
    fallback_available: Option<bool>,
    overage_in_use: Option<bool>,
    overage_period_monthly_utilization: Option<f64>,
    upgrade_paths: Option<Vec<String>>,
    disabled_reason: Option<String>,
}

impl PartialUnifiedQuotaObservation {
    fn into_observation(self, window: SubscriptionQuotaWindow) -> Option<UnifiedQuotaObservation> {
        if self.utilization.is_none()
            && self.status.is_none()
            && self.resets_at_unix_secs.is_none()
            && self.surpassed_threshold.is_none()
            && self.representative_claim.is_none()
            && self.fallback_percentage.is_none()
            && self.fallback_available.is_none()
            && self.overage_in_use.is_none()
            && self.overage_period_monthly_utilization.is_none()
            && self.upgrade_paths.is_none()
            && self.disabled_reason.is_none()
        {
            return None;
        }

        Some(UnifiedQuotaObservation {
            window,
            utilization: self.utilization,
            status: self.status,
            resets_at_unix_secs: self.resets_at_unix_secs,
            surpassed_threshold: self.surpassed_threshold,
            representative_claim: self.representative_claim,
            fallback_percentage: self.fallback_percentage,
            fallback_available: self.fallback_available,
            overage_in_use: self.overage_in_use,
            overage_period_monthly_utilization: self.overage_period_monthly_utilization,
            upgrade_paths: self.upgrade_paths,
            disabled_reason: self.disabled_reason,
        })
    }
}

impl PartialSnapshot {
    fn into_snapshot(self, kind: RateLimitKind, window: String) -> Option<RateLimitObservation> {
        if self.limit.is_none() && self.remaining.is_none() && self.reset.is_none() {
            return None;
        }

        Some(RateLimitObservation {
            kind,
            window,
            limit: self.limit,
            remaining: self.remaining,
            reset: self.reset,
        })
    }
}

pub fn parse_anthropic_rate_limit_headers(headers: &HeaderMap) -> Vec<RateLimitObservation> {
    let mut snapshots = BTreeMap::<(u8, String), (RateLimitKind, PartialSnapshot)>::new();

    for (name, value) in headers {
        let Some((kind, field, window)) = parse_header_name(name.as_str()) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            continue;
        };
        let snapshot = &mut snapshots
            .entry((rate_limit_kind_order(kind), window))
            .or_insert_with(|| (kind, PartialSnapshot::default()))
            .1;
        match field {
            RateLimitField::Limit => {
                if let Some(parsed) = parse_u64(value) {
                    snapshot.limit = Some(parsed);
                }
            }
            RateLimitField::Remaining => {
                if let Some(parsed) = parse_u64(value) {
                    snapshot.remaining = Some(parsed);
                }
            }
            RateLimitField::Reset => {
                if let Some(parsed) = parse_reset(value) {
                    snapshot.reset = Some(parsed);
                }
            }
        }
    }

    snapshots
        .into_iter()
        .filter_map(|((_order, window), (kind, snapshot))| snapshot.into_snapshot(kind, window))
        .collect()
}

pub fn parse_anthropic_unified_headers(headers: &HeaderMap) -> Vec<UnifiedQuotaObservation> {
    let mut observations =
        BTreeMap::<u8, (SubscriptionQuotaWindow, PartialUnifiedQuotaObservation)>::new();

    for (name, value) in headers {
        let Some((window, field)) = parse_unified_header_name(name.as_str()) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            continue;
        };
        let partial = &mut observations
            .entry(subscription_quota_window_order(window))
            .or_insert_with(|| (window, PartialUnifiedQuotaObservation::default()))
            .1;
        match field {
            UnifiedQuotaField::Utilization => {
                if let Some(parsed) = parse_f64(value) {
                    partial.utilization = Some(clamp_utilization_fraction(parsed));
                }
            }
            UnifiedQuotaField::Reset => {
                if let Some(parsed) = parse_u64(value) {
                    partial.resets_at_unix_secs = Some(parsed);
                }
            }
            UnifiedQuotaField::Status => {
                if let Some(parsed) = parse_subscription_quota_status(value) {
                    partial.status = Some(parsed);
                }
            }
            UnifiedQuotaField::SurpassedThreshold => {
                if let Some(parsed) = parse_f64(value) {
                    partial.surpassed_threshold = Some(clamp_utilization_fraction(parsed));
                }
            }
            UnifiedQuotaField::RepresentativeClaim => {
                if let Some(parsed) = parse_string(value) {
                    partial.representative_claim = Some(parsed);
                }
            }
            UnifiedQuotaField::FallbackPercentage => {
                if let Some(parsed) = parse_f64(value) {
                    partial.fallback_percentage = Some(clamp_utilization_fraction(parsed));
                }
            }
            UnifiedQuotaField::FallbackAvailable => {
                partial.fallback_available = Some(value.trim() == "available");
            }
            UnifiedQuotaField::OverageInUse => {
                partial.overage_in_use = Some(value.trim() == "true");
            }
            UnifiedQuotaField::OverageMonthlyUtilization => {
                if let Some(parsed) = parse_f64(value) {
                    partial.overage_period_monthly_utilization =
                        Some(clamp_utilization_fraction(parsed));
                }
            }
            UnifiedQuotaField::UpgradePaths => {
                if let Some(parsed) = parse_csv_list(value) {
                    partial.upgrade_paths = Some(parsed);
                }
            }
            UnifiedQuotaField::DisabledReason => {
                if let Some(parsed) = parse_string(value) {
                    partial.disabled_reason = Some(parsed);
                }
            }
        }
    }

    observations
        .into_iter()
        .filter_map(|(_order, (window, observation))| observation.into_observation(window))
        .collect()
}

pub fn clamp_utilization_fraction(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

pub fn percent_to_utilization_fraction(value: f64) -> f64 {
    clamp_utilization_fraction(value / 100.0)
}

pub(crate) fn derive_limit_identity(principal: &Principal, headers: &HeaderMap) -> LimitIdentity {
    if let Some(account) = header_string(headers, "anthropic-organization-id") {
        return LimitIdentity::Account(account);
    }

    for key in [
        "anthropic_account_id",
        "account_id",
        "account_identity",
        "organization_id",
        "org_id",
    ] {
        if let Some(account) = claim_string(&principal.claims, key) {
            return LimitIdentity::Account(account);
        }
    }

    for key in [
        "credentials_ref",
        "credential_ref",
        "real_credential_storage_key",
        "oauth_credential_id",
        "oauth_provider",
    ] {
        if let Some(credential) = claim_string(&principal.claims, key) {
            return LimitIdentity::Credential(credential);
        }
    }

    LimitIdentity::Unobserved
}

fn parse_header_name(name: &str) -> Option<(RateLimitKind, RateLimitField, String)> {
    let suffix = name.strip_prefix(HEADER_PREFIX)?;
    let parts = suffix
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    for (field_index, part) in parts.iter().enumerate() {
        let Some(field) = parse_field(part) else {
            continue;
        };
        let before = &parts[..field_index];
        let after = &parts[field_index + 1..];
        let (kind, pre_field_window) = parse_kind_and_pre_field_window(before)?;
        let window = if after.is_empty() {
            window_identity(pre_field_window)
        } else {
            window_identity(after)
        };
        return Some((kind, field, window));
    }
    None
}

fn parse_field(part: &str) -> Option<RateLimitField> {
    match part {
        "limit" => Some(RateLimitField::Limit),
        "remaining" => Some(RateLimitField::Remaining),
        "reset" => Some(RateLimitField::Reset),
        _ => None,
    }
}

fn parse_kind_and_pre_field_window<'a>(
    parts: &'a [&'a str],
) -> Option<(RateLimitKind, &'a [&'a str])> {
    match parts {
        ["requests", window @ ..] | ["request", window @ ..] => {
            Some((RateLimitKind::Requests, window))
        }
        ["tokens", window @ ..] | ["token", window @ ..] => Some((RateLimitKind::Tokens, window)),
        ["input", "tokens", window @ ..] | ["input", "token", window @ ..] => {
            Some((RateLimitKind::InputTokens, window))
        }
        ["output", "tokens", window @ ..] | ["output", "token", window @ ..] => {
            Some((RateLimitKind::OutputTokens, window))
        }
        _ => None,
    }
}

fn rate_limit_kind_order(kind: RateLimitKind) -> u8 {
    match kind {
        RateLimitKind::Requests => 0,
        RateLimitKind::Tokens => 1,
        RateLimitKind::InputTokens => 2,
        RateLimitKind::OutputTokens => 3,
    }
}

fn window_identity(parts: &[&str]) -> String {
    if parts.is_empty() {
        DEFAULT_WINDOW.to_owned()
    } else {
        parts.join("-")
    }
}

fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

fn parse_reset(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn parse_unified_header_name(name: &str) -> Option<(SubscriptionQuotaWindow, UnifiedQuotaField)> {
    let suffix = name.strip_prefix(HEADER_PREFIX)?.strip_prefix("unified-")?;
    if let Some(field) = parse_unified_top_level_only_field(suffix) {
        return Some((SubscriptionQuotaWindow::Unified, field));
    }
    if let Some(field) = suffix.strip_prefix("5h-").and_then(parse_unified_field) {
        return Some((SubscriptionQuotaWindow::FiveHour, field));
    }
    if let Some(field) = suffix
        .strip_prefix("7d-sonnet-")
        .and_then(parse_unified_field)
    {
        return Some((SubscriptionQuotaWindow::SevenDaySonnet, field));
    }
    if let Some(field) = suffix
        .strip_prefix("7d-opus-")
        .and_then(parse_unified_field)
    {
        return Some((SubscriptionQuotaWindow::SevenDayOpus, field));
    }
    if let Some(field) = suffix.strip_prefix("7d-").and_then(parse_unified_field) {
        return Some((SubscriptionQuotaWindow::SevenDay, field));
    }
    if let Some(field) = suffix
        .strip_prefix("overage-")
        .and_then(parse_unified_field)
    {
        return Some((SubscriptionQuotaWindow::Overage, field));
    }
    parse_unified_field(suffix).map(|field| (SubscriptionQuotaWindow::Unified, field))
}

fn parse_unified_top_level_only_field(value: &str) -> Option<UnifiedQuotaField> {
    match value {
        "overage-in-use" => Some(UnifiedQuotaField::OverageInUse),
        "overage-period-monthly-utilization" => Some(UnifiedQuotaField::OverageMonthlyUtilization),
        _ => None,
    }
}

fn parse_unified_field(value: &str) -> Option<UnifiedQuotaField> {
    match value {
        "utilization" => Some(UnifiedQuotaField::Utilization),
        "reset" => Some(UnifiedQuotaField::Reset),
        "status" => Some(UnifiedQuotaField::Status),
        "surpassed-threshold" => Some(UnifiedQuotaField::SurpassedThreshold),
        "representative-claim" => Some(UnifiedQuotaField::RepresentativeClaim),
        "fallback-percentage" => Some(UnifiedQuotaField::FallbackPercentage),
        "fallback" => Some(UnifiedQuotaField::FallbackAvailable),
        "upgrade-paths" => Some(UnifiedQuotaField::UpgradePaths),
        "disabled-reason" => Some(UnifiedQuotaField::DisabledReason),
        _ => None,
    }
}

fn subscription_quota_window_order(window: SubscriptionQuotaWindow) -> u8 {
    match window {
        SubscriptionQuotaWindow::Unified => 0,
        SubscriptionQuotaWindow::FiveHour => 1,
        SubscriptionQuotaWindow::SevenDaySonnet => 2,
        SubscriptionQuotaWindow::SevenDayOpus => 3,
        SubscriptionQuotaWindow::SevenDay => 4,
        SubscriptionQuotaWindow::Overage => 5,
    }
}

fn parse_f64(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok()
}

fn parse_subscription_quota_status(value: &str) -> Option<SubscriptionQuotaStatus> {
    SubscriptionQuotaStatus::from_str(value.trim())
}

fn parse_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn parse_csv_list(value: &str) -> Option<Vec<String>> {
    let mut entries: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if entries.is_empty() {
        return None;
    }
    entries.sort();
    entries.dedup();
    Some(entries)
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn claim_string(claims: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    claims
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use cc_lb_plugin_api::{Principal, PrincipalKind, RateLimitKind, RateLimitObservation};
    use http::header::{HeaderName, HeaderValue};
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_default_request_and_token_families() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-limit", "1000"),
            ("anthropic-ratelimit-requests-remaining", "997"),
            ("anthropic-ratelimit-requests-reset", "2026-05-20T00:00:01Z"),
            ("anthropic-ratelimit-tokens-limit", "100000"),
            ("anthropic-ratelimit-tokens-remaining", "99990"),
            ("anthropic-ratelimit-tokens-reset", "2026-05-20T00:00:02Z"),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(
            snapshots[0],
            RateLimitObservation {
                kind: RateLimitKind::Requests,
                window: "default".to_owned(),
                limit: Some(1000),
                remaining: Some(997),
                reset: Some("2026-05-20T00:00:01Z".to_owned()),
            }
        );
        assert_eq!(
            snapshots[1],
            RateLimitObservation {
                kind: RateLimitKind::Tokens,
                window: "default".to_owned(),
                limit: Some(100000),
                remaining: Some(99990),
                reset: Some("2026-05-20T00:00:02Z".to_owned()),
            }
        );
    }

    #[test]
    fn preserves_post_field_window_identity_for_5h_and_weekly() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-limit-5h", "5000"),
            ("anthropic-ratelimit-requests-remaining-5h", "4999"),
            (
                "anthropic-ratelimit-tokens-reset-weekly",
                "2026-05-27T00:00:00Z",
            ),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].kind, RateLimitKind::Requests);
        assert_eq!(snapshots[0].window, "5h");
        assert_eq!(snapshots[0].limit, Some(5000));
        assert_eq!(snapshots[0].remaining, Some(4999));
        assert_eq!(snapshots[1].kind, RateLimitKind::Tokens);
        assert_eq!(snapshots[1].window, "weekly");
        assert_eq!(snapshots[1].reset.as_deref(), Some("2026-05-27T00:00:00Z"));
    }

    #[test]
    fn preserves_pre_field_window_identity() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-5h-limit", "5000"),
            ("anthropic-ratelimit-tokens-weekly-remaining", "42"),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].window, "5h");
        assert_eq!(snapshots[0].limit, Some(5000));
        assert_eq!(snapshots[1].window, "weekly");
        assert_eq!(snapshots[1].remaining, Some(42));
    }

    #[test]
    fn parses_input_and_output_token_families() {
        let headers = headers(&[
            ("anthropic-ratelimit-input-tokens-limit", "25000"),
            ("anthropic-ratelimit-input-tokens-remaining", "24000"),
            ("anthropic-ratelimit-output-tokens-limit-weekly", "75000"),
            (
                "anthropic-ratelimit-output-tokens-reset-weekly",
                "2026-05-28T00:00:00Z",
            ),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].kind, RateLimitKind::InputTokens);
        assert_eq!(snapshots[0].limit, Some(25000));
        assert_eq!(snapshots[0].remaining, Some(24000));
        assert_eq!(snapshots[1].kind, RateLimitKind::OutputTokens);
        assert_eq!(snapshots[1].window, "weekly");
        assert_eq!(snapshots[1].limit, Some(75000));
    }

    #[test]
    fn missing_and_malformed_headers_are_ignored() {
        let headers = headers(&[
            ("anthropic-ratelimit-requests-limit", "not-a-number"),
            ("anthropic-ratelimit-tokens-remaining", ""),
            ("anthropic-ratelimit-unknown-limit", "9"),
            ("x-ratelimit-requests-limit", "1"),
        ]);

        let snapshots = parse_anthropic_rate_limit_headers(&headers);

        assert!(snapshots.is_empty());
        assert!(parse_anthropic_rate_limit_headers(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn derives_account_identity_from_response_header_before_claims() {
        let headers = headers(&[("anthropic-organization-id", "org_header")]);
        let principal = principal_with_claims(&[
            ("account_id", json!("org_claim")),
            ("credentials_ref", json!("credential_claim")),
        ]);

        let identity = derive_limit_identity(&principal, &headers);

        assert_eq!(identity, LimitIdentity::Account("org_header".to_owned()));
    }

    #[test]
    fn derives_credential_identity_when_account_is_unobserved() {
        let principal = principal_with_claims(&[("credentials_ref", json!("credential-a"))]);

        let identity = derive_limit_identity(&principal, &HeaderMap::new());

        assert_eq!(
            identity,
            LimitIdentity::Credential("credential-a".to_owned())
        );
    }

    #[test]
    fn marks_identity_unobserved_when_no_account_or_credential_is_known() {
        let principal = principal_with_claims(&[]);

        let identity = derive_limit_identity(&principal, &HeaderMap::new());

        assert_eq!(identity, LimitIdentity::Unobserved);
    }

    #[test]
    fn unified_headers_empty_map_returns_empty() {
        assert!(parse_anthropic_unified_headers(&HeaderMap::new()).is_empty());
    }

    #[test]
    fn unified_top_level_status_emits_unified_window() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-status",
            "allowed",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Allowed)
        );
    }

    #[test]
    fn unified_top_level_reset_uses_unix_secs() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-reset",
            "1800000000",
        )]));

        assert_eq!(observations[0].resets_at_unix_secs, Some(1_800_000_000));
    }

    #[test]
    fn unified_top_level_representative_claim_is_trimmed() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-representative-claim",
            " org:claim ",
        )]));

        assert_eq!(
            observations[0].representative_claim.as_deref(),
            Some("org:claim")
        );
    }

    #[test]
    fn unified_top_level_fallback_percentage_is_clamped() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-fallback-percentage",
            "1.5",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(observations[0].fallback_percentage, Some(1.0));
    }

    #[test]
    fn unified_five_hour_utilization_emits_five_hour_window() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-5h-utilization",
            "0.42",
        )]));

        assert_eq!(observations[0].window, SubscriptionQuotaWindow::FiveHour);
        assert_eq!(observations[0].utilization, Some(0.42));
    }

    #[test]
    fn unified_five_hour_status_uses_storage_status_string() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-5h-status",
            "allowed_warning",
        )]));

        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::AllowedWarning)
        );
    }

    #[test]
    fn unified_exceeded_statuses_map_to_rejected() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            ("anthropic-ratelimit-unified-5h-status", "exceeded"),
            (
                "anthropic-ratelimit-unified-overage-status",
                "exceeded_overage",
            ),
        ]));

        assert_eq!(observations.len(), 2);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Rejected)
        );
        assert_eq!(
            observations[1].status,
            Some(SubscriptionQuotaStatus::Rejected)
        );
    }

    #[test]
    fn unified_five_hour_reset_uses_numeric_only() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-5h-reset",
            "1700000001",
        )]));

        assert_eq!(observations[0].resets_at_unix_secs, Some(1_700_000_001));
    }

    #[test]
    fn unified_five_hour_surpassed_threshold_parses_numeric_fraction() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-5h-surpassed-threshold",
            "0.75",
        )]));

        assert_eq!(observations[0].surpassed_threshold, Some(0.75));
    }

    #[test]
    fn unified_surpassed_threshold_above_one_is_clamped() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-overage-surpassed-threshold",
            "1.5",
        )]));

        assert_eq!(observations[0].surpassed_threshold, Some(1.0));
    }

    #[test]
    fn unified_generic_seven_day_emits_generic_window() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-7d-status",
            "rejected",
        )]));

        assert_eq!(observations[0].window, SubscriptionQuotaWindow::SevenDay);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Rejected)
        );
    }

    #[test]
    fn unified_seven_day_sonnet_does_not_parse_as_generic_seven_day() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-7d-sonnet-status",
            "allowed",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(
            observations[0].window,
            SubscriptionQuotaWindow::SevenDaySonnet
        );
    }

    #[test]
    fn unified_seven_day_opus_emits_opus_window() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-7d-opus-status",
            "allowed_warning",
        )]));

        assert_eq!(
            observations[0].window,
            SubscriptionQuotaWindow::SevenDayOpus
        );
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::AllowedWarning)
        );
    }

    #[test]
    fn unified_overage_status_reset_and_disabled_reason_emit_overage_window() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            ("anthropic-ratelimit-unified-overage-status", "rejected"),
            ("anthropic-ratelimit-unified-overage-reset", "1700000002"),
            (
                "anthropic-ratelimit-unified-overage-disabled-reason",
                "quota exhausted",
            ),
        ]));

        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Overage);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Rejected)
        );
        assert_eq!(observations[0].resets_at_unix_secs, Some(1_700_000_002));
        assert_eq!(
            observations[0].disabled_reason.as_deref(),
            Some("quota exhausted")
        );
    }

    #[test]
    fn unified_all_window_prefixes_get_own_rows() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            ("anthropic-ratelimit-unified-status", "allowed"),
            ("anthropic-ratelimit-unified-5h-status", "allowed"),
            ("anthropic-ratelimit-unified-7d-status", "allowed"),
            ("anthropic-ratelimit-unified-7d-sonnet-status", "allowed"),
            ("anthropic-ratelimit-unified-7d-opus-status", "allowed"),
            ("anthropic-ratelimit-unified-overage-status", "allowed"),
        ]));

        let windows = observations
            .iter()
            .map(|observation| observation.window)
            .collect::<Vec<_>>();
        assert_eq!(windows.len(), 6);
        assert!(windows.contains(&SubscriptionQuotaWindow::Unified));
        assert!(windows.contains(&SubscriptionQuotaWindow::FiveHour));
        assert!(windows.contains(&SubscriptionQuotaWindow::SevenDay));
        assert!(windows.contains(&SubscriptionQuotaWindow::SevenDaySonnet));
        assert!(windows.contains(&SubscriptionQuotaWindow::SevenDayOpus));
        assert!(windows.contains(&SubscriptionQuotaWindow::Overage));
    }

    #[test]
    fn unified_malformed_reset_becomes_none_but_status_keeps_row() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            (
                "anthropic-ratelimit-unified-7d-reset",
                "2026-05-20T00:00:00Z",
            ),
            ("anthropic-ratelimit-unified-7d-status", "allowed"),
        ]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].resets_at_unix_secs, None);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Allowed)
        );
    }

    #[test]
    fn unified_malformed_utilization_becomes_none_but_status_keeps_row() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            ("anthropic-ratelimit-unified-7d-sonnet-utilization", "many"),
            ("anthropic-ratelimit-unified-7d-sonnet-status", "allowed"),
        ]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].utilization, None);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Allowed)
        );
    }

    #[test]
    fn unified_malformed_status_becomes_none_but_reset_keeps_row() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            (
                "anthropic-ratelimit-unified-7d-opus-status",
                "almost_allowed",
            ),
            ("anthropic-ratelimit-unified-7d-opus-reset", "1700000003"),
        ]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].status, None);
        assert_eq!(observations[0].resets_at_unix_secs, Some(1_700_000_003));
    }

    #[test]
    fn unified_malformed_surpassed_threshold_becomes_none_but_status_keeps_row() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            ("anthropic-ratelimit-unified-5h-surpassed-threshold", "yes"),
            ("anthropic-ratelimit-unified-5h-status", "allowed"),
        ]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].surpassed_threshold, None);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Allowed)
        );
    }

    #[test]
    fn live_unified_headers_extract_all_observed_windows() {
        let observations = parse_anthropic_unified_headers(&headers(&[
            ("anthropic-ratelimit-unified-5h-utilization", "0.10"),
            ("anthropic-ratelimit-unified-5h-status", "allowed_warning"),
            ("anthropic-ratelimit-unified-5h-reset", "1800000001"),
            ("anthropic-ratelimit-unified-5h-surpassed-threshold", "0.75"),
            ("anthropic-ratelimit-unified-7d-utilization", "0.25"),
            ("anthropic-ratelimit-unified-7d-status", "allowed"),
            ("anthropic-ratelimit-unified-7d-reset", "1800000002"),
            ("anthropic-ratelimit-unified-overage-status", "rejected"),
            (
                "anthropic-ratelimit-unified-overage-disabled-reason",
                "quota exhausted",
            ),
            ("anthropic-ratelimit-unified-status", "allowed"),
            ("anthropic-ratelimit-unified-reset", "1800000000"),
            (
                "anthropic-ratelimit-unified-representative-claim",
                "org:claim",
            ),
            ("anthropic-ratelimit-unified-fallback-percentage", "0.5"),
            ("anthropic-ratelimit-unified-fallback", "available"),
            ("anthropic-ratelimit-unified-overage-in-use", "true"),
            (
                "anthropic-ratelimit-unified-overage-period-monthly-utilization",
                "0.20",
            ),
            (
                "anthropic-ratelimit-unified-upgrade-paths",
                "team_growth,max_5x",
            ),
        ]));

        assert_eq!(observations.len(), 4);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(
            observations[0].status,
            Some(SubscriptionQuotaStatus::Allowed)
        );
        assert_eq!(observations[0].resets_at_unix_secs, Some(1_800_000_000));
        assert_eq!(
            observations[0].representative_claim.as_deref(),
            Some("org:claim")
        );
        assert_eq!(observations[0].fallback_percentage, Some(0.5));
        assert_eq!(observations[0].fallback_available, Some(true));
        assert_eq!(observations[0].overage_in_use, Some(true));
        assert_eq!(
            observations[0].overage_period_monthly_utilization,
            Some(0.20)
        );
        assert_eq!(
            observations[0].upgrade_paths.as_deref(),
            Some(["max_5x".to_owned(), "team_growth".to_owned()].as_slice())
        );
        assert_eq!(observations[1].window, SubscriptionQuotaWindow::FiveHour);
        assert_eq!(observations[1].utilization, Some(0.10));
        assert_eq!(
            observations[1].status,
            Some(SubscriptionQuotaStatus::AllowedWarning)
        );
        assert_eq!(observations[1].resets_at_unix_secs, Some(1_800_000_001));
        assert_eq!(observations[1].surpassed_threshold, Some(0.75));
        assert_eq!(observations[2].window, SubscriptionQuotaWindow::SevenDay);
        assert_eq!(observations[2].utilization, Some(0.25));
        assert_eq!(
            observations[2].status,
            Some(SubscriptionQuotaStatus::Allowed)
        );
        assert_eq!(observations[2].resets_at_unix_secs, Some(1_800_000_002));
        assert_eq!(observations[3].window, SubscriptionQuotaWindow::Overage);
        assert_eq!(
            observations[3].status,
            Some(SubscriptionQuotaStatus::Rejected)
        );
        assert_eq!(
            observations[3].disabled_reason.as_deref(),
            Some("quota exhausted")
        );
    }

    #[test]
    fn unified_fallback_available_signals_true_only_for_available_value() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-fallback",
            "available",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(observations[0].fallback_available, Some(true));
    }

    #[test]
    fn unified_fallback_non_available_value_signals_false() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-fallback",
            "unavailable",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(observations[0].fallback_available, Some(false));
    }

    #[test]
    fn unified_overage_in_use_is_top_level_not_overage_window() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-overage-in-use",
            "true",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(observations[0].overage_in_use, Some(true));
    }

    #[test]
    fn unified_overage_in_use_non_true_value_signals_false() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-overage-in-use",
            "false",
        )]));

        assert_eq!(observations[0].overage_in_use, Some(false));
    }

    #[test]
    fn unified_overage_period_monthly_utilization_is_top_level_and_clamped() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-overage-period-monthly-utilization",
            "1.5",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(
            observations[0].overage_period_monthly_utilization,
            Some(1.0)
        );
    }

    #[test]
    fn unified_upgrade_paths_csv_is_split_and_normalized() {
        let observations = parse_anthropic_unified_headers(&headers(&[(
            "anthropic-ratelimit-unified-upgrade-paths",
            "team_growth , max_5x,team_growth, ",
        )]));

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].window, SubscriptionQuotaWindow::Unified);
        assert_eq!(
            observations[0].upgrade_paths.as_deref(),
            Some(["max_5x".to_owned(), "team_growth".to_owned()].as_slice())
        );
    }

    #[test]
    fn utilization_fraction_inputs_are_clamped() {
        assert_eq!(clamp_utilization_fraction(0.0), 0.0);
        assert_eq!(clamp_utilization_fraction(0.5), 0.5);
        assert_eq!(clamp_utilization_fraction(1.0), 1.0);
        assert_eq!(clamp_utilization_fraction(1.5), 1.0);
        assert_eq!(clamp_utilization_fraction(-0.5), 0.0);
    }

    #[test]
    fn utilization_percent_inputs_are_divided_and_clamped() {
        assert_eq!(percent_to_utilization_fraction(0.0), 0.0);
        assert_eq!(percent_to_utilization_fraction(10.0), 0.10);
        assert_eq!(percent_to_utilization_fraction(25.0), 0.25);
        assert_eq!(percent_to_utilization_fraction(100.0), 1.0);
        assert_eq!(percent_to_utilization_fraction(125.0), 1.0);
        assert_eq!(percent_to_utilization_fraction(-10.0), 0.0);
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("test header name parses"),
                HeaderValue::from_str(value).expect("test header value parses"),
            );
        }
        headers
    }

    fn principal_with_claims(pairs: &[(&str, Value)]) -> Principal {
        let mut claims = serde_json::Map::new();
        for (key, value) in pairs {
            claims.insert((*key).to_owned(), value.clone());
        }
        Principal {
            id: "principal-test".to_owned(),
            kind: PrincipalKind::ApiKey,
            claims,
        }
    }
}
