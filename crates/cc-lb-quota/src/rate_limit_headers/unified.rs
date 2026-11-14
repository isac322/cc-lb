use std::collections::BTreeMap;

use cc_lb_domain::SubscriptionQuotaCredits;
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow};
use http::HeaderMap;

use super::{UnifiedQuotaObservation, clamp_utilization_fraction};

const HEADER_PREFIX: &str = "anthropic-ratelimit-";

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
            credits: None,
            upgrade_paths: self.upgrade_paths,
            disabled_reason: self.disabled_reason,
        })
    }
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
        apply_field(partial, field, value);
    }

    observations
        .into_iter()
        .filter_map(|(_order, (window, observation))| observation.into_observation(window))
        .collect()
}

fn apply_field(
    partial: &mut PartialUnifiedQuotaObservation,
    field: UnifiedQuotaField,
    value: &str,
) {
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
            if let Some(parsed) = SubscriptionQuotaStatus::from_str(value.trim()) {
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
        UnifiedQuotaField::OverageInUse => partial.overage_in_use = Some(value.trim() == "true"),
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
    if let Some(field) = suffix.strip_prefix("7d_oi-").and_then(parse_unified_field) {
        return Some((SubscriptionQuotaWindow::SevenDayFable, field));
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

/// Parse the Codex/ChatGPT OAuth quota headers documented by the Codex CLI.
///
/// The meter slot (`primary` or `secondary`) does not identify the window:
/// Codex reports the duration separately. Only the stable five-hour and
/// seven-day durations are stored.
pub fn parse_openai_codex_quota_headers(headers: &http::HeaderMap) -> Vec<UnifiedQuotaObservation> {
    let mut observations = Vec::new();
    for prefix in ["primary", "secondary"] {
        let used = headers
            .get(format!("x-codex-{prefix}-used-percent"))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite());
        let reset = headers
            .get(format!("x-codex-{prefix}-reset-at"))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<i64>().ok())
            .and_then(|value| u64::try_from(value).ok());
        let minutes = headers
            .get(format!("x-codex-{prefix}-window-minutes"))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<i64>().ok());
        let Some(window) = (match minutes {
            Some(300) => Some(SubscriptionQuotaWindow::FiveHour),
            Some(10_080) => Some(SubscriptionQuotaWindow::SevenDay),
            _ => None,
        }) else {
            continue;
        };
        if used.is_none() && reset.is_none() {
            continue;
        }
        let utilization = used.map(|value| clamp_utilization_fraction(value / 100.0));
        let status = utilization.map(|value| {
            if value >= 1.0 {
                SubscriptionQuotaStatus::Rejected
            } else {
                SubscriptionQuotaStatus::Allowed
            }
        });
        observations.push(UnifiedQuotaObservation {
            window,
            utilization,
            status,
            resets_at_unix_secs: reset,
            representative_claim: Some(format!("Codex {prefix} meter")),
            ..UnifiedQuotaObservation::default()
        });
    }
    if let Some(credits) = parse_codex_credits(headers) {
        observations.push(UnifiedQuotaObservation {
            window: SubscriptionQuotaWindow::Unified,
            representative_claim: Some("Codex credits".to_owned()),
            fallback_available: credits.fallback_available(),
            credits: Some(credits),
            ..UnifiedQuotaObservation::default()
        });
    }
    observations
}

fn parse_codex_credits(headers: &http::HeaderMap) -> Option<SubscriptionQuotaCredits> {
    let has_credits = headers
        .get("x-codex-credits-has-credits")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_codex_bool);
    let unlimited = headers
        .get("x-codex-credits-unlimited")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_codex_bool);
    let balance = headers
        .get("x-codex-credits-balance")
        .and_then(|value| value.to_str().ok())
        .filter(|value| cc_lb_domain::is_credit_balance(value))
        .map(str::to_owned);
    if has_credits.is_none() && unlimited.is_none() && balance.is_none() {
        return None;
    }
    Some(SubscriptionQuotaCredits {
        has_credits,
        unlimited,
        limit_reached: None,
        balance,
        approx_local_messages: None,
        approx_cloud_messages: None,
        ..Default::default()
    })
}

fn parse_codex_bool(value: &str) -> Option<bool> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("true") || value == "1" {
        Some(true)
    } else if value.eq_ignore_ascii_case("false") || value == "0" {
        Some(false)
    } else {
        None
    }
}

fn subscription_quota_window_order(window: SubscriptionQuotaWindow) -> u8 {
    match window {
        SubscriptionQuotaWindow::Unified => 0,
        SubscriptionQuotaWindow::FiveHour => 1,
        SubscriptionQuotaWindow::SevenDaySonnet => 2,
        SubscriptionQuotaWindow::SevenDayOpus => 3,
        SubscriptionQuotaWindow::SevenDayFable => 4,
        SubscriptionQuotaWindow::SevenDay => 5,
        SubscriptionQuotaWindow::GeminiFiveHour => 6,
        SubscriptionQuotaWindow::GeminiSevenDay => 7,
        SubscriptionQuotaWindow::ThirdPartyFiveHour => 8,
        SubscriptionQuotaWindow::ThirdPartySevenDay => 9,
        SubscriptionQuotaWindow::Overage => 10,
    }
}

fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

fn parse_f64(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok()
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
