use cc_lb_core::SubscriptionQuotaSink;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_storage_api::{
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::subscription_quota_cache::SubscriptionQuotaCache;

#[derive(Debug, Deserialize)]
struct UsageBody {
    five_hour: Option<UsageWindow>,
    seven_day: Option<UsageWindow>,
    seven_day_sonnet: Option<UsageWindow>,
    seven_day_opus: Option<UsageWindow>,
    extra_usage: Option<ExtraUsage>,
}

#[derive(Debug, Deserialize)]
struct UsageWindow {
    utilization: Option<f64>,
    resets_at: Option<ResetsAt>,
}

// Anthropic's /api/oauth/usage emits `resets_at` as a Unix-seconds number or an RFC3339 string; accept both.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ResetsAt {
    Number(f64),
    String(String),
}

fn resets_at_to_unix_secs(value: ResetsAt) -> Option<u64> {
    match value {
        ResetsAt::Number(value) if value.is_finite() && value >= 0.0 => Some(value as u64),
        ResetsAt::Number(_) => None,
        ResetsAt::String(value) => chrono::DateTime::parse_from_rfc3339(value.trim())
            .ok()
            .and_then(|dt| u64::try_from(dt.timestamp()).ok()),
    }
}

#[derive(Debug, Deserialize)]
struct ExtraUsage {
    #[serde(rename = "is_enabled")]
    enabled: Option<bool>,
    monthly_limit: Option<f64>,
    used_credits: Option<f64>,
}

pub(super) fn observe_usage_body(
    upstream_id: Uuid,
    body: &[u8],
    observed_at_unix_millis: u64,
    sink: &SubscriptionQuotaSink,
    cache: &SubscriptionQuotaCache,
) -> SchedulerResult<()> {
    let usage: UsageBody = serde_json::from_slice(body)
        .map_err(|error| SchedulerError::Job(format!("oauth usage JSON parse failed: {error}")))?;
    for record in records_from_usage(upstream_id, usage, observed_at_unix_millis) {
        cache.upsert_observation(upstream_id, &record);
        sink.enqueue(record)
            .map_err(|error| SchedulerError::Job(error.to_string()))?;
    }
    Ok(())
}

fn records_from_usage(
    upstream_id: Uuid,
    usage: UsageBody,
    observed_at_unix_millis: u64,
) -> Vec<SubscriptionQuotaObservationRecord> {
    let mut records = Vec::new();
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::FiveHour,
        usage.five_hour,
        observed_at_unix_millis,
    );
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::SevenDay,
        usage.seven_day,
        observed_at_unix_millis,
    );
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::SevenDaySonnet,
        usage.seven_day_sonnet,
        observed_at_unix_millis,
    );
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::SevenDayOpus,
        usage.seven_day_opus,
        observed_at_unix_millis,
    );
    if let Some(extra_usage) = usage.extra_usage {
        let utilization = match (extra_usage.used_credits, extra_usage.monthly_limit) {
            (Some(used), Some(limit)) if limit > 0.0 => Some((used / limit).clamp(0.0, 1.0)),
            _ => None,
        };
        records.push(base_record(
            upstream_id,
            SubscriptionQuotaWindow::Overage,
            observed_at_unix_millis,
            utilization,
            None,
            extra_usage.enabled,
            extra_usage.monthly_limit,
            extra_usage.used_credits,
        ));
    }
    records
}

fn push_window(
    records: &mut Vec<SubscriptionQuotaObservationRecord>,
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    usage: Option<UsageWindow>,
    observed_at_unix_millis: u64,
) {
    if let Some(usage) = usage {
        records.push(base_record(
            upstream_id,
            window,
            observed_at_unix_millis,
            usage
                .utilization
                .map(|value| (value / 100.0).clamp(0.0, 1.0)),
            usage.resets_at.and_then(resets_at_to_unix_secs),
            None,
            None,
            None,
        ));
    }
}

#[allow(clippy::too_many_arguments)]
fn base_record(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    observed_at_unix_millis: u64,
    utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
    extra_usage_enabled: Option<bool>,
    extra_usage_monthly_limit: Option<f64>,
    extra_usage_used_credits: Option<f64>,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Api,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization,
        status: utilization.map(status_from_utilization),
        resets_at_unix_secs,
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled,
        extra_usage_monthly_limit,
        extra_usage_used_credits,
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

fn status_from_utilization(utilization: f64) -> SubscriptionQuotaStatus {
    if utilization >= 1.0 {
        SubscriptionQuotaStatus::Rejected
    } else if utilization >= 0.8 {
        SubscriptionQuotaStatus::AllowedWarning
    } else {
        SubscriptionQuotaStatus::Allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &str) -> Vec<SubscriptionQuotaObservationRecord> {
        let usage: UsageBody = serde_json::from_str(body).expect("usage body parses");
        records_from_usage(Uuid::nil(), usage, 1_700_000_000_000)
    }

    #[test]
    fn rfc3339_resets_at_is_accepted() {
        let records =
            parse(r#"{"five_hour":{"utilization":42,"resets_at":"2026-06-21T20:30:00Z"}}"#);
        let five_hour = records
            .iter()
            .find(|r| r.window == SubscriptionQuotaWindow::FiveHour)
            .expect("five hour record");
        assert_eq!(five_hour.resets_at_unix_secs, Some(1_782_073_800));
    }

    #[test]
    fn numeric_resets_at_is_accepted() {
        let records = parse(r#"{"seven_day":{"utilization":12,"resets_at":1800000000}}"#);
        let seven_day = records
            .iter()
            .find(|r| r.window == SubscriptionQuotaWindow::SevenDay)
            .expect("seven day record");
        assert_eq!(seven_day.resets_at_unix_secs, Some(1_800_000_000));
    }

    #[test]
    fn float_resets_at_is_accepted() {
        let records = parse(r#"{"five_hour":{"utilization":1,"resets_at":1800000000.5}}"#);
        let five_hour = records
            .iter()
            .find(|r| r.window == SubscriptionQuotaWindow::FiveHour)
            .expect("five hour record");
        assert_eq!(five_hour.resets_at_unix_secs, Some(1_800_000_000));
    }

    #[test]
    fn string_resets_at_does_not_poison_other_windows() {
        let records = parse(
            r#"{"five_hour":{"utilization":10,"resets_at":"2026-06-21T20:30:00Z"},"seven_day":{"utilization":3,"resets_at":1800000000}}"#,
        );
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn full_production_body_produces_all_four_records() {
        let body = r#"{"five_hour":{"utilization":60.0,"resets_at":"2026-06-21T08:10:00.885300+00:00"},"seven_day":{"utilization":21.0,"resets_at":"2026-06-25T19:00:00.885325+00:00"},"seven_day_oauth_apps":null,"seven_day_opus":null,"seven_day_sonnet":{"utilization":0.0,"resets_at":"2026-06-25T18:59:59.885338+00:00"},"extra_usage":{"is_enabled":false,"monthly_limit":null,"used_credits":null}}"#;
        let records = parse(body);
        let windows: Vec<_> = records.iter().map(|r| r.window).collect();
        assert_eq!(
            records.len(),
            4,
            "expected 4 records, got windows={:?}",
            windows
        );
    }

    #[test]
    fn extra_usage_is_enabled_field_is_parsed() {
        let records = parse(
            r#"{"extra_usage":{"is_enabled":true,"monthly_limit":30000,"used_credits":15000}}"#,
        );
        let overage = records
            .iter()
            .find(|r| r.window == SubscriptionQuotaWindow::Overage)
            .expect("overage record");
        assert_eq!(overage.extra_usage_enabled, Some(true));
        assert_eq!(overage.extra_usage_monthly_limit, Some(30000.0));
        assert_eq!(overage.extra_usage_used_credits, Some(15000.0));
    }
}
