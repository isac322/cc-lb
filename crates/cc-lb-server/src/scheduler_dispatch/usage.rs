use cc_lb_engine::SubscriptionQuotaSink;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_storage_api::{
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
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
    limits: Option<Vec<UsageLimit>>,
    extra_usage: Option<ExtraUsage>,
}

#[derive(Debug, Deserialize)]
struct UsageWindow {
    utilization: Option<f64>,
    resets_at: Option<ResetsAt>,
}

#[derive(Debug, Deserialize)]
struct UsageLimit {
    kind: String,
    percent: Option<f64>,
    resets_at: Option<ResetsAt>,
    scope: Option<UsageLimitScope>,
    is_active: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct UsageLimitScope {
    model: Option<UsageLimitModel>,
}

#[derive(Debug, Deserialize)]
struct UsageLimitModel {
    display_name: Option<String>,
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
    let usage: UsageBody = sonic_rs::from_slice(body)
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
) -> Vec<SubscriptionQuotaSample> {
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
    if let Some(limit) = usage.limits.into_iter().flatten().find(|limit| {
        limit.is_active == Some(true)
            && limit.kind == "weekly_scoped"
            && limit
                .scope
                .as_ref()
                .and_then(|scope| scope.model.as_ref())
                .and_then(|model| model.display_name.as_deref())
                == Some("Fable")
    }) {
        push_window(
            &mut records,
            upstream_id,
            SubscriptionQuotaWindow::SevenDayFable,
            Some(UsageWindow {
                utilization: limit.percent,
                resets_at: limit.resets_at,
            }),
            observed_at_unix_millis,
        );
    }
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
    records: &mut Vec<SubscriptionQuotaSample>,
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
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
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
mod tests;
