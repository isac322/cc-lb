use cc_lb_core::SubscriptionQuotaSink;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::oauth_usage_poll::{
    OAuthUsagePollHandler, OAuthUsagePollJob, compute_next_run_at,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::state_stores::{OAuthUsagePollCursorsStore, OAuthUsagePollScheduleConfig};
use cc_lb_scheduler::worker::{EntityJob, SchedulerBackend, SchedulerPushTask};
use cc_lb_storage_api::{
    SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::scheduler_dispatch::time::now_unix_secs;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

use super::SchedulerDispatch;

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

impl SchedulerDispatch {
    pub(super) async fn dispatch_oauth_usage_poll(
        &self,
        job: OAuthUsagePollJob,
    ) -> SchedulerResult<JobOutcome> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                let config = OAuthUsagePollScheduleConfig::default();
                let cursors = OAuthUsagePollCursorsStore::new(sqlite.pool.clone());
                let outcome = OAuthUsagePollHandler::new(cursors.clone(), config.clone())
                    .handle(job.clone(), now_unix_secs(), |job| self.poll_usage(job))
                    .await?;
                let Some(cursor) = cursors.read(job.upstream_id).await? else {
                    return Ok(outcome);
                };
                let next_run_at = compute_next_run_at(now_unix_secs(), &config, Some(&cursor));
                push_next_oauth_usage_poll_task(&self.backend, job, next_run_at).await?;
                Ok(outcome)
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                let config = OAuthUsagePollScheduleConfig::default();
                let cursors = OAuthUsagePollCursorsStore::new(postgres.pool.clone());
                let outcome = OAuthUsagePollHandler::new(cursors.clone(), config.clone())
                    .handle(job.clone(), now_unix_secs(), |job| self.poll_usage(job))
                    .await?;
                let Some(cursor) = cursors.read(job.upstream_id).await? else {
                    return Ok(outcome);
                };
                let next_run_at = compute_next_run_at(now_unix_secs(), &config, Some(&cursor));
                push_next_oauth_usage_poll_task(&self.backend, job, next_run_at).await?;
                Ok(outcome)
            }
        }
    }
}

pub(super) async fn push_next_oauth_usage_poll_task(
    backend: &SchedulerBackend,
    job: OAuthUsagePollJob,
    unlock_at_unix_secs: u64,
) -> SchedulerResult<()> {
    let idempotency_key = job.idempotency_key(unlock_at_unix_secs);
    let task = SchedulerPushTask {
        args: EntityJob::OAuthUsagePoll(job),
        idempotency_key: Some(idempotency_key),
        run_at_unix_secs: Some(unlock_at_unix_secs),
    };
    match backend.push_entity_task(task).await {
        Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
        Err(error) => Err(error),
    }
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
mod tests;
