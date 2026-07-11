use cc_lb_quota::rate_limit_headers::UnifiedQuotaObservation;
use cc_lb_storage_api::upstream::UpstreamRecord;
use cc_lb_storage_api::{
    Storage, SubscriptionQuotaWindow, WarmupAttemptOutcome, WarmupAttemptRecord,
    WarmupAttemptTrigger, WarmupDispatchKind, WarmupPermanentFailureReason, WarmupSkipReason,
    WarmupSuccessReason, WarmupTransientFailureReason,
};
use http::StatusCode;
use uuid::Uuid;

mod classification;

use classification::attempt_fields;

#[derive(Clone, Copy)]
pub struct WarmupAttemptExecution<'a> {
    pub storage: &'a dyn Storage,
    pub upstream: &'a UpstreamRecord,
    pub scheduled_for_unix_secs: i64,
    pub trigger: WarmupAttemptTrigger,
    pub replica_id: Option<Uuid>,
    pub lease_holder: Option<&'a str>,
    pub expected_cycle_key: Option<i64>,
    pub attempted_at_unix_secs: i64,
    pub completed_at_unix_secs: Option<i64>,
    pub dispatch_kind: WarmupDispatchKind,
    pub result: WarmupAttemptExecutionResult<'a>,
}

#[derive(Clone, Copy, Debug)]
pub enum WarmupAttemptExecutionResult<'a> {
    Response {
        status: StatusCode,
        observations: &'a [UnifiedQuotaObservation],
        error_detail: Option<&'a str>,
    },
    TransientFailure {
        reason: WarmupTransientFailureReason,
        http_status: Option<StatusCode>,
        error_detail: Option<&'a str>,
    },
    PermanentFailure {
        reason: WarmupPermanentFailureReason,
        http_status: Option<StatusCode>,
        error_detail: Option<&'a str>,
    },
    Skipped {
        reason: WarmupSkipReason,
        cycle_key: Option<i64>,
        error_detail: Option<&'a str>,
    },
    PreflightActiveWindow {
        cycle_key: i64,
    },
}

pub async fn execute_warmup_attempt(execution: WarmupAttemptExecution<'_>) -> WarmupAttemptRecord {
    let fields = attempt_fields(&execution);
    let idle_secs_since_prev_window = match fields.outcome {
        WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced) => {
            idle_secs_since_prev_window(
                execution.storage,
                execution.upstream.id,
                execution.attempted_at_unix_secs,
            )
            .await
        }
        WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive)
        | WarmupAttemptOutcome::Skipped(_)
        | WarmupAttemptOutcome::TransientFailure(_)
        | WarmupAttemptOutcome::PermanentFailure(_) => None,
    };
    let record = WarmupAttemptRecord {
        id: Uuid::new_v4(),
        upstream_id: execution.upstream.id,
        attempted_at_unix_secs: execution.attempted_at_unix_secs,
        completed_at_unix_secs: execution.completed_at_unix_secs,
        scheduled_for_unix_secs: execution.scheduled_for_unix_secs,
        trigger: execution.trigger,
        outcome: fields.outcome,
        dispatch_kind: Some(execution.dispatch_kind),
        http_status: fields.http_status,
        cycle_key: fields.cycle_key,
        expected_cycle_key: execution.expected_cycle_key,
        idle_secs_since_prev_window,
        replica_id: execution.replica_id,
        lease_holder: execution.lease_holder.map(ToOwned::to_owned),
        upstream_spec_revision: u64_to_i64_lossy(execution.upstream.revision),
        dialect_plugin_snapshot: dialect_plugin_snapshot(execution.upstream),
        error_detail: fields.error_detail,
    };
    if let Err(error) = execution.storage.insert_warmup_attempt(&record).await {
        tracing::error!(
            upstream_id = %record.upstream_id,
            outcome = ?record.outcome,
            %error,
            "warmup attempt persistence failed"
        );
    }
    record
}

async fn idle_secs_since_prev_window(
    storage: &dyn Storage,
    upstream_id: Uuid,
    attempted_at_unix_secs: i64,
) -> Option<i64> {
    let latest = match storage
        .list_latest_subscription_quota_for_upstreams(&[upstream_id])
        .await
    {
        Ok(latest) => latest,
        Err(error) => {
            tracing::warn!(%upstream_id, %error, "warmup attempt idle lookup failed");
            return None;
        }
    };
    latest
        .into_iter()
        .filter(|record| record.window == SubscriptionQuotaWindow::FiveHour)
        .max_by_key(|record| record.observed_at_unix_millis)
        .and_then(|record| record.resets_at_unix_secs)
        .and_then(|resets_at| i64::try_from(resets_at).ok())
        .map(|prev_window_end| {
            attempted_at_unix_secs
                .saturating_sub(prev_window_end)
                .max(0)
        })
}

fn dialect_plugin_snapshot(upstream: &UpstreamRecord) -> Option<serde_json::Value> {
    upstream.warmup_dialect_plugin.as_ref().map(|plugin| {
        serde_json::json!({
            "wasm_registry_id": plugin.wasm_registry_id,
            "wire_version": plugin.wire_version,
            "config": plugin.config.clone(),
        })
    })
}

fn u64_to_i64_lossy(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
