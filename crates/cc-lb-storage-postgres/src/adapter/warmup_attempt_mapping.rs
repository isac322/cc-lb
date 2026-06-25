use cc_lb_storage_api::{
    StorageError, StorageResult, WarmupAttemptOutcome, WarmupAttemptReason, WarmupAttemptRecord,
    WarmupAttemptTrigger,
};
use sqlx::{Row, postgres::PgRow};

use crate::error_map::map_sqlx_error;

pub(crate) fn row_to_attempt(row: PgRow) -> StorageResult<WarmupAttemptRecord> {
    Ok(WarmupAttemptRecord {
        id: row.try_get("id").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        attempted_at_unix_secs: row
            .try_get("attempted_at_unix_secs")
            .map_err(map_sqlx_error)?,
        completed_at_unix_secs: row
            .try_get("completed_at_unix_secs")
            .map_err(map_sqlx_error)?,
        scheduled_for_unix_secs: row
            .try_get("scheduled_for_unix_secs")
            .map_err(map_sqlx_error)?,
        trigger: trigger_from_str(
            &row.try_get::<String, _>("trigger")
                .map_err(map_sqlx_error)?,
        )?,
        outcome: outcome_from_str(
            &row.try_get::<String, _>("outcome")
                .map_err(map_sqlx_error)?,
        )?,
        reason: row
            .try_get::<Option<String>, _>("reason")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(reason_from_str)
            .transpose()?,
        http_status: row.try_get("http_status").map_err(map_sqlx_error)?,
        cycle_key: row.try_get("cycle_key").map_err(map_sqlx_error)?,
        expected_cycle_key: row.try_get("expected_cycle_key").map_err(map_sqlx_error)?,
        idle_secs_since_prev_window: row
            .try_get("idle_secs_since_prev_window")
            .map_err(map_sqlx_error)?,
        replica_id: row.try_get("replica_id").map_err(map_sqlx_error)?,
        lease_holder: row.try_get("lease_holder").map_err(map_sqlx_error)?,
        upstream_spec_revision: row
            .try_get("upstream_spec_revision")
            .map_err(map_sqlx_error)?,
        dialect_plugin_snapshot: row
            .try_get("dialect_plugin_snapshot")
            .map_err(map_sqlx_error)?,
        error_detail: row.try_get("error_detail").map_err(map_sqlx_error)?,
    })
}

pub(crate) fn outcome_to_str(outcome: WarmupAttemptOutcome) -> &'static str {
    match outcome {
        WarmupAttemptOutcome::SuccessFresh => "success_fresh",
        WarmupAttemptOutcome::SuccessRedundant => "success_redundant",
        WarmupAttemptOutcome::TransientFailure => "transient_failure",
        WarmupAttemptOutcome::PermanentFailure => "permanent_failure",
        WarmupAttemptOutcome::Skipped => "skipped",
    }
}

pub(crate) fn outcome_from_str(value: &str) -> StorageResult<WarmupAttemptOutcome> {
    match value {
        "success_fresh" => Ok(WarmupAttemptOutcome::SuccessFresh),
        "success_redundant" => Ok(WarmupAttemptOutcome::SuccessRedundant),
        "transient_failure" => Ok(WarmupAttemptOutcome::TransientFailure),
        "permanent_failure" => Ok(WarmupAttemptOutcome::PermanentFailure),
        "skipped" => Ok(WarmupAttemptOutcome::Skipped),
        value => Err(StorageError::Corrupted {
            message: format!("invalid warmup attempt outcome {value}"),
        }),
    }
}

pub(crate) fn reason_to_str(reason: WarmupAttemptReason) -> &'static str {
    match reason {
        WarmupAttemptReason::WindowAlreadyActive => "window_already_active",
        WarmupAttemptReason::SevenDayQuotaExhausted => "seven_day_quota_exhausted",
        WarmupAttemptReason::Http429MissingCycleKey => "http_429_missing_cycle_key",
        WarmupAttemptReason::Upstream5xx => "upstream_5xx",
        WarmupAttemptReason::NetworkError => "network_error",
        WarmupAttemptReason::RequestTimeout => "request_timeout",
        WarmupAttemptReason::RequestBuildFailed => "request_build_failed",
        WarmupAttemptReason::OauthRefreshFailed => "oauth_refresh_failed",
        WarmupAttemptReason::CredentialDecryptFailed => "credential_decrypt_failed",
        WarmupAttemptReason::AuthFailed => "auth_failed",
        WarmupAttemptReason::Forbidden => "forbidden",
        WarmupAttemptReason::BadRequest => "bad_request",
        WarmupAttemptReason::NotFound => "not_found",
        WarmupAttemptReason::DialectPluginFailed => "dialect_plugin_failed",
        WarmupAttemptReason::DialectPluginTransient => "dialect_plugin_transient",
        WarmupAttemptReason::OauthCredentialsMissing => "oauth_credentials_missing",
        WarmupAttemptReason::LeaseHeld => "lease_held",
        WarmupAttemptReason::UpstreamDisabled => "upstream_disabled",
        WarmupAttemptReason::UpstreamDeleted => "upstream_deleted",
    }
}

fn reason_from_str(value: &str) -> StorageResult<WarmupAttemptReason> {
    match value {
        "window_already_active" => Ok(WarmupAttemptReason::WindowAlreadyActive),
        "seven_day_quota_exhausted" => Ok(WarmupAttemptReason::SevenDayQuotaExhausted),
        "http_429_missing_cycle_key" => Ok(WarmupAttemptReason::Http429MissingCycleKey),
        "upstream_5xx" => Ok(WarmupAttemptReason::Upstream5xx),
        "network_error" => Ok(WarmupAttemptReason::NetworkError),
        "request_timeout" => Ok(WarmupAttemptReason::RequestTimeout),
        "request_build_failed" => Ok(WarmupAttemptReason::RequestBuildFailed),
        "oauth_refresh_failed" => Ok(WarmupAttemptReason::OauthRefreshFailed),
        "credential_decrypt_failed" => Ok(WarmupAttemptReason::CredentialDecryptFailed),
        "auth_failed" => Ok(WarmupAttemptReason::AuthFailed),
        "forbidden" => Ok(WarmupAttemptReason::Forbidden),
        "bad_request" => Ok(WarmupAttemptReason::BadRequest),
        "not_found" => Ok(WarmupAttemptReason::NotFound),
        "dialect_plugin_failed" => Ok(WarmupAttemptReason::DialectPluginFailed),
        "dialect_plugin_transient" => Ok(WarmupAttemptReason::DialectPluginTransient),
        "oauth_credentials_missing" => Ok(WarmupAttemptReason::OauthCredentialsMissing),
        "lease_held" => Ok(WarmupAttemptReason::LeaseHeld),
        "upstream_disabled" => Ok(WarmupAttemptReason::UpstreamDisabled),
        "upstream_deleted" => Ok(WarmupAttemptReason::UpstreamDeleted),
        value => Err(StorageError::Corrupted {
            message: format!("invalid warmup attempt reason {value}"),
        }),
    }
}

pub(crate) fn trigger_to_str(trigger: WarmupAttemptTrigger) -> &'static str {
    match trigger {
        WarmupAttemptTrigger::Scheduled => "scheduled",
        WarmupAttemptTrigger::Manual => "manual",
    }
}

fn trigger_from_str(value: &str) -> StorageResult<WarmupAttemptTrigger> {
    match value {
        "scheduled" => Ok(WarmupAttemptTrigger::Scheduled),
        "manual" => Ok(WarmupAttemptTrigger::Manual),
        value => Err(StorageError::Corrupted {
            message: format!("invalid warmup attempt trigger {value}"),
        }),
    }
}
