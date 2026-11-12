use cc_lb_storage_api::{
    StorageError, StorageResult, WarmupAttemptOutcome, WarmupAttemptRecord, WarmupAttemptStatus,
    WarmupAttemptTrigger, WarmupDispatchKind, WarmupLane, WarmupPermanentFailureReason,
    WarmupSkipReason, WarmupSuccessReason, WarmupTransientFailureReason,
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
        lane: lane_from_str(&row.try_get::<String, _>("lane").map_err(map_sqlx_error)?)?,
        outcome: parse_reason(
            status_from_str(
                &row.try_get::<String, _>("outcome")
                    .map_err(map_sqlx_error)?,
            )?,
            &row.try_get::<String, _>("reason").map_err(map_sqlx_error)?,
        )?,
        dispatch_kind: row
            .try_get::<Option<String>, _>("dispatch_kind")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(dispatch_kind_from_str)
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

pub(crate) fn outcome_components(outcome: WarmupAttemptOutcome) -> (&'static str, &'static str) {
    match outcome {
        WarmupAttemptOutcome::Success(WarmupSuccessReason::CycleAdvanced) => {
            ("success", "cycle_advanced")
        }
        WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive) => {
            ("success", "window_already_active")
        }
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted) => {
            ("skipped", "seven_day_quota_exhausted")
        }
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDisabled) => {
            ("skipped", "upstream_disabled")
        }
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::UpstreamDeleted) => {
            ("skipped", "upstream_deleted")
        }
        WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable) => {
            ("skipped", "quota_group_unavailable")
        }
        WarmupAttemptOutcome::TransientFailure(
            WarmupTransientFailureReason::RateLimitedCycleKeyMissing,
        ) => ("transient_failure", "rate_limited_cycle_key_missing"),
        WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::Upstream5xx) => {
            ("transient_failure", "upstream_5xx")
        }
        WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::NetworkError) => {
            ("transient_failure", "network_error")
        }
        WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::RequestTimeout) => {
            ("transient_failure", "request_timeout")
        }
        WarmupAttemptOutcome::TransientFailure(
            WarmupTransientFailureReason::DialectPluginTransient,
        ) => ("transient_failure", "dialect_plugin_transient"),
        WarmupAttemptOutcome::PermanentFailure(
            WarmupPermanentFailureReason::OauthCredentialsMissing,
        ) => ("permanent_failure", "oauth_credentials_missing"),
        WarmupAttemptOutcome::PermanentFailure(
            WarmupPermanentFailureReason::RequestBuildFailed,
        ) => ("permanent_failure", "request_build_failed"),
        WarmupAttemptOutcome::PermanentFailure(
            WarmupPermanentFailureReason::OauthRefreshFailed,
        ) => ("permanent_failure", "oauth_refresh_failed"),
        WarmupAttemptOutcome::PermanentFailure(
            WarmupPermanentFailureReason::CredentialDecryptFailed,
        ) => ("permanent_failure", "credential_decrypt_failed"),
        WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::AuthFailed) => {
            ("permanent_failure", "auth_failed")
        }
        WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::Forbidden) => {
            ("permanent_failure", "forbidden")
        }
        WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::BadRequest) => {
            ("permanent_failure", "bad_request")
        }
        WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::NotFound) => {
            ("permanent_failure", "not_found")
        }
        WarmupAttemptOutcome::PermanentFailure(
            WarmupPermanentFailureReason::DialectPluginFailed,
        ) => ("permanent_failure", "dialect_plugin_failed"),
    }
}

pub(crate) fn status_to_str(status: WarmupAttemptStatus) -> &'static str {
    match status {
        WarmupAttemptStatus::Success => "success",
        WarmupAttemptStatus::Skipped => "skipped",
        WarmupAttemptStatus::TransientFailure => "transient_failure",
        WarmupAttemptStatus::PermanentFailure => "permanent_failure",
    }
}

pub(crate) fn status_from_str(value: &str) -> StorageResult<WarmupAttemptStatus> {
    match value {
        "success" => Ok(WarmupAttemptStatus::Success),
        "skipped" => Ok(WarmupAttemptStatus::Skipped),
        "transient_failure" => Ok(WarmupAttemptStatus::TransientFailure),
        "permanent_failure" => Ok(WarmupAttemptStatus::PermanentFailure),
        value => Err(StorageError::Corrupted {
            message: format!("invalid warmup attempt status {value}"),
        }),
    }
}

pub(crate) fn parse_reason(
    status: WarmupAttemptStatus,
    reason: &str,
) -> StorageResult<WarmupAttemptOutcome> {
    match (status, reason) {
        (WarmupAttemptStatus::Success, "cycle_advanced") => Ok(WarmupAttemptOutcome::Success(
            WarmupSuccessReason::CycleAdvanced,
        )),
        (WarmupAttemptStatus::Success, "window_already_active") => Ok(
            WarmupAttemptOutcome::Success(WarmupSuccessReason::WindowAlreadyActive),
        ),
        (WarmupAttemptStatus::Skipped, "seven_day_quota_exhausted") => Ok(
            WarmupAttemptOutcome::Skipped(WarmupSkipReason::SevenDayQuotaExhausted),
        ),
        (WarmupAttemptStatus::Skipped, "upstream_disabled") => Ok(WarmupAttemptOutcome::Skipped(
            WarmupSkipReason::UpstreamDisabled,
        )),
        (WarmupAttemptStatus::Skipped, "upstream_deleted") => Ok(WarmupAttemptOutcome::Skipped(
            WarmupSkipReason::UpstreamDeleted,
        )),
        (WarmupAttemptStatus::Skipped, "quota_group_unavailable") => Ok(
            WarmupAttemptOutcome::Skipped(WarmupSkipReason::QuotaGroupUnavailable),
        ),
        (WarmupAttemptStatus::TransientFailure, "rate_limited_cycle_key_missing") => {
            Ok(WarmupAttemptOutcome::TransientFailure(
                WarmupTransientFailureReason::RateLimitedCycleKeyMissing,
            ))
        }
        (WarmupAttemptStatus::TransientFailure, "upstream_5xx") => Ok(
            WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::Upstream5xx),
        ),
        (WarmupAttemptStatus::TransientFailure, "network_error") => Ok(
            WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::NetworkError),
        ),
        (WarmupAttemptStatus::TransientFailure, "request_timeout") => Ok(
            WarmupAttemptOutcome::TransientFailure(WarmupTransientFailureReason::RequestTimeout),
        ),
        (WarmupAttemptStatus::TransientFailure, "dialect_plugin_transient") => {
            Ok(WarmupAttemptOutcome::TransientFailure(
                WarmupTransientFailureReason::DialectPluginTransient,
            ))
        }
        (WarmupAttemptStatus::PermanentFailure, "oauth_credentials_missing") => {
            Ok(WarmupAttemptOutcome::PermanentFailure(
                WarmupPermanentFailureReason::OauthCredentialsMissing,
            ))
        }
        (WarmupAttemptStatus::PermanentFailure, "request_build_failed") => {
            Ok(WarmupAttemptOutcome::PermanentFailure(
                WarmupPermanentFailureReason::RequestBuildFailed,
            ))
        }
        (WarmupAttemptStatus::PermanentFailure, "oauth_refresh_failed") => {
            Ok(WarmupAttemptOutcome::PermanentFailure(
                WarmupPermanentFailureReason::OauthRefreshFailed,
            ))
        }
        (WarmupAttemptStatus::PermanentFailure, "credential_decrypt_failed") => {
            Ok(WarmupAttemptOutcome::PermanentFailure(
                WarmupPermanentFailureReason::CredentialDecryptFailed,
            ))
        }
        (WarmupAttemptStatus::PermanentFailure, "auth_failed") => Ok(
            WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::AuthFailed),
        ),
        (WarmupAttemptStatus::PermanentFailure, "forbidden") => Ok(
            WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::Forbidden),
        ),
        (WarmupAttemptStatus::PermanentFailure, "bad_request") => Ok(
            WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::BadRequest),
        ),
        (WarmupAttemptStatus::PermanentFailure, "not_found") => Ok(
            WarmupAttemptOutcome::PermanentFailure(WarmupPermanentFailureReason::NotFound),
        ),
        (WarmupAttemptStatus::PermanentFailure, "dialect_plugin_failed") => {
            Ok(WarmupAttemptOutcome::PermanentFailure(
                WarmupPermanentFailureReason::DialectPluginFailed,
            ))
        }
        (status, reason) => Err(StorageError::Corrupted {
            message: format!("invalid warmup attempt reason {reason} for status {status:?}"),
        }),
    }
}

pub(crate) fn dispatch_kind_to_str(dispatch_kind: WarmupDispatchKind) -> &'static str {
    match dispatch_kind {
        WarmupDispatchKind::NotDispatched => "not_dispatched",
        WarmupDispatchKind::Http => "http",
        WarmupDispatchKind::DialectPlugin => "dialect_plugin",
    }
}

pub(crate) fn dispatch_kind_from_str(value: &str) -> StorageResult<WarmupDispatchKind> {
    match value {
        "not_dispatched" => Ok(WarmupDispatchKind::NotDispatched),
        "http" => Ok(WarmupDispatchKind::Http),
        "dialect_plugin" => Ok(WarmupDispatchKind::DialectPlugin),
        value => Err(StorageError::Corrupted {
            message: format!("invalid warmup attempt dispatch_kind {value}"),
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

pub(crate) fn lane_from_str(value: &str) -> StorageResult<WarmupLane> {
    WarmupLane::parse(value).ok_or_else(|| StorageError::Corrupted {
        message: format!("invalid warmup attempt lane {value}"),
    })
}
