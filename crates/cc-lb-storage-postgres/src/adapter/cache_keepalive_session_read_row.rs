use crate::error_map::map_sqlx_error;
use cc_lb_storage_api::{
    CacheKeepaliveConfigSnapshot, CacheKeepaliveDecisionRecord, CacheKeepaliveEnqueueState,
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem, CacheKeepaliveSessionStatus,
    CacheKeepaliveTerminalReason, CacheKeepaliveTurnRecord, CacheTtl, StorageError, StorageResult,
};
use sqlx::{Row, postgres::PgRow};

pub(crate) fn list_item_from_row(row: PgRow) -> StorageResult<CacheKeepaliveSessionListItem> {
    let source = source_from_db(
        &row.try_get::<String, _>("entry_source")
            .map_err(map_sqlx_error)?,
    )?;
    let entry_id: String = row.try_get("entry_id").map_err(map_sqlx_error)?;
    let id = entry_id
        .strip_prefix(match source {
            CacheKeepaliveSessionEntrySource::Session => "session:",
            CacheKeepaliveSessionEntrySource::Decision => "decision:",
        })
        .ok_or_else(|| StorageError::Corrupted {
            message: "invalid cache keepalive list entry id".to_owned(),
        })?
        .to_owned();
    Ok(CacheKeepaliveSessionListItem {
        id,
        source,
        session_key_hash: row.try_get("session_key_hash").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        last_message_at_ms: i64_to_u64(
            row.try_get("last_message_at_ms").map_err(map_sqlx_error)?,
            "cache keepalive last message",
        )?,
        ttl: ttl_from_db(&row.try_get::<String, _>("ttl").map_err(map_sqlx_error)?)?,
        generation: i64_to_u64(
            row.try_get("generation").map_err(map_sqlx_error)?,
            "cache keepalive generation",
        )?,
        refresh_count: row
            .try_get::<Option<i64>, _>("refresh_count")
            .map_err(map_sqlx_error)?
            .map(|value| i64_to_u32(value, "cache keepalive refresh count"))
            .transpose()?,
        status: row
            .try_get::<Option<String>, _>("status")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(status_from_db)
            .transpose()?,
        enqueue_state: row
            .try_get::<Option<String>, _>("enqueue_state")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(enqueue_state_from_db)
            .transpose()?,
        terminal_reason: row
            .try_get::<Option<String>, _>("terminal_reason")
            .map_err(map_sqlx_error)?
            .as_deref()
            .map(terminal_reason_from_db)
            .transpose()?,
        decision: row.try_get("decision").map_err(map_sqlx_error)?,
        reason: row.try_get("reason").map_err(map_sqlx_error)?,
        error: row.try_get("error").map_err(map_sqlx_error)?,
        config_snapshot: config_snapshot_from_db(
            row.try_get("config_snapshot").map_err(map_sqlx_error)?,
        )?,
    })
}

pub(crate) fn turn_from_row(row: PgRow) -> StorageResult<CacheKeepaliveTurnRecord> {
    Ok(CacheKeepaliveTurnRecord {
        source_ref_id: row.try_get("source_ref_id").map_err(map_sqlx_error)?,
        session_key_hash: row.try_get("session_key_hash").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        accounting_key_id: row.try_get("accounting_key_id").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        model: row.try_get("model").map_err(map_sqlx_error)?,
        input_tokens: i64_to_u64(
            row.try_get("input_tokens").map_err(map_sqlx_error)?,
            "cache keepalive input tokens",
        )?,
        output_tokens: i64_to_u64(
            row.try_get("output_tokens").map_err(map_sqlx_error)?,
            "cache keepalive output tokens",
        )?,
        cache_creation_input_tokens: i64_to_u64(
            row.try_get("cache_creation_input_tokens")
                .map_err(map_sqlx_error)?,
            "cache keepalive cache creation input tokens",
        )?,
        cache_creation_input_tokens_5m: i64_to_u64(
            row.try_get("cache_creation_input_tokens_5m")
                .map_err(map_sqlx_error)?,
            "cache keepalive cache creation 5m input tokens",
        )?,
        cache_creation_input_tokens_1h: i64_to_u64(
            row.try_get("cache_creation_input_tokens_1h")
                .map_err(map_sqlx_error)?,
            "cache keepalive cache creation 1h input tokens",
        )?,
        cache_read_input_tokens: i64_to_u64(
            row.try_get("cache_read_input_tokens")
                .map_err(map_sqlx_error)?,
            "cache keepalive cache read input tokens",
        )?,
        cost_micros: row.try_get("cost_micros").map_err(map_sqlx_error)?,
        hit_miss: row.try_get("hit_miss").map_err(map_sqlx_error)?,
        ts: i64_to_u64(
            row.try_get("ts").map_err(map_sqlx_error)?,
            "cache keepalive turn timestamp",
        )?,
    })
}

pub(crate) fn decision_from_row(row: PgRow) -> StorageResult<CacheKeepaliveDecisionRecord> {
    Ok(CacheKeepaliveDecisionRecord {
        source_ref_id: row.try_get("source_ref_id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        session_key_hash: row.try_get("session_key_hash").map_err(map_sqlx_error)?,
        upstream_id: row.try_get("upstream_id").map_err(map_sqlx_error)?,
        decision: row.try_get("decision").map_err(map_sqlx_error)?,
        reason: row.try_get("reason").map_err(map_sqlx_error)?,
        error: row.try_get("error").map_err(map_sqlx_error)?,
        generation: i64_to_u64(
            row.try_get("generation").map_err(map_sqlx_error)?,
            "cache keepalive decision generation",
        )?,
        ttl: ttl_from_db(&row.try_get::<String, _>("ttl").map_err(map_sqlx_error)?)?,
        config_snapshot: config_snapshot_from_db(
            row.try_get("config_snapshot").map_err(map_sqlx_error)?,
        )?,
        last_message_at_ms: i64_to_u64(
            row.try_get("last_message_at_ms").map_err(map_sqlx_error)?,
            "cache keepalive decision last message",
        )?,
        ts: i64_to_u64(
            row.try_get("ts").map_err(map_sqlx_error)?,
            "cache keepalive decision timestamp",
        )?,
    })
}

fn source_from_db(value: &str) -> StorageResult<CacheKeepaliveSessionEntrySource> {
    match value {
        "session" => Ok(CacheKeepaliveSessionEntrySource::Session),
        "decision" => Ok(CacheKeepaliveSessionEntrySource::Decision),
        _ => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive list source {value}"),
        }),
    }
}
fn ttl_from_db(value: &str) -> StorageResult<CacheTtl> {
    match value {
        "5m" => Ok(CacheTtl::Ttl5m),
        "1h" => Ok(CacheTtl::Ttl1h),
        _ => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive ttl {value}"),
        }),
    }
}
fn status_from_db(value: &str) -> StorageResult<CacheKeepaliveSessionStatus> {
    match value {
        "active" => Ok(CacheKeepaliveSessionStatus::Active),
        "terminal" => Ok(CacheKeepaliveSessionStatus::Terminal),
        _ => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive status {value}"),
        }),
    }
}
fn enqueue_state_from_db(value: &str) -> StorageResult<CacheKeepaliveEnqueueState> {
    match value {
        "pending" => Ok(CacheKeepaliveEnqueueState::Pending),
        "enqueued" => Ok(CacheKeepaliveEnqueueState::Enqueued),
        "running" => Ok(CacheKeepaliveEnqueueState::Running),
        _ => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive enqueue state {value}"),
        }),
    }
}
fn terminal_reason_from_db(value: &str) -> StorageResult<CacheKeepaliveTerminalReason> {
    match value {
        "cancelled" => Ok(CacheKeepaliveTerminalReason::Cancelled),
        "expired" => Ok(CacheKeepaliveTerminalReason::Expired),
        "max_refreshes" => Ok(CacheKeepaliveTerminalReason::MaxRefreshes),
        "max_duration" => Ok(CacheKeepaliveTerminalReason::MaxDuration),
        "cache_miss" => Ok(CacheKeepaliveTerminalReason::CacheMiss),
        "dispatch_error" => Ok(CacheKeepaliveTerminalReason::DispatchError),
        "decrypt_failed" => Ok(CacheKeepaliveTerminalReason::DecryptFailed),
        "unsupported_provider" => Ok(CacheKeepaliveTerminalReason::UnsupportedProvider),
        "stale" => Ok(CacheKeepaliveTerminalReason::Stale),
        _ => Err(StorageError::Corrupted {
            message: format!("invalid cache keepalive terminal reason {value}"),
        }),
    }
}
fn config_snapshot_from_db(
    value: Option<String>,
) -> StorageResult<Option<CacheKeepaliveConfigSnapshot>> {
    value
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| StorageError::Corrupted {
                message: format!("invalid cache keepalive config snapshot: {error}"),
            })
        })
        .transpose()
}
fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("negative {field} value {value}"),
    })
}
fn i64_to_u32(value: i64, field: &str) -> StorageResult<u32> {
    u32::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("invalid {field} value {value}"),
    })
}
