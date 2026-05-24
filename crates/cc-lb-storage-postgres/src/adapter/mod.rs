use cc_lb_storage_api::{StorageError, StorageResult};
use chrono::{DateTime, TimeZone, Utc};
use sqlx::PgPool;

pub mod audit;
pub mod config_store;
pub mod limit_state;
pub mod meta;
pub mod request_events;

#[derive(Debug, Clone)]
pub struct PostgresStorage {
    pub(crate) pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub(crate) fn unix_secs_to_datetime(value: u64, field: &str) -> StorageResult<DateTime<Utc>> {
    let seconds = i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as postgres timestamptz"),
    })?;
    Utc.timestamp_opt(seconds, 0)
        .single()
        .ok_or_else(|| StorageError::Fatal {
            message: format!("{field} cannot be represented as postgres timestamptz"),
        })
}

pub(crate) fn unix_secs_to_datetime_upper(
    value: u64,
    field: &str,
) -> StorageResult<Option<DateTime<Utc>>> {
    let Ok(seconds) = i64::try_from(value) else {
        return Ok(None);
    };
    match Utc.timestamp_opt(seconds, 0).single() {
        Some(value) => Ok(Some(value)),
        None => {
            let _ = field;
            Ok(None)
        }
    }
}

pub(crate) fn unix_secs_to_datetime_lower(
    value: u64,
    field: &str,
) -> StorageResult<Option<DateTime<Utc>>> {
    let Ok(seconds) = i64::try_from(value) else {
        return Ok(None);
    };
    match Utc.timestamp_opt(seconds, 0).single() {
        Some(value) => Ok(Some(value)),
        None => {
            let _ = field;
            Ok(None)
        }
    }
}

pub(crate) fn datetime_to_unix_secs(value: DateTime<Utc>, field: &str) -> StorageResult<u64> {
    u64::try_from(value.timestamp()).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is before the unix epoch"),
    })
}

pub(crate) fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as bigint"),
    })
}

pub(crate) fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

pub(crate) fn i32_to_u16(value: i32, field: &str) -> StorageResult<u16> {
    u16::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is outside u16 range"),
    })
}

pub(crate) fn conflict(message: impl Into<String>) -> StorageError {
    StorageError::Conflict {
        message: message.into(),
    }
}
