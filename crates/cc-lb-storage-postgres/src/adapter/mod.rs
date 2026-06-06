use std::sync::{Arc, atomic::AtomicBool};

use cc_lb_storage_api::{ChangeEvent, StorageError, StorageResult};
use chrono::{DateTime, TimeZone, Utc};
use sqlx::PgPool;

pub mod anthropic_compatibility_kv;
pub mod api_keys;
pub mod audit;
pub mod config_store;
pub mod limit_state;
pub mod managed_keys;
pub mod meta;
pub mod notifier;
pub mod oauth_credentials;
pub mod organization_metadata;
pub mod plugin_registry;
pub mod price_catalog;
pub mod principals;
pub mod quota;
pub mod request_events;
pub mod retry;
pub mod upstream_rate_limit;
pub mod upstream_subscription_metadata;
pub mod upstream_subscription_quota;
pub mod upstreams;
pub mod usage_rollups;

#[derive(Debug, Clone)]
pub struct PostgresStorage {
    pub(crate) pool: PgPool,
    pub(crate) listener_pool: PgPool,
    pub(crate) change_tx: tokio::sync::broadcast::Sender<ChangeEvent>,
    pub(crate) notifier_running: Arc<AtomicBool>,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_listener_pool(pool.clone(), pool)
    }

    pub fn new_with_listener_pool(pool: PgPool, listener_pool: PgPool) -> Self {
        Self {
            pool,
            listener_pool,
            change_tx: notifier::change_sender(),
            notifier_running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
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
