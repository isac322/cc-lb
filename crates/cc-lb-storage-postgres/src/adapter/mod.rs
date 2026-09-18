use std::sync::{Arc, atomic::AtomicBool};

use cc_lb_clock::ClockHandle;
use cc_lb_storage_api::{ChangeEvent, StorageError, StorageResult};
use chrono::{DateTime, TimeZone, Utc};
use sqlx::PgPool;

pub mod anthropic_compatibility_kv;
pub mod api_key_usage;
pub mod audit;
mod cache_keepalive_session_read_row;
pub mod cache_keepalive_session_reads;
pub mod cache_keepalive_sessions;
pub mod config_store;
pub mod managed_keys;
pub mod meta;
pub mod notifier;
pub mod oauth_pkce;
pub mod organization_metadata;
pub mod plan_tiers;
pub mod plugin_registry;
pub mod pool_quota_history;
mod pool_quota_history_summary;
pub mod price_catalog;
pub mod principals;
pub mod prompt_cache_observation;
mod request_event_list_row;
mod request_event_list_sql;
pub mod request_events;
pub mod retry;
pub mod upstream_affinity;
pub mod upstream_rate_limit;
pub mod upstream_subscription_metadata;
pub mod upstream_subscription_quota;
pub mod upstreams;
pub mod usage_rollups;
mod usage_token_intervals;
mod warmup_attempt_mapping;
pub mod warmup_attempts;

#[derive(Clone)]
pub struct PostgresStorage {
    pub(crate) pool: PgPool,
    pub(crate) listener_pool: PgPool,
    pub(crate) clock: ClockHandle,
    pub(crate) change_tx: tokio::sync::broadcast::Sender<ChangeEvent>,
    pub(crate) notifier_running: Arc<AtomicBool>,
}

impl PostgresStorage {
    pub fn new(pool: PgPool, clock: ClockHandle) -> Self {
        Self::new_with_listener_pool(pool.clone(), pool, clock)
    }

    pub fn new_with_listener_pool(pool: PgPool, listener_pool: PgPool, clock: ClockHandle) -> Self {
        Self {
            pool,
            listener_pool,
            clock,
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
