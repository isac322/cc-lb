use std::{
    str::FromStr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use cc_lb_clock::{Clock, ClockHandle};
use cc_lb_storage_api::{StorageError, StorageResult};
use sqlx::{
    Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

pub mod adapter;

pub use adapter::subscription_quota_backfill::{
    SUBSCRIPTION_QUOTA_CHECKPOINT_BACKFILL_MARKER_KEY, SubscriptionQuotaCheckpointBackfillError,
    SubscriptionQuotaCheckpointBackfillReport,
};
pub use adapter::subscription_quota_cleanup::{
    SUBSCRIPTION_QUOTA_CHECKPOINT_CLEANUP_MARKER_KEY, SubscriptionQuotaCheckpointCleanupError,
    SubscriptionQuotaCheckpointCleanupReport,
};

#[derive(Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
    clock: ClockHandle,
    // Cleanup drops this raw table offline with the service stopped, so its
    // presence is fixed for a process lifetime and safe to memoize.
    raw_subscription_quota_observations_present: Arc<OnceLock<bool>>,
}

impl SqliteStorage {
    pub fn new(pool: SqlitePool, clock: ClockHandle) -> Self {
        Self {
            pool,
            clock,
            raw_subscription_quota_observations_present: Arc::new(OnceLock::new()),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn begin_immediate(&self) -> StorageResult<Transaction<'static, Sqlite>> {
        begin_immediate(&self.pool).await
    }

    pub(crate) fn clock(&self) -> &dyn Clock {
        &*self.clock
    }

    pub(crate) async fn raw_subscription_quota_observations_present(&self) -> StorageResult<bool> {
        if let Some(present) = self.raw_subscription_quota_observations_present.get() {
            return Ok(*present);
        }
        let present = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_schema \
             WHERE type = 'table' AND name = 'upstream_subscription_quota_observations_v1'",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?
            > 0;
        let _ = self
            .raw_subscription_quota_observations_present
            .set(present);
        Ok(present)
    }
}

pub async fn begin_immediate(pool: &SqlitePool) -> StorageResult<Transaction<'static, Sqlite>> {
    pool.begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(map_sqlx_error)
}

pub async fn open_sqlite(database_url: &str, clock: ClockHandle) -> StorageResult<SqliteStorage> {
    let options = SqliteConnectOptions::from_str(database_url)
        .map_err(map_sqlx_error)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(map_sqlx_error)?;

    Ok(SqliteStorage::new(pool, clock))
}

fn map_sqlx_error(error: sqlx::Error) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}
