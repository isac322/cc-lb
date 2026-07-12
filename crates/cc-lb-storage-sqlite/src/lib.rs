use std::{str::FromStr, time::Duration};

use cc_lb_clock::{Clock, ClockHandle};
use cc_lb_storage_api::{StorageError, StorageResult};
use sqlx::{
    Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

pub mod adapter;

#[derive(Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
    clock: ClockHandle,
}

impl SqliteStorage {
    pub fn new(pool: SqlitePool, clock: ClockHandle) -> Self {
        Self { pool, clock }
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

    // >1 connection lets request-path reads run concurrently with background
    // writes via WAL instead of serializing on one shared connection. Safe only
    // because every write txn uses BEGIN IMMEDIATE (`begin_immediate`): no
    // deferred->write upgrades to deadlock; writers serialize via the write lock
    // + busy_timeout.
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .min_connections(1)
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
