use std::{str::FromStr, time::Duration};

use cc_lb_storage_api::{StorageError, StorageResult};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

pub mod adapter;

#[derive(Debug, Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

pub async fn open_sqlite(database_url: &str) -> StorageResult<SqliteStorage> {
    let options = SqliteConnectOptions::from_str(database_url)
        .map_err(map_sqlx_error)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));

    // SQLite serializes writes through a single writer regardless of pool size,
    // so multi-connection pools provide no write throughput benefit and instead
    // expose SQLITE_BUSY_SNAPSHOT (code 517) races between concurrent readers
    // and a writer holding a snapshot. The `busy_timeout` PRAGMA only handles
    // SQLITE_BUSY (code 5) — not SQLITE_BUSY_SNAPSHOT — so the only reliable
    // mitigation is a single connection. WAL mode keeps reads non-blocking
    // within that connection, and tests / production both serialize their
    // own request handling at the axum / tower layer.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(map_sqlx_error)?;

    Ok(SqliteStorage::new(pool))
}

fn map_sqlx_error(error: sqlx::Error) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}
