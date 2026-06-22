//! Leader election via advisory locks for cron scheduling.

use std::future::Future;

#[cfg(feature = "postgres")]
use sqlx::postgres::PgConnection;

mod errors;
#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

pub use errors::{LeaderError, LeaderState};
#[cfg(feature = "postgres")]
pub use postgres::{ConnectionReclaimer, LeaderLockKey, PostgresLeaderElection};
pub use sqlite::SqliteLeaderElection;

#[derive(Debug)]
pub enum LeaderElection {
    Sqlite(SqliteLeaderElection),
    #[cfg(feature = "postgres")]
    Postgres(PostgresLeaderElection),
}

impl LeaderElection {
    pub const fn sqlite() -> Self {
        Self::Sqlite(SqliteLeaderElection)
    }

    #[cfg(feature = "postgres")]
    pub async fn postgres(
        database_url: impl Into<String>,
        lock_key: i64,
    ) -> Result<Self, LeaderError> {
        PostgresLeaderElection::connect(database_url, lock_key)
            .await
            .map(Self::Postgres)
    }

    #[cfg(feature = "postgres")]
    pub fn from_postgres_connection(
        database_url: impl Into<String>,
        connection: PgConnection,
        lock_key: i64,
    ) -> Self {
        Self::Postgres(PostgresLeaderElection::from_connection(
            database_url,
            connection,
            lock_key,
        ))
    }

    pub async fn run<F, Fut>(&self, work: F) -> Result<(), LeaderError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        match self {
            Self::Sqlite(sqlite) => sqlite.run(work).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres.run(work).await,
        }
    }

    pub async fn try_acquire(&self) -> Result<bool, LeaderError> {
        let acquired = match self {
            Self::Sqlite(sqlite) => sqlite.try_acquire().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres.try_acquire().await,
        }?;
        if acquired {
            crate::scheduler_metrics::record_leader_acquired(1);
        }
        Ok(acquired)
    }

    pub async fn heartbeat(&self) -> Result<(), LeaderError> {
        match self {
            Self::Sqlite(sqlite) => sqlite.heartbeat().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres.heartbeat().await,
        }
    }

    pub async fn release(&self) -> Result<bool, LeaderError> {
        match self {
            Self::Sqlite(sqlite) => sqlite.release().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres.release().await,
        }
    }

    pub async fn close(&self) -> Result<(), LeaderError> {
        match self {
            Self::Sqlite(sqlite) => sqlite.close().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres.close().await,
        }
    }

    pub fn current_state(&self) -> LeaderState {
        match self {
            Self::Sqlite(_sqlite) => LeaderState::Single,
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres.current_state(),
        }
    }
}
