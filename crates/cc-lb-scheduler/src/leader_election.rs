//! Leader election via advisory locks for cron scheduling.

use std::future::Future;
#[cfg(feature = "postgres")]
use std::time::Duration;

use thiserror::Error;

#[cfg(feature = "postgres")]
use sqlx::Connection as _;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgConnection;
#[cfg(feature = "postgres")]
use tokio::sync::Mutex;

#[cfg(feature = "postgres")]
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum LeaderError {
    #[cfg(feature = "postgres")]
    #[error("leader database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("leader connection is not available")]
    NotConnected,
    #[error("leader lock lost: {reason}")]
    LockLost { reason: String },
}

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
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteLeaderElection;

impl SqliteLeaderElection {
    pub async fn run<F, Fut>(&self, work: F) -> Result<(), LeaderError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        work().await;
        Ok(())
    }

    pub async fn try_acquire(&self) -> Result<bool, LeaderError> {
        Ok(true)
    }

    pub async fn heartbeat(&self) -> Result<(), LeaderError> {
        Ok(())
    }

    pub async fn release(&self) -> Result<bool, LeaderError> {
        Ok(true)
    }

    pub async fn close(&self) -> Result<(), LeaderError> {
        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[derive(Debug)]
pub struct PostgresLeaderElection {
    database_url: String,
    lock_key: LeaderLockKey,
    heartbeat_interval: Duration,
    connection: Mutex<Option<PgConnection>>,
}

#[cfg(feature = "postgres")]
impl PostgresLeaderElection {
    pub async fn connect(
        database_url: impl Into<String>,
        lock_key: i64,
    ) -> Result<Self, LeaderError> {
        let database_url = database_url.into();
        let connection = PgConnection::connect(&database_url).await?;
        Ok(Self::from_connection(database_url, connection, lock_key))
    }

    pub fn from_connection(
        database_url: impl Into<String>,
        connection: PgConnection,
        lock_key: i64,
    ) -> Self {
        Self {
            database_url: database_url.into(),
            lock_key: LeaderLockKey(lock_key),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            connection: Mutex::new(Some(connection)),
        }
    }

    pub async fn run<F, Fut>(&self, work: F) -> Result<(), LeaderError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        if !self.try_acquire().await? {
            return Ok(());
        }

        let reclaimer = ConnectionReclaimer::new(self.heartbeat_interval);
        let work = work();
        tokio::pin!(work);

        let result = loop {
            tokio::select! {
                () = &mut work => break Ok(()),
                heartbeat = reclaimer.heartbeat_once(self) => {
                    if let Err(error) = heartbeat {
                        self.close_current().await.ok();
                        self.ensure_connected().await?;
                        crate::scheduler_metrics::record_leader_lost(1);
                        break Err(LeaderError::LockLost { reason: error.to_string() });
                    }
                }
            }
        };

        let release_result = self.release().await;
        match (result, release_result) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(_released)) => Ok(()),
        }
    }

    pub async fn try_acquire(&self) -> Result<bool, LeaderError> {
        self.ensure_connected().await?;
        let mut guard = self.connection.lock().await;
        let connection = guard.as_mut().ok_or(LeaderError::NotConnected)?;
        query_bool(connection, "SELECT pg_try_advisory_lock($1)", self.lock_key).await
    }

    pub async fn heartbeat(&self) -> Result<(), LeaderError> {
        self.ensure_connected().await?;
        let mut guard = self.connection.lock().await;
        let connection = guard.as_mut().ok_or(LeaderError::NotConnected)?;
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&mut *connection)
            .await?;
        Ok(())
    }

    pub async fn release(&self) -> Result<bool, LeaderError> {
        let mut guard = self.connection.lock().await;
        let Some(connection) = guard.as_mut() else {
            return Ok(false);
        };
        query_bool(connection, "SELECT pg_advisory_unlock($1)", self.lock_key).await
    }

    pub async fn close(&self) -> Result<(), LeaderError> {
        self.close_current().await
    }

    async fn ensure_connected(&self) -> Result<(), LeaderError> {
        if self.connection.lock().await.is_some() {
            return Ok(());
        }

        let connection = PgConnection::connect(&self.database_url).await?;
        let mut guard = self.connection.lock().await;
        if guard.is_none() {
            *guard = Some(connection);
        }
        Ok(())
    }

    async fn close_current(&self) -> Result<(), LeaderError> {
        let connection = self.connection.lock().await.take();
        let Some(connection) = connection else {
            return Ok(());
        };
        connection.close().await?;
        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaderLockKey(i64);

#[cfg(feature = "postgres")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionReclaimer {
    heartbeat_interval: Duration,
}

#[cfg(feature = "postgres")]
impl ConnectionReclaimer {
    pub const fn new(heartbeat_interval: Duration) -> Self {
        Self { heartbeat_interval }
    }

    pub async fn heartbeat_once(&self, leader: &PostgresLeaderElection) -> Result<(), LeaderError> {
        tokio::time::sleep(self.heartbeat_interval).await;
        leader.heartbeat().await
    }
}

#[cfg(feature = "postgres")]
async fn query_bool(
    connection: &mut PgConnection,
    query: &str,
    lock_key: LeaderLockKey,
) -> Result<bool, LeaderError> {
    sqlx::query_scalar::<_, bool>(query)
        .bind(lock_key.0)
        .fetch_one(connection)
        .await
        .map_err(LeaderError::Database)
}
