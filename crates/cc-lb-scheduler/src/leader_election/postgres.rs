use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use sqlx::Connection as _;
use sqlx::postgres::PgConnection;
use tokio::sync::Mutex;

use super::{LeaderError, LeaderState};

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const LEADER_STATE_FOLLOWER: u8 = 0;
const LEADER_STATE_LEADER: u8 = 1;

#[derive(Debug)]
pub struct PostgresLeaderElection {
    database_url: String,
    lock_key: LeaderLockKey,
    heartbeat_interval: Duration,
    connection: Mutex<Option<PgConnection>>,
    state: AtomicU8,
}

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
            state: AtomicU8::new(LEADER_STATE_FOLLOWER),
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
        let acquired =
            query_bool(connection, "SELECT pg_try_advisory_lock($1)", self.lock_key).await?;
        self.state.store(
            if acquired {
                LEADER_STATE_LEADER
            } else {
                LEADER_STATE_FOLLOWER
            },
            Ordering::SeqCst,
        );
        Ok(acquired)
    }

    pub async fn heartbeat(&self) -> Result<(), LeaderError> {
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
        let released =
            query_bool(connection, "SELECT pg_advisory_unlock($1)", self.lock_key).await?;
        self.state.store(LEADER_STATE_FOLLOWER, Ordering::SeqCst);
        Ok(released)
    }

    pub async fn close(&self) -> Result<(), LeaderError> {
        self.close_current().await
    }

    pub fn current_state(&self) -> LeaderState {
        match self.state.load(Ordering::SeqCst) {
            LEADER_STATE_LEADER => LeaderState::Leader,
            LEADER_STATE_FOLLOWER => LeaderState::Follower,
            _unknown => LeaderState::Follower,
        }
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
            self.state.store(LEADER_STATE_FOLLOWER, Ordering::SeqCst);
            return Ok(());
        };
        connection.close().await?;
        self.state.store(LEADER_STATE_FOLLOWER, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaderLockKey(i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionReclaimer {
    heartbeat_interval: Duration,
}

impl ConnectionReclaimer {
    pub const fn new(heartbeat_interval: Duration) -> Self {
        Self { heartbeat_interval }
    }

    pub async fn heartbeat_once(&self, leader: &PostgresLeaderElection) -> Result<(), LeaderError> {
        tokio::time::sleep(self.heartbeat_interval).await;
        leader.heartbeat().await
    }
}

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
