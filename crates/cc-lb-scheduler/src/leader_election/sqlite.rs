use std::future::Future;

use super::LeaderError;

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
