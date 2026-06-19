use std::future::Future;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{error::Result, idempotency::WarmupEffectsStore};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UpstreamWarmupJob {
    pub upstream_id: Uuid,
    pub cycle_key: u64,
}

impl UpstreamWarmupJob {
    pub const fn new(upstream_id: Uuid, cycle_key: u64) -> Self {
        Self {
            upstream_id,
            cycle_key,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpstreamWarmupOutcome {
    Fired,
    AlreadyCompleted,
    UpstreamDeleted,
}

#[cfg(any(test, debug_assertions))]
type HookFn = Box<dyn Fn() -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[cfg(any(test, debug_assertions))]
static AFTER_DISPATCH_HOOK: std::sync::Mutex<Option<HookFn>> = std::sync::Mutex::new(None);

#[cfg(any(test, debug_assertions))]
pub fn set_after_dispatch_hook(hook: HookFn) {
    *after_dispatch_hook_lock() = Some(hook);
}

#[derive(Clone, Debug)]
pub struct UpstreamWarmupJobHandler<Effects> {
    effects: Effects,
}

impl<Effects> UpstreamWarmupJobHandler<Effects> {
    pub const fn new(effects: Effects) -> Self {
        Self { effects }
    }
}

impl<Effects> UpstreamWarmupJobHandler<Effects>
where
    Effects: Send + Sync + WarmupCycleEffects,
{
    pub async fn handle<Lookup, LookupFuture, Fire, FireFuture>(
        &self,
        job: UpstreamWarmupJob,
        completed_at_unix_secs: u64,
        upstream_is_live: Lookup,
        fire: Fire,
    ) -> Result<UpstreamWarmupOutcome>
    where
        Lookup: FnOnce(Uuid) -> LookupFuture + Send,
        LookupFuture: Future<Output = Result<bool>> + Send,
        Fire: FnOnce(UpstreamWarmupJob) -> FireFuture + Send,
        FireFuture: Future<Output = Result<()>> + Send,
    {
        if !upstream_is_live(job.upstream_id).await? {
            return Ok(UpstreamWarmupOutcome::UpstreamDeleted);
        }
        if self
            .effects
            .is_already_done(job.upstream_id, job.cycle_key)
            .await?
        {
            return Ok(UpstreamWarmupOutcome::AlreadyCompleted);
        }

        fire(job).await?;

        #[cfg(any(test, debug_assertions))]
        {
            let hook_opt = {
                let mut lock = after_dispatch_hook_lock();
                lock.take()
            };
            if let Some(hook) = hook_opt {
                hook().await;
            }
        }

        if self
            .effects
            .try_acquire_cycle(job.upstream_id, job.cycle_key, completed_at_unix_secs)
            .await?
        {
            Ok(UpstreamWarmupOutcome::Fired)
        } else {
            Ok(UpstreamWarmupOutcome::AlreadyCompleted)
        }
    }
}

#[cfg(any(test, debug_assertions))]
fn after_dispatch_hook_lock() -> std::sync::MutexGuard<'static, Option<HookFn>> {
    match AFTER_DISPATCH_HOOK.lock() {
        Ok(lock) => lock,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub trait WarmupCycleEffects {
    fn try_acquire_cycle(
        &self,
        upstream_id: Uuid,
        cycle_key: u64,
        completed_at_unix_secs: u64,
    ) -> impl Future<Output = Result<bool>> + Send + '_;

    fn is_already_done(
        &self,
        upstream_id: Uuid,
        cycle_key: u64,
    ) -> impl Future<Output = Result<bool>> + Send + '_;
}

#[cfg(feature = "sqlite")]
impl WarmupCycleEffects for WarmupEffectsStore<sqlx::Sqlite> {
    async fn try_acquire_cycle(
        &self,
        upstream_id: Uuid,
        cycle_key: u64,
        completed_at_unix_secs: u64,
    ) -> Result<bool> {
        WarmupEffectsStore::<sqlx::Sqlite>::try_acquire_cycle(
            self,
            upstream_id,
            cycle_key,
            completed_at_unix_secs,
        )
        .await
    }

    async fn is_already_done(&self, upstream_id: Uuid, cycle_key: u64) -> Result<bool> {
        WarmupEffectsStore::<sqlx::Sqlite>::is_already_done(self, upstream_id, cycle_key).await
    }
}

#[cfg(feature = "postgres")]
impl WarmupCycleEffects for WarmupEffectsStore<sqlx::Postgres> {
    async fn try_acquire_cycle(
        &self,
        upstream_id: Uuid,
        cycle_key: u64,
        completed_at_unix_secs: u64,
    ) -> Result<bool> {
        WarmupEffectsStore::<sqlx::Postgres>::try_acquire_cycle(
            self,
            upstream_id,
            cycle_key,
            completed_at_unix_secs,
        )
        .await
    }

    async fn is_already_done(&self, upstream_id: Uuid, cycle_key: u64) -> Result<bool> {
        WarmupEffectsStore::<sqlx::Postgres>::is_already_done(self, upstream_id, cycle_key).await
    }
}

#[cfg(test)]
mod tests;
