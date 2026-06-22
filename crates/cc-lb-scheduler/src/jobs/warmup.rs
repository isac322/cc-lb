use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, OnceLock};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;

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

    pub fn idempotency_key(&self, cycle_secs: u64) -> String {
        format!("entity:warmup:{}:{cycle_secs}", self.upstream_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpstreamWarmupOutcome {
    Fired,
    AlreadyCompleted,
    UpstreamDeleted,
}

#[doc(hidden)]
pub type HookFn = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

static AFTER_DISPATCH_HOOK: OnceLock<Mutex<Option<HookFn>>> = OnceLock::new();

#[doc(hidden)]
pub fn set_after_dispatch_hook(hook: HookFn) {
    let lock = AFTER_DISPATCH_HOOK.get_or_init(|| Mutex::new(None));
    *after_dispatch_hook_lock(lock) = Some(hook);
}

#[derive(Clone, Debug, Default)]
pub struct UpstreamWarmupJobHandler;

impl UpstreamWarmupJobHandler {
    pub const fn new() -> Self {
        Self
    }
}

impl UpstreamWarmupJobHandler {
    pub async fn handle<Lookup, LookupFuture, Fire, FireFuture>(
        &self,
        job: UpstreamWarmupJob,
        _completed_at_unix_secs: u64,
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

        fire(job).await?;

        if let Some(lock) = AFTER_DISPATCH_HOOK.get() {
            let hook_opt = {
                let mut guard = after_dispatch_hook_lock(lock);
                guard.take()
            };
            if let Some(hook) = hook_opt {
                hook().await;
            }
        }

        Ok(UpstreamWarmupOutcome::Fired)
    }
}

fn after_dispatch_hook_lock(
    lock: &'static Mutex<Option<HookFn>>,
) -> MutexGuard<'static, Option<HookFn>> {
    match lock.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests;
