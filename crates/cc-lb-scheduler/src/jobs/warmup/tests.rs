#![allow(non_snake_case)]

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use uuid::Uuid;

use super::{UpstreamWarmupJob, UpstreamWarmupJobHandler, UpstreamWarmupOutcome};
use crate::{
    error::{Result, SchedulerError},
    jobs::warmup::set_after_dispatch_hook,
};

const CYCLE_KEY: u64 = 1_800_000_000;
const COMPLETED_AT_UNIX_SECS: u64 = 1_800_000_001;
type FireFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

static WARMUP_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

struct AfterDispatchHookTestGuard {
    _test_lock: tokio::sync::MutexGuard<'static, ()>,
}

async fn after_dispatch_hook_test_guard() -> AfterDispatchHookTestGuard {
    let test_lock = WARMUP_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    clear_after_dispatch_hook();
    AfterDispatchHookTestGuard {
        _test_lock: test_lock,
    }
}

impl Drop for AfterDispatchHookTestGuard {
    fn drop(&mut self) {
        clear_after_dispatch_hook();
    }
}

fn clear_after_dispatch_hook() {
    if let Some(lock) = super::AFTER_DISPATCH_HOOK.get() {
        *super::after_dispatch_hook_lock(lock) = None;
    }
}

#[tokio::test]
async fn t2__fires_when_upstream_is_live() -> Result<()> {
    let _test_guard = after_dispatch_hook_test_guard().await;
    let handler = UpstreamWarmupJobHandler::new();
    let job = UpstreamWarmupJob::new(Uuid::from_u128(1), CYCLE_KEY);
    let fired = Arc::new(AtomicUsize::new(0));

    let outcome = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(true) },
            incrementing_fire(Arc::clone(&fired)),
        )
        .await?;

    assert_eq!(outcome, UpstreamWarmupOutcome::Fired);
    assert_eq!(fired.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn t2__retries_after_crash_mid_fire() -> Result<()> {
    let _test_guard = after_dispatch_hook_test_guard().await;
    let handler = UpstreamWarmupJobHandler::new();
    let job = UpstreamWarmupJob::new(Uuid::from_u128(2), CYCLE_KEY);
    let fired = Arc::new(AtomicUsize::new(0));

    let failed = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(true) },
            failing_fire(Arc::clone(&fired)),
        )
        .await;
    let retried = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS + 1,
            |_| async { Ok(true) },
            incrementing_fire(Arc::clone(&fired)),
        )
        .await?;

    assert!(failed.is_err());
    assert_eq!(retried, UpstreamWarmupOutcome::Fired);
    assert_eq!(fired.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn t2__skips_deleted_upstream() -> Result<()> {
    let _test_guard = after_dispatch_hook_test_guard().await;
    let handler = UpstreamWarmupJobHandler::new();
    let job = UpstreamWarmupJob::new(Uuid::from_u128(3), CYCLE_KEY);
    let fired = Arc::new(AtomicUsize::new(0));

    let outcome = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(false) },
            incrementing_fire(Arc::clone(&fired)),
        )
        .await?;

    assert_eq!(outcome, UpstreamWarmupOutcome::UpstreamDeleted);
    assert_eq!(fired.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn t2__after_dispatch_hook_runs_before_success() -> Result<()> {
    let _test_guard = after_dispatch_hook_test_guard().await;
    let handler = UpstreamWarmupJobHandler::new();
    let job = UpstreamWarmupJob::new(Uuid::from_u128(4), CYCLE_KEY);
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    set_after_dispatch_hook(Box::new(move || {
        let hook_calls = Arc::clone(&hook_calls_for_hook);
        Box::pin(async move {
            hook_calls.fetch_add(1, Ordering::SeqCst);
        })
    }));

    let outcome = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(true) },
            incrementing_fire(Arc::new(AtomicUsize::new(0))),
        )
        .await?;

    assert_eq!(outcome, UpstreamWarmupOutcome::Fired);
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn after_dispatch_hook_test_guard_clears_unconsumed_hook() -> Result<()> {
    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);

    {
        let test_guard = after_dispatch_hook_test_guard().await;
        set_after_dispatch_hook(Box::new(move || {
            let hook_calls = Arc::clone(&hook_calls_for_hook);
            Box::pin(async move {
                hook_calls.fetch_add(1, Ordering::SeqCst);
            })
        }));
        drop(test_guard);
    }

    let _test_guard = after_dispatch_hook_test_guard().await;
    let handler = UpstreamWarmupJobHandler::new();
    let job = UpstreamWarmupJob::new(Uuid::from_u128(5), CYCLE_KEY);
    let outcome = handler
        .handle(
            job,
            COMPLETED_AT_UNIX_SECS,
            |_| async { Ok(true) },
            incrementing_fire(Arc::new(AtomicUsize::new(0))),
        )
        .await?;

    assert_eq!(outcome, UpstreamWarmupOutcome::Fired);
    assert_eq!(hook_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

fn incrementing_fire(
    fired: Arc<AtomicUsize>,
) -> impl FnOnce(UpstreamWarmupJob) -> FireFuture + Send {
    move |_| {
        Box::pin(async move {
            fired.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn failing_fire(fired: Arc<AtomicUsize>) -> impl FnOnce(UpstreamWarmupJob) -> FireFuture + Send {
    move |_| {
        Box::pin(async move {
            fired.fetch_add(1, Ordering::SeqCst);
            Err(SchedulerError::Job("crash mid-fire".to_owned()))
        })
    }
}
