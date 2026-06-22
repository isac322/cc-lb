use std::future::Future;
use std::pin::Pin;

use apalis::prelude::Data;

use crate::error::SchedulerError;
use crate::retry::JobOutcome;

use super::{EntityJob, SchedulerCtx, SingletonJob};

pub(super) type EntityHandlerFn =
    fn(
        EntityJob,
        Data<SchedulerCtx>,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

pub(super) fn entity_job_handler(
    job: EntityJob,
    ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    let dispatch = ctx.entity_dispatch.clone();
    Box::pin(async move { dispatch(job).await })
}

pub(super) type SingletonHandlerFn =
    fn(
        SingletonJob,
        Data<SchedulerCtx>,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

pub(super) fn singleton_job_handler(
    job: SingletonJob,
    ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    let dispatch = ctx.singleton_dispatch.clone();
    match &job {
        SingletonJob::AnthropicCompatRefresh(_) => {}
        SingletonJob::WarmupWatchdog(_) => {}
        SingletonJob::OAuthRefreshWatchdog(_) => {}
        SingletonJob::OAuthUsagePollWatchdog(_) => {}
        _ => {}
    }
    Box::pin(async move { dispatch(job).await })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use apalis::prelude::Data;

    use crate::jobs::metadata_refresh::MetadataRefreshJob;
    use crate::jobs::usage_prune::UsagePruneJob;
    use crate::retry::JobOutcome;
    use crate::worker::{EntityJob, SchedulerCtx, SingletonJob};

    use super::{entity_job_handler, singleton_job_handler};

    #[tokio::test]
    async fn entity_handler_invokes_ctx_dispatch_closure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let ctx = SchedulerCtx::new(
            Default::default(),
            Arc::new(move |_job| {
                observed.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(JobOutcome::Done) })
            }),
            Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
        );

        let outcome = entity_job_handler(
            EntityJob::MetadataRefresh(MetadataRefreshJob::new(uuid::Uuid::new_v4(), 1)),
            Data::new(ctx),
        )
        .await
        .expect("entity dispatch succeeds");

        assert_eq!(outcome, JobOutcome::Done);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn singleton_handler_invokes_ctx_dispatch_closure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let ctx = SchedulerCtx::new(
            Default::default(),
            Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
            Arc::new(move |_job| {
                observed.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Ok(JobOutcome::Retry {
                        delay: Duration::from_secs(7),
                    })
                })
            }),
        );

        let outcome = singleton_job_handler(
            SingletonJob::UsagePrune(UsagePruneJob::default()),
            Data::new(ctx),
        )
        .await
        .expect("singleton dispatch succeeds");

        assert_eq!(
            outcome,
            JobOutcome::Retry {
                delay: Duration::from_secs(7),
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
