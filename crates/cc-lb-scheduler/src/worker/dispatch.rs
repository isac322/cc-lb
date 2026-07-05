use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use apalis::prelude::Data;

use crate::error::SchedulerError;
use crate::retry::JobOutcome;

use super::{AdaptiveJob, CronJob, SchedulerCtx};

pub(super) const SINGLETON_TIMEOUT: Duration = Duration::from_secs(60);
pub(super) const ENTITY_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) type EntityHandlerFn =
    fn(
        AdaptiveJob,
        Data<SchedulerCtx>,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

pub(super) fn entity_job_handler(
    job: AdaptiveJob,
    ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    let dispatch = ctx.adaptive_dispatch.clone();
    let job_kind = job.kind();
    Box::pin(async move {
        match tokio::time::timeout(ENTITY_TIMEOUT, dispatch(job)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(SchedulerError::JobTimeout {
                job_kind,
                timeout: ENTITY_TIMEOUT,
            }),
        }
    })
}

pub(super) type SingletonHandlerFn =
    fn(
        CronJob,
        Data<SchedulerCtx>,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

pub(super) fn singleton_job_handler(
    job: CronJob,
    ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    let dispatch = ctx.cron_dispatch.clone();
    let job_kind = job.kind();
    Box::pin(async move {
        match tokio::time::timeout(SINGLETON_TIMEOUT, dispatch(job)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(SchedulerError::JobTimeout {
                job_kind,
                timeout: SINGLETON_TIMEOUT,
            }),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use apalis::prelude::Data;
    use cc_lb_clock::SystemClock;

    use crate::error::SchedulerError;
    use crate::jobs::metadata_refresh::MetadataRefreshJob;
    use crate::jobs::usage_prune::UsagePruneJob;
    use crate::retry::JobOutcome;
    use crate::worker::{AdaptiveJob, CronJob, SchedulerCtx};

    use super::{ENTITY_TIMEOUT, SINGLETON_TIMEOUT, entity_job_handler, singleton_job_handler};

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
            Arc::new(SystemClock),
        );

        let outcome = entity_job_handler(
            AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(uuid::Uuid::new_v4(), 1)),
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
            Arc::new(SystemClock),
        );

        let outcome = singleton_job_handler(
            CronJob::UsagePrune(UsagePruneJob::default()),
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

    #[tokio::test(start_paused = true)]
    async fn singleton_handler_maps_dispatch_hang_to_job_timeout() {
        let ctx = SchedulerCtx::new(
            Default::default(),
            Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
            Arc::new(|_job| {
                Box::pin(async {
                    tokio::time::sleep(SINGLETON_TIMEOUT + Duration::from_secs(5)).await;
                    Ok(JobOutcome::Done)
                })
            }),
            Arc::new(SystemClock),
        );

        let err = singleton_job_handler(
            CronJob::UsagePrune(UsagePruneJob::default()),
            Data::new(ctx),
        )
        .await
        .expect_err("singleton handler must fail when dispatch hangs past SINGLETON_TIMEOUT");

        match err {
            SchedulerError::JobTimeout { job_kind, timeout } => {
                assert_eq!(job_kind, "usage_prune");
                assert_eq!(timeout, SINGLETON_TIMEOUT);
            }
            other => panic!("expected SchedulerError::JobTimeout, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn entity_handler_maps_dispatch_hang_to_job_timeout() {
        let ctx = SchedulerCtx::new(
            Default::default(),
            Arc::new(|_job| {
                Box::pin(async {
                    tokio::time::sleep(ENTITY_TIMEOUT + Duration::from_secs(5)).await;
                    Ok(JobOutcome::Done)
                })
            }),
            Arc::new(|_job| Box::pin(async { Ok(JobOutcome::Done) })),
            Arc::new(SystemClock),
        );

        let err = entity_job_handler(
            AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(uuid::Uuid::new_v4(), 1)),
            Data::new(ctx),
        )
        .await
        .expect_err("entity handler must fail when dispatch hangs past ENTITY_TIMEOUT");

        match err {
            SchedulerError::JobTimeout { job_kind, timeout } => {
                assert_eq!(job_kind, "metadata_refresh");
                assert_eq!(timeout, ENTITY_TIMEOUT);
            }
            other => panic!("expected SchedulerError::JobTimeout, got {other:?}"),
        }
    }
}
