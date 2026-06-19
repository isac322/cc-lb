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
    _job: EntityJob,
    _ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    Box::pin(async move { Ok(JobOutcome::Done) })
}

pub(super) type SingletonHandlerFn =
    fn(
        SingletonJob,
        Data<SchedulerCtx>,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>>;

pub(super) fn singleton_job_handler(
    _job: SingletonJob,
    _ctx: Data<SchedulerCtx>,
) -> Pin<Box<dyn Future<Output = Result<JobOutcome, SchedulerError>> + Send>> {
    Box::pin(async move { Ok(JobOutcome::Done) })
}
