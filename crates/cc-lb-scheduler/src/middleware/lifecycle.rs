use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use apalis_core::task::Task;
use tower::{Layer, Service};

use crate::jobs::reconcile::SchedulerReconcileJob;
use crate::retry::JobOutcome;
use crate::scheduler_metrics;
use crate::worker::{EntityJob, SingletonJob};

type MetricsFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobOutcomeStatus {
    Started,
    Done,
    Retry,
    Skip,
    Panicked,
    DuplicateEffect,
    Noop,
}

impl JobOutcomeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Done => "done",
            Self::Retry => "retry",
            Self::Skip => "skip",
            Self::Panicked => "panicked",
            Self::DuplicateEffect => "duplicate_effect",
            Self::Noop => "noop",
        }
    }
}

pub trait SchedulerMetricPayload {
    fn scheduler_job_type(&self) -> &'static str;
}

impl<P, Ctx, IdType> SchedulerMetricPayload for Task<P, Ctx, IdType>
where
    P: SchedulerMetricPayload,
{
    fn scheduler_job_type(&self) -> &'static str {
        self.args.scheduler_job_type()
    }
}

impl SchedulerMetricPayload for EntityJob {
    fn scheduler_job_type(&self) -> &'static str {
        match self {
            Self::Warmup(_) => "entity:warmup",
            Self::OAuthRefresh(_) => "entity:oauth_refresh",
            Self::OAuthUsagePoll(_) => "entity:oauth_usage_poll",
            Self::AnthropicCompatRefresh(_) => "entity:anthropic_compat_refresh",
            Self::MetadataRefresh(_) => "entity:metadata_refresh",
        }
    }
}

impl SchedulerMetricPayload for SingletonJob {
    fn scheduler_job_type(&self) -> &'static str {
        match self {
            Self::UsageRollup(_) => "singleton:usage_rollup",
            Self::UsagePrune(_) => "singleton:usage_prune",
            Self::QuotaGc(_) => "singleton:quota_gc",
            Self::PromptCachePurge(_) => "singleton:prompt_cache_purge",
            Self::PriceCatalogRefresh(_) => "singleton:price_catalog_refresh",
            Self::ApalisHousekeeping(_) => "singleton:apalis_housekeeping",
        }
    }
}

impl SchedulerMetricPayload for SchedulerReconcileJob {
    fn scheduler_job_type(&self) -> &'static str {
        "scheduler_reconcile"
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SchedulerMetricsLayer;

impl SchedulerMetricsLayer {
    pub const fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for SchedulerMetricsLayer {
    type Service = SchedulerMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SchedulerMetricsService::new(inner)
    }
}

#[derive(Clone, Debug)]
pub struct SchedulerMetricsService<S> {
    inner: S,
}

impl<S> SchedulerMetricsService<S> {
    pub const fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, P> Service<P> for SchedulerMetricsService<S>
where
    P: SchedulerMetricPayload + Send + 'static,
    S: Service<P, Response = JobOutcome>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = MetricsFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, payload: P) -> Self::Future {
        let job_type = payload.scheduler_job_type();
        scheduler_metrics::record_scheduler_job_status(
            job_type,
            JobOutcomeStatus::Started.as_str(),
        );
        let started_at = Instant::now();
        let future = self.inner.call(payload);

        Box::pin(async move {
            match future.await {
                Ok(outcome) => {
                    scheduler_metrics::record_scheduler_job_status(
                        job_type,
                        outcome.metric_status().as_str(),
                    );
                    scheduler_metrics::record_scheduler_job_duration(
                        job_type,
                        started_at.elapsed(),
                    );
                    if outcome.is_terminal_failure() {
                        scheduler_metrics::record_scheduler_failure(job_type, 1);
                    }
                    Ok(outcome)
                }
                Err(error) => {
                    scheduler_metrics::record_scheduler_job_status(
                        job_type,
                        JobOutcomeStatus::Panicked.as_str(),
                    );
                    scheduler_metrics::record_scheduler_job_duration(
                        job_type,
                        started_at.elapsed(),
                    );
                    Err(error)
                }
            }
        })
    }
}
