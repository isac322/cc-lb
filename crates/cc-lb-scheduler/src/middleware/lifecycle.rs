use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use apalis_core::task::Task;
use tower::{Layer, Service};

use crate::retry::JobOutcome;
use crate::scheduler_metrics;
use crate::worker::{AdaptiveJob, CronJob};

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

impl SchedulerMetricPayload for AdaptiveJob {
    fn scheduler_job_type(&self) -> &'static str {
        match self {
            Self::Warmup(_) => "upstream_warmup",
            Self::OAuthRefresh(_) => "adaptive:oauth_refresh",
            Self::OAuthUsagePoll(_) => "adaptive:oauth_usage_poll",
            Self::MetadataRefresh(_) => "adaptive:metadata_refresh",
        }
    }
}

impl SchedulerMetricPayload for CronJob {
    fn scheduler_job_type(&self) -> &'static str {
        match self {
            Self::UsageRollup(_) => "cron:usage_rollup",
            Self::UsagePrune(_) => "cron:usage_prune",
            Self::QuotaGc(_) => "cron:quota_gc",
            Self::PromptCachePurge(_) => "cron:prompt_cache_purge",
            Self::PriceCatalogRefresh(_) => "cron:price_catalog_refresh",
            Self::ApalisHousekeeping(_) => "cron:apalis_housekeeping",
            Self::WarmupWatchdog(_) => "cron:warmup_watchdog",
            Self::OAuthRefreshWatchdog(_) => "cron:oauth_refresh_watchdog",
            Self::OAuthUsagePollWatchdog(_) => "cron:oauth_usage_poll_watchdog",
            Self::AnthropicCompatRefresh(_) => "cron:anthropic_compat_refresh",
        }
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
