use std::sync::Arc;

use cc_lb_engine::clock::unix_secs;
use cc_lb_pricing::LiteLlmLoader;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::apalis_housekeeping::{
    ApalisHousekeepingConfig, ApalisHousekeepingJob, ApalisHousekeepingJobHandler,
};
use cc_lb_scheduler::jobs::compat::{
    AnthropicCompatRefreshJob, handle_anthropic_compat_refresh_job_with_core_fetcher,
};
use cc_lb_scheduler::jobs::oauth_usage_poll::{
    NETWORK_FAILURE_STATUS, OAuthUsagePollCronJob, OAuthUsagePollObservation,
};
use cc_lb_scheduler::jobs::pool_quota_snapshot::PoolQuotaSnapshotCronJob;
use cc_lb_scheduler::jobs::price_catalog::{PriceCatalogRefreshJob, PriceCatalogRefreshJobHandler};
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJobHandler;
use cc_lb_scheduler::jobs::upstream_affinity_purge::{
    UpstreamAffinityPurgeJob, UpstreamAffinityPurgeJobHandler,
};
use cc_lb_scheduler::jobs::usage_prune::handle_usage_prune_job;
use cc_lb_scheduler::jobs::usage_rollup::handle_usage_rollup_job;
use cc_lb_scheduler::jobs::watchdog::{
    OAuthRefreshWatchdogJob, WarmupWatchdogJob, WatchdogEntityKind, WatchdogSeedStats,
    run_entity_watchdog,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::state_stores::{
    AnthropicCompatEtagsStore, OAuthUsagePollCursorsStore, PriceCatalogVersionsStore,
};
use cc_lb_scheduler::worker::{AdaptiveJob, CronJob, SchedulerBackend, SchedulerPushTask};
use uuid::Uuid;

use super::SchedulerDispatch;
use crate::scheduler_dispatch::outcomes::{
    apalis_housekeeping_outcome, price_catalog_outcome, prompt_cache_purge_outcome,
    upstream_affinity_purge_outcome, usage_prune_outcome, usage_rollup_outcome,
};
use crate::scheduler_dispatch::storage::{StorageHandle, storage_scheduler_error};

#[async_trait::async_trait]
pub(crate) trait SchedulerDispatchBackend: Send + Sync {
    async fn push_job(&self, job: AdaptiveJob) -> SchedulerResult<()>;

    async fn push_adaptive_task(&self, task: SchedulerPushTask<AdaptiveJob>)
    -> SchedulerResult<()>;

    async fn purge_upstream_affinity(
        &self,
        storage: Arc<dyn cc_lb_storage_api::Storage>,
        job: UpstreamAffinityPurgeJob,
        now_unix_secs: u64,
        ttl_secs: u64,
        cancel: tokio_util::sync::CancellationToken,
    ) -> SchedulerResult<JobOutcome>;

    async fn run_apalis_housekeeping(
        &self,
        job: ApalisHousekeepingJob,
        config: ApalisHousekeepingConfig,
        now_unix_secs: u64,
    ) -> SchedulerResult<JobOutcome>;

    async fn seed_watchdog(
        &self,
        kind: WatchdogEntityKind,
        upstream_ids: Vec<Uuid>,
        tick_unix_secs: u64,
        run_at_unix_secs: u64,
    ) -> SchedulerResult<WatchdogSeedStats>;

    async fn record_oauth_usage_attempt(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        status: i32,
    ) -> SchedulerResult<()>;

    async fn refresh_anthropic_compat(
        &self,
        job: AnthropicCompatRefreshJob,
        storage: Arc<dyn cc_lb_storage_api::Storage>,
        cancel: tokio_util::sync::CancellationToken,
        clock: cc_lb_engine::clock::ClockHandle,
    ) -> SchedulerResult<JobOutcome>;

    async fn refresh_price_catalog(
        &self,
        job: PriceCatalogRefreshJob,
        loader: LiteLlmLoader,
        now_unix_secs: u64,
    ) -> SchedulerResult<JobOutcome>;
}

#[async_trait::async_trait]
impl SchedulerDispatchBackend for SchedulerBackend {
    async fn push_job(&self, job: AdaptiveJob) -> SchedulerResult<()> {
        SchedulerBackend::push_job(self, job).await
    }

    async fn push_adaptive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        SchedulerBackend::push_adaptive_task(self, task).await
    }

    async fn purge_upstream_affinity(
        &self,
        storage: Arc<dyn cc_lb_storage_api::Storage>,
        job: UpstreamAffinityPurgeJob,
        now_unix_secs: u64,
        ttl_secs: u64,
        cancel: tokio_util::sync::CancellationToken,
    ) -> SchedulerResult<JobOutcome> {
        upstream_affinity_purge_outcome(
            UpstreamAffinityPurgeJobHandler::new(StorageHandle::new(storage))
                .handle(job, now_unix_secs, ttl_secs, &cancel)
                .await,
        )
    }

    async fn run_apalis_housekeeping(
        &self,
        job: ApalisHousekeepingJob,
        config: ApalisHousekeepingConfig,
        now_unix_secs: u64,
    ) -> SchedulerResult<JobOutcome> {
        match self {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => apalis_housekeeping_outcome(
                ApalisHousekeepingJobHandler::new(sqlite.pool().clone(), config)
                    .handle(job, now_unix_secs)
                    .await,
            ),
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => apalis_housekeeping_outcome(
                ApalisHousekeepingJobHandler::new(postgres.pool().clone(), config)
                    .handle(job, now_unix_secs)
                    .await,
            ),
        }
    }

    async fn seed_watchdog(
        &self,
        kind: WatchdogEntityKind,
        upstream_ids: Vec<Uuid>,
        tick_unix_secs: u64,
        run_at_unix_secs: u64,
    ) -> SchedulerResult<WatchdogSeedStats> {
        run_entity_watchdog(self, kind, &upstream_ids, tick_unix_secs, run_at_unix_secs).await
    }

    async fn record_oauth_usage_attempt(
        &self,
        upstream_id: Uuid,
        observed_at_unix_secs: u64,
        status: i32,
    ) -> SchedulerResult<()> {
        match self {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                OAuthUsagePollCursorsStore::new(sqlite.pool().clone())
                    .record_attempt(upstream_id, observed_at_unix_secs, status)
                    .await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                OAuthUsagePollCursorsStore::new(postgres.pool().clone())
                    .record_attempt(upstream_id, observed_at_unix_secs, status)
                    .await
            }
        }
    }

    async fn refresh_anthropic_compat(
        &self,
        job: AnthropicCompatRefreshJob,
        storage: Arc<dyn cc_lb_storage_api::Storage>,
        cancel: tokio_util::sync::CancellationToken,
        clock: cc_lb_engine::clock::ClockHandle,
    ) -> SchedulerResult<JobOutcome> {
        match self {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                handle_anthropic_compat_refresh_job_with_core_fetcher(
                    job,
                    &AnthropicCompatEtagsStore::new(sqlite.pool().clone()),
                    storage.as_ref(),
                    &cancel,
                    &*clock,
                )
                .await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                handle_anthropic_compat_refresh_job_with_core_fetcher(
                    job,
                    &AnthropicCompatEtagsStore::new(postgres.pool().clone()),
                    storage.as_ref(),
                    &cancel,
                    &*clock,
                )
                .await
            }
        }
    }

    async fn refresh_price_catalog(
        &self,
        job: PriceCatalogRefreshJob,
        loader: LiteLlmLoader,
        now_unix_secs: u64,
    ) -> SchedulerResult<JobOutcome> {
        match self {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => price_catalog_outcome(
                PriceCatalogRefreshJobHandler::new(
                    PriceCatalogVersionsStore::new(sqlite.pool().clone()),
                    loader,
                )
                .handle(job, now_unix_secs)
                .await,
            ),
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => price_catalog_outcome(
                PriceCatalogRefreshJobHandler::new(
                    PriceCatalogVersionsStore::new(postgres.pool().clone()),
                    loader,
                )
                .handle(job, now_unix_secs)
                .await,
            ),
        }
    }
}

impl SchedulerDispatch {
    pub(super) async fn dispatch_singleton(&self, job: CronJob) -> SchedulerResult<JobOutcome> {
        match job {
            CronJob::UsageRollup(job) => {
                usage_rollup_outcome(handle_usage_rollup_job(job, self.storage.as_ref()).await)
            }
            CronJob::UsagePrune(job) => {
                let result = handle_usage_prune_job(
                    job,
                    &StorageHandle::new(self.storage.clone()),
                    self.config.api_keys.usage_retention_days,
                    self.clock.clone(),
                    cc_lb_control::api_keys::limit_engine::LimitEngine::durable_usage_writer_inactive_after_secs(),
                    cc_lb_control::api_keys::limit_engine::LimitEngine::durable_usage_retention_secs(),
                    cc_lb_control::api_keys::limit_engine::LimitEngine::durable_usage_compaction_batch_size(),
                )
                .await
                .map_err(storage_scheduler_error)?;
                usage_prune_outcome(result)
            }
            CronJob::QuotaGc(_job) => {
                tracing::debug!(
                    "quota_gc retired; subscription-quota retention removed (ADR 0005)"
                );
                Ok(JobOutcome::Done)
            }
            CronJob::PromptCachePurge(job) => prompt_cache_purge_outcome(
                PromptCacheObservationPurgeJobHandler::new(StorageHandle::new(
                    self.storage.clone(),
                ))
                .handle(job, unix_secs(self.clock.now()))
                .await,
            ),
            CronJob::UpstreamAffinityPurge(job) => {
                self.backend
                    .purge_upstream_affinity(
                        self.storage.clone(),
                        job,
                        unix_secs(self.clock.now()),
                        self.config.upstream_affinity.ttl_secs(),
                        self.cancel.clone(),
                    )
                    .await
            }
            CronJob::PriceCatalogRefresh(job) => self.dispatch_price_catalog(job).await,
            CronJob::ApalisHousekeeping(job) => {
                self.backend
                    .run_apalis_housekeeping(
                        job,
                        ApalisHousekeepingConfig::new(self.config.scheduler.dlq_retention_days),
                        unix_secs(self.clock.now()),
                    )
                    .await
            }
            CronJob::WarmupWatchdog(job) => self.handle_warmup_watchdog(job).await,
            CronJob::OAuthRefreshWatchdog(job) => self.handle_oauth_refresh_watchdog(job).await,
            CronJob::OAuthUsagePoll(job) => self.handle_oauth_usage_poll(job).await,
            CronJob::AnthropicCompatRefresh(job) => self.dispatch_anthropic_compat(job).await,
            CronJob::PoolQuotaSnapshot(job) => self.handle_pool_quota_snapshot(job).await,
        }
    }

    async fn handle_pool_quota_snapshot(
        &self,
        job: PoolQuotaSnapshotCronJob,
    ) -> SchedulerResult<JobOutcome> {
        handle_pool_quota_snapshot(
            self.storage.as_ref(),
            self.dynamic_view.as_ref(),
            &*self.clock,
            job,
        )
        .await
    }
}

pub(crate) async fn handle_pool_quota_snapshot(
    storage: &dyn cc_lb_storage_api::Storage,
    dynamic_view: &cc_lb_engine::DynamicViewHolder,
    clock: &dyn cc_lb_engine::Clock,
    job: PoolQuotaSnapshotCronJob,
) -> SchedulerResult<JobOutcome> {
    use cc_lb_admin::subscription_quotas::{
        POOLED_HISTORY_WINDOWS, build_cc_lb_aggregate_response, record_pool_quota_snapshots_now,
    };
    use cc_lb_storage_api::SubscriptionQuotaSourceMerge;

    let view = dynamic_view.load();
    let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
    drop(view);

    let aggregate = match build_cc_lb_aggregate_response(
        storage,
        dynamic_view,
        None,
        POOLED_HISTORY_WINDOWS.to_vec(),
        SubscriptionQuotaSourceMerge::Merged,
        max_staleness_secs,
        clock,
    )
    .await
    {
        Ok(aggregate) => aggregate,
        Err(error) => {
            tracing::warn!(
                tick_unix_secs = job.tick_unix_secs,
                %error,
                "pool quota snapshot aggregate build failed",
            );
            return Ok(JobOutcome::Done);
        }
    };

    if let Err(error) = record_pool_quota_snapshots_now(storage, &aggregate, clock).await {
        tracing::warn!(
            tick_unix_secs = job.tick_unix_secs,
            %error,
            "pool quota snapshot persist failed",
        );
    }

    Ok(JobOutcome::Done)
}

impl SchedulerDispatch {
    async fn handle_warmup_watchdog(&self, job: WarmupWatchdogJob) -> SchedulerResult<JobOutcome> {
        let upstream_ids = self.list_warmup_watchdog_upstream_ids().await?;
        let stats = self
            .backend
            .seed_watchdog(
                WatchdogEntityKind::Warmup,
                upstream_ids,
                job.tick_unix_secs,
                unix_secs(self.clock.now()),
            )
            .await?;
        tracing::info!(
            tick_unix_secs = job.tick_unix_secs,
            seeded = stats.seeded,
            "warmup watchdog completed"
        );
        Ok(JobOutcome::Done)
    }

    async fn handle_oauth_refresh_watchdog(
        &self,
        job: OAuthRefreshWatchdogJob,
    ) -> SchedulerResult<JobOutcome> {
        let upstream_ids = self.list_oauth_watchdog_upstream_ids().await?;
        let stats = self
            .backend
            .seed_watchdog(
                WatchdogEntityKind::OAuthRefresh,
                upstream_ids,
                job.tick_unix_secs,
                unix_secs(self.clock.now()),
            )
            .await?;
        tracing::info!(
            tick_unix_secs = job.tick_unix_secs,
            seeded = stats.seeded,
            "oauth refresh watchdog completed"
        );
        Ok(JobOutcome::Done)
    }

    async fn handle_oauth_usage_poll(
        &self,
        job: OAuthUsagePollCronJob,
    ) -> SchedulerResult<JobOutcome> {
        let upstream_ids = self.list_oauth_watchdog_upstream_ids().await?;
        let total = upstream_ids.len();
        let traceparent = job.traceparent.as_deref();
        // Poll upstreams concurrently so one slow/hanging upstream cannot
        // consume the shared 60s tick budget and starve the rest (the prior
        // sequential loop did exactly that). Per-call/job timeouts unchanged.
        let results = futures_util::future::join_all(upstream_ids.into_iter().map(
            |upstream_id| async move {
                (
                    upstream_id,
                    self.poll_and_record_oauth_usage(upstream_id, traceparent)
                        .await,
                )
            },
        ))
        .await;
        let mut stats = OAuthUsagePollTickStats::default();
        for (upstream_id, result) in results {
            match result {
                Ok(label) => stats.record(label),
                Err(error) => {
                    stats.handler_err += 1;
                    tracing::warn!(%upstream_id, %error, "oauth usage poll failed; skipping until next tick");
                }
            }
        }
        tracing::info!(
            tick_unix_secs = job.tick_unix_secs,
            total,
            success = stats.success,
            throttled = stats.throttled,
            status_failure = stats.status_failure,
            network_failure = stats.network_failure,
            skipped = stats.skipped,
            handler_err = stats.handler_err,
            "oauth usage poll tick complete"
        );
        Ok(JobOutcome::Done)
    }

    async fn poll_and_record_oauth_usage(
        &self,
        upstream_id: Uuid,
        traceparent: Option<&str>,
    ) -> SchedulerResult<OAuthUsagePollOutcomeLabel> {
        let observation = self.poll_usage(upstream_id, traceparent).await?;
        let label = OAuthUsagePollOutcomeLabel::from_observation(&observation);
        self.record_oauth_usage_observation(upstream_id, observation)
            .await?;
        Ok(label)
    }

    async fn record_oauth_usage_observation(
        &self,
        upstream_id: Uuid,
        observation: OAuthUsagePollObservation,
    ) -> SchedulerResult<()> {
        let Some(update) = OAuthUsageCursorUpdate::from_observation(upstream_id, observation)
        else {
            return Ok(());
        };
        self.backend
            .record_oauth_usage_attempt(
                update.upstream_id,
                update.observed_at_unix_secs,
                update.status,
            )
            .await
    }

    async fn dispatch_anthropic_compat(
        &self,
        job: AnthropicCompatRefreshJob,
    ) -> SchedulerResult<JobOutcome> {
        self.backend
            .refresh_anthropic_compat(
                job,
                self.storage.clone(),
                self.cancel.clone(),
                self.clock.clone(),
            )
            .await
    }

    async fn dispatch_price_catalog(
        &self,
        job: cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJob,
    ) -> SchedulerResult<JobOutcome> {
        let loader = LiteLlmLoader::new(
            self.price_catalog.clone(),
            self.storage.clone(),
            self.config.api_keys.price_catalog.url.clone(),
            self.config.api_keys.price_catalog.refresh_interval,
            self.config.api_keys.price_catalog.cache_path.clone(),
            self.clock.clone(),
        );
        self.backend
            .refresh_price_catalog(job, loader, unix_secs(self.clock.now()))
            .await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OAuthUsageCursorUpdate {
    upstream_id: Uuid,
    observed_at_unix_secs: u64,
    status: i32,
}

impl OAuthUsageCursorUpdate {
    fn from_observation(upstream_id: Uuid, observation: OAuthUsagePollObservation) -> Option<Self> {
        match observation {
            OAuthUsagePollObservation::Skip => None,
            OAuthUsagePollObservation::Success {
                observed_at_unix_secs,
                ..
            } => Some(Self {
                upstream_id,
                observed_at_unix_secs,
                status: 200,
            }),
            OAuthUsagePollObservation::Throttled {
                observed_at_unix_secs,
            } => Some(Self {
                upstream_id,
                observed_at_unix_secs,
                status: 429,
            }),
            OAuthUsagePollObservation::StatusFailure {
                observed_at_unix_secs,
                status,
            } => Some(Self {
                upstream_id,
                observed_at_unix_secs,
                status: i32::from(status),
            }),
            OAuthUsagePollObservation::NetworkFailure {
                observed_at_unix_secs,
            } => Some(Self {
                upstream_id,
                observed_at_unix_secs,
                status: NETWORK_FAILURE_STATUS,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct OAuthUsagePollTickStats {
    success: usize,
    throttled: usize,
    status_failure: usize,
    network_failure: usize,
    skipped: usize,
    handler_err: usize,
}

impl OAuthUsagePollTickStats {
    fn record(&mut self, label: OAuthUsagePollOutcomeLabel) {
        match label {
            OAuthUsagePollOutcomeLabel::Success => self.success += 1,
            OAuthUsagePollOutcomeLabel::Throttled => self.throttled += 1,
            OAuthUsagePollOutcomeLabel::StatusFailure => self.status_failure += 1,
            OAuthUsagePollOutcomeLabel::NetworkFailure => self.network_failure += 1,
            OAuthUsagePollOutcomeLabel::Skipped => self.skipped += 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OAuthUsagePollOutcomeLabel {
    Success,
    Throttled,
    StatusFailure,
    NetworkFailure,
    Skipped,
}

impl OAuthUsagePollOutcomeLabel {
    fn from_observation(observation: &OAuthUsagePollObservation) -> Self {
        match observation {
            OAuthUsagePollObservation::Success { .. } => Self::Success,
            OAuthUsagePollObservation::Throttled { .. } => Self::Throttled,
            OAuthUsagePollObservation::StatusFailure { .. } => Self::StatusFailure,
            OAuthUsagePollObservation::NetworkFailure { .. } => Self::NetworkFailure,
            OAuthUsagePollObservation::Skip => Self::Skipped,
        }
    }
}
