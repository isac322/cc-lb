use cc_lb_pricing::LiteLlmLoader;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::apalis_housekeeping::{
    ApalisHousekeepingConfig, ApalisHousekeepingJobHandler,
};
use cc_lb_scheduler::jobs::compat::{
    AnthropicCompatRefreshJob, handle_anthropic_compat_refresh_job_with_core_fetcher,
};
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJobHandler;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJobHandler;
use cc_lb_scheduler::jobs::quota_gc::{SubscriptionQuotaGcConfig, SubscriptionQuotaGcJobHandler};
use cc_lb_scheduler::jobs::usage_prune::handle_usage_prune_job;
use cc_lb_scheduler::jobs::usage_rollup::handle_usage_rollup_job;
use cc_lb_scheduler::jobs::watchdog::{
    OAuthRefreshWatchdogJob, OAuthUsagePollWatchdogJob, WarmupWatchdogJob, WatchdogEntityKind,
    run_entity_watchdog,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::state_stores::{AnthropicCompatEtagsStore, PriceCatalogVersionsStore};
use cc_lb_scheduler::worker::{SchedulerBackend, CronJob};

use crate::scheduler_dispatch::outcomes::{
    apalis_housekeeping_outcome, price_catalog_outcome, prompt_cache_purge_outcome,
    quota_gc_outcome, usage_prune_outcome, usage_rollup_outcome,
};
use crate::scheduler_dispatch::storage::StorageHandle;
use crate::scheduler_dispatch::time::now_unix_secs;

use super::SchedulerDispatch;

impl SchedulerDispatch {
    pub(super) async fn dispatch_singleton(
        &self,
        job: CronJob,
    ) -> SchedulerResult<JobOutcome> {
        match job {
            CronJob::UsageRollup(job) => {
                usage_rollup_outcome(handle_usage_rollup_job(job, self.storage.as_ref()).await)
            }
            CronJob::UsagePrune(job) => usage_prune_outcome(
                handle_usage_prune_job(
                    job,
                    &StorageHandle::new(self.storage.clone()),
                    self.config.api_keys.usage_retention_days,
                )
                .await,
            ),
            CronJob::QuotaGc(job) => {
                let config = SubscriptionQuotaGcConfig::new(
                    self.config.subscription_quota.retention_days,
                    self.config.subscription_quota.gc_batch_size,
                );
                quota_gc_outcome(
                    SubscriptionQuotaGcJobHandler::new(
                        StorageHandle::new(self.storage.clone()),
                        config,
                    )
                    .handle(job, now_unix_secs())
                    .await,
                )
            }
            CronJob::PromptCachePurge(job) => prompt_cache_purge_outcome(
                PromptCacheObservationPurgeJobHandler::new(StorageHandle::new(
                    self.storage.clone(),
                ))
                .handle(job, now_unix_secs())
                .await,
            ),
            CronJob::PriceCatalogRefresh(job) => self.dispatch_price_catalog(job).await,
            CronJob::ApalisHousekeeping(job) => match &self.backend {
                #[cfg(feature = "sqlite")]
                SchedulerBackend::Sqlite(sqlite) => {
                    let config =
                        ApalisHousekeepingConfig::new(self.config.scheduler.dlq_retention_days);
                    apalis_housekeeping_outcome(
                        ApalisHousekeepingJobHandler::new(sqlite.pool.clone(), config)
                            .handle(job, now_unix_secs())
                            .await,
                    )
                }
                #[cfg(feature = "postgres")]
                SchedulerBackend::Postgres(postgres) => {
                    let config =
                        ApalisHousekeepingConfig::new(self.config.scheduler.dlq_retention_days);
                    apalis_housekeeping_outcome(
                        ApalisHousekeepingJobHandler::new(postgres.pool.clone(), config)
                            .handle(job, now_unix_secs())
                            .await,
                    )
                }
            },
            CronJob::WarmupWatchdog(job) => self.handle_warmup_watchdog(job).await,
            CronJob::OAuthRefreshWatchdog(job) => {
                self.handle_oauth_refresh_watchdog(job).await
            }
            CronJob::OAuthUsagePollWatchdog(job) => {
                self.handle_oauth_usage_poll_watchdog(job).await
            }
            CronJob::AnthropicCompatRefresh(job) => self.dispatch_anthropic_compat(job).await,
        }
    }

    async fn handle_warmup_watchdog(&self, job: WarmupWatchdogJob) -> SchedulerResult<JobOutcome> {
        let upstream_ids = self.list_warmup_watchdog_upstream_ids().await?;
        let stats = run_entity_watchdog(
            &self.backend,
            WatchdogEntityKind::Warmup,
            &upstream_ids,
            job.tick_unix_secs,
            now_unix_secs(),
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
        let stats = run_entity_watchdog(
            &self.backend,
            WatchdogEntityKind::OAuthRefresh,
            &upstream_ids,
            job.tick_unix_secs,
            now_unix_secs(),
        )
        .await?;
        tracing::info!(
            tick_unix_secs = job.tick_unix_secs,
            seeded = stats.seeded,
            "oauth refresh watchdog completed"
        );
        Ok(JobOutcome::Done)
    }

    async fn handle_oauth_usage_poll_watchdog(
        &self,
        job: OAuthUsagePollWatchdogJob,
    ) -> SchedulerResult<JobOutcome> {
        let upstream_ids = self.list_oauth_watchdog_upstream_ids().await?;
        let stats = run_entity_watchdog(
            &self.backend,
            WatchdogEntityKind::OAuthUsagePoll,
            &upstream_ids,
            job.tick_unix_secs,
            now_unix_secs(),
        )
        .await?;
        tracing::info!(
            tick_unix_secs = job.tick_unix_secs,
            seeded = stats.seeded,
            "oauth usage poll watchdog completed"
        );
        Ok(JobOutcome::Done)
    }

    async fn dispatch_anthropic_compat(
        &self,
        job: AnthropicCompatRefreshJob,
    ) -> SchedulerResult<JobOutcome> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                handle_anthropic_compat_refresh_job_with_core_fetcher(
                    job,
                    &AnthropicCompatEtagsStore::new(sqlite.pool.clone()),
                    self.storage.as_ref(),
                    &self.cancel,
                )
                .await
            }
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => {
                handle_anthropic_compat_refresh_job_with_core_fetcher(
                    job,
                    &AnthropicCompatEtagsStore::new(postgres.pool.clone()),
                    self.storage.as_ref(),
                    &self.cancel,
                )
                .await
            }
        }
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
        );
        match &self.backend {
            #[cfg(feature = "sqlite")]
            SchedulerBackend::Sqlite(sqlite) => price_catalog_outcome(
                PriceCatalogRefreshJobHandler::new(
                    PriceCatalogVersionsStore::new(sqlite.pool.clone()),
                    loader,
                )
                .handle(job, now_unix_secs())
                .await,
            ),
            #[cfg(feature = "postgres")]
            SchedulerBackend::Postgres(postgres) => price_catalog_outcome(
                PriceCatalogRefreshJobHandler::new(
                    PriceCatalogVersionsStore::new(postgres.pool.clone()),
                    loader,
                )
                .handle(job, now_unix_secs())
                .await,
            ),
        }
    }
}
