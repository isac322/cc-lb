use cc_lb_clock::unix_secs;
use cc_lb_pricing::LiteLlmLoader;
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::jobs::apalis_housekeeping::{
    ApalisHousekeepingConfig, ApalisHousekeepingJobHandler,
};
use cc_lb_scheduler::jobs::compat::handle_anthropic_compat_refresh_job_with_core_fetcher;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::{
    NETWORK_FAILURE_STATUS, OAuthUsagePollCronJob, OAuthUsagePollObservation,
};
use cc_lb_scheduler::jobs::pool_quota_snapshot::PoolQuotaSnapshotCronJob;
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJobHandler;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJobHandler;
use cc_lb_scheduler::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJobHandler;
use cc_lb_scheduler::jobs::usage_prune::{
    UsagePruneJob, UsagePruneJobResult, handle_usage_prune_job,
};
use cc_lb_scheduler::jobs::usage_rollup::handle_usage_rollup_job;
use cc_lb_scheduler::jobs::watchdog::{
    OAuthRefreshWatchdogJob, WarmupWatchdogJob, WatchdogEntityKind, run_entity_watchdog,
    run_oauth_refresh_watchdog,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::state_stores::{
    AnthropicCompatEtagsStore, OAuthUsagePollCursorsStore, PriceCatalogVersionsStore,
};
use cc_lb_scheduler::worker::{CronJob, SchedulerBackend, SchedulerPushTask};
use cc_lb_storage_api::WarmupLane;
use uuid::Uuid;

use super::SchedulerDispatch;
use crate::scheduler_dispatch::outcomes::{
    apalis_housekeeping_outcome, price_catalog_outcome, prompt_cache_purge_outcome,
    upstream_affinity_purge_outcome, usage_prune_outcome, usage_rollup_outcome,
};
use crate::scheduler_dispatch::storage::{StorageHandle, storage_scheduler_error};

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
                    self.config.request_event_retention_days,
                    self.clock.clone(),
                    cc_lb_control::api_keys::limit_engine::LimitEngine::durable_usage_writer_inactive_after_secs(),
                    cc_lb_control::api_keys::limit_engine::LimitEngine::durable_usage_retention_secs(),
                    cc_lb_control::api_keys::limit_engine::LimitEngine::durable_usage_compaction_batch_size(),
                )
                .await
                .map_err(storage_scheduler_error)?;
                if matches!(
                    result,
                    UsagePruneJobResult::Done {
                        has_more_api_key_usage_work: true,
                        ..
                    }
                ) {
                    let run_at = unix_secs(self.clock.now()).saturating_add(5);
                    let continuation = UsagePruneJob {
                        traceparent: None,
                        api_key_usage_continuation: true,
                    };
                    match self
                        .backend
                        .push_cron_task(SchedulerPushTask {
                            args: CronJob::UsagePrune(continuation),
                            idempotency_key: Some(format!("usage_prune_continuation:{run_at}")),
                            run_at_unix_secs: Some(run_at),
                            max_attempts: Some(3),
                        })
                        .await
                    {
                        Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => {}
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "failed to schedule api_key_usage continuation prune task"
                            );
                            return Err(error);
                        }
                    }
                }
                usage_prune_outcome(result)
            }
            CronJob::PromptCachePurge(job) => prompt_cache_purge_outcome(
                PromptCacheObservationPurgeJobHandler::new(StorageHandle::new(
                    self.storage.clone(),
                ))
                .handle(job, unix_secs(self.clock.now()))
                .await,
            ),
            CronJob::UpstreamAffinityPurge(job) => upstream_affinity_purge_outcome(
                UpstreamAffinityPurgeJobHandler::new(StorageHandle::new(self.storage.clone()))
                    .handle(
                        job,
                        unix_secs(self.clock.now()),
                        self.config.upstream_affinity.ttl_secs(),
                        &self.cancel,
                    )
                    .await,
            ),
            CronJob::PriceCatalogRefresh(job) => self.dispatch_price_catalog(job).await,
            CronJob::ApalisHousekeeping(job) => match &self.backend {
                #[cfg(feature = "storage-sqlite")]
                SchedulerBackend::Sqlite(sqlite) => {
                    let config =
                        ApalisHousekeepingConfig::new(self.config.scheduler.dlq_retention_days);
                    apalis_housekeeping_outcome(
                        ApalisHousekeepingJobHandler::new(sqlite.pool().clone(), config)
                            .handle(job, unix_secs(self.clock.now()), self.storage.as_ref())
                            .await,
                    )
                }
                #[cfg(feature = "storage-postgres")]
                SchedulerBackend::Postgres(postgres) => {
                    let config =
                        ApalisHousekeepingConfig::new(self.config.scheduler.dlq_retention_days);
                    apalis_housekeeping_outcome(
                        ApalisHousekeepingJobHandler::new(postgres.pool().clone(), config)
                            .handle(job, unix_secs(self.clock.now()), self.storage.as_ref())
                            .await,
                    )
                }
            },
            CronJob::WarmupWatchdog(job) => self.handle_warmup_watchdog(job).await,
            CronJob::OAuthRefreshWatchdog(job) => self.handle_oauth_refresh_watchdog(job).await,
            CronJob::OAuthUsagePoll(job) => self.handle_oauth_usage_poll(job).await,
            CronJob::AnthropicCompatRefresh(_) => self.dispatch_anthropic_compat().await,
            CronJob::PoolQuotaSnapshot(job) => self.handle_pool_quota_snapshot(job).await,
            CronJob::ApiKeyUsagePoll(job) => self.handle_api_key_usage_poll(job).await,
        }
    }

    async fn handle_pool_quota_snapshot(
        &self,
        job: PoolQuotaSnapshotCronJob,
    ) -> SchedulerResult<JobOutcome> {
        use cc_lb_control::subscription_quotas::record_provider_pool_quota_snapshots_now;
        use cc_lb_storage_api::Provider;

        for provider in [Provider::Anthropic, Provider::Openai, Provider::Gemini] {
            if let Err(error) = record_provider_pool_quota_snapshots_now(
                self.storage.as_ref(),
                self.dynamic_view.as_ref(),
                provider,
                &*self.clock,
            )
            .await
            {
                tracing::warn!(
                    tick_unix_secs = job.tick_unix_secs,
                    ?provider,
                    %error,
                    "provider pool quota snapshot persist failed",
                );
            }
        }

        Ok(JobOutcome::Done)
    }

    async fn handle_warmup_watchdog(&self, job: WarmupWatchdogJob) -> SchedulerResult<JobOutcome> {
        // Each lane is seeded independently: a failure listing or seeding one
        // lane must not starve the other.
        let mut first_error = None;
        for lane in WarmupLane::ALL {
            let result = async {
                let upstream_ids = self.list_warmup_watchdog_upstream_ids(lane).await?;
                run_entity_watchdog(
                    &self.backend,
                    WatchdogEntityKind::for_warmup_lane(lane),
                    &upstream_ids,
                    job.tick_unix_secs,
                    unix_secs(self.clock.now()),
                )
                .await
            }
            .await;
            match result {
                Ok(stats) => tracing::info!(
                    tick_unix_secs = job.tick_unix_secs,
                    lane = lane.as_str(),
                    seeded = stats.seeded,
                    "warmup watchdog completed"
                ),
                Err(error) => {
                    tracing::warn!(
                        tick_unix_secs = job.tick_unix_secs,
                        lane = lane.as_str(),
                        %error,
                        "warmup watchdog failed"
                    );
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(JobOutcome::Done),
        }
    }

    async fn handle_oauth_refresh_watchdog(
        &self,
        job: OAuthRefreshWatchdogJob,
    ) -> SchedulerResult<JobOutcome> {
        let targets = self.list_oauth_refresh_watchdog_targets().await?;
        let result =
            run_oauth_refresh_watchdog(&self.backend, &targets, job.tick_unix_secs).await?;
        let mut first_metadata_error = None;
        for target in &targets {
            if !target.is_anthropic_oauth {
                continue;
            }
            if let Err(error) = self
                .reconcile_metadata_refresh(
                    MetadataRefreshJob::new(target.upstream_id, target.expected_generation),
                    job.tick_unix_secs,
                )
                .await
            {
                tracing::warn!(
                    tick_unix_secs = job.tick_unix_secs,
                    upstream_id = %target.upstream_id,
                    generation = target.expected_generation,
                    %error,
                    "oauth refresh watchdog metadata reconciliation failed",
                );
                first_metadata_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_metadata_error {
            return Err(error);
        }
        tracing::info!(
            tick_unix_secs = job.tick_unix_secs,
            seeded = result.seeded,
            "oauth refresh watchdog completed"
        );
        Ok(JobOutcome::Done)
    }

    async fn handle_oauth_usage_poll(
        &self,
        job: OAuthUsagePollCronJob,
    ) -> SchedulerResult<JobOutcome> {
        const OAUTH_USAGE_POLL_CONCURRENCY: usize = 8;
        use futures_util::StreamExt as _;

        let mut upstream_ids = self.list_oauth_usage_poll_upstream_ids().await?;
        let total = upstream_ids.len();
        let traceparent = job.traceparent.as_deref();
        let poll_interval_secs = self
            .config
            .scheduler
            .recurring_jobs
            .get("oauth_usage_poll")
            .map(|cfg| cfg.interval_secs.max(1))
            .unwrap_or(60);
        let throttle_backoff_secs = poll_interval_secs.saturating_mul(2);
        rotate_oauth_usage_poll_ids(
            &mut upstream_ids,
            job.tick_unix_secs,
            poll_interval_secs,
            &self.oauth_usage_poll_rotation,
            OAUTH_USAGE_POLL_CONCURRENCY,
        );
        // Bound concurrent polls across upstreams (matching api_key_usage::POLL_CONCURRENCY)
        // while rotating the starting slice per tick so slow upstreams never starve the tail.
        let results: Vec<_> = futures_util::stream::iter(upstream_ids)
            .map(|upstream_id| async move {
                (
                    upstream_id,
                    self.poll_and_record_oauth_usage(
                        upstream_id,
                        traceparent,
                        throttle_backoff_secs,
                    )
                    .await,
                )
            })
            .buffer_unordered(OAUTH_USAGE_POLL_CONCURRENCY)
            .collect()
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
            backed_off = stats.backed_off,
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
        throttle_backoff_secs: u64,
    ) -> SchedulerResult<OAuthUsagePollOutcomeLabel> {
        if let Some(cursor) = self.read_oauth_usage_cursor(upstream_id).await
            && cursor.last_status == Some(429)
            && let Some(last_observed_at_unix_secs) = cursor.last_observed_at_unix_secs
            && unix_secs(self.clock.now()).saturating_sub(last_observed_at_unix_secs)
                < throttle_backoff_secs
        {
            return Ok(OAuthUsagePollOutcomeLabel::BackedOff);
        }
        let observation = self.poll_usage(upstream_id, traceparent).await?;
        let label = OAuthUsagePollOutcomeLabel::from_observation(&observation);
        self.record_oauth_usage_observation(upstream_id, observation)
            .await?;
        Ok(label)
    }

    async fn read_oauth_usage_cursor(
        &self,
        upstream_id: Uuid,
    ) -> Option<cc_lb_scheduler::state_stores::OAuthUsagePollCursor> {
        let result = match &self.backend {
            #[cfg(feature = "storage-sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                OAuthUsagePollCursorsStore::new(sqlite.pool().clone())
                    .read(upstream_id)
                    .await
            }
            #[cfg(feature = "storage-postgres")]
            SchedulerBackend::Postgres(postgres) => {
                OAuthUsagePollCursorsStore::with_scheduler_schema(postgres.pool().clone())
                    .read(upstream_id)
                    .await
            }
        };
        match result {
            Ok(cursor) => cursor,
            Err(error) => {
                tracing::warn!(
                    %upstream_id,
                    %error,
                    "oauth usage poll cursor read failed; polling anyway"
                );
                None
            }
        }
    }

    pub(super) async fn record_oauth_usage_observation(
        &self,
        upstream_id: Uuid,
        observation: OAuthUsagePollObservation,
    ) -> SchedulerResult<()> {
        let Some(update) = OAuthUsageCursorUpdate::from_observation(upstream_id, observation)
        else {
            return Ok(());
        };
        match &self.backend {
            #[cfg(feature = "storage-sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                OAuthUsagePollCursorsStore::new(sqlite.pool().clone())
                    .record_attempt(
                        update.upstream_id,
                        update.observed_at_unix_secs,
                        update.status,
                    )
                    .await
            }
            #[cfg(feature = "storage-postgres")]
            SchedulerBackend::Postgres(postgres) => {
                OAuthUsagePollCursorsStore::with_scheduler_schema(postgres.pool().clone())
                    .record_attempt(
                        update.upstream_id,
                        update.observed_at_unix_secs,
                        update.status,
                    )
                    .await
            }
        }
    }

    async fn dispatch_anthropic_compat(&self) -> SchedulerResult<JobOutcome> {
        match &self.backend {
            #[cfg(feature = "storage-sqlite")]
            SchedulerBackend::Sqlite(sqlite) => {
                handle_anthropic_compat_refresh_job_with_core_fetcher(
                    &AnthropicCompatEtagsStore::new(sqlite.pool().clone()),
                    self.storage.as_ref(),
                    &self.cancel,
                    &*self.clock,
                )
                .await
            }
            #[cfg(feature = "storage-postgres")]
            SchedulerBackend::Postgres(postgres) => {
                handle_anthropic_compat_refresh_job_with_core_fetcher(
                    &AnthropicCompatEtagsStore::with_scheduler_schema(postgres.pool().clone()),
                    self.storage.as_ref(),
                    &self.cancel,
                    &*self.clock,
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
            self.config.price_catalog.url.clone(),
            self.config.price_catalog.cache_path.clone(),
            self.clock.clone(),
        );
        match &self.backend {
            #[cfg(feature = "storage-sqlite")]
            SchedulerBackend::Sqlite(sqlite) => price_catalog_outcome(
                PriceCatalogRefreshJobHandler::new(
                    PriceCatalogVersionsStore::new(sqlite.pool().clone()),
                    loader,
                )
                .handle(job, unix_secs(self.clock.now()))
                .await,
            ),
            #[cfg(feature = "storage-postgres")]
            SchedulerBackend::Postgres(postgres) => price_catalog_outcome(
                PriceCatalogRefreshJobHandler::new(
                    PriceCatalogVersionsStore::with_scheduler_schema(postgres.pool().clone()),
                    loader,
                )
                .handle(job, unix_secs(self.clock.now()))
                .await,
            ),
        }
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
    backed_off: usize,
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
            OAuthUsagePollOutcomeLabel::BackedOff => self.backed_off += 1,
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
    BackedOff,
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

fn rotate_oauth_usage_poll_ids(
    upstream_ids: &mut [Uuid],
    tick_unix_secs: u64,
    interval_secs: u64,
    rotation_state: &parking_lot::Mutex<(Option<u64>, usize)>,
    concurrency: usize,
) {
    if upstream_ids.len() <= 1 || concurrency == 0 {
        return;
    }
    let tick_step = usize::try_from(tick_unix_secs / interval_secs.max(1)).unwrap_or(0);
    let same_tick_seq = {
        let mut guard = rotation_state.lock();
        if guard.0 == Some(tick_unix_secs) {
            guard.1 = guard.1.wrapping_add(1);
            guard.1
        } else {
            *guard = (Some(tick_unix_secs), 0);
            0
        }
    };
    let offset = tick_step
        .wrapping_add(same_tick_seq)
        .wrapping_mul(concurrency)
        % upstream_ids.len();
    upstream_ids.rotate_left(offset);
}

#[cfg(test)]
mod tests {
    use super::rotate_oauth_usage_poll_ids;
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use uuid::Uuid;

    #[test]
    fn oauth_usage_poll_rotation_advances_tail_upstreams_to_first_wave() {
        let ids: Vec<Uuid> = (1_u128..=17).map(Uuid::from_u128).collect();
        let state = Mutex::new((None, 0));
        let mut first_wave_seen = HashSet::new();

        for tick in [0_u64, 60, 120] {
            let mut slice = ids.clone();
            rotate_oauth_usage_poll_ids(&mut slice, tick, 60, &state, 8);
            first_wave_seen.extend(slice.iter().take(8).copied());
        }
        assert_eq!(
            first_wave_seen.len(),
            ids.len(),
            "all 17 upstreams must reach the first wave within ceil(17/8)=3 ticks"
        );

        let mut a = ids.clone();
        let mut b = ids.clone();
        let same_tick_state = Mutex::new((None, 0));
        rotate_oauth_usage_poll_ids(&mut a, 1_800_000_000, 60, &same_tick_state, 8);
        rotate_oauth_usage_poll_ids(&mut b, 1_800_000_000, 60, &same_tick_state, 8);
        assert_ne!(a[0], b[0]);
    }
}
