use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJobResult;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJobOutcome;
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJobResult;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJobResult;
use cc_lb_scheduler::jobs::quota_gc::SubscriptionQuotaGcJobResult;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJobResult;
use cc_lb_scheduler::jobs::usage_rollup::UsageRollupResult;
use cc_lb_scheduler::jobs::warmup::UpstreamWarmupOutcome;
use cc_lb_scheduler::retry::JobOutcome;

pub(super) fn metadata_outcome(outcome: MetadataRefreshJobOutcome) -> SchedulerResult<JobOutcome> {
    Ok(match outcome {
        MetadataRefreshJobOutcome::Applied => JobOutcome::Done,
        MetadataRefreshJobOutcome::Stale => JobOutcome::Noop,
        MetadataRefreshJobOutcome::UpstreamRemoved => JobOutcome::Skip,
    })
}

pub(super) fn warmup_outcome(outcome: UpstreamWarmupOutcome) -> SchedulerResult<JobOutcome> {
    Ok(match outcome {
        UpstreamWarmupOutcome::Fired => JobOutcome::Done,
        UpstreamWarmupOutcome::AlreadyCompleted => JobOutcome::DuplicateEffect,
        UpstreamWarmupOutcome::UpstreamDeleted => JobOutcome::Skip,
    })
}

pub(super) fn usage_rollup_outcome(result: UsageRollupResult) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        UsageRollupResult::Done { run: _ } => JobOutcome::Done,
        UsageRollupResult::Retry { delay, error: _ } => JobOutcome::Retry { delay },
    })
}

pub(super) fn usage_prune_outcome(result: UsagePruneJobResult) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        UsagePruneJobResult::Done { result: _ } => JobOutcome::Done,
        UsagePruneJobResult::Skip => JobOutcome::Skip,
    })
}

pub(super) fn quota_gc_outcome(
    result: SubscriptionQuotaGcJobResult,
) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        SubscriptionQuotaGcJobResult::Done { .. } => JobOutcome::Done,
        SubscriptionQuotaGcJobResult::Retry { delay, error: _ } => JobOutcome::Retry { delay },
    })
}

pub(super) fn prompt_cache_purge_outcome(
    result: PromptCacheObservationPurgeJobResult,
) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        PromptCacheObservationPurgeJobResult::Done { .. } => JobOutcome::Done,
        PromptCacheObservationPurgeJobResult::Retry { delay, error: _ } => {
            JobOutcome::Retry { delay }
        }
    })
}

pub(super) fn price_catalog_outcome(
    result: PriceCatalogRefreshJobResult,
) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        PriceCatalogRefreshJobResult::Done { .. } => JobOutcome::Done,
        PriceCatalogRefreshJobResult::Retry { delay, error } => {
            if delay.is_zero() {
                return Err(SchedulerError::Job(error));
            }
            JobOutcome::Retry { delay }
        }
    })
}

pub(super) fn apalis_housekeeping_outcome(
    result: ApalisHousekeepingJobResult,
) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        ApalisHousekeepingJobResult::Done { .. } => JobOutcome::Done,
        ApalisHousekeepingJobResult::Retry { delay, error: _ } => JobOutcome::Retry { delay },
    })
}
