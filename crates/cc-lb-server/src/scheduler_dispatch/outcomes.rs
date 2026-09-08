use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_scheduler::jobs::apalis_housekeeping::ApalisHousekeepingJobResult;
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJobOutcome;
use cc_lb_scheduler::jobs::price_catalog::PriceCatalogRefreshJobResult;
use cc_lb_scheduler::jobs::prompt_cache_purge::PromptCacheObservationPurgeJobResult;
use cc_lb_scheduler::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJobResult;
use cc_lb_scheduler::jobs::usage_prune::UsagePruneJobResult;
use cc_lb_scheduler::jobs::usage_rollup::UsageRollupResult;
use cc_lb_scheduler::retry::JobOutcome;

pub(super) fn metadata_outcome(outcome: MetadataRefreshJobOutcome) -> SchedulerResult<JobOutcome> {
    Ok(match outcome {
        MetadataRefreshJobOutcome::Applied => JobOutcome::Done,
        MetadataRefreshJobOutcome::Stale => JobOutcome::Noop,
        MetadataRefreshJobOutcome::UpstreamRemoved => JobOutcome::Skip,
    })
}

pub(super) fn usage_rollup_outcome(result: UsageRollupResult) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        UsageRollupResult::Done { run: _ } => JobOutcome::Done,
        UsageRollupResult::Retry { delay, error: _ } => JobOutcome::Retry { delay },
    })
}

pub(super) fn usage_prune_outcome(result: UsagePruneJobResult) -> SchedulerResult<JobOutcome> {
    match result {
        UsagePruneJobResult::Done { .. } => Ok(JobOutcome::Done),
    }
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

pub(super) fn upstream_affinity_purge_outcome(
    result: UpstreamAffinityPurgeJobResult,
) -> SchedulerResult<JobOutcome> {
    Ok(match result {
        UpstreamAffinityPurgeJobResult::Done { .. } => JobOutcome::Done,
        UpstreamAffinityPurgeJobResult::Cancelled { .. } => JobOutcome::Noop,
        UpstreamAffinityPurgeJobResult::Retry { delay, .. } => JobOutcome::Retry { delay },
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn upstream_affinity_purge_maps_completion_and_limit_to_done() {
        for limit_reached in [false, true] {
            assert_eq!(
                upstream_affinity_purge_outcome(UpstreamAffinityPurgeJobResult::Done {
                    rows_removed: 1_000,
                    batches: 1,
                    limit_reached,
                })
                .expect("purge outcome"),
                JobOutcome::Done
            );
        }
    }

    #[test]
    fn upstream_affinity_purge_maps_retry_and_cancellation() {
        assert_eq!(
            upstream_affinity_purge_outcome(UpstreamAffinityPurgeJobResult::Retry {
                delay: Duration::from_secs(60),
                error: "unavailable".to_owned(),
                rows_removed: 0,
                batches: 0,
            })
            .expect("retry outcome"),
            JobOutcome::Retry {
                delay: Duration::from_secs(60),
            }
        );
        assert_eq!(
            upstream_affinity_purge_outcome(UpstreamAffinityPurgeJobResult::Cancelled {
                rows_removed: 0,
                batches: 0,
            })
            .expect("cancel outcome"),
            JobOutcome::Noop
        );
    }
}
