use apalis_core::task::Task;
use serde::{Deserialize, Serialize};

use crate::jobs::apalis_housekeeping::ApalisHousekeepingJob;
use crate::jobs::cache_keepalive::CacheKeepaliveJob;
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::metadata_refresh::MetadataRefreshJob;
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
use crate::jobs::pool_quota_snapshot::PoolQuotaSnapshotCronJob;
use crate::jobs::price_catalog::PriceCatalogRefreshJob;
use crate::jobs::prompt_cache_purge::PromptCacheObservationPurgeJob;
use crate::jobs::quota_gc::SubscriptionQuotaGcJob;
use crate::jobs::usage_prune::UsagePruneJob;
use crate::jobs::usage_rollup::UsageRollupTask;
use crate::jobs::warmup::UpstreamWarmupJob;
use crate::jobs::watchdog::{OAuthRefreshWatchdogJob, WarmupWatchdogJob};
use crate::middleware::TraceparentCarrier;
use crate::retry::RetryPayload;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum AdaptiveJob {
    Warmup(UpstreamWarmupJob),
    #[serde(rename = "oauth_refresh", alias = "o_auth_refresh")]
    OAuthRefresh(OAuthRefreshJob),
    MetadataRefresh(MetadataRefreshJob),
    CacheKeepalive(CacheKeepaliveJob),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum CronJob {
    UsageRollup(UsageRollupTask),
    UsagePrune(UsagePruneJob),
    // Retired: subscription-quota retention removed (ADR 0005). Never scheduled; dispatch is a no-op. Kept as a deserialization tombstone so queued CRON_QUEUE rows still deserialize.
    QuotaGc(SubscriptionQuotaGcJob),
    PromptCachePurge(PromptCacheObservationPurgeJob),
    PriceCatalogRefresh(PriceCatalogRefreshJob),
    ApalisHousekeeping(ApalisHousekeepingJob),
    WarmupWatchdog(WarmupWatchdogJob),
    #[serde(rename = "oauth_refresh_watchdog", alias = "o_auth_refresh_watchdog")]
    OAuthRefreshWatchdog(OAuthRefreshWatchdogJob),
    #[serde(rename = "oauth_usage_poll", alias = "o_auth_usage_poll")]
    OAuthUsagePoll(OAuthUsagePollCronJob),
    AnthropicCompatRefresh(AnthropicCompatRefreshJob),
    PoolQuotaSnapshot(PoolQuotaSnapshotCronJob),
}

impl CronJob {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::UsageRollup(_) => "usage_rollup",
            Self::UsagePrune(_) => "usage_prune",
            Self::QuotaGc(_) => "quota_gc",
            Self::PromptCachePurge(_) => "prompt_cache_purge",
            Self::PriceCatalogRefresh(_) => "price_catalog_refresh",
            Self::ApalisHousekeeping(_) => "apalis_housekeeping",
            Self::WarmupWatchdog(_) => "warmup_watchdog",
            Self::OAuthRefreshWatchdog(_) => "oauth_refresh_watchdog",
            Self::OAuthUsagePoll(_) => "oauth_usage_poll",
            Self::AnthropicCompatRefresh(_) => "anthropic_compat_refresh",
            Self::PoolQuotaSnapshot(_) => "pool_quota_snapshot",
        }
    }
}

impl AdaptiveJob {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Warmup(_) => "warmup",
            Self::OAuthRefresh(_) => "oauth_refresh",
            Self::MetadataRefresh(_) => "metadata_refresh",
            Self::CacheKeepalive(_) => "cache_keepalive",
        }
    }

    pub(crate) fn idempotency_key(&self, run_at_unix_secs: u64) -> String {
        match self {
            Self::Warmup(job) => job.idempotency_key(job.cycle_key),
            Self::OAuthRefresh(job) => job.idempotency_key(run_at_unix_secs),
            Self::MetadataRefresh(job) => job.idempotency_key(),
            Self::CacheKeepalive(job) => job.idempotency_key(),
        }
    }
}

impl TraceparentCarrier for AdaptiveJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::Warmup(_) => None,
            Self::OAuthRefresh(job) => job.traceparent(),
            Self::MetadataRefresh(job) => job.traceparent(),
            Self::CacheKeepalive(job) => job.traceparent(),
        }
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        match self {
            Self::Warmup(_) => {}
            Self::OAuthRefresh(job) => job.set_traceparent(traceparent),
            Self::MetadataRefresh(job) => job.set_traceparent(traceparent),
            Self::CacheKeepalive(job) => job.set_traceparent(traceparent),
        }
    }
}

impl<Ctx, IdType> TraceparentCarrier for Task<AdaptiveJob, Ctx, IdType> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl<Ctx, IdType> RetryPayload for Task<AdaptiveJob, Ctx, IdType> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.parts.attempt.current().saturating_add(1)).unwrap_or(u32::MAX)
    }
}

impl TraceparentCarrier for CronJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::UsageRollup(job) => job.traceparent.as_deref(),
            Self::UsagePrune(job) => job.traceparent.as_deref(),
            Self::QuotaGc(job) => job.traceparent(),
            Self::PromptCachePurge(job) => job.traceparent(),
            Self::PriceCatalogRefresh(job) => job.traceparent(),
            Self::ApalisHousekeeping(job) => job.traceparent(),
            Self::WarmupWatchdog(job) => job.traceparent(),
            Self::OAuthRefreshWatchdog(job) => job.traceparent(),
            Self::OAuthUsagePoll(job) => job.traceparent(),
            Self::AnthropicCompatRefresh(job) => job.traceparent(),
            Self::PoolQuotaSnapshot(job) => job.traceparent(),
        }
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        match self {
            Self::UsageRollup(job) => job.traceparent = traceparent,
            Self::UsagePrune(job) => job.traceparent = traceparent,
            Self::QuotaGc(job) => job.set_traceparent(traceparent),
            Self::PromptCachePurge(job) => job.set_traceparent(traceparent),
            Self::PriceCatalogRefresh(job) => job.set_traceparent(traceparent),
            Self::ApalisHousekeeping(job) => job.set_traceparent(traceparent),
            Self::WarmupWatchdog(job) => job.set_traceparent(traceparent),
            Self::OAuthRefreshWatchdog(job) => job.set_traceparent(traceparent),
            Self::OAuthUsagePoll(job) => job.set_traceparent(traceparent),
            Self::AnthropicCompatRefresh(job) => job.set_traceparent(traceparent),
            Self::PoolQuotaSnapshot(job) => job.set_traceparent(traceparent),
        }
    }
}

impl<Ctx, IdType> TraceparentCarrier for Task<CronJob, Ctx, IdType> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl<Ctx, IdType> RetryPayload for Task<CronJob, Ctx, IdType> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.parts.attempt.current().saturating_add(1)).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::time::Duration;

    use apalis_core::task::builder::TaskBuilder;
    use tower::{Layer as _, ServiceExt as _, service_fn};
    use uuid::Uuid;

    use super::*;
    use crate::jobs::oauth_refresh::OAuthRefreshJob;
    use crate::retry::{JobOutcome, RetryClass};
    #[test]
    fn quota_gc_tombstone_preserves_serialized_shape() {
        let serialized =
            serde_json::to_string(&CronJob::QuotaGc(SubscriptionQuotaGcJob::default()))
                .expect("serialize quota_gc tombstone");
        assert_eq!(serialized, r#"{"type":"quota_gc","payload":{}}"#);

        let deserialized: CronJob = serde_json::from_str(&serialized)
            .expect("deserialize quota_gc tombstone from cron queue payload");
        assert!(matches!(deserialized, CronJob::QuotaGc(_)));
    }

    #[tokio::test]
    async fn fresh_task_retry_uses_first_attempt_delay() {
        let task: Task<AdaptiveJob, (), ulid::Ulid> =
            TaskBuilder::new(AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(Uuid::nil()))).build();
        assert_eq!(task.attempt_count(), 1);

        let service = RetryClass::Adaptive.layer().layer(service_fn(
            |_task: Task<AdaptiveJob, (), ulid::Ulid>| async {
                Ok::<_, Infallible>(JobOutcome::Retry {
                    delay: Duration::ZERO,
                })
            },
        ));
        let result = service.oneshot(task).await.expect("handler succeeds");

        let JobOutcome::Retry { delay } = result else {
            panic!("fresh task retry must remain retryable, got {result:?}");
        };
        assert!(delay >= Duration::from_secs(27));
        assert!(delay <= Duration::from_secs(33));
    }
}
