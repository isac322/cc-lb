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
use crate::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJob;
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
    #[serde(rename = "oauth_refresh")]
    OAuthRefresh(OAuthRefreshJob),
    MetadataRefresh(MetadataRefreshJob),
    CacheKeepalive(CacheKeepaliveJob),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum CronJob {
    UsageRollup(UsageRollupTask),
    UsagePrune(UsagePruneJob),
    PromptCachePurge(PromptCacheObservationPurgeJob),
    UpstreamAffinityPurge(UpstreamAffinityPurgeJob),
    PriceCatalogRefresh(PriceCatalogRefreshJob),
    ApalisHousekeeping(ApalisHousekeepingJob),
    WarmupWatchdog(WarmupWatchdogJob),
    #[serde(rename = "oauth_refresh_watchdog")]
    OAuthRefreshWatchdog(OAuthRefreshWatchdogJob),
    #[serde(rename = "oauth_usage_poll")]
    OAuthUsagePoll(OAuthUsagePollCronJob),
    AnthropicCompatRefresh(AnthropicCompatRefreshJob),
    PoolQuotaSnapshot(PoolQuotaSnapshotCronJob),
}

impl CronJob {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::UsageRollup(_) => "usage_rollup",
            Self::UsagePrune(_) => "usage_prune",
            Self::PromptCachePurge(_) => "prompt_cache_purge",
            Self::UpstreamAffinityPurge(_) => "upstream_affinity_purge",
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
        u32::try_from(self.parts.attempt.current()).unwrap_or(u32::MAX)
    }
}

impl TraceparentCarrier for CronJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::UsageRollup(job) => job.traceparent.as_deref(),
            Self::UsagePrune(job) => job.traceparent.as_deref(),
            Self::PromptCachePurge(job) => job.traceparent(),
            Self::UpstreamAffinityPurge(job) => job.traceparent(),
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
            Self::PromptCachePurge(job) => job.set_traceparent(traceparent),
            Self::UpstreamAffinityPurge(job) => job.set_traceparent(traceparent),
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
        u32::try_from(self.parts.attempt.current()).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_compat_refresh_payload_round_trips() {
        let serialized = serde_json::to_string(&CronJob::AnthropicCompatRefresh(
            AnthropicCompatRefreshJob::default(),
        ))
        .expect("serialize compat refresh");
        assert_eq!(
            serialized,
            r#"{"type":"anthropic_compat_refresh","payload":{"traceparent":null}}"#
        );
        let deserialized: CronJob =
            serde_json::from_str(&serialized).expect("deserialize compat refresh");
        assert!(matches!(
            deserialized,
            CronJob::AnthropicCompatRefresh(AnthropicCompatRefreshJob { traceparent: None })
        ));
    }

    #[test]
    fn upstream_affinity_purge_payload_contains_only_trace_context() {
        let default_payload = serde_json::to_value(CronJob::UpstreamAffinityPurge(
            UpstreamAffinityPurgeJob::default(),
        ))
        .expect("serialize default upstream affinity purge");
        assert_eq!(
            default_payload,
            serde_json::json!({
                "type": "upstream_affinity_purge",
                "payload": {},
            })
        );

        let traced_payload =
            serde_json::to_value(CronJob::UpstreamAffinityPurge(UpstreamAffinityPurgeJob {
                traceparent: Some("00-trace-parent".to_owned()),
            }))
            .expect("serialize traced upstream affinity purge");
        assert_eq!(
            traced_payload,
            serde_json::json!({
                "type": "upstream_affinity_purge",
                "payload": {
                    "traceparent": "00-trace-parent",
                },
            })
        );
    }
}
