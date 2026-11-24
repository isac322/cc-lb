use apalis_core::task::Task;
use serde::{Deserialize, Serialize};

use crate::jobs::apalis_housekeeping::ApalisHousekeepingJob;
use crate::jobs::api_key_usage_poll::ApiKeyUsagePollCronJob;
use crate::jobs::cache_keepalive::CacheKeepaliveJob;
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::metadata_refresh::MetadataRefreshJob;
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::oauth_usage_poll::{OAuthUsagePollCronJob, OAuthUsageRefreshJob};
use crate::jobs::pool_quota_snapshot::PoolQuotaSnapshotCronJob;
use crate::jobs::price_catalog::PriceCatalogRefreshJob;
use crate::jobs::prompt_cache_purge::PromptCacheObservationPurgeJob;
use crate::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJob;
use crate::jobs::usage_prune::UsagePruneJob;
use crate::jobs::usage_rollup::UsageRollupTask;
use crate::jobs::warmup::{ManualWarmupJob, UpstreamWarmupJob};
use crate::jobs::watchdog::{OAuthRefreshWatchdogJob, WarmupWatchdogJob};
use crate::middleware::TraceparentCarrier;
use crate::retry::RetryPayload;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum AdaptiveJob {
    Warmup(UpstreamWarmupJob),
    ManualWarmup(ManualWarmupJob),
    #[serde(rename = "oauth_refresh")]
    OAuthRefresh(OAuthRefreshJob),
    #[serde(rename = "oauth_usage_refresh")]
    OAuthUsageRefresh(OAuthUsageRefreshJob),
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
    ApiKeyUsagePoll(ApiKeyUsagePollCronJob),
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
            Self::ApiKeyUsagePoll(_) => "api_key_usage_poll",
        }
    }
}

impl AdaptiveJob {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Warmup(job) => job.lane.job_kind(),
            Self::ManualWarmup(_) => "manual_warmup",
            Self::OAuthRefresh(_) => "oauth_refresh",
            Self::OAuthUsageRefresh(_) => "oauth_usage_refresh",
            Self::MetadataRefresh(_) => "metadata_refresh",
            Self::CacheKeepalive(_) => "cache_keepalive",
        }
    }
}

impl TraceparentCarrier for AdaptiveJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::Warmup(_) => None,
            Self::ManualWarmup(job) => job.traceparent(),
            Self::OAuthRefresh(job) => job.traceparent(),
            Self::OAuthUsageRefresh(job) => job.traceparent(),
            Self::MetadataRefresh(job) => job.traceparent(),
            Self::CacheKeepalive(job) => job.traceparent(),
        }
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        match self {
            Self::Warmup(_) => {}
            Self::ManualWarmup(job) => job.set_traceparent(traceparent),
            Self::OAuthRefresh(job) => job.set_traceparent(traceparent),
            Self::OAuthUsageRefresh(job) => job.set_traceparent(traceparent),
            Self::MetadataRefresh(job) => job.set_traceparent(traceparent),
            Self::CacheKeepalive(job) => job.set_traceparent(traceparent),
        }
    }
}

impl TraceparentCarrier for Task<AdaptiveJob> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl RetryPayload for Task<AdaptiveJob> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.attempt()).unwrap_or(u32::MAX)
    }

    fn allows_retry(&self) -> bool {
        self.max_attempts() != Some(1)
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
            Self::ApiKeyUsagePoll(job) => job.traceparent(),
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
            Self::ApiKeyUsagePoll(job) => job.set_traceparent(traceparent),
        }
    }
}

impl TraceparentCarrier for Task<CronJob> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl RetryPayload for Task<CronJob> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.attempt()).unwrap_or(u32::MAX)
    }

    fn allows_retry(&self) -> bool {
        self.max_attempts() != Some(1)
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

    #[test]
    fn api_key_usage_poll_payload_round_trips() {
        let upstream_id = uuid::Uuid::from_u128(7);
        let job = CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob::for_upstream(
            1_800_000_000,
            upstream_id,
        ));
        assert_eq!(job.kind(), "api_key_usage_poll");
        let value = serde_json::to_value(&job).expect("serialize api-key usage poll");
        assert_eq!(
            value,
            serde_json::json!({
                "type": "api_key_usage_poll",
                "payload": {
                    "tick_unix_secs": 1_800_000_000_u64,
                    "upstream_id": upstream_id,
                },
            })
        );
        let decoded: CronJob = serde_json::from_value(value).expect("deserialize");
        assert!(matches!(
            decoded,
            CronJob::ApiKeyUsagePoll(ApiKeyUsagePollCronJob {
                tick_unix_secs: 1_800_000_000,
                upstream_id: Some(id),
                traceparent: None,
            }) if id == upstream_id
        ));
    }

    #[test]
    fn split_bridge_adaptive_jobs_round_trip() {
        let upstream_id = uuid::Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        let nonce = uuid::Uuid::from_u128(0x99);
        let warmup = AdaptiveJob::ManualWarmup(ManualWarmupJob::new(
            upstream_id,
            cc_lb_storage_api::WarmupLane::Primary,
            "fire-now:test".to_owned(),
            1_800_000_000,
            nonce,
        ));
        assert_eq!(warmup.kind(), "manual_warmup");
        let warmup_json = serde_json::to_value(&warmup).expect("serialize manual warmup");
        let warmup_decoded: AdaptiveJob =
            serde_json::from_value(warmup_json).expect("deserialize manual warmup");
        assert!(matches!(warmup_decoded, AdaptiveJob::ManualWarmup(_)));

        let usage = AdaptiveJob::OAuthUsageRefresh(OAuthUsageRefreshJob::new(upstream_id, nonce));
        assert_eq!(usage.kind(), "oauth_usage_refresh");
        let usage_json = serde_json::to_value(&usage).expect("serialize usage refresh");
        let usage_decoded: AdaptiveJob =
            serde_json::from_value(usage_json).expect("deserialize usage refresh");
        assert!(matches!(usage_decoded, AdaptiveJob::OAuthUsageRefresh(_)));
    }
}
