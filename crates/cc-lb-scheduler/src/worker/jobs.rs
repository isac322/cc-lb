use apalis_core::task::Task;
use serde::{Deserialize, Serialize};

use crate::jobs::apalis_housekeeping::ApalisHousekeepingJob;
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::metadata_refresh::MetadataRefreshJob;
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::oauth_usage_poll::OAuthUsagePollJob;
use crate::jobs::price_catalog::PriceCatalogRefreshJob;
use crate::jobs::prompt_cache_purge::PromptCacheObservationPurgeJob;
use crate::jobs::quota_gc::SubscriptionQuotaGcJob;
use crate::jobs::usage_prune::UsagePruneJob;
use crate::jobs::usage_rollup::UsageRollupTask;
use crate::jobs::warmup::UpstreamWarmupJob;
use crate::middleware::TraceparentCarrier;
use crate::retry::RetryPayload;

use super::EntityJobKind;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EntityJob {
    Warmup(UpstreamWarmupJob),
    OAuthRefresh(OAuthRefreshJob),
    OAuthUsagePoll(OAuthUsagePollJob),
    AnthropicCompatRefresh(AnthropicCompatRefreshJob),
    MetadataRefresh(MetadataRefreshJob),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum SingletonJob {
    UsageRollup(UsageRollupTask),
    UsagePrune(UsagePruneJob),
    QuotaGc(SubscriptionQuotaGcJob),
    PromptCachePurge(PromptCacheObservationPurgeJob),
    PriceCatalogRefresh(PriceCatalogRefreshJob),
    ApalisHousekeeping(ApalisHousekeepingJob),
}

impl SingletonJob {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::UsageRollup(_) => "usage_rollup",
            Self::UsagePrune(_) => "usage_prune",
            Self::QuotaGc(_) => "quota_gc",
            Self::PromptCachePurge(_) => "prompt_cache_purge",
            Self::PriceCatalogRefresh(_) => "price_catalog_refresh",
            Self::ApalisHousekeeping(_) => "apalis_housekeeping",
        }
    }
}

impl EntityJob {
    pub const fn kind(&self) -> EntityJobKind {
        match self {
            Self::Warmup(_) => EntityJobKind::Warmup,
            Self::OAuthRefresh(_) => EntityJobKind::OAuthRefresh,
            Self::OAuthUsagePoll(_) => EntityJobKind::OAuthUsagePoll,
            Self::AnthropicCompatRefresh(_) => EntityJobKind::AnthropicCompatRefresh,
            Self::MetadataRefresh(_) => EntityJobKind::MetadataRefresh,
        }
    }
}

impl TraceparentCarrier for EntityJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::Warmup(_) => None,
            Self::OAuthRefresh(job) => job.traceparent(),
            Self::OAuthUsagePoll(job) => job.traceparent(),
            Self::AnthropicCompatRefresh(job) => job.traceparent(),
            Self::MetadataRefresh(job) => job.traceparent(),
        }
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        match self {
            Self::Warmup(_) => {}
            Self::OAuthRefresh(job) => job.set_traceparent(traceparent),
            Self::OAuthUsagePoll(job) => job.set_traceparent(traceparent),
            Self::AnthropicCompatRefresh(job) => job.set_traceparent(traceparent),
            Self::MetadataRefresh(job) => job.set_traceparent(traceparent),
        }
    }
}

impl<Ctx, IdType> TraceparentCarrier for Task<EntityJob, Ctx, IdType> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl<Ctx, IdType> RetryPayload for Task<EntityJob, Ctx, IdType> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.parts.attempt.current()).unwrap_or(u32::MAX)
    }
}

impl TraceparentCarrier for SingletonJob {
    fn traceparent(&self) -> Option<&str> {
        match self {
            Self::UsageRollup(job) => job.traceparent.as_deref(),
            Self::UsagePrune(job) => job.traceparent.as_deref(),
            Self::QuotaGc(job) => job.traceparent(),
            Self::PromptCachePurge(job) => job.traceparent(),
            Self::PriceCatalogRefresh(job) => job.traceparent(),
            Self::ApalisHousekeeping(job) => job.traceparent(),
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
        }
    }
}

impl<Ctx, IdType> TraceparentCarrier for Task<SingletonJob, Ctx, IdType> {
    fn traceparent(&self) -> Option<&str> {
        self.args.traceparent()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.args.set_traceparent(traceparent);
    }
}

impl<Ctx, IdType> RetryPayload for Task<SingletonJob, Ctx, IdType> {
    fn attempt_count(&self) -> u32 {
        u32::try_from(self.parts.attempt.current()).unwrap_or(u32::MAX)
    }
}
