use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SubscriptionQuotaWindow {
    #[serde(rename = "5h")]
    FiveHour,
    #[serde(rename = "7d")]
    SevenDay,
    #[serde(rename = "7d_sonnet")]
    SevenDaySonnet,
    #[serde(rename = "7d_opus")]
    SevenDayOpus,
    #[serde(rename = "overage")]
    Overage,
    #[serde(rename = "unified")]
    Unified,
}

impl SubscriptionQuotaWindow {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FiveHour => "5h",
            Self::SevenDay => "7d",
            Self::SevenDaySonnet => "7d_sonnet",
            Self::SevenDayOpus => "7d_opus",
            Self::Overage => "overage",
            Self::Unified => "unified",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "5h" => Self::FiveHour,
            "7d" => Self::SevenDay,
            "7d_sonnet" => Self::SevenDaySonnet,
            "7d_opus" => Self::SevenDayOpus,
            "overage" => Self::Overage,
            "unified" => Self::Unified,
            _ => return None,
        })
    }

    pub fn code(self) -> u8 {
        match self {
            Self::FiveHour => 1,
            Self::SevenDay => 2,
            Self::SevenDaySonnet => 3,
            Self::SevenDayOpus => 4,
            Self::Overage => 5,
            Self::Unified => 6,
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::FiveHour,
            2 => Self::SevenDay,
            3 => Self::SevenDaySonnet,
            4 => Self::SevenDayOpus,
            5 => Self::Overage,
            6 => Self::Unified,
            _ => return None,
        })
    }

    pub const fn all() -> &'static [SubscriptionQuotaWindow] {
        &[
            Self::FiveHour,
            Self::SevenDay,
            Self::SevenDaySonnet,
            Self::SevenDayOpus,
            Self::Overage,
            Self::Unified,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionQuotaSource {
    Header,
    Api,
}

impl SubscriptionQuotaSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Api => "api",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "header" => Self::Header,
            "api" => Self::Api,
            _ => return None,
        })
    }

    pub fn code(self) -> u8 {
        match self {
            Self::Header => 1,
            Self::Api => 2,
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::Header,
            2 => Self::Api,
            _ => return None,
        })
    }

    pub const fn all() -> &'static [SubscriptionQuotaSource] {
        &[Self::Header, Self::Api]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionQuotaStatus {
    Allowed,
    AllowedWarning,
    Rejected,
}

impl SubscriptionQuotaStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::AllowedWarning => "allowed_warning",
            Self::Rejected => "rejected",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "allowed" => Self::Allowed,
            "allowed_warning" => Self::AllowedWarning,
            "exceeded" | "exceeded_overage" => Self::Rejected,
            "rejected" => Self::Rejected,
            _ => return None,
        })
    }
}

/// `Sample` is a real quota observation. `ProcessStart` is a synthetic marker
/// emitted once on writer/poller boot so downstream Δ-rate estimators can
/// detect that the dedup cache restarted and avoid attributing a discontinuity
/// to a spike. See `SubscriptionQuotaSeriesQuery` consumers (R3 prediction).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionQuotaSampleKind {
    Sample,
    ProcessStart,
}

impl SubscriptionQuotaSampleKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sample => "sample",
            Self::ProcessStart => "process_start",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "sample" => Self::Sample,
            "process_start" => Self::ProcessStart,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionQuotaSourceMerge {
    Header,
    Api,
    Merged,
}

impl SubscriptionQuotaSourceMerge {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Api => "api",
            Self::Merged => "merged",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "header" => Self::Header,
            "api" => Self::Api,
            "merged" => Self::Merged,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaObservationRecord {
    pub upstream_id: Uuid,
    pub window: SubscriptionQuotaWindow,
    pub source: SubscriptionQuotaSource,
    pub sample_kind: SubscriptionQuotaSampleKind,
    pub observed_at_unix_millis: u64,
    pub sample_id: Uuid,

    pub utilization: Option<f64>,
    pub status: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs: Option<u64>,
    pub surpassed_threshold: Option<bool>,
    pub representative_claim: Option<String>,
    pub fallback_percentage: Option<f64>,
    pub disabled_reason: Option<String>,

    pub extra_usage_enabled: Option<bool>,
    pub extra_usage_monthly_limit: Option<f64>,
    pub extra_usage_used_credits: Option<f64>,

    pub ingested_at_unix_millis: u64,
}

pub type SubscriptionQuotaLatestRecord = SubscriptionQuotaObservationRecord;

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionQuotaSeriesQuery {
    pub upstream_ids: Vec<Uuid>,
    pub windows: Vec<SubscriptionQuotaWindow>,
    pub sources: Vec<SubscriptionQuotaSource>,
    pub since_unix_millis: u64,
    pub until_unix_millis: u64,
    pub bucket_secs: u64,
    pub max_points_per_series: u32,
    pub source_merge: SubscriptionQuotaSourceMerge,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaBucket {
    pub bucket_start_unix_secs: u64,
    pub observed: bool,
    pub sample_count: u32,
    pub utilization_min: Option<f64>,
    pub utilization_avg: Option<f64>,
    pub utilization_max: Option<f64>,
    pub utilization_last: Option<f64>,
    pub status_last: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs_last: Option<u64>,
    pub observed_at_unix_millis_last: Option<u64>,
    pub sources_seen: Vec<SubscriptionQuotaSource>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaSeries {
    pub upstream_id: Uuid,
    pub window: SubscriptionQuotaWindow,
    pub source: SubscriptionQuotaSourceMerge,
    pub buckets: Vec<SubscriptionQuotaBucket>,
}

#[async_trait]
pub trait UpstreamSubscriptionQuotaStore: Send + Sync {
    /// Atomically appends every record to the observations table AND upserts
    /// the latest sidecar row per `(upstream_id, window, source)` in one
    /// transaction. The latest UPSERT is monotonic: rows whose
    /// `observed_at_unix_millis` is less than the existing latest are SKIPPED
    /// for the sidecar but STILL APPENDED to observations.
    async fn put_subscription_quota_batch(
        &self,
        records: &[SubscriptionQuotaObservationRecord],
    ) -> StorageResult<()>;

    async fn put_subscription_quota(
        &self,
        record: &SubscriptionQuotaObservationRecord,
    ) -> StorageResult<()> {
        self.put_subscription_quota_batch(std::slice::from_ref(record))
            .await
    }

    /// Returns the latest sidecar row for each `(upstream_id, window, source)`
    /// intersected with the given upstream IDs. Empty input returns empty.
    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaLatestRecord>>;

    /// Server-side bucketed downsample of the raw observations table. The
    /// caller specifies bucket width and per-series point cap. The backend
    /// MUST honor `query.sources` and `query.windows` filters and the
    /// `query.source_merge` policy (per-source series vs merged-per-window).
    async fn list_subscription_quota_series(
        &self,
        query: SubscriptionQuotaSeriesQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSeries>>;

    /// Deletes raw observations whose `observed_at_unix_millis` is strictly
    /// less than `cutoff_unix_millis`. Returns the number of deleted rows.
    /// The latest sidecar is NEVER deleted by this method, so routing
    /// readers do not lose ground state when GC trims an inactive upstream.
    async fn delete_subscription_quota_before(
        &self,
        cutoff_unix_millis: u64,
        batch_size: u32,
    ) -> StorageResult<u64>;
}
