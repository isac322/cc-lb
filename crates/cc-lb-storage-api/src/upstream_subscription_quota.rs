use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageResult;
use crate::{
    SubscriptionQuotaCheckpointRange, SubscriptionQuotaCheckpointRangeQuery,
    SubscriptionQuotaCheckpointRecord,
};

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
    #[serde(rename = "7d_fable")]
    SevenDayFable,
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
            Self::SevenDayFable => "7d_fable",
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
            "7d_fable" => Self::SevenDayFable,
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
            Self::SevenDayFable => 7,
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
            7 => Self::SevenDayFable,
            _ => return None,
        })
    }

    pub const fn all() -> &'static [SubscriptionQuotaWindow] {
        &[
            Self::FiveHour,
            Self::SevenDay,
            Self::SevenDaySonnet,
            Self::SevenDayOpus,
            Self::SevenDayFable,
            Self::Overage,
            Self::Unified,
        ]
    }
}

#[cfg(test)]
mod subscription_quota_window_tests {
    use super::SubscriptionQuotaWindow;

    #[test]
    fn seven_day_fable_round_trips_stable_vocabulary() {
        let window = SubscriptionQuotaWindow::SevenDayFable;

        assert_eq!(window.as_str(), "7d_fable");
        assert_eq!(SubscriptionQuotaWindow::from_str("7d_fable"), Some(window));
        assert_eq!(window.code(), 7);
        assert_eq!(SubscriptionQuotaWindow::from_code(7), Some(window));
        assert_eq!(
            serde_json::to_string(&window).expect("Fable window serializes"),
            "\"7d_fable\""
        );
        assert_eq!(
            serde_json::from_str::<SubscriptionQuotaWindow>("\"7d_fable\"")
                .expect("Fable window deserializes"),
            window
        );
        assert!(SubscriptionQuotaWindow::all().contains(&window));
    }

    #[test]
    fn existing_window_codes_remain_stable() {
        let stable_codes = [
            (SubscriptionQuotaWindow::FiveHour, 1),
            (SubscriptionQuotaWindow::SevenDay, 2),
            (SubscriptionQuotaWindow::SevenDaySonnet, 3),
            (SubscriptionQuotaWindow::SevenDayOpus, 4),
            (SubscriptionQuotaWindow::Overage, 5),
            (SubscriptionQuotaWindow::Unified, 6),
        ];

        for (window, code) in stable_codes {
            assert_eq!(window.code(), code);
            assert_eq!(SubscriptionQuotaWindow::from_code(code), Some(window));
        }
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
pub struct SubscriptionQuotaSample {
    pub upstream_id: Uuid,
    pub window: SubscriptionQuotaWindow,
    pub source: SubscriptionQuotaSource,
    pub sample_kind: SubscriptionQuotaSampleKind,
    pub observed_at_unix_millis: u64,
    pub sample_id: Uuid,

    pub utilization: Option<f64>,
    pub status: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs: Option<u64>,
    pub surpassed_threshold: Option<f64>,
    pub representative_claim: Option<String>,
    pub fallback_percentage: Option<f64>,
    pub fallback_available: Option<bool>,
    pub overage_in_use: Option<bool>,
    pub overage_period_monthly_utilization: Option<f64>,
    pub upgrade_paths: Option<Vec<String>>,
    pub disabled_reason: Option<String>,

    pub extra_usage_enabled: Option<bool>,
    pub extra_usage_monthly_limit: Option<f64>,
    pub extra_usage_used_credits: Option<f64>,

    pub ingested_at_unix_millis: u64,
}

pub type SubscriptionQuotaLatestRecord = SubscriptionQuotaSample;

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

/// Allocation-light checkpoint row used to build quota series and aggregates.
///
/// Backends return rows ordered by `(upstream_id, window, source,
/// changed_at_unix_millis, sample_id)`. For each physical quota key, the
/// projection includes the last row before the query start when one exists,
/// followed by rows inside the query's inclusive time range.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaSlimCheckpoint {
    pub upstream_id: Uuid,
    pub window: SubscriptionQuotaWindow,
    pub source: SubscriptionQuotaSource,
    pub changed_at_unix_millis: u64,
    pub sample_id: Uuid,
    pub utilization: Option<f64>,
    pub status: Option<SubscriptionQuotaStatus>,
    pub resets_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionQuotaProviderLotQuery {
    pub upstream_ids: Vec<Uuid>,
    pub windows: Vec<SubscriptionQuotaWindow>,
    pub sources: Vec<SubscriptionQuotaSource>,
    pub since_unix_millis: u64,
    pub until_unix_millis: u64,
    pub source_merge: SubscriptionQuotaSourceMerge,
    pub evaluation_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionQuotaProviderLot {
    pub upstream_id: Uuid,
    pub window: SubscriptionQuotaWindow,
    pub source: SubscriptionQuotaSourceMerge,
    pub provider_start_unix_secs: Option<u64>,
    pub provider_reset_unix_secs: Option<u64>,
    pub observed_at_unix_millis: u64,
    pub evaluation_unix_secs: u64,
    pub utilization: f64,
}

#[cfg(test)]
mod slim_projection_tests {
    use std::collections::BTreeSet;

    use serde_json::Value;

    use super::{
        SubscriptionQuotaSlimCheckpoint, SubscriptionQuotaSource, SubscriptionQuotaStatus,
        SubscriptionQuotaWindow,
    };

    #[test]
    fn slim_projection_field_set() {
        let projection = SubscriptionQuotaSlimCheckpoint {
            upstream_id: uuid::Uuid::nil(),
            window: SubscriptionQuotaWindow::FiveHour,
            source: SubscriptionQuotaSource::Header,
            changed_at_unix_millis: 1,
            sample_id: uuid::Uuid::nil(),
            utilization: Some(0.5),
            status: Some(SubscriptionQuotaStatus::Allowed),
            resets_at_unix_secs: Some(2),
        };

        let Value::Object(fields) =
            serde_json::to_value(projection).expect("slim projection serializes")
        else {
            panic!("slim projection must serialize as an object");
        };
        let actual = fields.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let expected = [
            "upstream_id",
            "window",
            "source",
            "changed_at_unix_millis",
            "sample_id",
            "utilization",
            "status",
            "resets_at_unix_secs",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected);
    }
}

#[async_trait]
pub trait UpstreamSubscriptionQuotaStore: Send + Sync {
    /// Atomically upserts the latest sidecar row and inserts change-only
    /// semantic checkpoints per `(upstream_id, window, source)` in one
    /// transaction. The latest UPSERT is monotonic: rows whose
    /// `observed_at_unix_millis` is less than the existing latest are SKIPPED
    /// for the sidecar but still considered for checkpoint history.
    async fn record_subscription_quota_samples(
        &self,
        records: &[SubscriptionQuotaSample],
    ) -> StorageResult<()>;

    async fn record_subscription_quota_sample(
        &self,
        record: &SubscriptionQuotaSample,
    ) -> StorageResult<()> {
        self.record_subscription_quota_samples(std::slice::from_ref(record))
            .await
    }

    /// Returns the latest sidecar row for each `(upstream_id, window, source)`
    /// intersected with the given upstream IDs. Empty input returns empty.
    async fn list_latest_subscription_quota_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaLatestRecord>>;

    /// Server-side bucketed downsample of the change-only checkpoint history.
    /// The caller specifies bucket width and per-series point cap. The backend
    /// MUST honor `query.sources` and `query.windows` filters and the
    /// `query.source_merge` policy (per-source series vs merged-per-window).
    async fn list_subscription_quota_series(
        &self,
        query: SubscriptionQuotaSeriesQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSeries>>;

    /// Inserts semantic checkpoints, skipping a row when the latest persisted
    /// checkpoint for `(upstream_id, window, source)` has the same semantic
    /// fingerprint. Returns the number of rows inserted.
    async fn put_subscription_quota_checkpoints(
        &self,
        records: &[SubscriptionQuotaCheckpointRecord],
    ) -> StorageResult<usize>;

    async fn put_subscription_quota_checkpoint(
        &self,
        record: &SubscriptionQuotaCheckpointRecord,
    ) -> StorageResult<usize> {
        self.put_subscription_quota_checkpoints(std::slice::from_ref(record))
            .await
    }

    /// Returns the latest checkpoint for every physical `(upstream_id, window,
    /// source)` key intersecting the given upstream IDs. Empty input returns empty.
    async fn list_latest_subscription_quota_checkpoints_for_upstreams(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<SubscriptionQuotaCheckpointRecord>>;

    /// Returns per-source checkpoint ranges. Each range includes the last
    /// checkpoint before `since_unix_millis` as `left_anchor` when available,
    /// plus all checkpoints inside `[since_unix_millis, until_unix_millis]`.
    async fn list_subscription_quota_checkpoint_ranges(
        &self,
        query: SubscriptionQuotaCheckpointRangeQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaCheckpointRange>>;
}

#[async_trait]
pub trait UpstreamSubscriptionQuotaAggregateStore: Send + Sync {
    /// Returns slim checkpoint rows in stable key/time/sample order, including
    /// the same per-key left anchor and inclusive range as checkpoint ranges.
    async fn list_subscription_quota_slim_checkpoints(
        &self,
        query: SubscriptionQuotaCheckpointRangeQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaSlimCheckpoint>>;

    async fn list_subscription_quota_provider_lots(
        &self,
        query: SubscriptionQuotaProviderLotQuery,
    ) -> StorageResult<Vec<SubscriptionQuotaProviderLot>>;
}
