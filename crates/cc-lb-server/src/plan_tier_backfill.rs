use std::path::PathBuf;

use cc_lb_storage_api::{BackfillApplyOutcome, Storage, StorageError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use crate::plan_tier_backfill_inference::{BackfillInference, infer_pool_quota_blob_backfill};

pub(crate) const BACKFILL_PROVENANCE: &str = "pool_subscription_quota_history_backfill:v1";

const BACKFILL_MARKER_KEY: &str = "plan_tier_backfill_pool_quota_v1_complete";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillReport {
    pub scanned_blobs: u64,
    pub upstreams: u64,
    pub inserted_rows: u64,
    pub skipped_upstreams: u64,
    pub malformed_blobs: u64,
    pub malformed_entries: u64,
}

#[derive(Debug, Error)]
pub enum PlanTierBackfillError {
    #[error(
        "SQLite storage path does not exist for plan-tier backfill: {path}; refusing to create a new database"
    )]
    MissingSqliteStoragePath { path: PathBuf },
    #[error("failed to check SQLite storage path for plan-tier backfill at {path}: {source}")]
    SqliteStoragePathCheck {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "plan-tier backfill saw malformed source data: malformed_blobs={malformed_blobs} malformed_entries={malformed_entries}; completion marker was not written"
    )]
    MalformedSourceData {
        malformed_blobs: u64,
        malformed_entries: u64,
    },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),
}

pub async fn run_pool_quota_blob_backfill(
    storage: &dyn Storage,
    now_unix_millis: i64,
) -> Result<BackfillReport, PlanTierBackfillError> {
    let marker = storage.get_meta_value(BACKFILL_MARKER_KEY).await?;
    if marker_indicates_complete(marker.as_deref()) {
        return Ok(BackfillReport::default());
    }

    let inference =
        crate::plan_tier_backfill_inference::infer_from_storage(storage, now_unix_millis).await?;
    let mut report = inference.report;
    report.upstreams =
        u64::try_from(inference.intervals_by_upstream.len()).map_err(|_| StorageError::Fatal {
            message: "plan tier backfill upstream count exceeds u64::MAX".to_owned(),
        })?;

    for (upstream_id, intervals) in &inference.intervals_by_upstream {
        match storage
            .backfill_upstream_plan_tier_intervals(
                *upstream_id,
                intervals,
                now_unix_millis,
                BACKFILL_PROVENANCE,
            )
            .await?
        {
            BackfillApplyOutcome::Skipped => report.skipped_upstreams += 1,
            BackfillApplyOutcome::Applied(counts) => report.inserted_rows += counts.inserted,
        }
    }

    if should_write_marker(&report) {
        let marker_json = serde_json::to_string(&BackfillMarker {
            version: 1,
            completed_at_unix_millis: now_unix_millis,
            report: report.clone(),
        })?;
        storage
            .put_meta_value(BACKFILL_MARKER_KEY, &marker_json)
            .await?;
    }
    Ok(report)
}

pub fn marker_indicates_complete(marker_json: Option<&str>) -> bool {
    marker_json
        .and_then(|value| serde_json::from_str::<BackfillMarker>(value).ok())
        .is_some_and(|marker| marker.version == 1)
}

pub const fn should_write_marker(report: &BackfillReport) -> bool {
    report.malformed_blobs == 0 && report.malformed_entries == 0
}

pub fn require_existing_sqlite_storage_path(
    path: PathBuf,
) -> Result<PathBuf, PlanTierBackfillError> {
    match path.try_exists() {
        Ok(true) => Ok(path),
        Ok(false) => Err(PlanTierBackfillError::MissingSqliteStoragePath { path }),
        Err(source) => Err(PlanTierBackfillError::SqliteStoragePathCheck { path, source }),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillMarker {
    version: u32,
    completed_at_unix_millis: i64,
    report: BackfillReport,
}
