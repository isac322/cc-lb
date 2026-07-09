use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::SqliteStorage;
use crate::adapter::subscription_quota_backfill::{
    SubscriptionQuotaCheckpointBackfillError, SubscriptionQuotaCheckpointBackfillReport,
    completed_at_unix_millis, validate_compacted_history_against_raw,
};
use crate::adapter::subscription_quota_cleanup_sql::{
    QuotaTable, checkpoint_truncate_wal, database_file_size, drop_raw_observation_history,
    integrity_check, load_cleanup_marker, pragma_u64, preflight_cleanup_marker_write, table_count,
    vacuum_database, validate_completed_cleanup_marker, write_cleanup_marker,
};

pub const SUBSCRIPTION_QUOTA_CHECKPOINT_CLEANUP_MARKER_KEY: &str =
    "subscription_quota_checkpoint_cleanup_v1_complete";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionQuotaCheckpointCleanupReport {
    pub version: u32,
    pub completed_at_unix_millis: u64,
    pub backfill: SubscriptionQuotaCheckpointBackfillReport,
    pub raw_observation_rows_dropped: u64,
    pub raw_observation_table_dropped: bool,
    pub checkpoint_rows_preserved: u64,
    pub latest_rows_preserved: u64,
    pub page_count_before: u64,
    pub page_count_after: u64,
    pub freelist_count_before: u64,
    pub freelist_count_after: u64,
    pub database_size_bytes_before: u64,
    pub database_size_bytes_after: u64,
    pub integrity_check: String,
    pub validation_outcome: String,
}

#[derive(Debug, Error)]
pub enum SubscriptionQuotaCheckpointCleanupError {
    #[error("subscription quota checkpoint cleanup marker is malformed: {0}")]
    MalformedMarker(#[from] serde_json::Error),
    #[error(
        "failed to stat SQLite database file for subscription quota checkpoint cleanup at {path}: {source}"
    )]
    DatabaseFileMetadata {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("subscription quota checkpoint cleanup validation failed: {message}")]
    ValidationMismatch { message: String },
    #[error(transparent)]
    Backfill(#[from] SubscriptionQuotaCheckpointBackfillError),
    #[error(transparent)]
    Storage(#[from] cc_lb_storage_api::StorageError),
}

impl SqliteStorage {
    pub async fn cleanup_compacted_subscription_quota_history(
        &self,
        database_path: &Path,
    ) -> Result<SubscriptionQuotaCheckpointCleanupReport, SubscriptionQuotaCheckpointCleanupError>
    {
        if let Some(marker) = load_cleanup_marker(self).await? {
            validate_completed_cleanup_marker(self, &marker).await?;
            return Ok(marker);
        }

        let backfill = self.compact_subscription_quota_history().await?;
        let expected = validate_compacted_history_against_raw(self).await?;
        let page_count_before = pragma_u64(self, "PRAGMA page_count").await?;
        let freelist_count_before = pragma_u64(self, "PRAGMA freelist_count").await?;
        let database_size_bytes_before = database_file_size(database_path)?;
        let latest_rows_preserved = table_count(self, QuotaTable::Latest).await?;
        preflight_cleanup_marker_write(self).await?;

        drop_raw_observation_history(self).await?;
        checkpoint_truncate_wal(self).await?;
        vacuum_database(self).await?;
        checkpoint_truncate_wal(self).await?;

        let integrity_check = integrity_check(self).await?;
        if integrity_check != "ok" {
            return Err(
                SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
                    message: format!("PRAGMA integrity_check returned {integrity_check}"),
                },
            );
        }
        let page_count_after = pragma_u64(self, "PRAGMA page_count").await?;
        let freelist_count_after = pragma_u64(self, "PRAGMA freelist_count").await?;
        let database_size_bytes_after = database_file_size(database_path)?;
        let checkpoint_rows_preserved = table_count(self, QuotaTable::Checkpoints).await?;
        if checkpoint_rows_preserved != expected.inserted_checkpoint_rows {
            return Err(
                SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
                    message: format!(
                        "checkpoint rows after cleanup {checkpoint_rows_preserved} != expected {}",
                        expected.inserted_checkpoint_rows
                    ),
                },
            );
        }
        let latest_rows_after = table_count(self, QuotaTable::Latest).await?;
        if latest_rows_after != latest_rows_preserved {
            return Err(
                SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
                    message: format!(
                        "latest rows after cleanup {latest_rows_after} != before cleanup {latest_rows_preserved}"
                    ),
                },
            );
        }

        let report = SubscriptionQuotaCheckpointCleanupReport {
            version: 1,
            completed_at_unix_millis: completed_at_unix_millis(self)?,
            backfill,
            raw_observation_rows_dropped: expected.scanned_raw_rows,
            raw_observation_table_dropped: true,
            checkpoint_rows_preserved,
            latest_rows_preserved,
            page_count_before,
            page_count_after,
            freelist_count_before,
            freelist_count_after,
            database_size_bytes_before,
            database_size_bytes_after,
            integrity_check,
            validation_outcome: "passed".to_owned(),
        };
        write_cleanup_marker(self, &report).await?;
        Ok(report)
    }
}
