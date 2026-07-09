use cc_lb_clock::unix_millis;
use cc_lb_storage_api::{
    StorageError, SubscriptionQuotaCheckpointRecord, SubscriptionQuotaSample,
    SubscriptionQuotaSemanticFingerprint, SubscriptionQuotaSource, SubscriptionQuotaWindow,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

use crate::adapter::upstream_subscription_quota::{
    insert_checkpoint_if_changed, row_to_checkpoint_record, row_to_record,
};
use crate::{SqliteStorage, map_sqlx_error};

pub const SUBSCRIPTION_QUOTA_CHECKPOINT_BACKFILL_MARKER_KEY: &str =
    "subscription_quota_checkpoint_backfill_v1_complete";

type ReplayKey = (Uuid, SubscriptionQuotaWindow, SubscriptionQuotaSource);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionQuotaCheckpointBackfillReport {
    pub version: u32,
    pub completed_at_unix_millis: u64,
    pub scanned_raw_rows: u64,
    pub inserted_checkpoint_rows: u64,
    pub skipped_duplicate_semantic_rows: u64,
    pub malformed_rows: u64,
    pub validation_outcome: String,
}

#[derive(Debug, Error)]
pub enum SubscriptionQuotaCheckpointBackfillError {
    #[error("subscription quota checkpoint backfill marker is malformed: {0}")]
    MalformedMarker(#[from] serde_json::Error),
    #[error(
        "subscription quota checkpoint backfill saw malformed raw rows: malformed_rows={malformed_rows}: {source}"
    )]
    MalformedRows {
        malformed_rows: u64,
        #[source]
        source: StorageError,
    },
    #[error("subscription quota checkpoint backfill validation failed: {message}")]
    ValidationMismatch { message: String },
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl SqliteStorage {
    pub async fn compact_subscription_quota_history(
        &self,
    ) -> Result<SubscriptionQuotaCheckpointBackfillReport, SubscriptionQuotaCheckpointBackfillError>
    {
        if let Some(marker) = load_completed_marker(self).await? {
            return Ok(marker);
        }

        let mut tx = self.begin_immediate().await?;
        ensure_empty_checkpoint_table(&mut tx).await?;
        let observations = read_ordered_observations(&mut tx).await?;
        let expected = expected_checkpoints(&observations)?;
        let mut inserted = 0u64;
        for checkpoint in &expected.checkpoints {
            if insert_checkpoint_if_changed(&mut tx, checkpoint).await? {
                inserted += 1;
            }
        }
        validate_checkpoint_rows(&mut tx, &expected.checkpoints).await?;
        if inserted != expected.inserted_checkpoint_rows {
            return Err(
                SubscriptionQuotaCheckpointBackfillError::ValidationMismatch {
                    message: format!(
                        "inserted checkpoint rows {inserted} != expected {}",
                        expected.inserted_checkpoint_rows
                    ),
                },
            );
        }

        let report = SubscriptionQuotaCheckpointBackfillReport {
            version: 1,
            completed_at_unix_millis: completed_at_unix_millis(self)?,
            scanned_raw_rows: u64::try_from(observations.len()).map_err(|_| {
                SubscriptionQuotaCheckpointBackfillError::ValidationMismatch {
                    message: "raw observation count exceeds u64::MAX".to_owned(),
                }
            })?,
            inserted_checkpoint_rows: inserted,
            skipped_duplicate_semantic_rows: expected.skipped_duplicate_semantic_rows,
            malformed_rows: 0,
            validation_outcome: "passed".to_owned(),
        };
        write_completed_marker(&mut tx, &report).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(report)
    }
}

pub(super) struct ExpectedCheckpoints {
    pub(super) checkpoints: Vec<SubscriptionQuotaCheckpointRecord>,
    pub(super) inserted_checkpoint_rows: u64,
    pub(super) skipped_duplicate_semantic_rows: u64,
    pub(super) scanned_raw_rows: u64,
}

async fn load_completed_marker(
    storage: &SqliteStorage,
) -> Result<
    Option<SubscriptionQuotaCheckpointBackfillReport>,
    SubscriptionQuotaCheckpointBackfillError,
> {
    let marker: Option<String> = sqlx::query_scalar("SELECT value FROM meta_v1 WHERE key = ?")
        .bind(SUBSCRIPTION_QUOTA_CHECKPOINT_BACKFILL_MARKER_KEY)
        .fetch_optional(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
    marker
        .map(|value| serde_json::from_str::<SubscriptionQuotaCheckpointBackfillReport>(&value))
        .transpose()
        .map_err(SubscriptionQuotaCheckpointBackfillError::MalformedMarker)
}

async fn ensure_empty_checkpoint_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), SubscriptionQuotaCheckpointBackfillError> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1",
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    if count == 0 {
        return Ok(());
    }
    Err(
        SubscriptionQuotaCheckpointBackfillError::ValidationMismatch {
            message: format!(
                "checkpoint table already contains {count} rows and completion marker is absent"
            ),
        },
    )
}

async fn read_ordered_observations(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<Vec<SubscriptionQuotaSample>, SubscriptionQuotaCheckpointBackfillError> {
    let rows = sqlx::query(
        "SELECT * FROM upstream_subscription_quota_observations_v1 \
         ORDER BY upstream_id ASC, window ASC, source ASC, observed_at_unix_millis ASC, sample_id ASC",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;

    rows.into_iter()
        .map(parse_observation_row)
        .collect::<Result<Vec<_>, _>>()
}

fn parse_observation_row(
    row: SqliteRow,
) -> Result<SubscriptionQuotaSample, SubscriptionQuotaCheckpointBackfillError> {
    row_to_record(row).map_err(
        |source| SubscriptionQuotaCheckpointBackfillError::MalformedRows {
            malformed_rows: 1,
            source,
        },
    )
}

fn expected_checkpoints(
    observations: &[SubscriptionQuotaSample],
) -> Result<ExpectedCheckpoints, SubscriptionQuotaCheckpointBackfillError> {
    let mut latest = BTreeMap::<ReplayKey, SubscriptionQuotaSemanticFingerprint>::new();
    let mut checkpoints = Vec::new();
    let mut skipped_duplicate_semantic_rows = 0u64;
    for observation in observations {
        let fingerprint = observation.semantic_checkpoint_fingerprint();
        let key = (
            observation.upstream_id,
            observation.window,
            observation.source,
        );
        if latest.get(&key) == Some(&fingerprint) {
            skipped_duplicate_semantic_rows += 1;
            continue;
        }
        latest.insert(key, fingerprint);
        checkpoints.push(SubscriptionQuotaCheckpointRecord::from(observation));
    }
    let inserted_checkpoint_rows = u64::try_from(checkpoints.len()).map_err(|_| {
        SubscriptionQuotaCheckpointBackfillError::ValidationMismatch {
            message: "expected checkpoint count exceeds u64::MAX".to_owned(),
        }
    })?;
    Ok(ExpectedCheckpoints {
        checkpoints,
        inserted_checkpoint_rows,
        skipped_duplicate_semantic_rows,
        scanned_raw_rows: u64::try_from(observations.len()).map_err(|_| {
            SubscriptionQuotaCheckpointBackfillError::ValidationMismatch {
                message: "raw observation count exceeds u64::MAX".to_owned(),
            }
        })?,
    })
}

pub(super) async fn validate_compacted_history_against_raw(
    storage: &SqliteStorage,
) -> Result<ExpectedCheckpoints, SubscriptionQuotaCheckpointBackfillError> {
    let mut tx = storage.begin_immediate().await?;
    let observations = read_ordered_observations(&mut tx).await?;
    let expected = expected_checkpoints(&observations)?;
    validate_checkpoint_rows(&mut tx, &expected.checkpoints).await?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(expected)
}

async fn validate_checkpoint_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    expected: &[SubscriptionQuotaCheckpointRecord],
) -> Result<(), SubscriptionQuotaCheckpointBackfillError> {
    let rows = sqlx::query(
        "SELECT * FROM upstream_subscription_quota_checkpoints_v1 \
         ORDER BY upstream_id ASC, window ASC, source ASC, changed_at_unix_millis ASC, sample_id ASC",
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    let actual = rows
        .into_iter()
        .map(row_to_checkpoint_record)
        .collect::<Result<Vec<_>, _>>()?;
    if actual == expected {
        return Ok(());
    }
    Err(
        SubscriptionQuotaCheckpointBackfillError::ValidationMismatch {
            message: format!(
                "actual checkpoint rows {} did not match expected rows {}",
                actual.len(),
                expected.len()
            ),
        },
    )
}

pub(super) fn completed_at_unix_millis(
    storage: &SqliteStorage,
) -> Result<u64, SubscriptionQuotaCheckpointBackfillError> {
    u64::try_from(unix_millis(storage.clock().now())).map_err(|_| {
        SubscriptionQuotaCheckpointBackfillError::ValidationMismatch {
            message: "completed_at_unix_millis exceeds u64::MAX".to_owned(),
        }
    })
}

async fn write_completed_marker(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    report: &SubscriptionQuotaCheckpointBackfillReport,
) -> Result<(), SubscriptionQuotaCheckpointBackfillError> {
    let value = serde_json::to_string(report)?;
    sqlx::query(
        "INSERT INTO meta_v1 (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(SUBSCRIPTION_QUOTA_CHECKPOINT_BACKFILL_MARKER_KEY)
    .bind(value)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}
