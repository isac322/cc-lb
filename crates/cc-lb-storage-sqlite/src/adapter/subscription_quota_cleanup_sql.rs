use std::path::Path;

use crate::adapter::subscription_quota_cleanup::{
    SUBSCRIPTION_QUOTA_CHECKPOINT_CLEANUP_MARKER_KEY, SubscriptionQuotaCheckpointCleanupError,
    SubscriptionQuotaCheckpointCleanupReport,
};
use crate::{SqliteStorage, map_sqlx_error};

pub(super) enum QuotaTable {
    Checkpoints,
    Latest,
}

pub(super) async fn load_cleanup_marker(
    storage: &SqliteStorage,
) -> Result<Option<SubscriptionQuotaCheckpointCleanupReport>, SubscriptionQuotaCheckpointCleanupError>
{
    let marker: Option<String> = sqlx::query_scalar("SELECT value FROM meta_v1 WHERE key = ?")
        .bind(SUBSCRIPTION_QUOTA_CHECKPOINT_CLEANUP_MARKER_KEY)
        .fetch_optional(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
    marker
        .map(|value| serde_json::from_str::<SubscriptionQuotaCheckpointCleanupReport>(&value))
        .transpose()
        .map_err(SubscriptionQuotaCheckpointCleanupError::MalformedMarker)
}

pub(super) async fn validate_completed_cleanup_marker(
    storage: &SqliteStorage,
    marker: &SubscriptionQuotaCheckpointCleanupReport,
) -> Result<(), SubscriptionQuotaCheckpointCleanupError> {
    if raw_observation_table_exists(storage).await? {
        return Err(
            SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
                message: "cleanup marker exists but raw observation table is still present"
                    .to_owned(),
            },
        );
    }
    let checkpoint_rows = table_count(storage, QuotaTable::Checkpoints).await?;
    if checkpoint_rows != marker.checkpoint_rows_preserved {
        return Err(
            SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
                message: format!(
                    "cleanup marker checkpoint rows {} != actual {checkpoint_rows}",
                    marker.checkpoint_rows_preserved
                ),
            },
        );
    }
    let latest_rows = table_count(storage, QuotaTable::Latest).await?;
    if latest_rows != marker.latest_rows_preserved {
        return Err(
            SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
                message: format!(
                    "cleanup marker latest rows {} != actual {latest_rows}",
                    marker.latest_rows_preserved
                ),
            },
        );
    }
    let integrity_check = integrity_check(storage).await?;
    if integrity_check == "ok" {
        return Ok(());
    }
    Err(
        SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
            message: format!("PRAGMA integrity_check returned {integrity_check}"),
        },
    )
}

pub(super) async fn preflight_cleanup_marker_write(
    storage: &SqliteStorage,
) -> Result<(), SubscriptionQuotaCheckpointCleanupError> {
    let mut tx = storage.begin_immediate().await?;
    sqlx::query(
        "INSERT INTO meta_v1 (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(SUBSCRIPTION_QUOTA_CHECKPOINT_CLEANUP_MARKER_KEY)
    .bind(preflight_marker_json())
    .execute(&mut *tx)
    .await
    .map_err(map_sqlx_error)?;
    sqlx::query("DELETE FROM meta_v1 WHERE key = ?")
        .bind(SUBSCRIPTION_QUOTA_CHECKPOINT_CLEANUP_MARKER_KEY)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) async fn drop_raw_observation_history(
    storage: &SqliteStorage,
) -> Result<(), SubscriptionQuotaCheckpointCleanupError> {
    let mut tx = storage.begin_immediate().await?;
    sqlx::query("DROP INDEX IF EXISTS upstream_subscription_quota_obs_series_idx")
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    sqlx::query("DROP INDEX IF EXISTS upstream_subscription_quota_obs_gc_idx")
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    sqlx::query("DROP TABLE upstream_subscription_quota_observations_v1")
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) async fn checkpoint_truncate_wal(
    storage: &SqliteStorage,
) -> Result<(), SubscriptionQuotaCheckpointCleanupError> {
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .fetch_all(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) async fn vacuum_database(
    storage: &SqliteStorage,
) -> Result<(), SubscriptionQuotaCheckpointCleanupError> {
    sqlx::query("VACUUM")
        .execute(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
    Ok(())
}

pub(super) async fn integrity_check(
    storage: &SqliteStorage,
) -> Result<String, SubscriptionQuotaCheckpointCleanupError> {
    sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(storage.pool())
        .await
        .map_err(map_sqlx_error)
        .map_err(SubscriptionQuotaCheckpointCleanupError::Storage)
}

pub(super) async fn table_count(
    storage: &SqliteStorage,
    table: QuotaTable,
) -> Result<u64, SubscriptionQuotaCheckpointCleanupError> {
    let count = match table {
        QuotaTable::Checkpoints => {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1",
            )
            .fetch_one(storage.pool())
            .await
        }
        QuotaTable::Latest => {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM upstream_subscription_quota_latest_v1",
            )
            .fetch_one(storage.pool())
            .await
        }
    }
    .map_err(map_sqlx_error)?;
    u64::try_from(count).map_err(
        |_| SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
            message: format!("negative row count for quota cleanup table: {count}"),
        },
    )
}

pub(super) async fn pragma_u64(
    storage: &SqliteStorage,
    sql: &'static str,
) -> Result<u64, SubscriptionQuotaCheckpointCleanupError> {
    let value = sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(storage.pool())
        .await
        .map_err(map_sqlx_error)?;
    u64::try_from(value).map_err(
        |_| SubscriptionQuotaCheckpointCleanupError::ValidationMismatch {
            message: format!("PRAGMA returned negative value {value}"),
        },
    )
}

pub(super) fn database_file_size(
    database_path: &Path,
) -> Result<u64, SubscriptionQuotaCheckpointCleanupError> {
    database_path
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(
            |source| SubscriptionQuotaCheckpointCleanupError::DatabaseFileMetadata {
                path: database_path.to_path_buf(),
                source,
            },
        )
}

pub(super) async fn write_cleanup_marker(
    storage: &SqliteStorage,
    report: &SubscriptionQuotaCheckpointCleanupReport,
) -> Result<(), SubscriptionQuotaCheckpointCleanupError> {
    let value = serde_json::to_string(report)?;
    sqlx::query(
        "INSERT INTO meta_v1 (key, value) VALUES (?, ?) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(SUBSCRIPTION_QUOTA_CHECKPOINT_CLEANUP_MARKER_KEY)
    .bind(value)
    .execute(storage.pool())
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn raw_observation_table_exists(
    storage: &SqliteStorage,
) -> Result<bool, SubscriptionQuotaCheckpointCleanupError> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_schema \
         WHERE type = 'table' AND name = 'upstream_subscription_quota_observations_v1'",
    )
    .fetch_one(storage.pool())
    .await
    .map_err(map_sqlx_error)?;
    Ok(count > 0)
}

fn preflight_marker_json() -> &'static str {
    r#"{"version":1,"completed_at_unix_millis":0,"backfill":{"version":1,"completed_at_unix_millis":0,"scanned_raw_rows":0,"inserted_checkpoint_rows":0,"skipped_duplicate_semantic_rows":0,"malformed_rows":0,"validation_outcome":"preflight"},"raw_observation_rows_dropped":0,"raw_observation_table_dropped":false,"checkpoint_rows_preserved":0,"latest_rows_preserved":0,"page_count_before":0,"page_count_after":0,"freelist_count_before":0,"freelist_count_after":0,"database_size_bytes_before":0,"database_size_bytes_after":0,"integrity_check":"preflight","validation_outcome":"preflight"}"#
}
