//! Retention policies for captured data.

use sqlx::SqlitePool;

use crate::store::CaptureStoreError;

/// Removes all but the newest `cap` capture rows and reclaims free pages.
///
/// # Errors
/// Returns [`CaptureStoreError`] when the cap cannot fit SQLite's integer range,
/// the delete fails, or incremental vacuuming fails.
pub async fn prune_by_row_cap(pool: &SqlitePool, cap: u64) -> Result<u64, CaptureStoreError> {
    let sqlite_cap = i64::try_from(cap).map_err(|_| CaptureStoreError::IntegerOutOfRange {
        field: "retention_row_cap",
        value: cap,
    })?;
    let result = sqlx::query(
        "DELETE FROM capture_v1
         WHERE event_id NOT IN (
             SELECT event_id FROM capture_v1 ORDER BY ts_unix_ms DESC LIMIT ?
         )",
    )
    .bind(sqlite_cap)
    .execute(pool)
    .await?;
    let deleted = result.rows_affected();

    if deleted > 0 {
        sqlx::query("PRAGMA incremental_vacuum")
            .execute(pool)
            .await?;
    }

    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::prune_by_row_cap;
    use crate::store::open_capture_store;
    use sqlx::SqlitePool;

    async fn insert_rows(pool: &SqlitePool, count: u64) -> Result<(), Box<dyn std::error::Error>> {
        for index in 0..count {
            sqlx::query(
                "INSERT INTO capture_v1 (
                    event_id, request_id, ts_unix_ms, disposition, schema_version, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(format!("event-{index:03}"))
            .bind(format!("request-{index:03}"))
            .bind(i64::try_from(index)?)
            .bind("routed_dispatched_success")
            .bind(1_i64)
            .bind("{}")
            .execute(pool)
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn keeps_newest_rows_when_count_exceeds_cap() -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let directory = tempfile::tempdir()?;
        let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;
        let cap = 100_u64;
        insert_rows(store.pool(), cap + 50).await?;

        // When
        let deleted = prune_by_row_cap(store.pool(), cap).await?;

        // Then
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
            .fetch_one(store.pool())
            .await?;
        let oldest_surviving: i64 = sqlx::query_scalar("SELECT MIN(ts_unix_ms) FROM capture_v1")
            .fetch_one(store.pool())
            .await?;
        let old_event_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1 WHERE event_id = ?")
                .bind("event-000")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(deleted, 50);
        assert_eq!(count, i64::try_from(cap)?);
        assert_eq!(oldest_surviving, 50);
        assert_eq!(old_event_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn returns_zero_when_table_is_empty() -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let directory = tempfile::tempdir()?;
        let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;

        // When
        let deleted = prune_by_row_cap(store.pool(), 100).await?;

        // Then
        assert_eq!(deleted, 0);
        Ok(())
    }
}
