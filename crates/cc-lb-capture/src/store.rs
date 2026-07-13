//! Storage backend for captured data.

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use sqlx::{
    Sqlite, SqlitePool, Transaction,
    migrate::MigrateError,
    sqlite::{
        SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions,
        SqliteSynchronous,
    },
};

use crate::schema::CaptureRecord;

/// Failures from opening or writing the isolated capture database.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CaptureStoreError {
    #[error("capture database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("capture database migration failed: {0}")]
    Migration(#[from] MigrateError),
    #[error("capture record serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("capture database path is not valid UTF-8: {path:?}")]
    InvalidPath { path: PathBuf },
    #[error("capture field {field} value {value} exceeds SQLite INTEGER range")]
    IntegerOutOfRange { field: &'static str, value: u64 },
}

/// A connection pool dedicated to capture data.
#[derive(Clone)]
pub struct CaptureStore {
    pool: SqlitePool,
}

impl CaptureStore {
    /// Returns the store's dedicated SQLite pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Starts a write transaction that acquires SQLite's write lock immediately.
    pub async fn begin_immediate(&self) -> Result<Transaction<'static, Sqlite>, CaptureStoreError> {
        begin_immediate(&self.pool).await
    }

    /// Persists one complete capture record.
    pub async fn insert_record(&self, record: &CaptureRecord) -> Result<(), CaptureStoreError> {
        let mut transaction = self.begin_immediate().await?;
        self.insert_record_in_tx(&mut transaction, record).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn insert_record_in_tx(
        &self,
        transaction: &mut Transaction<'static, Sqlite>,
        record: &CaptureRecord,
    ) -> Result<(), CaptureStoreError> {
        let payload_json = serde_json::to_string(record)?;
        let disposition_json = serde_json::to_string(&record.disposition)?;
        let disposition = disposition_json.trim_matches('"');
        let chosen_upstream_id = record
            .response
            .chosen_upstream_id
            .map(|upstream_id| upstream_id.to_string());
        let ts_unix_ms = sqlite_integer("captured_at_unix_ms", record.input.captured_at_unix_ms)?;
        let cache_read_input_tokens = optional_sqlite_integer(
            "cache_read_input_tokens",
            record.response.cache_read_input_tokens,
        )?;
        let cache_creation_5m = optional_sqlite_integer(
            "cache_creation_input_tokens_5m",
            record.response.cache_creation_input_tokens_5m,
        )?;
        let cache_creation_1h = optional_sqlite_integer(
            "cache_creation_input_tokens_1h",
            record.response.cache_creation_input_tokens_1h,
        )?;
        let input_tokens = optional_sqlite_integer("input_tokens", record.response.input_tokens)?;
        let output_tokens =
            optional_sqlite_integer("output_tokens", record.response.output_tokens)?;

        sqlx::query(
            "INSERT INTO capture_v1 (
                event_id, request_id, ts_unix_ms, canonical_model, chosen_upstream_id,
                disposition, attempt_num, cache_read_input_tokens, cache_creation_5m,
                cache_creation_1h, input_tokens, output_tokens, client_status, upstream_status,
                schema_version, salt_version, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&record.input.event_id)
        .bind(&record.input.request_id)
        .bind(ts_unix_ms)
        .bind(&record.input.canonical_model_id)
        .bind(chosen_upstream_id)
        .bind(disposition)
        .bind(record.response.attempt_num)
        .bind(cache_read_input_tokens)
        .bind(cache_creation_5m)
        .bind(cache_creation_1h)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(record.response.client_status)
        .bind(record.response.upstream_status)
        .bind(record.input.capture_schema_version)
        .bind(&record.input.salt_version)
        .bind(payload_json)
        .execute(&mut **transaction)
        .await?;

        Ok(())
    }
}

/// Opens and migrates a standalone SQLite database for capture data.
pub async fn open_capture_store(path: &Path) -> Result<CaptureStore, CaptureStoreError> {
    let database_url = path
        .to_str()
        .ok_or_else(|| CaptureStoreError::InvalidPath {
            path: path.to_path_buf(),
        })?;
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5));

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
        .execute(&pool)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    Ok(CaptureStore { pool })
}

/// Starts an immediate transaction on a capture pool.
pub async fn begin_immediate(
    pool: &SqlitePool,
) -> Result<Transaction<'static, Sqlite>, CaptureStoreError> {
    pool.begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(CaptureStoreError::from)
}

fn sqlite_integer(field: &'static str, value: u64) -> Result<i64, CaptureStoreError> {
    i64::try_from(value).map_err(|_| CaptureStoreError::IntegerOutOfRange { field, value })
}

fn optional_sqlite_integer(
    field: &'static str,
    value: Option<u64>,
) -> Result<Option<i64>, CaptureStoreError> {
    value.map(|value| sqlite_integer(field, value)).transpose()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::open_capture_store;
    use crate::schema::{CaptureRecord, CapturedRequestInput, CapturedResponse, Disposition};
    use cc_lb_domain::{CachePricingSummary, RoutingTrace};
    use uuid::Uuid;

    fn capture_record() -> CaptureRecord {
        CaptureRecord {
            input: CapturedRequestInput {
                event_id: "event-01".to_owned(),
                request_id: "request-01".to_owned(),
                thread_id: None,
                canonical_model_id: "claude-sonnet-4-5".to_owned(),
                cache_pricing: CachePricingSummary {
                    status: "available".to_owned(),
                    input_micros_per_million: Some(3_000_000),
                    cache_creation_5m_micros_per_million: Some(3_750_000),
                    cache_creation_1h_micros_per_million: Some(6_000_000),
                    cache_read_micros_per_million: Some(300_000),
                },
                breakpoints: Vec::new(),
                candidates: Vec::new(),
                subscription_preference_input_upstream_ids: Vec::new(),
                routing_trace: RoutingTrace {
                    stages: Vec::new(),
                    terminal_decision: None,
                },
                captured_at_unix_ms: 1_700_000_000_123,
                salt_version: "v11".to_owned(),
                cache_cost_basis_version: "v1".to_owned(),
                capture_schema_version: 1,
                build_version: "test".to_owned(),
            },
            response: CapturedResponse {
                input_tokens: Some(2_000),
                output_tokens: Some(500),
                cache_read_input_tokens: Some(1_000),
                cache_creation_input_tokens_5m: Some(200),
                cache_creation_input_tokens_1h: Some(300),
                chosen_upstream_id: Some(Uuid::from_u128(1)),
                upstream_status: Some(200),
                client_status: Some(200),
                duration_ms: Some(345),
                attempt_num: Some(2),
            },
            disposition: Disposition::RoutedDispatchedSuccess,
        }
    }

    #[tokio::test]
    async fn inserts_full_record_when_store_opens() -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("capture.sqlite");
        let store = open_capture_store(&path).await?;
        let record = capture_record();

        // When
        store.insert_record(&record).await?;

        // Then
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
            .fetch_one(store.pool())
            .await?;
        let payload: String =
            sqlx::query_scalar("SELECT payload_json FROM capture_v1 WHERE event_id = ?")
                .bind(&record.input.event_id)
                .fetch_one(store.pool())
                .await?;
        assert_eq!(count, 1);
        assert_eq!(serde_json::from_str::<CaptureRecord>(&payload)?, record);
        Ok(())
    }

    #[tokio::test]
    async fn reruns_migrations_when_store_reopens() -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("capture.sqlite");
        let first = open_capture_store(&path).await?;
        drop(first);

        // When
        let reopened = open_capture_store(&path).await?;

        // Then
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
            .fetch_one(reopened.pool())
            .await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn returns_error_when_parent_directory_does_not_exist() {
        // Given
        let directory = tempfile::tempdir().unwrap();
        let path = directory
            .path()
            .join("missing-directory")
            .join("capture.sqlite");

        // When
        let result = open_capture_store(&path).await;

        // Then
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn enables_incremental_auto_vacuum_before_migrations()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("capture.sqlite");

        // When
        let store = open_capture_store(&path).await?;

        // Then
        let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(mode, 2);
        Ok(())
    }
}
