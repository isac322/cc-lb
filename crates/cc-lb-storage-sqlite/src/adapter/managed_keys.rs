use async_trait::async_trait;
use cc_lb_clock::{Clock, unix_secs};
use cc_lb_storage_api::{
    ManagedKeyStore, StorageError, StorageResult,
    types::{ApiKeyMutation, IssueParams, KeyStatus, Limit, StoredApiKeyRecord},
    validate_identifier,
};
use sqlx::{Row, sqlite::SqliteRow};

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl ManagedKeyStore for SqliteStorage {
    async fn issue(
        &self,
        _principal_id: &str,
        _key_id: &str,
        _params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        Err(StorageError::Unavailable {
            message: "api key issuance is paused".to_owned(),
        })
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let row = sqlx::query(SELECT_BY_KEY_SQL)
            .bind(principal_id)
            .bind(key_id)
            .fetch_optional(self.pool())
            .await
            .map_err(map_managed_sqlx_error)?;

        row.map(row_to_record).transpose()
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        let row = sqlx::query(SELECT_BY_INDEX_HASH_SQL)
            .bind(index_hash.as_slice())
            .fetch_optional(self.pool())
            .await
            .map_err(map_managed_sqlx_error)?;

        row.map(row_to_keyed_record).transpose()
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        validate_identifier("principal_id", principal_id)?;

        let rows = sqlx::query(SELECT_BY_PRINCIPAL_SQL)
            .bind(principal_id)
            .fetch_all(self.pool())
            .await
            .map_err(map_managed_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        let rows = sqlx::query(SELECT_ALL_SQL)
            .fetch_all(self.pool())
            .await
            .map_err(map_managed_sqlx_error)?;

        rows.into_iter().map(row_to_keyed_record).collect()
    }

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> StorageResult<()> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let Some(mut record) = self.get(principal_id, key_id).await? else {
            return Ok(());
        };

        apply_mutation(&mut record, mutation, self.clock());
        update_record(self, principal_id, key_id, &record).await
    }

    async fn revoke_zero_secrets(&self, principal_id: &str, key_id: &str) -> StorageResult<()> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let Some(mut record) = self.get(principal_id, key_id).await? else {
            return Ok(());
        };

        record.status = KeyStatus::Revoked;
        if record.revoked_at_unix_secs.is_none() {
            record.revoked_at_unix_secs = Some(unix_secs(self.clock().now()));
        }
        record.index_hash = [0; 32];
        record.verify_hash = [0; 32];
        record.secret_salt = [0; 16];

        update_record(self, principal_id, key_id, &record).await
    }
}

const SELECT_BY_KEY_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, limit_overrides, status, expires_at, last_4, \
     description, index_hash FROM managed_keys_v1 WHERE principal_id = ? AND key_id = ?";
const SELECT_BY_INDEX_HASH_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, limit_overrides, status, expires_at, last_4, \
     description, index_hash FROM managed_keys_v1 WHERE index_hash = ? AND status != 'revoked'";
const SELECT_BY_PRINCIPAL_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, limit_overrides, status, expires_at, last_4, \
     description, index_hash FROM managed_keys_v1 WHERE principal_id = ? ORDER BY key_id ASC";
const SELECT_ALL_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, limit_overrides, status, expires_at, last_4, \
     description, index_hash FROM managed_keys_v1 ORDER BY principal_id ASC, key_id ASC";

async fn update_record(
    storage: &SqliteStorage,
    principal_id: &str,
    key_id: &str,
    record: &StoredApiKeyRecord,
) -> StorageResult<()> {
    let limit_overrides = serde_json::to_string(&record.limit_overrides)?;

    sqlx::query(
        "UPDATE managed_keys_v1 SET \
         secret_hash = ?, expires_at = ?, status = ?, label = ?, revoked_at = ?, verify_hash = ?, \
         secret_salt = ?, limit_overrides = ?, last_4 = ?, description = ?, \
         index_hash = ?, updated_at = ? \
         WHERE principal_id = ? AND key_id = ?",
    )
    .bind(&record.key_hash_b64)
    .bind(option_u64_to_i64(
        record.expires_at_unix_secs,
        "expires_at_unix_secs",
    )?)
    .bind(key_status_as_str(record.status))
    .bind(&record.label)
    .bind(option_u64_to_i64(
        record.revoked_at_unix_secs,
        "revoked_at_unix_secs",
    )?)
    .bind(record.verify_hash.as_slice())
    .bind(record.secret_salt.as_slice())
    .bind(limit_overrides)
    .bind(&record.last_4)
    .bind(&record.description)
    .bind(record.index_hash.as_slice())
    .bind(now_i64(storage.clock())?)
    .bind(principal_id)
    .bind(key_id)
    .execute(storage.pool())
    .await
    .map_err(map_managed_sqlx_error)?;

    Ok(())
}

fn row_to_keyed_record(row: SqliteRow) -> StorageResult<(String, String, StoredApiKeyRecord)> {
    let principal_id = row
        .try_get::<String, _>("principal_id")
        .map_err(map_sqlx_error)?;
    let key_id = row.try_get::<String, _>("key_id").map_err(map_sqlx_error)?;
    let record = row_to_record(row)?;
    Ok((principal_id, key_id, record))
}

fn row_to_record(row: SqliteRow) -> StorageResult<StoredApiKeyRecord> {
    let issued_at_unix_secs = row
        .try_get::<i64, _>("created_at")
        .map_err(map_sqlx_error)?;
    let revoked_at_unix_secs = row
        .try_get::<Option<i64>, _>("revoked_at")
        .map_err(map_sqlx_error)?;
    let expires_at_unix_secs = row
        .try_get::<Option<i64>, _>("expires_at")
        .map_err(map_sqlx_error)?;
    let limit_overrides = serde_json::from_str::<Vec<Limit>>(
        &row.try_get::<String, _>("limit_overrides")
            .map_err(map_sqlx_error)?,
    )?;

    Ok(StoredApiKeyRecord {
        label: row.try_get("label").map_err(map_sqlx_error)?,
        issued_at_unix_secs: i64_to_u64(issued_at_unix_secs, "issued_at_unix_secs")?,
        revoked_at_unix_secs: revoked_at_unix_secs
            .map(|value| i64_to_u64(value, "revoked_at_unix_secs"))
            .transpose()?,
        key_hash_b64: row.try_get("secret_hash").map_err(map_sqlx_error)?,
        verify_hash: vec_to_array(
            row.try_get("verify_hash").map_err(map_sqlx_error)?,
            "verify_hash",
        )?,
        secret_salt: vec_to_array(
            row.try_get("secret_salt").map_err(map_sqlx_error)?,
            "secret_salt",
        )?,
        limit_overrides,
        status: parse_key_status(&row.try_get::<String, _>("status").map_err(map_sqlx_error)?)?,
        expires_at_unix_secs: expires_at_unix_secs
            .map(|value| i64_to_u64(value, "expires_at_unix_secs"))
            .transpose()?,
        last_4: row.try_get("last_4").map_err(map_sqlx_error)?,
        description: row.try_get("description").map_err(map_sqlx_error)?,
        index_hash: vec_to_array(
            row.try_get("index_hash").map_err(map_sqlx_error)?,
            "index_hash",
        )?,
    })
}

fn apply_mutation(record: &mut StoredApiKeyRecord, mutation: ApiKeyMutation, clock: &dyn Clock) {
    if let Some(label) = mutation.label {
        record.label = label;
    }
    if let Some(description) = mutation.description {
        record.description = description;
    }
    if let Some(expires_at_unix_secs) = mutation.expires_at_unix_secs {
        record.expires_at_unix_secs = expires_at_unix_secs;
    }
    if let Some(limit_overrides) = mutation.limit_overrides {
        record.limit_overrides = limit_overrides;
    }
    if let Some(status) = mutation.status {
        if status == KeyStatus::Revoked && record.revoked_at_unix_secs.is_none() {
            record.revoked_at_unix_secs = Some(unix_secs(clock.now()));
        }
        record.status = status;
    }
}

fn vec_to_array<const N: usize>(value: Vec<u8>, field: &str) -> StorageResult<[u8; N]> {
    value
        .try_into()
        .map_err(|value: Vec<u8>| StorageError::Corrupted {
            message: format!("{field} length is {}, expected {N}", value.len()),
        })
}

fn u64_to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::Fatal {
        message: format!("{field} cannot be represented as INTEGER"),
    })
}

fn option_u64_to_i64(value: Option<u64>, field: &str) -> StorageResult<Option<i64>> {
    value.map(|value| u64_to_i64(value, field)).transpose()
}

fn i64_to_u64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}

fn parse_key_status(value: &str) -> StorageResult<KeyStatus> {
    match value {
        "active" => Ok(KeyStatus::Active),
        "disabled" => Ok(KeyStatus::Disabled),
        "revoked" => Ok(KeyStatus::Revoked),
        value => Err(corrupted_enum("status", value)),
    }
}

fn corrupted_enum(field: &str, value: &str) -> StorageError {
    StorageError::Corrupted {
        message: format!("invalid {field} value {value}"),
    }
}

fn key_status_as_str(value: KeyStatus) -> &'static str {
    match value {
        KeyStatus::Active => "active",
        KeyStatus::Disabled => "disabled",
        KeyStatus::Revoked => "revoked",
    }
}

fn now_i64(clock: &dyn Clock) -> StorageResult<i64> {
    u64_to_i64(unix_secs(clock.now()), "now_unix_secs")
}

fn map_managed_sqlx_error(error: sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(database_error) = &error
        && database_error.is_unique_violation()
    {
        return StorageError::Conflict {
            message: database_error.to_string(),
        };
    }
    map_sqlx_error(error)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use cc_lb_storage_api::{ManagedKeyStore, types::LimitKind};
    use sqlx::{Connection, Row, SqliteConnection, SqlitePool, sqlite::SqliteConnectOptions};

    use super::*;

    #[tokio::test]
    async fn issue_returns_unavailable_while_paused() {
        let pool = SqlitePool::connect_lazy("sqlite::memory:").expect("lazy sqlite pool");
        let storage = SqliteStorage::new(pool, Arc::new(cc_lb_clock::SystemClock));

        let error = storage
            .issue("principal-a", "key-a", issue_params(7))
            .await
            .expect_err("issuance should be unavailable");
        assert!(matches!(error, StorageError::Unavailable { .. }));
    }

    #[tokio::test]
    async fn migration_drops_kind_columns_and_preserves_managed_key_data() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let database_url = format!(
            "sqlite://{}",
            temp_dir
                .path()
                .join("managed-key-kind-columns.sqlite")
                .display()
        );
        let options = SqliteConnectOptions::from_str(&database_url)
            .expect("sqlite connect options")
            .create_if_missing(true)
            .foreign_keys(true);
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .expect("connect sqlite database");
        let migrator = sqlx::migrate!("./migrations");

        migrator
            .run_direct(None, &mut connection, false)
            .await
            .expect("apply migrations before kind-column drop");
        sqlx::query(
            "INSERT INTO managed_keys_v1 (
                id, name, secret_hash, created_at, expires_at, status, principal_id, key_id,
                label, revoked_at, verify_hash, secret_salt, upstream_kind, limit_overrides,
                last_4, description, principal_kind, index_hash, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("principal-preserved/key-preserved")
        .bind("principal-preserved/key-preserved")
        .bind("preserved-key-hash")
        .bind(1_700_000_000_i64)
        .bind(Some(1_800_000_000_i64))
        .bind("disabled")
        .bind("principal-preserved")
        .bind("key-preserved")
        .bind("preserved label")
        .bind(Some(1_700_000_100_i64))
        .bind(vec![0x11_u8; 32])
        .bind(vec![0x22_u8; 16])
        .bind("anthropic_oauth")
        .bind(r#"[{"kind":"requests","window_secs":60,"cap_micros":17}]"#)
        .bind("1234")
        .bind(Some("preserved description"))
        .bind("human")
        .bind(vec![0x33_u8; 32])
        .bind(1_700_000_200_i64)
        .execute(&mut connection)
        .await
        .expect("seed legacy managed key");

        sqlx::raw_sql(include_str!(
            "../../../../tests/fixtures/sqlite_drop_managed_key_kind_columns.sql"
        ))
        .execute(&mut connection)
        .await
        .expect("apply managed key kind-column migration");

        let columns = sqlx::query("PRAGMA table_info(managed_keys_v1)")
            .fetch_all(&mut connection)
            .await
            .expect("read managed key columns")
            .into_iter()
            .map(|row| row.try_get::<String, _>("name").expect("column name"))
            .collect::<Vec<_>>();
        assert!(!columns.iter().any(|name| name == "upstream_kind"));
        assert!(!columns.iter().any(|name| name == "principal_kind"));

        let row = sqlx::query(
            "SELECT id, name, secret_hash, created_at, expires_at, status, principal_id, key_id,
                    label, revoked_at, verify_hash, secret_salt, limit_overrides, last_4,
                    description, index_hash, updated_at
             FROM managed_keys_v1
             WHERE principal_id = ? AND key_id = ?",
        )
        .bind("principal-preserved")
        .bind("key-preserved")
        .fetch_one(&mut connection)
        .await
        .expect("read preserved managed key");
        assert_eq!(
            row.try_get::<String, _>("id").expect("id"),
            "principal-preserved/key-preserved"
        );
        assert_eq!(
            row.try_get::<String, _>("name").expect("name"),
            "principal-preserved/key-preserved"
        );
        assert_eq!(
            row.try_get::<String, _>("secret_hash")
                .expect("secret_hash"),
            "preserved-key-hash"
        );
        assert_eq!(
            row.try_get::<i64, _>("created_at").expect("created_at"),
            1_700_000_000
        );
        assert_eq!(
            row.try_get::<Option<i64>, _>("expires_at")
                .expect("expires_at"),
            Some(1_800_000_000)
        );
        assert_eq!(
            row.try_get::<String, _>("status").expect("status"),
            "disabled"
        );
        assert_eq!(
            row.try_get::<String, _>("principal_id")
                .expect("principal_id"),
            "principal-preserved"
        );
        assert_eq!(
            row.try_get::<String, _>("key_id").expect("key_id"),
            "key-preserved"
        );
        assert_eq!(
            row.try_get::<String, _>("label").expect("label"),
            "preserved label"
        );
        assert_eq!(
            row.try_get::<Option<i64>, _>("revoked_at")
                .expect("revoked_at"),
            Some(1_700_000_100)
        );
        assert_eq!(
            row.try_get::<Vec<u8>, _>("verify_hash")
                .expect("verify_hash"),
            vec![0x11; 32]
        );
        assert_eq!(
            row.try_get::<Vec<u8>, _>("secret_salt")
                .expect("secret_salt"),
            vec![0x22; 16]
        );
        assert_eq!(
            row.try_get::<String, _>("limit_overrides")
                .expect("limit_overrides"),
            r#"[{"kind":"requests","window_secs":60,"cap_micros":17}]"#
        );
        assert_eq!(row.try_get::<String, _>("last_4").expect("last_4"), "1234");
        assert_eq!(
            row.try_get::<Option<String>, _>("description")
                .expect("description"),
            Some("preserved description".to_owned())
        );
        assert_eq!(
            row.try_get::<Vec<u8>, _>("index_hash").expect("index_hash"),
            vec![0x33; 32]
        );
        assert_eq!(
            row.try_get::<i64, _>("updated_at").expect("updated_at"),
            1_700_000_200
        );

        let indexes = sqlx::query_scalar::<_, String>(
            "SELECT name
             FROM sqlite_master
             WHERE type = 'index' AND tbl_name = 'managed_keys_v1'",
        )
        .fetch_all(&mut connection)
        .await
        .expect("read managed key indexes");
        assert!(
            indexes
                .iter()
                .any(|name| name == "managed_keys_v1_active_index_hash")
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "managed_keys_v1_principal_id")
        );
    }

    fn issue_params(seed: u8) -> IssueParams {
        IssueParams {
            label: format!("label-{seed}"),
            description: Some(format!("description-{seed}")),
            expires_at_unix_secs: Some(1_800_000_000 + u64::from(seed)),
            limit_overrides: vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: i64::from(seed),
            }],
            secret_salt: [seed; 16],
            verify_hash: [seed; 32],
            last_4: format!("{seed:04}"),
            index_hash: [seed.wrapping_add(100); 32],
        }
    }
}
