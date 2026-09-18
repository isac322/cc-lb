use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_clock::{Clock, ClockHandle, unix_secs};
use cc_lb_storage_api::{
    ManagedKeyStore, StorageError, StorageResult,
    types::{ApiKeyMutation, IssueParams, KeyStatus, Limit, StoredApiKeyRecord},
    validate_identifier,
};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{
    adapter::{i64_to_u64, retry, u64_to_i64},
    error_map::map_sqlx_error,
};

#[derive(Clone)]
pub struct PostgresManagedKeyStore {
    pool: PgPool,
    retry_policy: Arc<retry::RetryPolicy>,
    clock: ClockHandle,
}

impl PostgresManagedKeyStore {
    pub fn new(pool: PgPool, retry_policy: Arc<retry::RetryPolicy>, clock: ClockHandle) -> Self {
        Self {
            pool,
            retry_policy,
            clock,
        }
    }
}

#[async_trait]
impl ManagedKeyStore for PostgresManagedKeyStore {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;
        if !params.label.is_empty() {
            validate_identifier("label", &params.label)?;
        }
        if let Some(desc) = &params.description {
            validate_identifier("description", desc)?;
        }

        let record = StoredApiKeyRecord {
            label: params.label,
            issued_at_unix_secs: unix_secs(self.clock.now()),
            revoked_at_unix_secs: None,
            key_hash_b64: base64_url_no_pad(&params.verify_hash),
            verify_hash: params.verify_hash,
            secret_salt: params.secret_salt,
            limit_overrides: params.limit_overrides,
            status: KeyStatus::Active,
            expires_at_unix_secs: params.expires_at_unix_secs,
            last_4: params.last_4,
            description: params.description,
            index_hash: params.index_hash,
        };
        let stored_record = record.clone();

        retry::with_retry(&self.retry_policy, || {
            let pool = self.pool.clone();
            let record = stored_record.clone();
            async move {
                let mut tx = pool.begin().await?;
                insert_record(principal_id, key_id, &record, &mut tx).await?;
                sqlx::query(
                    "INSERT INTO managed_api_key_index_v1 (index_hash, principal_id, key_id) VALUES ($1, $2, $3)",
                )
                .bind(record.index_hash.as_slice())
                .bind(principal_id)
                .bind(key_id)
                .execute(&mut *tx)
                .await?;
                notify_principal_changed(&mut tx, principal_id).await?;
                tx.commit().await?;
                Ok(())
            }
        })
        .await
        .map_err(map_sqlx_error)?;

        Ok(record)
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let row = retry::with_retry(&self.retry_policy, || async {
            sqlx::query(SELECT_BY_KEY_SQL)
                .bind(principal_id)
                .bind(key_id)
                .fetch_optional(&self.pool)
                .await
        })
        .await
        .map_err(map_sqlx_error)?;

        row.map(row_to_record).transpose()
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        validate_identifier("index_hash", "index_hash")?;

        let row = retry::with_retry(&self.retry_policy, || async {
            sqlx::query(
                "SELECT k.principal_id, k.key_id, k.label, k.issued_at_unix_secs, k.revoked_at_unix_secs, \
                 k.key_hash_b64, k.verify_hash, k.secret_salt, k.limit_overrides, k.status, \
                 k.expires_at_unix_secs, k.last_4, k.description, k.index_hash \
                 FROM managed_api_keys_v1 k \
                 INNER JOIN managed_api_key_index_v1 i \
                 ON k.principal_id = i.principal_id AND k.key_id = i.key_id \
                 WHERE i.index_hash = $1",
            )
            .bind(index_hash.as_slice())
            .fetch_optional(&self.pool)
            .await
        })
        .await
        .map_err(map_sqlx_error)?;

        row.map(|row| {
            let principal_id = row
                .try_get::<String, _>("principal_id")
                .map_err(map_sqlx_error)?;
            let key_id = row.try_get::<String, _>("key_id").map_err(map_sqlx_error)?;
            let record = row_to_record(row)?;
            Ok((principal_id, key_id, record))
        })
        .transpose()
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        validate_identifier("principal_id", principal_id)?;

        let rows = retry::with_retry(&self.retry_policy, || async {
            sqlx::query(SELECT_BY_PRINCIPAL_SQL)
                .bind(principal_id)
                .fetch_all(&self.pool)
                .await
        })
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter().map(row_to_record).collect()
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        validate_identifier("list_all", "all")?;

        let rows = retry::with_retry(&self.retry_policy, || async {
            sqlx::query(SELECT_ALL_SQL).fetch_all(&self.pool).await
        })
        .await
        .map_err(map_sqlx_error)?;

        rows.into_iter()
            .map(|row| {
                let principal_id = row
                    .try_get::<String, _>("principal_id")
                    .map_err(map_sqlx_error)?;
                let key_id = row.try_get::<String, _>("key_id").map_err(map_sqlx_error)?;
                let record = row_to_record(row)?;
                Ok((principal_id, key_id, record))
            })
            .collect()
    }

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> StorageResult<()> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let clock = Arc::clone(&self.clock);
        retry::with_retry(&self.retry_policy, || {
            let pool = self.pool.clone();
            let mutation = mutation.clone();
            let clock = Arc::clone(&clock);
            async move {
                let mut tx = pool.begin().await?;
                let Some(row) = sqlx::query(SELECT_BY_KEY_FOR_UPDATE_SQL)
                    .bind(principal_id)
                    .bind(key_id)
                    .fetch_optional(&mut *tx)
                    .await?
                else {
                    tx.commit().await?;
                    return Ok(());
                };
                let mut record = row_to_record_sqlx(row)?;

                apply_mutation(&mut record, mutation, &*clock);
                update_record(principal_id, key_id, &record, &mut tx).await?;
                notify_principal_changed(&mut tx, principal_id).await?;
                tx.commit().await?;
                Ok(())
            }
        })
        .await
        .map_err(map_sqlx_error)
    }

    async fn revoke_zero_secrets(&self, principal_id: &str, key_id: &str) -> StorageResult<()> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let clock = Arc::clone(&self.clock);
        retry::with_retry(&self.retry_policy, || {
            let pool = self.pool.clone();
            let clock = Arc::clone(&clock);
            async move {
                let mut tx = pool.begin().await?;
                let Some(row) = sqlx::query(SELECT_BY_KEY_FOR_UPDATE_SQL)
                    .bind(principal_id)
                    .bind(key_id)
                    .fetch_optional(&mut *tx)
                    .await?
                else {
                    tx.commit().await?;
                    return Ok(());
                };
                let mut record = row_to_record_sqlx(row)?;
                let captured_index_hash = record.index_hash;
                record.status = KeyStatus::Revoked;
                if record.revoked_at_unix_secs.is_none() {
                    record.revoked_at_unix_secs = Some(unix_secs(clock.now()));
                }
                record.index_hash = [0; 32];
                record.verify_hash = [0; 32];
                record.secret_salt = [0; 16];

                update_record(principal_id, key_id, &record, &mut tx).await?;
                sqlx::query("DELETE FROM managed_api_key_index_v1 WHERE index_hash = $1")
                    .bind(captured_index_hash.as_slice())
                    .execute(&mut *tx)
                    .await?;
                notify_principal_changed(&mut tx, principal_id).await?;
                tx.commit().await?;
                Ok(())
            }
        })
        .await
        .map_err(map_sqlx_error)
    }
}

async fn notify_principal_changed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    principal_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_notify('cclb_principal_changed', $1)")
        .bind(principal_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

const SELECT_BY_KEY_SQL: &str = "SELECT principal_id, key_id, label, issued_at_unix_secs, revoked_at_unix_secs, \
     key_hash_b64, verify_hash, secret_salt, limit_overrides, status, expires_at_unix_secs, \
     last_4, description, index_hash FROM managed_api_keys_v1 WHERE principal_id = $1 AND key_id = $2";
const SELECT_BY_KEY_FOR_UPDATE_SQL: &str = "SELECT principal_id, key_id, label, issued_at_unix_secs, revoked_at_unix_secs, \
     key_hash_b64, verify_hash, secret_salt, limit_overrides, status, expires_at_unix_secs, \
     last_4, description, index_hash FROM managed_api_keys_v1 WHERE principal_id = $1 AND key_id = $2 FOR UPDATE";
const SELECT_BY_PRINCIPAL_SQL: &str = "SELECT principal_id, key_id, label, issued_at_unix_secs, revoked_at_unix_secs, \
     key_hash_b64, verify_hash, secret_salt, limit_overrides, status, expires_at_unix_secs, \
     last_4, description, index_hash FROM managed_api_keys_v1 WHERE principal_id = $1 ORDER BY key_id ASC";
const SELECT_ALL_SQL: &str = "SELECT principal_id, key_id, label, issued_at_unix_secs, revoked_at_unix_secs, \
     key_hash_b64, verify_hash, secret_salt, limit_overrides, status, expires_at_unix_secs, \
     last_4, description, index_hash FROM managed_api_keys_v1 ORDER BY principal_id ASC, key_id ASC";

async fn insert_record(
    principal_id: &str,
    key_id: &str,
    record: &StoredApiKeyRecord,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO managed_api_keys_v1 \
         (principal_id, key_id, label, issued_at_unix_secs, revoked_at_unix_secs, key_hash_b64, \
          verify_hash, secret_salt, limit_overrides, status, expires_at_unix_secs, last_4, \
          description, index_hash, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW())",
    )
    .bind(principal_id)
    .bind(key_id)
    .bind(&record.label)
    .bind(u64_to_i64_sqlx(
        record.issued_at_unix_secs,
        "issued_at_unix_secs",
    )?)
    .bind(option_u64_to_i64_sqlx(
        record.revoked_at_unix_secs,
        "revoked_at_unix_secs",
    )?)
    .bind(&record.key_hash_b64)
    .bind(record.verify_hash.as_slice())
    .bind(record.secret_salt.as_slice())
    .bind(
        serde_json::to_value(&record.limit_overrides)
            .map_err(|error| sqlx::Error::Encode(Box::new(error)))?,
    )
    .bind(key_status_as_str(record.status))
    .bind(option_u64_to_i64_sqlx(
        record.expires_at_unix_secs,
        "expires_at_unix_secs",
    )?)
    .bind(&record.last_4)
    .bind(&record.description)
    .bind(record.index_hash.as_slice())
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn update_record(
    principal_id: &str,
    key_id: &str,
    record: &StoredApiKeyRecord,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE managed_api_keys_v1 SET \
         label = $3, revoked_at_unix_secs = $4, key_hash_b64 = $5, verify_hash = $6, \
         secret_salt = $7, limit_overrides = $8, status = $9, expires_at_unix_secs = $10, \
         last_4 = $11, description = $12, index_hash = $13, updated_at = NOW() \
         WHERE principal_id = $1 AND key_id = $2",
    )
    .bind(principal_id)
    .bind(key_id)
    .bind(&record.label)
    .bind(option_u64_to_i64_sqlx(
        record.revoked_at_unix_secs,
        "revoked_at_unix_secs",
    )?)
    .bind(&record.key_hash_b64)
    .bind(record.verify_hash.as_slice())
    .bind(record.secret_salt.as_slice())
    .bind(
        serde_json::to_value(&record.limit_overrides)
            .map_err(|error| sqlx::Error::Encode(Box::new(error)))?,
    )
    .bind(key_status_as_str(record.status))
    .bind(option_u64_to_i64_sqlx(
        record.expires_at_unix_secs,
        "expires_at_unix_secs",
    )?)
    .bind(&record.last_4)
    .bind(&record.description)
    .bind(record.index_hash.as_slice())
    .execute(&mut **tx)
    .await?;

    Ok(())
}

fn row_to_record(row: PgRow) -> StorageResult<StoredApiKeyRecord> {
    row_to_record_inner(row).map_err(map_sqlx_error)
}

fn row_to_record_sqlx(row: PgRow) -> Result<StoredApiKeyRecord, sqlx::Error> {
    row_to_record_inner(row)
}

fn row_to_record_inner(row: PgRow) -> Result<StoredApiKeyRecord, sqlx::Error> {
    let issued_at_unix_secs = row.try_get::<i64, _>("issued_at_unix_secs")?;
    let revoked_at_unix_secs = row.try_get::<Option<i64>, _>("revoked_at_unix_secs")?;
    let expires_at_unix_secs = row.try_get::<Option<i64>, _>("expires_at_unix_secs")?;
    let verify_hash = vec_to_array::<32>(row.try_get("verify_hash")?, "verify_hash")?;
    let secret_salt = vec_to_array::<16>(row.try_get("secret_salt")?, "secret_salt")?;
    let index_hash = vec_to_array::<32>(row.try_get("index_hash")?, "index_hash")?;
    let limit_overrides: Vec<Limit> =
        serde_json::from_value(row.try_get::<Value, _>("limit_overrides")?).map_err(|error| {
            sqlx::Error::ColumnDecode {
                index: "limit_overrides".to_owned(),
                source: Box::new(error),
            }
        })?;

    Ok(StoredApiKeyRecord {
        label: row.try_get("label")?,
        issued_at_unix_secs: u64_from_i64_sqlx(issued_at_unix_secs, "issued_at_unix_secs")?,
        revoked_at_unix_secs: revoked_at_unix_secs
            .map(|value| u64_from_i64_sqlx(value, "revoked_at_unix_secs"))
            .transpose()?,
        key_hash_b64: row.try_get("key_hash_b64")?,
        verify_hash,
        secret_salt,
        limit_overrides,
        status: parse_key_status(&row.try_get::<String, _>("status")?)?,
        expires_at_unix_secs: expires_at_unix_secs
            .map(|value| u64_from_i64_sqlx(value, "expires_at_unix_secs"))
            .transpose()?,
        last_4: row.try_get("last_4")?,
        description: row.try_get("description")?,
        index_hash,
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

fn vec_to_array<const N: usize>(value: Vec<u8>, field: &str) -> Result<[u8; N], sqlx::Error> {
    value
        .try_into()
        .map_err(|value: Vec<u8>| sqlx::Error::ColumnDecode {
            index: field.to_owned(),
            source: Box::new(StorageError::Corrupted {
                message: format!("{field} length is {}, expected {N}", value.len()),
            }),
        })
}

fn u64_to_i64_sqlx(value: u64, field: &str) -> Result<i64, sqlx::Error> {
    u64_to_i64(value, field).map_err(|error| sqlx::Error::Encode(Box::new(error)))
}

fn option_u64_to_i64_sqlx(value: Option<u64>, field: &str) -> Result<Option<i64>, sqlx::Error> {
    value.map(|value| u64_to_i64_sqlx(value, field)).transpose()
}

fn u64_from_i64_sqlx(value: i64, field: &str) -> Result<u64, sqlx::Error> {
    i64_to_u64(value, field).map_err(|error| sqlx::Error::ColumnDecode {
        index: field.to_owned(),
        source: Box::new(error),
    })
}

fn parse_key_status(value: &str) -> Result<KeyStatus, sqlx::Error> {
    match value {
        "active" => Ok(KeyStatus::Active),
        "disabled" => Ok(KeyStatus::Disabled),
        "revoked" => Ok(KeyStatus::Revoked),
        value => Err(corrupted_enum("status", value)),
    }
}

fn corrupted_enum(field: &str, value: &str) -> sqlx::Error {
    sqlx::Error::ColumnDecode {
        index: field.to_owned(),
        source: Box::new(StorageError::Corrupted {
            message: format!("invalid {field} value {value}"),
        }),
    }
}

fn key_status_as_str(value: KeyStatus) -> &'static str {
    match value {
        KeyStatus::Active => "active",
        KeyStatus::Disabled => "disabled",
        KeyStatus::Revoked => "revoked",
    }
}

fn base64_url_no_pad(value: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(b2 & 0b0011_1111) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::{error::Error, str::FromStr, sync::Arc};

    use cc_lb_storage_api::{ManagedKeyStore, types::LimitKind};
    use sqlx::{
        AssertSqlSafe,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::*;

    const LEGACY_MIGRATIONS: &[&str] = &[
        include_str!("../../migrations/0013_managed_api_keys.sql"),
        include_str!("../../migrations/0014_managed_api_key_index.sql"),
        include_str!("../../migrations/0020_drop_per_key_pin.sql"),
    ];
    const DROP_KIND_COLUMNS_MIGRATION: &str =
        include_str!("../../migrations/0117_drop_managed_key_kind_columns.sql");
    const MIGRATIONS: &[&str] = &[
        include_str!("../../migrations/0013_managed_api_keys.sql"),
        include_str!("../../migrations/0014_managed_api_key_index.sql"),
        include_str!("../../migrations/0020_drop_per_key_pin.sql"),
        DROP_KIND_COLUMNS_MIGRATION,
    ];

    type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

    #[tokio::test]
    async fn issue_get_lookup_update_list_and_revoke() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create(&url).await?;
        let store = fixture.store();
        let params = issue_params(11);
        let index_hash = params.index_hash;

        let issued = store.issue("principal-a", "key-a", params.clone()).await?;
        assert_eq!(issued.label, "label-11");
        assert_eq!(issued.status, KeyStatus::Active);
        assert_eq!(issued.verify_hash, params.verify_hash);

        let fetched = store.get("principal-a", "key-a").await?;
        assert_eq!(fetched.as_ref(), Some(&issued));

        let lookup = store
            .lookup_by_index_hash(&index_hash)
            .await?
            .ok_or("lookup should find issued key")?;
        assert_eq!(lookup.0, "principal-a");
        assert_eq!(lookup.1, "key-a");
        assert_eq!(lookup.2, issued);

        let second = store
            .issue("principal-b", "key-b", issue_params(22))
            .await?;
        assert_eq!(store.list_by_principal("principal-a").await?.len(), 1);
        let all = store.list_all().await?;
        assert_eq!(all.len(), 2);
        assert!(all.iter().any(|(_, _, record)| record == &second));

        store
            .update(
                "principal-a",
                "key-a",
                ApiKeyMutation {
                    label: Some("renamed".to_owned()),
                    description: Some(None),
                    expires_at_unix_secs: Some(None),
                    limit_overrides: Some(vec![Limit {
                        kind: LimitKind::OutputTokens,
                        window_secs: 120,
                        cap_micros: 55,
                    }]),
                    status: Some(KeyStatus::Disabled),
                },
            )
            .await?;
        let updated = store
            .lookup_by_index_hash(&index_hash)
            .await?
            .ok_or("disabled key should remain indexed")?
            .2;
        assert_eq!(updated.label, "renamed");
        assert_eq!(updated.description, None);
        assert_eq!(updated.expires_at_unix_secs, None);
        assert_eq!(updated.status, KeyStatus::Disabled);
        assert_eq!(updated.limit_overrides[0].kind, LimitKind::OutputTokens);

        store.revoke_zero_secrets("principal-a", "key-a").await?;
        assert!(store.lookup_by_index_hash(&index_hash).await?.is_none());
        let revoked = store
            .get("principal-a", "key-a")
            .await?
            .ok_or("revoked record should remain stored")?;
        assert_eq!(revoked.status, KeyStatus::Revoked);
        assert!(revoked.revoked_at_unix_secs.is_some());
        assert_eq!(revoked.index_hash, [0; 32]);
        assert_eq!(revoked.verify_hash, [0; 32]);
        assert_eq!(revoked.secret_salt, [0; 16]);
        assert_eq!(revoked.last_4, params.last_4);

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_index_hash_surfaces_conflict() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create(&url).await?;
        let store = fixture.store();
        let params = issue_params(33);
        store.issue("principal-a", "key-a", params.clone()).await?;
        let error = store
            .issue("principal-a", "key-b", params)
            .await
            .err()
            .ok_or("duplicate index hash should fail")?;
        assert!(matches!(error, StorageError::Conflict { .. }));

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_identifier_rejected_before_sql() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create(&url).await?;
        let store = fixture.store();
        let error = store
            .get("", "key-a")
            .await
            .err()
            .ok_or("empty principal should fail")?;
        assert!(matches!(error, StorageError::InvalidInput { .. }));

        fixture.drop_schema().await?;
        Ok(())
    }

    #[tokio::test]
    async fn migration_drops_kind_columns_and_preserves_managed_key_data() -> TestResult<()> {
        let Some(url) = std::env::var("CI_POSTGRES_URL").ok() else {
            eprintln!("skip: CI_POSTGRES_URL not set");
            return Ok(());
        };
        let fixture = Fixture::create_with_migrations(&url, LEGACY_MIGRATIONS).await?;

        sqlx::query(
            "INSERT INTO managed_api_keys_v1 (
                principal_id, key_id, label, issued_at_unix_secs, revoked_at_unix_secs,
                key_hash_b64, verify_hash, secret_salt, upstream_kind, limit_overrides,
                status, expires_at_unix_secs, last_4, description, principal_kind, index_hash,
                created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                to_timestamp(1700000200), to_timestamp(1700000300)
             )",
        )
        .bind("principal-preserved")
        .bind("key-preserved")
        .bind("preserved label")
        .bind(1_700_000_000_i64)
        .bind(Some(1_700_000_100_i64))
        .bind("preserved-key-hash")
        .bind(vec![0x11_u8; 32])
        .bind(vec![0x22_u8; 16])
        .bind("anthropic_oauth")
        .bind(serde_json::json!([{
            "kind": "requests",
            "window_secs": 60,
            "cap_micros": 17
        }]))
        .bind("disabled")
        .bind(Some(1_800_000_000_i64))
        .bind("1234")
        .bind(Some("preserved description"))
        .bind("human")
        .bind(vec![0x33_u8; 32])
        .execute(&fixture.pool)
        .await?;
        sqlx::query(
            "INSERT INTO managed_api_key_index_v1 (index_hash, principal_id, key_id)
             VALUES ($1, $2, $3)",
        )
        .bind(vec![0x33_u8; 32])
        .bind("principal-preserved")
        .bind("key-preserved")
        .execute(&fixture.pool)
        .await?;

        sqlx::raw_sql(DROP_KIND_COLUMNS_MIGRATION)
            .execute(&fixture.pool)
            .await?;

        let dropped_kind_column_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = 'managed_api_keys_v1'
               AND column_name IN ('upstream_kind', 'principal_kind')",
        )
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(dropped_kind_column_count, 0);

        let row = sqlx::query(
            "SELECT label, issued_at_unix_secs, revoked_at_unix_secs, key_hash_b64,
                    verify_hash, secret_salt, limit_overrides, status, expires_at_unix_secs,
                    last_4, description, index_hash,
                    EXTRACT(EPOCH FROM created_at)::BIGINT AS created_at_unix_secs,
                    EXTRACT(EPOCH FROM updated_at)::BIGINT AS updated_at_unix_secs
             FROM managed_api_keys_v1
             WHERE principal_id = $1 AND key_id = $2",
        )
        .bind("principal-preserved")
        .bind("key-preserved")
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(row.try_get::<String, _>("label")?, "preserved label");
        assert_eq!(row.try_get::<i64, _>("issued_at_unix_secs")?, 1_700_000_000);
        assert_eq!(
            row.try_get::<Option<i64>, _>("revoked_at_unix_secs")?,
            Some(1_700_000_100)
        );
        assert_eq!(
            row.try_get::<String, _>("key_hash_b64")?,
            "preserved-key-hash"
        );
        assert_eq!(row.try_get::<Vec<u8>, _>("verify_hash")?, vec![0x11; 32]);
        assert_eq!(row.try_get::<Vec<u8>, _>("secret_salt")?, vec![0x22; 16]);
        assert_eq!(
            row.try_get::<Value, _>("limit_overrides")?,
            serde_json::json!([{
                "kind": "requests",
                "window_secs": 60,
                "cap_micros": 17
            }])
        );
        assert_eq!(row.try_get::<String, _>("status")?, "disabled");
        assert_eq!(
            row.try_get::<Option<i64>, _>("expires_at_unix_secs")?,
            Some(1_800_000_000)
        );
        assert_eq!(row.try_get::<String, _>("last_4")?, "1234");
        assert_eq!(
            row.try_get::<Option<String>, _>("description")?,
            Some("preserved description".to_owned())
        );
        assert_eq!(row.try_get::<Vec<u8>, _>("index_hash")?, vec![0x33; 32]);
        assert_eq!(
            row.try_get::<i64, _>("created_at_unix_secs")?,
            1_700_000_200
        );
        assert_eq!(
            row.try_get::<i64, _>("updated_at_unix_secs")?,
            1_700_000_300
        );

        let indexed_key = sqlx::query_as::<_, (String, String)>(
            "SELECT principal_id, key_id
             FROM managed_api_key_index_v1
             WHERE index_hash = $1",
        )
        .bind(vec![0x33_u8; 32])
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(
            indexed_key,
            ("principal-preserved".to_owned(), "key-preserved".to_owned())
        );

        let indexes = sqlx::query_scalar::<_, String>(
            "SELECT indexname
             FROM pg_indexes
             WHERE schemaname = current_schema()
               AND tablename = 'managed_api_keys_v1'",
        )
        .fetch_all(&fixture.pool)
        .await?;
        assert!(
            indexes
                .iter()
                .any(|name| name == "managed_api_keys_v1_principal_id")
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "managed_api_keys_v1_active_idx")
        );

        fixture.drop_schema().await?;
        Ok(())
    }

    struct Fixture {
        url: String,
        schema: String,
        pool: PgPool,
    }

    impl Fixture {
        async fn create(url: &str) -> TestResult<Self> {
            Self::create_with_migrations(url, MIGRATIONS).await
        }

        async fn create_with_migrations(
            url: &str,
            migrations: &'static [&'static str],
        ) -> TestResult<Self> {
            let schema = format!("test_managed_keys_{}", Uuid::new_v4().simple());
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(PgConnectOptions::from_str(url)?)
                .await?;
            sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
                .execute(&admin_pool)
                .await?;
            admin_pool.close().await;

            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect_with(
                    PgConnectOptions::from_str(url)?.options([("search_path", schema.as_str())]),
                )
                .await?;
            for migration in migrations {
                sqlx::raw_sql(*migration).execute(&pool).await?;
            }

            Ok(Self {
                url: url.to_owned(),
                schema,
                pool,
            })
        }

        fn store(&self) -> PostgresManagedKeyStore {
            PostgresManagedKeyStore::new(
                self.pool.clone(),
                Arc::new(retry::RetryPolicy::default()),
                Arc::new(cc_lb_clock::SystemClock),
            )
        }

        async fn drop_schema(self) -> TestResult<()> {
            self.pool.close().await;
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(PgConnectOptions::from_str(&self.url)?)
                .await?;
            sqlx::query(AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                self.schema
            )))
            .execute(&admin_pool)
            .await?;
            admin_pool.close().await;
            Ok(())
        }
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
