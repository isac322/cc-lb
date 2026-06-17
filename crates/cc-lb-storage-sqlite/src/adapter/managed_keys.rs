use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_storage_api::{
    ManagedKeyStore, StorageError, StorageResult,
    types::{
        ApiKeyMutation, IssueParams, KeyStatus, Limit, PrincipalKindLite, StoredApiKeyRecord,
        UpstreamKind,
    },
    validate_identifier,
};
use sqlx::{Row, sqlite::SqliteRow};

use crate::{SqliteStorage, map_sqlx_error};

#[async_trait]
impl ManagedKeyStore for SqliteStorage {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;
        validate_identifier("label", &params.label)?;
        if let Some(description) = &params.description {
            validate_identifier("description", description)?;
        }

        let record = StoredApiKeyRecord {
            label: params.label,
            issued_at_unix_secs: now_unix_secs(),
            revoked_at_unix_secs: None,
            key_hash_b64: base64_url_no_pad(&params.verify_hash),
            verify_hash: params.verify_hash,
            secret_salt: params.secret_salt,
            upstream_kind: params.upstream_kind,
            limit_overrides: params.limit_overrides,
            status: KeyStatus::Active,
            expires_at_unix_secs: params.expires_at_unix_secs,
            last_4: params.last_4,
            description: params.description,
            principal_kind: params.principal_kind,
            index_hash: params.index_hash,
        };

        insert_record(self, principal_id, key_id, &record).await?;
        Ok(record)
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

        apply_mutation(&mut record, mutation);
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
            record.revoked_at_unix_secs = Some(now_unix_secs());
        }
        record.index_hash = [0; 32];
        record.verify_hash = [0; 32];
        record.secret_salt = [0; 16];

        update_record(self, principal_id, key_id, &record).await
    }
}

const SELECT_BY_KEY_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, upstream_kind, limit_overrides, status, expires_at, last_4, \
     description, principal_kind, index_hash FROM managed_keys_v1 WHERE principal_id = ? AND key_id = ?";
const SELECT_BY_INDEX_HASH_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, upstream_kind, limit_overrides, status, expires_at, last_4, \
     description, principal_kind, index_hash FROM managed_keys_v1 WHERE index_hash = ? AND status != 'revoked'";
const SELECT_BY_PRINCIPAL_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, upstream_kind, limit_overrides, status, expires_at, last_4, \
     description, principal_kind, index_hash FROM managed_keys_v1 WHERE principal_id = ? ORDER BY key_id ASC";
const SELECT_ALL_SQL: &str = "SELECT principal_id, key_id, label, created_at, revoked_at, secret_hash, \
     verify_hash, secret_salt, upstream_kind, limit_overrides, status, expires_at, last_4, \
     description, principal_kind, index_hash FROM managed_keys_v1 ORDER BY principal_id ASC, key_id ASC";

async fn insert_record(
    storage: &SqliteStorage,
    principal_id: &str,
    key_id: &str,
    record: &StoredApiKeyRecord,
) -> StorageResult<()> {
    let id = composite_id(principal_id, key_id);
    let limit_overrides = serde_json::to_string(&record.limit_overrides)?;
    let issued_at = u64_to_i64(record.issued_at_unix_secs, "issued_at_unix_secs")?;
    let expires_at = option_u64_to_i64(record.expires_at_unix_secs, "expires_at_unix_secs")?;
    let now = now_i64()?;

    sqlx::query(
        "INSERT INTO managed_keys_v1 \
         (id, name, secret_hash, created_at, expires_at, status, principal_id, key_id, label, \
          revoked_at, verify_hash, secret_salt, upstream_kind, limit_overrides, last_4, description, \
          principal_kind, index_hash, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(&id)
    .bind(&record.key_hash_b64)
    .bind(issued_at)
    .bind(expires_at)
    .bind(key_status_as_str(record.status))
    .bind(principal_id)
    .bind(key_id)
    .bind(&record.label)
    .bind(option_u64_to_i64(
        record.revoked_at_unix_secs,
        "revoked_at_unix_secs",
    )?)
    .bind(record.verify_hash.as_slice())
    .bind(record.secret_salt.as_slice())
    .bind(upstream_kind_as_str(record.upstream_kind))
    .bind(limit_overrides)
    .bind(&record.last_4)
    .bind(&record.description)
    .bind(principal_kind_as_str(record.principal_kind))
    .bind(record.index_hash.as_slice())
    .bind(now)
    .execute(storage.pool())
    .await
    .map_err(map_managed_sqlx_error)?;

    Ok(())
}

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
         secret_salt = ?, upstream_kind = ?, limit_overrides = ?, last_4 = ?, description = ?, \
         principal_kind = ?, index_hash = ?, updated_at = ? \
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
    .bind(upstream_kind_as_str(record.upstream_kind))
    .bind(limit_overrides)
    .bind(&record.last_4)
    .bind(&record.description)
    .bind(principal_kind_as_str(record.principal_kind))
    .bind(record.index_hash.as_slice())
    .bind(now_i64()?)
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
        upstream_kind: parse_upstream_kind(
            &row.try_get::<String, _>("upstream_kind")
                .map_err(map_sqlx_error)?,
        )?,
        limit_overrides,
        status: parse_key_status(&row.try_get::<String, _>("status").map_err(map_sqlx_error)?)?,
        expires_at_unix_secs: expires_at_unix_secs
            .map(|value| i64_to_u64(value, "expires_at_unix_secs"))
            .transpose()?,
        last_4: row.try_get("last_4").map_err(map_sqlx_error)?,
        description: row.try_get("description").map_err(map_sqlx_error)?,
        principal_kind: parse_principal_kind(
            &row.try_get::<String, _>("principal_kind")
                .map_err(map_sqlx_error)?,
        )?,
        index_hash: vec_to_array(
            row.try_get("index_hash").map_err(map_sqlx_error)?,
            "index_hash",
        )?,
    })
}

fn apply_mutation(record: &mut StoredApiKeyRecord, mutation: ApiKeyMutation) {
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
            record.revoked_at_unix_secs = Some(now_unix_secs());
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

fn parse_upstream_kind(value: &str) -> StorageResult<UpstreamKind> {
    match value {
        "anthropic_key" => Ok(UpstreamKind::AnthropicKey),
        "anthropic_oauth" => Ok(UpstreamKind::AnthropicOAuth),
        value => Err(corrupted_enum("upstream_kind", value)),
    }
}

fn parse_key_status(value: &str) -> StorageResult<KeyStatus> {
    match value {
        "active" => Ok(KeyStatus::Active),
        "disabled" => Ok(KeyStatus::Disabled),
        "revoked" => Ok(KeyStatus::Revoked),
        value => Err(corrupted_enum("status", value)),
    }
}

fn parse_principal_kind(value: &str) -> StorageResult<PrincipalKindLite> {
    match value {
        "human" => Ok(PrincipalKindLite::Human),
        "machine" => Ok(PrincipalKindLite::Machine),
        value => Err(corrupted_enum("principal_kind", value)),
    }
}

fn corrupted_enum(field: &str, value: &str) -> StorageError {
    StorageError::Corrupted {
        message: format!("invalid {field} value {value}"),
    }
}

fn upstream_kind_as_str(value: UpstreamKind) -> &'static str {
    match value {
        UpstreamKind::AnthropicKey => "anthropic_key",
        UpstreamKind::AnthropicOAuth => "anthropic_oauth",
    }
}

fn key_status_as_str(value: KeyStatus) -> &'static str {
    match value {
        KeyStatus::Active => "active",
        KeyStatus::Disabled => "disabled",
        KeyStatus::Revoked => "revoked",
    }
}

fn principal_kind_as_str(value: PrincipalKindLite) -> &'static str {
    match value {
        PrincipalKindLite::Human => "human",
        PrincipalKindLite::Machine => "machine",
    }
}

fn composite_id(principal_id: &str, key_id: &str) -> String {
    format!("{principal_id}/{key_id}")
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
            if chunk.len() > 2 {
                out.push(ALPHABET[(b2 & 0b0011_1111) as usize] as char);
            }
        }
    }
    out
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn now_i64() -> StorageResult<i64> {
    u64_to_i64(now_unix_secs(), "now_unix_secs")
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
