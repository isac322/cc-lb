use crate::SqliteStorage;
use async_trait::async_trait;
use cc_lb_clock::unix_secs;
use cc_lb_storage_api::{
    KeyStatus, StorageError, StoredApiKeyRecord, account_owner::OwnerBindingExpectation,
    codex_sessions::*,
};
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};

type Result<T, E = SessionStoreError> = std::result::Result<T, E>;

fn map_sqlx_error(error: sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(database) = &error
        && database.is_unique_violation()
    {
        return StorageError::Conflict {
            message: "session identifier or verifier already exists".into(),
        };
    }
    crate::map_sqlx_error(error)
}

fn integer(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| SessionStoreError::InvalidOperation)
}

fn number(row: &SqliteRow, name: &str) -> Result<u64> {
    let value: i64 = row.try_get(name).map_err(map_sqlx_error)?;
    u64::try_from(value).map_err(|_| {
        StorageError::Corrupted {
            message: format!("negative session {name}"),
        }
        .into()
    })
}

fn optional_number(row: &SqliteRow, name: &str) -> Result<Option<u64>> {
    let value: Option<i64> = row.try_get(name).map_err(map_sqlx_error)?;
    value
        .map(|v| {
            u64::try_from(v).map_err(|_| {
                StorageError::Corrupted {
                    message: format!("negative session {name}"),
                }
                .into()
            })
        })
        .transpose()
}

fn bytes<const N: usize>(row: &SqliteRow, name: &str) -> Result<[u8; N]> {
    let value: Vec<u8> = row.try_get(name).map_err(map_sqlx_error)?;
    value.try_into().map_err(|_| {
        StorageError::Corrupted {
            message: format!("invalid session {name} length"),
        }
        .into()
    })
}

pub(crate) fn family_row(row: SqliteRow) -> Result<SessionFamily> {
    macro_rules! text {
        ($name:literal) => {
            row.try_get($name).map_err(map_sqlx_error)?
        };
    }
    Ok(SessionFamily {
        family_id: text!("family_id"),
        principal_id: text!("principal_id"),
        parent_key_id: text!("parent_key_id"),
        owner_binding_id: text!("owner_binding_id"),
        provider: text!("provider"),
        owner_generation: number(&row, "owner_generation")?,
        binding_revision: number(&row, "binding_revision")?,
        workspace_id: text!("workspace_id"),
        workspace_generation: number(&row, "workspace_generation")?,
        access_generation: number(&row, "access_generation")?,
        local_user_id: text!("local_user_id"),
        plan_type: text!("plan_type"),
        origin: text!("origin"),
        access_ttl_secs: number(&row, "access_ttl_secs")?,
        created_at: number(&row, "created_at")?,
        expires_at: number(&row, "expires_at")?,
        revoked_at: optional_number(&row, "revoked_at")?,
    })
}

fn credential_row(row: SqliteRow) -> Result<SessionCredential> {
    let purpose: String = row.try_get("purpose").map_err(map_sqlx_error)?;
    Ok(SessionCredential {
        credential_id: row.try_get("credential_id").map_err(map_sqlx_error)?,
        family_id: row.try_get("family_id").map_err(map_sqlx_error)?,
        purpose: match purpose.as_str() {
            "access" => CredentialPurpose::SessionAccess,
            "renewal" => CredentialPurpose::SessionRenewal,
            _ => {
                return Err(StorageError::Corrupted {
                    message: "invalid session purpose".into(),
                }
                .into());
            }
        },
        index_hash: bytes(&row, "index_hash")?,
        verify_hash: bytes(&row, "verify_hash")?,
        salt: bytes(&row, "salt")?,
        workspace_generation: number(&row, "workspace_generation")?,
        access_generation: number(&row, "access_generation")?,
        issued_at: number(&row, "issued_at")?,
        expires_at: number(&row, "expires_at")?,
        revoked_at: optional_number(&row, "revoked_at")?,
    })
}

fn parent_row(row: SqliteRow) -> Result<StoredApiKeyRecord> {
    let status: String = row.try_get("status").map_err(map_sqlx_error)?;
    Ok(StoredApiKeyRecord {
        label: row.try_get("label").map_err(map_sqlx_error)?,
        issued_at_unix_secs: number(&row, "created_at")?,
        revoked_at_unix_secs: optional_number(&row, "revoked_at")?,
        key_hash_b64: row.try_get("secret_hash").map_err(map_sqlx_error)?,
        verify_hash: bytes(&row, "verify_hash")?,
        secret_salt: bytes(&row, "secret_salt")?,
        limit_overrides: serde_json::from_str(
            &row.try_get::<String, _>("limit_overrides")
                .map_err(map_sqlx_error)?,
        )
        .map_err(StorageError::from)?,
        status: match status.as_str() {
            "active" => KeyStatus::Active,
            "revoked" => KeyStatus::Revoked,
            _ => {
                return Err(StorageError::Corrupted {
                    message: "invalid parent key status".into(),
                }
                .into());
            }
        },
        expires_at_unix_secs: optional_number(&row, "expires_at")?,
        last_4: row.try_get("last_4").map_err(map_sqlx_error)?,
        description: row.try_get("description").map_err(map_sqlx_error)?,
        index_hash: bytes(&row, "index_hash")?,
    })
}

async fn family(tx: &mut Transaction<'_, Sqlite>, id: &str) -> Result<SessionFamily> {
    let row = sqlx::query("SELECT * FROM codex_session_families_v1 WHERE family_id = ?")
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or(SessionStoreError::CredentialRejected)?;
    family_row(row)
}

async fn credential(tx: &mut Transaction<'_, Sqlite>, id: &str) -> Result<SessionCredential> {
    let row = sqlx::query("SELECT * FROM codex_session_credentials_v1 WHERE credential_id = ?")
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or(SessionStoreError::CredentialRejected)?;
    credential_row(row)
}

async fn authority(
    tx: &mut Transaction<'_, Sqlite>,
    f: &SessionFamily,
    workspace: &str,
    now: u64,
) -> Result<()> {
    let binding =
        super::account_owner::validate_owner_binding_in_tx(tx, &owner_expectation(f, workspace))
            .await
            .map_err(owner_error)?;
    if binding.local_user_id != f.local_user_id {
        return Err(SessionStoreError::OwnerConflict);
    }
    principal_and_parent(tx, f, now).await
}

fn owner_error(error: StorageError) -> SessionStoreError {
    match error {
        StorageError::Conflict { .. } => SessionStoreError::OwnerConflict,
        other => SessionStoreError::Storage(other),
    }
}

fn owner_expectation(f: &SessionFamily, workspace: &str) -> OwnerBindingExpectation {
    OwnerBindingExpectation {
        binding_id: f.owner_binding_id.clone(),
        principal_id: f.principal_id.clone(),
        provider: f.provider.clone(),
        owner_generation: f.owner_generation,
        binding_revision: f.binding_revision,
        workspace_id: workspace.into(),
    }
}

async fn principal_and_parent(
    tx: &mut Transaction<'_, Sqlite>,
    f: &SessionFamily,
    now: u64,
) -> Result<()> {
    let principal = sqlx::query("SELECT enabled, deleted_at FROM principals_v1 WHERE id = ?")
        .bind(&f.principal_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or(SessionStoreError::CredentialRejected)?;
    if principal
        .try_get::<Option<i64>, _>("deleted_at")
        .map_err(map_sqlx_error)?
        .is_some()
    {
        return Err(SessionStoreError::CredentialRejected);
    }
    if !principal
        .try_get::<bool, _>("enabled")
        .map_err(map_sqlx_error)?
    {
        return Err(SessionStoreError::PrincipalDisabled);
    }
    let parent = sqlx::query("SELECT * FROM managed_keys_v1 WHERE principal_id = ? AND key_id = ?")
        .bind(&f.principal_id)
        .bind(&f.parent_key_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or(SessionStoreError::CredentialRejected)?;
    let parent = parent_row(parent)?;
    // Preserve the existing principal-key expiry boundary.
    if parent.status != KeyStatus::Active
        || parent.revoked_at_unix_secs.is_some()
        || parent
            .expires_at_unix_secs
            .is_some_and(|expiry| now > expiry)
    {
        return Err(SessionStoreError::CredentialRejected);
    }
    Ok(())
}

fn validate_credential(
    c: &SessionCredential,
    f: &SessionFamily,
    purpose: CredentialPurpose,
    now: u64,
) -> Result<()> {
    if c.family_id != f.family_id
        || c.purpose != purpose
        || c.revoked_at.is_some()
        || f.revoked_at.is_some()
        || now < c.issued_at
        || now < f.created_at
        || now >= c.expires_at
        || now >= f.expires_at
        || (purpose == CredentialPurpose::SessionAccess
            && c.workspace_generation != f.workspace_generation)
    {
        return Err(SessionStoreError::CredentialRejected);
    }
    Ok(())
}

fn validate_candidate(c: &SessionCredential, f: &SessionFamily, now: u64) -> Result<()> {
    let expiry = now
        .checked_add(f.access_ttl_secs)
        .ok_or(SessionStoreError::InvalidOperation)?
        .min(f.expires_at);
    if c.credential_id.is_empty()
        || c.family_id != f.family_id
        || c.purpose != CredentialPurpose::SessionAccess
        || c.revoked_at.is_some()
        || c.issued_at != now
        || c.expires_at != expiry
        || expiry <= now
        || c.workspace_generation != f.workspace_generation
        || c.access_generation != f.access_generation
    {
        return Err(SessionStoreError::InvalidOperation);
    }
    Ok(())
}

async fn insert_credential(tx: &mut Transaction<'_, Sqlite>, c: &SessionCredential) -> Result<()> {
    let purpose = match c.purpose {
        CredentialPurpose::SessionAccess => "access",
        CredentialPurpose::SessionRenewal => "renewal",
        _ => return Err(SessionStoreError::InvalidOperation),
    };
    sqlx::query("INSERT INTO codex_session_credentials_v1 (credential_id, family_id, purpose, index_hash, verify_hash, salt, workspace_generation, access_generation, issued_at, expires_at, revoked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)")
        .bind(&c.credential_id).bind(&c.family_id).bind(purpose).bind(c.index_hash.as_slice())
        .bind(c.verify_hash.as_slice()).bind(c.salt.as_slice()).bind(integer(c.workspace_generation)?)
        .bind(integer(c.access_generation)?).bind(integer(c.issued_at)?).bind(integer(c.expires_at)?)
        .execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

#[async_trait]
impl CodexSessionStore for SqliteStorage {
    async fn get_session_family(&self, family_id: &str) -> Result<Option<SessionFamily>> {
        sqlx::query("SELECT * FROM codex_session_families_v1 WHERE family_id = ?")
            .bind(family_id)
            .fetch_optional(self.pool())
            .await
            .map_err(map_sqlx_error)?
            .map(family_row)
            .transpose()
    }

    async fn lookup_session_credential(
        &self,
        index_hash: &[u8; 32],
    ) -> Result<Option<SessionAuthRecord>> {
        // One read transaction provides a consistent lineage snapshot without owner discovery.
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let Some(row) =
            sqlx::query("SELECT * FROM codex_session_credentials_v1 WHERE index_hash = ?")
                .bind(index_hash.as_slice())
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?
        else {
            return Ok(None);
        };
        let credential = credential_row(row)?;
        let family = family(&mut tx, &credential.family_id).await?;
        let Some(row) =
            sqlx::query("SELECT * FROM managed_keys_v1 WHERE principal_id = ? AND key_id = ?")
                .bind(&family.principal_id)
                .bind(&family.parent_key_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_sqlx_error)?
        else {
            return Ok(None);
        };
        let parent_key = parent_row(row)?;
        let Some(row) = sqlx::query("SELECT enabled, deleted_at FROM principals_v1 WHERE id = ?")
            .bind(&family.principal_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
        else {
            return Ok(None);
        };
        if row
            .try_get::<Option<i64>, _>("deleted_at")
            .map_err(map_sqlx_error)?
            .is_some()
        {
            return Ok(None);
        }
        let principal_enabled = row.try_get::<bool, _>("enabled").map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(Some(SessionAuthRecord {
            credential,
            family,
            parent_key,
            principal_enabled,
        }))
    }

    async fn issue_session(&self, command: IssueSession) -> Result<()> {
        let mut tx = self.begin_immediate().await?;
        let now = unix_secs(self.clock().now());
        let f = &command.family;
        let origin = url::Url::parse(&f.origin).map_err(|_| SessionStoreError::InvalidOperation)?;
        if origin.scheme() != "https"
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err(SessionStoreError::InvalidOperation);
        }
        if [
            &f.family_id,
            &f.principal_id,
            &f.parent_key_id,
            &f.owner_binding_id,
            &f.provider,
            &f.workspace_id,
            &f.local_user_id,
            &f.plan_type,
            &f.origin,
        ]
        .iter()
        .any(|s| s.is_empty())
            || f.created_at > now
            || f.revoked_at.is_some()
            || f.expires_at <= now
            || f.access_ttl_secs == 0
            || f.access_ttl_secs > f.expires_at - f.created_at
            || f.workspace_generation != 1
            || f.access_generation != 1
            || f.owner_generation == 0
            || f.binding_revision == 0
        {
            return Err(SessionStoreError::InvalidOperation);
        }
        validate_candidate(&command.access, f, f.created_at)?;
        if now >= command.access.expires_at {
            return Err(SessionStoreError::InvalidOperation);
        }
        let r = &command.renewal;
        if r.credential_id.is_empty()
            || r.family_id != f.family_id
            || r.purpose != CredentialPurpose::SessionRenewal
            || r.revoked_at.is_some()
            || r.issued_at != f.created_at
            || r.expires_at != f.expires_at
            || now >= r.expires_at
            || r.workspace_generation != 1
            || r.access_generation != 1
        {
            return Err(SessionStoreError::InvalidOperation);
        }
        authority(&mut tx, f, &f.workspace_id, now).await?;
        sqlx::query("INSERT INTO codex_session_families_v1 (family_id, principal_id, parent_key_id, owner_binding_id, provider, owner_generation, binding_revision, workspace_id, workspace_generation, access_generation, local_user_id, plan_type, origin, access_ttl_secs, created_at, expires_at, revoked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)")
            .bind(&f.family_id).bind(&f.principal_id).bind(&f.parent_key_id).bind(&f.owner_binding_id)
            .bind(&f.provider).bind(integer(f.owner_generation)?).bind(integer(f.binding_revision)?)
            .bind(&f.workspace_id).bind(integer(f.workspace_generation)?).bind(integer(f.access_generation)?)
            .bind(&f.local_user_id).bind(&f.plan_type).bind(&f.origin).bind(integer(f.access_ttl_secs)?)
            .bind(integer(f.created_at)?).bind(integer(f.expires_at)?)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        insert_credential(&mut tx, &command.access).await?;
        insert_credential(&mut tx, r).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn replace_session_access(
        &self,
        command: ReplaceSessionAccess,
        now: u64,
    ) -> Result<SessionFamily> {
        let mut tx = self.begin_immediate().await?;
        let expected = &command.expected;
        if let Some(target) = &command.target_owner {
            let (source, target_binding) = super::account_owner::validate_owner_bindings_in_tx(
                &mut tx,
                &owner_expectation(expected, &expected.workspace_id),
                Some(target),
            )
            .await
            .map_err(owner_error)?;
            let target_binding = target_binding.ok_or(SessionStoreError::OwnerConflict)?;
            if source.local_user_id != expected.local_user_id
                || target_binding.local_user_id != expected.local_user_id
                || target.principal_id != expected.principal_id
                || target.provider != expected.provider
            {
                return Err(SessionStoreError::OwnerConflict);
            }
            principal_and_parent(&mut tx, expected, now).await?;
        } else {
            authority(&mut tx, expected, &expected.workspace_id, now).await?;
        }
        let mut f = family(&mut tx, &expected.family_id).await?;
        let c = credential(&mut tx, &command.authorizing_credential_id).await?;
        let switching = command.target_owner.is_some();
        validate_credential(
            &c,
            &f,
            if switching {
                CredentialPurpose::SessionAccess
            } else {
                CredentialPurpose::SessionRenewal
            },
            now,
        )?;
        if &f != expected {
            return Err(SessionStoreError::OwnerConflict);
        }
        if let Some(target) = &command.target_owner {
            f.owner_binding_id = target.binding_id.clone();
            f.provider = target.provider.clone();
            f.owner_generation = target.owner_generation;
            f.binding_revision = target.binding_revision;
            f.workspace_id = target.workspace_id.clone();
            f.plan_type = "unknown".into();
            f.workspace_generation = f
                .workspace_generation
                .checked_add(1)
                .ok_or(SessionStoreError::InvalidOperation)?;
        }
        f.access_generation = f
            .access_generation
            .checked_add(1)
            .ok_or(SessionStoreError::InvalidOperation)?;
        validate_candidate(&command.access, &f, now)?;
        sqlx::query("UPDATE codex_session_families_v1 SET workspace_id = ?, workspace_generation = ?, access_generation = ?, owner_binding_id = ?, provider = ?, owner_generation = ?, binding_revision = ?, plan_type = ? WHERE family_id = ?")
            .bind(&f.workspace_id).bind(integer(f.workspace_generation)?).bind(integer(f.access_generation)?)
            .bind(&f.owner_binding_id).bind(&f.provider).bind(integer(f.owner_generation)?)
            .bind(integer(f.binding_revision)?).bind(&f.plan_type).bind(&f.family_id)
            .execute(&mut *tx).await.map_err(map_sqlx_error)?;
        if switching {
            sqlx::query("UPDATE codex_session_credentials_v1 SET revoked_at = ?, verify_hash = zeroblob(32), salt = zeroblob(16) WHERE family_id = ? AND purpose = 'access' AND revoked_at IS NULL")
                .bind(integer(now)?).bind(&f.family_id).execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }
        insert_credential(&mut tx, &command.access).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(f)
    }

    async fn revoke_session_family(
        &self,
        family_id: &str,
        renewal_credential_id: &str,
        now: u64,
    ) -> Result<()> {
        let mut tx = self.begin_immediate().await?;
        // Revocation only contracts authority. A live renewal proof is checked by
        // the public endpoint; internal issuance compensation must still work
        // after owner, principal, parent or credential expiry has changed.
        let f = family(&mut tx, family_id).await?;
        let c = credential(&mut tx, renewal_credential_id).await?;
        if c.family_id != f.family_id
            || c.purpose != CredentialPurpose::SessionRenewal
            || c.revoked_at.is_some()
            || f.revoked_at.is_some()
        {
            return Err(SessionStoreError::CredentialRejected);
        }
        sqlx::query("UPDATE codex_session_families_v1 SET revoked_at = ? WHERE family_id = ?")
            .bind(integer(now)?)
            .bind(family_id)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("UPDATE codex_session_credentials_v1 SET revoked_at = COALESCE(revoked_at, ?), verify_hash = zeroblob(32), salt = zeroblob(16) WHERE family_id = ?")
            .bind(integer(now)?).bind(family_id).execute(&mut *tx).await.map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }
    async fn list_session_workspace_aliases(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        now: u64,
    ) -> Result<Vec<String>> {
        // This endpoint is discovery only: one deferred read transaction keeps
        // credential, lineage, owner binding and upstream health consistent.
        if snapshot.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.pool().begin().await.map_err(map_sqlx_error)?;
        let f = family(&mut tx, &snapshot.family_id).await?;
        if f.principal_id != principal_id
            || snapshot.parent_key_id != f.parent_key_id
            || snapshot.owner_binding_id != f.owner_binding_id
            || snapshot.provider != f.provider
            || snapshot.owner_generation != f.owner_generation
            || snapshot.binding_revision != f.binding_revision
            || snapshot.workspace_id != f.workspace_id
            || snapshot.workspace_generation != f.workspace_generation
            || snapshot.origin != f.origin
        {
            return Err(SessionStoreError::OwnerConflict);
        }
        let c = credential(&mut tx, &snapshot.credential_id).await?;
        validate_credential(&c, &f, CredentialPurpose::SessionAccess, now)?;
        if snapshot.access_generation != c.access_generation
            || snapshot.access_generation != f.access_generation
        {
            return Err(SessionStoreError::CredentialRejected);
        }
        principal_and_parent(&mut tx, &f, now).await?;

        let source = sqlx::query(
            "SELECT b.principal_id, b.provider, b.owner_generation, b.revision,
                    b.local_user_id, b.revoked_at_unix_secs,
                    s.kind, s.enabled, s.deleted_at, st.last_apply_error
             FROM account_owner_bindings_v1 b
             JOIN upstream_spec_v1 s ON s.id = b.upstream_id
             LEFT JOIN upstream_status_v1 st ON st.upstream_id = s.id
             WHERE b.binding_id = ?",
        )
        .bind(&f.owner_binding_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_error)?
        .ok_or_else(|| StorageError::Conflict {
            message: "source owner binding does not exist".into(),
        })?;
        let binding_principal: String = source.try_get("principal_id").map_err(map_sqlx_error)?;
        let provider: String = source.try_get("provider").map_err(map_sqlx_error)?;
        let owner_generation: i64 = source.try_get("owner_generation").map_err(map_sqlx_error)?;
        let revision: i64 = source.try_get("revision").map_err(map_sqlx_error)?;
        let revoked: Option<i64> = source
            .try_get("revoked_at_unix_secs")
            .map_err(map_sqlx_error)?;
        let kind: String = source.try_get("kind").map_err(map_sqlx_error)?;
        let enabled: i64 = source.try_get("enabled").map_err(map_sqlx_error)?;
        let deleted: Option<i64> = source.try_get("deleted_at").map_err(map_sqlx_error)?;
        let unhealthy: Option<String> =
            source.try_get("last_apply_error").map_err(map_sqlx_error)?;
        let expected_api_key = format!("{}_api_key", f.provider);
        let expected_oauth = format!("{}_oauth", f.provider);
        if binding_principal != principal_id
            || provider != f.provider
            || owner_generation != integer(f.owner_generation)?
            || revision != integer(f.binding_revision)?
            || revoked.is_some()
            || (kind != expected_api_key && kind != expected_oauth)
        {
            return Err(SessionStoreError::OwnerConflict);
        }
        if enabled == 0 || deleted.is_some() || unhealthy.is_some() {
            return Err(StorageError::Unavailable {
                message: "source owner upstream is unavailable".into(),
            }
            .into());
        }
        let local_user_id: String = source.try_get("local_user_id").map_err(map_sqlx_error)?;
        let aliases = super::account_owner::list_owner_workspace_aliases_in_read_tx(
            &mut tx,
            principal_id,
            &f.provider,
            &local_user_id,
        )
        .await
        .map_err(|error| match error {
            StorageError::Conflict { .. } => SessionStoreError::OwnerConflict,
            StorageError::Unavailable { .. } => SessionStoreError::Storage(error),
            other => SessionStoreError::Storage(other),
        })?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(aliases)
    }
    async fn fence_session_dispatch(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        now: u64,
    ) -> Result<()> {
        let mut tx = self.begin_immediate().await?;
        if snapshot.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionStoreError::CredentialRejected);
        }
        let f = validate_session_family_in_tx(&mut tx, principal_id, snapshot, now).await?;
        let c = credential(&mut tx, &snapshot.credential_id).await?;
        validate_credential(&c, &f, CredentialPurpose::SessionAccess, now)?;
        if snapshot.access_generation != c.access_generation {
            return Err(SessionStoreError::CredentialRejected);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }
}

/// Shared family fence for independently verified access or relay credentials.
/// The caller must hold an immediate transaction and validate its own credential row.
pub(crate) async fn validate_session_family_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    principal_id: &str,
    snapshot: &SessionSnapshot,
    now: u64,
) -> SessionStoreResult<SessionFamily> {
    let f = family(tx, &snapshot.family_id).await?;
    if f.principal_id != principal_id
        || f.revoked_at.is_some()
        || now < f.created_at
        || now >= f.expires_at
    {
        return Err(SessionStoreError::CredentialRejected);
    }
    authority(tx, &f, &f.workspace_id, now).await?;
    // Lineage discovery precedes owner lookup; this is the family validation
    // point after owner, principal and parent under the immediate write fence.
    let f = family(tx, &snapshot.family_id).await?;
    if snapshot.parent_key_id != f.parent_key_id
        || snapshot.owner_binding_id != f.owner_binding_id
        || snapshot.provider != f.provider
        || snapshot.owner_generation != f.owner_generation
        || snapshot.binding_revision != f.binding_revision
        || snapshot.workspace_id != f.workspace_id
        || snapshot.workspace_generation != f.workspace_generation
        || snapshot.origin != f.origin
    {
        return Err(SessionStoreError::OwnerConflict);
    }
    Ok(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_storage_api::{AccountOwnerBinding, AccountOwnerStore};

    async fn store() -> SqliteStorage {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("memory sqlite");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("real migrations including F-OWNER 0127");
        let storage = SqliteStorage::new(
            pool,
            std::sync::Arc::new(cc_lb_clock::TestClock::new_at_secs(100)),
        );
        for statement in [
            "INSERT INTO principals_v1 (id,name,created_at,updated_at) VALUES ('principal','principal',100,100)",
            "INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000001','owner','openai_oauth',100,100)",
            "INSERT INTO account_owner_bindings_v1 (binding_id,principal_id,provider,owner_id,upstream_id,owner_generation,local_user_id,allowed_workspaces_json,provider_workspace_id,revision) VALUES ('binding','principal','openai','owner','00000000-0000-0000-0000-000000000001',1,'user','[\"workspace\",\"other\"]','provider-workspace',1)",
            "INSERT INTO managed_keys_v1 (secret_hash,created_at,status,principal_id,key_id,label,verify_hash,secret_salt,limit_overrides,last_4,index_hash,updated_at) VALUES ('parent-hash',100,'active','principal','parent','parent-label',zeroblob(32),zeroblob(16),'[]','1234',zeroblob(32),100)",
        ] {
            sqlx::query(statement)
                .execute(storage.pool())
                .await
                .expect("fixture");
        }
        storage
    }

    #[tokio::test]
    async fn switch_discards_source_plan_and_renewal_keeps_unknown() {
        let s = store().await;
        let mut command = issue();
        command.family.plan_type = "plus".into();
        let source = command.family.clone();
        s.issue_session(command).await.unwrap();
        let switched = s
            .replace_session_access(
                replacement(source, "switch-plan", 5, 101, Some("other")),
                101,
            )
            .await
            .unwrap();
        assert_eq!(switched.plan_type, "unknown");
        assert_eq!(
            s.get_session_family("family")
                .await
                .unwrap()
                .unwrap()
                .plan_type,
            "unknown"
        );
        let renewed = s
            .replace_session_access(replacement(switched, "renew-plan", 6, 102, None), 102)
            .await
            .unwrap();
        assert_eq!(renewed.plan_type, "unknown");
    }

    #[tokio::test]
    async fn cross_binding_switch_is_atomic_and_preserves_local_identity() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        for statement in [
            "INSERT INTO upstream_spec_v1 (id,name,kind,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000002','target','openai_oauth',100,100)",
            "INSERT INTO account_owner_bindings_v1 (binding_id,principal_id,provider,owner_id,upstream_id,owner_generation,local_user_id,allowed_workspaces_json,provider_workspace_id,revision) VALUES ('target','principal','openai','target-owner','00000000-0000-0000-0000-000000000002',3,'user','[\"target-workspace\"]','target-provider-workspace',4)",
        ] {
            sqlx::query(statement).execute(s.pool()).await.unwrap();
        }
        let mut command = replacement(family(), "target-access", 5, 101, Some("target-workspace"));
        let target = command.target_owner.as_mut().unwrap();
        target.binding_id = "target".into();
        target.owner_generation = 3;
        target.binding_revision = 4;
        let switched = s.replace_session_access(command, 101).await.unwrap();
        assert_eq!(switched.owner_binding_id, "target");
        assert_eq!(switched.owner_generation, 3);
        assert_eq!(switched.binding_revision, 4);
        assert_eq!(switched.local_user_id, "user");
        assert_eq!(switched.workspace_generation, 2);
        assert!(
            s.fence_session_dispatch("principal", &snapshot(&family(), "credential", 1), 102)
                .await
                .is_err()
        );
        s.fence_session_dispatch("principal", &snapshot(&switched, "target-access", 2), 102)
            .await
            .unwrap();
        s.replace_session_access(replacement(switched, "renew-target", 6, 102, None), 102)
            .await
            .expect("same renewal follows target binding");
    }

    #[tokio::test]
    async fn lookup_distinguishes_disabled_principal_from_deleted_principal() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        sqlx::query("UPDATE principals_v1 SET enabled=0")
            .execute(s.pool())
            .await
            .unwrap();
        let disabled = s
            .lookup_session_credential(&[1; 32])
            .await
            .unwrap()
            .expect("live disabled principal remains classifiable");
        assert!(!disabled.principal_enabled);
        assert!(matches!(
            s.fence_session_dispatch("principal", &snapshot(&family(), "credential", 1), 101)
                .await,
            Err(SessionStoreError::PrincipalDisabled)
        ));
        sqlx::query("UPDATE principals_v1 SET deleted_at=101")
            .execute(s.pool())
            .await
            .unwrap();
        assert!(
            s.lookup_session_credential(&[1; 32])
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            s.lookup_session_credential(&[4; 32])
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            s.fence_session_dispatch("principal", &snapshot(&family(), "credential", 1), 101)
                .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }

    #[tokio::test]
    async fn concurrent_renewals_cas_and_revoke_fence_new_dispatch() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        let (left, right) = tokio::join!(
            s.replace_session_access(replacement(family(), "left", 5, 101, None), 101),
            s.replace_session_access(replacement(family(), "right", 6, 101, None), 101),
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let failure = if left.is_err() {
            left.unwrap_err()
        } else {
            right.unwrap_err()
        };
        assert!(matches!(failure, SessionStoreError::OwnerConflict));
        s.revoke_session_family("family", "renewal", 102)
            .await
            .unwrap();
        assert!(matches!(
            s.fence_session_dispatch("principal", &snapshot(&family(), "credential", 1), 103)
                .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }

    #[tokio::test]
    async fn header_lookup_does_not_discover_owner_and_fence_preserves_unavailable() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        sqlx::query("UPDATE upstream_spec_v1 SET enabled=0")
            .execute(s.pool())
            .await
            .unwrap();
        assert!(
            s.lookup_session_credential(&[1; 32])
                .await
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            s.fence_session_dispatch("principal", &snapshot(&family(), "credential", 1), 101)
                .await,
            Err(SessionStoreError::Storage(StorageError::Unavailable { .. }))
        ));
    }

    #[tokio::test]
    async fn revoke_contracts_authority_after_parent_owner_and_session_expiry() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        sqlx::query("UPDATE managed_keys_v1 SET status='revoked', expires_at=100")
            .execute(s.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision=2, revoked_at_unix_secs=101")
            .execute(s.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE upstream_spec_v1 SET enabled=0")
            .execute(s.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE principals_v1 SET enabled=0")
            .execute(s.pool())
            .await
            .unwrap();
        assert!(matches!(
            s.revoke_session_family("family", "credential", 201).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        s.revoke_session_family("family", "renewal", 201)
            .await
            .expect("compensation only contracts authority");
        let record = s
            .lookup_session_credential(&[4; 32])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.family.revoked_at, Some(201));
        assert_eq!(record.credential.revoked_at, Some(201));
        assert_eq!(record.credential.verify_hash, [0; 32]);
        assert_eq!(record.credential.salt, [0; 16]);
    }

    #[tokio::test]
    async fn racing_revocations_have_one_success_and_reject_replay() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        let (left, right) = tokio::join!(
            s.revoke_session_family("family", "renewal", 101),
            s.revoke_session_family("family", "renewal", 101),
        );
        assert!(matches!(
            (left, right),
            (Ok(()), Err(SessionStoreError::CredentialRejected))
                | (Err(SessionStoreError::CredentialRejected), Ok(()))
        ));
        assert!(matches!(
            s.replace_session_access(replacement(family(), "after-revoke", 5, 102, None), 102)
                .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }

    #[tokio::test]
    async fn invalid_issue_metadata_and_candidate_leave_no_partial_authority() {
        let s = store().await;
        let mut invalid = issue();
        invalid.family.access_ttl_secs = 0;
        assert!(matches!(
            s.issue_session(invalid).await,
            Err(SessionStoreError::InvalidOperation)
        ));
        let mut invalid = issue();
        invalid.family.origin = "https://proxy.example.com/path".into();
        assert!(matches!(
            s.issue_session(invalid).await,
            Err(SessionStoreError::InvalidOperation)
        ));
        let mut invalid = issue();
        invalid.family.created_at = 99;
        assert!(matches!(
            s.issue_session(invalid).await,
            Err(SessionStoreError::InvalidOperation)
        ));
        assert!(s.get_session_family("family").await.unwrap().is_none());
        s.issue_session(issue()).await.unwrap();
        let mut invalid = replacement(family(), "bad-claims", 5, 101, Some("other"));
        invalid.access.workspace_generation = 1;
        assert!(matches!(
            s.replace_session_access(invalid, 101).await,
            Err(SessionStoreError::InvalidOperation)
        ));
        assert_eq!(
            s.get_session_family("family").await.unwrap().unwrap(),
            family()
        );
        s.fence_session_dispatch("principal", &snapshot(&family(), "credential", 1), 102)
            .await
            .expect("failed switch did not revoke old access");
    }

    #[tokio::test]
    async fn issuance_survives_clock_step_but_not_future_or_expired_credentials() {
        let fixture = store().await;
        let clock = std::sync::Arc::new(cc_lb_clock::TestClock::new_at_secs(100));
        let s = SqliteStorage::new(fixture.pool().clone(), clock.clone());
        let command = issue();
        clock.set_unix_secs(101);
        s.issue_session(command)
            .await
            .expect("mint at 100, transaction at 101");

        let fixture = store().await;
        let s = SqliteStorage::new(fixture.pool().clone(), clock.clone());
        let mut future = issue();
        future.family.created_at = 102;
        future.access.issued_at = 102;
        future.access.expires_at = 112;
        future.renewal.issued_at = 102;
        assert!(matches!(
            s.issue_session(future).await,
            Err(SessionStoreError::InvalidOperation)
        ));
        clock.set_unix_secs(110);
        assert!(matches!(
            s.issue_session(issue()).await,
            Err(SessionStoreError::InvalidOperation)
        ));
        clock.set_unix_secs(200);
        assert!(matches!(
            s.issue_session(issue()).await,
            Err(SessionStoreError::InvalidOperation)
        ));
        assert!(s.get_session_family("family").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn discovery_rejects_wrong_purpose_revoked_and_stale_credentials() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        let f = family();
        let mut renewal_snapshot = snapshot(&f, "renewal", 1);
        renewal_snapshot.purpose = CredentialPurpose::SessionRenewal;
        assert!(matches!(
            s.list_session_workspace_aliases("principal", &renewal_snapshot, 101)
                .await,
            Err(SessionStoreError::CredentialRejected)
        ));
        sqlx::query("UPDATE codex_session_credentials_v1 SET revoked_at=101 WHERE credential_id='credential'")
            .execute(s.pool()).await.unwrap();
        assert!(matches!(
            s.list_session_workspace_aliases("principal", &snapshot(&f, "credential", 1), 101)
                .await,
            Err(SessionStoreError::CredentialRejected)
        ));
        sqlx::query("UPDATE codex_session_credentials_v1 SET revoked_at=NULL WHERE credential_id='credential'")
            .execute(s.pool()).await.unwrap();
        let mut stale = snapshot(&f, "credential", 1);
        stale.workspace_generation = 99;
        assert!(matches!(
            s.list_session_workspace_aliases("principal", &stale, 101)
                .await,
            Err(SessionStoreError::OwnerConflict)
        ));
        let generation = snapshot(&f, "credential", 99);
        assert!(matches!(
            s.list_session_workspace_aliases("principal", &generation, 101)
                .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }

    #[tokio::test]
    async fn discovery_reports_source_upstream_unavailable_in_read_snapshot() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        sqlx::query("UPDATE upstream_spec_v1 SET enabled=0")
            .execute(s.pool())
            .await
            .unwrap();
        assert!(matches!(
            s.list_session_workspace_aliases(
                "principal",
                &snapshot(&family(), "credential", 1),
                101
            )
            .await,
            Err(SessionStoreError::Storage(StorageError::Unavailable { .. }))
        ));
    }

    #[tokio::test]
    async fn discovery_lists_only_live_reserved_aliases_once() {
        let s = store().await;
        let principal = "00000000-0000-0000-0000-000000000011";
        for statement in [
            "DELETE FROM account_owner_workspaces_v1",
            "DELETE FROM account_owner_bindings_v1",
            "DELETE FROM managed_keys_v1",
            "DELETE FROM principals_v1",
            "INSERT INTO principals_v1 (id,name,created_at,updated_at) VALUES ('00000000-0000-0000-0000-000000000011','canonical',100,100)",
            "INSERT INTO managed_keys_v1 (secret_hash,created_at,status,principal_id,key_id,label,verify_hash,secret_salt,limit_overrides,last_4,index_hash,updated_at) VALUES ('parent-hash',100,'active','00000000-0000-0000-0000-000000000011','parent','parent-label',zeroblob(32),zeroblob(16),'[]','1234',zeroblob(32),100)",
        ] {
            sqlx::query(statement).execute(s.pool()).await.unwrap();
        }
        s.create_owner_binding(AccountOwnerBinding {
            binding_id: "binding".into(),
            principal_id: principal.into(),
            provider: "openai".into(),
            owner_id: "owner".into(),
            upstream_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            owner_generation: 1,
            local_user_id: "user".into(),
            allowed_workspaces: vec!["workspace".into(), "other".into()],
            provider_workspace_id: "provider-workspace".into(),
            outbound_region: None,
            revision: 1,
            revoked_at_unix_secs: None,
        })
        .await
        .unwrap();
        s.create_owner_binding(AccountOwnerBinding {
            binding_id: "binding-2".into(),
            principal_id: principal.into(),
            provider: "openai".into(),
            owner_id: "owner-2".into(),
            upstream_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            owner_generation: 1,
            local_user_id: "user".into(),
            allowed_workspaces: vec!["workspace-2".into()],
            provider_workspace_id: "provider-workspace-2".into(),
            outbound_region: None,
            revision: 1,
            revoked_at_unix_secs: None,
        })
        .await
        .unwrap();
        s.create_owner_binding(AccountOwnerBinding {
            binding_id: "foreign".into(),
            principal_id: principal.into(),
            provider: "openai".into(),
            owner_id: "foreign-owner".into(),
            upstream_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            owner_generation: 1,
            local_user_id: "other-user".into(),
            allowed_workspaces: vec!["foreign".into()],
            provider_workspace_id: "provider-workspace-3".into(),
            outbound_region: None,
            revision: 1,
            revoked_at_unix_secs: None,
        })
        .await
        .unwrap();
        let mut command = issue();
        command.family.principal_id = principal.into();
        let family = command.family.clone();
        s.issue_session(command).await.unwrap();
        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM account_owner_workspaces_v1")
            .fetch_one(s.pool())
            .await
            .unwrap();
        let mut aliases = s
            .list_session_workspace_aliases(principal, &snapshot(&family, "credential", 1), 101)
            .await
            .unwrap();
        aliases.sort();
        assert_eq!(
            aliases,
            vec![
                "other".to_string(),
                "workspace".to_string(),
                "workspace-2".to_string()
            ]
        );
        let after: i64 = sqlx::query_scalar("SELECT count(*) FROM account_owner_workspaces_v1")
            .fetch_one(s.pool())
            .await
            .unwrap();
        assert_eq!(before, after);
    }

    fn issue() -> IssueSession {
        let mut renewal = credential(CredentialPurpose::SessionRenewal, 100);
        renewal.credential_id = "renewal".into();
        renewal.index_hash = [4; 32];
        renewal.expires_at = 200;
        IssueSession {
            family: family(),
            access: credential(CredentialPurpose::SessionAccess, 100),
            renewal,
        }
    }

    fn snapshot(f: &SessionFamily, id: &str, access_generation: u64) -> SessionSnapshot {
        SessionSnapshot {
            family_id: f.family_id.clone(),
            parent_key_id: f.parent_key_id.clone(),
            credential_id: id.into(),
            purpose: CredentialPurpose::SessionAccess,
            owner_binding_id: f.owner_binding_id.clone(),
            provider: f.provider.clone(),
            owner_generation: f.owner_generation,
            binding_revision: f.binding_revision,
            workspace_id: f.workspace_id.clone(),
            workspace_generation: f.workspace_generation,
            access_generation,
            origin: f.origin.clone(),
        }
    }

    fn replacement(
        f: SessionFamily,
        id: &str,
        hash: u8,
        now: u64,
        target: Option<&str>,
    ) -> ReplaceSessionAccess {
        let mut access = credential(CredentialPurpose::SessionAccess, now);
        access.credential_id = id.into();
        access.index_hash = [hash; 32];
        access.access_generation = f.access_generation + 1;
        access.workspace_generation = f.workspace_generation + u64::from(target.is_some());
        access.expires_at = (now + f.access_ttl_secs).min(f.expires_at);
        let target_owner = target.map(|workspace| owner_expectation(&f, workspace));
        ReplaceSessionAccess {
            expected: f,
            authorizing_credential_id: if target.is_some() {
                "credential"
            } else {
                "renewal"
            }
            .into(),
            access,
            target_owner,
        }
    }

    #[tokio::test]
    async fn issue_lookup_expiry_renewal_and_overlapping_access() {
        let s = store().await;
        s.issue_session(issue()).await.expect("issue");
        let record = s
            .lookup_session_credential(&[1; 32])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.parent_key.label, "parent-label");
        assert_eq!(record.parent_key.key_hash_b64, "parent-hash");
        assert!(record.principal_enabled);
        assert!(
            s.lookup_session_credential(&[9; 32])
                .await
                .unwrap()
                .is_none()
        );
        let old = snapshot(&record.family, "credential", 1);
        let f = s
            .replace_session_access(replacement(record.family, "new", 5, 105, None), 105)
            .await
            .unwrap();
        s.fence_session_dispatch("principal", &old, 109)
            .await
            .expect("overlapping old access");
        assert!(matches!(
            s.fence_session_dispatch("principal", &old, 110).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        s.replace_session_access(replacement(f, "after-expiry", 6, 111, None), 111)
            .await
            .expect("renewal independent of access expiry");
    }

    #[tokio::test]
    async fn stale_cas_switch_revocation_and_retained_lookup() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        let old = snapshot(&family(), "credential", 1);
        let f = s
            .replace_session_access(
                replacement(family(), "switched", 5, 101, Some("other")),
                101,
            )
            .await
            .unwrap();
        assert_eq!(f.workspace_generation, 2);
        assert!(
            s.fence_session_dispatch("principal", &old, 102)
                .await
                .is_err()
        );
        assert!(matches!(
            s.replace_session_access(replacement(family(), "stale", 6, 102, None), 102)
                .await,
            Err(SessionStoreError::OwnerConflict)
        ));
        let f = s
            .replace_session_access(replacement(f, "renew-current", 7, 102, None), 102)
            .await
            .expect("original renewal follows workspace");
        s.revoke_session_family(&f.family_id, "renewal", 103)
            .await
            .unwrap();
        let record = s
            .lookup_session_credential(&[4; 32])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.family.revoked_at, Some(103));
        assert_eq!(record.credential.verify_hash, [0; 32]);
        assert!(matches!(
            s.revoke_session_family(&f.family_id, "renewal", 104).await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }

    #[tokio::test]
    async fn owner_revision_and_disabled_parent_principal_are_distinct() {
        let s = store().await;
        s.issue_session(issue()).await.unwrap();
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision=2")
            .execute(s.pool())
            .await
            .unwrap();
        assert!(matches!(
            s.replace_session_access(replacement(family(), "new", 5, 101, None), 101)
                .await,
            Err(SessionStoreError::OwnerConflict)
        ));
        sqlx::query("UPDATE account_owner_bindings_v1 SET revision=1")
            .execute(s.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE principals_v1 SET enabled=0")
            .execute(s.pool())
            .await
            .unwrap();
        assert!(matches!(
            s.replace_session_access(replacement(family(), "new", 5, 101, None), 101)
                .await,
            Err(SessionStoreError::PrincipalDisabled)
        ));
        sqlx::query("UPDATE principals_v1 SET enabled=1")
            .execute(s.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE managed_keys_v1 SET status='revoked'")
            .execute(s.pool())
            .await
            .unwrap();
        assert!(matches!(
            s.replace_session_access(replacement(family(), "new", 5, 101, None), 101)
                .await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }

    #[tokio::test]
    async fn issue_is_atomic_and_cross_family_authorizer_is_rejected() {
        let s = store().await;
        let mut invalid = issue();
        invalid.renewal.credential_id = invalid.access.credential_id.clone();
        assert!(s.issue_session(invalid).await.is_err());
        assert!(
            s.lookup_session_credential(&[1; 32])
                .await
                .unwrap()
                .is_none()
        );
        s.issue_session(issue()).await.unwrap();
        let mut wrong = replacement(family(), "wrong-purpose", 5, 101, None);
        wrong.authorizing_credential_id = "credential".into();
        assert!(matches!(
            s.replace_session_access(wrong, 101).await,
            Err(SessionStoreError::CredentialRejected)
        ));
        let mut other = issue();
        other.family.family_id = "other-family".into();
        other.access.family_id = "other-family".into();
        other.access.credential_id = "other-access".into();
        other.access.index_hash = [8; 32];
        other.renewal.family_id = "other-family".into();
        other.renewal.credential_id = "other-renewal".into();
        other.renewal.index_hash = [9; 32];
        s.issue_session(other).await.unwrap();
        let mut wrong = replacement(family(), "cross", 5, 101, None);
        wrong.authorizing_credential_id = "other-renewal".into();
        assert!(matches!(
            s.replace_session_access(wrong, 101).await,
            Err(SessionStoreError::CredentialRejected)
        ));
    }

    fn family() -> SessionFamily {
        SessionFamily {
            family_id: "family".into(),
            principal_id: "principal".into(),
            parent_key_id: "parent".into(),
            owner_binding_id: "binding".into(),
            provider: "openai".into(),
            owner_generation: 1,
            binding_revision: 1,
            workspace_id: "workspace".into(),
            workspace_generation: 1,
            access_generation: 1,
            local_user_id: "user".into(),
            plan_type: "unknown".into(),
            origin: "https://proxy.example.com".into(),
            access_ttl_secs: 10,
            created_at: 100,
            expires_at: 200,
            revoked_at: None,
        }
    }

    fn credential(purpose: CredentialPurpose, now: u64) -> SessionCredential {
        SessionCredential {
            credential_id: "credential".into(),
            family_id: "family".into(),
            purpose,
            index_hash: [1; 32],
            verify_hash: [2; 32],
            salt: [3; 16],
            workspace_generation: 1,
            access_generation: 1,
            issued_at: now,
            expires_at: now + 10,
            revoked_at: None,
        }
    }

    #[test]
    fn literal_expiry_and_cross_family_and_purpose() {
        let f = family();
        let mut c = credential(CredentialPurpose::SessionAccess, 100);
        assert!(validate_credential(&c, &f, CredentialPurpose::SessionAccess, 109).is_ok());
        assert!(validate_credential(&c, &f, CredentialPurpose::SessionAccess, 110).is_err());
        assert!(validate_credential(&c, &f, CredentialPurpose::SessionRenewal, 101).is_err());
        c.family_id = "other".into();
        assert!(validate_credential(&c, &f, CredentialPurpose::SessionAccess, 101).is_err());
    }

    #[test]
    fn overlapping_access_survives_renewal_but_not_switch_or_revoke() {
        let mut f = family();
        let c = credential(CredentialPurpose::SessionAccess, 100);
        f.access_generation = 2;
        assert!(validate_credential(&c, &f, CredentialPurpose::SessionAccess, 101).is_ok());
        f.workspace_generation = 2;
        assert!(validate_credential(&c, &f, CredentialPurpose::SessionAccess, 101).is_err());
        let renewal = credential(CredentialPurpose::SessionRenewal, 100);
        assert!(validate_credential(&renewal, &f, CredentialPurpose::SessionRenewal, 101).is_ok());
        f.revoked_at = Some(101);
        assert!(validate_credential(&renewal, &f, CredentialPurpose::SessionRenewal, 101).is_err());
    }

    #[test]
    fn candidate_claim_stamps_and_ttl_are_exact() {
        let f = family();
        let mut c = credential(CredentialPurpose::SessionAccess, 100);
        assert!(validate_candidate(&c, &f, 100).is_ok());
        c.access_generation = 2;
        assert!(validate_candidate(&c, &f, 100).is_err());
        c.access_generation = 1;
        c.expires_at = 111;
        assert!(validate_candidate(&c, &f, 100).is_err());
    }
}
