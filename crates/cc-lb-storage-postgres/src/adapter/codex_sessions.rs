use async_trait::async_trait;
use cc_lb_storage_api::{
    AccountOwnerBinding, KeyStatus, OwnerBindingExpectation, StorageError, StorageResult, StoredApiKeyRecord,
    codex_sessions::{
        CodexSessionStore, CredentialPurpose, IssueSession, ReplaceSessionAccess, SessionAuthRecord,
        SessionCredential, SessionFamily, SessionSnapshot, SessionStoreError, SessionStoreResult,
    },
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::{
    adapter::{
        PostgresStorage,
        account_owner::{validate_owner_binding_in_tx, validate_owner_bindings_in_tx},
        i64_to_u64, u64_to_i64,
    },
    error_map::map_sqlx_error,
};

// Header authentication never joins owner bindings or upstreams. Every join is a point lookup
// through the credential hash, family ID, principal ID or composite managed-key identity.
const LOOKUP_SQL: &str = "SELECT f.*, \
    c.credential_id, c.purpose, c.index_hash AS credential_index_hash, \
    c.verify_hash AS credential_verify_hash, c.salt AS credential_salt, \
    c.workspace_generation AS credential_workspace_generation, \
    c.access_generation AS credential_access_generation, c.issued_at AS credential_issued_at, \
    c.expires_at AS credential_expires_at, c.revoked_at AS credential_revoked_at, \
    p.enabled AS principal_enabled, \
    k.label AS parent_label, k.issued_at_unix_secs AS parent_issued_at, \
    k.revoked_at_unix_secs AS parent_revoked_at, k.key_hash_b64 AS parent_key_hash_b64, \
    k.verify_hash AS parent_verify_hash, k.secret_salt AS parent_salt, \
    k.limit_overrides AS parent_limit_overrides, k.status AS parent_status, \
    k.expires_at_unix_secs AS parent_expires_at, k.last_4 AS parent_last_4, \
    k.description AS parent_description, k.index_hash AS parent_index_hash \
    FROM codex_session_credentials_v1 c \
    JOIN codex_session_families_v1 f ON f.family_id = c.family_id \
    JOIN principals_v1 p ON p.id = f.principal_id::uuid AND p.deleted_at IS NULL \
    JOIN managed_api_keys_v1 k ON k.principal_id = f.principal_id AND k.key_id = f.parent_key_id \
    JOIN managed_api_key_index_v1 i ON i.principal_id = k.principal_id \
        AND i.key_id = k.key_id AND i.index_hash = k.index_hash \
    WHERE c.index_hash = $1";

#[async_trait]
impl CodexSessionStore for PostgresStorage {
    async fn lookup_session_credential(
        &self,
        index_hash: &[u8; 32],
    ) -> SessionStoreResult<Option<SessionAuthRecord>> {
        let row = sqlx::query(LOOKUP_SQL)
            .bind(index_hash.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.map(|row| {
            Ok(SessionAuthRecord {
                credential: decode_credential(&row, true)?,
                family: decode_family(&row)?,
                parent_key: decode_parent(&row)?,
                principal_enabled: row.try_get("principal_enabled").map_err(map_sqlx_error)?,
            })
        })
        .transpose()
    }

    async fn get_session_family(&self, family_id: &str) -> SessionStoreResult<Option<SessionFamily>> {
        let row = sqlx::query("SELECT * FROM codex_session_families_v1 WHERE family_id = $1")
            .bind(family_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;
        row.as_ref().map(decode_family).transpose().map_err(Into::into)
    }

    async fn issue_session(&self, command: IssueSession) -> SessionStoreResult<()> {
        let family = &command.family;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let now = cc_lb_clock::unix_secs(self.clock.now());
        validate_issue(&command, now)?;
        validate_owner(&mut tx, family).await?;
        lock_principal_and_parent(&mut tx, family, now).await?;
        insert_family(&mut tx, family).await?;
        insert_credential(&mut tx, &command.access).await?;
        insert_credential(&mut tx, &command.renewal).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn replace_session_access(
        &self,
        command: ReplaceSessionAccess,
        now: u64,
    ) -> SessionStoreResult<SessionFamily> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let snapshot = family_snapshot(
            &command.expected,
            &command.authorizing_credential_id,
            if command.target_owner.is_some() {
                CredentialPurpose::SessionAccess
            } else {
                CredentialPurpose::SessionRenewal
            },
        );
        let (current, binding) = validate_family_authority_in_tx(
            &mut tx,
            &command.expected.principal_id,
            &snapshot,
            now,
            command.target_owner.as_ref(),
        )
        .await?;
        // Compare the complete pre-mint authority, not only mutable generations. Never
        // rewrite externally minted token claims to fit a newer database snapshot.
        if current != command.expected {
            return Err(SessionStoreError::OwnerConflict);
        }
        let authorizing = read_credential(&mut tx, &command.authorizing_credential_id, &current.family_id)
            .await?
            .ok_or(SessionStoreError::CredentialRejected)?;
        validate_live_credential(&authorizing, &current, snapshot.purpose, now)?;

        let mut replacement = current;
        replacement.access_generation = replacement
            .access_generation
            .checked_add(1)
            .ok_or(SessionStoreError::InvalidOperation)?;
        if let Some(target) = command.target_owner.as_ref() {
            let binding = binding.ok_or(SessionStoreError::InvalidOperation)?;
            replacement.owner_binding_id = binding.binding_id;
            replacement.provider = binding.provider;
            replacement.owner_generation = binding.owner_generation;
            replacement.binding_revision = binding.revision;
            replacement.workspace_id.clone_from(&target.workspace_id);
            replacement.plan_type.clear();
            replacement.plan_type.push_str("unknown");
            replacement.workspace_generation = replacement
                .workspace_generation
                .checked_add(1)
                .ok_or(SessionStoreError::InvalidOperation)?;
        }
        validate_new_access(&command.access, &replacement, now)?;
        sqlx::query(
            "UPDATE codex_session_families_v1 SET workspace_id = $2, workspace_generation = $3, \
             access_generation = $4, owner_binding_id = $5, provider = $6, \
             owner_generation = $7, binding_revision = $8, plan_type = $9 WHERE family_id = $1",
        )
        .bind(&replacement.family_id)
        .bind(&replacement.workspace_id)
        .bind(u64_to_i64(replacement.workspace_generation, "workspace_generation")?)
        .bind(u64_to_i64(replacement.access_generation, "access_generation")?)
        .bind(&replacement.owner_binding_id)
        .bind(&replacement.provider)
        .bind(u64_to_i64(replacement.owner_generation, "owner_generation")?)
        .bind(u64_to_i64(replacement.binding_revision, "binding_revision")?)
        .bind(&replacement.plan_type)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        if command.target_owner.is_some() {
            sqlx::query(
                "UPDATE codex_session_credentials_v1 SET revoked_at = $2 \
                 WHERE family_id = $1 AND purpose = 'access' AND revoked_at IS NULL",
            )
            .bind(&replacement.family_id)
            .bind(u64_to_i64(now, "revoked_at")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        }
        insert_credential(&mut tx, &command.access).await?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(replacement)
    }

    async fn revoke_session_family(
        &self,
        family_id: &str,
        renewal_credential_id: &str,
        now: u64,
    ) -> SessionStoreResult<()> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        // Contraction-only compensation must remain possible after an owner,
        // principal or parent is withdrawn. Never acquire their locks here.
        let current = read_family(&mut tx, family_id, true)
            .await?
            .ok_or(SessionStoreError::CredentialRejected)?;
        if current.revoked_at.is_some() {
            return Err(SessionStoreError::CredentialRejected);
        }
        let renewal = read_credential(&mut tx, renewal_credential_id, family_id)
            .await?
            .ok_or(SessionStoreError::CredentialRejected)?;
        if renewal.purpose != CredentialPurpose::SessionRenewal || renewal.revoked_at.is_some() {
            return Err(SessionStoreError::CredentialRejected);
        }
        if now < current.created_at || now < renewal.issued_at {
            return Err(SessionStoreError::InvalidOperation);
        }
        // Expiry is intentionally not an internal compensation prerequisite.
        // Public lifecycle handlers must first authenticate a live renewal header.
        // Family revocation is the common fence for access, renewal and separately
        // stored relay descendants. It does not depend on a relay table being present.
        sqlx::query("UPDATE codex_session_families_v1 SET revoked_at = $2 WHERE family_id = $1")
            .bind(family_id)
            .bind(u64_to_i64(now, "revoked_at")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query(
            "UPDATE codex_session_credentials_v1 SET revoked_at = $2 \
             WHERE family_id = $1 AND revoked_at IS NULL",
        )
        .bind(family_id)
        .bind(u64_to_i64(now, "revoked_at")?)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_error)?;
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn list_session_workspace_aliases(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        now: u64,
    ) -> SessionStoreResult<Vec<String>> {
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let family = validate_session_family_in_tx(&mut tx, principal_id, snapshot, now).await?;
        let aliases = super::account_owner::list_owner_workspace_aliases_in_read_tx(
            &mut tx, principal_id, &family.provider, &family.local_user_id,
        ).await.map_err(|error| match error {
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
    ) -> SessionStoreResult<()> {
        if snapshot.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let family = validate_session_family_in_tx(&mut tx, principal_id, snapshot, now).await?;
        let credential = read_credential(&mut tx, &snapshot.credential_id, &family.family_id)
            .await?
            .ok_or(SessionStoreError::CredentialRejected)?;
        validate_live_credential(&credential, &family, CredentialPurpose::SessionAccess, now)?;
        if credential.access_generation != snapshot.access_generation {
            return Err(SessionStoreError::CredentialRejected);
        }
        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(())
    }
}

/// Internal family fence for the relay adapter after its own credential validation.
/// Lock order: upstream -> binding -> principal -> parent -> family -> credential.
/// Call before acquiring any relay-credential row lock; this is not a public proof constructor.
/// The caller must validate relay lineage, purpose, expiry and revocation in this same transaction.
pub(crate) async fn validate_session_family_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    principal_id: &str,
    snapshot: &SessionSnapshot,
    now: u64,
) -> SessionStoreResult<SessionFamily> {
    validate_family_authority_in_tx(tx, principal_id, snapshot, now, None).await.map(|(family, _)| family)
}

async fn validate_family_authority_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    principal_id: &str,
    snapshot: &SessionSnapshot,
    now: u64,
    target: Option<&OwnerBindingExpectation>,
) -> SessionStoreResult<(SessionFamily, Option<AccountOwnerBinding>)> {
    let mut discovered = read_family(tx, &snapshot.family_id, false)
        .await?
        .ok_or(SessionStoreError::CredentialRejected)?;
    validate_live_family(&discovered, now)?;
    validate_snapshot(&discovered, principal_id, snapshot)?;
    let binding = if let Some(target) = target {
        let (source_binding, target_binding) =
            validate_owner_bindings_in_tx(tx, &owner_expectation(&discovered), Some(target))
                .await
                .map_err(map_owner_error)?;
        let target_binding = target_binding.ok_or(SessionStoreError::OwnerConflict)?;
        if source_binding.local_user_id != discovered.local_user_id
            || target_binding.local_user_id != discovered.local_user_id
            || target_binding.principal_id != discovered.principal_id
            || target_binding.provider != discovered.provider
        {
            return Err(SessionStoreError::OwnerConflict);
        }
        Some(target_binding)
    } else {
        validate_owner(tx, &discovered).await?;
        None
    };
    lock_principal_and_parent(tx, &discovered, now).await?;
    let current = read_family(tx, &snapshot.family_id, true)
        .await?
        .ok_or(SessionStoreError::CredentialRejected)?;
    validate_live_family(&current, now)?;
    validate_snapshot(&current, principal_id, snapshot)?;
    // Renew may have committed while the ordered authority locks were acquired.
    // It changes only access_generation, which is not a dispatch fence.
    discovered.access_generation = current.access_generation;
    if current != discovered {
        return Err(SessionStoreError::OwnerConflict);
    }
    Ok((current, binding))
}

fn validate_snapshot(
    family: &SessionFamily,
    principal_id: &str,
    snapshot: &SessionSnapshot,
) -> SessionStoreResult<()> {
    if family.principal_id != principal_id
        || family.family_id != snapshot.family_id
        || family.parent_key_id != snapshot.parent_key_id
        || family.owner_binding_id != snapshot.owner_binding_id
        || family.provider != snapshot.provider
        || family.owner_generation != snapshot.owner_generation
        || family.binding_revision != snapshot.binding_revision
        || family.workspace_id != snapshot.workspace_id
        || family.workspace_generation != snapshot.workspace_generation
        || family.origin != snapshot.origin
    {
        return Err(SessionStoreError::OwnerConflict);
    }
    // Deliberately no equality against family.access_generation: renew preserves
    // every prior access until its own expiry, whereas workspace changes fence all.
    Ok(())
}

fn validate_live_family(family: &SessionFamily, now: u64) -> SessionStoreResult<()> {
    if family.revoked_at.is_some() || now < family.created_at || now >= family.expires_at {
        return Err(SessionStoreError::CredentialRejected);
    }
    Ok(())
}

fn owner_expectation(family: &SessionFamily) -> OwnerBindingExpectation {
    OwnerBindingExpectation {
        binding_id: family.owner_binding_id.clone(),
        principal_id: family.principal_id.clone(),
        provider: family.provider.clone(),
        owner_generation: family.owner_generation,
        binding_revision: family.binding_revision,
        workspace_id: family.workspace_id.clone(),
    }
}

fn map_owner_error(error: StorageError) -> SessionStoreError {
    match error {
        StorageError::Conflict { .. } => SessionStoreError::OwnerConflict,
        other => SessionStoreError::Storage(other),
    }
}

async fn validate_owner(
    tx: &mut Transaction<'_, Postgres>,
    family: &SessionFamily,
) -> SessionStoreResult<AccountOwnerBinding> {
    let binding = validate_owner_binding_in_tx(tx, &owner_expectation(family))
        .await
        .map_err(map_owner_error)?;
    if binding.local_user_id != family.local_user_id {
        return Err(SessionStoreError::OwnerConflict);
    }
    Ok(binding)
}

async fn lock_principal_and_parent(
    tx: &mut Transaction<'_, Postgres>,
    family: &SessionFamily,
    now: u64,
) -> SessionStoreResult<()> {
    let principal_id = Uuid::parse_str(&family.principal_id)
        .map_err(|_| SessionStoreError::CredentialRejected)?;
    let principal: Option<(bool, bool)> = sqlx::query_as(
        "SELECT enabled, deleted_at IS NOT NULL FROM principals_v1 WHERE id = $1 FOR UPDATE",
    )
    .bind(principal_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    match principal {
        None | Some((_, true)) => return Err(SessionStoreError::CredentialRejected),
        Some((false, false)) => return Err(SessionStoreError::PrincipalDisabled),
        Some((true, false)) => {}
    }
    let parent: Option<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT k.status, k.revoked_at_unix_secs, k.expires_at_unix_secs \
         FROM managed_api_keys_v1 k WHERE k.principal_id = $1 AND k.key_id = $2 \
         AND EXISTS (SELECT 1 FROM managed_api_key_index_v1 i \
             WHERE i.principal_id = k.principal_id AND i.key_id = k.key_id AND i.index_hash = k.index_hash) \
         FOR UPDATE OF k",
    )
    .bind(&family.principal_id)
    .bind(&family.parent_key_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_sqlx_error)?;
    let Some((status, revoked_at, expires_at)) = parent else {
        return Err(SessionStoreError::CredentialRejected);
    };
    let expires_at = expires_at.map(|value| i64_to_u64(value, "parent.expires_at")).transpose()?;
    // Keep the established ordinary managed-key expiry boundary unchanged.
    if status != "active" || revoked_at.is_some() || expires_at.is_some_and(|expiry| now > expiry) {
        return Err(SessionStoreError::CredentialRejected);
    }
    Ok(())
}

fn validate_live_credential(
    credential: &SessionCredential,
    family: &SessionFamily,
    purpose: CredentialPurpose,
    now: u64,
) -> SessionStoreResult<()> {
    if credential.family_id != family.family_id
        || credential.purpose != purpose
        || credential.revoked_at.is_some()
        || now < credential.issued_at
        || now >= credential.expires_at
        || credential.expires_at > family.expires_at
        || (purpose == CredentialPurpose::SessionAccess
            && (credential.workspace_generation != family.workspace_generation
                || credential.access_generation > family.access_generation))
    {
        return Err(SessionStoreError::CredentialRejected);
    }
    Ok(())
}

fn validate_issue(command: &IssueSession, now: u64) -> SessionStoreResult<()> {
    let family = &command.family;
    if [
        family.family_id.as_str(), family.principal_id.as_str(), family.parent_key_id.as_str(),
        family.owner_binding_id.as_str(), family.provider.as_str(), family.workspace_id.as_str(),
        family.local_user_id.as_str(), family.plan_type.as_str(), family.origin.as_str(),
        command.access.credential_id.as_str(), command.renewal.credential_id.as_str(),
    ].iter().any(|value| value.is_empty())
        || family.owner_generation == 0
        || family.binding_revision == 0
        || family.workspace_generation != 1
        || family.access_generation != 1
        || family.access_ttl_secs == 0
        || family.revoked_at.is_some()
        || family.created_at > now
        || now >= family.expires_at
        || now >= command.access.expires_at
        || now >= command.renewal.expires_at
        || family.expires_at <= family.created_at
        || family.access_ttl_secs > family.expires_at - family.created_at
        || command.access.credential_id == command.renewal.credential_id
        || command.access.index_hash == command.renewal.index_hash
    {
        return Err(SessionStoreError::InvalidOperation);
    }
    validate_new_access(&command.access, family, family.created_at)?;
    let renewal = &command.renewal;
    if renewal.family_id != family.family_id
        || renewal.purpose != CredentialPurpose::SessionRenewal
        || renewal.workspace_generation != 1
        || renewal.access_generation != 1
        || renewal.issued_at != family.created_at
        || renewal.expires_at != family.expires_at
        || renewal.revoked_at.is_some()
    {
        return Err(SessionStoreError::InvalidOperation);
    }
    Ok(())
}

fn validate_new_access(
    access: &SessionCredential,
    family: &SessionFamily,
    now: u64,
) -> SessionStoreResult<()> {
    let expires_at = now.checked_add(family.access_ttl_secs)
        .ok_or(SessionStoreError::InvalidOperation)?.min(family.expires_at);
    if access.credential_id.is_empty()
        || access.family_id != family.family_id
        || access.purpose != CredentialPurpose::SessionAccess
        || access.workspace_generation != family.workspace_generation
        || access.access_generation != family.access_generation
        || access.issued_at != now
        || access.expires_at != expires_at
        || access.expires_at <= now
        || access.revoked_at.is_some()
    {
        return Err(SessionStoreError::InvalidOperation);
    }
    Ok(())
}

fn family_snapshot(family: &SessionFamily, credential_id: &str, purpose: CredentialPurpose) -> SessionSnapshot {
    SessionSnapshot {
        family_id: family.family_id.clone(),
        parent_key_id: family.parent_key_id.clone(),
        credential_id: credential_id.to_owned(),
        purpose,
        owner_binding_id: family.owner_binding_id.clone(),
        provider: family.provider.clone(),
        owner_generation: family.owner_generation,
        binding_revision: family.binding_revision,
        workspace_id: family.workspace_id.clone(),
        workspace_generation: family.workspace_generation,
        access_generation: family.access_generation,
        origin: family.origin.clone(),
    }
}

async fn read_family(
    tx: &mut Transaction<'_, Postgres>,
    family_id: &str,
    lock: bool,
) -> SessionStoreResult<Option<SessionFamily>> {
    let sql = if lock {
        "SELECT * FROM codex_session_families_v1 WHERE family_id = $1 FOR UPDATE"
    } else {
        "SELECT * FROM codex_session_families_v1 WHERE family_id = $1"
    };
    let row = sqlx::query(sql).bind(family_id).fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.as_ref().map(decode_family).transpose().map_err(Into::into)
}

async fn read_credential(
    tx: &mut Transaction<'_, Postgres>,
    credential_id: &str,
    family_id: &str,
) -> SessionStoreResult<Option<SessionCredential>> {
    let row = sqlx::query("SELECT * FROM codex_session_credentials_v1 WHERE credential_id = $1 AND family_id = $2 FOR UPDATE")
        .bind(credential_id).bind(family_id).fetch_optional(&mut **tx).await.map_err(map_sqlx_error)?;
    row.as_ref().map(|row| decode_credential(row, false)).transpose().map_err(Into::into)
}

async fn insert_family(tx: &mut Transaction<'_, Postgres>, family: &SessionFamily) -> SessionStoreResult<()> {
    sqlx::query(
        "INSERT INTO codex_session_families_v1 (family_id, principal_id, parent_key_id, \
         owner_binding_id, provider, owner_generation, binding_revision, workspace_id, \
         workspace_generation, access_generation, local_user_id, plan_type, origin, \
         access_ttl_secs, created_at, expires_at, revoked_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
    )
    .bind(&family.family_id).bind(&family.principal_id).bind(&family.parent_key_id)
    .bind(&family.owner_binding_id).bind(&family.provider)
    .bind(u64_to_i64(family.owner_generation, "owner_generation")?)
    .bind(u64_to_i64(family.binding_revision, "binding_revision")?).bind(&family.workspace_id)
    .bind(u64_to_i64(family.workspace_generation, "workspace_generation")?)
    .bind(u64_to_i64(family.access_generation, "access_generation")?)
    .bind(&family.local_user_id).bind(&family.plan_type).bind(&family.origin)
    .bind(u64_to_i64(family.access_ttl_secs, "access_ttl_secs")?)
    .bind(u64_to_i64(family.created_at, "created_at")?)
    .bind(u64_to_i64(family.expires_at, "expires_at")?)
    .bind(family.revoked_at.map(|value| u64_to_i64(value, "revoked_at")).transpose()?)
    .execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

async fn insert_credential(tx: &mut Transaction<'_, Postgres>, credential: &SessionCredential) -> SessionStoreResult<()> {
    let purpose = match credential.purpose {
        CredentialPurpose::SessionAccess => "access",
        CredentialPurpose::SessionRenewal => "renewal",
        _ => return Err(SessionStoreError::InvalidOperation),
    };
    sqlx::query(
        "INSERT INTO codex_session_credentials_v1 (credential_id, family_id, purpose, index_hash, \
         verify_hash, salt, workspace_generation, access_generation, issued_at, expires_at, revoked_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
    )
    .bind(&credential.credential_id).bind(&credential.family_id).bind(purpose)
    .bind(credential.index_hash.as_slice()).bind(credential.verify_hash.as_slice()).bind(credential.salt.as_slice())
    .bind(u64_to_i64(credential.workspace_generation, "workspace_generation")?)
    .bind(u64_to_i64(credential.access_generation, "access_generation")?)
    .bind(u64_to_i64(credential.issued_at, "issued_at")?)
    .bind(u64_to_i64(credential.expires_at, "expires_at")?)
    .bind(credential.revoked_at.map(|value| u64_to_i64(value, "revoked_at")).transpose()?)
    .execute(&mut **tx).await.map_err(map_sqlx_error)?;
    Ok(())
}

fn number(row: &PgRow, column: &str) -> StorageResult<u64> {
    i64_to_u64(row.try_get(column).map_err(map_sqlx_error)?, column)
}

fn optional_number(row: &PgRow, column: &str) -> StorageResult<Option<u64>> {
    row.try_get::<Option<i64>, _>(column).map_err(map_sqlx_error)?
        .map(|value| i64_to_u64(value, column)).transpose()
}

fn bytes<const N: usize>(row: &PgRow, column: &str) -> StorageResult<[u8; N]> {
    let value: Vec<u8> = row.try_get(column).map_err(map_sqlx_error)?;
    value.try_into().map_err(|_| StorageError::Corrupted {
        message: format!("invalid {column} length"),
    })
}

fn decode_family(row: &PgRow) -> StorageResult<SessionFamily> {
    Ok(SessionFamily {
        family_id: row.try_get("family_id").map_err(map_sqlx_error)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        parent_key_id: row.try_get("parent_key_id").map_err(map_sqlx_error)?,
        owner_binding_id: row.try_get("owner_binding_id").map_err(map_sqlx_error)?,
        provider: row.try_get("provider").map_err(map_sqlx_error)?,
        owner_generation: number(row, "owner_generation")?,
        binding_revision: number(row, "binding_revision")?,
        workspace_id: row.try_get("workspace_id").map_err(map_sqlx_error)?,
        workspace_generation: number(row, "workspace_generation")?,
        access_generation: number(row, "access_generation")?,
        local_user_id: row.try_get("local_user_id").map_err(map_sqlx_error)?,
        plan_type: row.try_get("plan_type").map_err(map_sqlx_error)?,
        origin: row.try_get("origin").map_err(map_sqlx_error)?,
        access_ttl_secs: number(row, "access_ttl_secs")?,
        created_at: number(row, "created_at")?,
        expires_at: number(row, "expires_at")?,
        revoked_at: optional_number(row, "revoked_at")?,
    })
}

fn decode_credential(row: &PgRow, joined: bool) -> StorageResult<SessionCredential> {
    let purpose: String = row.try_get("purpose").map_err(map_sqlx_error)?;
    Ok(SessionCredential {
        credential_id: row.try_get("credential_id").map_err(map_sqlx_error)?,
        family_id: row.try_get("family_id").map_err(map_sqlx_error)?,
        purpose: match purpose.as_str() {
            "access" => CredentialPurpose::SessionAccess,
            "renewal" => CredentialPurpose::SessionRenewal,
            _ => return Err(StorageError::Corrupted { message: "invalid session purpose".into() }),
        },
        index_hash: bytes(row, if joined { "credential_index_hash" } else { "index_hash" })?,
        verify_hash: bytes(row, if joined { "credential_verify_hash" } else { "verify_hash" })?,
        salt: bytes(row, if joined { "credential_salt" } else { "salt" })?,
        workspace_generation: number(row, if joined { "credential_workspace_generation" } else { "workspace_generation" })?,
        access_generation: number(row, if joined { "credential_access_generation" } else { "access_generation" })?,
        issued_at: number(row, if joined { "credential_issued_at" } else { "issued_at" })?,
        expires_at: number(row, if joined { "credential_expires_at" } else { "expires_at" })?,
        revoked_at: optional_number(row, if joined { "credential_revoked_at" } else { "revoked_at" })?,
    })
}

fn decode_parent(row: &PgRow) -> StorageResult<StoredApiKeyRecord> {
    let status: String = row.try_get("parent_status").map_err(map_sqlx_error)?;
    let limits: serde_json::Value = row.try_get("parent_limit_overrides").map_err(map_sqlx_error)?;
    Ok(StoredApiKeyRecord {
        label: row.try_get("parent_label").map_err(map_sqlx_error)?,
        issued_at_unix_secs: number(row, "parent_issued_at")?,
        revoked_at_unix_secs: optional_number(row, "parent_revoked_at")?,
        key_hash_b64: row.try_get("parent_key_hash_b64").map_err(map_sqlx_error)?,
        verify_hash: bytes(row, "parent_verify_hash")?,
        secret_salt: bytes(row, "parent_salt")?,
        limit_overrides: serde_json::from_value(limits).map_err(|_| StorageError::Corrupted {
            message: "invalid parent limit overrides".into(),
        })?,
        status: match status.as_str() {
            "active" => KeyStatus::Active,
            "revoked" => KeyStatus::Revoked,
            _ => return Err(StorageError::Corrupted { message: "invalid parent key status".into() }),
        },
        expires_at_unix_secs: optional_number(row, "parent_expires_at")?,
        last_4: row.try_get("parent_last_4").map_err(map_sqlx_error)?,
        description: row.try_get("parent_description").map_err(map_sqlx_error)?,
        index_hash: bytes(row, "parent_index_hash")?,
    })
}

#[cfg(test)]
#[path = "codex_sessions_tests.rs"]
mod tests;
