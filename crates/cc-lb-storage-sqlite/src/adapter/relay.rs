use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError,
    codex_sessions::{CredentialPurpose, SessionSnapshot, SessionStoreError, SessionStoreResult},
    relay::{
        AccessRelaySelector, IssueRelayEnrollment, RefreshRelayEnrollment, RelayAuthRecord,
        RelayCredential, RelayDispatch, RelayEnrollment, RelayEnrollmentIdentity,
        RelayEnrollmentMetadata, RelayEnrollmentStore, RelayLineage, validate_relay_dispatch,
        validate_relay_grant,
    },
};
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};

use super::codex_sessions::{
    family_row, parent_row, validate_session_access_credential_in_tx, validate_session_family_in_tx,
};
use crate::SqliteStorage;

// Explicit metadata projection: neither provider expiry nor ciphertext is read.
const HEADER_SQL: &str = "SELECT c.*, e.principal_id, e.parent_key_id, e.owner_binding_id, \
    e.provider, e.owner_generation, e.binding_revision, e.workspace_id, \
    e.workspace_generation, e.access_generation, e.origin, e.generation \
    FROM codex_relay_credentials_v1 c JOIN codex_relay_enrollments_v1 e \
    ON e.enrollment_id = c.enrollment_id AND e.family_id = c.family_id \
    AND e.server_id = c.server_id AND e.environment_id = c.environment_id \
    AND e.installation_id = c.installation_id WHERE c.index_hash = ?";

#[async_trait]
impl RelayEnrollmentStore for SqliteStorage {
    async fn issue_relay_enrollment(
        &self,
        command: IssueRelayEnrollment,
        now: u64,
    ) -> SessionStoreResult<RelayEnrollment> {
        if command.access.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.begin_immediate().await?;
        let family =
            validate_session_family_in_tx(&mut tx, &command.principal_id, &command.access, now)
                .await?;
        validate_relay_grant(&command, &family, 1, now)?;
        validate_session_access_credential_in_tx(&mut tx, &family, &command.access, now).await?;
        let enrollment = RelayEnrollment {
            enrollment_id: command.enrollment_id,
            identity: command.identity,
            lineage: RelayLineage::from_family(&family),
            vendor_ciphertext: command.vendor_ciphertext,
            provider_expires_at_unix_secs: command.provider_expires_at_unix_secs,
            generation: 1,
            updated_at_unix_secs: now,
        };
        insert_enrollment(&mut tx, &enrollment).await?;
        insert_credential(&mut tx, &command.credential).await?;
        tx.commit().await.map_err(map_error)?;
        Ok(enrollment)
    }

    async fn refresh_relay_enrollment(
        &self,
        command: RefreshRelayEnrollment,
        now: u64,
    ) -> SessionStoreResult<RelayEnrollment> {
        let grant = command.grant;
        if grant.access.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.begin_immediate().await?;
        let family =
            validate_session_family_in_tx(&mut tx, &grant.principal_id, &grant.access, now).await?;
        let mut current = read_enrollment(&mut tx, &grant.identity).await?;
        if current.enrollment_id != grant.enrollment_id
            || !current.lineage.matches_family(&current.identity, &family)
            || current.generation != command.expected_generation
        {
            return Err(SessionStoreError::OwnerConflict);
        }
        let generation = current
            .generation
            .checked_add(1)
            .ok_or(SessionStoreError::InvalidOperation)?;
        validate_relay_grant(&grant, &family, generation, now)?;
        validate_session_access_credential_in_tx(&mut tx, &family, &grant.access, now).await?;
        let changed = sqlx::query(
            "UPDATE codex_relay_enrollments_v1 SET vendor_ciphertext = ?, \
             provider_expires_at_unix_secs = ?, generation = ?, updated_at_unix_secs = ? \
             WHERE enrollment_id = ? AND generation = ?",
        )
        .bind(&grant.vendor_ciphertext)
        .bind(integer(grant.provider_expires_at_unix_secs)?)
        .bind(integer(generation)?)
        .bind(integer(now)?)
        .bind(&current.enrollment_id)
        .bind(integer(command.expected_generation)?)
        .execute(&mut *tx)
        .await
        .map_err(map_error)?;
        if changed.rows_affected() != 1 {
            return Err(SessionStoreError::OwnerConflict);
        }
        insert_credential(&mut tx, &grant.credential).await?;
        current.vendor_ciphertext = grant.vendor_ciphertext;
        current.provider_expires_at_unix_secs = grant.provider_expires_at_unix_secs;
        current.generation = generation;
        current.updated_at_unix_secs = now;
        tx.commit().await.map_err(map_error)?;
        Ok(current)
    }

    async fn lookup_relay_credential(
        &self,
        index_hash: &[u8; 32],
    ) -> SessionStoreResult<Option<RelayAuthRecord>> {
        let mut tx = self.pool().begin().await.map_err(map_error)?;
        let Some(row) = sqlx::query(HEADER_SQL)
            .bind(index_hash.as_slice())
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        let credential = credential_row(&row)?;
        let lineage = lineage_row(&row)?;
        if credential.enrollment_generation > number(&row, "generation")? {
            return Ok(None);
        }
        let Some(row) = sqlx::query("SELECT * FROM codex_session_families_v1 WHERE family_id = ?")
            .bind(&credential.identity.family_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        let family = family_row(row)?;
        if !lineage.matches_family(&credential.identity, &family) {
            return Ok(None);
        }
        let Some(row) =
            sqlx::query("SELECT * FROM managed_keys_v1 WHERE principal_id = ? AND key_id = ?")
                .bind(&family.principal_id)
                .bind(&family.parent_key_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(map_error)?
        else {
            return Ok(None);
        };
        let parent_key = parent_row(row)?;
        let Some(row) = sqlx::query("SELECT enabled, deleted_at FROM principals_v1 WHERE id = ?")
            .bind(&family.principal_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        if row
            .try_get::<Option<i64>, _>("deleted_at")
            .map_err(map_error)?
            .is_some()
        {
            return Ok(None);
        }
        let principal_enabled = row.try_get("enabled").map_err(map_error)?;
        let snapshot = lineage.snapshot(&credential);
        tx.commit().await.map_err(map_error)?;
        Ok(Some(RelayAuthRecord {
            credential,
            snapshot,
            family,
            parent_key,
            principal_enabled,
        }))
    }

    async fn lookup_access_relay_enrollment(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        selector: &AccessRelaySelector,
        now: u64,
    ) -> SessionStoreResult<RelayEnrollmentMetadata> {
        if snapshot.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.begin_immediate().await?;
        let family = validate_session_family_in_tx(&mut tx, principal_id, snapshot, now).await?;
        validate_session_access_credential_in_tx(&mut tx, &family, snapshot, now).await?;
        let (server_id, installation_id, environment_id) = match selector {
            AccessRelaySelector::Server { server_id, installation_id } => {
                if server_id.is_empty() || installation_id.is_empty() {
                    return Err(SessionStoreError::InvalidOperation);
                }
                (Some(server_id.as_str()), Some(installation_id.as_str()), None)
            }
            AccessRelaySelector::Environment { environment_id } => {
                if environment_id.is_empty() {
                    return Err(SessionStoreError::InvalidOperation);
                }
                (None, None, Some(environment_id.as_str()))
            }
        };
        let rows = sqlx::query(
            "SELECT enrollment_id, family_id, server_id, environment_id, \
             installation_id, principal_id, parent_key_id, owner_binding_id, provider, \
             owner_generation, binding_revision, workspace_id, workspace_generation, \
             access_generation, origin, provider_expires_at_unix_secs, generation, \
             updated_at_unix_secs FROM codex_relay_enrollments_v1 \
             WHERE family_id = ? AND ((server_id = ? AND installation_id = ?) \
             OR environment_id = ?) LIMIT 2",
        )
        .bind(&family.family_id)
        .bind(server_id)
        .bind(installation_id)
        .bind(environment_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(map_error)?;
        if rows.len() != 1 {
            return Err(SessionStoreError::OwnerConflict);
        }
        let row = &rows[0];
        let metadata = RelayEnrollmentMetadata {
            enrollment_id: row.try_get("enrollment_id").map_err(map_error)?,
            identity: identity_row(row)?,
            lineage: lineage_row(row)?,
            provider_expires_at_unix_secs: number(row, "provider_expires_at_unix_secs")?,
            generation: number(row, "generation")?,
            updated_at_unix_secs: number(row, "updated_at_unix_secs")?,
        };
        if !metadata.lineage.matches_family(&metadata.identity, &family) {
            return Err(SessionStoreError::OwnerConflict);
        }
        tx.commit().await.map_err(map_error)?;
        Ok(metadata)
    }

    async fn fence_relay_dispatch(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        now: u64,
    ) -> SessionStoreResult<RelayDispatch> {
        if snapshot.purpose != CredentialPurpose::RemoteRelay {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.begin_immediate().await?;
        let family = validate_session_family_in_tx(&mut tx, principal_id, snapshot, now).await?;
        let row = sqlx::query(
            "SELECT * FROM codex_relay_credentials_v1 \
             WHERE credential_id = ? AND family_id = ?",
        )
        .bind(&snapshot.credential_id)
        .bind(&family.family_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_error)?
        .ok_or(SessionStoreError::CredentialRejected)?;
        let credential = credential_row(&row)?;
        let enrollment = read_enrollment(&mut tx, &credential.identity).await?;
        if enrollment.enrollment_id != credential.enrollment_id {
            return Err(SessionStoreError::CredentialRejected);
        }
        validate_relay_dispatch(&enrollment, &credential, snapshot, &family, now)?;
        let result = if now >= enrollment.provider_expires_at_unix_secs {
            RelayDispatch::ProviderExpired
        } else {
            RelayDispatch::Authorized(Box::new(enrollment))
        };
        tx.commit().await.map_err(map_error)?;
        Ok(result)
    }
}

async fn read_enrollment(
    tx: &mut Transaction<'_, Sqlite>,
    identity: &RelayEnrollmentIdentity,
) -> SessionStoreResult<RelayEnrollment> {
    let row = sqlx::query(
        "SELECT * FROM codex_relay_enrollments_v1 \
         WHERE family_id = ? AND server_id = ? AND environment_id = ? AND installation_id = ?",
    )
    .bind(&identity.family_id)
    .bind(&identity.server_id)
    .bind(&identity.environment_id)
    .bind(&identity.installation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(map_error)?
    .ok_or(SessionStoreError::CredentialRejected)?;
    Ok(RelayEnrollment {
        enrollment_id: row.try_get("enrollment_id").map_err(map_error)?,
        identity: identity_row(&row)?,
        lineage: lineage_row(&row)?,
        vendor_ciphertext: row.try_get("vendor_ciphertext").map_err(map_error)?,
        provider_expires_at_unix_secs: number(&row, "provider_expires_at_unix_secs")?,
        generation: number(&row, "generation")?,
        updated_at_unix_secs: number(&row, "updated_at_unix_secs")?,
    })
}

async fn insert_enrollment(
    tx: &mut Transaction<'_, Sqlite>,
    e: &RelayEnrollment,
) -> SessionStoreResult<()> {
    let l = &e.lineage;
    sqlx::query(
        "INSERT INTO codex_relay_enrollments_v1 (enrollment_id, family_id, server_id, \
         environment_id, installation_id, principal_id, parent_key_id, owner_binding_id, \
         provider, owner_generation, binding_revision, workspace_id, workspace_generation, \
         access_generation, origin, vendor_ciphertext, provider_expires_at_unix_secs, \
         generation, updated_at_unix_secs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ).bind(&e.enrollment_id).bind(&e.identity.family_id).bind(&e.identity.server_id)
        .bind(&e.identity.environment_id).bind(&e.identity.installation_id)
        .bind(&l.principal_id).bind(&l.parent_key_id).bind(&l.owner_binding_id).bind(&l.provider)
        .bind(integer(l.owner_generation)?).bind(integer(l.binding_revision)?).bind(&l.workspace_id)
        .bind(integer(l.workspace_generation)?).bind(integer(l.access_generation)?).bind(&l.origin)
        .bind(&e.vendor_ciphertext).bind(integer(e.provider_expires_at_unix_secs)?)
        .bind(integer(e.generation)?).bind(integer(e.updated_at_unix_secs)?)
        .execute(&mut **tx).await.map_err(map_error)?;
    Ok(())
}

async fn insert_credential(
    tx: &mut Transaction<'_, Sqlite>,
    c: &RelayCredential,
) -> SessionStoreResult<()> {
    sqlx::query(
        "INSERT INTO codex_relay_credentials_v1 (credential_id, enrollment_id, family_id, \
         server_id, environment_id, installation_id, enrollment_generation, index_hash, \
         verify_hash, salt, issued_at_unix_secs, expires_at_unix_secs) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&c.credential_id)
    .bind(&c.enrollment_id)
    .bind(&c.identity.family_id)
    .bind(&c.identity.server_id)
    .bind(&c.identity.environment_id)
    .bind(&c.identity.installation_id)
    .bind(integer(c.enrollment_generation)?)
    .bind(c.index_hash.as_slice())
    .bind(c.verify_hash.as_slice())
    .bind(c.salt.as_slice())
    .bind(integer(c.issued_at_unix_secs)?)
    .bind(integer(c.expires_at_unix_secs)?)
    .execute(&mut **tx)
    .await
    .map_err(map_error)?;
    Ok(())
}

fn identity_row(row: &SqliteRow) -> SessionStoreResult<RelayEnrollmentIdentity> {
    Ok(RelayEnrollmentIdentity {
        family_id: row.try_get("family_id").map_err(map_error)?,
        server_id: row.try_get("server_id").map_err(map_error)?,
        environment_id: row.try_get("environment_id").map_err(map_error)?,
        installation_id: row.try_get("installation_id").map_err(map_error)?,
    })
}

fn lineage_row(row: &SqliteRow) -> SessionStoreResult<RelayLineage> {
    Ok(RelayLineage {
        principal_id: row.try_get("principal_id").map_err(map_error)?,
        parent_key_id: row.try_get("parent_key_id").map_err(map_error)?,
        owner_binding_id: row.try_get("owner_binding_id").map_err(map_error)?,
        provider: row.try_get("provider").map_err(map_error)?,
        owner_generation: number(row, "owner_generation")?,
        binding_revision: number(row, "binding_revision")?,
        workspace_id: row.try_get("workspace_id").map_err(map_error)?,
        workspace_generation: number(row, "workspace_generation")?,
        access_generation: number(row, "access_generation")?,
        origin: row.try_get("origin").map_err(map_error)?,
    })
}

fn credential_row(row: &SqliteRow) -> SessionStoreResult<RelayCredential> {
    Ok(RelayCredential {
        credential_id: row.try_get("credential_id").map_err(map_error)?,
        enrollment_id: row.try_get("enrollment_id").map_err(map_error)?,
        identity: identity_row(row)?,
        enrollment_generation: number(row, "enrollment_generation")?,
        index_hash: bytes(row, "index_hash")?,
        verify_hash: bytes(row, "verify_hash")?,
        salt: bytes(row, "salt")?,
        issued_at_unix_secs: number(row, "issued_at_unix_secs")?,
        expires_at_unix_secs: number(row, "expires_at_unix_secs")?,
    })
}

fn integer(value: u64) -> SessionStoreResult<i64> {
    i64::try_from(value).map_err(|_| SessionStoreError::InvalidOperation)
}

fn number(row: &SqliteRow, name: &str) -> SessionStoreResult<u64> {
    u64::try_from(row.try_get::<i64, _>(name).map_err(map_error)?).map_err(|_| {
        StorageError::Corrupted {
            message: format!("negative relay {name}"),
        }
        .into()
    })
}

fn bytes<const N: usize>(row: &SqliteRow, name: &str) -> SessionStoreResult<[u8; N]> {
    row.try_get::<Vec<u8>, _>(name)
        .map_err(map_error)?
        .try_into()
        .map_err(|_| {
            StorageError::Corrupted {
                message: format!("invalid relay {name} length"),
            }
            .into()
        })
}

fn map_error(error: sqlx::Error) -> StorageError {
    if let sqlx::Error::Database(database) = &error
        && database.is_unique_violation()
    {
        return StorageError::Conflict {
            message: "relay identity or verifier already exists".into(),
        };
    }
    crate::map_sqlx_error(error)
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
