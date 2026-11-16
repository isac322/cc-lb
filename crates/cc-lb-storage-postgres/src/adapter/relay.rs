use async_trait::async_trait;
use cc_lb_storage_api::{
    StorageError,
    codex_sessions::{CredentialPurpose, SessionSnapshot, SessionStoreError, SessionStoreResult},
    relay::{
        IssueRelayEnrollment, RefreshRelayEnrollment, RelayAuthRecord, RelayCredential,
        RelayDispatch, RelayEnrollment, RelayEnrollmentIdentity, RelayEnrollmentStore,
        RelayLineage, validate_relay_dispatch, validate_relay_grant,
    },
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};

use super::codex_sessions::{
    decode_family, decode_parent, validate_session_access_credential_in_tx,
    validate_session_family_in_tx,
};
use crate::adapter::PostgresStorage;

// One snapshot; neither provider expiry/ciphertext nor owner discovery is involved.
const HEADER_SQL: &str = "SELECT f.*, c.*, e.generation, \
    e.principal_id AS frozen_principal_id, e.parent_key_id AS frozen_parent_key_id, \
    e.owner_binding_id AS frozen_owner_binding_id, e.provider AS frozen_provider, \
    e.owner_generation AS frozen_owner_generation, e.binding_revision AS frozen_binding_revision, \
    e.workspace_id AS frozen_workspace_id, e.workspace_generation AS frozen_workspace_generation, \
    e.access_generation AS frozen_access_generation, e.origin AS frozen_origin, \
    p.enabled AS principal_enabled, \
    k.label AS parent_label, k.issued_at_unix_secs AS parent_issued_at, \
    k.revoked_at_unix_secs AS parent_revoked_at, k.key_hash_b64 AS parent_key_hash_b64, \
    k.verify_hash AS parent_verify_hash, k.secret_salt AS parent_salt, \
    k.limit_overrides AS parent_limit_overrides, k.status AS parent_status, \
    k.expires_at_unix_secs AS parent_expires_at, k.last_4 AS parent_last_4, \
    k.description AS parent_description, k.index_hash AS parent_index_hash \
    FROM codex_relay_credentials_v1 c JOIN codex_relay_enrollments_v1 e \
    ON e.enrollment_id = c.enrollment_id AND e.family_id = c.family_id \
    AND e.server_id = c.server_id AND e.environment_id = c.environment_id \
    AND e.installation_id = c.installation_id \
    JOIN codex_session_families_v1 f ON f.family_id = c.family_id \
    JOIN principals_v1 p ON p.id = f.principal_id::uuid AND p.deleted_at IS NULL \
    JOIN managed_api_keys_v1 k ON k.principal_id = f.principal_id AND k.key_id = f.parent_key_id \
    JOIN managed_api_key_index_v1 i ON i.principal_id = k.principal_id \
    AND i.key_id = k.key_id AND i.index_hash = k.index_hash WHERE c.index_hash = $1";

#[async_trait]
impl RelayEnrollmentStore for PostgresStorage {
    async fn issue_relay_enrollment(
        &self,
        command: IssueRelayEnrollment,
        now: u64,
    ) -> SessionStoreResult<RelayEnrollment> {
        if command.access.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.pool.begin().await.map_err(map_error)?;
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
        let mut tx = self.pool.begin().await.map_err(map_error)?;
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
            "UPDATE codex_relay_enrollments_v1 SET vendor_ciphertext = $1, \
             provider_expires_at_unix_secs = $2, generation = $3, updated_at_unix_secs = $4 \
             WHERE enrollment_id = $5 AND generation = $6",
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
        let Some(row) = sqlx::query(HEADER_SQL)
            .bind(index_hash.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        let credential = credential_row(&row)?;
        let lineage = lineage_row(&row, true)?;
        let family = decode_family(&row)?;
        if credential.enrollment_generation > number(&row, "generation")?
            || !lineage.matches_family(&credential.identity, &family)
        {
            return Ok(None);
        }
        let snapshot = lineage.snapshot(&credential);
        Ok(Some(RelayAuthRecord {
            credential,
            snapshot,
            family,
            parent_key: decode_parent(&row)?,
            principal_enabled: row.try_get("principal_enabled").map_err(map_error)?,
        }))
    }

    async fn fence_relay_dispatch(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        identity: &RelayEnrollmentIdentity,
        now: u64,
    ) -> SessionStoreResult<RelayDispatch> {
        if snapshot.purpose != CredentialPurpose::RemoteRelay
            || snapshot.family_id != identity.family_id
        {
            return Err(SessionStoreError::CredentialRejected);
        }
        let mut tx = self.pool.begin().await.map_err(map_error)?;
        let family = validate_session_family_in_tx(&mut tx, principal_id, snapshot, now).await?;
        let enrollment = read_enrollment(&mut tx, identity).await?;
        let row = sqlx::query(
            "SELECT * FROM codex_relay_credentials_v1 \
             WHERE credential_id = $1 AND enrollment_id = $2 AND family_id = $3 FOR UPDATE",
        )
        .bind(&snapshot.credential_id)
        .bind(&enrollment.enrollment_id)
        .bind(&family.family_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_error)?
        .ok_or(SessionStoreError::CredentialRejected)?;
        let credential = credential_row(&row)?;
        validate_relay_dispatch(&enrollment, &credential, snapshot, &family, now)?;
        let result = if now >= enrollment.provider_expires_at_unix_secs {
            RelayDispatch::ProviderExpired
        } else {
            RelayDispatch::Authorized(enrollment)
        };
        tx.commit().await.map_err(map_error)?;
        Ok(result)
    }
}

async fn read_enrollment(
    tx: &mut Transaction<'_, Postgres>,
    identity: &RelayEnrollmentIdentity,
) -> SessionStoreResult<RelayEnrollment> {
    let row = sqlx::query(
        "SELECT * FROM codex_relay_enrollments_v1 \
         WHERE family_id = $1 AND server_id = $2 AND environment_id = $3 AND installation_id = $4 FOR UPDATE",
    ).bind(&identity.family_id).bind(&identity.server_id).bind(&identity.environment_id)
        .bind(&identity.installation_id).fetch_optional(&mut **tx).await.map_err(map_error)?
        .ok_or(SessionStoreError::CredentialRejected)?;
    Ok(RelayEnrollment {
        enrollment_id: row.try_get("enrollment_id").map_err(map_error)?,
        identity: identity_row(&row)?,
        lineage: lineage_row(&row, false)?,
        vendor_ciphertext: row.try_get("vendor_ciphertext").map_err(map_error)?,
        provider_expires_at_unix_secs: number(&row, "provider_expires_at_unix_secs")?,
        generation: number(&row, "generation")?,
        updated_at_unix_secs: number(&row, "updated_at_unix_secs")?,
    })
}

async fn insert_enrollment(
    tx: &mut Transaction<'_, Postgres>,
    e: &RelayEnrollment,
) -> SessionStoreResult<()> {
    let l = &e.lineage;
    sqlx::query(
        "INSERT INTO codex_relay_enrollments_v1 (enrollment_id, family_id, server_id, \
         environment_id, installation_id, principal_id, parent_key_id, owner_binding_id, \
         provider, owner_generation, binding_revision, workspace_id, workspace_generation, \
         access_generation, origin, vendor_ciphertext, provider_expires_at_unix_secs, \
         generation, updated_at_unix_secs) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)",
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
    tx: &mut Transaction<'_, Postgres>,
    c: &RelayCredential,
) -> SessionStoreResult<()> {
    sqlx::query(
        "INSERT INTO codex_relay_credentials_v1 (credential_id, enrollment_id, family_id, \
         server_id, environment_id, installation_id, enrollment_generation, index_hash, \
         verify_hash, salt, issued_at_unix_secs, expires_at_unix_secs) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
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

fn identity_row(row: &PgRow) -> SessionStoreResult<RelayEnrollmentIdentity> {
    Ok(RelayEnrollmentIdentity {
        family_id: row.try_get("family_id").map_err(map_error)?,
        server_id: row.try_get("server_id").map_err(map_error)?,
        environment_id: row.try_get("environment_id").map_err(map_error)?,
        installation_id: row.try_get("installation_id").map_err(map_error)?,
    })
}

fn lineage_row(row: &PgRow, frozen: bool) -> SessionStoreResult<RelayLineage> {
    Ok(RelayLineage {
        principal_id: row
            .try_get(if frozen {
                "frozen_principal_id"
            } else {
                "principal_id"
            })
            .map_err(map_error)?,
        parent_key_id: row
            .try_get(if frozen {
                "frozen_parent_key_id"
            } else {
                "parent_key_id"
            })
            .map_err(map_error)?,
        owner_binding_id: row
            .try_get(if frozen {
                "frozen_owner_binding_id"
            } else {
                "owner_binding_id"
            })
            .map_err(map_error)?,
        provider: row
            .try_get(if frozen {
                "frozen_provider"
            } else {
                "provider"
            })
            .map_err(map_error)?,
        owner_generation: number(
            row,
            if frozen {
                "frozen_owner_generation"
            } else {
                "owner_generation"
            },
        )?,
        binding_revision: number(
            row,
            if frozen {
                "frozen_binding_revision"
            } else {
                "binding_revision"
            },
        )?,
        workspace_id: row
            .try_get(if frozen {
                "frozen_workspace_id"
            } else {
                "workspace_id"
            })
            .map_err(map_error)?,
        workspace_generation: number(
            row,
            if frozen {
                "frozen_workspace_generation"
            } else {
                "workspace_generation"
            },
        )?,
        access_generation: number(
            row,
            if frozen {
                "frozen_access_generation"
            } else {
                "access_generation"
            },
        )?,
        origin: row
            .try_get(if frozen { "frozen_origin" } else { "origin" })
            .map_err(map_error)?,
    })
}

fn credential_row(row: &PgRow) -> SessionStoreResult<RelayCredential> {
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

fn number(row: &PgRow, name: &str) -> SessionStoreResult<u64> {
    u64::try_from(row.try_get::<i64, _>(name).map_err(map_error)?).map_err(|_| {
        StorageError::Corrupted {
            message: format!("negative relay {name}"),
        }
        .into()
    })
}

fn bytes<const N: usize>(row: &PgRow, name: &str) -> SessionStoreResult<[u8; N]> {
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
    crate::error_map::map_sqlx_error(error)
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
