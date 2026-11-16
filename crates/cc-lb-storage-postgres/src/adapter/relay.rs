use async_trait::async_trait;
use cc_lb_storage_api::{
    RelayCredential, RelayEnrollment, RelayEnrollmentIdentity, RelayEnrollmentStore, StorageError,
    StorageResult, validate_relay_credential, validate_relay_enrollment,
};
use sqlx::{Row, postgres::PgRow};

use crate::{adapter::PostgresStorage, error_map::map_sqlx_error};

#[async_trait]
impl RelayEnrollmentStore for PostgresStorage {
    async fn get_relay_enrollment(
        &self,
        identity: &RelayEnrollmentIdentity,
    ) -> StorageResult<Option<RelayEnrollment>> {
        identity.validate()?;
        let row = sqlx::query(
            "SELECT family_id, server_id, environment_id, installation_id, principal_id,
                    owner_epoch, vendor_ciphertext, provider_expires_at_unix_secs,
                    revision, updated_at_unix_secs
             FROM relay_enrollments_v1
             WHERE family_id = $1 AND server_id = $2 AND environment_id = $3 AND installation_id = $4",
        )
        .bind(&identity.family_id)
        .bind(&identity.server_id)
        .bind(&identity.environment_id)
        .bind(&identity.installation_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        row.map(enrollment_from_row).transpose()
    }

    async fn compare_and_swap_relay_enrollment(
        &self,
        enrollment: &RelayEnrollment,
        expected_revision: Option<u64>,
    ) -> StorageResult<()> {
        validate_relay_enrollment(enrollment)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
        let result = if let Some(expected) = expected_revision {
            sqlx::query(
                "UPDATE relay_enrollments_v1
                 SET vendor_ciphertext = $1, provider_expires_at_unix_secs = $2,
                     revision = $3, updated_at_unix_secs = $4
                 WHERE family_id = $5 AND server_id = $6 AND environment_id = $7
                   AND installation_id = $8 AND revision = $9
                   AND principal_id = $10 AND owner_epoch = $11",
            )
            .bind(&enrollment.vendor_ciphertext)
            .bind(to_i64(
                enrollment.provider_expires_at_unix_secs,
                "provider_expires_at_unix_secs",
            )?)
            .bind(to_i64(enrollment.revision, "revision")?)
            .bind(to_i64(
                enrollment.updated_at_unix_secs,
                "updated_at_unix_secs",
            )?)
            .bind(&enrollment.identity.family_id)
            .bind(&enrollment.identity.server_id)
            .bind(&enrollment.identity.environment_id)
            .bind(&enrollment.identity.installation_id)
            .bind(to_i64(expected, "expected_revision")?)
            .bind(&enrollment.principal_id)
            .bind(to_i64(enrollment.owner_epoch, "owner_epoch")?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
        } else {
            sqlx::query(
                "INSERT INTO relay_enrollments_v1
                 (family_id, server_id, environment_id, installation_id, principal_id,
                  owner_epoch, vendor_ciphertext, provider_expires_at_unix_secs,
                  revision, updated_at_unix_secs)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            )
            .bind(&enrollment.identity.family_id)
            .bind(&enrollment.identity.server_id)
            .bind(&enrollment.identity.environment_id)
            .bind(&enrollment.identity.installation_id)
            .bind(&enrollment.principal_id)
            .bind(to_i64(enrollment.owner_epoch, "owner_epoch")?)
            .bind(&enrollment.vendor_ciphertext)
            .bind(to_i64(
                enrollment.provider_expires_at_unix_secs,
                "provider_expires_at_unix_secs",
            )?)
            .bind(to_i64(enrollment.revision, "revision")?)
            .bind(to_i64(
                enrollment.updated_at_unix_secs,
                "updated_at_unix_secs",
            )?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_error)?
        };
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict {
                message: "relay enrollment revision conflict".to_owned(),
            });
        }
        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn insert_relay_credential(&self, credential: &RelayCredential) -> StorageResult<()> {
        validate_relay_credential(credential)?;
        sqlx::query(
            "INSERT INTO relay_credentials_v1
             (credential_id, family_id, server_id, environment_id, installation_id,
              principal_id, owner_epoch, enrollment_revision, verifier,
              issued_at_unix_secs, expires_at_unix_secs)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(&credential.credential_id)
        .bind(&credential.identity.family_id)
        .bind(&credential.identity.server_id)
        .bind(&credential.identity.environment_id)
        .bind(&credential.identity.installation_id)
        .bind(&credential.principal_id)
        .bind(to_i64(credential.owner_epoch, "owner_epoch")?)
        .bind(to_i64(
            credential.enrollment_revision,
            "enrollment_revision",
        )?)
        .bind(&credential.verifier)
        .bind(to_i64(
            credential.issued_at_unix_secs,
            "issued_at_unix_secs",
        )?)
        .bind(to_i64(
            credential.expires_at_unix_secs,
            "expires_at_unix_secs",
        )?)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn find_relay_credential(
        &self,
        verifier: &[u8],
        now_unix_secs: u64,
    ) -> StorageResult<Option<RelayCredential>> {
        let row = sqlx::query(
            "SELECT credential_id, family_id, server_id, environment_id, installation_id,
                    principal_id, owner_epoch, enrollment_revision, verifier,
                    issued_at_unix_secs, expires_at_unix_secs
             FROM relay_credentials_v1 WHERE verifier = $1 AND expires_at_unix_secs > $2",
        )
        .bind(verifier)
        .bind(to_i64(now_unix_secs, "now_unix_secs")?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;
        row.map(credential_from_row).transpose()
    }
}

fn enrollment_from_row(row: PgRow) -> StorageResult<RelayEnrollment> {
    Ok(RelayEnrollment {
        identity: identity_from_row(&row)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        owner_epoch: from_i64(
            row.try_get("owner_epoch").map_err(map_sqlx_error)?,
            "owner_epoch",
        )?,
        vendor_ciphertext: row.try_get("vendor_ciphertext").map_err(map_sqlx_error)?,
        provider_expires_at_unix_secs: from_i64(
            row.try_get("provider_expires_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "provider_expires_at_unix_secs",
        )?,
        revision: from_i64(row.try_get("revision").map_err(map_sqlx_error)?, "revision")?,
        updated_at_unix_secs: from_i64(
            row.try_get("updated_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "updated_at_unix_secs",
        )?,
    })
}

fn credential_from_row(row: PgRow) -> StorageResult<RelayCredential> {
    Ok(RelayCredential {
        credential_id: row.try_get("credential_id").map_err(map_sqlx_error)?,
        identity: identity_from_row(&row)?,
        principal_id: row.try_get("principal_id").map_err(map_sqlx_error)?,
        owner_epoch: from_i64(
            row.try_get("owner_epoch").map_err(map_sqlx_error)?,
            "owner_epoch",
        )?,
        enrollment_revision: from_i64(
            row.try_get("enrollment_revision").map_err(map_sqlx_error)?,
            "enrollment_revision",
        )?,
        verifier: row.try_get("verifier").map_err(map_sqlx_error)?,
        issued_at_unix_secs: from_i64(
            row.try_get("issued_at_unix_secs").map_err(map_sqlx_error)?,
            "issued_at_unix_secs",
        )?,
        expires_at_unix_secs: from_i64(
            row.try_get("expires_at_unix_secs")
                .map_err(map_sqlx_error)?,
            "expires_at_unix_secs",
        )?,
    })
}

fn identity_from_row(row: &PgRow) -> StorageResult<RelayEnrollmentIdentity> {
    Ok(RelayEnrollmentIdentity {
        family_id: row.try_get("family_id").map_err(map_sqlx_error)?,
        server_id: row.try_get("server_id").map_err(map_sqlx_error)?,
        environment_id: row.try_get("environment_id").map_err(map_sqlx_error)?,
        installation_id: row.try_get("installation_id").map_err(map_sqlx_error)?,
    })
}

fn to_i64(value: u64, field: &str) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: field.to_owned(),
        reason: "exceeds PostgreSQL bigint range".to_owned(),
    })
}

fn from_i64(value: i64, field: &str) -> StorageResult<u64> {
    u64::try_from(value).map_err(|_| StorageError::Corrupted {
        message: format!("{field} is negative"),
    })
}
