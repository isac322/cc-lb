//! Durable storage vocabulary for Codex remote-control relay enrollment.
//!
//! Enrollment rows own the current provider ciphertext. Credential rows are local
//! verifier records: they deliberately carry no provider ciphertext or revocation
//! marker and remain usable until their own expiry.

use std::fmt;

use async_trait::async_trait;

use crate::{StorageError, StorageResult};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayEnrollmentIdentity {
    pub family_id: String,
    pub server_id: String,
    pub environment_id: String,
    pub installation_id: String,
}

impl RelayEnrollmentIdentity {
    pub fn validate(&self) -> StorageResult<()> {
        for (field, value) in [
            ("family_id", &self.family_id),
            ("server_id", &self.server_id),
            ("environment_id", &self.environment_id),
            ("installation_id", &self.installation_id),
        ] {
            if value.trim().is_empty() {
                return Err(StorageError::InvalidInput {
                    field: field.to_owned(),
                    reason: "must not be empty".to_owned(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RelayEnrollment {
    pub identity: RelayEnrollmentIdentity,
    pub principal_id: String,
    pub owner_epoch: u64,
    pub vendor_ciphertext: Vec<u8>,
    pub provider_expires_at_unix_secs: u64,
    pub revision: u64,
    pub updated_at_unix_secs: u64,
}

impl fmt::Debug for RelayEnrollment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayEnrollment")
            .field("identity", &self.identity)
            .field("principal_id", &self.principal_id)
            .field("owner_epoch", &self.owner_epoch)
            .field("vendor_ciphertext", &"[redacted]")
            .field(
                "provider_expires_at_unix_secs",
                &self.provider_expires_at_unix_secs,
            )
            .field("revision", &self.revision)
            .field("updated_at_unix_secs", &self.updated_at_unix_secs)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RelayCredential {
    pub credential_id: String,
    pub identity: RelayEnrollmentIdentity,
    pub principal_id: String,
    pub owner_epoch: u64,
    pub enrollment_revision: u64,
    pub verifier: Vec<u8>,
    pub issued_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
}

impl fmt::Debug for RelayCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayCredential")
            .field("credential_id", &self.credential_id)
            .field("identity", &self.identity)
            .field("principal_id", &self.principal_id)
            .field("owner_epoch", &self.owner_epoch)
            .field("enrollment_revision", &self.enrollment_revision)
            .field("verifier", &"[redacted]")
            .field("issued_at_unix_secs", &self.issued_at_unix_secs)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .finish()
    }
}

#[async_trait]
pub trait RelayEnrollmentStore: Send + Sync {
    async fn get_relay_enrollment(
        &self,
        identity: &RelayEnrollmentIdentity,
    ) -> StorageResult<Option<RelayEnrollment>>;

    /// Creates or replaces the current enrollment using a revision CAS.
    /// `expected_revision = None` creates a new row; `Some` replaces only that revision.
    async fn compare_and_swap_relay_enrollment(
        &self,
        enrollment: &RelayEnrollment,
        expected_revision: Option<u64>,
    ) -> StorageResult<()>;

    /// Inserts a local verifier row. Existing credentials are never revoked or updated.
    async fn insert_relay_credential(&self, credential: &RelayCredential) -> StorageResult<()>;

    /// Looks up an unexpired verifier and removes no rows. Expiry is checked in SQL.
    async fn find_relay_credential(
        &self,
        verifier: &[u8],
        now_unix_secs: u64,
    ) -> StorageResult<Option<RelayCredential>>;
}

pub fn validate_relay_enrollment(enrollment: &RelayEnrollment) -> StorageResult<()> {
    enrollment.identity.validate()?;
    if enrollment.principal_id.trim().is_empty() {
        return Err(StorageError::InvalidInput {
            field: "principal_id".to_owned(),
            reason: "must not be empty".to_owned(),
        });
    }
    if enrollment.owner_epoch == 0 || enrollment.revision == 0 {
        return Err(StorageError::InvalidInput {
            field: "owner_epoch/revision".to_owned(),
            reason: "must be greater than zero".to_owned(),
        });
    }
    if enrollment.vendor_ciphertext.is_empty() {
        return Err(StorageError::InvalidInput {
            field: "vendor_ciphertext".to_owned(),
            reason: "must not be empty".to_owned(),
        });
    }
    if enrollment.provider_expires_at_unix_secs == 0 {
        return Err(StorageError::InvalidInput {
            field: "provider_expires_at_unix_secs".to_owned(),
            reason: "must be greater than zero".to_owned(),
        });
    }
    Ok(())
}

pub fn validate_relay_credential(credential: &RelayCredential) -> StorageResult<()> {
    credential.identity.validate()?;
    for (field, value) in [
        ("credential_id", &credential.credential_id),
        ("principal_id", &credential.principal_id),
    ] {
        if value.trim().is_empty() {
            return Err(StorageError::InvalidInput {
                field: field.to_owned(),
                reason: "must not be empty".to_owned(),
            });
        }
    }
    if credential.owner_epoch == 0 || credential.enrollment_revision == 0 {
        return Err(StorageError::InvalidInput {
            field: "owner_epoch/enrollment_revision".to_owned(),
            reason: "must be greater than zero".to_owned(),
        });
    }
    if credential.verifier.is_empty() {
        return Err(StorageError::InvalidInput {
            field: "verifier".to_owned(),
            reason: "must not be empty".to_owned(),
        });
    }
    if credential.expires_at_unix_secs <= credential.issued_at_unix_secs {
        return Err(StorageError::InvalidInput {
            field: "expires_at_unix_secs".to_owned(),
            reason: "must be after issued_at_unix_secs".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> RelayEnrollmentIdentity {
        RelayEnrollmentIdentity {
            family_id: "family".into(),
            server_id: "server".into(),
            environment_id: "environment".into(),
            installation_id: "installation".into(),
        }
    }

    fn enrollment() -> RelayEnrollment {
        RelayEnrollment {
            identity: identity(),
            principal_id: "principal".into(),
            owner_epoch: 1,
            vendor_ciphertext: vec![0x42],
            provider_expires_at_unix_secs: 200,
            revision: 1,
            updated_at_unix_secs: 100,
        }
    }

    #[test]
    fn enrollment_debug_redacts_vendor_ciphertext() {
        let debug = format!("{:?}", enrollment());

        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("42"));
    }

    #[test]
    fn enrollment_requires_provider_expiry() {
        let mut value = enrollment();
        value.provider_expires_at_unix_secs = 0;

        assert!(validate_relay_enrollment(&value).is_err());
    }

    #[test]
    fn credential_requires_strictly_positive_lifetime() {
        let credential = RelayCredential {
            credential_id: "credential".into(),
            identity: identity(),
            principal_id: "principal".into(),
            owner_epoch: 1,
            enrollment_revision: 1,
            verifier: vec![0x24],
            issued_at_unix_secs: 100,
            expires_at_unix_secs: 100,
        };

        assert!(validate_relay_credential(&credential).is_err());
    }

    #[test]
    fn identity_rejects_blank_components() {
        let mut value = identity();
        value.environment_id = " ".into();

        assert!(value.validate().is_err());
    }
}
