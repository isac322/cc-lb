//! Atomic persistence for remote-control enrollment and local relay verifiers.
//! Header lookup never returns provider ciphertext or performs owner discovery.

use std::fmt;

use async_trait::async_trait;

use crate::{
    StorageError, StorageResult, StoredApiKeyRecord,
    codex_sessions::{
        CredentialPurpose, SessionFamily, SessionSnapshot, SessionStoreError, SessionStoreResult,
    },
};

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

/// Issue-time authority. Refresh never rewrites these fields, even after renewal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayLineage {
    pub principal_id: String,
    pub parent_key_id: String,
    pub owner_binding_id: String,
    pub provider: String,
    pub owner_generation: u64,
    pub binding_revision: u64,
    pub workspace_id: String,
    pub workspace_generation: u64,
    pub access_generation: u64,
    pub origin: String,
}

impl RelayLineage {
    pub fn from_family(family: &SessionFamily) -> Self {
        Self {
            principal_id: family.principal_id.clone(),
            parent_key_id: family.parent_key_id.clone(),
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

    pub fn matches_family(
        &self,
        identity: &RelayEnrollmentIdentity,
        family: &SessionFamily,
    ) -> bool {
        identity.family_id == family.family_id
            && self.principal_id == family.principal_id
            && self.parent_key_id == family.parent_key_id
            && self.owner_binding_id == family.owner_binding_id
            && self.provider == family.provider
            && self.owner_generation == family.owner_generation
            && self.binding_revision == family.binding_revision
            && self.workspace_id == family.workspace_id
            && self.workspace_generation == family.workspace_generation
            && self.access_generation <= family.access_generation
            && self.origin == family.origin
    }

    pub fn matches_snapshot(
        &self,
        credential: &RelayCredential,
        snapshot: &SessionSnapshot,
    ) -> bool {
        snapshot.family_id == credential.identity.family_id
            && snapshot.credential_id == credential.credential_id
            && snapshot.purpose == CredentialPurpose::RemoteRelay
            && snapshot.parent_key_id == self.parent_key_id
            && snapshot.owner_binding_id == self.owner_binding_id
            && snapshot.provider == self.provider
            && snapshot.owner_generation == self.owner_generation
            && snapshot.binding_revision == self.binding_revision
            && snapshot.workspace_id == self.workspace_id
            && snapshot.workspace_generation == self.workspace_generation
            && snapshot.access_generation == self.access_generation
            && snapshot.origin == self.origin
    }

    pub fn snapshot(&self, credential: &RelayCredential) -> SessionSnapshot {
        SessionSnapshot {
            family_id: credential.identity.family_id.clone(),
            parent_key_id: self.parent_key_id.clone(),
            credential_id: credential.credential_id.clone(),
            purpose: CredentialPurpose::RemoteRelay,
            owner_binding_id: self.owner_binding_id.clone(),
            provider: self.provider.clone(),
            owner_generation: self.owner_generation,
            binding_revision: self.binding_revision,
            workspace_id: self.workspace_id.clone(),
            workspace_generation: self.workspace_generation,
            access_generation: self.access_generation,
            origin: self.origin.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RelayEnrollment {
    pub enrollment_id: String,
    pub identity: RelayEnrollmentIdentity,
    pub lineage: RelayLineage,
    pub vendor_ciphertext: Vec<u8>,
    pub provider_expires_at_unix_secs: u64,
    pub generation: u64,
    pub updated_at_unix_secs: u64,
}

impl fmt::Debug for RelayEnrollment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayEnrollment")
            .field("enrollment_id", &self.enrollment_id)
            .field("identity", &self.identity)
            .field("lineage", &self.lineage)
            .field("vendor_ciphertext", &"[redacted]")
            .field(
                "provider_expires_at_unix_secs",
                &self.provider_expires_at_unix_secs,
            )
            .field("generation", &self.generation)
            .field("updated_at_unix_secs", &self.updated_at_unix_secs)
            .finish()
    }
}

/// No bearer plaintext or provider ciphertext is persisted in a credential row.
#[derive(Clone, PartialEq, Eq)]
pub struct RelayCredential {
    pub credential_id: String,
    pub enrollment_id: String,
    pub identity: RelayEnrollmentIdentity,
    pub enrollment_generation: u64,
    pub index_hash: [u8; 32],
    pub verify_hash: [u8; 32],
    pub salt: [u8; 16],
    pub issued_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
}

impl fmt::Debug for RelayCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayCredential")
            .field("credential_id", &self.credential_id)
            .field("enrollment_id", &self.enrollment_id)
            .field("identity", &self.identity)
            .field("enrollment_generation", &self.enrollment_generation)
            .field("verifier", &"[redacted]")
            .field("issued_at_unix_secs", &self.issued_at_unix_secs)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .finish()
    }
}

/// Account-authenticated selectors are equality filters within the stored
/// session family; they never establish ownership or release relay ciphertext.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessRelaySelector {
    Server { server_id: String, installation_id: String },
    Environment { environment_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayEnrollmentMetadata {
    pub enrollment_id: String,
    pub identity: RelayEnrollmentIdentity,
    pub lineage: RelayLineage,
    pub provider_expires_at_unix_secs: u64,
    pub generation: u64,
    pub updated_at_unix_secs: u64,
}

pub struct IssueRelayEnrollment {
    pub principal_id: String,
    pub access: SessionSnapshot,
    pub enrollment_id: String,
    pub identity: RelayEnrollmentIdentity,
    pub vendor_ciphertext: Vec<u8>,
    pub provider_expires_at_unix_secs: u64,
    pub credential: RelayCredential,
}

pub struct RefreshRelayEnrollment {
    pub grant: IssueRelayEnrollment,
    pub expected_generation: u64,
}

/// Durable header-authentication snapshot; deliberately has no ciphertext/expiry
/// from the provider. The verifier must check the full bearer and local lifetimes.
pub struct RelayAuthRecord {
    pub credential: RelayCredential,
    pub snapshot: SessionSnapshot,
    pub family: SessionFamily,
    pub parent_key: StoredApiKeyRecord,
    pub principal_enabled: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RelayDispatch {
    Authorized(RelayEnrollment),
    /// Local expiry knowledge is not a received provider HTTP response. No
    /// ciphertext is released, and callers must not retry or renew local auth.
    ProviderExpired,
}

#[async_trait]
pub trait RelayEnrollmentStore: Send + Sync {
    /// Validates live access authority and inserts enrollment + verifier atomically.
    async fn issue_relay_enrollment(
        &self,
        command: IssueRelayEnrollment,
        now: u64,
    ) -> SessionStoreResult<RelayEnrollment>;

    /// Rotates current ciphertext and inserts a verifier in the same transaction.
    /// Prior verifier rows remain immutable and use the current enrollment.
    async fn refresh_relay_enrollment(
        &self,
        command: RefreshRelayEnrollment,
        now: u64,
    ) -> SessionStoreResult<RelayEnrollment>;

    /// No owner discovery and no provider ciphertext projection. Frozen lineage
    /// must match the live family before a record is returned.
    async fn lookup_relay_credential(
        &self,
        index_hash: &[u8; 32],
    ) -> SessionStoreResult<Option<RelayAuthRecord>>;

    /// Resolves one stored enrollment under a live SessionAccess family/owner
    /// fence. Metadata only: account client list/revoke and refresh do not need
    /// provider relay ciphertext. Provider expiry must not prevent refresh.
    async fn lookup_access_relay_enrollment(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        selector: &AccessRelaySelector,
        now: u64,
    ) -> SessionStoreResult<RelayEnrollmentMetadata>;

    /// Derives enrollment identity from the authenticated credential/family,
    /// checks all frozen stamps and live owner authority, and only then releases
    /// server-only ciphertext. Client identity selectors are not authority.
    async fn fence_relay_dispatch(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        now: u64,
    ) -> SessionStoreResult<RelayDispatch>;
}

/// Validate caller-supplied material before any enrollment mutation.
pub fn validate_relay_grant(
    command: &IssueRelayEnrollment,
    family: &SessionFamily,
    generation: u64,
    now: u64,
) -> SessionStoreResult<()> {
    let c = &command.credential;
    command
        .identity
        .validate()
        .map_err(|_| SessionStoreError::InvalidOperation)?;
    if command.enrollment_id.trim().is_empty()
        || c.credential_id.trim().is_empty()
        || command.identity.family_id != family.family_id
        || c.identity != command.identity
        || c.enrollment_id != command.enrollment_id
        || c.enrollment_generation != generation
        || generation == 0
        || command.vendor_ciphertext.is_empty()
        || command.provider_expires_at_unix_secs <= now
        || c.issued_at_unix_secs != now
        || c.expires_at_unix_secs <= now
        || c.expires_at_unix_secs > family.expires_at
        || c.expires_at_unix_secs > command.provider_expires_at_unix_secs
    {
        return Err(SessionStoreError::InvalidOperation);
    }
    Ok(())
}

pub fn validate_relay_dispatch(
    enrollment: &RelayEnrollment,
    credential: &RelayCredential,
    snapshot: &SessionSnapshot,
    family: &SessionFamily,
    now: u64,
) -> SessionStoreResult<()> {
    if snapshot.purpose != CredentialPurpose::RemoteRelay
        || credential.identity != enrollment.identity
        || credential.enrollment_id != enrollment.enrollment_id
        || credential.enrollment_generation == 0
        || credential.enrollment_generation > enrollment.generation
        || !enrollment.lineage.matches_snapshot(credential, snapshot)
        || !enrollment
            .lineage
            .matches_family(&enrollment.identity, family)
        || now < credential.issued_at_unix_secs
        || now >= credential.expires_at_unix_secs
        || credential.expires_at_unix_secs > family.expires_at
    {
        return Err(SessionStoreError::CredentialRejected);
    }
    Ok(())
}
