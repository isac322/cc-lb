//! Local Codex session authority. Token claims are never an authorization source.

use async_trait::async_trait;

use crate::{StorageError, StoredApiKeyRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialPurpose {
    PrincipalKey,
    SessionAccess,
    SessionRenewal,
    RemoteRelay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub family_id: String,
    pub parent_key_id: String,
    pub credential_id: String,
    pub purpose: CredentialPurpose,
    pub owner_binding_id: String,
    pub provider: String,
    pub owner_generation: u64,
    pub binding_revision: u64,
    pub workspace_id: String,
    pub workspace_generation: u64,
    pub access_generation: u64,
    pub origin: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFamily {
    pub family_id: String,
    pub principal_id: String,
    pub parent_key_id: String,
    pub owner_binding_id: String,
    pub provider: String,
    pub owner_generation: u64,
    pub binding_revision: u64,
    pub workspace_id: String,
    pub workspace_generation: u64,
    pub access_generation: u64,
    pub local_user_id: String,
    pub plan_type: String,
    pub origin: String,
    pub access_ttl_secs: u64,
    pub created_at: u64,
    pub expires_at: u64,
    pub revoked_at: Option<u64>,
}

/// Only verifier material crosses the storage boundary, never bearer plaintext.
#[derive(Clone)]
pub struct SessionCredential {
    pub credential_id: String,
    pub family_id: String,
    pub purpose: CredentialPurpose,
    pub index_hash: [u8; 32],
    pub verify_hash: [u8; 32],
    pub salt: [u8; 16],
    pub workspace_generation: u64,
    pub access_generation: u64,
    pub issued_at: u64,
    pub expires_at: u64,
    pub revoked_at: Option<u64>,
}

/// Header-authentication lookup includes lineage but performs no owner discovery.
pub struct SessionAuthRecord {
    pub credential: SessionCredential,
    pub family: SessionFamily,
    pub parent_key: StoredApiKeyRecord,
    pub principal_enabled: bool,
}

pub struct IssueSession {
    pub family: SessionFamily,
    pub access: SessionCredential,
    pub renewal: SessionCredential,
}

/// The stored family must still match every authority field when this commits.
/// The new token is minted from this snapshot before the transaction; a conflict
/// discards it rather than publishing claims for a different workspace.
pub struct ReplaceSessionAccess {
    pub expected: SessionFamily,
    pub authorizing_credential_id: String,
    pub access: SessionCredential,
    pub target_owner: Option<crate::account_owner::OwnerBindingExpectation>,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("session credential rejected")]
    CredentialRejected,
    #[error("principal disabled")]
    PrincipalDisabled,
    #[error("session owner authority changed")]
    OwnerConflict,
    #[error("invalid session operation")]
    InvalidOperation,
}

pub type SessionStoreResult<T> = Result<T, SessionStoreError>;

/// Implementations validate live principal, parent and family lineage inside
/// each transaction. Owner validation is deliberately separate from the header
/// lookup and runs before issuance or new dispatch, under the same transaction.
#[async_trait]
pub trait CodexSessionStore: Send + Sync {
    async fn lookup_session_credential(
        &self,
        index_hash: &[u8; 32],
    ) -> SessionStoreResult<Option<SessionAuthRecord>>;

    async fn issue_session(&self, command: IssueSession) -> SessionStoreResult<()>;

    async fn replace_session_access(
        &self,
        command: ReplaceSessionAccess,
        now: u64,
    ) -> SessionStoreResult<SessionFamily>;

    async fn get_session_family(
        &self,
        family_id: &str,
    ) -> SessionStoreResult<Option<SessionFamily>>;

    async fn revoke_session_family(
        &self,
        family_id: &str,
        renewal_credential_id: &str,
        now: u64,
    ) -> SessionStoreResult<()>;

    async fn fence_session_dispatch(
        &self,
        principal_id: &str,
        snapshot: &SessionSnapshot,
        now: u64,
    ) -> SessionStoreResult<()>;
}
