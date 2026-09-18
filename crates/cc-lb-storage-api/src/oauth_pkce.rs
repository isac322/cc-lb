use async_trait::async_trait;

use crate::StorageResult;

/// Opaque in-flight OAuth PKCE handshake, keyed by the public `state` token.
///
/// `encrypted_payload` is an AEAD ciphertext produced by the admin crate. This
/// store does not interpret it. Postgres persists the row so replicas can
/// complete a handshake started on another instance. SQLite keeps the same
/// record in process memory because that backend is single-instance only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOAuthPkceFlow {
    pub state_token: String,
    pub encrypted_payload: Vec<u8>,
    pub created_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
}

#[async_trait]
pub trait OAuthPkceStore: Send + Sync + 'static {
    /// Insert or replace a handshake. Implementations MAY delete other expired
    /// rows as a side effect.
    async fn put_pkce_flow(&self, flow: &StoredOAuthPkceFlow) -> StorageResult<()>;

    /// Return the handshake when it exists and `expires_at_unix_secs > now`.
    /// Expired rows MUST be treated as missing and SHOULD be deleted.
    async fn get_pkce_flow(
        &self,
        state_token: &str,
        now_unix_secs: u64,
    ) -> StorageResult<Option<StoredOAuthPkceFlow>>;

    /// Delete the handshake. Missing keys are success.
    async fn delete_pkce_flow(&self, state_token: &str) -> StorageResult<()>;
}
