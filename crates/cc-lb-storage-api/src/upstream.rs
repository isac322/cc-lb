use async_trait::async_trait;
use cc_lb_aead::EncryptedOAuthTokens;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

pub use cc_lb_domain::{CredentialKind, Provider};

use crate::StorageResult;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    #[default]
    AnthropicApiKey,
    AnthropicOauth,
    OpenaiApiKey,
    OpenaiOauth,
    GeminiApiKey,
    GeminiOauth,
}

impl UpstreamKind {
    pub const fn provider(self) -> Provider {
        match self {
            Self::AnthropicApiKey | Self::AnthropicOauth => Provider::Anthropic,
            Self::OpenaiApiKey | Self::OpenaiOauth => Provider::Openai,
            Self::GeminiApiKey | Self::GeminiOauth => Provider::Gemini,
        }
    }

    pub const fn credential_kind(self) -> CredentialKind {
        match self {
            Self::AnthropicApiKey | Self::OpenaiApiKey | Self::GeminiApiKey => {
                CredentialKind::ApiKey
            }
            Self::AnthropicOauth | Self::OpenaiOauth | Self::GeminiOauth => CredentialKind::Oauth,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicApiKey => "anthropic_api_key",
            Self::AnthropicOauth => "anthropic_oauth",
            Self::OpenaiApiKey => "openai_api_key",
            Self::OpenaiOauth => "openai_oauth",
            Self::GeminiApiKey => "gemini_api_key",
            Self::GeminiOauth => "gemini_oauth",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamWarmupDialectPlugin {
    pub wasm_registry_id: Uuid,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: UpstreamKind,
    pub base_url: Option<Url>,
    pub enabled: bool,
    pub oauth_credentials: Option<EncryptedOAuthTokens>,
    /// Mirrors `OAuthTokenBundle.never_refresh` for the stored credential.
    ///
    /// Persisted as its own column so list enumeration can filter on the
    /// credential mode without decrypting every upstream's AEAD bundle.
    /// Always `false` for non-OAuth or credential-less upstreams.
    #[serde(default)]
    pub oauth_never_refresh: bool,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub last_apply_error: Option<String>,
    pub last_apply_at_unix_secs: Option<u64>,
    pub deleted_at_unix_secs: Option<u64>,
    pub revision: u64,
    pub oauth_token_generation: u64,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
    #[serde(default)]
    pub warmup_enabled: bool,
    #[serde(default)]
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    /// Wall-clock unix seconds when the most recent successful warmup HTTP call
    /// completed. Used by the admin UI to render "Last cycle ran <X> ago".
    #[serde(default)]
    pub last_warmup_at_unix_secs: Option<u64>,
    /// Anthropic workspace this API-key upstream sends as
    /// `anthropic-workspace-id`. `None` for OAuth upstreams, bound keys, and
    /// org keys used without a workspace.
    #[serde(default)]
    pub workspace_id: Option<String>,
}

impl UpstreamRecord {
    /// Fingerprint of the stored OAuth credential ciphertext. Every
    /// credential write — refresh, reauthorization, replacement — advances
    /// `oauth_token_generation` and produces new ciphertext (random nonce).
    /// The fingerprint identifies the actual ciphertext for state bound to
    /// a specific credential, like the polled `cedar_ember` snapshot.
    pub fn oauth_credential_fingerprint(&self) -> Option<u64> {
        use std::hash::{Hash, Hasher};
        self.oauth_credentials.as_ref().map(|credentials| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            credentials.ciphertext().hash(&mut hasher);
            hasher.finish()
        })
    }
}

/// Initial OAuth credential committed together with a new upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthTokensCreate {
    /// Token bundle sealed with AAD = the upstream id.
    pub tokens: EncryptedOAuthTokens,
    /// Mirrors `OAuthTokenBundle.never_refresh` for the stored credential.
    pub never_refresh: bool,
}

/// Spec and initial credential for a new upstream.
///
/// [`UpstreamStore::create`] commits the spec row and the credential row in
/// one transaction, so no reader ever observes the upstream without its
/// credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamCreate {
    /// Caller-assigned id. Credentials are sealed with AAD = `id` before the
    /// insert, so the id must exist before storage sees the record. A
    /// duplicate id is rejected with [`crate::StorageError::Conflict`].
    pub id: Uuid,
    pub name: String,
    pub kind: UpstreamKind,
    pub base_url: Option<Url>,
    /// API-key ciphertext sealed with AAD = `id`.
    pub api_key_ciphertext: Option<Vec<u8>>,
    /// Initial OAuth credential; stored with `oauth_token_generation == 1`.
    pub oauth_tokens: Option<OAuthTokensCreate>,
    pub warmup_enabled: bool,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    pub workspace_id: Option<String>,
    /// Capability/access snapshot written in the same transaction as the spec
    /// row. `None` creates no details row (reads as "not checked").
    pub api_key_access: Option<crate::upstream_api_key::ApiKeyAccessWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamUpdate {
    pub name: Option<String>,
    /// `None` preserves the override; `Some(None)` clears it;
    /// `Some(Some(url))` sets a new override.
    pub base_url: Option<Option<Url>>,
    pub enabled: Option<bool>,
    pub api_key_ciphertext: Option<Vec<u8>>,
    pub oauth_token_generation: Option<u64>,
    pub warmup_enabled: Option<bool>,
    pub warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    /// `None` preserves the workspace; `Some(None)` clears it;
    /// `Some(Some(id))` sets it.
    pub workspace_id: Option<Option<String>>,
    /// Capability/access snapshot written in the same transaction as the spec
    /// update. `None` leaves the details row untouched.
    pub api_key_access: Option<crate::upstream_api_key::ApiKeyAccessWrite>,
    /// Zeroes usage-poll failures and clears the usage-poll schedule and
    /// success/attempt timestamps. Usage rows are kept.
    pub reset_api_key_poll_state: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpstreamStatusUpdate {
    /// For Anthropic OAuth records, terminal refresh failures cannot be
    /// replaced by nonterminal errors or cleared through status updates.
    /// Credential writes clear them.
    pub last_apply_error: Option<Option<String>>,
    pub last_apply_at_unix_secs: Option<Option<u64>>,
    /// When `Some(value)`, write `value` to `upstream_status_v1.last_warmup_at`.
    /// `Some(None)` clears it; `None` leaves it untouched.
    pub last_warmup_at_unix_secs: Option<Option<u64>>,
    /// When `Some(value)`, only apply the status update if the stored OAuth
    /// token generation still equals `value`; a mismatch is a conflict.
    pub expected_oauth_token_generation: Option<u64>,
}

#[async_trait]
pub trait UpstreamStore: Send + Sync {
    async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord>;
    /// Returns the live (non-soft-deleted) record with this name.
    ///
    /// Soft-deleted rows are never returned; look them up by id with [`Self::get_by_id`].
    /// Names are unique only among live rows, so a soft-deleted name is reusable.
    async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>>;
    async fn get_by_id(&self, id: Uuid) -> StorageResult<Option<UpstreamRecord>>;
    async fn list(&self, after: Option<Uuid>, limit: usize) -> StorageResult<Vec<UpstreamRecord>>;
    async fn update(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord>;
    async fn set_enabled(
        &self,
        id: Uuid,
        expected_revision: u64,
        enabled: bool,
    ) -> StorageResult<UpstreamRecord>;
    async fn update_spec(
        &self,
        id: Uuid,
        expected_revision: u64,
        update: UpstreamUpdate,
    ) -> StorageResult<UpstreamRecord>;
    async fn update_api_key_secret(
        &self,
        id: Uuid,
        api_key_ciphertext: Option<Vec<u8>>,
    ) -> StorageResult<UpstreamRecord>;
    async fn update_oauth_token(
        &self,
        id: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    async fn set_status(&self, id: Uuid, status: UpstreamStatusUpdate) -> StorageResult<()>;
    async fn store_oauth_tokens(
        &self,
        id: Uuid,
        expected_revision: u64,
        tokens: EncryptedOAuthTokens,
        never_refresh: bool,
    ) -> StorageResult<UpstreamRecord>;
    async fn complete_refresh(
        &self,
        id: Uuid,
        holder: Uuid,
        tokens: EncryptedOAuthTokens,
    ) -> StorageResult<UpstreamRecord>;
    async fn read_oauth_token_generation(&self, id: Uuid) -> StorageResult<Option<u64>> {
        Ok(self
            .get_by_id(id)
            .await?
            .map(|record| record.oauth_token_generation))
    }
    async fn set_last_apply_error(&self, id: Uuid, error: Option<String>) -> StorageResult<()>;
    async fn soft_delete(&self, id: Uuid, expected_revision: u64) -> StorageResult<()>;
    async fn hard_delete(&self, id: Uuid) -> StorageResult<()>;
    async fn clear_warmup_dialect_plugin(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> StorageResult<Option<UpstreamRecord>>;
}

#[cfg(test)]
mod tests {
    use super::UpstreamKind;
    use crate::{CredentialKind, Provider};

    #[test]
    fn gemini_oauth_has_gemini_oauth_classification() {
        assert_eq!(UpstreamKind::GeminiOauth.provider(), Provider::Gemini);
        assert_eq!(
            UpstreamKind::GeminiOauth.credential_kind(),
            CredentialKind::Oauth
        );
        assert_eq!(UpstreamKind::GeminiOauth.as_str(), "gemini_oauth");
    }
}
