use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{StorageResult, UpstreamRecordId};

/// Provider-owned server state whose follow-up requests must reach the upstream
/// that created or first observed it. Values are stored only as SHA-256 digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamAffinityKind {
    AnthropicWebSearchEncryptedContent,
    AnthropicFileId,
    AnthropicContainerId,
    OpenaiResponseId,
    OpenaiConversationId,
    GeminiCachedContent,
}

impl UpstreamAffinityKind {
    pub const ALL: [Self; 6] = [
        Self::AnthropicWebSearchEncryptedContent,
        Self::AnthropicFileId,
        Self::AnthropicContainerId,
        Self::OpenaiResponseId,
        Self::OpenaiConversationId,
        Self::GeminiCachedContent,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicWebSearchEncryptedContent => "anthropic_web_search_encrypted_content",
            Self::AnthropicFileId => "anthropic_file_id",
            Self::AnthropicContainerId => "anthropic_container_id",
            Self::OpenaiResponseId => "openai_response_id",
            Self::OpenaiConversationId => "openai_conversation_id",
            Self::GeminiCachedContent => "gemini_cached_content",
        }
    }

    /// Parses the persisted `as_str` form; returns `None` for unknown kinds so
    /// adapters can report corrupted rows.
    pub fn from_storage_str(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpstreamAffinityKey {
    pub principal_id: String,
    pub provider: String,
    pub kind: UpstreamAffinityKind,
    pub value_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamAffinityBinding {
    pub key: UpstreamAffinityKey,
    pub upstream_id: UpstreamRecordId,
    /// Owner identity epoch captured when this binding was created.
    ///
    /// A binding is valid only while this equals the current upstream owner
    /// epoch. `0` preserves compatibility with rows written before epoch
    /// fencing was introduced.
    #[serde(default)]
    pub owner_epoch: u64,
    pub observed_at_unix_secs: u64,
    pub expires_at_unix_secs: Option<u64>,
}

#[async_trait]
pub trait UpstreamAffinityStore: Send + Sync + 'static {
    /// Returns an epoch only if the dispatched credential snapshot is still current.
    async fn read_upstream_owner_epoch(
        &self,
        upstream_id: UpstreamRecordId,
        expected_spec_revision: u64,
    ) -> StorageResult<Option<u64>>;

    async fn resolve_upstream_affinities(
        &self,
        keys: &[UpstreamAffinityKey],
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<Vec<UpstreamAffinityBinding>>;

    async fn bind_upstream_affinities(
        &self,
        bindings: &[UpstreamAffinityBinding],
        now_unix_secs: u64,
        ttl_secs: u64,
    ) -> StorageResult<()>;

    async fn purge_expired_upstream_affinities(
        &self,
        now_unix_secs: u64,
        ttl_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64>;
}
