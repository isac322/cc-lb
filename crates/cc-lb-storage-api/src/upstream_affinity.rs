use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{StorageResult, UpstreamRecordId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamAffinityKind {
    AnthropicWebSearchEncryptedContent,
}

impl UpstreamAffinityKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AnthropicWebSearchEncryptedContent => "anthropic_web_search_encrypted_content",
        }
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
    pub observed_at_unix_secs: u64,
    pub expires_at_unix_secs: Option<u64>,
}

#[async_trait]
pub trait UpstreamAffinityStore: Send + Sync + 'static {
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
