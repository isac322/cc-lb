//! Durable owner-bound provider file lifecycle storage.
//!
//! File records contain provider references only. They deliberately do not carry
//! usage, pricing, vendor credentials, or a proxy-fetch capability.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{AccountOwnerBinding, OwnerBindingExpectation, StorageResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexFileState {
    Created,
    Finalized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexFileResource {
    pub file_id: String,
    pub principal_id: String,
    pub provider: String,
    pub kind: String,
    pub owner_binding_id: String,
    pub owner_id: String,
    pub upstream_id: crate::UpstreamRecordId,
    pub owner_generation: u64,
    pub binding_revision: u64,
    pub workspace_id: String,
    pub state: CodexFileState,
    pub created_at_unix_secs: u64,
    pub expires_at_unix_secs: Option<u64>,
    pub reference: Option<String>,
}

impl CodexFileResource {
    pub fn bind(file_id: String, kind: String, binding: &AccountOwnerBinding, workspace_id: String, created_at_unix_secs: u64, expires_at_unix_secs: Option<u64>) -> Self {
        Self {
            file_id,
            principal_id: binding.principal_id.clone(),
            provider: binding.provider.clone(),
            kind,
            owner_binding_id: binding.binding_id.clone(),
            owner_id: binding.owner_id.clone(),
            upstream_id: binding.upstream_id,
            owner_generation: binding.owner_generation,
            binding_revision: binding.revision,
            workspace_id,
            state: CodexFileState::Created,
            created_at_unix_secs,
            expires_at_unix_secs,
            reference: None,
        }
    }
}

#[async_trait]
pub trait CodexFileStore: Send + Sync {
    /// Validates the live owner binding and durably records a provider create success.
    async fn create_codex_file(
        &self,
        expected: &OwnerBindingExpectation,
        resource: CodexFileResource,
    ) -> StorageResult<()>;

    /// Validates the live owner binding and atomically records finalization.
    async fn finalize_codex_file(
        &self,
        expected: &OwnerBindingExpectation,
        file_id: &str,
        reference: String,
        expires_at_unix_secs: Option<u64>,
    ) -> StorageResult<CodexFileResource>;

    /// Validates the live owner binding before returning a reference record.
    async fn resolve_codex_file(
        &self,
        expected: &OwnerBindingExpectation,
        file_id: &str,
    ) -> StorageResult<CodexFileResource>;
}
