use std::sync::Arc;

use cc_lb_storage_api::{
    ManagedKeyStore, StorageError,
    types::{ApiKeyMutation, IssueParams, KeyStatus, Limit, StoredApiKeyRecord},
};
use thiserror::Error;

use super::secret::{self, NewKeyOutput, RedactedSecret};

pub type Result<T> = std::result::Result<T, KeyStoreError>;

#[derive(Clone)]
pub struct KeyStore {
    storage: Arc<dyn ManagedKeyStore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateParams {
    pub label: String,
    pub description: Option<String>,
    pub expires_at_unix_secs: Option<u64>,
    pub limit_overrides: Vec<Limit>,
}

#[derive(Debug, Error)]
pub enum KeyStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("api key {principal_id}/{key_id} is already revoked")]
    KeyAlreadyRevoked {
        principal_id: String,
        key_id: String,
    },
}

impl KeyStore {
    pub fn new(storage: Arc<dyn ManagedKeyStore>) -> Self {
        Self { storage }
    }

    pub async fn create(
        &self,
        principal_id: &str,
        params: CreateParams,
    ) -> Result<(StoredApiKeyRecord, RedactedSecret)> {
        let NewKeyOutput {
            plaintext,
            key_id,
            secret_salt,
            index_hash,
            verify_hash,
            last_4,
        } = secret::generate_new();

        let issue_params = IssueParams {
            label: params.label,
            description: params.description,
            expires_at_unix_secs: params.expires_at_unix_secs,
            limit_overrides: params.limit_overrides,
            secret_salt,
            verify_hash,
            last_4,
            index_hash,
        };

        let record = self
            .storage
            .issue(principal_id, &key_id, issue_params)
            .await?;

        Ok((record, plaintext))
    }

    pub async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> Result<Option<(String, String, StoredApiKeyRecord)>> {
        Ok(self.storage.lookup_by_index_hash(index_hash).await?)
    }

    pub async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<StoredApiKeyRecord>> {
        Ok(self.storage.get(principal_id, key_id).await?)
    }

    pub async fn list_by_principal(&self, principal_id: &str) -> Result<Vec<StoredApiKeyRecord>> {
        Ok(self.storage.list_by_principal(principal_id).await?)
    }

    pub async fn list_all(&self) -> Result<Vec<(String, String, StoredApiKeyRecord)>> {
        Ok(self.storage.list_all().await?)
    }

    pub async fn disable(&self, principal_id: &str, key_id: &str) -> Result<()> {
        self.storage
            .update(
                principal_id,
                key_id,
                ApiKeyMutation {
                    status: Some(KeyStatus::Disabled),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    pub async fn enable(&self, principal_id: &str, key_id: &str) -> Result<()> {
        if matches!(
            self.storage.get(principal_id, key_id).await?,
            Some(StoredApiKeyRecord {
                status: KeyStatus::Revoked,
                ..
            })
        ) {
            return Err(KeyStoreError::KeyAlreadyRevoked {
                principal_id: principal_id.to_owned(),
                key_id: key_id.to_owned(),
            });
        }

        self.storage
            .update(
                principal_id,
                key_id,
                ApiKeyMutation {
                    status: Some(KeyStatus::Active),
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    pub async fn revoke(&self, principal_id: &str, key_id: &str) -> Result<()> {
        self.storage
            .revoke_zero_secrets(principal_id, key_id)
            .await?;
        Ok(())
    }

    pub async fn patch(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> Result<()> {
        self.storage.update(principal_id, key_id, mutation).await?;
        Ok(())
    }
}
