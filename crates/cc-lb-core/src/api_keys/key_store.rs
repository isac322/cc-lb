use std::sync::Arc;

use cc_lb_storage_api::{
    ManagedKeyStore, StorageError,
    types::{
        ApiKeyMutation, IssueParams, KeyStatus, Limit, PrincipalKindLite, StoredApiKeyRecord,
        UpstreamKind,
    },
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
    pub upstream_kind: UpstreamKind,
    pub upstream_credential_ref: String,
    pub label: String,
    pub description: Option<String>,
    pub expires_at_unix_secs: Option<u64>,
    pub limit_overrides: Vec<Limit>,
    pub principal_kind: PrincipalKindLite,
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
            upstream_kind: params.upstream_kind,
            upstream_credential_ref: params.upstream_credential_ref,
            expires_at_unix_secs: params.expires_at_unix_secs,
            limit_overrides: params.limit_overrides,
            secret_salt,
            verify_hash,
            last_4,
            principal_kind: params.principal_kind,
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

    pub async fn list_all(&self) -> Result<Vec<StoredApiKeyRecord>> {
        Ok(self
            .storage
            .list_all()
            .await?
            .into_iter()
            .map(|(_, _, record)| record)
            .collect())
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

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_storage_api::types::LimitKind;
    use cc_lb_storage_redb::{RedbManagedKeyStore, Storage};

    #[tokio::test]
    async fn create_lists_principal() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = new_store()?;

        store
            .create("principal-1", create_params("managed key"))
            .await?;

        let listed = store.list_by_principal("principal-1").await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].label, "managed key");

        Ok(())
    }

    #[tokio::test]
    async fn lookup_by_index_hash_returns_matching_key()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = new_store()?;
        let (record, secret) = store
            .create("principal-1", create_params("managed key"))
            .await?;
        let (key_id, _) = secret::parse(secret.expose())?;

        let lookup = store
            .lookup_by_index_hash(&record.index_hash)
            .await?
            .expect("index lookup returns record");

        assert_eq!(lookup.0, "principal-1");
        assert_eq!(lookup.1, key_id);
        assert_eq!(lookup.2, record);

        Ok(())
    }

    #[tokio::test]
    async fn revoke_removes_index() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = new_store()?;
        let (record, secret) = store
            .create("principal-1", create_params("managed key"))
            .await?;
        let (key_id, _) = secret::parse(secret.expose())?;
        let index_hash = record.index_hash;

        store.revoke("principal-1", &key_id).await?;

        assert!(store.lookup_by_index_hash(&index_hash).await?.is_none());
        let listed = store.list_by_principal("principal-1").await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, KeyStatus::Revoked);
        assert!(listed[0].revoked_at_unix_secs.is_some());
        assert_eq!(listed[0].index_hash, [0; 32]);
        assert_eq!(listed[0].verify_hash, [0; 32]);
        assert_eq!(listed[0].secret_salt, [0; 16]);
        assert_eq!(listed[0].last_4, "");

        Ok(())
    }

    #[tokio::test]
    async fn disable_keeps_index() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = new_store()?;
        let (record, secret) = store
            .create("principal-1", create_params("managed key"))
            .await?;
        let (key_id, _) = secret::parse(secret.expose())?;

        store.disable("principal-1", &key_id).await?;

        let lookup = store
            .lookup_by_index_hash(&record.index_hash)
            .await?
            .expect("disabled key remains indexed");
        assert_eq!(lookup.2.status, KeyStatus::Disabled);

        Ok(())
    }

    #[tokio::test]
    async fn patch_label_change_reflected_in_list()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (_dir, store) = new_store()?;
        let (_record, secret) = store
            .create("principal-1", create_params("managed key"))
            .await?;
        let (key_id, _) = secret::parse(secret.expose())?;

        store
            .patch(
                "principal-1",
                &key_id,
                ApiKeyMutation {
                    label: Some("renamed key".to_owned()),
                    ..Default::default()
                },
            )
            .await?;

        let listed = store.list_by_principal("principal-1").await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].label, "renamed key");

        Ok(())
    }

    #[tokio::test]
    async fn enable_on_revoked_returns_err() -> std::result::Result<(), Box<dyn std::error::Error>>
    {
        let (_dir, store) = new_store()?;
        let (_record, secret) = store
            .create("principal-1", create_params("managed key"))
            .await?;
        let (key_id, _) = secret::parse(secret.expose())?;

        store.revoke("principal-1", &key_id).await?;
        let error = store
            .enable("principal-1", &key_id)
            .await
            .expect_err("revoked key cannot be enabled");

        assert!(matches!(error, KeyStoreError::KeyAlreadyRevoked { .. }));

        Ok(())
    }

    fn new_store() -> std::result::Result<(tempfile::TempDir, KeyStore), Box<dyn std::error::Error>>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("key_store.redb");
        let storage = Storage::open(&path, [21; 32])?;
        let managed_store = RedbManagedKeyStore::new(Arc::new(storage));
        Ok((dir, KeyStore::new(Arc::new(managed_store))))
    }

    fn create_params(label: &str) -> CreateParams {
        CreateParams {
            upstream_kind: UpstreamKind::AnthropicKey,
            upstream_credential_ref: "anthropic-prod".to_owned(),
            label: label.to_owned(),
            description: Some("test key".to_owned()),
            expires_at_unix_secs: Some(1_800_000_000),
            limit_overrides: vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 100,
            }],
            principal_kind: PrincipalKindLite::Machine,
        }
    }
}
