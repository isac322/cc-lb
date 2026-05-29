use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_storage_api::{
    ManagedKeyStore, StorageError as ApiStorageError, StorageResult,
    types::{ApiKeyMutation, IssueParams, StoredApiKeyRecord},
    validate_identifier,
};

use crate::{Storage, StorageError};

use crate::error_map::{map_join_err, map_redb_err};

#[derive(Clone)]
pub struct RedbManagedKeyStore {
    storage: Arc<Storage>,
}

impl RedbManagedKeyStore {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ManagedKeyStore for RedbManagedKeyStore {
    async fn issue(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> StorageResult<StoredApiKeyRecord> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;
        validate_identifier("label", &params.label)?;
        if let Some(desc) = &params.description {
            validate_identifier("description", desc)?;
        }

        let storage = Arc::clone(&self.storage);
        let principal_id = principal_id.to_owned();
        let key_id = key_id.to_owned();

        tokio::task::spawn_blocking(move || {
            storage.issue_api_key_record_with_index(&principal_id, &key_id, params)?;
            storage.get_api_key(&principal_id, &key_id)?.ok_or_else(|| {
                StorageError::UnknownApiKey {
                    principal_id,
                    key_id,
                }
            })
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn get(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> StorageResult<Option<StoredApiKeyRecord>> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let storage = Arc::clone(&self.storage);
        let principal_id = principal_id.to_owned();
        let key_id = key_id.to_owned();

        tokio::task::spawn_blocking(move || storage.get_api_key(&principal_id, &key_id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> StorageResult<Option<(String, String, StoredApiKeyRecord)>> {
        let storage = Arc::clone(&self.storage);
        let index_hash = *index_hash;

        tokio::task::spawn_blocking(move || -> StorageResult<_> {
            let Some(composite_row_key) = storage
                .get_composite_by_index(&index_hash)
                .map_err(map_redb_err)?
            else {
                return Ok(None);
            };

            let (principal_id, key_id) = decode_composite_row_key(&composite_row_key)?;
            let Some(record) = storage
                .get_api_key(&principal_id, &key_id)
                .map_err(map_redb_err)?
            else {
                return Ok(None);
            };

            Ok(Some((principal_id, key_id, record)))
        })
        .await
        .map_err(map_join_err)?
    }

    async fn list_by_principal(
        &self,
        principal_id: &str,
    ) -> StorageResult<Vec<StoredApiKeyRecord>> {
        validate_identifier("principal_id", principal_id)?;

        let storage = Arc::clone(&self.storage);
        let principal_id = principal_id.to_owned();

        tokio::task::spawn_blocking(move || storage.list_api_keys(&principal_id))
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn list_all(&self) -> StorageResult<Vec<(String, String, StoredApiKeyRecord)>> {
        let storage = Arc::clone(&self.storage);

        tokio::task::spawn_blocking(move || storage.list_api_keys_all())
            .await
            .map_err(map_join_err)?
            .map_err(map_redb_err)
    }

    async fn update(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> StorageResult<()> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let storage = Arc::clone(&self.storage);
        let principal_id = principal_id.to_owned();
        let key_id = key_id.to_owned();

        tokio::task::spawn_blocking(move || {
            storage
                .update_api_key_record(&principal_id, &key_id, mutation)?
                .ok_or_else(|| StorageError::UnknownApiKey {
                    principal_id,
                    key_id,
                })?;
            Ok(())
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }

    async fn revoke_zero_secrets(&self, principal_id: &str, key_id: &str) -> StorageResult<()> {
        validate_identifier("principal_id", principal_id)?;
        validate_identifier("key_id", key_id)?;

        let storage = Arc::clone(&self.storage);
        let principal_id = principal_id.to_owned();
        let key_id = key_id.to_owned();

        tokio::task::spawn_blocking(move || {
            storage
                .revoke_api_key_record_zero_secret_fields(&principal_id, &key_id)?
                .ok_or_else(|| StorageError::UnknownApiKey {
                    principal_id,
                    key_id,
                })?;
            Ok(())
        })
        .await
        .map_err(map_join_err)?
        .map_err(map_redb_err)
    }
}

fn decode_composite_row_key(composite_row_key: &[u8]) -> StorageResult<(String, String)> {
    let Some(separator_index) = composite_row_key.iter().position(|byte| *byte == 0) else {
        return Err(ApiStorageError::Corrupted {
            message: "redb api key index row missing NUL separator".to_owned(),
        });
    };

    let (principal_id_bytes, remainder) = composite_row_key.split_at(separator_index);
    let key_id_bytes = &remainder[1..];
    let principal_id = std::str::from_utf8(principal_id_bytes)
        .map_err(|error| ApiStorageError::Corrupted {
            message: format!("redb api key index principal id is not UTF-8: {error}"),
        })?
        .to_owned();
    let key_id = std::str::from_utf8(key_id_bytes)
        .map_err(|error| ApiStorageError::Corrupted {
            message: format!("redb api key index key id is not UTF-8: {error}"),
        })?
        .to_owned();

    Ok((principal_id, key_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_key_storage_key;
    use cc_lb_storage_api::types::{KeyStatus, Limit, LimitKind, PrincipalKindLite, UpstreamKind};

    #[tokio::test]
    async fn issue_get_lookup_list_update_and_revoke() -> anyhow::Result<()> {
        let (_dir, store) = new_store()?;
        let params = issue_params("managed key", 5);

        let issued = store.issue("principal-1", "key-1", params.clone()).await?;
        assert_eq!(issued.label, params.label);
        assert_eq!(issued.status, KeyStatus::Active);
        assert_eq!(issued.index_hash, params.index_hash);

        let fetched = store
            .get("principal-1", "key-1")
            .await?
            .expect("issued key is readable");
        assert_eq!(fetched, issued);

        let lookup = store
            .lookup_by_index_hash(&params.index_hash)
            .await?
            .expect("index lookup returns issued key");
        assert_eq!(lookup.0, "principal-1");
        assert_eq!(lookup.1, "key-1");
        assert_eq!(lookup.2, issued);

        let principal_keys = store.list_by_principal("principal-1").await?;
        assert_eq!(principal_keys, vec![issued.clone()]);

        let all_keys = store.list_all().await?;
        assert_eq!(all_keys.len(), 1);
        assert_eq!(all_keys[0].0, "principal-1");
        assert_eq!(all_keys[0].1, "key-1");
        assert_eq!(all_keys[0].2, issued);

        store
            .update(
                "principal-1",
                "key-1",
                ApiKeyMutation {
                    label: Some("renamed key".to_owned()),
                    status: Some(KeyStatus::Disabled),
                    ..Default::default()
                },
            )
            .await?;
        let updated = store
            .get("principal-1", "key-1")
            .await?
            .expect("updated key is readable");
        assert_eq!(updated.label, "renamed key");
        assert_eq!(updated.status, KeyStatus::Disabled);

        store.revoke_zero_secrets("principal-1", "key-1").await?;
        let revoked = store
            .get("principal-1", "key-1")
            .await?
            .expect("revoked key remains listed");
        assert_eq!(revoked.status, KeyStatus::Revoked);
        assert_eq!(revoked.index_hash, [0; 32]);
        assert_eq!(revoked.verify_hash, [0; 32]);
        assert_eq!(revoked.secret_salt, [0; 16]);
        assert_eq!(revoked.last_4, "");
        assert!(
            store
                .lookup_by_index_hash(&params.index_hash)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_identifiers() -> anyhow::Result<()> {
        let (_dir, store) = new_store()?;

        let principal_err = store.get("", "key-1").await.unwrap_err();
        assert!(matches!(
            principal_err,
            ApiStorageError::InvalidInput { ref field, .. } if field == "principal_id"
        ));

        let key_err = store
            .update("principal-1", "bad\0key", ApiKeyMutation::default())
            .await
            .unwrap_err();
        assert!(matches!(
            key_err,
            ApiStorageError::InvalidInput { ref field, .. } if field == "key_id"
        ));

        let list_err = store.list_by_principal("").await.unwrap_err();
        assert!(matches!(
            list_err,
            ApiStorageError::InvalidInput { ref field, .. } if field == "principal_id"
        ));

        Ok(())
    }

    #[test]
    fn decodes_composite_row_keys() {
        let key = api_key_storage_key("principal-1", "key-1");
        let (principal_id, key_id) = decode_composite_row_key(&key).expect("valid key decodes");

        assert_eq!(principal_id, "principal-1");
        assert_eq!(key_id, "key-1");
    }

    fn new_store() -> anyhow::Result<(tempfile::TempDir, RedbManagedKeyStore)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("managed_keys.redb");
        let storage = Storage::open(&path, [31; 32])?;

        Ok((dir, RedbManagedKeyStore::new(Arc::new(storage))))
    }

    fn issue_params(label: &str, seed: u8) -> IssueParams {
        IssueParams {
            label: label.to_owned(),
            description: Some("test key".to_owned()),
            upstream_kind: UpstreamKind::AnthropicKey,
            expires_at_unix_secs: Some(1_800_000_000),
            limit_overrides: vec![Limit {
                kind: LimitKind::Requests,
                window_secs: 60,
                cap_micros: 100,
            }],
            secret_salt: [seed; 16],
            verify_hash: [seed + 1; 32],
            last_4: "abcd".to_owned(),
            principal_kind: PrincipalKindLite::Machine,
            index_hash: [seed + 2; 32],
        }
    }
}
