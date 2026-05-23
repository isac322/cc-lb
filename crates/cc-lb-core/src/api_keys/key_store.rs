use std::sync::Arc;

use cc_lb_storage_redb::{
    ApiKeyMutation, IssueParams, KeyStatus, Limit, PrincipalKindLite, Storage, StorageError,
    StoredApiKeyRecord, UpstreamKind,
};
use thiserror::Error;

use super::secret::{self, NewKeyOutput, RedactedSecret};

pub type Result<T> = std::result::Result<T, KeyStoreError>;

#[derive(Clone)]
pub struct KeyStore {
    storage: Arc<Storage>,
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
    #[error("api key index inconsistency: {reason}")]
    IndexInconsistency { reason: String },
    #[error("api key {principal_id}/{key_id} is already revoked")]
    KeyAlreadyRevoked {
        principal_id: String,
        key_id: String,
    },
}

impl KeyStore {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    pub fn create(
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

        self.storage
            .issue_api_key_record_with_index(principal_id, &key_id, issue_params)?;

        let record = self
            .storage
            .get_api_key(principal_id, &key_id)?
            .ok_or_else(|| KeyStoreError::IndexInconsistency {
                reason: format!("issued api key row missing for {principal_id}/{key_id}"),
            })?;

        Ok((record, plaintext))
    }

    pub fn lookup_by_index_hash(
        &self,
        index_hash: &[u8; 32],
    ) -> Result<Option<(String, String, StoredApiKeyRecord)>> {
        let Some(composite_row_key) = self.storage.get_composite_by_index(index_hash)? else {
            return Ok(None);
        };

        let (principal_id, key_id) = decode_composite_row_key(&composite_row_key)?;
        let Some(record) = self.storage.get_api_key(&principal_id, &key_id)? else {
            eprintln!(
                "WARN api key index points to missing row principal_id={principal_id} key_id={key_id}"
            );
            return Ok(None);
        };

        Ok(Some((principal_id, key_id, record)))
    }

    pub fn list_by_principal(&self, principal_id: &str) -> Result<Vec<StoredApiKeyRecord>> {
        Ok(self.storage.list_api_keys(principal_id)?)
    }

    pub fn list_all(&self) -> Result<Vec<StoredApiKeyRecord>> {
        Ok(self
            .storage
            .list_api_keys_all()?
            .into_iter()
            .map(|(_, _, record)| record)
            .collect())
    }

    pub fn disable(&self, principal_id: &str, key_id: &str) -> Result<()> {
        self.storage.update_api_key_record(
            principal_id,
            key_id,
            ApiKeyMutation {
                status: Some(KeyStatus::Disabled),
                ..Default::default()
            },
        )?;
        Ok(())
    }

    pub fn enable(&self, principal_id: &str, key_id: &str) -> Result<()> {
        if matches!(
            self.storage.get_api_key(principal_id, key_id)?,
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

        self.storage.update_api_key_record(
            principal_id,
            key_id,
            ApiKeyMutation {
                status: Some(KeyStatus::Active),
                ..Default::default()
            },
        )?;
        Ok(())
    }

    pub fn revoke(&self, principal_id: &str, key_id: &str) -> Result<()> {
        self.storage
            .revoke_api_key_record_zero_secret_fields(principal_id, key_id)?;
        Ok(())
    }

    pub fn patch(&self, principal_id: &str, key_id: &str, mutation: ApiKeyMutation) -> Result<()> {
        self.storage
            .update_api_key_record(principal_id, key_id, mutation)?;
        Ok(())
    }
}

fn decode_composite_row_key(composite_row_key: &[u8]) -> Result<(String, String)> {
    let Some(separator_index) = composite_row_key.iter().position(|byte| *byte == 0) else {
        return Err(KeyStoreError::IndexInconsistency {
            reason: "composite row key missing NUL separator".to_owned(),
        });
    };

    let (principal_id_bytes, remainder) = composite_row_key.split_at(separator_index);
    let key_id_bytes = &remainder[1..];
    let principal_id = std::str::from_utf8(principal_id_bytes)
        .map_err(|error| KeyStoreError::IndexInconsistency {
            reason: format!("principal id is not UTF-8: {error}"),
        })?
        .to_owned();
    let key_id = std::str::from_utf8(key_id_bytes)
        .map_err(|error| KeyStoreError::IndexInconsistency {
            reason: format!("key id is not UTF-8: {error}"),
        })?
        .to_owned();

    Ok((principal_id, key_id))
}
