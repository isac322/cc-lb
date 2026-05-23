use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::{API_KEYS_V1, OAUTH_CREDENTIALS_V1, RedbStorage, StorageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    #[default]
    AnthropicKey,
    AnthropicOAuth,
    AwsSigV4,
    GcpOAuth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LimitKind {
    #[default]
    Requests,
    InputTokens,
    OutputTokens,
    TotalTokens,
    CostUsd,
    Concurrent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Limit {
    pub kind: LimitKind,
    pub window_secs: u64,
    pub cap_micros: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum KeyStatus {
    #[default]
    Active,
    Disabled,
    Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKindLite {
    Human,
    #[default]
    Machine,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredApiKeyRecord {
    pub label: String,
    pub issued_at_unix_secs: u64,
    pub revoked_at_unix_secs: Option<u64>,
    pub key_hash_b64: String,
    pub verify_hash: [u8; 32],
    pub secret_salt: [u8; 16],
    pub upstream_kind: UpstreamKind,
    pub upstream_credential_ref: String,
    pub limit_overrides: Vec<Limit>,
    pub status: KeyStatus,
    pub expires_at_unix_secs: Option<u64>,
    pub last_4: String,
    pub description: Option<String>,
    pub principal_kind: PrincipalKindLite,
    pub index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredApiKeyRecordV1 {
    label: String,
    issued_at_unix_secs: u64,
    revoked_at_unix_secs: Option<u64>,
    key_hash_b64: String,
    verify_hash: [u8; 32],
    secret_salt: [u8; 16],
    upstream_kind: UpstreamKind,
    upstream_credential_ref: String,
    limit_overrides: Vec<Limit>,
    status: KeyStatus,
    expires_at_unix_secs: Option<u64>,
    last_4: String,
    description: Option<String>,
    principal_kind: PrincipalKindLite,
    index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredApiKeyRecordV0 {
    label: String,
    issued_at_unix_secs: u64,
    revoked_at_unix_secs: Option<u64>,
    key_hash_b64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum StoredApiKeyRecordWire {
    V0(StoredApiKeyRecordV0),
    V1(StoredApiKeyRecordV1),
}

impl From<&StoredApiKeyRecord> for StoredApiKeyRecordV1 {
    fn from(value: &StoredApiKeyRecord) -> Self {
        Self {
            label: value.label.clone(),
            issued_at_unix_secs: value.issued_at_unix_secs,
            revoked_at_unix_secs: value.revoked_at_unix_secs,
            key_hash_b64: value.key_hash_b64.clone(),
            verify_hash: value.verify_hash,
            secret_salt: value.secret_salt,
            upstream_kind: value.upstream_kind,
            upstream_credential_ref: value.upstream_credential_ref.clone(),
            limit_overrides: value.limit_overrides.clone(),
            status: value.status,
            expires_at_unix_secs: value.expires_at_unix_secs,
            last_4: value.last_4.clone(),
            description: value.description.clone(),
            principal_kind: value.principal_kind,
            index_hash: value.index_hash,
        }
    }
}

impl Serialize for StoredApiKeyRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        StoredApiKeyRecordWire::V1(StoredApiKeyRecordV1::from(self)).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StoredApiKeyRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match StoredApiKeyRecordWire::deserialize(deserializer)? {
            StoredApiKeyRecordWire::V0(value) => Ok(Self {
                label: value.label,
                issued_at_unix_secs: value.issued_at_unix_secs,
                revoked_at_unix_secs: value.revoked_at_unix_secs,
                key_hash_b64: value.key_hash_b64,
                ..Default::default()
            }),
            StoredApiKeyRecordWire::V1(value) => Ok(Self {
                label: value.label,
                issued_at_unix_secs: value.issued_at_unix_secs,
                revoked_at_unix_secs: value.revoked_at_unix_secs,
                key_hash_b64: value.key_hash_b64,
                verify_hash: value.verify_hash,
                secret_salt: value.secret_salt,
                upstream_kind: value.upstream_kind,
                upstream_credential_ref: value.upstream_credential_ref,
                limit_overrides: value.limit_overrides,
                status: value.status,
                expires_at_unix_secs: value.expires_at_unix_secs,
                last_4: value.last_4,
                description: value.description,
                principal_kind: value.principal_kind,
                index_hash: value.index_hash,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueParams {
    pub label: String,
    pub description: Option<String>,
    pub upstream_kind: UpstreamKind,
    pub upstream_credential_ref: String,
    pub expires_at_unix_secs: Option<u64>,
    pub limit_overrides: Vec<Limit>,
    pub secret_salt: [u8; 16],
    pub verify_hash: [u8; 32],
    pub last_4: String,
    pub principal_kind: PrincipalKindLite,
    pub index_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ApiKeyMutation {
    pub label: Option<String>,
    pub description: Option<Option<String>>,
    pub expires_at_unix_secs: Option<Option<u64>>,
    pub limit_overrides: Option<Vec<Limit>>,
    pub status: Option<KeyStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthCredentials {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: u64,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AnthropicApiKeyCredential {
    anthropic_api_key: String,
}

pub fn oauth_key(principal_id: &str, provider: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(principal_id.len() + 1 + provider.len());
    key.extend_from_slice(principal_id.as_bytes());
    key.push(b':');
    key.extend_from_slice(provider.as_bytes());
    key
}

pub fn api_key_storage_key(principal_id: &str, key_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(principal_id.len() + 1 + key_id.len());
    key.extend_from_slice(principal_id.as_bytes());
    key.push(0);
    key.extend_from_slice(key_id.as_bytes());
    key
}

impl RedbStorage {
    pub fn put_oauth(
        &self,
        principal_id: &str,
        provider: &str,
        creds: &OAuthCredentials,
    ) -> Result<(), StorageError> {
        let key = oauth_key(principal_id, provider);
        let value = serde_json::to_vec(creds)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_oauth(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> Result<Option<OAuthCredentials>, StorageError> {
        let key = oauth_key(principal_id, provider);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        let Some(value) = table.get(key.as_slice())?.map(|stored| stored.value().to_vec()) else {
            return Ok(None);
        };

        let creds = serde_json::from_slice(&value)?;
        Ok(Some(creds))
    }

    pub(crate) fn put_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
        ciphertext: &[u8],
    ) -> Result<(), StorageError> {
        let key = oauth_key(principal_id, provider);
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
            table.insert(key.as_slice(), ciphertext)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub(crate) fn get_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let key = oauth_key(principal_id, provider);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        Ok(table.get(key.as_slice())?.map(|stored| stored.value().to_vec()))
    }

    pub(crate) fn delete_oauth_ciphertext(
        &self,
        principal_id: &str,
        provider: &str,
    ) -> Result<bool, StorageError> {
        let key = oauth_key(principal_id, provider);
        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
            table.remove(key.as_slice())?.is_some()
        };
        write_txn.commit()?;
        Ok(removed)
    }

    pub fn delete_oauth(&self, principal_id: &str, provider: &str) -> Result<(), StorageError> {
        let key = oauth_key(principal_id, provider);
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
            table.remove(key.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn put_anthropic_api_key(
        &self,
        storage_key: &str,
        api_key: &str,
    ) -> Result<(), StorageError> {
        let credential = AnthropicApiKeyCredential {
            anthropic_api_key: api_key.to_owned(),
        };
        let value = serde_json::to_vec(&credential)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
            table.insert(storage_key.as_bytes(), value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_anthropic_api_key(&self, storage_key: &str) -> Result<Option<String>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        let Some(value) = table
            .get(storage_key.as_bytes())?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };

        let credential = serde_json::from_slice::<AnthropicApiKeyCredential>(&value)?;
        Ok(Some(credential.anthropic_api_key))
    }

    pub(crate) fn put_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
        ciphertext: &[u8],
    ) -> Result<(), StorageError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(OAUTH_CREDENTIALS_V1)?;
            table.insert(storage_key.as_bytes(), ciphertext)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub(crate) fn get_anthropic_api_key_ciphertext(
        &self,
        storage_key: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(OAUTH_CREDENTIALS_V1)?;
        Ok(table
            .get(storage_key.as_bytes())?
            .map(|stored| stored.value().to_vec()))
    }

    pub fn issue_api_key(
        &self,
        write_txn: &redb::WriteTransaction,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> Result<(), StorageError> {
        let storage_key = api_key_storage_key(principal_id, key_id);
        let record = StoredApiKeyRecord {
            label: params.label,
            issued_at_unix_secs: now_unix_secs(),
            revoked_at_unix_secs: None,
            key_hash_b64: URL_SAFE_NO_PAD.encode(params.verify_hash),
            verify_hash: params.verify_hash,
            secret_salt: params.secret_salt,
            upstream_kind: params.upstream_kind,
            upstream_credential_ref: params.upstream_credential_ref,
            limit_overrides: params.limit_overrides,
            status: KeyStatus::Active,
            expires_at_unix_secs: params.expires_at_unix_secs,
            last_4: params.last_4,
            description: params.description,
            principal_kind: params.principal_kind,
            index_hash: params.index_hash,
        };
        let value = encode_record(&record)?;

        {
            let mut table = write_txn.open_table(API_KEYS_V1)?;
            table.insert(storage_key.as_slice(), value.as_slice())?;
        }

        Ok(())
    }

    pub fn get_api_key(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<StoredApiKeyRecord>, StorageError> {
        let storage_key = api_key_storage_key(principal_id, key_id);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(API_KEYS_V1)?;
        let Some(value) = table
            .get(storage_key.as_slice())?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };

        let record = decode_record(&value)?;
        Ok(Some(record))
    }

    pub fn list_api_keys(
        &self,
        principal_id: &str,
    ) -> Result<Vec<StoredApiKeyRecord>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(API_KEYS_V1)?;
        let mut records = Vec::new();
        let prefix = principal_id.as_bytes();

        for row in table.iter()? {
            let (key, stored) = row?;
            let key_bytes = key.value();
            if !key_bytes.starts_with(prefix) || key_bytes.get(prefix.len()) != Some(&0) {
                continue;
            }

            records.push(decode_record(stored.value())?);
        }

        Ok(records)
    }

    pub fn update_api_key(
        &self,
        write_txn: &redb::WriteTransaction,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> Result<Option<StoredApiKeyRecord>, StorageError> {
        let storage_key = api_key_storage_key(principal_id, key_id);
        let mut table = write_txn.open_table(API_KEYS_V1)?;
        let Some(value) = table
            .get(storage_key.as_slice())?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };

        let mut record = decode_record(&value)?;

        if let Some(label) = mutation.label {
            record.label = label;
        }
        if let Some(description) = mutation.description {
            record.description = description;
        }
        if let Some(expires_at_unix_secs) = mutation.expires_at_unix_secs {
            record.expires_at_unix_secs = expires_at_unix_secs;
        }
        if let Some(limit_overrides) = mutation.limit_overrides {
            record.limit_overrides = limit_overrides;
        }
        if let Some(status) = mutation.status {
            if status == KeyStatus::Revoked && record.revoked_at_unix_secs.is_none() {
                record.revoked_at_unix_secs = Some(now_unix_secs());
            }
            record.status = status;
        }

        let updated_value = encode_record(&record)?;
        table.insert(storage_key.as_slice(), updated_value.as_slice())?;

        Ok(Some(record))
    }

    pub fn revoke_api_key(
        &self,
        write_txn: &redb::WriteTransaction,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<StoredApiKeyRecord>, StorageError> {
        self.update_api_key(
            write_txn,
            principal_id,
            key_id,
            ApiKeyMutation {
                status: Some(KeyStatus::Revoked),
                ..Default::default()
            },
        )
    }

    pub fn issue_api_key_record(
        &self,
        principal_id: &str,
        key_id: &str,
        params: IssueParams,
    ) -> Result<(), StorageError> {
        let write_txn = self.db.begin_write()?;
        self.issue_api_key(&write_txn, principal_id, key_id, params)?;
        write_txn.commit()?;
        Ok(())
    }

    pub fn update_api_key_record(
        &self,
        principal_id: &str,
        key_id: &str,
        mutation: ApiKeyMutation,
    ) -> Result<Option<StoredApiKeyRecord>, StorageError> {
        let write_txn = self.db.begin_write()?;
        let updated = self.update_api_key(&write_txn, principal_id, key_id, mutation)?;
        write_txn.commit()?;
        Ok(updated)
    }

    pub fn revoke_api_key_record(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<StoredApiKeyRecord>, StorageError> {
        let write_txn = self.db.begin_write()?;
        let updated = self.revoke_api_key(&write_txn, principal_id, key_id)?;
        write_txn.commit()?;
        Ok(updated)
    }

    pub(crate) fn put_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
        ciphertext: &[u8],
    ) -> Result<(), StorageError> {
        let storage_key = api_key_storage_key(principal_id, key_id);
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(API_KEYS_V1)?;
            table.insert(storage_key.as_slice(), ciphertext)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub(crate) fn get_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let storage_key = api_key_storage_key(principal_id, key_id);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(API_KEYS_V1)?;
        Ok(table
            .get(storage_key.as_slice())?
            .map(|stored| stored.value().to_vec()))
    }

    pub(crate) fn list_api_key_ciphertexts(
        &self,
        principal_id: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(API_KEYS_V1)?;
        let prefix = api_key_storage_prefix(principal_id);
        let mut records = Vec::new();
        for row in table.iter()? {
            let (key, value) = row?;
            let key_bytes = key.value();
            if let Some(key_id) = api_key_id_from_storage_key(&prefix, key_bytes) {
                records.push((key_id.to_owned(), value.value().to_vec()));
            }
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(records)
    }

    pub(crate) fn revoke_api_key_ciphertext(
        &self,
        principal_id: &str,
        key_id: &str,
        revoked_ciphertext: &[u8],
    ) -> Result<bool, StorageError> {
        let storage_key = api_key_storage_key(principal_id, key_id);
        let write_txn = self.db.begin_write()?;
        let replaced = {
            let mut table = write_txn.open_table(API_KEYS_V1)?;
            let exists = table.get(storage_key.as_slice())?.is_some();
            if exists {
                table.insert(storage_key.as_slice(), revoked_ciphertext)?;
            }
            exists
        };
        write_txn.commit()?;
        Ok(replaced)
    }

    pub(crate) fn encrypt_value(
        &self,
        _aad: &[u8],
        plaintext: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        Ok(plaintext.to_vec())
    }

    pub(crate) fn decrypt_value(&self, _aad: &[u8], value: &[u8]) -> Result<Vec<u8>, StorageError> {
        Ok(value.to_vec())
    }
}

fn api_key_storage_prefix(principal_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(principal_id.len() + 1);
    key.extend_from_slice(principal_id.as_bytes());
    key.push(0);
    key
}

fn api_key_id_from_storage_key<'a>(prefix: &[u8], storage_key: &'a [u8]) -> Option<&'a str> {
    storage_key
        .strip_prefix(prefix)
        .and_then(|suffix| std::str::from_utf8(suffix).ok())
        .filter(|key_id| !key_id.is_empty())
}

fn encode_record(record: &StoredApiKeyRecord) -> Result<Vec<u8>, StorageError> {
    Ok(encode_to_vec(
        record,
        standard().with_variable_int_encoding(),
    )?)
}

fn decode_record(bytes: &[u8]) -> Result<StoredApiKeyRecord, StorageError> {
    let (record, _) = decode_from_slice(bytes, standard().with_variable_int_encoding())?;
    Ok(record)
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
