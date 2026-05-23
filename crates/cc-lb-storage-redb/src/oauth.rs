use base64::Engine;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use redb::ReadableTable;
use ring::digest::{SHA256, digest};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{API_KEYS_V1, OAUTH_CREDENTIALS_V1, Storage, StorageError};

const NONCE_LEN: usize = 12;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuedKey {
    pub key_id: String,
    pub plaintext: String,
    pub issued_at_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub key_id: String,
    pub label: Option<String>,
    pub issued_at_unix_secs: u64,
    pub revoked_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredApiKeyRecord {
    label: Option<String>,
    issued_at_unix_secs: u64,
    revoked_at_unix_secs: Option<u64>,
    key_hash_b64: String,
}

pub fn oauth_key(principal_id: &str, provider: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(principal_id.len() + 1 + provider.len());
    key.extend_from_slice(principal_id.as_bytes());
    key.push(b':');
    key.extend_from_slice(provider.as_bytes());
    key
}

impl Storage {
    pub fn put_oauth(
        &self,
        principal_id: &str,
        provider: &str,
        creds: &OAuthCredentials,
    ) -> Result<(), StorageError> {
        let key = oauth_key(principal_id, provider);
        let plaintext = serde_json::to_vec(creds)?;
        let value = self.encrypt_oauth(principal_id, &plaintext)?;

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
        let Some(ciphertext) = table
            .get(key.as_slice())?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };

        let plaintext = self.decrypt_oauth(principal_id, &ciphertext)?;
        let creds = serde_json::from_slice(&plaintext)?;
        Ok(Some(creds))
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
        let plaintext = serde_json::to_vec(&credential)?;
        let value = self.encrypt_value(storage_key.as_bytes(), &plaintext)?;

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
        let Some(ciphertext) = table
            .get(storage_key.as_bytes())?
            .map(|stored| stored.value().to_vec())
        else {
            return Ok(None);
        };

        let plaintext = self.decrypt_value(storage_key.as_bytes(), &ciphertext)?;
        let credential = serde_json::from_slice::<AnthropicApiKeyCredential>(&plaintext)?;
        Ok(Some(credential.anthropic_api_key))
    }

    pub fn issue_api_key(
        &self,
        principal_id: &str,
        label: Option<String>,
    ) -> Result<IssuedKey, StorageError> {
        let mut raw_key = [0_u8; 32];
        SystemRandom::new()
            .fill(&mut raw_key)
            .map_err(|_| StorageError::Random)?;
        let plaintext = URL_SAFE_NO_PAD.encode(raw_key);
        let key_hash = digest(&SHA256, plaintext.as_bytes());
        let key_id = hex_prefix(key_hash.as_ref(), 12);
        let issued_at_unix_secs = unix_now_secs();
        let stored = StoredApiKeyRecord {
            label,
            issued_at_unix_secs,
            revoked_at_unix_secs: None,
            key_hash_b64: STANDARD_NO_PAD.encode(key_hash.as_ref()),
        };
        let storage_key = api_key_storage_key(principal_id, &key_id);
        let plaintext_record = serde_json::to_vec(&stored)?;
        let value = self.encrypt_value(storage_key.as_slice(), &plaintext_record)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(API_KEYS_V1)?;
            table.insert(storage_key.as_slice(), value.as_slice())?;
        }
        write_txn.commit()?;

        Ok(IssuedKey {
            key_id,
            plaintext,
            issued_at_unix_secs,
        })
    }

    pub fn list_api_keys(&self, principal_id: &str) -> Result<Vec<ApiKeyRecord>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(API_KEYS_V1)?;
        let prefix = api_key_storage_prefix(principal_id);
        let mut records = Vec::new();
        for row in table.iter()? {
            let (key, value) = row?;
            let key_bytes = key.value();
            if let Some(key_id) = api_key_id_from_storage_key(&prefix, key_bytes) {
                let plaintext = self.decrypt_value(key_bytes, value.value())?;
                records.push(api_key_record_from_stored(
                    key_id.to_owned(),
                    serde_json::from_slice(&plaintext)?,
                ));
            }
        }
        records.sort_by(|left, right| {
            left.issued_at_unix_secs
                .cmp(&right.issued_at_unix_secs)
                .then_with(|| left.key_id.cmp(&right.key_id))
        });
        Ok(records)
    }

    pub fn revoke_api_key(
        &self,
        principal_id: &str,
        key_id: &str,
    ) -> Result<ApiKeyRecord, StorageError> {
        let storage_key = api_key_storage_key(principal_id, key_id);
        let write_txn = self.db.begin_write()?;
        let record = {
            let mut table = write_txn.open_table(API_KEYS_V1)?;
            let Some(stored_value) = table
                .get(storage_key.as_slice())?
                .map(|stored| stored.value().to_vec())
            else {
                return Err(StorageError::UnknownApiKey {
                    principal_id: principal_id.to_owned(),
                    key_id: key_id.to_owned(),
                });
            };

            let plaintext = self.decrypt_value(storage_key.as_slice(), &stored_value)?;
            let mut stored: StoredApiKeyRecord = serde_json::from_slice(&plaintext)?;
            if stored.revoked_at_unix_secs.is_none() {
                stored.revoked_at_unix_secs = Some(unix_now_secs());
                let plaintext_record = serde_json::to_vec(&stored)?;
                let value = self.encrypt_value(storage_key.as_slice(), &plaintext_record)?;
                table.insert(storage_key.as_slice(), value.as_slice())?;
            }
            api_key_record_from_stored(key_id.to_owned(), stored)
        };
        write_txn.commit()?;
        Ok(record)
    }

    fn encrypt_oauth(&self, principal_id: &str, plaintext: &[u8]) -> Result<Vec<u8>, StorageError> {
        self.encrypt_value(principal_id.as_bytes(), plaintext)
    }

    fn decrypt_oauth(&self, principal_id: &str, value: &[u8]) -> Result<Vec<u8>, StorageError> {
        self.decrypt_value(principal_id.as_bytes(), value)
    }

    fn encrypt_value(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, StorageError> {
        let cipher = self.cipher();
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
        let payload = Payload {
            msg: plaintext,
            aad,
        };
        let ciphertext = cipher
            .encrypt(&nonce, payload)
            .map_err(|_| StorageError::AeadAuthenticationFailed)?;

        let mut value = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        value.extend_from_slice(&nonce);
        value.extend_from_slice(&ciphertext);
        Ok(value)
    }

    fn decrypt_value(&self, aad: &[u8], value: &[u8]) -> Result<Vec<u8>, StorageError> {
        if value.len() < NONCE_LEN {
            return Err(StorageError::CiphertextTooShort(value.len()));
        }

        let (nonce_bytes, ciphertext) = value.split_at(NONCE_LEN);
        let cipher = self.cipher();
        let nonce = Nonce::from_slice(nonce_bytes);
        let payload = Payload {
            msg: ciphertext,
            aad,
        };

        cipher
            .decrypt(nonce, payload)
            .map_err(|_| StorageError::AeadAuthenticationFailed)
    }

    fn cipher(&self) -> ChaCha20Poly1305 {
        let key = Key::from_slice(&self.master_key);
        ChaCha20Poly1305::new(key)
    }
}

fn api_key_storage_prefix(principal_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(principal_id.len() + 1);
    key.extend_from_slice(principal_id.as_bytes());
    key.push(0);
    key
}

fn api_key_storage_key(principal_id: &str, key_id: &str) -> Vec<u8> {
    let mut key = api_key_storage_prefix(principal_id);
    key.extend_from_slice(key_id.as_bytes());
    key
}

fn api_key_id_from_storage_key<'a>(prefix: &[u8], storage_key: &'a [u8]) -> Option<&'a str> {
    storage_key
        .strip_prefix(prefix)
        .and_then(|suffix| std::str::from_utf8(suffix).ok())
        .filter(|key_id| !key_id.is_empty())
}

fn api_key_record_from_stored(key_id: String, stored: StoredApiKeyRecord) -> ApiKeyRecord {
    ApiKeyRecord {
        key_id,
        label: stored.label,
        issued_at_unix_secs: stored.issued_at_unix_secs,
        revoked_at_unix_secs: stored.revoked_at_unix_secs,
    }
}

fn hex_prefix(bytes: &[u8], hex_chars: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(hex_chars);
    for byte in bytes.iter().take(hex_chars.div_ceil(2)) {
        out.push(HEX[(byte >> 4) as usize] as char);
        if out.len() == hex_chars {
            break;
        }
        out.push(HEX[(byte & 0x0f) as usize] as char);
        if out.len() == hex_chars {
            break;
        }
    }
    out
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
