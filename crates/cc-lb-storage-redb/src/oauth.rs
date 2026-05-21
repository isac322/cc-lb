use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use serde::{Deserialize, Serialize};

use crate::{Storage, StorageError, OAUTH_CREDENTIALS_V1};

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

pub fn oauth_key(principal_id: &str, provider: &str) -> Vec<u8> {
    format!("{principal_id}:{provider}").into_bytes()
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
