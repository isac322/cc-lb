use cc_lb_aead::{AeadError, AeadService};
use cc_lb_storage_api::StorageError;
use serde::{Serialize, de::DeserializeOwned};

pub(crate) fn oauth_aad(principal_id: &str, provider: &str) -> Vec<u8> {
    format!("oauth:{principal_id}:{provider}").into_bytes()
}

pub(crate) fn api_key_aad(principal_id: &str, key_id: &str) -> Vec<u8> {
    format!("api-key:{principal_id}:{key_id}").into_bytes()
}

#[allow(dead_code)]
pub(crate) fn anthropic_api_key_aad(storage_key: &str) -> Vec<u8> {
    format!("anthropic-api-key:{storage_key}").into_bytes()
}

pub(crate) fn encrypt_json<T: Serialize>(
    aead: &AeadService,
    value: &T,
    aad: &[u8],
) -> Result<Vec<u8>, StorageError> {
    let plaintext = serde_json::to_vec(value)?;
    aead.encrypt(&plaintext, aad).map_err(storage_aead_error)
}

pub(crate) fn decrypt_json<T: DeserializeOwned>(
    aead: &AeadService,
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<T, StorageError> {
    let plaintext = aead.decrypt(ciphertext, aad).map_err(storage_aead_error)?;
    serde_json::from_slice(&plaintext).map_err(StorageError::from)
}

fn storage_aead_error(error: AeadError) -> StorageError {
    StorageError::Aead(error.to_string())
}
