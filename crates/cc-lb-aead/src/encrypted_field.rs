use std::fmt;
use std::marker::PhantomData;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{AeadError, AeadResult};
use crate::service::AeadService;

/// A generic wrapper for AEAD-encrypted fields bound to AAD (Associated Authenticated Data).
///
/// `AeadEncryptedField<T>` stores an encrypted blob of a serializable value.
/// Encryption and decryption are bound to an Associated Authenticated Data (AAD)
/// value, typically a resource identifier. Decryption fails if the AAD does not match.
///
/// The wrapped plaintext type `T` must implement `Serialize` and `Deserialize`.
/// The ciphertext is stored as raw bytes internally and serialized as base64 for JSON
/// or raw bytes for binary formats (BYTEA, redb blob).
///
/// # Security
/// - Decryption fails if AAD does not match.
/// - Plaintext is never cached or exposed through Debug output.
/// - Ciphertext tampering is detected by authenticated encryption.
#[derive(Clone)]
pub struct AeadEncryptedField<T: Serialize + for<'de> Deserialize<'de>> {
    ciphertext: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + for<'de> Deserialize<'de>> AeadEncryptedField<T> {
    /// Encrypt a value with AEAD and bind it to AAD.
    ///
    /// The plaintext is serialized to JSON, encrypted, and the nonce is prepended
    /// to the ciphertext. Decryption requires the same AAD.
    ///
    /// # Arguments
    ///
    /// * `aead` - The AEAD service instance.
    /// * `plaintext` - The value to encrypt.
    /// * `aad` - Associated Authenticated Data (e.g., resource ID). Non-empty recommended.
    ///
    /// # Returns
    ///
    /// A new `AeadEncryptedField` wrapping the ciphertext, or an error if
    /// serialization or encryption fails.
    pub fn encrypt(aead: &AeadService, plaintext: &T, aad: &[u8]) -> AeadResult<Self> {
        let json = serde_json::to_vec(plaintext).map_err(|_| AeadError::EncryptionFailed)?;
        let ciphertext = aead.encrypt(&json, aad)?;
        Ok(Self {
            ciphertext,
            _phantom: PhantomData,
        })
    }

    /// Decrypt the wrapped value using AEAD, authenticated against AAD.
    ///
    /// The decrypted bytes are deserialized from JSON.
    ///
    /// # Arguments
    ///
    /// * `aead` - The AEAD service instance.
    /// * `aad` - Associated Authenticated Data (must match encryption AAD).
    ///
    /// # Returns
    ///
    /// The decrypted value, or an error if decryption fails (wrong AAD, tampered data)
    /// or deserialization fails.
    pub fn decrypt(&self, aead: &AeadService, aad: &[u8]) -> AeadResult<T> {
        let json = aead.decrypt(&self.ciphertext, aad)?;
        serde_json::from_slice(&json).map_err(|_| AeadError::DecryptionFailed)
    }

    /// Access the raw ciphertext for external storage/serialization.
    ///
    /// This is useful for backends that need the raw bytes directly
    /// (e.g., postgres BYTEA, redb BLOB).
    pub fn ciphertext(&self) -> &[u8] {
        &self.ciphertext
    }

    #[allow(dead_code)]
    pub fn from_ciphertext(ciphertext: Vec<u8>) -> Self {
        Self {
            ciphertext,
            _phantom: PhantomData,
        }
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> fmt::Debug for AeadEncryptedField<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = std::any::type_name::<T>().split("::").last().unwrap_or("T");
        write!(
            f,
            "AeadEncryptedField<{}({} bytes)>",
            type_name,
            self.ciphertext.len()
        )
    }
}

// Serde: serialize as base64 string (JSON-friendly and human-readable)
impl<T: Serialize + for<'de> Deserialize<'de>> Serialize for AeadEncryptedField<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = STANDARD.encode(&self.ciphertext);
        serializer.serialize_str(&encoded)
    }
}

// Serde: deserialize from base64 string
impl<'de, T: Serialize + for<'d> Deserialize<'d>> Deserialize<'de> for AeadEncryptedField<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let ciphertext = STANDARD
            .decode(&encoded)
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            ciphertext,
            _phantom: PhantomData,
        })
    }
}

/// OAuth token bundle: the plaintext shape encrypted into `EncryptedOAuthTokens`.
///
/// This struct represents the upstream credentials for OAuth-based authentication.
/// It is the plaintext type that gets encrypted by `AeadEncryptedField`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuthTokenBundle {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at_unix_secs: u64,
    pub scopes: Vec<String>,
}

/// Type alias for encrypted OAuth tokens.
pub type EncryptedOAuthTokens = AeadEncryptedField<OAuthTokenBundle>;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_service() -> AeadService {
        AeadService::from_master_key([42; 32])
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let aead = test_service();
        let original = OAuthTokenBundle {
            access_token: "sk-test-access".to_string(),
            refresh_token: "sk-test-refresh".to_string(),
            expires_at_unix_secs: 1234567890,
            scopes: vec!["scope1".to_string(), "scope2".to_string()],
        };
        let aad = b"upstream-id-001";

        let encrypted =
            AeadEncryptedField::encrypt(&aead, &original, aad).expect("encrypt succeeds");
        let decrypted = encrypted.decrypt(&aead, aad).expect("decrypt succeeds");

        assert_eq!(decrypted.access_token, original.access_token);
        assert_eq!(decrypted.refresh_token, original.refresh_token);
        assert_eq!(
            decrypted.expires_at_unix_secs,
            original.expires_at_unix_secs
        );
        assert_eq!(decrypted.scopes, original.scopes);
    }

    #[test]
    fn wrong_aad_fails_decrypt() {
        let aead = test_service();
        let bundle = OAuthTokenBundle {
            access_token: "sk-test".to_string(),
            refresh_token: "sk-test-refresh".to_string(),
            expires_at_unix_secs: 1234567890,
            scopes: vec![],
        };

        let encrypted = AeadEncryptedField::encrypt(&aead, &bundle, b"upstream-001")
            .expect("encrypt with AAD=upstream-001");

        let result = encrypted.decrypt(&aead, b"upstream-002");
        assert!(matches!(result, Err(AeadError::DecryptionFailed)));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let aead = test_service();
        let bundle = OAuthTokenBundle {
            access_token: "sk-test".to_string(),
            refresh_token: "sk-test-refresh".to_string(),
            expires_at_unix_secs: 1234567890,
            scopes: vec![],
        };
        let aad = b"upstream-001";

        let mut encrypted = AeadEncryptedField::encrypt(&aead, &bundle, aad).expect("encrypt");

        // Tamper with last byte of ciphertext
        if let Some(last) = encrypted.ciphertext.last_mut() {
            *last ^= 0x01;
        }

        let result = encrypted.decrypt(&aead, aad);
        assert!(matches!(result, Err(AeadError::DecryptionFailed)));
    }

    #[test]
    fn serde_base64_roundtrip() {
        let aead = test_service();
        let bundle = OAuthTokenBundle {
            access_token: "sk-test".to_string(),
            refresh_token: "sk-test-refresh".to_string(),
            expires_at_unix_secs: 1234567890,
            scopes: vec!["scope1".to_string()],
        };
        let aad = b"upstream-001";

        let encrypted = AeadEncryptedField::encrypt(&aead, &bundle, aad).expect("encrypt");

        // Serialize to JSON (uses base64)
        let json = serde_json::to_string(&encrypted).expect("serialize to json");
        assert!(json.contains('"')); // JSON string quotes
        // Base64 is human-readable within the JSON
        assert!(json.chars().all(|c| c.is_ascii()));

        // Deserialize back
        let decrypted_field: AeadEncryptedField<OAuthTokenBundle> =
            serde_json::from_str(&json).expect("deserialize from json");

        let decrypted = decrypted_field
            .decrypt(&aead, aad)
            .expect("decrypt after serde roundtrip");

        assert_eq!(decrypted.access_token, bundle.access_token);
        assert_eq!(decrypted.refresh_token, bundle.refresh_token);
    }

    #[test]
    fn serde_bytes_roundtrip() {
        let aead = test_service();
        let bundle = OAuthTokenBundle {
            access_token: "sk-test".to_string(),
            refresh_token: "sk-test-refresh".to_string(),
            expires_at_unix_secs: 1234567890,
            scopes: vec!["scope1".to_string()],
        };
        let aad = b"upstream-001";

        let encrypted = AeadEncryptedField::encrypt(&aead, &bundle, aad).expect("encrypt");

        // For raw bytes storage (postgres BYTEA, redb blob), we access ciphertext directly
        let raw_bytes = encrypted.ciphertext().to_vec();

        // Reconstruct from raw bytes
        let reconstructed = AeadEncryptedField::<OAuthTokenBundle>::from_ciphertext(raw_bytes);

        let decrypted = reconstructed
            .decrypt(&aead, aad)
            .expect("decrypt after bytes roundtrip");

        assert_eq!(decrypted.access_token, bundle.access_token);
        assert_eq!(decrypted.refresh_token, bundle.refresh_token);
    }

    #[test]
    fn debug_does_not_expose_plaintext() {
        let aead = test_service();
        let bundle = OAuthTokenBundle {
            access_token: "sk-secret-token-that-must-not-appear".to_string(),
            refresh_token: "sk-refresh-secret".to_string(),
            expires_at_unix_secs: 1234567890,
            scopes: vec![],
        };
        let aad = b"upstream-001";

        let encrypted = AeadEncryptedField::encrypt(&aead, &bundle, aad).expect("encrypt");

        let debug_output = format!("{:?}", encrypted);
        assert!(debug_output.contains("AeadEncryptedField"));
        assert!(debug_output.contains("bytes"));
        assert!(!debug_output.contains("sk-secret"));
        assert!(!debug_output.contains("sk-refresh"));
        assert!(!debug_output.contains(&bundle.access_token));
        assert!(!debug_output.contains(&bundle.refresh_token));
    }
}
