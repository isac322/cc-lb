use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AeadError {
    #[error("invalid AEAD master key length: got {got}, want 32 bytes")]
    InvalidMasterKey { got: usize },

    #[error("AEAD encryption failed")]
    EncryptionFailed,

    #[error("AEAD decryption failed")]
    DecryptionFailed,

    #[error("AEAD ciphertext is shorter than the 12-byte nonce prefix")]
    CiphertextTooShort,
}

pub type AeadResult<T> = Result<T, AeadError>;
