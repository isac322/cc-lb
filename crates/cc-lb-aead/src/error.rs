use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AeadError {
    #[error("AEAD service is not implemented yet")]
    Unimplemented,
}

pub type AeadResult<T> = Result<T, AeadError>;
