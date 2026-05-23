use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage API placeholder error")]
    Placeholder,
}

pub type StorageResult<T> = Result<T, StorageError>;
