use cc_lb_storage_api::StorageError;

pub fn map_sqlx_error(error: sqlx::Error) -> StorageError {
    match error {
        sqlx::Error::Database(db_error) if db_error.is_unique_violation() => {
            StorageError::Conflict {
                message: db_error.message().to_owned(),
            }
        }
        sqlx::Error::Database(db_error) if db_error.is_foreign_key_violation() => {
            StorageError::Conflict {
                message: db_error.message().to_owned(),
            }
        }
        sqlx::Error::Database(db_error) if db_error.is_check_violation() => {
            StorageError::InvalidInput {
                field: "postgres".to_owned(),
                reason: db_error.message().to_owned(),
            }
        }
        sqlx::Error::Database(db_error)
            if matches!(db_error.code().as_deref(), Some("40001") | Some("40P01")) =>
        {
            StorageError::Transient {
                retryable: true,
                source: Box::new(sqlx::Error::Database(db_error)),
            }
        }
        sqlx::Error::PoolTimedOut => StorageError::Unavailable {
            message: "postgres connection pool timed out".to_owned(),
        },
        sqlx::Error::PoolClosed => StorageError::Unavailable {
            message: "postgres connection pool closed".to_owned(),
        },
        sqlx::Error::Io(error) => StorageError::Transient {
            retryable: true,
            source: Box::new(error),
        },
        error => StorageError::Fatal {
            message: error.to_string(),
        },
    }
}
