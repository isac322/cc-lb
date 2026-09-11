#![allow(non_snake_case)]

use std::error::Error;

use cc_lb_storage_api::{StorageError, StorageResult};

use super::PromptCacheObservationPurgeJobResult;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const NOW_UNIX_SECS: u64 = 1_800_000_000;
const ACTIVE_PREFIX: &str = "sha256:prompt-cache-active";

fn assert_done(result: PromptCacheObservationPurgeJobResult) {
    assert_eq!(
        result,
        PromptCacheObservationPurgeJobResult::Done {
            rows_removed: 2,
            cutoff_unix_secs: NOW_UNIX_SECS,
        }
    );
}

fn u64_to_i64(value: u64) -> StorageResult<i64> {
    i64::try_from(value).map_err(|_| StorageError::InvalidInput {
        field: "prompt cache purge cutoff unix secs".to_owned(),
        reason: "value exceeds i64::MAX".to_owned(),
    })
}

fn map_sqlx_error(error: sqlx::Error) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}

#[cfg(feature = "postgres")]
#[path = "tests/postgres.rs"]
mod postgres;

#[cfg(feature = "sqlite")]
mod sqlite;
