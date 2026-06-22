use std::time::Duration;

use cc_lb_core::anthropic_compat::{COMPATIBILITY_KEYS, CompatibilityKey};
use cc_lb_storage_api::AnthropicCompatibilityKvStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::error::{Result, SchedulerError};
use crate::middleware::TraceparentCarrier;
use crate::retry::JobOutcome;
use crate::state_stores::AnthropicCompatEtag;

mod core_fetcher;
mod etag_repository;

pub use core_fetcher::{
    current_unix_secs, fetch_compat_key, handle_anthropic_compat_refresh_job_with_core_fetcher,
};
pub use etag_repository::{CompatEtagRepository, CompatJobFuture};

pub const COMPAT_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AnthropicCompatRefreshJob {
    pub key: String,
    pub traceparent: Option<String>,
}

impl AnthropicCompatRefreshJob {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            traceparent: None,
        }
    }
}

impl TraceparentCarrier for AnthropicCompatRefreshJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompatFetch {
    NotModified {
        etag: Option<String>,
    },
    Modified {
        value: String,
        etag: Option<String>,
        source_url: Option<String>,
    },
}

pub async fn handle_anthropic_compat_refresh_job<E, K, Fetch, Fetched>(
    job: AnthropicCompatRefreshJob,
    etags: &E,
    compatibility_kv: &K,
    fetch: Fetch,
    now_unix_secs: u64,
) -> Result<JobOutcome>
where
    E: CompatEtagRepository + Sync,
    K: AnthropicCompatibilityKvStore + ?Sized,
    Fetch: FnOnce(CompatibilityKey, Option<String>) -> Fetched,
    Fetched: Future<Output = Result<CompatFetch>>,
{
    let Some(compatibility_key) = compatibility_key(&job.key) else {
        return Ok(JobOutcome::Skip);
    };

    let stored = etags.read_compat_etag(&job.key).await?;
    let stored_etag = stored.as_ref().and_then(|row| row.etag.clone());
    let fetched = match fetch(compatibility_key, stored_etag).await {
        Ok(fetched) => fetched,
        Err(error) => {
            compatibility_kv
                .put_compatibility_kv_failure(&job.key, now_unix_secs, &error.to_string())
                .await
                .map_err(storage_error)?;
            return Ok(JobOutcome::Retry {
                delay: COMPAT_REFRESH_RETRY_DELAY,
            });
        }
    };

    match fetched {
        CompatFetch::NotModified { etag } => {
            if let Some(stored) = stored.as_ref() {
                let next_etag = etag.as_deref().or(stored.etag.as_deref());
                etags
                    .upsert_compat_value(
                        &job.key,
                        next_etag,
                        &stored.last_value_hash,
                        now_unix_secs,
                    )
                    .await?;
            }
            Ok(JobOutcome::Noop)
        }
        CompatFetch::Modified {
            value,
            etag,
            source_url,
        } => {
            let modified = ModifiedCompatFetch {
                value,
                etag,
                source_url,
            };
            handle_modified(
                job,
                etags,
                compatibility_kv,
                stored.as_ref(),
                modified,
                now_unix_secs,
            )
            .await
        }
    }
}

struct ModifiedCompatFetch {
    value: String,
    etag: Option<String>,
    source_url: Option<String>,
}

async fn handle_modified<E, K>(
    job: AnthropicCompatRefreshJob,
    etags: &E,
    compatibility_kv: &K,
    stored: Option<&AnthropicCompatEtag>,
    modified: ModifiedCompatFetch,
    now_unix_secs: u64,
) -> Result<JobOutcome>
where
    E: CompatEtagRepository + Sync,
    K: AnthropicCompatibilityKvStore + ?Sized,
{
    let next_hash = compatibility_value_hash(&modified.value);
    if let Some(stored) = stored
        && stored.last_value_hash == next_hash
    {
        let next_etag = modified.etag.as_deref().or(stored.etag.as_deref());
        etags
            .upsert_compat_value(&job.key, next_etag, &next_hash, now_unix_secs)
            .await?;
        return Ok(JobOutcome::Noop);
    }

    compatibility_kv
        .put_compatibility_kv_value(
            &job.key,
            &modified.value,
            now_unix_secs,
            modified.source_url.as_deref(),
        )
        .await
        .map_err(storage_error)?;
    etags
        .upsert_compat_value(
            &job.key,
            modified.etag.as_deref(),
            &next_hash,
            now_unix_secs,
        )
        .await?;
    Ok(JobOutcome::Done)
}

pub fn compatibility_value_hash(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub fn compatibility_key(name: &str) -> Option<CompatibilityKey> {
    COMPATIBILITY_KEYS
        .iter()
        .copied()
        .find(|key| key.name == name)
}

fn storage_error(error: cc_lb_storage_api::StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
