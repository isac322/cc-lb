use std::time::Duration;

use cc_lb_control::anthropic_compat::{COMPATIBILITY_KEYS, CompatibilityKey};
use cc_lb_storage_api::AnthropicCompatibilityKvStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::error::{Result, SchedulerError};
use crate::middleware::TraceparentCarrier;
use crate::retry::JobOutcome;
use crate::state_stores::AnthropicCompatEtag;

mod core_fetcher;
mod etag_repository;

pub use core_fetcher::{fetch_compat_key, handle_anthropic_compat_refresh_job_with_core_fetcher};
pub use etag_repository::{CompatEtagRepository, CompatJobFuture};

pub const COMPAT_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AnthropicCompatRefreshJob {
    /// `Some(key)` refreshes that registered compatibility key; `None`
    /// refreshes every key in `COMPATIBILITY_KEYS`. Serialized jobs written by
    /// older binaries always carry `Some` and keep their keyed semantics.
    pub key: Option<String>,
    pub traceparent: Option<String>,
}

impl AnthropicCompatRefreshJob {
    /// Recurring-cron payload: refresh every registered compatibility key in
    /// one queued job so the singleton idempotency key stays unique per tick.
    pub fn all() -> Self {
        Self {
            key: None,
            traceparent: None,
        }
    }

    /// Keyed payload: refresh one registered compatibility key.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
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
    Fetch: Fn(CompatibilityKey, Option<String>) -> Fetched,
    Fetched: Future<Output = Result<CompatFetch>>,
{
    let single_key;
    let keys: &[CompatibilityKey] = match job.key.as_deref() {
        Some(name) => {
            let Some(compatibility_key) = compatibility_key(name) else {
                return Ok(JobOutcome::Skip);
            };
            single_key = [compatibility_key];
            &single_key
        }
        None => COMPATIBILITY_KEYS,
    };

    // Keys refresh concurrently and independently: each channel's fetch can
    // take up to its full primary+npm timeout budget, so running them in
    // sequence would overrun the singleton job timeout, and one key's fetch
    // failure or storage error never blocks the others.
    let results = futures_util::future::join_all(keys.iter().map(|compatibility_key| {
        refresh_compat_key(
            compatibility_key,
            etags,
            compatibility_kv,
            &fetch,
            now_unix_secs,
        )
    }))
    .await;

    let mut outcome = JobOutcome::Skip;
    let mut first_error = None;
    for result in results {
        match result {
            Ok(next) => outcome = merge_compat_outcome(outcome, next),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(outcome),
    }
}

fn merge_compat_outcome(current: JobOutcome, next: JobOutcome) -> JobOutcome {
    // A pending retry dominates: failed keys reschedule the job while
    // refreshed keys re-run as cheap no-ops through the unchanged-hash guard.
    match (&current, &next) {
        (JobOutcome::Retry { .. }, _) => current,
        (_, JobOutcome::Retry { .. }) => next,
        (JobOutcome::Done, _) | (_, JobOutcome::Done) => JobOutcome::Done,
        (JobOutcome::Noop, _) | (_, JobOutcome::Noop) => JobOutcome::Noop,
        _ => next,
    }
}

async fn refresh_compat_key<E, K, Fetch, Fetched>(
    compatibility_key: &CompatibilityKey,
    etags: &E,
    compatibility_kv: &K,
    fetch: &Fetch,
    now_unix_secs: u64,
) -> Result<JobOutcome>
where
    E: CompatEtagRepository + Sync,
    K: AnthropicCompatibilityKvStore + ?Sized,
    Fetch: Fn(CompatibilityKey, Option<String>) -> Fetched,
    Fetched: Future<Output = Result<CompatFetch>>,
{
    let stored = etags.read_compat_etag(compatibility_key.name).await?;
    let stored_etag = stored.as_ref().and_then(|row| row.etag.clone());
    let fetched = match fetch(*compatibility_key, stored_etag).await {
        Ok(fetched) => fetched,
        Err(error) => {
            compatibility_kv
                .put_compatibility_kv_failure(
                    compatibility_key.name,
                    now_unix_secs,
                    &error.to_string(),
                )
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
                        compatibility_key.name,
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
                compatibility_key.name,
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
    key_name: &str,
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
            .upsert_compat_value(key_name, next_etag, &next_hash, now_unix_secs)
            .await?;
        return Ok(JobOutcome::Noop);
    }

    compatibility_kv
        .put_compatibility_kv_value(
            key_name,
            &modified.value,
            now_unix_secs,
            modified.source_url.as_deref(),
        )
        .await
        .map_err(storage_error)?;
    etags
        .upsert_compat_value(
            key_name,
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
