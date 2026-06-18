use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_core::anthropic_compat::{CompatibilityKey, run_compat_fetcher};
use cc_lb_storage_api::AnthropicCompatibilityKvStore;
use tokio_util::sync::CancellationToken;

use crate::error::{Result, SchedulerError};
use crate::retry::JobOutcome;

use super::{
    AnthropicCompatRefreshJob, CompatEtagRepository, CompatFetch,
    handle_anthropic_compat_refresh_job,
};

pub async fn handle_anthropic_compat_refresh_job_with_core_fetcher<E, K>(
    job: AnthropicCompatRefreshJob,
    etags: &E,
    compatibility_kv: &K,
    cancel: &CancellationToken,
) -> Result<JobOutcome>
where
    E: CompatEtagRepository + Sync,
    K: AnthropicCompatibilityKvStore + ?Sized,
{
    let now_unix_secs = current_unix_secs()?;
    handle_anthropic_compat_refresh_job(
        job,
        etags,
        compatibility_kv,
        |compatibility_key, stored_etag| fetch_compat_key(compatibility_key, stored_etag, cancel),
        now_unix_secs,
    )
    .await
}

pub async fn fetch_compat_key(
    compatibility_key: CompatibilityKey,
    _stored_etag: Option<String>,
    cancel: &CancellationToken,
) -> Result<CompatFetch> {
    let outcome = run_compat_fetcher(compatibility_key.fetcher, cancel)
        .await
        .map_err(|error| SchedulerError::Job(error.to_string()))?;
    Ok(CompatFetch::Modified {
        value: outcome.value,
        etag: None,
        source_url: Some(outcome.source_url),
    })
}

pub fn current_unix_secs() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| SchedulerError::Job(error.to_string()))
}
