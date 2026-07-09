use cc_lb_clock::{Clock, unix_secs};
use cc_lb_engine::anthropic_compat::{CompatibilityKey, run_compat_fetcher};
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
    clock: &dyn Clock,
) -> Result<JobOutcome>
where
    E: CompatEtagRepository + Sync,
    K: AnthropicCompatibilityKvStore + ?Sized,
{
    handle_anthropic_compat_refresh_job(
        job,
        etags,
        compatibility_kv,
        |compatibility_key, stored_etag| fetch_compat_key(compatibility_key, stored_etag, cancel),
        unix_secs(clock.now()),
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
