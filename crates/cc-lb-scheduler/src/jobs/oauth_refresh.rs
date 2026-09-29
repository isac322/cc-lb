use std::future::Future;
use std::time::Duration;

use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::UpstreamRecord;
use cc_lb_storage_api::upstream::UpstreamKind;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;
use crate::jobs::metadata_refresh::MetadataRefreshJob;
use crate::middleware::TraceparentCarrier;
use crate::retry::JobOutcome;

mod repository;
pub use repository::OAuthRefreshUpstreams;

const DEFAULT_RETRY_DELAY_SECS: u64 = 30;
const REFRESH_BEFORE_EXPIRY_SECS: u64 = 300;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OAuthRefreshJob {
    pub upstream_id: Uuid,
    pub traceparent: Option<String>,
}

impl OAuthRefreshJob {
    pub const fn new(upstream_id: Uuid) -> Self {
        Self {
            upstream_id,
            traceparent: None,
        }
    }

    pub fn idempotency_key(&self, expires_at_unix_secs: u64) -> String {
        format!(
            "adaptive:oauth_refresh:{}:{}",
            self.upstream_id, expires_at_unix_secs
        )
    }

    pub const fn run_at_for_expires_at(expires_at_unix_secs: u64) -> u64 {
        expires_at_unix_secs.saturating_sub(REFRESH_BEFORE_EXPIRY_SECS)
    }
}

impl TraceparentCarrier for OAuthRefreshJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OAuthRefreshConfig {
    pub retry_delay: Duration,
}

impl Default for OAuthRefreshConfig {
    fn default() -> Self {
        Self {
            retry_delay: Duration::from_secs(DEFAULT_RETRY_DELAY_SECS),
        }
    }
}

#[derive(Debug)]
pub struct RefreshedOAuthTokens {
    pub encrypted_tokens: EncryptedOAuthTokens,
    pub expires_at_unix_secs: u64,
}

/// Result of attempting a scheduled OAuth refresh.
#[derive(Debug)]
pub enum RefreshOutcome {
    Refreshed(RefreshedOAuthTokens),
    /// Credential is long-lived; refreshing it would destroy it.
    ///
    /// Refreshing a 365-day Anthropic access token revokes it and downgrades
    /// the upstream to an 8-hour credential, so it must never be attempted.
    NotRefreshable,
}

#[derive(Clone, Debug)]
pub struct OAuthRefreshJobHandler<Upstreams> {
    upstreams: Upstreams,
    replica_id: Uuid,
    config: OAuthRefreshConfig,
}

impl<Upstreams> OAuthRefreshJobHandler<Upstreams> {
    pub fn new(upstreams: Upstreams, replica_id: Uuid) -> Self {
        Self {
            upstreams,
            replica_id,
            config: OAuthRefreshConfig::default(),
        }
    }

    pub const fn with_config(
        upstreams: Upstreams,
        replica_id: Uuid,
        config: OAuthRefreshConfig,
    ) -> Self {
        Self {
            upstreams,
            replica_id,
            config,
        }
    }
}

impl<Upstreams> OAuthRefreshJobHandler<Upstreams>
where
    Upstreams: OAuthRefreshUpstreams + Send + Sync,
{
    pub async fn handle<Refresh, Refreshed, Enqueue, Enqueued, Schedule, Scheduled>(
        &self,
        job: OAuthRefreshJob,
        _now_unix_secs: u64,
        refresh: Refresh,
        enqueue_metadata: Enqueue,
        schedule_next_refresh: Schedule,
    ) -> Result<JobOutcome>
    where
        Refresh: FnOnce(UpstreamRecord) -> Refreshed + Send,
        Refreshed: Future<Output = Result<RefreshOutcome>> + Send,
        Enqueue: FnOnce(MetadataRefreshJob) -> Enqueued + Send,
        Enqueued: Future<Output = Result<()>> + Send,
        Schedule: FnOnce(Uuid, u64) -> Scheduled + Send,
        Scheduled: Future<Output = Result<()>> + Send,
    {
        let Some(upstream) = self.upstreams.get_by_id(job.upstream_id).await? else {
            return Ok(JobOutcome::Skip);
        };
        if !is_refreshable(&upstream) {
            return Ok(JobOutcome::Skip);
        }

        match refresh(upstream).await {
            Ok(RefreshOutcome::Refreshed(refreshed)) => {
                let updated = match self
                    .upstreams
                    .complete_refresh(job.upstream_id, self.replica_id, refreshed.encrypted_tokens)
                    .await
                {
                    Ok(updated) => updated,
                    Err(error) => return Err(error),
                };
                let generation = updated.oauth_token_generation;
                let mut metadata_job = MetadataRefreshJob::new(job.upstream_id, generation);
                metadata_job.traceparent = job.traceparent;
                enqueue_metadata(metadata_job).await?;
                schedule_next_refresh(job.upstream_id, refreshed.expires_at_unix_secs).await?;
                Ok(JobOutcome::Done)
            }
            Ok(RefreshOutcome::NotRefreshable) => Ok(JobOutcome::Skip),
            Err(error) => {
                tracing::warn!(upstream_id = %job.upstream_id, error = %error, "oauth refresh job failed");
                Ok(JobOutcome::Retry {
                    delay: self.config.retry_delay,
                })
            }
        }
    }
}

fn is_refreshable(upstream: &UpstreamRecord) -> bool {
    upstream.kind == UpstreamKind::AnthropicOauth
        && upstream.deleted_at_unix_secs.is_none()
        && upstream.oauth_credentials.is_some()
}
