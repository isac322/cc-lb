use std::future::Future;
use std::time::Duration;

use apalis_core::task::{Task, builder::TaskBuilder};
use cc_lb_aead::EncryptedOAuthTokens;
use cc_lb_storage_api::UpstreamRecord;
use cc_lb_storage_api::upstream::UpstreamKind;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Result, SchedulerError};
use crate::jobs::metadata_refresh::MetadataRefreshJob;
use crate::middleware::TraceparentCarrier;
use crate::retry::JobOutcome;

mod repository;
pub use repository::OAuthRefreshUpstreams;

const DEFAULT_RETRY_DELAY_SECS: u64 = 30;
const REFRESH_BEFORE_EXPIRY_SECS: u64 = 300;
// Adaptive handlers time out after 60 seconds. Keep a small commit margin without
// leaving abandoned ownership live for a second full handler timeout.
pub const OAUTH_REFRESH_LEASE_TTL_SECS: u64 = 75;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OAuthRefreshJob {
    pub upstream_id: Uuid,
    pub traceparent: Option<String>,
    #[serde(default)]
    pub expected_generation: Option<u64>,
}

impl OAuthRefreshJob {
    pub const fn new(upstream_id: Uuid) -> Self {
        Self {
            upstream_id,
            traceparent: None,
            expected_generation: None,
        }
    }

    pub const fn for_generation(upstream_id: Uuid, expected_generation: u64) -> Self {
        Self {
            upstream_id,
            traceparent: None,
            expected_generation: Some(expected_generation),
        }
    }

    pub fn idempotency_key(&self, expires_at_unix_secs: u64) -> String {
        match self.expected_generation {
            Some(generation) => format!(
                "adaptive:oauth_refresh:{}:{generation}:{expires_at_unix_secs}",
                self.upstream_id
            ),
            None => format!(
                "adaptive:oauth_refresh:{}:{expires_at_unix_secs}",
                self.upstream_id
            ),
        }
    }

    pub fn into_apalis_task<Ctx, IdType>(self, expires_at_unix_secs: u64) -> Task<Self, Ctx, IdType>
    where
        Ctx: Default,
    {
        let run_at_unix_secs = Self::run_at_for_expires_at(expires_at_unix_secs);
        let idempotency_key = self.idempotency_key(expires_at_unix_secs);
        TaskBuilder::<Self, Ctx, IdType>::new(self)
            .run_at_timestamp(run_at_unix_secs)
            .with_idempotency_key(idempotency_key)
            .build()
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

pub struct RefreshedOAuthTokens {
    pub encrypted_tokens: EncryptedOAuthTokens,
    pub expires_at_unix_secs: u64,
}

#[derive(Clone, Debug)]
pub struct OAuthRefreshJobHandler<Upstreams> {
    upstreams: Upstreams,
    config: OAuthRefreshConfig,
}

impl<Upstreams> OAuthRefreshJobHandler<Upstreams> {
    pub fn new(upstreams: Upstreams) -> Self {
        Self {
            upstreams,
            config: OAuthRefreshConfig::default(),
        }
    }

    pub const fn with_config(upstreams: Upstreams, config: OAuthRefreshConfig) -> Self {
        Self { upstreams, config }
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
        Refreshed: Future<Output = Result<RefreshedOAuthTokens>> + Send,
        Enqueue: FnOnce(MetadataRefreshJob) -> Enqueued + Send,
        Enqueued: Future<Output = Result<()>> + Send,
        Schedule: FnOnce(Uuid, u64, u64) -> Scheduled + Send,
        Scheduled: Future<Output = Result<()>> + Send,
    {
        let Some(initial_upstream) = self.upstreams.get_by_id(job.upstream_id).await? else {
            return Ok(JobOutcome::Skip);
        };
        if !is_refreshable(&initial_upstream) {
            return Ok(JobOutcome::Skip);
        }

        let starting_generation = initial_upstream.oauth_token_generation;
        if let Some(expected_generation) = job.expected_generation
            && starting_generation != expected_generation
        {
            if starting_generation < expected_generation {
                return Ok(JobOutcome::Retry {
                    delay: self.config.retry_delay,
                });
            }
            let mut metadata_job = MetadataRefreshJob::new(job.upstream_id, starting_generation);
            metadata_job.traceparent = job.traceparent.clone();
            enqueue_metadata(metadata_job).await?;
            return Ok(JobOutcome::Done);
        }
        let holder = Uuid::new_v4();
        if !self
            .upstreams
            .claim_refresh_lease(
                job.upstream_id,
                holder,
                starting_generation,
                OAUTH_REFRESH_LEASE_TTL_SECS,
            )
            .await?
        {
            if self
                .generation_advanced(job.upstream_id, starting_generation)
                .await?
            {
                return Ok(JobOutcome::Done);
            }
            if self
                .upstreams
                .read_oauth_refresh_terminal_failure(job.upstream_id)
                .await?
                .is_some_and(|failure| failure.expected_generation == starting_generation)
            {
                return Ok(JobOutcome::DeadLetter);
            }
            if job.expected_generation.is_none() {
                tracing::warn!(
                    upstream_id = %job.upstream_id,
                    "legacy oauth refresh task lost the lease; watchdog will reseed it",
                );
                return Ok(JobOutcome::Done);
            }
            return Ok(JobOutcome::Retry {
                delay: self.config.retry_delay,
            });
        }

        let upstream = match self.upstreams.get_by_id(job.upstream_id).await {
            Ok(Some(upstream)) => upstream,
            Ok(None) => {
                self.abandon_claim(
                    job.upstream_id,
                    holder,
                    "oauth upstream disappeared after refresh claim",
                )
                .await;
                return Ok(JobOutcome::Skip);
            }
            Err(error) => {
                self.abandon_claim(
                    job.upstream_id,
                    holder,
                    "oauth upstream reload failed after refresh claim",
                )
                .await;
                return Err(error);
            }
        };
        if upstream.oauth_token_generation != starting_generation {
            self.abandon_claim(
                job.upstream_id,
                holder,
                "oauth credential generation changed after refresh claim",
            )
            .await;
            return Ok(JobOutcome::Done);
        }
        if !is_refreshable(&upstream) {
            self.abandon_claim(
                job.upstream_id,
                holder,
                "oauth upstream stopped being refreshable after refresh claim",
            )
            .await;
            return Ok(JobOutcome::Skip);
        }

        match refresh(upstream).await {
            Ok(refreshed) => {
                let updated = match self
                    .upstreams
                    .complete_refresh(job.upstream_id, holder, refreshed.encrypted_tokens)
                    .await
                {
                    Ok(updated) => updated,
                    Err(error) => {
                        if self
                            .generation_advanced_best_effort(
                                job.upstream_id,
                                starting_generation,
                                "oauth refresh completion",
                            )
                            .await
                        {
                            tracing::info!(
                                upstream_id = %job.upstream_id,
                                "oauth refresh adopted a concurrently completed credential generation",
                            );
                            return Ok(JobOutcome::Done);
                        }
                        self.abandon_claim(
                            job.upstream_id,
                            holder,
                            "oauth refresh completion failed",
                        )
                        .await;
                        return Err(error);
                    }
                };
                let generation = updated.oauth_token_generation;
                schedule_next_refresh(job.upstream_id, generation, refreshed.expires_at_unix_secs)
                    .await?;
                let mut metadata_job = MetadataRefreshJob::new(job.upstream_id, generation);
                metadata_job.traceparent = job.traceparent;
                enqueue_metadata(metadata_job).await?;
                Ok(JobOutcome::Done)
            }
            Err(error) => {
                if self
                    .generation_advanced_best_effort(
                        job.upstream_id,
                        starting_generation,
                        "oauth refresh failure",
                    )
                    .await
                {
                    tracing::info!(
                        upstream_id = %job.upstream_id,
                        "oauth refresh failure superseded by a newer credential generation",
                    );
                    return Ok(JobOutcome::Done);
                }

                let terminal_error = match &error {
                    SchedulerError::TerminalJob(code) => Some(code.clone()),
                    _ => None,
                };
                let released = match self
                    .upstreams
                    .fail_refresh(job.upstream_id, holder, terminal_error)
                    .await
                {
                    Ok(released) => released,
                    Err(release_error) => {
                        tracing::warn!(
                            upstream_id = %job.upstream_id,
                            %holder,
                            error = %release_error,
                            "oauth refresh failure could not release its lease",
                        );
                        false
                    }
                };
                if !released {
                    if self
                        .generation_advanced_best_effort(
                            job.upstream_id,
                            starting_generation,
                            "oauth refresh failure lease reconciliation",
                        )
                        .await
                    {
                        tracing::info!(
                            upstream_id = %job.upstream_id,
                            "oauth refresh failure adopted a concurrently completed credential generation",
                        );
                        return Ok(JobOutcome::Done);
                    }
                    tracing::warn!(
                        upstream_id = %job.upstream_id,
                        %holder,
                        "oauth refresh failure lease was no longer live",
                    );
                }
                match error {
                    SchedulerError::TerminalJob(error) => {
                        tracing::warn!(upstream_id = %job.upstream_id, %error, "oauth refresh requires reauthorization");
                        Ok(JobOutcome::DeadLetter)
                    }
                    error => {
                        tracing::warn!(upstream_id = %job.upstream_id, error = %error, "oauth refresh job failed");
                        Ok(JobOutcome::Retry {
                            delay: self.config.retry_delay,
                        })
                    }
                }
            }
        }
    }

    async fn generation_advanced_best_effort(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
        operation: &'static str,
    ) -> bool {
        match self
            .generation_advanced(upstream_id, starting_generation)
            .await
        {
            Ok(advanced) => advanced,
            Err(error) => {
                tracing::warn!(
                    %upstream_id,
                    %error,
                    operation,
                    "oauth refresh could not verify credential generation",
                );
                false
            }
        }
    }

    async fn abandon_claim(&self, upstream_id: Uuid, holder: Uuid, reason: &str) {
        match self.upstreams.fail_refresh(upstream_id, holder, None).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::debug!(
                    %upstream_id,
                    %holder,
                    reason,
                    "oauth refresh claim was already released",
                );
            }
            Err(error) => {
                tracing::warn!(
                    %upstream_id,
                    %holder,
                    %error,
                    reason,
                    "oauth refresh claim release failed",
                );
            }
        }
    }

    async fn generation_advanced(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
    ) -> Result<bool> {
        Ok(self
            .upstreams
            .get_by_id(upstream_id)
            .await?
            .is_some_and(|upstream| upstream.oauth_token_generation > starting_generation))
    }
}

fn is_refreshable(upstream: &UpstreamRecord) -> bool {
    upstream.kind == UpstreamKind::AnthropicOauth
        && upstream.deleted_at_unix_secs.is_none()
        && upstream.oauth_credentials.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_is_generation_scoped() {
        let upstream_id = Uuid::nil();

        assert_ne!(
            OAuthRefreshJob::for_generation(upstream_id, 1).idempotency_key(1_700_000_000),
            OAuthRefreshJob::for_generation(upstream_id, 2).idempotency_key(1_700_000_000),
        );
    }
}
