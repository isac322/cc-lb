use std::future::Future;
use std::time::Duration;

use apalis_core::task::{Task, builder::TaskBuilder};
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
        Refreshed: Future<Output = Result<RefreshedOAuthTokens>> + Send,
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
            Ok(refreshed) => {
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

#[cfg(test)]
mod tests {
    use cc_lb_aead::EncryptedOAuthTokens;

    use super::*;

    #[test]
    fn is_refreshable_matches_eligibility_matrix() {
        struct Case {
            name: &'static str,
            kind: UpstreamKind,
            enabled: bool,
            deleted: bool,
            has_credentials: bool,
            expected: bool,
        }

        let cases = [
            Case {
                name: "oauth upstream with credentials",
                kind: UpstreamKind::AnthropicOauth,
                enabled: true,
                deleted: false,
                has_credentials: true,
                expected: true,
            },
            Case {
                name: "disabled oauth upstream with credentials",
                kind: UpstreamKind::AnthropicOauth,
                enabled: false,
                deleted: false,
                has_credentials: true,
                expected: true,
            },
            Case {
                name: "deleted oauth upstream",
                kind: UpstreamKind::AnthropicOauth,
                enabled: true,
                deleted: true,
                has_credentials: true,
                expected: false,
            },
            Case {
                name: "oauth upstream without credentials",
                kind: UpstreamKind::AnthropicOauth,
                enabled: true,
                deleted: false,
                has_credentials: false,
                expected: false,
            },
            Case {
                name: "non-oauth upstream",
                kind: UpstreamKind::AnthropicApiKey,
                enabled: true,
                deleted: false,
                has_credentials: true,
                expected: false,
            },
        ];

        for case in cases {
            let upstream = UpstreamRecord {
                id: Uuid::from_u128(1),
                kind: case.kind,
                enabled: case.enabled,
                deleted_at_unix_secs: case.deleted.then_some(1_800_000_000),
                oauth_credentials: case
                    .has_credentials
                    .then(|| EncryptedOAuthTokens::from_ciphertext(vec![1])),
                ..UpstreamRecord::default()
            };

            assert_eq!(
                is_refreshable(&upstream),
                case.expected,
                "case: {}",
                case.name
            );
        }
    }
}
