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
pub use repository::{OAuthRefreshClaims, OAuthRefreshUpstreams};

const DEFAULT_CLAIM_TTL_SECS: u64 = 60;
const DEFAULT_CONTENTION_WAIT_SECS: u64 = 50;
const DEFAULT_CONTENTION_POLL_MILLIS: u64 = 1_000;
const DEFAULT_RETRY_DELAY_SECS: u64 = 30;

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

    pub fn idempotency_key(&self) -> String {
        format!("entity:oauth_refresh:{}", self.upstream_id)
    }

    pub fn into_apalis_task<Ctx, IdType>(self, run_at_unix_secs: u64) -> Task<Self, Ctx, IdType>
    where
        Ctx: Default,
    {
        let idempotency_key = self.idempotency_key();
        TaskBuilder::<Self, Ctx, IdType>::new(self)
            .run_at_timestamp(run_at_unix_secs)
            .with_idempotency_key(idempotency_key)
            .build()
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
    pub claim_ttl_secs: u64,
    pub contention_wait: Duration,
    pub contention_poll_interval: Duration,
    pub retry_delay: Duration,
}

impl Default for OAuthRefreshConfig {
    fn default() -> Self {
        Self {
            claim_ttl_secs: DEFAULT_CLAIM_TTL_SECS,
            contention_wait: Duration::from_secs(DEFAULT_CONTENTION_WAIT_SECS),
            contention_poll_interval: Duration::from_millis(DEFAULT_CONTENTION_POLL_MILLIS),
            retry_delay: Duration::from_secs(DEFAULT_RETRY_DELAY_SECS),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OAuthRefreshJobHandler<Claims, Upstreams> {
    claims: Claims,
    upstreams: Upstreams,
    replica_id: Uuid,
    config: OAuthRefreshConfig,
}

impl<Claims, Upstreams> OAuthRefreshJobHandler<Claims, Upstreams> {
    pub const fn new(claims: Claims, upstreams: Upstreams, replica_id: Uuid) -> Self {
        Self {
            claims,
            upstreams,
            replica_id,
            config: OAuthRefreshConfig {
                claim_ttl_secs: DEFAULT_CLAIM_TTL_SECS,
                contention_wait: Duration::from_secs(DEFAULT_CONTENTION_WAIT_SECS),
                contention_poll_interval: Duration::from_millis(DEFAULT_CONTENTION_POLL_MILLIS),
                retry_delay: Duration::from_secs(DEFAULT_RETRY_DELAY_SECS),
            },
        }
    }

    pub const fn with_config(
        claims: Claims,
        upstreams: Upstreams,
        replica_id: Uuid,
        config: OAuthRefreshConfig,
    ) -> Self {
        Self {
            claims,
            upstreams,
            replica_id,
            config,
        }
    }
}

impl<Claims, Upstreams> OAuthRefreshJobHandler<Claims, Upstreams>
where
    Claims: OAuthRefreshClaims + Send + Sync,
    Upstreams: OAuthRefreshUpstreams + Send + Sync,
{
    pub async fn handle<Refresh, Refreshed, Enqueue, Enqueued>(
        &self,
        job: OAuthRefreshJob,
        now_unix_secs: u64,
        refresh: Refresh,
        enqueue_metadata: Enqueue,
    ) -> Result<JobOutcome>
    where
        Refresh: FnOnce(UpstreamRecord) -> Refreshed + Send,
        Refreshed: Future<Output = Result<EncryptedOAuthTokens>> + Send,
        Enqueue: FnOnce(MetadataRefreshJob) -> Enqueued + Send,
        Enqueued: Future<Output = Result<()>> + Send,
    {
        let Some(upstream) = self.upstreams.get_by_id(job.upstream_id).await? else {
            record_oauth_refresh_status("skip");
            return Ok(JobOutcome::Done);
        };
        if !is_refreshable(&upstream) {
            record_oauth_refresh_status("skip");
            return Ok(JobOutcome::Done);
        }

        let holder = holder_name(self.replica_id);
        if !self
            .claims
            .try_acquire(
                job.upstream_id,
                &holder,
                self.config.claim_ttl_secs,
                now_unix_secs,
            )
            .await?
        {
            return self
                .wait_for_other_holder(job.upstream_id, upstream.oauth_token_generation)
                .await;
        }

        match refresh(upstream).await {
            Ok(tokens) => {
                let updated = match self
                    .upstreams
                    .complete_refresh(job.upstream_id, self.replica_id, tokens)
                    .await
                {
                    Ok(updated) => updated,
                    Err(error) => {
                        self.claims
                            .release_if_holder(job.upstream_id, &holder)
                            .await?;
                        return Err(error);
                    }
                };
                let generation = updated.oauth_token_generation;
                self.claims
                    .complete_and_bump_generation(job.upstream_id, &holder, generation)
                    .await?;
                let mut metadata_job = MetadataRefreshJob::new(job.upstream_id, generation);
                metadata_job.traceparent = job.traceparent;
                enqueue_metadata(metadata_job).await?;
                record_oauth_refresh_status("success");
                Ok(JobOutcome::Done)
            }
            Err(error) => {
                self.claims
                    .release_if_holder(job.upstream_id, &holder)
                    .await?;
                tracing::warn!(upstream_id = %job.upstream_id, error = %error, "oauth refresh job failed");
                record_oauth_refresh_status("retry");
                Ok(JobOutcome::Retry {
                    delay: self.config.retry_delay,
                })
            }
        }
    }

    async fn wait_for_other_holder(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
    ) -> Result<JobOutcome> {
        let deadline = tokio::time::Instant::now() + self.config.contention_wait;
        loop {
            match self
                .upstreams
                .read_oauth_token_generation(upstream_id)
                .await?
            {
                Some(generation) if generation > starting_generation => {
                    record_oauth_refresh_status("single_flight_join");
                    return Ok(JobOutcome::Done);
                }
                Some(_) => {}
                None => {
                    record_oauth_refresh_status("skip");
                    return Ok(JobOutcome::Done);
                }
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                record_oauth_refresh_status("retry");
                return Ok(JobOutcome::Retry {
                    delay: self.config.retry_delay,
                });
            }
            tokio::time::sleep(self.config.contention_poll_interval.min(deadline - now)).await;
        }
    }
}

fn is_refreshable(upstream: &UpstreamRecord) -> bool {
    upstream.kind == UpstreamKind::AnthropicOauth
        && upstream.enabled
        && upstream.deleted_at_unix_secs.is_none()
        && upstream.oauth_credentials.is_some()
}

fn holder_name(replica_id: Uuid) -> String {
    format!("apalis-worker:{replica_id}")
}

fn record_oauth_refresh_status(status: &'static str) {
    metrics::counter!("cclb_scheduler_jobs_total", "job_type" => "oauth_refresh", "status" => status)
        .increment(1);
}
