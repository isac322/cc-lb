use std::future::Future;
use std::sync::Arc;

use apalis_core::task::{Task, builder::TaskBuilder};
use cc_lb_aead::AeadService;
use cc_lb_core::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_core::anthropic_metadata::MetadataHttpClient;
use cc_lb_storage_api::{
    AnthropicCompatibilityKvStore, Storage, StorageError, UpstreamRecord, UpstreamStore,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::{Result, SchedulerError};
use crate::middleware::TraceparentCarrier;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataRefreshJob {
    pub upstream_id: Uuid,
    pub credential_generation: u64,
    pub traceparent: Option<String>,
}

impl MetadataRefreshJob {
    pub const fn new(upstream_id: Uuid, credential_generation: u64) -> Self {
        Self {
            upstream_id,
            credential_generation,
            traceparent: None,
        }
    }

    pub fn idempotency_key(&self) -> String {
        format!(
            "entity:metadata_refresh:{}:{}",
            self.upstream_id, self.credential_generation
        )
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

impl TraceparentCarrier for MetadataRefreshJob {
    fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    fn set_traceparent(&mut self, traceparent: Option<String>) {
        self.traceparent = traceparent;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataRefreshJobOutcome {
    Applied,
    Stale,
    UpstreamRemoved,
}

#[derive(Clone, Debug)]
pub struct MetadataRefreshJobHandler<Runner> {
    runner: Runner,
}

impl<Runner> MetadataRefreshJobHandler<Runner> {
    pub const fn new(runner: Runner) -> Self {
        Self { runner }
    }
}

impl<Runner> MetadataRefreshJobHandler<Runner>
where
    Runner: MetadataRefreshRunner + Send + Sync,
{
    pub async fn handle(&self, job: MetadataRefreshJob) -> Result<MetadataRefreshJobOutcome> {
        let Some(upstream) = self.runner.load_upstream(job.upstream_id).await? else {
            record_metadata_refresh_status("upstream_removed");
            return Ok(MetadataRefreshJobOutcome::UpstreamRemoved);
        };
        if upstream.oauth_token_generation > job.credential_generation {
            record_metadata_refresh_status("stale");
            return Ok(MetadataRefreshJobOutcome::Stale);
        }
        self.runner.run_metadata_refresh(&upstream).await?;
        record_metadata_refresh_status("applied");
        Ok(MetadataRefreshJobOutcome::Applied)
    }
}

pub trait MetadataRefreshRunner {
    fn load_upstream(
        &self,
        upstream_id: Uuid,
    ) -> impl Future<Output = Result<Option<UpstreamRecord>>> + Send + '_;

    fn run_metadata_refresh<'a>(
        &'a self,
        upstream: &'a UpstreamRecord,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

#[derive(Clone)]
pub struct CoreMetadataRefreshRunner {
    storage: Arc<dyn Storage>,
    aead: Arc<AeadService>,
    client: MetadataHttpClient,
    cancel: CancellationToken,
}

impl CoreMetadataRefreshRunner {
    pub fn new(
        storage: Arc<dyn Storage>,
        aead: Arc<AeadService>,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_client(
            storage,
            aead,
            cc_lb_core::make_metadata_http_client(),
            cancel,
        )
    }

    pub const fn with_client(
        storage: Arc<dyn Storage>,
        aead: Arc<AeadService>,
        client: MetadataHttpClient,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            storage,
            aead,
            client,
            cancel,
        }
    }
}

impl MetadataRefreshRunner for CoreMetadataRefreshRunner {
    async fn load_upstream(&self, upstream_id: Uuid) -> Result<Option<UpstreamRecord>> {
        UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await
            .map_err(storage_error)
    }

    async fn run_metadata_refresh(&self, upstream: &UpstreamRecord) -> Result<()> {
        let access_token = upstream
            .oauth_credentials
            .as_ref()
            .ok_or_else(|| {
                SchedulerError::Job("metadata refresh missing oauth credentials".to_owned())
            })?
            .decrypt(self.aead.as_ref(), upstream.id.as_bytes())
            .map_err(|_| {
                SchedulerError::Job("metadata refresh oauth credentials decrypt failed".to_owned())
            })?
            .access_token;
        let user_agent = self.user_agent().await?;
        cc_lb_core::subscription_metadata_hook::run_metadata_refresh(
            self.storage.clone(),
            &self.client,
            upstream.id,
            &access_token,
            &user_agent,
            &self.cancel,
        )
        .await
        .map_err(|error| SchedulerError::Job(error.to_string()))
    }
}

impl CoreMetadataRefreshRunner {
    async fn user_agent(&self) -> Result<String> {
        let version = AnthropicCompatibilityKvStore::get_compatibility_kv(
            self.storage.as_ref(),
            CLAUDE_CODE_STABLE_VERSION_KEY,
        )
        .await
        .map_err(storage_error)?
        .map(|record| record.value)
        .unwrap_or_else(|| CLAUDE_CODE_STABLE_VERSION_FALLBACK.to_owned());
        Ok(claude_code_user_agent(&version))
    }
}

fn storage_error(error: StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}

fn record_metadata_refresh_status(status: &'static str) {
    ::metrics::counter!(crate::scheduler_metrics::METADATA_REFRESH_STATUS_TOTAL, "status" => status)
        .increment(1);
}

#[cfg(test)]
mod tests;
