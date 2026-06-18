use std::collections::HashSet;
use std::future::Future;

use cc_lb_core::anthropic_compat::COMPATIBILITY_KEYS;
use cc_lb_storage_api::{StorageError, UpstreamRecord, UpstreamStore, upstream::UpstreamKind};
use serde::Serialize;
use uuid::Uuid;

use super::SchedulerReconcileJob;
use crate::error::{Result, SchedulerError};
use crate::jobs::compat::AnthropicCompatRefreshJob;
use crate::jobs::oauth_refresh::OAuthRefreshJob;
use crate::jobs::oauth_usage_poll::OAuthUsagePollJob;
use crate::jobs::warmup::UpstreamWarmupJob;

const PAGE_LIMIT: usize = 100;
pub(super) const ENTITY_MAX_ATTEMPTS: i32 = 5;
const WARMUP_JOB_TYPE: &str = "entity:warmup";
const OAUTH_REFRESH_JOB_TYPE: &str = "entity:oauth_refresh";
const OAUTH_USAGE_POLL_JOB_TYPE: &str = "entity:oauth_usage_poll";
const COMPAT_REFRESH_JOB_TYPE: &str = "entity:anthropic_compat_refresh";
#[cfg(feature = "postgres")]
pub(super) const UPSTREAM_ENTITY_TYPES: [&str; 3] = [
    WARMUP_JOB_TYPE,
    OAUTH_REFRESH_JOB_TYPE,
    OAUTH_USAGE_POLL_JOB_TYPE,
];

pub trait ReconcileUpstreams: Send + Sync {
    fn list(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<UpstreamRecord>>> + Send + '_;
}

impl<T> ReconcileUpstreams for T
where
    T: UpstreamStore + Send + Sync,
{
    async fn list(&self, after: Option<Uuid>, limit: usize) -> Result<Vec<UpstreamRecord>> {
        UpstreamStore::list(self, after, limit)
            .await
            .map_err(storage_error)
    }
}

#[derive(Clone, Debug)]
pub(super) struct ReconcileJobSpec {
    pub(super) job_type: &'static str,
    pub(super) idempotency_key: String,
    pub(super) payload: Vec<u8>,
}

pub(super) async fn collect_specs<Upstreams>(
    upstreams: &Upstreams,
    job: &SchedulerReconcileJob,
    now_unix_secs: u64,
) -> Result<(Vec<ReconcileJobSpec>, HashSet<String>)>
where
    Upstreams: ReconcileUpstreams,
{
    let mut specs = Vec::new();
    let mut upstream_keys = HashSet::new();
    let mut after = None;
    loop {
        let page = upstreams.list(after, PAGE_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        for upstream in page {
            append_upstream_specs(
                &mut specs,
                &mut upstream_keys,
                &upstream,
                job,
                now_unix_secs,
            )?;
        }
    }
    for key in COMPATIBILITY_KEYS {
        let mut payload = AnthropicCompatRefreshJob::new(key.name);
        payload.traceparent = job.traceparent.clone();
        specs.push(spec(COMPAT_REFRESH_JOB_TYPE, key.name, &payload)?);
    }
    Ok((specs, upstream_keys))
}

fn append_upstream_specs(
    specs: &mut Vec<ReconcileJobSpec>,
    upstream_keys: &mut HashSet<String>,
    upstream: &UpstreamRecord,
    job: &SchedulerReconcileJob,
    now_unix_secs: u64,
) -> Result<()> {
    if !is_reconcilable_oauth(upstream) {
        return Ok(());
    }
    push_upstream_spec(
        specs,
        upstream_keys,
        OAUTH_REFRESH_JOB_TYPE,
        upstream.id,
        &OAuthRefreshJob {
            upstream_id: upstream.id,
            traceparent: job.traceparent.clone(),
        },
    )?;
    push_upstream_spec(
        specs,
        upstream_keys,
        OAUTH_USAGE_POLL_JOB_TYPE,
        upstream.id,
        &OAuthUsagePollJob {
            upstream_id: upstream.id,
            traceparent: job.traceparent.clone(),
        },
    )?;
    if upstream.warmup_enabled {
        push_upstream_spec(
            specs,
            upstream_keys,
            WARMUP_JOB_TYPE,
            upstream.id,
            &UpstreamWarmupJob::new(upstream.id, now_unix_secs),
        )?;
    }
    Ok(())
}

fn push_upstream_spec<Payload: Serialize>(
    specs: &mut Vec<ReconcileJobSpec>,
    upstream_keys: &mut HashSet<String>,
    job_type: &'static str,
    upstream_id: Uuid,
    payload: &Payload,
) -> Result<()> {
    let id = job_type
        .strip_prefix("entity:")
        .ok_or_else(|| SchedulerError::Job(format!("invalid entity job_type {job_type}")))?;
    let key = format!("entity:{id}:{upstream_id}");
    upstream_keys.insert(key.clone());
    specs.push(spec_with_key(job_type, key, payload)?);
    Ok(())
}

fn spec<Payload: Serialize>(
    job_type: &'static str,
    id: &str,
    payload: &Payload,
) -> Result<ReconcileJobSpec> {
    spec_with_key(job_type, format!("{job_type}:{id}"), payload)
}

fn spec_with_key<Payload: Serialize>(
    job_type: &'static str,
    idempotency_key: String,
    payload: &Payload,
) -> Result<ReconcileJobSpec> {
    Ok(ReconcileJobSpec {
        job_type,
        idempotency_key,
        payload: serde_json::to_vec(payload)
            .map_err(|error| SchedulerError::Job(error.to_string()))?,
    })
}

fn is_reconcilable_oauth(upstream: &UpstreamRecord) -> bool {
    upstream.kind == UpstreamKind::AnthropicOauth
        && upstream.enabled
        && upstream.deleted_at_unix_secs.is_none()
        && upstream.oauth_credentials.is_some()
}

fn storage_error(error: StorageError) -> SchedulerError {
    SchedulerError::Job(error.to_string())
}
