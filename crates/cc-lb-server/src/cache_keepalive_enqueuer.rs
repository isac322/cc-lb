use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveCancelRequest, CacheKeepaliveEnqueueError, CacheKeepaliveEnqueueRequest,
    CacheKeepaliveEnqueuer, CancelReason,
};
use cc_lb_engine::{ClockHandle, clock::unix_secs};
use cc_lb_scheduler::error::Result as SchedulerResult;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_storage_api::{
    CacheKeepaliveReplaceRequest, CacheKeepaliveSessionStore, CacheKeepaliveTerminalReason, Storage,
};

use crate::scheduler_dispatch::cache_keepalive_payload_aad;

#[async_trait]
pub(crate) trait CacheKeepaliveTaskPusher: Send + Sync {
    async fn push_cache_keepalive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()>;
}

#[async_trait]
impl CacheKeepaliveTaskPusher for crate::scheduler_factory::SchedulerBackend {
    async fn push_cache_keepalive_task(
        &self,
        task: SchedulerPushTask<AdaptiveJob>,
    ) -> SchedulerResult<()> {
        self.push_adaptive_task(task).await
    }
}

pub(crate) struct ServerCacheKeepaliveEnqueuerDeps {
    pub storage: Arc<dyn Storage>,
    pub pusher: Arc<dyn CacheKeepaliveTaskPusher>,
    pub aead: Arc<AeadService>,
    pub clock: ClockHandle,
}

pub(crate) struct ServerCacheKeepaliveEnqueuer {
    storage: Arc<dyn Storage>,
    pusher: Arc<dyn CacheKeepaliveTaskPusher>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
}

impl ServerCacheKeepaliveEnqueuer {
    pub(crate) fn new(deps: ServerCacheKeepaliveEnqueuerDeps) -> Self {
        Self {
            storage: deps.storage,
            pusher: deps.pusher,
            aead: deps.aead,
            clock: deps.clock,
        }
    }
}

#[async_trait]
impl CacheKeepaliveEnqueuer for ServerCacheKeepaliveEnqueuer {
    async fn enqueue_cache_keepalive(
        &self,
        request: CacheKeepaliveEnqueueRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        let now_unix_secs = unix_secs(self.clock.now());
        let cache_anchor_at_unix_secs =
            now_unix_secs.saturating_sub(request.cache_anchor_age.as_secs());
        let run_at_unix_secs = now_unix_secs.saturating_add(request.params.delay.as_secs());
        let expires_at_unix_secs =
            cache_anchor_at_unix_secs.saturating_add(request.snapshot.ttl.as_secs());
        let record = CacheKeepaliveSessionStore::replace_from_real_request(
            self.storage.as_ref(),
            &CacheKeepaliveReplaceRequest {
                session_key_hash: request.session_key_hash.clone(),
                principal_id: request.principal_id.clone(),
                upstream_id: request.snapshot.upstream_id,
                cache_anchor_at_unix_secs,
                ttl: request.snapshot.ttl,
                run_at_unix_secs,
                expires_at_unix_secs,
                encrypted_payload: Vec::new(),
                now_unix_secs,
            },
        )
        .await
        .map_err(cache_keepalive_enqueue_error)?;
        let persisted = request.snapshot.to_persisted();
        let plaintext = serde_json::to_vec(&persisted).map_err(cache_keepalive_enqueue_error)?;
        let encrypted_payload = self
            .aead
            .encrypt(
                &plaintext,
                &cache_keepalive_payload_aad(
                    &record.principal_id,
                    &record.session_key_hash,
                    record.upstream_id,
                    record.generation,
                ),
            )
            .map_err(cache_keepalive_enqueue_error)?;
        if !CacheKeepaliveSessionStore::update_cache_keepalive_payload(
            self.storage.as_ref(),
            &record.session_key_hash,
            record.generation,
            &encrypted_payload,
            now_unix_secs,
        )
        .await
        .map_err(cache_keepalive_enqueue_error)?
        {
            return Err(cache_keepalive_enqueue_error(
                "cache keepalive generation changed before payload update",
            ));
        }
        let job = cc_lb_scheduler::jobs::cache_keepalive::CacheKeepaliveJob {
            session_key_hash: record.session_key_hash.clone(),
            generation: record.generation,
            principal_id: record.principal_id.clone(),
            upstream_id: record.upstream_id,
            ttl: record.ttl,
            cache_anchor_at_unix_secs: record.cache_anchor_at_unix_secs,
            expires_at_unix_secs: record.expires_at_unix_secs,
            refresh_delay_secs: request.params.delay.as_secs(),
            max_refreshes: request.params.max_refreshes,
            max_total_duration_secs: request.params.max_total_duration_secs,
            traceparent: None,
        };
        let idempotency_key = job.idempotency_key();
        let task = SchedulerPushTask {
            args: AdaptiveJob::CacheKeepalive(job),
            idempotency_key: Some(idempotency_key),
            run_at_unix_secs: Some(record.run_at_unix_secs),
            max_attempts: Some(1),
        };
        match self.pusher.push_cache_keepalive_task(task).await {
            Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => {
                CacheKeepaliveSessionStore::mark_cache_keepalive_enqueued(
                    self.storage.as_ref(),
                    &record.session_key_hash,
                    record.generation,
                    now_unix_secs,
                )
                .await
                .map_err(cache_keepalive_enqueue_error)?;
                Ok(())
            }
            Err(error) => {
                CacheKeepaliveSessionStore::mark_cache_keepalive_terminal(
                    self.storage.as_ref(),
                    &record.session_key_hash,
                    record.generation,
                    CacheKeepaliveTerminalReason::DispatchError,
                    now_unix_secs,
                )
                .await
                .map_err(cache_keepalive_enqueue_error)?;
                Err(cache_keepalive_enqueue_error(error))
            }
        }
    }

    async fn cancel_cache_keepalive(
        &self,
        request: CacheKeepaliveCancelRequest,
    ) -> Result<(), CacheKeepaliveEnqueueError> {
        CacheKeepaliveSessionStore::mark_latest_cache_keepalive_terminal(
            self.storage.as_ref(),
            &request.session_key_hash,
            terminal_reason_from_cancel(request.reason),
            unix_secs(self.clock.now()),
        )
        .await
        .map_err(cache_keepalive_enqueue_error)?;
        Ok(())
    }
}

fn terminal_reason_from_cancel(reason: CancelReason) -> CacheKeepaliveTerminalReason {
    match reason {
        CancelReason::MaxRefreshes => CacheKeepaliveTerminalReason::MaxRefreshes,
        CancelReason::MaxDuration => CacheKeepaliveTerminalReason::MaxDuration,
        CancelReason::UpstreamGone => CacheKeepaliveTerminalReason::Stale,
        CancelReason::NewRequest
        | CancelReason::NoCacheControl
        | CancelReason::UserTurnDetected
        | CancelReason::SnapshotTooLarge
        | CancelReason::Shutdown => CacheKeepaliveTerminalReason::Cancelled,
    }
}

pub(crate) fn cache_keepalive_enqueue_error(
    error: impl std::fmt::Display,
) -> CacheKeepaliveEnqueueError {
    CacheKeepaliveEnqueueError(error.to_string())
}

#[cfg(test)]
#[path = "cache_keepalive_enqueuer_tests.rs"]
mod tests;
