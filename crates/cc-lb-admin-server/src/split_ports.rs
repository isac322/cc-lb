use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_admin::ports::{SplitManualWarmupError, SplitManualWarmupPort};
use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_control::{
    LimitResetUsageRefresh, LimitResetUsageRefreshPort, ManualWarmupJobResult,
    SPLIT_BRIDGE_DEFAULT_TIMEOUT, SPLIT_BRIDGE_POLL_INTERVAL, load_manual_warmup_result,
    load_oauth_usage_refresh_result, oauth_refresh_bridge_idempotency_key,
};
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsageRefreshJob;
use cc_lb_scheduler::jobs::warmup::ManualWarmupJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerBackend, SchedulerPushTask, TaskStatus};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::{Storage, UpstreamStore, WarmupLane};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
struct SplitSchedulerBridge {
    storage: Arc<dyn Storage>,
    backend: SchedulerBackend,
    clock: ClockHandle,
    timeout: Duration,
    poll_interval: Duration,
}

impl SplitSchedulerBridge {
    fn new(storage: Arc<dyn Storage>, backend: SchedulerBackend, clock: ClockHandle) -> Self {
        Self {
            storage,
            backend,
            clock,
            timeout: SPLIT_BRIDGE_DEFAULT_TIMEOUT,
            poll_interval: SPLIT_BRIDGE_POLL_INTERVAL,
        }
    }

    fn with_timeout(mut self, timeout: Duration, poll_interval: Duration) -> Self {
        self.timeout = timeout;
        self.poll_interval = poll_interval;
        self
    }

    async fn push_expedited(
        &self,
        args: AdaptiveJob,
        idempotency_key: String,
        max_attempts: i32,
    ) -> Result<(), String> {
        let now_unix_secs = unix_secs(self.clock.now());
        self.backend
            .push_or_expedite_adaptive_task(SchedulerPushTask {
                args,
                idempotency_key: Some(idempotency_key),
                run_at_unix_secs: Some(now_unix_secs),
                max_attempts: Some(max_attempts),
            })
            .await
            .map_err(|error| error.to_string())
    }

    async fn poll_until_settled<T, Check, Fut>(
        &self,
        idempotency_key: &str,
        cancel: Option<&CancellationToken>,
        mut check_ready: Check,
    ) -> Result<Option<T>, String>
    where
        Check: FnMut() -> Fut,
        Fut: Future<Output = Result<Option<T>, String>>,
    {
        let deadline = Instant::now() + self.timeout;
        loop {
            if cancel.is_some_and(CancellationToken::is_cancelled) {
                return Err("cancelled".to_owned());
            }
            if let Some(value) = check_ready().await? {
                return Ok(Some(value));
            }
            let statuses = self
                .backend
                .get_adaptive_task_statuses_by_key(idempotency_key)
                .await
                .map_err(|error| error.to_string())?;
            if let Some(row) = statuses.first() {
                let terminal = match row.status {
                    TaskStatus::Done | TaskStatus::Killed => true,
                    TaskStatus::Failed => row.attempts >= row.max_attempts,
                    TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running | _ => false,
                };
                if terminal {
                    return check_ready().await;
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let wait = (deadline - now).min(self.poll_interval);
            match cancel {
                Some(token) => {
                    tokio::select! {
                        _ = token.cancelled() => return Err("cancelled".to_owned()),
                        _ = tokio::time::sleep(wait) => {}
                    }
                }
                None => tokio::time::sleep(wait).await,
            }
        }
    }
}

/// Bridges on-demand metadata-refresh OAuth token refreshes in DB-only split
/// `cc-lb-admin` via durable `AdaptiveJob::OAuthRefresh` tasks claimed by
/// proxy scheduler workers.
#[derive(Clone)]
pub struct SplitSchedulerLazyRefresher {
    bridge: SplitSchedulerBridge,
}

impl SplitSchedulerLazyRefresher {
    pub fn new(storage: Arc<dyn Storage>, backend: SchedulerBackend, clock: ClockHandle) -> Self {
        Self {
            bridge: SplitSchedulerBridge::new(storage, backend, clock),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration, poll_interval: Duration) -> Self {
        self.bridge = self.bridge.with_timeout(timeout, poll_interval);
        self
    }
}

#[async_trait]
impl LazyRefreshHandle for SplitSchedulerLazyRefresher {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let upstream = UpstreamStore::get_by_id(self.bridge.storage.as_ref(), upstream_id)
            .await
            .map_err(|error| LazyRefreshError::Failed {
                reason: error.to_string(),
            })?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "upstream not found".to_owned(),
            })?;
        let gen_before = upstream.oauth_token_generation;
        let key = oauth_refresh_bridge_idempotency_key(upstream_id, Uuid::new_v4());
        self.bridge
            .push_expedited(
                AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
                key.clone(),
                2,
            )
            .await
            .map_err(|reason| LazyRefreshError::Failed { reason })?;

        let settled = self
            .bridge
            .poll_until_settled(&key, None, || async {
                let latest = UpstreamStore::get_by_id(self.bridge.storage.as_ref(), upstream_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "upstream not found".to_owned())?;
                if latest.oauth_token_generation > gen_before {
                    Ok(Some(()))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(|reason| LazyRefreshError::Failed { reason })?;

        match settled {
            Some(()) => Ok(()),
            None => Err(LazyRefreshError::Failed {
                reason: "oauth refresh was not completed by a proxy scheduler worker".to_owned(),
            }),
        }
    }

    async fn enqueue_only(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let key = oauth_refresh_bridge_idempotency_key(upstream_id, Uuid::new_v4());
        self.bridge
            .push_expedited(
                AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
                key,
                2,
            )
            .await
            .map_err(|reason| LazyRefreshError::Failed { reason })
    }
}

/// Bridges post-limit-reset usage refresh in DB-only split `cc-lb-admin` via
/// durable `AdaptiveJob::OAuthUsageRefresh` tasks executed by proxy workers.
#[derive(Clone)]
pub struct SplitSchedulerLimitResetUsageRefresh {
    bridge: SplitSchedulerBridge,
}

impl SplitSchedulerLimitResetUsageRefresh {
    pub fn new(storage: Arc<dyn Storage>, backend: SchedulerBackend, clock: ClockHandle) -> Self {
        Self {
            bridge: SplitSchedulerBridge::new(storage, backend, clock),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration, poll_interval: Duration) -> Self {
        self.bridge = self.bridge.with_timeout(timeout, poll_interval);
        self
    }
}

#[async_trait]
impl LimitResetUsageRefreshPort for SplitSchedulerLimitResetUsageRefresh {
    async fn refresh_upstream_usage(
        &self,
        upstream_id: Uuid,
        cancel: &CancellationToken,
    ) -> LimitResetUsageRefresh {
        let nonce = Uuid::new_v4();
        let job = OAuthUsageRefreshJob::new(upstream_id, nonce);
        let key = job.idempotency_key();
        if let Err(error) = self
            .bridge
            .push_expedited(AdaptiveJob::OAuthUsageRefresh(job), key.clone(), 1)
            .await
        {
            tracing::warn!(%upstream_id, %error, "split usage refresh enqueue failed");
            return LimitResetUsageRefresh::Failed;
        }

        match self
            .bridge
            .poll_until_settled(&key, Some(cancel), || async {
                load_oauth_usage_refresh_result(self.bridge.storage.as_ref(), upstream_id, nonce)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
        {
            Ok(Some(result)) => result.outcome,
            Ok(None) => {
                tracing::warn!(
                    %upstream_id,
                    timeout_secs = self.bridge.timeout.as_secs(),
                    "split usage refresh was not completed by a proxy scheduler worker"
                );
                LimitResetUsageRefresh::Failed
            }
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "split usage refresh bridge failed");
                LimitResetUsageRefresh::Failed
            }
        }
    }
}

/// Bridges manual warmup fire-now in DB-only split `cc-lb-admin` via durable
/// `AdaptiveJob::ManualWarmup` tasks executed by proxy scheduler workers.
#[derive(Clone)]
pub struct SplitSchedulerManualWarmup {
    bridge: SplitSchedulerBridge,
}

impl SplitSchedulerManualWarmup {
    pub fn new(storage: Arc<dyn Storage>, backend: SchedulerBackend, clock: ClockHandle) -> Self {
        Self {
            bridge: SplitSchedulerBridge::new(storage, backend, clock),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration, poll_interval: Duration) -> Self {
        self.bridge = self.bridge.with_timeout(timeout, poll_interval);
        self
    }
}

#[async_trait]
impl SplitManualWarmupPort for SplitSchedulerManualWarmup {
    async fn fire_manual_warmup(
        &self,
        upstream_id: Uuid,
        lane: WarmupLane,
        holder: String,
        candidate_cycle_key: i64,
    ) -> Result<ManualWarmupJobResult, SplitManualWarmupError> {
        let nonce = Uuid::new_v4();
        let job = ManualWarmupJob::new(upstream_id, lane, holder, candidate_cycle_key, nonce);
        let key = job.idempotency_key();
        self.bridge
            .push_expedited(AdaptiveJob::ManualWarmup(job), key.clone(), 1)
            .await
            .map_err(SplitManualWarmupError)?;

        let settled = self
            .bridge
            .poll_until_settled(&key, None, || async {
                load_manual_warmup_result(self.bridge.storage.as_ref(), upstream_id, lane, nonce)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(SplitManualWarmupError)?;

        settled.ok_or_else(|| {
            SplitManualWarmupError(
                "manual warmup was not completed by a proxy scheduler worker".to_owned(),
            )
        })
    }
}
