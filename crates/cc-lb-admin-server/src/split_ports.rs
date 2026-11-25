use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_admin::ports::{SplitManualWarmupError, SplitManualWarmupPort};
use cc_lb_aead::AeadService;
use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_control::{
    LimitResetUsageRefresh, LimitResetUsageRefreshPort, ManualWarmupJobResult,
    SPLIT_BRIDGE_DEFAULT_TIMEOUT, SPLIT_BRIDGE_POLL_INTERVAL, oauth_refresh_bridge_idempotency_key,
    take_manual_warmup_result, take_oauth_usage_refresh_result,
};
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsageRefreshJob;
use cc_lb_scheduler::jobs::warmup::ManualWarmupJob;
use cc_lb_scheduler::worker::{
    AdaptiveJob, SchedulerBackend, SchedulerPushTask, SchedulerTaskStatusRow, TaskStatus,
};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::{Storage, UpstreamRecord, UpstreamStore, WarmupLane};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn task_status_is_active(row: &SchedulerTaskStatusRow) -> bool {
    matches!(
        row.status,
        TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running
    ) || (row.status == TaskStatus::Failed && row.attempts < row.max_attempts)
}

fn task_status_is_terminal(row: &SchedulerTaskStatusRow) -> bool {
    !task_status_is_active(row)
}

#[derive(Clone)]
struct SplitSchedulerBridge {
    storage: Arc<dyn Storage>,
    backend: SchedulerBackend,
    clock: ClockHandle,
    timeout: Duration,
    poll_interval: Duration,
}

enum BridgeStepError {
    TimedOut,
    Failed(String),
}

impl BridgeStepError {
    fn into_reason(self, timeout_reason: &'static str) -> String {
        match self {
            Self::TimedOut => timeout_reason.to_owned(),
            Self::Failed(reason) => reason,
        }
    }
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

    fn deadline(&self) -> Instant {
        Instant::now() + self.timeout
    }

    async fn await_bounded<T, Fut>(
        deadline: Instant,
        cancel: Option<&CancellationToken>,
        fut: Fut,
    ) -> Result<T, BridgeStepError>
    where
        Fut: Future<Output = Result<T, String>>,
    {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(BridgeStepError::Failed("cancelled".to_owned()));
        }
        if Instant::now() >= deadline {
            return Err(BridgeStepError::TimedOut);
        }
        match cancel {
            Some(token) => tokio::select! {
                _ = token.cancelled() => Err(BridgeStepError::Failed("cancelled".to_owned())),
                res = tokio::time::timeout_at(deadline, fut) => match res {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(error)) => Err(BridgeStepError::Failed(error)),
                    Err(_) => Err(BridgeStepError::TimedOut),
                },
            },
            None => match tokio::time::timeout_at(deadline, fut).await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(error)) => Err(BridgeStepError::Failed(error)),
                Err(_) => Err(BridgeStepError::TimedOut),
            },
        }
    }

    async fn push_expedited(
        &self,
        args: AdaptiveJob,
        idempotency_key: String,
        max_attempts: i32,
        cancel: Option<&CancellationToken>,
        deadline: Instant,
    ) -> Result<(), BridgeStepError> {
        let now_unix_secs = unix_secs(self.clock.now());
        Self::await_bounded(deadline, cancel, async {
            self.backend
                .push_or_expedite_adaptive_task(SchedulerPushTask {
                    args,
                    idempotency_key: Some(idempotency_key),
                    run_at_unix_secs: Some(now_unix_secs),
                    max_attempts: Some(max_attempts),
                })
                .await
                .map_err(|error| error.to_string())
        })
        .await
    }

    async fn poll_until_settled<T, Check, Fut>(
        &self,
        idempotency_key: &str,
        cancel: Option<&CancellationToken>,
        deadline: Instant,
        mut check_ready: Check,
    ) -> Result<Option<T>, String>
    where
        Check: FnMut() -> Fut,
        Fut: Future<Output = Result<Option<T>, String>>,
    {
        loop {
            if cancel.is_some_and(CancellationToken::is_cancelled) {
                return Err("cancelled".to_owned());
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            match Self::await_bounded(deadline, cancel, check_ready()).await {
                Ok(Some(value)) => return Ok(Some(value)),
                Ok(None) => {}
                Err(BridgeStepError::TimedOut) => return Ok(None),
                Err(BridgeStepError::Failed(error)) => return Err(error),
            }
            let statuses = match Self::await_bounded(deadline, cancel, async {
                self.backend
                    .get_adaptive_task_statuses_by_key(idempotency_key)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            {
                Ok(statuses) => statuses,
                Err(BridgeStepError::TimedOut) => return Ok(None),
                Err(BridgeStepError::Failed(error)) => return Err(error),
            };
            if let Some(row) = statuses.first() {
                let terminal = match row.status {
                    TaskStatus::Done | TaskStatus::Killed => true,
                    TaskStatus::Failed => row.attempts >= row.max_attempts,
                    TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running | _ => false,
                };
                if terminal {
                    return match Self::await_bounded(deadline, cancel, check_ready()).await {
                        Ok(value) => Ok(value),
                        Err(BridgeStepError::TimedOut) => Ok(None),
                        Err(BridgeStepError::Failed(error)) => Err(error),
                    };
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
    aead: Option<Arc<AeadService>>,
}

impl SplitSchedulerLazyRefresher {
    pub fn new(
        storage: Arc<dyn Storage>,
        backend: SchedulerBackend,
        clock: ClockHandle,
        aead: Option<Arc<AeadService>>,
    ) -> Self {
        Self {
            bridge: SplitSchedulerBridge::new(storage, backend, clock),
            aead,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration, poll_interval: Duration) -> Self {
        self.bridge = self.bridge.with_timeout(timeout, poll_interval);
        self
    }

    fn plan_oauth_refresh(
        &self,
        upstream: &UpstreamRecord,
        gen_before: u64,
    ) -> (OAuthRefreshJob, Option<String>) {
        if let (Some(aead), Some(creds)) =
            (self.aead.as_deref(), upstream.oauth_credentials.as_ref())
            && let Ok(bundle) = creds.decrypt(aead, upstream.id.as_bytes())
        {
            let job = OAuthRefreshJob::for_credential(
                upstream.id,
                gen_before,
                bundle.expires_at_unix_secs,
            );
            let gen_key = job.generation_idempotency_key(gen_before, bundle.expires_at_unix_secs);
            return (job, Some(gen_key));
        }
        (
            OAuthRefreshJob::with_expected_generation(upstream.id, gen_before),
            None,
        )
    }

    async fn enqueue_or_join_oauth_refresh(
        &self,
        upstream_id: Uuid,
        deadline: Instant,
    ) -> Result<(u64, Option<String>), BridgeStepError> {
        let upstream = SplitSchedulerBridge::await_bounded(deadline, None, async {
            UpstreamStore::get_by_id(self.bridge.storage.as_ref(), upstream_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "upstream not found".to_owned())
        })
        .await?;
        let gen_before = upstream.oauth_token_generation;
        let (job, tagged_gen_key) = self.plan_oauth_refresh(&upstream, gen_before);
        if let Some(ref gen_key) = tagged_gen_key {
            let statuses = SplitSchedulerBridge::await_bounded(deadline, None, async {
                self.bridge
                    .backend
                    .get_adaptive_task_statuses_by_key_or_subkey(gen_key)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await?;
            if statuses.iter().any(task_status_is_active) {
                let exact_statuses = SplitSchedulerBridge::await_bounded(deadline, None, async {
                    self.bridge
                        .backend
                        .get_adaptive_task_statuses_by_key(gen_key)
                        .await
                        .map_err(|error| error.to_string())
                })
                .await?;
                if exact_statuses
                    .iter()
                    .any(|row| row.status == TaskStatus::Pending && row.attempts == 0)
                {
                    self.bridge
                        .push_expedited(
                            AdaptiveJob::OAuthRefresh(job),
                            gen_key.clone(),
                            2,
                            None,
                            deadline,
                        )
                        .await?;
                }
            } else {
                let push_key = if statuses.is_empty() {
                    gen_key.clone()
                } else {
                    format!("{gen_key}:bridge:{}", Uuid::new_v4())
                };
                self.bridge
                    .push_expedited(AdaptiveJob::OAuthRefresh(job), push_key, 2, None, deadline)
                    .await?;
            }
            return Ok((gen_before, tagged_gen_key));
        }

        let family_prefix = format!("adaptive:oauth_refresh:{upstream_id}:");
        let active_keys = SplitSchedulerBridge::await_bounded(deadline, None, async {
            self.bridge
                .backend
                .list_active_adaptive_idempotency_keys_by_prefix(&family_prefix)
                .await
                .map_err(|error| error.to_string())
        })
        .await?;
        if active_keys.is_empty() {
            let key = oauth_refresh_bridge_idempotency_key(upstream_id, Uuid::new_v4());
            self.bridge
                .push_expedited(AdaptiveJob::OAuthRefresh(job), key, 2, None, deadline)
                .await?;
        }
        Ok((gen_before, None))
    }
}

#[async_trait]
impl LazyRefreshHandle for SplitSchedulerLazyRefresher {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let deadline = self.bridge.deadline();
        let (gen_before, tagged_gen_key) = self
            .enqueue_or_join_oauth_refresh(upstream_id, deadline)
            .await
            .map_err(|err| LazyRefreshError::Failed {
                reason: err
                    .into_reason("oauth refresh was not completed by a proxy scheduler worker"),
            })?;

        let check_advanced = || async {
            let latest = UpstreamStore::get_by_id(self.bridge.storage.as_ref(), upstream_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "upstream not found".to_owned())?;
            Ok::<bool, String>(latest.oauth_token_generation > gen_before)
        };

        loop {
            match SplitSchedulerBridge::await_bounded(deadline, None, check_advanced()).await {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(BridgeStepError::TimedOut) => {
                    return Err(LazyRefreshError::Failed {
                        reason: "oauth refresh was not completed by a proxy scheduler worker"
                            .to_owned(),
                    });
                }
                Err(BridgeStepError::Failed(reason)) => {
                    return Err(LazyRefreshError::Failed { reason });
                }
            }
            if let Some(ref gen_key) = tagged_gen_key {
                let statuses = match SplitSchedulerBridge::await_bounded(deadline, None, async {
                    self.bridge
                        .backend
                        .get_adaptive_task_statuses_by_key_or_subkey(gen_key)
                        .await
                        .map_err(|error| error.to_string())
                })
                .await
                {
                    Ok(statuses) => statuses,
                    Err(BridgeStepError::TimedOut) => {
                        return Err(LazyRefreshError::Failed {
                            reason: "oauth refresh was not completed by a proxy scheduler worker"
                                .to_owned(),
                        });
                    }
                    Err(BridgeStepError::Failed(reason)) => {
                        return Err(LazyRefreshError::Failed { reason });
                    }
                };
                if !statuses.is_empty() && statuses.iter().all(task_status_is_terminal) {
                    return match SplitSchedulerBridge::await_bounded(
                        deadline,
                        None,
                        check_advanced(),
                    )
                    .await
                    {
                        Ok(true) => Ok(()),
                        Ok(false) | Err(BridgeStepError::TimedOut) => {
                            Err(LazyRefreshError::Failed {
                                reason:
                                    "oauth refresh was not completed by a proxy scheduler worker"
                                        .to_owned(),
                            })
                        }
                        Err(BridgeStepError::Failed(reason)) => {
                            Err(LazyRefreshError::Failed { reason })
                        }
                    };
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(LazyRefreshError::Failed {
                    reason: "oauth refresh was not completed by a proxy scheduler worker"
                        .to_owned(),
                });
            }
            tokio::time::sleep((deadline - now).min(self.bridge.poll_interval)).await;
        }
    }

    async fn enqueue_only(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let deadline = self.bridge.deadline();
        self.enqueue_or_join_oauth_refresh(upstream_id, deadline)
            .await
            .map(|_| ())
            .map_err(|err| LazyRefreshError::Failed {
                reason: err.into_reason("oauth refresh enqueue timed out"),
            })
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
        let deadline = self.bridge.deadline();
        let nonce = Uuid::new_v4();
        let job = OAuthUsageRefreshJob::new(upstream_id, nonce);
        let key = job.idempotency_key();
        if let Err(error) = self
            .bridge
            .push_expedited(
                AdaptiveJob::OAuthUsageRefresh(job),
                key.clone(),
                1,
                Some(cancel),
                deadline,
            )
            .await
        {
            let reason = error.into_reason("split usage refresh enqueue timed out");
            tracing::warn!(%upstream_id, error = %reason, "split usage refresh enqueue failed");
            return LimitResetUsageRefresh::Failed;
        }

        match self
            .bridge
            .poll_until_settled(&key, Some(cancel), deadline, || async {
                take_oauth_usage_refresh_result(self.bridge.storage.as_ref(), upstream_id, nonce)
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
        let deadline = self.bridge.deadline();
        let expires_at_unix_secs =
            unix_secs(self.bridge.clock.now()).saturating_add(self.bridge.timeout.as_secs().max(1));
        let nonce = Uuid::new_v4();
        let job = ManualWarmupJob::new(upstream_id, lane, holder, candidate_cycle_key, nonce)
            .with_expires_at(expires_at_unix_secs);
        let key = job.idempotency_key();
        self.bridge
            .push_expedited(
                AdaptiveJob::ManualWarmup(job),
                key.clone(),
                1,
                None,
                deadline,
            )
            .await
            .map_err(|err| {
                SplitManualWarmupError::Unavailable(
                    err.into_reason("manual warmup was not completed by a proxy scheduler worker"),
                )
            })?;

        let settled = self
            .bridge
            .poll_until_settled(&key, None, deadline, || async {
                take_manual_warmup_result(self.bridge.storage.as_ref(), upstream_id, lane, nonce)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(SplitManualWarmupError::OutcomeUnknown)?;

        if let Some(result) = settled {
            return Ok(result);
        }
        if let Ok(Some(result)) =
            take_manual_warmup_result(self.bridge.storage.as_ref(), upstream_id, lane, nonce).await
        {
            return Ok(result);
        }
        let now_secs = unix_secs(self.bridge.clock.now());
        if now_secs >= expires_at_unix_secs
            && let Ok(statuses) = self
                .bridge
                .backend
                .get_adaptive_task_statuses_by_key(&key)
                .await
            && let Some(row) = statuses.first()
            && matches!(row.status, TaskStatus::Pending | TaskStatus::Queued)
            && row.attempts == 0
        {
            return Err(SplitManualWarmupError::ExpiredBeforeStart(
                "manual warmup expired in scheduler queue before a worker started it".to_owned(),
            ));
        }
        Err(SplitManualWarmupError::OutcomeUnknown(
            "manual warmup did not report a result before deadline and may have started on a worker"
                .to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_config::{SchedulerConfig, StorageConfig};
    use cc_lb_storage_api::MetaStore;

    #[tokio::test]
    async fn split_bridge_bounds_stalled_reads_and_respects_cancellation() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let sqlite_path = dir.path().join("split-bridge.sqlite");
        let database_url = format!("sqlite://{}", sqlite_path.display());
        let clock: ClockHandle = Arc::new(cc_lb_clock::SystemClock);
        let storage = Arc::new(
            cc_lb_storage_sqlite::open_sqlite(&database_url, clock.clone())
                .await
                .expect("open sqlite"),
        );
        storage.initialize().await.expect("init sqlite");

        let scheduler = cc_lb_scheduler::factory::open_scheduler_storage_with_role(
            &StorageConfig::Sqlite { path: sqlite_path },
            &SchedulerConfig::default(),
            cc_lb_scheduler::factory::SchedulerPoolRole::EnqueueOnly,
            clock.clone(),
        )
        .await
        .expect("open scheduler");

        let bridge = SplitSchedulerBridge::new(storage, scheduler.lazy_handle(), clock)
            .with_timeout(Duration::from_millis(60), Duration::from_millis(10));

        // 1. Stalled enqueue/upstream lookup under await_bounded expires cleanly at deadline.
        let started = std::time::Instant::now();
        let deadline = bridge.deadline();
        let err = SplitSchedulerBridge::await_bounded::<(), _>(
            deadline,
            None,
            std::future::pending::<Result<(), String>>(),
        )
        .await
        .expect_err("stalled future must time out at deadline");
        assert!(matches!(err, BridgeStepError::TimedOut));
        assert!(started.elapsed() < Duration::from_secs(2));

        // 2. Stalled check_ready inside poll_until_settled returns Ok(None) within the single deadline.
        let started = std::time::Instant::now();
        let settled = bridge
            .poll_until_settled::<(), _, _>("stalled-key", None, bridge.deadline(), || async {
                std::future::pending::<Result<Option<()>, String>>().await
            })
            .await
            .expect("deadline expiry maps to Ok(None)");
        assert!(settled.is_none());
        assert!(started.elapsed() < Duration::from_secs(2));

        // 3. Cancellation aborts stalled reads immediately.
        let token = CancellationToken::new();
        let cancel_trigger = token.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel_trigger.cancel();
        });
        let long_bridge = bridge.with_timeout(Duration::from_secs(30), Duration::from_millis(10));
        let err = long_bridge
            .poll_until_settled::<(), _, _>(
                "cancelled-key",
                Some(&token),
                long_bridge.deadline(),
                || async { std::future::pending::<Result<Option<()>, String>>().await },
            )
            .await
            .expect_err("cancellation must abort stalled poll");
        assert_eq!(err, "cancelled");
    }
}
