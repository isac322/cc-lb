use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_engine::clock::{ClockHandle, unix_secs};
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{
    AdaptiveJob, Filter, SchedulerBackend, SchedulerPushTask, TaskStatus,
};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::{StorageError, StorageResult, UpstreamRecord};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;

const LAZY_REFRESH_CONTENTION_WAIT_SECS: u64 = 60;
const LAZY_REFRESH_POLL_INTERVAL_SECS: u64 = 1;
const LAZY_REFRESH_TASK_PAGE_SIZE: u32 = 500;

#[derive(Clone)]
pub struct LazyRefresher {
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    cancel: CancellationToken,
    claim_guard: Arc<dyn LazyRefreshClaimGuard>,
    contention: LazyRefreshContentionConfig,
    apalis_handle: SchedulerBackend,
    clock: ClockHandle,
}

#[derive(Clone, Copy)]
struct LazyRefreshContentionConfig {
    wait_timeout: Duration,
    poll_interval: Duration,
}

impl LazyRefreshContentionConfig {
    const fn production() -> Self {
        Self {
            wait_timeout: Duration::from_secs(LAZY_REFRESH_CONTENTION_WAIT_SECS),
            poll_interval: Duration::from_secs(LAZY_REFRESH_POLL_INTERVAL_SECS),
        }
    }
}

#[async_trait]
pub trait LazyRefreshClaimGuard: Send + Sync {
    async fn begin_refresh(
        &self,
        upstream_id: Uuid,
        expected_generation: u64,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<String>;

    async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        Ok(LazyRefreshTaskState::Active)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyRefreshTaskState {
    Active,
    Done,
    TerminalFailure { reason: String },
}

#[derive(Clone, Debug)]
pub struct ApalisLazyRefreshClaimGuard {
    scheduler_backend: SchedulerBackend,
}

impl ApalisLazyRefreshClaimGuard {
    pub fn new(scheduler_backend: SchedulerBackend) -> Self {
        Self { scheduler_backend }
    }
}

#[async_trait]
impl LazyRefreshClaimGuard for ApalisLazyRefreshClaimGuard {
    async fn begin_refresh(
        &self,
        upstream_id: Uuid,
        expected_generation: u64,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<String> {
        enqueue_lazy_refresh_task(
            &self.scheduler_backend,
            upstream_id,
            expected_generation,
            expires_at_unix_secs,
            now_unix_secs,
        )
        .await
    }

    async fn task_state(&self, idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        apalis_lazy_refresh_task_state(&self.scheduler_backend, idempotency_key).await
    }
}

async fn enqueue_lazy_refresh_task(
    scheduler_backend: &SchedulerBackend,
    upstream_id: Uuid,
    expected_generation: u64,
    expires_at_unix_secs: u64,
    now_unix_secs: u64,
) -> StorageResult<String> {
    let job = OAuthRefreshJob::for_generation(upstream_id, expected_generation);
    let idempotency_key = job.idempotency_key(expires_at_unix_secs);
    let task = SchedulerPushTask {
        args: AdaptiveJob::OAuthRefresh(job),
        idempotency_key: Some(idempotency_key.clone()),
        run_at_unix_secs: Some(now_unix_secs),
        max_attempts: None,
    };
    match scheduler_backend.push_adaptive_task(task).await {
        Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => Ok(idempotency_key),
        Err(error) => Err(scheduler_claim_error(error)),
    }
}

async fn apalis_lazy_refresh_task_state(
    scheduler_backend: &SchedulerBackend,
    idempotency_key: &str,
) -> StorageResult<LazyRefreshTaskState> {
    let mut terminal_state = None;
    for status in [
        TaskStatus::Pending,
        TaskStatus::Queued,
        TaskStatus::Running,
        TaskStatus::Failed,
        TaskStatus::Killed,
        TaskStatus::Done,
    ] {
        let mut page = 1;
        loop {
            let filter = Filter {
                status: Some(status.clone()),
                page,
                page_size: Some(LAZY_REFRESH_TASK_PAGE_SIZE),
            };
            let tasks = scheduler_backend
                .list_adaptive_tasks(&filter)
                .await
                .map_err(scheduler_claim_error)?;
            for task in &tasks {
                if task.idempotency_key.as_deref() != Some(idempotency_key) {
                    continue;
                }
                match lazy_refresh_task_state_from_row(task) {
                    LazyRefreshTaskState::Active => return Ok(LazyRefreshTaskState::Active),
                    state @ LazyRefreshTaskState::TerminalFailure { .. } => {
                        terminal_state = Some(state);
                    }
                    LazyRefreshTaskState::Done => {
                        terminal_state.get_or_insert(LazyRefreshTaskState::Done);
                    }
                }
            }
            if tasks.len() < usize::try_from(LAZY_REFRESH_TASK_PAGE_SIZE).unwrap_or(usize::MAX) {
                break;
            }
            page += 1;
        }
    }
    Ok(terminal_state.unwrap_or(LazyRefreshTaskState::Active))
}

fn lazy_refresh_task_state_from_row(
    task: &cc_lb_scheduler::worker::SchedulerTaskRow<AdaptiveJob>,
) -> LazyRefreshTaskState {
    match &task.status {
        TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running => {
            LazyRefreshTaskState::Active
        }
        TaskStatus::Failed if task.attempts < task.max_attempts => LazyRefreshTaskState::Active,
        TaskStatus::Failed => LazyRefreshTaskState::TerminalFailure {
            reason: "oauth refresh job failed permanently".to_owned(),
        },
        TaskStatus::Killed => LazyRefreshTaskState::TerminalFailure {
            reason: "oauth refresh job was killed after exhausting retries".to_owned(),
        },
        TaskStatus::Done => LazyRefreshTaskState::Done,
        _ => {
            tracing::warn!(status = ?task.status, "unknown oauth refresh task status");
            LazyRefreshTaskState::Active
        }
    }
}

impl LazyRefresher {
    pub fn new(params: LazyRefresherParams) -> Self {
        let claim_guard = lazy_refresh_claim_guard_from_scheduler(&params.apalis_handle);
        Self::new_with_claim_guard(
            params.deps,
            params.cancel,
            claim_guard,
            params.apalis_handle,
        )
    }

    pub fn new_with_claim_guard(
        deps: LazyRefresherDeps,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self::new_with_claim_guard_and_config(
            deps,
            cancel,
            claim_guard,
            LazyRefreshContentionConfig::production(),
            apalis_handle,
        )
    }

    fn new_with_claim_guard_and_config(
        deps: LazyRefresherDeps,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        contention: LazyRefreshContentionConfig,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self {
            stores: deps.stores,
            aead: deps.aead,
            cancel,
            claim_guard,
            contention,
            apalis_handle,
            clock: deps.clock,
        }
    }

    async fn wait_for_token_generation(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
        idempotency_key: &str,
    ) -> Result<(), LazyRefreshError> {
        let deadline = tokio::time::Instant::now() + self.contention.wait_timeout;
        loop {
            let Some(current_generation) = self
                .stores
                .upstreams
                .read_oauth_token_generation(upstream_id)
                .await
                .map_err(lazy_error)?
            else {
                return Err(LazyRefreshError::Failed {
                    reason: "oauth upstream not found".to_owned(),
                });
            };
            if current_generation > starting_generation {
                return Ok(());
            }

            let task_state = self
                .claim_guard
                .task_state(idempotency_key)
                .await
                .map_err(lazy_error)?;
            if !matches!(task_state, LazyRefreshTaskState::Active) {
                // The worker may have committed complete_refresh AND marked the task
                // terminal between our generation read above and this task_state read.
                // Re-read the generation so we observe its post-commit state before
                // concluding that the job completed without advancing the generation.
                let Some(post_terminal_generation) = self
                    .stores
                    .upstreams
                    .read_oauth_token_generation(upstream_id)
                    .await
                    .map_err(lazy_error)?
                else {
                    return Err(LazyRefreshError::Failed {
                        reason: "oauth upstream not found".to_owned(),
                    });
                };
                if post_terminal_generation > starting_generation {
                    return Ok(());
                }
                return match task_state {
                    LazyRefreshTaskState::Done => Err(LazyRefreshError::Failed {
                        reason: "oauth refresh job completed without advancing generation"
                            .to_owned(),
                    }),
                    LazyRefreshTaskState::TerminalFailure { reason } => {
                        Err(LazyRefreshError::Failed { reason })
                    }
                    LazyRefreshTaskState::Active => unreachable!(),
                };
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                ::metrics::counter!(cc_lb_scheduler::scheduler_metrics::LAZY_REFRESH_TIMEOUT_TOTAL)
                    .increment(1);
                return Err(LazyRefreshError::Failed {
                    reason: "oauth refresh timed out waiting for another holder".to_owned(),
                });
            }
            let sleep_for = self.contention.poll_interval.min(deadline - now);
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    return Err(LazyRefreshError::Failed {
                        reason: "oauth refresh cancelled".to_owned(),
                    });
                }
                _ = tokio::time::sleep(sleep_for) => {}
            }
        }
    }
}

pub(crate) fn lazy_refresh_claim_guard_from_scheduler(
    scheduler_backend: &SchedulerBackend,
) -> Arc<dyn LazyRefreshClaimGuard> {
    Arc::new(ApalisLazyRefreshClaimGuard::new(scheduler_backend.clone()))
}

#[derive(Clone)]
pub struct LazyRefresherDeps {
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub clock: ClockHandle,
}

pub struct LazyRefresherParams {
    pub deps: LazyRefresherDeps,
    pub cancel: CancellationToken,
    pub apalis_handle: SchedulerBackend,
}

#[async_trait]
impl LazyRefreshHandle for LazyRefresher {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let initial_upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        if let Some(reason) = terminal_refresh_failure_reason(
            &self.stores,
            initial_upstream.id,
            initial_upstream.oauth_token_generation,
        )
        .await
        .map_err(lazy_error)?
        {
            return Err(LazyRefreshError::Failed {
                reason: format!("oauth refresh requires reauthorization: {reason}"),
            });
        }
        let starting_generation = initial_upstream.oauth_token_generation;
        let expires_at_unix_secs = oauth_expires_at(&initial_upstream, &self.aead)?;
        let idempotency_key = self
            .claim_guard
            .begin_refresh(
                upstream_id,
                starting_generation,
                expires_at_unix_secs,
                unix_secs(self.clock.now()),
            )
            .await
            .map_err(lazy_error)?;
        self.wait_for_token_generation(upstream_id, starting_generation, &idempotency_key)
            .await
    }

    async fn enqueue_only(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        if let Some(reason) = terminal_refresh_failure_reason(
            &self.stores,
            upstream.id,
            upstream.oauth_token_generation,
        )
        .await
        .map_err(lazy_error)?
        {
            return Err(LazyRefreshError::Failed {
                reason: format!("oauth refresh requires reauthorization: {reason}"),
            });
        }
        let expires_at_unix_secs = oauth_expires_at(&upstream, &self.aead)?;
        enqueue_lazy_refresh_task(
            &self.apalis_handle,
            upstream_id,
            upstream.oauth_token_generation,
            expires_at_unix_secs,
            unix_secs(self.clock.now()),
        )
        .await
        .map_err(lazy_error)?;
        Ok(())
    }
}

async fn terminal_refresh_failure_reason(
    stores: &Stores,
    upstream_id: Uuid,
    generation: u64,
) -> StorageResult<Option<String>> {
    Ok(stores
        .upstreams
        .read_oauth_refresh_terminal_failure(upstream_id)
        .await?
        .filter(|failure| failure.expected_generation == generation)
        .map(|failure| failure.code))
}

fn lazy_error(error: StorageError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn oauth_expires_at(
    upstream: &UpstreamRecord,
    aead: &AeadService,
) -> Result<u64, LazyRefreshError> {
    let credentials =
        upstream
            .oauth_credentials
            .as_ref()
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "missing oauth credentials".to_owned(),
            })?;
    let bundle = credentials
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| LazyRefreshError::Failed {
            reason: "oauth decrypt failed".to_owned(),
        })?;
    Ok(bundle.expires_at_unix_secs)
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn scheduler_claim_error(error: cc_lb_scheduler::error::SchedulerError) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}
