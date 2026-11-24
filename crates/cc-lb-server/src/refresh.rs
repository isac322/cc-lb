use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_clock::{Clock, ClockHandle, unix_secs};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_control::{AuditPayload, MetadataHookHandle, MetadataHookRequest};
use cc_lb_oauth_protocol::{
    ExistingTokenParts, TokenEndpointResponse, is_terminal_refresh_failure,
    parse_token_endpoint_response, refresh_requires_reconnect, refresh_token_form_body,
    refresh_token_json_body, refreshed_token_parts,
};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{
    AdaptiveJob, Filter, SchedulerBackend, SchedulerPushTask, TaskStatus,
};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AuditActorFields, AuditEntry, AuditStore, StorageError, StorageResult, UpstreamRecord,
    UpstreamStatusUpdate,
};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;

const LAZY_REFRESH_CONTENTION_WAIT_SECS: u64 = 60;
const LAZY_REFRESH_POLL_INTERVAL_SECS: u64 = 1;
const LAZY_REFRESH_TASK_PAGE_SIZE: u32 = 500;

const OPENAI_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const OPENAI_CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
use cc_lb_oauth_protocol::GEMINI_OAUTH_TOKEN_URL;
const GEMINI_OAUTH_CLIENT_ID: &str =
    "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com";
const GEMINI_OAUTH_CLIENT_SECRET: &str = "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";

type HyperTokenClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Clone)]
struct TokenHttpClient {
    client: HyperTokenClient,
    timeout: Duration,
}

impl TokenHttpClient {
    fn new(timeout: Duration) -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client, timeout }
    }
}

type InFlightRefreshOutcome = Result<(), String>;
type InFlightRefreshMap = Arc<
    std::sync::Mutex<
        std::collections::HashMap<
            Uuid,
            tokio::sync::watch::Receiver<Option<InFlightRefreshOutcome>>,
        >,
    >,
>;

struct InFlightRefreshGuard {
    in_flight: InFlightRefreshMap,
    upstream_id: Uuid,
}

impl Drop for InFlightRefreshGuard {
    fn drop(&mut self) {
        self.in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.upstream_id);
    }
}

#[derive(Clone)]
pub struct LazyRefresher {
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    google_config: Arc<cc_lb_config::Config>,
    replica_id: Uuid,
    http: TokenHttpClient,
    metadata_hook: Option<MetadataHookHandle>,
    cancel: CancellationToken,
    claim_guard: Arc<dyn LazyRefreshClaimGuard>,
    contention: LazyRefreshContentionConfig,
    apalis_handle: SchedulerBackend,
    clock: ClockHandle,
    in_flight: InFlightRefreshMap,
}

#[derive(Clone, Copy)]
pub struct LazyRefreshContentionConfig {
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

    #[cfg(test)]
    const fn for_tests(wait_timeout: Duration, poll_interval: Duration) -> Self {
        Self {
            wait_timeout,
            poll_interval,
        }
    }
}

#[async_trait]
pub trait LazyRefreshClaimGuard: Send + Sync {
    async fn begin_refresh(
        &self,
        upstream_id: Uuid,
        holder: &str,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim>;

    async fn complete_and_bump_generation(
        &self,
        upstream_id: Uuid,
        holder: &str,
        generation: u64,
    ) -> StorageResult<bool>;

    async fn release_if_holder(&self, upstream_id: Uuid, holder: &str) -> StorageResult<bool>;

    async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        Ok(LazyRefreshTaskState::Active)
    }

    async fn try_claim_fallback(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _idempotency_key: &str,
        _now_unix_secs: u64,
        allow_done: bool,
    ) -> StorageResult<bool> {
        Ok(allow_done)
    }

    async fn finish_fallback_claim(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _idempotency_key: &str,
        _succeeded: bool,
        _restore_done: bool,
        _now_unix_secs: u64,
    ) -> StorageResult<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyRefreshClaim {
    Acquired,
    Enqueued { idempotency_key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LazyRefreshTaskState {
    Active,
    Done,
    TerminalFailure { reason: String },
}

enum WaitGenerationOutcome {
    GenerationAdvanced,
    JobDoneWithoutAdvance,
    JobTerminalFailure { reason: String },
    TimedOut { reason: String },
    Cancelled,
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
        _holder: &str,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> StorageResult<LazyRefreshClaim> {
        let job = OAuthRefreshJob::new(upstream_id);
        let idempotency_key = job.idempotency_key(expires_at_unix_secs);
        let task = SchedulerPushTask {
            args: AdaptiveJob::OAuthRefresh(job),
            idempotency_key: Some(idempotency_key.clone()),
            run_at_unix_secs: Some(now_unix_secs),
            max_attempts: None,
        };
        match self
            .scheduler_backend
            .push_or_expedite_adaptive_task(task)
            .await
        {
            Ok(()) | Err(cc_lb_scheduler::error::SchedulerError::Conflict(_)) => {
                Ok(LazyRefreshClaim::Enqueued { idempotency_key })
            }
            Err(error) => Err(scheduler_claim_error(error)),
        }
    }

    async fn complete_and_bump_generation(
        &self,
        _upstream_id: Uuid,
        _holder: &str,
        _generation: u64,
    ) -> StorageResult<bool> {
        Ok(false)
    }

    async fn release_if_holder(&self, _upstream_id: Uuid, _holder: &str) -> StorageResult<bool> {
        Ok(false)
    }

    async fn task_state(&self, idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
        apalis_lazy_refresh_task_state(&self.scheduler_backend, idempotency_key).await
    }

    async fn try_claim_fallback(
        &self,
        _upstream_id: Uuid,
        holder: &str,
        idempotency_key: &str,
        now_unix_secs: u64,
        allow_done: bool,
    ) -> StorageResult<bool> {
        self.scheduler_backend
            .claim_adaptive_task_for_fallback(idempotency_key, holder, now_unix_secs, allow_done)
            .await
            .map_err(scheduler_claim_error)
    }

    async fn finish_fallback_claim(
        &self,
        _upstream_id: Uuid,
        holder: &str,
        idempotency_key: &str,
        succeeded: bool,
        restore_done: bool,
        now_unix_secs: u64,
    ) -> StorageResult<()> {
        self.scheduler_backend
            .finish_adaptive_task_fallback(
                idempotency_key,
                holder,
                succeeded,
                restore_done,
                now_unix_secs,
            )
            .await
            .map_err(scheduler_claim_error)
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
            params.replica_id,
            params.metadata_hook,
            params.cancel,
            claim_guard,
            params.apalis_handle,
        )
    }

    pub fn new_with_claim_guard(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self::new_with_claim_guard_and_config(
            deps,
            replica_id,
            metadata_hook,
            cancel,
            claim_guard,
            LazyRefreshContentionConfig::production(),
            apalis_handle,
        )
    }

    #[cfg(test)]
    fn new_with_claim_guard_for_tests(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        contention: LazyRefreshContentionConfig,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        Self::new_with_claim_guard_and_config(
            deps,
            replica_id,
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            apalis_handle,
        )
    }

    fn new_with_claim_guard_and_config(
        deps: LazyRefresherDeps,
        replica_id: Uuid,
        metadata_hook: Option<MetadataHookHandle>,
        cancel: CancellationToken,
        claim_guard: Arc<dyn LazyRefreshClaimGuard>,
        contention: LazyRefreshContentionConfig,
        apalis_handle: SchedulerBackend,
    ) -> Self {
        let http = TokenHttpClient::new(Duration::from_secs(30));
        Self {
            stores: deps.stores,
            aead: deps.aead,
            oauth_cfg: deps.oauth_cfg,
            google_config: deps.google_config,
            replica_id,
            http,
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            apalis_handle,
            clock: deps.clock,
            in_flight: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    async fn wait_for_token_generation(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
        idempotency_key: Option<&str>,
    ) -> Result<WaitGenerationOutcome, LazyRefreshError> {
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
                return Ok(WaitGenerationOutcome::GenerationAdvanced);
            }

            if let Some(idempotency_key) = idempotency_key {
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
                        return Ok(WaitGenerationOutcome::GenerationAdvanced);
                    }
                    return Ok(match task_state {
                        LazyRefreshTaskState::Done => WaitGenerationOutcome::JobDoneWithoutAdvance,
                        LazyRefreshTaskState::TerminalFailure { reason } => {
                            WaitGenerationOutcome::JobTerminalFailure { reason }
                        }
                        LazyRefreshTaskState::Active => unreachable!(),
                    });
                }
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                ::metrics::counter!(cc_lb_scheduler::scheduler_metrics::LAZY_REFRESH_TIMEOUT_TOTAL)
                    .increment(1);
                return Ok(WaitGenerationOutcome::TimedOut {
                    reason: "oauth refresh timed out waiting for another holder".to_owned(),
                });
            }
            let sleep_for = self.contention.poll_interval.min(deadline - now);
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    return Ok(WaitGenerationOutcome::Cancelled);
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
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub google_config: Arc<cc_lb_config::Config>,
    pub clock: ClockHandle,
}

pub struct LazyRefresherParams {
    pub deps: LazyRefresherDeps,
    pub replica_id: Uuid,
    pub metadata_hook: Option<MetadataHookHandle>,
    pub cancel: CancellationToken,
    pub apalis_handle: SchedulerBackend,
}

impl LazyRefresher {
    async fn run_in_process_refresh(
        self,
        upstream_id: Uuid,
        starting_generation: u64,
    ) -> Result<(), LazyRefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        if upstream.oauth_token_generation > starting_generation {
            return Ok(());
        }
        let is_anthropic_oauth = upstream.kind == UpstreamKind::AnthropicOauth;
        let result = refresh_flow(
            &self.stores,
            self.stores.audit.as_deref(),
            &self.aead,
            &self.oauth_cfg,
            &self.google_config,
            self.replica_id,
            &self.http,
            &self.cancel,
            upstream,
            &*self.clock,
        )
        .await;
        match result {
            Ok(generation) => {
                if is_anthropic_oauth && let Some(metadata_hook) = &self.metadata_hook {
                    metadata_hook
                        .enqueue(MetadataHookRequest {
                            upstream_id,
                            credential_generation: generation,
                            traceparent: None,
                        })
                        .await
                        .map_err(lazy_metadata_hook_error)?;
                }
                Ok(())
            }
            Err(error) => Err(LazyRefreshError::Failed {
                reason: error.to_string(),
            }),
        }
    }

    async fn execute_refresh_one(self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        let bundle = oauth_token_bundle(&upstream, &self.aead)?;
        if bundle.never_refresh {
            // Access-only imports and Anthropic long-lived grants must stop
            // before acquiring a refresh claim or contacting a token endpoint.
            return Err(LazyRefreshError::Failed {
                reason: RefreshError::NotRefreshable.to_string(),
            });
        }
        check_refresh_allowed(&self.stores, &upstream, &bundle, &*self.clock)
            .await
            .map_err(lazy_refresh_error)?;
        let holder = lazy_refresh_holder(self.replica_id);
        let expires_at_unix_secs = bundle.expires_at_unix_secs;
        let starting_generation = upstream.oauth_token_generation;
        let claim = self
            .claim_guard
            .begin_refresh(
                upstream_id,
                &holder,
                expires_at_unix_secs,
                unix_secs(self.clock.now()),
            )
            .await
            .map_err(lazy_error)?;
        match claim {
            LazyRefreshClaim::Enqueued { idempotency_key } => {
                match self
                    .wait_for_token_generation(
                        upstream_id,
                        starting_generation,
                        Some(&idempotency_key),
                    )
                    .await?
                {
                    WaitGenerationOutcome::GenerationAdvanced => Ok(()),
                    WaitGenerationOutcome::Cancelled => Err(LazyRefreshError::Failed {
                        reason: "oauth refresh cancelled".to_owned(),
                    }),
                    WaitGenerationOutcome::JobTerminalFailure { reason } => {
                        Err(LazyRefreshError::Failed { reason })
                    }
                    WaitGenerationOutcome::JobDoneWithoutAdvance => {
                        self.run_exclusive_in_process_fallback(
                            upstream_id,
                            &holder,
                            &idempotency_key,
                            starting_generation,
                            true,
                            "oauth refresh job completed without advancing generation",
                        )
                        .await
                    }
                    WaitGenerationOutcome::TimedOut { reason } => {
                        self.run_exclusive_in_process_fallback(
                            upstream_id,
                            &holder,
                            &idempotency_key,
                            starting_generation,
                            false,
                            &reason,
                        )
                        .await
                    }
                }
            }
            LazyRefreshClaim::Acquired => self.run_acquired_refresh(upstream, holder).await,
        }
    }

    async fn run_exclusive_in_process_fallback(
        self,
        upstream_id: Uuid,
        holder: &str,
        idempotency_key: &str,
        starting_generation: u64,
        allow_done: bool,
        contended_reason: &str,
    ) -> Result<(), LazyRefreshError> {
        let fallback_holder = format!("{holder}:fallback:{}", Uuid::new_v4());
        let claimed = self
            .claim_guard
            .try_claim_fallback(
                upstream_id,
                &fallback_holder,
                idempotency_key,
                unix_secs(self.clock.now()),
                allow_done,
            )
            .await
            .map_err(lazy_error)?;
        if !claimed {
            if let Some(current_generation) = self
                .stores
                .upstreams
                .read_oauth_token_generation(upstream_id)
                .await
                .map_err(lazy_error)?
                && current_generation > starting_generation
            {
                return Ok(());
            }
            tracing::warn!(
                upstream_id = %upstream_id,
                idempotency_key = %idempotency_key,
                "scheduled oauth refresh still holds exclusive ownership; skipping duplicate in-process exchange",
            );
            return Err(LazyRefreshError::Failed {
                reason: contended_reason.to_owned(),
            });
        }
        tracing::warn!(
            upstream_id = %upstream_id,
            idempotency_key = %idempotency_key,
            reason = %contended_reason,
            "acquired exclusive fallback ownership after scheduler-backed wait; running in-process refresh",
        );
        ::metrics::counter!("cclb_lazy_refresh_inproc_fallback_total").increment(1);
        let fallback_result = self
            .clone()
            .run_in_process_refresh(upstream_id, starting_generation)
            .await;
        if let Err(error) = self
            .claim_guard
            .finish_fallback_claim(
                upstream_id,
                &fallback_holder,
                idempotency_key,
                fallback_result.is_ok(),
                allow_done,
                unix_secs(self.clock.now()),
            )
            .await
        {
            tracing::warn!(
                error = %error,
                upstream_id = %upstream_id,
                idempotency_key = %idempotency_key,
                "failed to finalize fallback refresh task state",
            );
        }
        fallback_result
    }

    async fn run_acquired_refresh(
        self,
        upstream: UpstreamRecord,
        holder: String,
    ) -> Result<(), LazyRefreshError> {
        let upstream_id = upstream.id;
        let is_anthropic_oauth = upstream.kind == UpstreamKind::AnthropicOauth;
        let result = refresh_flow(
            &self.stores,
            self.stores.audit.as_deref(),
            &self.aead,
            &self.oauth_cfg,
            &self.google_config,
            self.replica_id,
            &self.http,
            &self.cancel,
            upstream,
            &*self.clock,
        )
        .await;
        match result {
            Ok(generation) => {
                let completed = self
                    .claim_guard
                    .complete_and_bump_generation(upstream_id, &holder, generation)
                    .await
                    .map_err(lazy_error)?;
                if !completed {
                    return Err(LazyRefreshError::Failed {
                        reason: "oauth refresh claim was not held at completion".to_owned(),
                    });
                }
                if is_anthropic_oauth {
                    if let Some(metadata_hook) = &self.metadata_hook {
                        metadata_hook
                            .enqueue(MetadataHookRequest {
                                upstream_id,
                                credential_generation: generation,
                                traceparent: None,
                            })
                            .await
                            .map_err(lazy_metadata_hook_error)?;
                    } else {
                        self.apalis_handle
                            .push_job(AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(
                                upstream_id,
                                generation,
                            )))
                            .await
                            .map_err(lazy_scheduler_error)?;
                    }
                }
                Ok(())
            }
            Err(error) => {
                if let Err(release_error) = self
                    .claim_guard
                    .release_if_holder(upstream_id, &holder)
                    .await
                {
                    tracing::warn!(error = %release_error, upstream_id = %upstream_id, "oauth refresh claim release failed");
                }
                Err(LazyRefreshError::Failed {
                    reason: error.to_string(),
                })
            }
        }
    }
}

#[async_trait]
impl LazyRefreshHandle for LazyRefresher {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        // Share one detached per-upstream refresh task across concurrent or
        // subsequent callers so cancelling a caller (for example a timed-out
        // WebSocket handshake) releases nothing prematurely and never races a
        // second credential exchange before the first task persists its result.
        let mut rx = {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = in_flight.get(&upstream_id) {
                existing.clone()
            } else {
                let (tx, rx) = tokio::sync::watch::channel(None);
                in_flight.insert(upstream_id, rx.clone());
                let this = self.clone();
                let in_flight_map = Arc::clone(&self.in_flight);
                tokio::spawn(async move {
                    let guard = InFlightRefreshGuard {
                        in_flight: in_flight_map,
                        upstream_id,
                    };
                    let outcome = this
                        .execute_refresh_one(upstream_id)
                        .await
                        .map_err(|error| match error {
                            LazyRefreshError::Failed { reason } => reason,
                        });
                    drop(guard);
                    let _ = tx.send(Some(outcome));
                });
                rx
            }
        };
        let outcome = rx
            .wait_for(Option::is_some)
            .await
            .map_err(|_| LazyRefreshError::Failed {
                reason: "oauth refresh task failed".to_owned(),
            })?;
        match outcome.as_ref().expect("watch waited for Some(outcome)") {
            Ok(()) => Ok(()),
            Err(reason) => Err(LazyRefreshError::Failed {
                reason: reason.clone(),
            }),
        }
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
        let holder = lazy_refresh_holder(self.replica_id);
        let bundle = oauth_token_bundle(&upstream, &self.aead)?;
        if bundle.never_refresh {
            // Long-lived credentials are never refreshed; there is nothing to
            // enqueue.
            return Ok(());
        }
        check_refresh_allowed(&self.stores, &upstream, &bundle, &*self.clock)
            .await
            .map_err(lazy_refresh_error)?;
        let expires_at_unix_secs = bundle.expires_at_unix_secs;
        let claim = self
            .claim_guard
            .begin_refresh(
                upstream_id,
                &holder,
                expires_at_unix_secs,
                unix_secs(self.clock.now()),
            )
            .await
            .map_err(lazy_error)?;
        if let LazyRefreshClaim::Acquired = claim
            && let Err(release_error) = self
                .claim_guard
                .release_if_holder(upstream_id, &holder)
                .await
        {
            tracing::warn!(
                error = %release_error,
                upstream_id = %upstream_id,
                "soft refresh claim release failed",
            );
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RefreshError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("missing oauth credentials")]
    MissingCredentials,
    #[error("oauth decrypt failed")]
    Decrypt,
    #[error("oauth encrypt failed")]
    Encrypt,
    #[error("oauth token endpoint returned {0}")]
    Status(StatusCode),
    #[error("oauth token endpoint returned {0}; reauthorization required")]
    ReconnectRequired(StatusCode),
    #[error("oauth refresh token expired; reauthorization required")]
    RefreshTokenExpired,
    #[error("oauth credential requires reauthorization")]
    ExistingReconnectRequired,
    #[error("oauth token endpoint request failed: {0}")]
    Http(String),
    #[error("oauth token response parse failed: {0}")]
    Parse(cc_lb_oauth_protocol::TokenEndpointParseError),
    #[error("oauth refresh cancelled")]
    Cancelled,
    #[error(
        "oauth_client_not_configured: configure oauth.antigravity.client_id and oauth.antigravity.client_secret before refreshing Antigravity credentials"
    )]
    AntigravityClientNotConfigured,
    /// The credential's policy prohibits token refresh. This protects both
    /// access-only imports and Anthropic long-lived grants from rotation.
    #[error("long-lived oauth credential must not be refreshed; reauthorization required")]
    NotRefreshable,
}

async fn check_refresh_allowed(
    stores: &Stores,
    upstream: &UpstreamRecord,
    bundle: &OAuthTokenBundle,
    clock: &dyn Clock,
) -> Result<(), RefreshError> {
    if bundle.never_refresh {
        return Err(RefreshError::NotRefreshable);
    }
    if refresh_requires_reconnect(upstream.last_apply_error.as_deref()) {
        return Err(RefreshError::ExistingReconnectRequired);
    }
    if bundle
        .refresh_token_expires_at_unix_secs
        .is_some_and(|expires_at| expires_at <= unix_secs(clock.now()))
    {
        record_refresh_failure(stores, upstream, "refresh_token_expired".to_owned()).await?;
        return Err(RefreshError::RefreshTokenExpired);
    }
    Ok(())
}

async fn record_refresh_failure(
    stores: &Stores,
    upstream: &UpstreamRecord,
    reason: String,
) -> StorageResult<()> {
    stores
        .upstreams
        .set_status(
            upstream.id,
            UpstreamStatusUpdate {
                last_apply_error: Some(Some(reason)),
                expected_oauth_token_generation: Some(upstream.oauth_token_generation),
                ..Default::default()
            },
        )
        .await
}

#[allow(clippy::too_many_arguments)]
async fn refresh_flow(
    stores: &Stores,
    audit: Option<&dyn AuditStore>,
    aead: &AeadService,
    oauth_cfg: &AnthropicOAuthConfig,
    google_config: &cc_lb_config::Config,
    replica_id: Uuid,
    http: &TokenHttpClient,
    cancel: &CancellationToken,
    upstream: UpstreamRecord,
    clock: &dyn Clock,
) -> Result<u64, RefreshError> {
    let current = stores
        .upstreams
        .get_by_id(upstream.id)
        .await?
        .ok_or(RefreshError::MissingCredentials)?;
    if current.oauth_token_generation > upstream.oauth_token_generation {
        return Ok(current.oauth_token_generation);
    }
    let upstream = current;
    let previous = upstream
        .oauth_credentials
        .as_ref()
        .ok_or(RefreshError::MissingCredentials)?
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| RefreshError::Decrypt)?;
    check_refresh_allowed(stores, &upstream, &previous, clock).await?;
    let token = request_refresh(
        http,
        oauth_cfg,
        google_config,
        upstream.kind,
        previous.oauth_profile,
        cancel,
        &previous.refresh_token,
    )
    .await;
    match token {
        Ok(response) => {
            let account_id = response.account_id.clone().or(previous.account_id.clone());
            let refreshed = refreshed_token_parts(
                ExistingTokenParts {
                    refresh_token: previous.refresh_token,
                    refresh_token_expires_at_unix_secs: previous.refresh_token_expires_at_unix_secs,
                    scopes: previous.scopes,
                },
                response,
                unix_secs(clock.now()),
            );
            let bundle = OAuthTokenBundle {
                access_token: refreshed.access_token,
                refresh_token: refreshed.refresh_token,
                expires_at_unix_secs: refreshed.expires_at_unix_secs,
                refresh_token_expires_at_unix_secs: refreshed.refresh_token_expires_at_unix_secs,
                scopes: refreshed.scopes,
                never_refresh: previous.never_refresh,
                account_id,
                project_id: previous.project_id,
                oauth_profile: previous.oauth_profile,
            };
            let fingerprint = access_token_fingerprint(&bundle.access_token);
            let expires_at = bundle.expires_at_unix_secs;
            let encrypted = EncryptedOAuthTokens::encrypt(aead, &bundle, upstream.id.as_bytes())
                .map_err(|_| RefreshError::Encrypt)?;
            let updated = stores
                .upstreams
                .complete_refresh(upstream.id, replica_id, encrypted)
                .await?;
            increment_metric(&upstream.name, "success");
            emit_audit(
                audit,
                AuditPayload::UpstreamOauthRefreshSuccess {
                    upstream_id: upstream.id.to_string(),
                    upstream_name: upstream.name.clone(),
                    expires_at_unix_secs: expires_at,
                    access_token_fingerprint: fingerprint,
                    replica_id,
                },
                &upstream,
                200,
                clock,
            )
            .await;
            Ok(updated.oauth_token_generation)
        }
        Err(error) => {
            increment_metric(&upstream.name, "failure");
            let reason = reason_for(&error);
            let status_result = record_refresh_failure(stores, &upstream, reason.clone()).await;
            emit_audit(
                audit,
                AuditPayload::UpstreamOauthRefreshFailure {
                    upstream_id: upstream.id.to_string(),
                    upstream_name: upstream.name.clone(),
                    reason,
                    replica_id,
                },
                &upstream,
                500,
                clock,
            )
            .await;
            status_result?;
            Err(error)
        }
    }
}

async fn request_refresh(
    http: &TokenHttpClient,
    oauth_cfg: &AnthropicOAuthConfig,
    google_config: &cc_lb_config::Config,
    oauth_kind: UpstreamKind,
    oauth_profile: cc_lb_oauth_protocol::GeminiOAuthProfile,
    cancel: &CancellationToken,
    refresh_token: &str,
) -> Result<TokenEndpointResponse, RefreshError> {
    let antigravity = google_config.oauth.antigravity.clone().unwrap_or_default();
    let (token_url, content_type, body) = match oauth_kind {
        UpstreamKind::AnthropicOauth => (
            oauth_cfg.token_url.as_str(),
            "application/x-www-form-urlencoded",
            refresh_token_form_body(oauth_cfg.client_id.as_str(), refresh_token),
        ),
        UpstreamKind::OpenaiOauth => (
            OPENAI_OAUTH_TOKEN_URL,
            "application/json",
            refresh_token_json_body(OPENAI_CODEX_CLIENT_ID, refresh_token),
        ),
        UpstreamKind::GeminiOauth => {
            let (client_id, client_secret, token_url) = match oauth_profile {
                cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity => {
                    if antigravity.missing_client_credentials().is_some() {
                        return Err(RefreshError::AntigravityClientNotConfigured);
                    }
                    (
                        antigravity.client_id.as_str(),
                        antigravity.client_secret.as_str(),
                        antigravity.token_url.as_str(),
                    )
                }
                cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli => (
                    GEMINI_OAUTH_CLIENT_ID,
                    GEMINI_OAUTH_CLIENT_SECRET,
                    GEMINI_OAUTH_TOKEN_URL,
                ),
            };
            (
                token_url,
                "application/x-www-form-urlencoded",
                cc_lb_oauth_protocol::gemini_refresh_token_form_body(
                    client_id,
                    client_secret,
                    refresh_token,
                ),
            )
        }
        _ => return Err(RefreshError::NotRefreshable),
    };
    let content_length = HeaderValue::from_str(&body.len().to_string())
        .map_err(|error| RefreshError::Http(error.to_string()))?;
    let request = Request::post(token_url)
        .header(CONTENT_TYPE, content_type)
        .header(CONTENT_LENGTH, content_length)
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| RefreshError::Http(error.to_string()))?;

    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        response = tokio::time::timeout(http.timeout, http.client.request(request)) => {
            response
                .map_err(|_| RefreshError::Http("request timed out".to_owned()))?
                .map_err(|error| RefreshError::Http(error.to_string()))?
        }
    };
    let status = response.status();
    if !status.is_success() && status != StatusCode::BAD_REQUEST {
        return Err(if is_terminal_refresh_failure(status.as_u16(), &[]) {
            RefreshError::ReconnectRequired(status)
        } else {
            RefreshError::Status(status)
        });
    }
    let bytes = tokio::select! {
        _ = cancel.cancelled() => return Err(RefreshError::Cancelled),
        bytes = tokio::time::timeout(http.timeout, response.into_body().collect()) => {
            bytes
                .map_err(|_| RefreshError::Http("response body timed out".to_owned()))?
                .map_err(|error| RefreshError::Http(error.to_string()))?
                .to_bytes()
        }
    };
    if !status.is_success() {
        return Err(if is_terminal_refresh_failure(status.as_u16(), &bytes) {
            RefreshError::ReconnectRequired(status)
        } else {
            RefreshError::Status(status)
        });
    }
    parse_token_endpoint_response(&bytes).map_err(RefreshError::Parse)
}

fn access_token_fingerprint(access_token: &str) -> String {
    let digest = Sha256::digest(access_token.as_bytes());
    to_hex(&digest[..4])
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn increment_metric(upstream: &str, outcome: &'static str) {
    metrics::counter!(
        "cclb_oauth_refresh_total",
        "upstream" => upstream.to_owned(),
        "outcome" => outcome
    )
    .increment(1);
}

async fn emit_audit(
    audit: Option<&dyn AuditStore>,
    payload: AuditPayload,
    upstream: &UpstreamRecord,
    status: u16,
    clock: &dyn Clock,
) {
    let Some(audit) = audit else {
        return;
    };
    let now = unix_secs(clock.now());
    let AuditActorFields {
        actor,
        authority,
        subject,
        kind,
        email,
    } = AuditActorFields::system("oauth_refresh");
    let entry = AuditEntry {
        ts: now,
        request_id: format!("oauth-refresh-{}-{now}", upstream.id),
        principal_id: String::new(),
        route: "oauth_refresh".to_owned(),
        upstream: upstream.id.to_string(),
        model: None,
        status,
        input_tokens: None,
        output_tokens: None,
        duration_ms: 0,
        agent_label: None,
        api_key_id: None,
        cost_usd_micros: None,
        limit_violation: None,
        admin_action: Some(payload.to_string()),
        actor: Some(actor),
        actor_authority: Some(authority),
        actor_subject: Some(subject),
        actor_kind: Some(kind),
        actor_email: email,
        payload: None,
    };
    if let Err(error) = audit.append_audit(&entry).await {
        tracing::warn!(error = %error, upstream_id = %upstream.id, "oauth refresh audit append failed");
    }
}

fn reason_for(error: &RefreshError) -> String {
    match error {
        RefreshError::ReconnectRequired(status) => format!("status_{}", status.as_u16()),
        RefreshError::Status(StatusCode::BAD_REQUEST) => "status_400_retryable".to_owned(),
        RefreshError::Status(status) => format!("status_{}", status.as_u16()),
        RefreshError::RefreshTokenExpired => "refresh_token_expired".to_owned(),
        RefreshError::ExistingReconnectRequired => "reauthorization_required".to_owned(),
        RefreshError::Http(_) => "network".to_owned(),
        RefreshError::Parse(_) => "parse".to_owned(),
        RefreshError::Cancelled => "cancelled".to_owned(),
        RefreshError::MissingCredentials => "missing_credentials".to_owned(),
        RefreshError::Decrypt => "decrypt".to_owned(),
        RefreshError::Encrypt => "encrypt".to_owned(),
        RefreshError::Storage(_) => "storage".to_owned(),
        RefreshError::AntigravityClientNotConfigured => "oauth_client_not_configured".to_owned(),
        // Unreachable in practice: `reason_for` is only called on errors from
        // `request_refresh`, and a long-lived credential is stopped by the
        // `refresh_one` guard long before that. The arm exists because the
        // variant does, and a catch-all would silently swallow future ones.
        RefreshError::NotRefreshable => "not_refreshable".to_owned(),
    }
}

fn lazy_refresh_error(error: RefreshError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_error(error: StorageError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_scheduler_error(error: cc_lb_scheduler::error::SchedulerError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn lazy_metadata_hook_error(error: cc_lb_control::MetadataHookEnqueueError) -> LazyRefreshError {
    LazyRefreshError::Failed {
        reason: error.to_string(),
    }
}

fn oauth_token_bundle(
    upstream: &UpstreamRecord,
    aead: &AeadService,
) -> Result<OAuthTokenBundle, LazyRefreshError> {
    let credentials =
        upstream
            .oauth_credentials
            .as_ref()
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "missing oauth credentials".to_owned(),
            })?;
    credentials
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| LazyRefreshError::Failed {
            reason: "oauth decrypt failed".to_owned(),
        })
}

fn lazy_refresh_holder(replica_id: Uuid) -> String {
    format!("cc-lb-server:lazy-refresher:{replica_id}")
}

#[cfg(any(feature = "storage-sqlite", feature = "storage-postgres"))]
fn scheduler_claim_error(error: cc_lb_scheduler::error::SchedulerError) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}

#[cfg(all(test, feature = "storage-sqlite"))]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_clock::{ClockHandle, TestClock};
    use cc_lb_config::AnthropicOAuthConfig;
    use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
    use cc_lb_storage_api::upstream::UpstreamKind;
    use cc_lb_storage_api::{MetaStore, StorageResult, UpstreamCreate, UpstreamStore};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, watch};
    use tokio_util::sync::CancellationToken;
    use url::Url;
    use uuid::Uuid;

    use super::{
        LazyRefreshClaim, LazyRefreshClaimGuard, LazyRefreshContentionConfig, LazyRefreshTaskState,
        LazyRefresher, LazyRefresherDeps, SchedulerBackend,
    };

    #[tokio::test]
    async fn lazy_refresher_single_flight_under_concurrency() {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        fixture.hold_refresh_response();
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await
            .expect("starting generation read")
            .expect("starting generation exists");
        let claims = Arc::new(TestOAuthRefreshClaims::single_winner());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_secs(30),
            Duration::from_millis(5),
        );
        let first = fixture.lazy_refresher(claims.clone(), config);
        let second = fixture.lazy_refresher(claims.clone(), config);

        let first = tokio::spawn(async move { first.refresh_one(upstream_id).await });
        fixture.wait_for_refresh_start().await;
        let second = tokio::spawn(async move { second.refresh_one(upstream_id).await });
        claims.wait_for_contention().await;
        fixture.release_refresh_response();

        first
            .await
            .expect("first lazy refresh task joins")
            .expect("first lazy refresh succeeds");
        second
            .await
            .expect("second lazy refresh task joins")
            .expect("second lazy refresh joins");
        assert_eq!(fixture.refresh_call_count(), 1);
        assert_eq!(claims.completed_generation(), Some(starting_generation + 1),);
    }

    #[tokio::test]
    async fn lazy_refresher_falls_back_to_inproc_when_scheduler_wait_times_out() {
        let fixture = LazyRefreshFixture::new(Duration::from_millis(0)).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let claims = Arc::new(TestOAuthRefreshClaims::always_contended());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_millis(20),
            Duration::from_millis(5),
        );
        let refresher = fixture.lazy_refresher(claims, config);

        refresher
            .refresh_one(upstream_id)
            .await
            .expect("scheduler-backed wait times out and falls back to in-proc refresh");

        assert_eq!(fixture.refresh_call_count(), 1);
    }

    #[tokio::test]
    async fn lazy_refresher_completes_token_rotation_when_caller_is_cancelled()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        fixture.hold_refresh_response();
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let claims = Arc::new(TestOAuthRefreshClaims::single_winner());
        let refresher = fixture.lazy_refresher(
            claims.clone(),
            LazyRefreshContentionConfig::for_tests(
                Duration::from_secs(30),
                Duration::from_millis(5),
            ),
        );

        let caller_task = tokio::spawn(async move { refresher.refresh_one(upstream_id).await });
        fixture.wait_for_refresh_start().await;
        let mut completion = claims.completion.subscribe();
        caller_task.abort();
        let _ = caller_task.await;

        fixture.release_refresh_response();
        let completed = tokio::time::timeout(
            Duration::from_secs(5),
            completion.wait_for(|generation| generation.is_some()),
        )
        .await
        .expect("detached refresh completes within 5s")
        .expect("completion watch stays open");
        assert_eq!(*completed, Some(starting_generation + 1));
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?
                .ok_or("missing rotated generation")?,
            starting_generation + 1
        );
        assert_eq!(fixture.refresh_call_count(), 1);
        assert_eq!(claims.completed_generation(), Some(starting_generation + 1));
        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await?
            .ok_or("missing rotated upstream")?;
        let decrypted = stored
            .oauth_credentials
            .as_ref()
            .ok_or("missing rotated credentials")?
            .decrypt(&fixture.aead, upstream_id.as_bytes())?;
        assert_eq!(decrypted.refresh_token, "sk-ant-ort01-new");
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_second_caller_joins_detached_in_process_fallback_after_first_times_out()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug)]
        struct DoneWithoutBumpClaims;

        #[async_trait::async_trait]
        impl LazyRefreshClaimGuard for DoneWithoutBumpClaims {
            async fn begin_refresh(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _expires_at_unix_secs: u64,
                _now_unix_secs: u64,
            ) -> StorageResult<LazyRefreshClaim> {
                Ok(LazyRefreshClaim::Enqueued {
                    idempotency_key: "done:no-bump".to_owned(),
                })
            }

            async fn complete_and_bump_generation(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
                _generation: u64,
            ) -> StorageResult<bool> {
                Ok(true)
            }

            async fn release_if_holder(
                &self,
                _upstream_id: Uuid,
                _holder: &str,
            ) -> StorageResult<bool> {
                Ok(true)
            }

            async fn task_state(
                &self,
                _idempotency_key: &str,
            ) -> StorageResult<LazyRefreshTaskState> {
                Ok(LazyRefreshTaskState::Done)
            }
        }

        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        fixture.hold_refresh_response();
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let claims = Arc::new(DoneWithoutBumpClaims);
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_secs(5),
            Duration::from_millis(5),
        );
        let refresher1 = fixture.lazy_refresher(claims.clone(), config);
        let refresher2 = refresher1.clone();

        let caller1 = tokio::spawn(async move { refresher1.refresh_one(upstream_id).await });
        fixture.wait_for_refresh_start().await;
        // Abort Caller 1 (simulating WebSocket connect timeout) while its detached in-process
        // exchange is still holding at `/oauth/token`.
        caller1.abort();
        let _ = caller1.await;
        assert_eq!(fixture.refresh_call_count(), 1);

        // Caller 2 arrives for the same upstream: synchronously polling `caller2` once via
        // `select! { biased; ... }` proves it joins Caller 1's shared in-flight watch channel
        // rather than launching a second `/oauth/token` exchange.
        let mut caller2 = Box::pin(refresher2.refresh_one(upstream_id));
        tokio::select! {
            biased;
            res = &mut caller2 => panic!("caller2 unexpectedly completed before token endpoint released: {res:?}"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(fixture.refresh_call_count(), 1);

        fixture.release_refresh_response();
        caller2.await?;

        assert_eq!(
            fixture.refresh_call_count(),
            1,
            "second caller must share detached in-process refresh instead of exchanging rotating token twice"
        );
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?
                .ok_or("missing rotated generation")?,
            starting_generation + 1
        );
        Ok(())
    }

    #[tokio::test]
    async fn lazy_refresher_refuses_in_process_fallback_when_scheduled_worker_is_running()
    -> Result<(), Box<dyn std::error::Error>> {
        // Part A: Mock claim guard simulating an active Running scheduled worker.
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let starting_generation = fixture
            .storage
            .read_oauth_token_generation(upstream_id)
            .await?
            .ok_or("missing starting generation")?;
        let claims = Arc::new(TestOAuthRefreshClaims::active_worker_in_progress());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_millis(15),
            Duration::from_millis(5),
        );
        let refresher = fixture.lazy_refresher(claims, config);

        let error = refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("timeout while scheduled worker holds active ownership must fail closed");
        assert!(matches!(
            error,
            crate::refresh::LazyRefreshError::Failed { .. }
        ));
        assert_eq!(
            fixture.refresh_call_count(),
            0,
            "must never invoke /oauth/token while scheduled worker owns the job"
        );

        // Part B: Real SQLite `ApalisLazyRefreshClaimGuard` + scheduler `Jobs` table.
        let guard = Arc::new(super::ApalisLazyRefreshClaimGuard::new(
            fixture.scheduler_backend.clone(),
        ));
        let job = cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob::new(upstream_id);
        let idempotency_key = job.idempotency_key(1);
        let real_refresher = fixture.lazy_refresher(
            guard,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(15),
                Duration::from_millis(5),
            ),
        );
        // Enqueue the job via `begin_refresh` indirectly by running once under a pre-claimed row:
        fixture
            .scheduler_backend
            .push_or_expedite_adaptive_task(cc_lb_scheduler::worker::SchedulerPushTask {
                args: cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(job),
                idempotency_key: Some(idempotency_key.clone()),
                run_at_unix_secs: Some(1_700_000_000),
                max_attempts: None,
            })
            .await?;
        sqlx::query(
            "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen, started_at) \
             VALUES ('worker-active', 'adaptive', 'default', '', 1700000000, 1700000000)",
        )
        .execute(&fixture.scheduler_pool)
        .await?;
        sqlx::query(
            "UPDATE Jobs SET status = 'Running', lock_by = 'worker-active', lock_at = 1700000000 WHERE idempotency_key = ?1",
        )
        .bind(&idempotency_key)
        .execute(&fixture.scheduler_pool)
        .await?;

        real_refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("running SQLite scheduler job must block in-process fallback");
        assert_eq!(fixture.refresh_call_count(), 0);
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?
                .ok_or("missing generation after active skip")?,
            starting_generation
        );

        // Once released back to unowned Pending, the fallback claims exclusive ownership and succeeds.
        sqlx::query(
            "UPDATE Jobs SET status = 'Pending', lock_by = NULL, lock_at = NULL WHERE idempotency_key = ?1",
        )
        .bind(&idempotency_key)
        .execute(&fixture.scheduler_pool)
        .await?;
        real_refresher.refresh_one(upstream_id).await?;
        assert_eq!(fixture.refresh_call_count(), 1);
        assert_eq!(
            fixture
                .storage
                .read_oauth_token_generation(upstream_id)
                .await?
                .ok_or("missing rotated generation after pending claim")?,
            starting_generation + 1
        );
        let leftover_fallback_workers: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM Workers WHERE worker_type = 'lazy-fallback'")
                .fetch_one(&fixture.scheduler_pool)
                .await?;
        assert_eq!(leftover_fallback_workers, 0);
        Ok(())
    }

    /// `LazyRefresher::refresh_one` is the single chokepoint every lazy and
    /// usage-poll refresh caller funnels through. The per-caller early returns
    /// upstream of it are defence in depth; this guard is what makes a refresh
    /// of a 365-day credential impossible for any present or future caller,
    /// so it needs coverage of its own.
    #[tokio::test]
    async fn lazy_refresher_refuses_to_refresh_a_long_lived_credential() {
        let fixture = LazyRefreshFixture::new(Duration::from_millis(0)).await;
        let upstream_id = fixture.create_oauth_upstream_with_mode(true).await;
        let claims = Arc::new(TestOAuthRefreshClaims::single_winner());
        let refresher = fixture.lazy_refresher(
            claims,
            LazyRefreshContentionConfig::for_tests(
                Duration::from_millis(20),
                Duration::from_millis(5),
            ),
        );

        let error = refresher
            .refresh_one(upstream_id)
            .await
            .expect_err("a long-lived credential must never be refreshed");

        assert!(
            error.to_string().contains("reauthorization required"),
            "operator-facing error must ask for reauthorization, got: {error}"
        );
        // The Anthropic token endpoint was never contacted: rotating the grant
        // is what destroys the 365-day token.
        assert_eq!(fixture.refresh_call_count(), 0);

        let stored = UpstreamStore::get_by_id(fixture.storage.as_ref(), upstream_id)
            .await
            .expect("upstream readable")
            .expect("upstream present");
        let bundle = stored
            .oauth_credentials
            .expect("credentials present")
            .decrypt(fixture.aead.as_ref(), upstream_id.as_bytes())
            .expect("credentials decrypt");
        assert_eq!(bundle.access_token, "sk-ant-oat01-old");
        assert!(bundle.never_refresh);
    }

    struct ForbiddenRefreshClaims {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl LazyRefreshClaimGuard for ForbiddenRefreshClaims {
        async fn begin_refresh(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _expires_at_unix_secs: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<LazyRefreshClaim> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(cc_lb_storage_api::StorageError::Fatal {
                message: "access-only refresh must stop before claim acquisition".to_owned(),
            })
        }

        async fn complete_and_bump_generation(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _generation: u64,
        ) -> StorageResult<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        }

        async fn release_if_holder(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
        ) -> StorageResult<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        }

        async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(LazyRefreshTaskState::Done)
        }
    }

    #[tokio::test]
    async fn access_only_oauth_lazy_refresh_stops_before_claim_or_generation_write()
    -> Result<(), Box<dyn std::error::Error>> {
        for kind in [UpstreamKind::OpenaiOauth, UpstreamKind::GeminiOauth] {
            let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
            let id = Uuid::new_v4();
            let bundle = OAuthTokenBundle {
                oauth_profile: Default::default(),
                access_token: "access-only-test-token".to_owned(),
                refresh_token: String::new(),
                expires_at_unix_secs: 1,
                refresh_token_expires_at_unix_secs: None,
                account_id: None,
                project_id: (kind == UpstreamKind::GeminiOauth).then(|| "test-project".to_owned()),
                scopes: Vec::new(),
                never_refresh: true,
            };
            let encrypted =
                EncryptedOAuthTokens::encrypt(fixture.aead.as_ref(), &bundle, id.as_bytes())?;
            let original = UpstreamStore::create(
                fixture.storage.as_ref(),
                UpstreamCreate {
                    vertex: None,
                    vertex_credential_type: None,
                    vertex_quota_project_id: None,
                    id,
                    name: format!("{}-access-only-guard", kind.as_str()),
                    kind,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: Some(cc_lb_storage_api::OAuthTokensCreate {
                        tokens: encrypted,
                        never_refresh: true,
                    }),
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                },
            )
            .await?;
            let claims = Arc::new(ForbiddenRefreshClaims {
                calls: AtomicUsize::new(0),
            });
            let refresher = fixture.lazy_refresher(
                claims.clone(),
                LazyRefreshContentionConfig::for_tests(
                    Duration::from_secs(1),
                    Duration::from_millis(1),
                ),
            );
            let result = refresher.refresh_one(id).await;
            assert!(
                matches!(result, Err(cc_lb_signer_anthropic_oauth::LazyRefreshError::Failed { ref reason })
                    if reason == &super::RefreshError::NotRefreshable.to_string()),
                "access-only credentials must be rejected before any claim: {result:?}"
            );
            refresher.enqueue_only(id).await?;
            assert_eq!(claims.calls.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.refresh_call_count(), 0);
            let current = UpstreamStore::get_by_id(fixture.storage.as_ref(), id)
                .await?
                .ok_or("missing access-only upstream")?;
            assert_eq!(
                current.oauth_token_generation,
                original.oauth_token_generation
            );
            assert_eq!(current.revision, original.revision);
            assert_eq!(current.last_apply_error, original.last_apply_error);
            assert_eq!(
                current
                    .oauth_credentials
                    .as_ref()
                    .map(EncryptedOAuthTokens::ciphertext),
                original
                    .oauth_credentials
                    .as_ref()
                    .map(EncryptedOAuthTokens::ciphertext)
            );

            let flow_gate = super::check_refresh_allowed(
                fixture.stores.as_ref(),
                &current,
                &bundle,
                fixture.clock.as_ref(),
            )
            .await;
            assert!(matches!(
                flow_gate,
                Err(super::RefreshError::NotRefreshable)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn wait_for_token_generation_returns_ok_when_done_state_races_with_generation_advance() {
        // Race between wait_for_token_generation's two consecutive reads:
        //   1. Lazy reads upstream snapshot → generation still equals the starting value
        //      (the worker has not yet committed complete_refresh).
        //   2. The proactive worker commits complete_refresh (generation += 1) and apalis
        //      marks the task as Done.
        //   3. Lazy reads task_state → Done.
        //
        // Without the fix the lazy refresher concludes "completed without advancing
        // generation" and falls back to in_process_refresh, firing a redundant HTTP
        // refresh against the OAuth provider. With the fix the lazy refresher re-reads
        // the upstream after observing the terminal task state, sees the advanced
        // generation, and returns Ok without triggering the fallback.
        let fixture = LazyRefreshFixture::new(Duration::ZERO).await;
        let upstream_id = fixture.create_oauth_upstream().await;
        let claims: Arc<dyn LazyRefreshClaimGuard> = Arc::new(RaceDoneClaims::new(
            fixture.storage.clone(),
            fixture.aead.clone(),
            upstream_id,
        ));
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_secs(5),
            Duration::from_millis(10),
        );
        let refresher = fixture.lazy_refresher(claims, config);

        refresher
            .refresh_one(upstream_id)
            .await
            .expect("lazy refresh observes the racing winner's generation advance");

        assert_eq!(
            fixture.refresh_call_count(),
            0,
            "no in-process HTTP fallback should fire when the worker advanced the generation",
        );
    }

    struct LazyRefreshFixture {
        _dir: tempfile::TempDir,
        storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
        stores: Arc<crate::dynamic_view_builder::Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        refresh_calls: Arc<AtomicUsize>,
        refresh_started: watch::Sender<bool>,
        hold_refresh_response: Arc<AtomicBool>,
        refresh_release: Arc<Notify>,
        scheduler_pool: sqlx::SqlitePool,
        scheduler_backend: SchedulerBackend,
        clock: ClockHandle,
    }

    impl LazyRefreshFixture {
        async fn new(refresh_delay: Duration) -> Self {
            let refresh_calls = Arc::new(AtomicUsize::new(0));
            let (refresh_started, _) = watch::channel(false);
            let hold_refresh_response = Arc::new(AtomicBool::new(false));
            let refresh_release = Arc::new(Notify::new());
            let token_url = spawn_lazy_refresh_token_server(
                refresh_calls.clone(),
                refresh_delay,
                refresh_started.clone(),
                hold_refresh_response.clone(),
                refresh_release.clone(),
            )
            .await;
            let dir = tempfile::tempdir().expect("tempdir");
            let database_url = format!(
                "sqlite://{}",
                dir.path().join("lazy-refresh.sqlite").display()
            );
            let storage = Arc::new(
                cc_lb_storage_sqlite::open_sqlite(
                    &database_url,
                    Arc::new(cc_lb_clock::SystemClock),
                )
                .await
                .expect("storage opens"),
            );
            storage.initialize().await.expect("storage initializes");
            let scheduler_db_url =
                format!("sqlite://{}", dir.path().join("scheduler.sqlite").display());
            use std::str::FromStr as _;
            let scheduler_options = sqlx::sqlite::SqliteConnectOptions::from_str(&scheduler_db_url)
                .expect("parse url")
                .create_if_missing(true);
            let scheduler_pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(5)
                .connect_with(scheduler_options)
                .await
                .expect("scheduler storage opens");
            apalis_sqlite::SqliteStorage::setup(&scheduler_pool)
                .await
                .expect("scheduler storage initializes");
            let scheduler_backend =
                SchedulerBackend::Sqlite(cc_lb_scheduler::worker::SqliteSchedulerBackend::new(
                    scheduler_pool.clone(),
                    Arc::new(cc_lb_clock::SystemClock),
                ));
            let stores = Arc::new(crate::dynamic_view_builder::Stores {
                upstreams: storage.clone(),
                vertex_state: Arc::new(crate::dynamic_view_builder::VertexState::new(
                    storage.clone(),
                )),
                principals: storage.clone(),
                plugin_registry: storage.clone(),
                upstream_rate_limits: storage.clone(),
                upstream_subscription_quotas: storage.clone(),
                upstream_subscription_metadata: storage.clone(),
                organization_metadata: storage.clone(),
                plan_tiers: storage.clone(),
                prompt_cache_observations: storage.clone(),
                anthropic_compatibility_kv: storage.clone(),
                audit: Some(storage.clone()),
            });
            let aead = Arc::new(AeadService::from_master_key([42; 32]));
            let oauth_cfg = Arc::new(AnthropicOAuthConfig {
                client_id: "lazy-client".to_owned(),
                auth_url: Url::parse("http://127.0.0.1/oauth/authorize").expect("auth url"),
                token_url,
                redirect_uri: Url::parse("http://127.0.0.1/callback").expect("redirect url"),
                scopes: vec!["messages".to_owned()],
            });
            let clock = Arc::new(TestClock::new_at_secs(1_700_000_000)) as ClockHandle;
            Self {
                _dir: dir,
                storage,
                stores,
                aead,
                oauth_cfg,
                refresh_calls,
                refresh_started,
                hold_refresh_response,
                refresh_release,
                scheduler_pool,
                scheduler_backend,
                clock,
            }
        }

        async fn create_oauth_upstream(&self) -> Uuid {
            self.create_oauth_upstream_with_mode(false).await
        }

        async fn create_oauth_upstream_with_mode(&self, never_refresh: bool) -> Uuid {
            let record = self
                .storage
                .create(UpstreamCreate {
                    vertex: None,
                    vertex_credential_type: None,
                    vertex_quota_project_id: None,
                    id: Uuid::new_v4(),
                    name: "lazy-guard".to_owned(),
                    kind: UpstreamKind::AnthropicOauth,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_tokens: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                    workspace_id: None,
                    api_key_access: None,
                })
                .await
                .expect("upstream created");
            let encrypted = EncryptedOAuthTokens::encrypt(
                self.aead.as_ref(),
                &OAuthTokenBundle {
                    oauth_profile: Default::default(),
                    access_token: "sk-ant-oat01-old".to_owned(),
                    refresh_token: "sk-ant-ort01-old".to_owned(),
                    expires_at_unix_secs: 1,
                    refresh_token_expires_at_unix_secs: None,
                    account_id: None,
                    project_id: None,
                    scopes: vec!["messages".to_owned()],
                    never_refresh,
                },
                record.id.as_bytes(),
            )
            .expect("tokens encrypt");
            self.storage
                .store_oauth_tokens(record.id, record.revision, encrypted, never_refresh)
                .await
                .expect("tokens stored");
            record.id
        }

        fn lazy_refresher(
            &self,
            claims: Arc<dyn LazyRefreshClaimGuard>,
            config: LazyRefreshContentionConfig,
        ) -> LazyRefresher {
            LazyRefresher::new_with_claim_guard_for_tests(
                LazyRefresherDeps {
                    google_config: Default::default(),
                    stores: self.stores.clone(),
                    aead: self.aead.clone(),
                    oauth_cfg: self.oauth_cfg.clone(),
                    clock: self.clock.clone(),
                },
                Uuid::new_v4(),
                None,
                CancellationToken::new(),
                claims,
                config,
                self.scheduler_backend.clone(),
            )
        }

        fn refresh_call_count(&self) -> usize {
            self.refresh_calls.load(Ordering::SeqCst)
        }

        fn hold_refresh_response(&self) {
            self.hold_refresh_response.store(true, Ordering::SeqCst);
        }

        async fn wait_for_refresh_start(&self) {
            let mut refresh_started = self.refresh_started.subscribe();
            if *refresh_started.borrow() {
                return;
            }
            refresh_started
                .changed()
                .await
                .expect("test token server start sender remains alive");
        }

        fn release_refresh_response(&self) {
            self.hold_refresh_response.store(false, Ordering::SeqCst);
            self.refresh_release.notify_one();
        }
    }

    async fn spawn_lazy_refresh_token_server(
        refresh_calls: Arc<AtomicUsize>,
        refresh_delay: Duration,
        refresh_started: watch::Sender<bool>,
        hold_refresh_response: Arc<AtomicBool>,
        refresh_release: Arc<Notify>,
    ) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("token listener binds");
        let addr = listener.local_addr().expect("token listener addr");
        let app = Router::new()
            .route("/oauth/token", post(lazy_refresh_token_response))
            .with_state(TokenServerState {
                refresh_calls,
                refresh_delay,
                refresh_started,
                hold_refresh_response,
                refresh_release,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("token server runs");
        });
        Url::parse(&format!("http://{addr}/oauth/token")).expect("token url")
    }

    #[derive(Clone)]
    struct TokenServerState {
        refresh_calls: Arc<AtomicUsize>,
        refresh_delay: Duration,
        refresh_started: watch::Sender<bool>,
        hold_refresh_response: Arc<AtomicBool>,
        refresh_release: Arc<Notify>,
    }

    async fn lazy_refresh_token_response(
        State(state): State<TokenServerState>,
    ) -> Json<serde_json::Value> {
        state.refresh_calls.fetch_add(1, Ordering::SeqCst);
        state.refresh_started.send_replace(true);
        if state.hold_refresh_response.load(Ordering::SeqCst) {
            state.refresh_release.notified().await;
        }
        if !state.refresh_delay.is_zero() {
            tokio::time::sleep(state.refresh_delay).await;
        }
        Json(serde_json::json!({
            "access_token": "sk-ant-oat01-new",
            "refresh_token": "sk-ant-ort01-new",
            "expires_in": 3600,
            "scope": "messages"
        }))
    }

    #[derive(Debug)]
    struct TestOAuthRefreshClaims {
        first_acquire_wins: bool,
        allow_timeout_fallback: bool,
        acquired: AtomicBool,
        completed_generation: Mutex<Option<u64>>,
        completion: watch::Sender<Option<u64>>,
        contention_observed: watch::Sender<bool>,
    }

    impl TestOAuthRefreshClaims {
        fn single_winner() -> Self {
            let (completion, _) = watch::channel(None);
            let (contention_observed, _) = watch::channel(false);
            Self {
                first_acquire_wins: true,
                allow_timeout_fallback: false,
                acquired: AtomicBool::new(false),
                completed_generation: Mutex::new(None),
                completion,
                contention_observed,
            }
        }

        fn always_contended() -> Self {
            let (completion, _) = watch::channel(None);
            let (contention_observed, _) = watch::channel(false);
            Self {
                first_acquire_wins: false,
                allow_timeout_fallback: true,
                acquired: AtomicBool::new(true),
                completed_generation: Mutex::new(None),
                completion,
                contention_observed,
            }
        }

        fn active_worker_in_progress() -> Self {
            let (completion, _) = watch::channel(None);
            let (contention_observed, _) = watch::channel(false);
            Self {
                first_acquire_wins: false,
                allow_timeout_fallback: false,
                acquired: AtomicBool::new(true),
                completed_generation: Mutex::new(None),
                completion,
                contention_observed,
            }
        }

        fn completed_generation(&self) -> Option<u64> {
            *self.completed_generation.lock().expect("completed lock")
        }

        async fn wait_for_contention(&self) {
            let mut contention_observed = self.contention_observed.subscribe();
            if *contention_observed.borrow() {
                return;
            }
            contention_observed
                .changed()
                .await
                .expect("test contention sender remains alive");
        }
    }

    #[async_trait::async_trait]
    impl LazyRefreshClaimGuard for TestOAuthRefreshClaims {
        async fn begin_refresh(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _expires_at_unix_secs: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<LazyRefreshClaim> {
            if self.first_acquire_wins && !self.acquired.swap(true, Ordering::SeqCst) {
                Ok(LazyRefreshClaim::Acquired)
            } else {
                Ok(LazyRefreshClaim::Enqueued {
                    idempotency_key: "test:oauth-refresh".to_owned(),
                })
            }
        }

        async fn complete_and_bump_generation(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            generation: u64,
        ) -> StorageResult<bool> {
            *self.completed_generation.lock().expect("completed lock") = Some(generation);
            self.completion.send_replace(Some(generation));
            Ok(true)
        }

        async fn release_if_holder(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
        ) -> StorageResult<bool> {
            Ok(true)
        }

        async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
            if self.completion.borrow().is_some() {
                Ok(LazyRefreshTaskState::Done)
            } else {
                self.contention_observed.send_replace(true);
                Ok(LazyRefreshTaskState::Active)
            }
        }

        async fn try_claim_fallback(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _idempotency_key: &str,
            _now_unix_secs: u64,
            allow_done: bool,
        ) -> StorageResult<bool> {
            Ok(allow_done || self.allow_timeout_fallback)
        }
    }

    struct RaceDoneClaims {
        storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
        aead: Arc<AeadService>,
        upstream_id: Uuid,
        invoked: AtomicBool,
    }

    impl RaceDoneClaims {
        fn new(
            storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
            aead: Arc<AeadService>,
            upstream_id: Uuid,
        ) -> Self {
            Self {
                storage,
                aead,
                upstream_id,
                invoked: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl LazyRefreshClaimGuard for RaceDoneClaims {
        async fn begin_refresh(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _expires_at_unix_secs: u64,
            _now_unix_secs: u64,
        ) -> StorageResult<LazyRefreshClaim> {
            Ok(LazyRefreshClaim::Enqueued {
                idempotency_key: "race:done".to_owned(),
            })
        }

        async fn complete_and_bump_generation(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
            _generation: u64,
        ) -> StorageResult<bool> {
            Ok(true)
        }

        async fn release_if_holder(
            &self,
            _upstream_id: Uuid,
            _holder: &str,
        ) -> StorageResult<bool> {
            Ok(true)
        }

        async fn task_state(&self, _idempotency_key: &str) -> StorageResult<LazyRefreshTaskState> {
            if !self.invoked.swap(true, Ordering::SeqCst) {
                let rotated = EncryptedOAuthTokens::encrypt(
                    self.aead.as_ref(),
                    &OAuthTokenBundle {
                        oauth_profile: Default::default(),
                        access_token: "sk-ant-oat01-rotated".to_owned(),
                        refresh_token: "sk-ant-ort01-rotated".to_owned(),
                        expires_at_unix_secs: 9_999_999_999,
                        refresh_token_expires_at_unix_secs: None,
                        account_id: None,
                        project_id: None,
                        scopes: vec!["messages".to_owned()],
                        never_refresh: false,
                    },
                    self.upstream_id.as_bytes(),
                )
                .expect("rotated tokens encrypt");
                UpstreamStore::complete_refresh(
                    self.storage.as_ref(),
                    self.upstream_id,
                    Uuid::new_v4(),
                    rotated,
                )
                .await?;
            }
            Ok(LazyRefreshTaskState::Done)
        }
    }
}
