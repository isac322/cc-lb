use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_engine::clock::{Clock, ClockHandle, unix_secs};
use cc_lb_engine::{AuditPayload, MetadataHookHandle, MetadataHookRequest};
use cc_lb_oauth_protocol::{
    ExistingTokenParts, TokenEndpointResponse, parse_token_endpoint_response,
    refresh_token_form_body, refreshed_token_parts,
};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{
    AdaptiveJob, Filter, SchedulerBackend, SchedulerPushTask, TaskStatus,
};
use cc_lb_signer_anthropic_oauth::{LazyRefreshError, LazyRefreshHandle};
use cc_lb_storage_api::{
    AuditActorFields, AuditEntry, AuditStore, StorageError, StorageResult, UpstreamRecord,
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

#[async_trait]
trait LazyRefreshMetadataTaskPusher: Send + Sync {
    async fn push_metadata_refresh(
        &self,
        upstream_id: Uuid,
        generation: u64,
    ) -> Result<(), cc_lb_scheduler::error::SchedulerError>;
}

#[derive(Clone, Debug)]
struct SchedulerLazyRefreshMetadataTaskPusher {
    backend: SchedulerBackend,
}

#[async_trait]
impl LazyRefreshMetadataTaskPusher for SchedulerLazyRefreshMetadataTaskPusher {
    async fn push_metadata_refresh(
        &self,
        upstream_id: Uuid,
        generation: u64,
    ) -> Result<(), cc_lb_scheduler::error::SchedulerError> {
        self.backend
            .push_job(AdaptiveJob::MetadataRefresh(MetadataRefreshJob::new(
                upstream_id,
                generation,
            )))
            .await
    }
}

#[async_trait]
trait LazyRefreshExecutor: Send + Sync {
    async fn refresh(
        &self,
        refresher: &LazyRefresher,
        upstream: UpstreamRecord,
    ) -> Result<u64, RefreshError>;
}

struct ProductionLazyRefreshExecutor;

#[async_trait]
impl LazyRefreshExecutor for ProductionLazyRefreshExecutor {
    async fn refresh(
        &self,
        refresher: &LazyRefresher,
        upstream: UpstreamRecord,
    ) -> Result<u64, RefreshError> {
        refresh_flow(
            &refresher.stores,
            refresher.stores.audit.as_deref(),
            &refresher.aead,
            &refresher.oauth_cfg,
            refresher.replica_id,
            &refresher.http,
            &refresher.cancel,
            upstream,
            &*refresher.clock,
        )
        .await
    }
}

#[derive(Clone)]
pub struct LazyRefresher {
    stores: Arc<Stores>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    replica_id: Uuid,
    http: TokenHttpClient,
    metadata_hook: Option<MetadataHookHandle>,
    cancel: CancellationToken,
    claim_guard: Arc<dyn LazyRefreshClaimGuard>,
    contention: LazyRefreshContentionConfig,
    metadata_task_pusher: Arc<dyn LazyRefreshMetadataTaskPusher>,
    clock: ClockHandle,
    refresh_executor: Arc<dyn LazyRefreshExecutor>,
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
        match self.scheduler_backend.push_adaptive_task(task).await {
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
        refresh_executor: Arc<dyn LazyRefreshExecutor>,
    ) -> Self {
        Self {
            stores: deps.stores,
            aead: deps.aead,
            oauth_cfg: deps.oauth_cfg,
            replica_id,
            http: TokenHttpClient::new(Duration::from_secs(30)),
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            metadata_task_pusher: Arc::new(SuccessfulMetadataTaskPusher),
            clock: deps.clock,
            refresh_executor,
        }
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
        let metadata_task_pusher = Arc::new(SchedulerLazyRefreshMetadataTaskPusher {
            backend: apalis_handle,
        });
        Self {
            stores: deps.stores,
            aead: deps.aead,
            oauth_cfg: deps.oauth_cfg,
            replica_id,
            http,
            metadata_hook,
            cancel,
            claim_guard,
            contention,
            metadata_task_pusher,
            clock: deps.clock,
            refresh_executor: Arc::new(ProductionLazyRefreshExecutor),
        }
    }

    async fn wait_for_token_generation(
        &self,
        upstream_id: Uuid,
        starting_generation: u64,
        idempotency_key: Option<&str>,
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
    pub oauth_cfg: Arc<AnthropicOAuthConfig>,
    pub clock: ClockHandle,
}

pub struct LazyRefresherParams {
    pub deps: LazyRefresherDeps,
    pub replica_id: Uuid,
    pub metadata_hook: Option<MetadataHookHandle>,
    pub cancel: CancellationToken,
    pub apalis_handle: SchedulerBackend,
}

#[cfg(test)]
struct SuccessfulMetadataTaskPusher;

#[cfg(test)]
#[async_trait]
impl LazyRefreshMetadataTaskPusher for SuccessfulMetadataTaskPusher {
    async fn push_metadata_refresh(
        &self,
        _upstream_id: Uuid,
        _generation: u64,
    ) -> Result<(), cc_lb_scheduler::error::SchedulerError> {
        Ok(())
    }
}

impl LazyRefresher {
    async fn execute_refresh(&self, upstream: UpstreamRecord) -> Result<u64, RefreshError> {
        self.refresh_executor.refresh(self, upstream).await
    }
}
impl LazyRefresher {
    async fn in_process_refresh(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await
            .map_err(lazy_error)?
            .ok_or_else(|| LazyRefreshError::Failed {
                reason: "oauth upstream not found".to_owned(),
            })?;
        let result = self.execute_refresh(upstream).await;
        match result {
            Ok(generation) => {
                if let Some(metadata_hook) = &self.metadata_hook {
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
}

#[async_trait]
impl LazyRefreshHandle for LazyRefresher {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
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
        let expires_at_unix_secs = oauth_expires_at(&upstream, &self.aead)?;
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
        if let LazyRefreshClaim::Enqueued { idempotency_key } = claim {
            match self
                .wait_for_token_generation(
                    upstream_id,
                    upstream.oauth_token_generation,
                    Some(&idempotency_key),
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(wait_error) => {
                    tracing::warn!(
                        upstream_id = %upstream_id,
                        error = %wait_error,
                        "scheduler-backed refresh wait failed; falling back to in-process refresh",
                    );
                    ::metrics::counter!("cclb_lazy_refresh_inproc_fallback_total").increment(1);
                    return self.in_process_refresh(upstream_id).await;
                }
            }
        }
        let result = self.execute_refresh(upstream).await;
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
                    self.metadata_task_pusher
                        .push_metadata_refresh(upstream_id, generation)
                        .await
                        .map_err(lazy_scheduler_error)?;
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
        let expires_at_unix_secs = oauth_expires_at(&upstream, &self.aead)?;
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
    #[error("oauth token endpoint request failed: {0}")]
    Http(String),
    #[error("oauth token response parse failed: {0}")]
    Parse(cc_lb_oauth_protocol::TokenEndpointParseError),
    #[error("oauth refresh cancelled")]
    Cancelled,
}

#[allow(clippy::too_many_arguments)]
async fn refresh_flow(
    stores: &Stores,
    audit: Option<&dyn AuditStore>,
    aead: &AeadService,
    oauth_cfg: &AnthropicOAuthConfig,
    replica_id: Uuid,
    http: &TokenHttpClient,
    cancel: &CancellationToken,
    upstream: UpstreamRecord,
    clock: &dyn Clock,
) -> Result<u64, RefreshError> {
    let previous = upstream
        .oauth_credentials
        .as_ref()
        .ok_or(RefreshError::MissingCredentials)?
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| RefreshError::Decrypt)?;
    let token = request_refresh(http, oauth_cfg, cancel, &previous.refresh_token).await;
    match token {
        Ok(response) => {
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
            let status_result = stores
                .upstreams
                .set_last_apply_error(upstream.id, Some(reason.clone()))
                .await;
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
    cancel: &CancellationToken,
    refresh_token: &str,
) -> Result<TokenEndpointResponse, RefreshError> {
    let body = refresh_token_form_body(oauth_cfg.client_id.as_str(), refresh_token);
    let content_length = HeaderValue::from_str(&body.len().to_string())
        .map_err(|error| RefreshError::Http(error.to_string()))?;
    let request = Request::post(oauth_cfg.token_url.as_str())
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
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
    if !status.is_success() {
        return Err(RefreshError::Status(status));
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
        kind: Some(payload.to_string()),
        payload: None,
    };
    if let Err(error) = audit.append_audit(&entry).await {
        tracing::warn!(error = %error, upstream_id = %upstream.id, "oauth refresh audit append failed");
    }
}

fn reason_for(error: &RefreshError) -> String {
    match error {
        RefreshError::Status(status) => format!("status_{}", status.as_u16()),
        RefreshError::Http(_) => "network".to_owned(),
        RefreshError::Parse(_) => "parse".to_owned(),
        RefreshError::Cancelled => "cancelled".to_owned(),
        RefreshError::MissingCredentials => "missing_credentials".to_owned(),
        RefreshError::Decrypt => "decrypt".to_owned(),
        RefreshError::Encrypt => "encrypt".to_owned(),
        RefreshError::Storage(_) => "storage".to_owned(),
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

fn lazy_metadata_hook_error(error: cc_lb_engine::MetadataHookEnqueueError) -> LazyRefreshError {
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

fn lazy_refresh_holder(replica_id: Uuid) -> String {
    format!("cc-lb-server:lazy-refresher:{replica_id}")
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn scheduler_claim_error(error: cc_lb_scheduler::error::SchedulerError) -> StorageError {
    StorageError::Unavailable {
        message: error.to_string(),
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_config::AnthropicOAuthConfig;
    use cc_lb_engine::clock::ClockHandle;
    use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
    use cc_lb_storage_api::upstream::UpstreamKind;
    use cc_lb_storage_api::{StorageResult, UpstreamCreate, UpstreamRecord, UpstreamStore};
    use cc_lb_testkit::{InMemoryStorage, fixed_clock};
    use tokio::sync::{Notify, watch};
    use tokio_util::sync::CancellationToken;
    use url::Url;
    use uuid::Uuid;

    use super::{
        LazyRefreshClaim, LazyRefreshClaimGuard, LazyRefreshContentionConfig, LazyRefreshExecutor,
        LazyRefreshTaskState, LazyRefresher, LazyRefresherDeps, RefreshError,
    };

    #[tokio::test(start_paused = true)]
    async fn t2__lazy_refresher_single_flight_under_concurrency() {
        let fixture = LazyRefreshFixture::new();
        fixture.hold_refresh_response();
        let upstream_id = fixture.create_oauth_upstream().await;
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
        assert_eq!(claims.completed_generation(), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn t2__lazy_refresher_falls_back_to_inproc_when_scheduler_wait_times_out() {
        let fixture = LazyRefreshFixture::new();
        let upstream_id = fixture.create_oauth_upstream().await;
        let claims = Arc::new(TestOAuthRefreshClaims::always_contended());
        let config = LazyRefreshContentionConfig::for_tests(
            Duration::from_millis(20),
            Duration::from_millis(5),
        );
        let refresher = fixture.lazy_refresher(claims.clone(), config);

        let refresh = tokio::spawn(async move { refresher.refresh_one(upstream_id).await });
        claims.wait_for_contention().await;
        tokio::time::advance(Duration::from_millis(20)).await;
        refresh
            .await
            .expect("lazy refresh task joins")
            .expect("scheduler-backed wait times out and falls back to in-proc refresh");

        assert_eq!(fixture.refresh_call_count(), 1);
    }

    #[tokio::test]
    async fn t2__wait_for_token_generation_returns_ok_when_done_state_races_with_generation_advance()
     {
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
        let fixture = LazyRefreshFixture::new();
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
        storage: Arc<InMemoryStorage>,
        stores: Arc<crate::dynamic_view_builder::Stores>,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        refresh_executor: Arc<ScriptedRefreshExecutor>,
        clock: ClockHandle,
    }

    impl LazyRefreshFixture {
        fn new() -> Self {
            let clock = fixed_clock(1_700_000_000);
            let storage = Arc::new(InMemoryStorage::with_clock(clock.clone()));
            let stores = Arc::new(crate::dynamic_view_builder::Stores {
                upstreams: storage.clone(),
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
                auth_url: Url::parse("https://oauth.test/authorize").expect("auth url"),
                token_url: Url::parse("https://oauth.test/token").expect("token url"),
                redirect_uri: Url::parse("https://oauth.test/callback").expect("redirect url"),
                scopes: vec!["messages".to_owned()],
            });
            let refresh_executor =
                Arc::new(ScriptedRefreshExecutor::new(storage.clone(), aead.clone()));
            Self {
                storage,
                stores,
                aead,
                oauth_cfg,
                refresh_executor,
                clock,
            }
        }

        async fn create_oauth_upstream(&self) -> Uuid {
            let record = self
                .storage
                .create(UpstreamCreate {
                    name: "lazy-guard".to_owned(),
                    kind: UpstreamKind::AnthropicOauth,
                    base_url: None,
                    api_key_ciphertext: None,
                    oauth_token_generation: None,
                    warmup_enabled: false,
                    warmup_dialect_plugin: None,
                })
                .await
                .expect("upstream created");
            let encrypted = EncryptedOAuthTokens::encrypt(
                self.aead.as_ref(),
                &OAuthTokenBundle {
                    access_token: "sk-ant-oat01-old".to_owned(),
                    refresh_token: "sk-ant-ort01-old".to_owned(),
                    expires_at_unix_secs: 1,
                    refresh_token_expires_at_unix_secs: None,
                    scopes: vec!["messages".to_owned()],
                },
                record.id.as_bytes(),
            )
            .expect("tokens encrypt");
            self.storage
                .store_oauth_tokens(record.id, record.revision, encrypted)
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
                    stores: self.stores.clone(),
                    aead: self.aead.clone(),
                    oauth_cfg: self.oauth_cfg.clone(),
                    clock: self.clock.clone(),
                },
                Uuid::from_u128(0xfeed),
                None,
                CancellationToken::new(),
                claims,
                config,
                self.refresh_executor.clone(),
            )
        }

        fn refresh_call_count(&self) -> usize {
            self.refresh_executor.call_count()
        }

        fn hold_refresh_response(&self) {
            self.refresh_executor.hold_response();
        }

        async fn wait_for_refresh_start(&self) {
            self.refresh_executor.wait_for_start().await;
        }

        fn release_refresh_response(&self) {
            self.refresh_executor.release_response();
        }
    }

    struct ScriptedRefreshExecutor {
        storage: Arc<InMemoryStorage>,
        aead: Arc<AeadService>,
        calls: AtomicUsize,
        started: watch::Sender<bool>,
        hold_response: AtomicBool,
        release: Notify,
    }

    impl ScriptedRefreshExecutor {
        fn new(storage: Arc<InMemoryStorage>, aead: Arc<AeadService>) -> Self {
            let (started, _) = watch::channel(false);
            Self {
                storage,
                aead,
                calls: AtomicUsize::new(0),
                started,
                hold_response: AtomicBool::new(false),
                release: Notify::new(),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn hold_response(&self) {
            self.hold_response.store(true, Ordering::SeqCst);
        }

        async fn wait_for_start(&self) {
            let mut started = self.started.subscribe();
            if *started.borrow() {
                return;
            }
            started
                .changed()
                .await
                .expect("scripted refresh executor remains alive");
        }

        fn release_response(&self) {
            self.hold_response.store(false, Ordering::SeqCst);
            self.release.notify_one();
        }
    }

    #[async_trait::async_trait]
    impl LazyRefreshExecutor for ScriptedRefreshExecutor {
        async fn refresh(
            &self,
            _refresher: &LazyRefresher,
            upstream: UpstreamRecord,
        ) -> Result<u64, RefreshError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.send_replace(true);
            if self.hold_response.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
            let encrypted = EncryptedOAuthTokens::encrypt(
                self.aead.as_ref(),
                &OAuthTokenBundle {
                    access_token: "sk-ant-oat01-new".to_owned(),
                    refresh_token: "sk-ant-ort01-new".to_owned(),
                    expires_at_unix_secs: 1_700_003_600,
                    refresh_token_expires_at_unix_secs: None,
                    scopes: vec!["messages".to_owned()],
                },
                upstream.id.as_bytes(),
            )
            .map_err(|_| RefreshError::Encrypt)?;
            let updated = UpstreamStore::complete_refresh(
                self.storage.as_ref(),
                upstream.id,
                Uuid::from_u128(0xbeef),
                encrypted,
            )
            .await?;
            Ok(updated.oauth_token_generation)
        }
    }

    #[derive(Debug)]
    struct TestOAuthRefreshClaims {
        first_acquire_wins: bool,
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
    }

    struct RaceDoneClaims {
        storage: Arc<InMemoryStorage>,
        aead: Arc<AeadService>,
        upstream_id: Uuid,
        invoked: AtomicBool,
    }

    impl RaceDoneClaims {
        fn new(storage: Arc<InMemoryStorage>, aead: Arc<AeadService>, upstream_id: Uuid) -> Self {
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
                        access_token: "sk-ant-oat01-rotated".to_owned(),
                        refresh_token: "sk-ant-ort01-rotated".to_owned(),
                        expires_at_unix_secs: 9_999_999_999,
                        refresh_token_expires_at_unix_secs: None,
                        scopes: vec!["messages".to_owned()],
                    },
                    self.upstream_id.as_bytes(),
                )
                .expect("rotated tokens encrypt");
                UpstreamStore::complete_refresh(
                    self.storage.as_ref(),
                    self.upstream_id,
                    Uuid::from_u128(0xcafe),
                    rotated,
                )
                .await?;
            }
            Ok(LazyRefreshTaskState::Done)
        }
    }
}
