use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use apalis::prelude::Data;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_oauth_protocol::{
    ExistingTokenParts, TokenEndpointResponse, parse_token_endpoint_response,
    refresh_token_form_body, refreshed_token_parts,
};
use cc_lb_scheduler::error::{Result, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshConfig, OAuthRefreshJobHandler, OAuthRefreshUpstreams, RefreshedOAuthTokens,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerBackend};
use cc_lb_storage_api::UpstreamRecord;
use url::Url;
use uuid::Uuid;

use super::common::now_secs;
use super::fake::raw_http;

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<JobOutcome>> + Send>>;

#[derive(Clone)]
pub struct OAuthWorkerState<Upstreams> {
    handler: OAuthRefreshJobHandler<Upstreams>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    backend: SchedulerBackend,
    probe: OAuthWorkerProbe,
}

const OAUTH_STARTED: u8 = 1 << 0;
const OAUTH_RUNNING: u8 = 1 << 1;
const OAUTH_FINISHED: u8 = 1 << 2;

#[derive(Clone)]
pub struct OAuthWorkerProbe {
    inner: Arc<OAuthWorkerProbeInner>,
}

struct OAuthWorkerProbeInner {
    state: AtomicU8,
    metadata_enqueued: AtomicUsize,
    notify: tokio::sync::Notify,
}

#[derive(Clone, Copy, Debug)]
pub struct OAuthWorkerProbeState {
    started: bool,
    running: bool,
    finished: bool,
    metadata_enqueued: usize,
}

impl OAuthWorkerProbeState {
    fn matches_running(self, running: bool) -> bool {
        if running {
            self.started && self.running && !self.finished
        } else {
            self.started && !self.running && self.finished
        }
    }
}

impl OAuthWorkerProbe {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(OAuthWorkerProbeInner {
                state: AtomicU8::new(0),
                metadata_enqueued: AtomicUsize::new(0),
                notify: tokio::sync::Notify::new(),
            }),
        }
    }

    pub fn state(&self) -> OAuthWorkerProbeState {
        let state = self.inner.state.load(Ordering::Acquire);
        OAuthWorkerProbeState {
            started: state & OAUTH_STARTED != 0,
            running: state & OAUTH_RUNNING != 0,
            finished: state & OAUTH_FINISHED != 0,
            metadata_enqueued: self.inner.metadata_enqueued.load(Ordering::Acquire),
        }
    }

    pub async fn wait_for_running(&self, running: bool, timeout: std::time::Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                if self.state().matches_running(running) {
                    return;
                }

                let notified = self.inner.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                if self.state().matches_running(running) {
                    return;
                }

                notified.await;
            }
        })
        .await
        .is_ok()
    }

    pub async fn wait_for_metadata_enqueued(
        &self,
        expected: usize,
        timeout: std::time::Duration,
    ) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                if self.state().metadata_enqueued >= expected {
                    return;
                }

                let notified = self.inner.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                if self.state().metadata_enqueued >= expected {
                    return;
                }

                notified.await;
            }
        })
        .await
        .is_ok()
    }

    fn mark_started(&self) {
        self.inner
            .state
            .store(OAUTH_STARTED | OAUTH_RUNNING, Ordering::Release);
        self.inner.notify.notify_waiters();
    }

    fn mark_finished(&self) {
        self.inner
            .state
            .store(OAUTH_STARTED | OAUTH_FINISHED, Ordering::Release);
        self.inner.notify.notify_waiters();
    }

    fn mark_metadata_enqueued(&self) {
        self.inner.metadata_enqueued.fetch_add(1, Ordering::Release);
        self.inner.notify.notify_waiters();
    }
}

impl<Upstreams> OAuthWorkerState<Upstreams> {
    pub fn new(
        upstreams: Upstreams,
        replica_id: Uuid,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        backend: SchedulerBackend,
        probe: OAuthWorkerProbe,
    ) -> Self {
        let config = OAuthRefreshConfig {
            retry_delay: super::common::POLL_INTERVAL,
        };
        Self {
            handler: OAuthRefreshJobHandler::with_config(upstreams, replica_id, config),
            aead,
            oauth_cfg,
            backend,
            probe,
        }
    }
}

pub fn entity_job_handler<Upstreams>(
    job: AdaptiveJob,
    ctx: Data<OAuthWorkerState<Upstreams>>,
) -> HandlerFuture
where
    Upstreams: Clone + OAuthRefreshUpstreams + Send + Sync + 'static,
{
    Box::pin(async move {
        let AdaptiveJob::OAuthRefresh(job) = job else {
            return Ok(JobOutcome::Done);
        };
        ctx.probe.mark_started();
        let aead = ctx.aead.clone();
        let oauth_cfg = ctx.oauth_cfg.clone();
        let backend = ctx.backend.clone();
        let enqueue_probe = ctx.probe.clone();
        let result = ctx
            .handler
            .handle(
                job,
                now_secs(),
                move |upstream| refresh_tokens(aead, oauth_cfg, upstream),
                move |metadata| enqueue_metadata(backend, metadata, enqueue_probe),
                |_upstream_id, _expires_at_unix_secs| async { Ok(()) },
            )
            .await;
        ctx.probe.mark_finished();
        result
    })
}

async fn enqueue_metadata(
    backend: SchedulerBackend,
    job: MetadataRefreshJob,
    probe: OAuthWorkerProbe,
) -> Result<()> {
    backend.push_job(AdaptiveJob::MetadataRefresh(job)).await?;
    probe.mark_metadata_enqueued();
    Ok(())
}

async fn refresh_tokens(
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    upstream: UpstreamRecord,
) -> Result<RefreshedOAuthTokens> {
    let previous = upstream
        .oauth_credentials
        .as_ref()
        .ok_or_else(|| SchedulerError::Job("missing oauth credentials".to_owned()))?
        .decrypt(aead.as_ref(), upstream.id.as_bytes())
        .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let response = request_refresh(
        &oauth_cfg.token_url,
        oauth_cfg.client_id.as_str(),
        previous.refresh_token.as_str(),
    )
    .await?;
    let refreshed = refreshed_token_parts(
        ExistingTokenParts {
            refresh_token: previous.refresh_token,
            refresh_token_expires_at_unix_secs: None,
            scopes: previous.scopes,
        },
        response,
        now_secs(),
    );
    let expires_at_unix_secs = refreshed.expires_at_unix_secs;
    let encrypted_tokens = EncryptedOAuthTokens::encrypt(
        aead.as_ref(),
        &OAuthTokenBundle {
            access_token: refreshed.access_token,
            refresh_token: refreshed.refresh_token,
            expires_at_unix_secs,
            refresh_token_expires_at_unix_secs: None,
            scopes: refreshed.scopes,
        },
        upstream.id.as_bytes(),
    )
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    Ok(RefreshedOAuthTokens {
        encrypted_tokens,
        expires_at_unix_secs,
    })
}

async fn request_refresh(
    token_url: &Url,
    client_id: &str,
    refresh_token: &str,
) -> Result<TokenEndpointResponse> {
    let body = refresh_token_form_body(client_id, refresh_token);
    let response = raw_http(
        "POST",
        token_url.as_str(),
        &[("content-type", "application/x-www-form-urlencoded")],
        body.as_bytes(),
    )
    .await
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    if !response.status.is_success() {
        return Err(SchedulerError::Job(format!(
            "token endpoint returned {}",
            response.status
        )));
    }
    parse_token_endpoint_response(&response.body)
        .map_err(|error| SchedulerError::Job(error.to_string()))
}
