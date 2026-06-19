use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use apalis::prelude::Data;
use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_scheduler::error::{Result, SchedulerError};
use cc_lb_scheduler::jobs::metadata_refresh::MetadataRefreshJob;
use cc_lb_scheduler::jobs::oauth_refresh::{
    OAuthRefreshClaims, OAuthRefreshConfig, OAuthRefreshJobHandler, OAuthRefreshUpstreams,
};
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::{EntityJob, SchedulerBackend};
use cc_lb_storage_api::UpstreamRecord;
use serde::Deserialize;
use url::Url;
use uuid::Uuid;

use crate::common::now_secs;
use crate::fake::raw_http;

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<JobOutcome>> + Send>>;

#[derive(Clone)]
pub struct OAuthWorkerState<Claims, Upstreams> {
    handler: OAuthRefreshJobHandler<Claims, Upstreams>,
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    backend: SchedulerBackend,
    probe: OAuthWorkerProbe,
}

#[derive(Clone)]
pub struct OAuthWorkerProbe {
    inner: Arc<OAuthWorkerProbeInner>,
}

struct OAuthWorkerProbeInner {
    started: AtomicBool,
    notify: tokio::sync::Notify,
}

impl OAuthWorkerProbe {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(OAuthWorkerProbeInner {
                started: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            }),
        }
    }

    pub fn has_started(&self) -> bool {
        self.inner.started.load(Ordering::SeqCst)
    }

    fn mark_started(&self) {
        self.inner.started.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }
}

impl<Claims, Upstreams> OAuthWorkerState<Claims, Upstreams> {
    pub fn new(
        claims: Claims,
        upstreams: Upstreams,
        replica_id: Uuid,
        aead: Arc<AeadService>,
        oauth_cfg: Arc<AnthropicOAuthConfig>,
        backend: SchedulerBackend,
        probe: OAuthWorkerProbe,
    ) -> Self {
        let config = OAuthRefreshConfig {
            contention_wait: crate::common::WAIT_TIMEOUT,
            contention_poll_interval: crate::common::POLL_INTERVAL,
            retry_delay: crate::common::POLL_INTERVAL,
            ..OAuthRefreshConfig::default()
        };
        Self {
            handler: OAuthRefreshJobHandler::with_config(claims, upstreams, replica_id, config),
            aead,
            oauth_cfg,
            backend,
            probe,
        }
    }
}

pub fn entity_job_handler<Claims, Upstreams>(
    job: EntityJob,
    ctx: Data<OAuthWorkerState<Claims, Upstreams>>,
) -> HandlerFuture
where
    Claims: Clone + OAuthRefreshClaims + Send + Sync + 'static,
    Upstreams: Clone + OAuthRefreshUpstreams + Send + Sync + 'static,
{
    Box::pin(async move {
        let EntityJob::OAuthRefresh(job) = job else {
            return Ok(JobOutcome::Done);
        };
        ctx.probe.mark_started();
        let aead = ctx.aead.clone();
        let oauth_cfg = ctx.oauth_cfg.clone();
        let backend = ctx.backend.clone();
        ctx.handler
            .handle(
                job,
                now_secs(),
                move |upstream| refresh_tokens(aead, oauth_cfg, upstream),
                move |metadata| enqueue_metadata(backend, metadata),
            )
            .await
    })
}

async fn enqueue_metadata(backend: SchedulerBackend, job: MetadataRefreshJob) -> Result<()> {
    backend.push_job(EntityJob::MetadataRefresh(job)).await
}

async fn refresh_tokens(
    aead: Arc<AeadService>,
    oauth_cfg: Arc<AnthropicOAuthConfig>,
    upstream: UpstreamRecord,
) -> Result<EncryptedOAuthTokens> {
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
    let scopes = response
        .scope
        .as_deref()
        .map(|scope| scope.split_whitespace().map(ToOwned::to_owned).collect())
        .unwrap_or(previous.scopes);
    EncryptedOAuthTokens::encrypt(
        aead.as_ref(),
        &OAuthTokenBundle {
            access_token: response.access_token,
            refresh_token: response.refresh_token.unwrap_or(previous.refresh_token),
            expires_at_unix_secs: now_secs().saturating_add(response.expires_in),
            scopes,
        },
        upstream.id.as_bytes(),
    )
    .map_err(|error| SchedulerError::Job(error.to_string()))
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
    scope: Option<String>,
}

async fn request_refresh(
    token_url: &Url,
    client_id: &str,
    refresh_token: &str,
) -> Result<TokenResponse> {
    let body = {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer.append_pair("grant_type", "refresh_token");
        serializer.append_pair("client_id", client_id);
        serializer.append_pair("refresh_token", refresh_token);
        serializer.finish()
    };
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
    serde_json::from_slice(&response.body).map_err(|error| SchedulerError::Job(error.to_string()))
}
