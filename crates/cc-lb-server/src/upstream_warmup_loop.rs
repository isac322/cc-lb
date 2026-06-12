use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_core::{
    SubscriptionQuotaSink, UnifiedQuotaObservation, parse_anthropic_unified_headers,
    unified_observation_to_record,
};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    StorageError, StorageResult, SubscriptionQuotaLatestRecord, SubscriptionQuotaWindow,
    UpstreamRecord,
};
use chrono::{DateTime, TimeZone, Utc};
use http::StatusCode;
use http_body_util::Full;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;
use crate::refresh::LazyRefresher;
use crate::warmup::{
    BackoffSchedule, WarmupAbandonReason, WarmupResult, classify_response,
    cycle_key_from_observation, dispatch_warmup, stable_jitter_ms,
};

pub const WARMUP_TICK_SECS: u64 = 30;
pub const WARMUP_POST_RESET_GUARD_SECS: u64 = 30;
pub const WARMUP_LEASE_TTL_SECS: i64 = 120;
pub const WARMUP_REQUEST_TIMEOUT_SECS: u64 = 30;

const _: () = assert!(WARMUP_LEASE_TTL_SECS as u64 > WARMUP_REQUEST_TIMEOUT_SECS + 30 + 10);
const PAGE_SIZE: usize = 100;
const TOKEN_REFRESH_LOOKAHEAD_SECS: u64 = 60;
const FIVE_HOURS_SECS: i64 = 5 * 60 * 60;
const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";

type WarmupHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

pub struct UpstreamWarmupLoop {
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Arc<LazyRefresher>,
    pub subscription_quota_sink: SubscriptionQuotaSink,
    pub replica_id: String,
    pub runtime: Option<Arc<ExtismRuntime>>,
    pub data_dir: PathBuf,
    pub client: WarmupHttpClient,
    pub backoffs: Mutex<HashMap<Uuid, BackoffSchedule>>,
}

impl UpstreamWarmupLoop {
    pub fn new(
        stores: Arc<Stores>,
        aead: Arc<AeadService>,
        lazy_refresher: Arc<LazyRefresher>,
        subscription_quota_sink: SubscriptionQuotaSink,
        replica_id: Uuid,
        runtime: Option<Arc<ExtismRuntime>>,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            stores,
            aead,
            lazy_refresher,
            subscription_quota_sink,
            replica_id: replica_id.to_string(),
            runtime,
            data_dir,
            client: warmup_http_client(),
            backoffs: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(WARMUP_TICK_SECS));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }
            let _ = self.scan_and_fire_once(&cancel).await;
        }
    }

    pub async fn scan_and_fire_once(&self, cancel: &CancellationToken) -> StorageResult<()> {
        let candidates = self.list_due_candidates(cancel).await?;
        for upstream in candidates {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let _ = self.fire_candidate(upstream, cancel).await;
        }
        Ok(())
    }

    async fn list_due_candidates(
        &self,
        cancel: &CancellationToken,
    ) -> StorageResult<Vec<UpstreamRecord>> {
        let mut after = None;
        let mut candidates = Vec::new();
        loop {
            let page = tokio::select! {
                _ = cancel.cancelled() => return Ok(candidates),
                page = self.stores.upstreams.list(after, PAGE_SIZE) => page?,
            };
            if page.is_empty() {
                return Ok(candidates);
            }
            after = page.last().map(|record| record.id);
            let now = self.db_now_datetime().await?;
            candidates.extend(
                page.into_iter()
                    .filter(|record| warmup_due_candidate(record, now)),
            );
        }
    }

    async fn fire_candidate(
        &self,
        upstream: UpstreamRecord,
        cancel: &CancellationToken,
    ) -> Result<(), WarmupLoopError> {
        let claimed = self
            .stores
            .upstreams
            .claim_warmup_lease(upstream.id, &self.replica_id, WARMUP_LEASE_TTL_SECS)
            .await?;
        if !claimed {
            return Ok(());
        }
        let now = self.db_now_unix_secs().await?;
        let lease_until = unix_secs_datetime(now.saturating_add(WARMUP_LEASE_TTL_SECS))?;
        tracing::info!(target: "warmup", upstream_id = %upstream.id, holder = %self.replica_id, lease_until = ?lease_until, action = "lease_claimed");

        let Some(latest) = self.latest_five_hour(upstream.id).await? else {
            return Ok(());
        };
        let Some(cycle_key) = cycle_key_from_observation(&latest) else {
            return Ok(());
        };
        if upstream.last_warmup_cycle_key == Some(cycle_key) {
            return Ok(());
        }

        let now = self.db_now_unix_secs().await?;
        if cycle_key > now {
            let next_warmup_at = schedule_for_cycle(upstream.id, cycle_key)?;
            self.write_warmup_next_at(upstream.id, next_warmup_at)
                .await?;
            return Ok(());
        }

        self.sleep_jitter(upstream.id, cycle_key, cancel).await?;
        let result = self
            .fire_with_current_credentials(&upstream, cycle_key, cancel)
            .await?;
        self.handle_warmup_result(upstream, cycle_key, result, cancel)
            .await
    }

    async fn fire_with_current_credentials(
        &self,
        upstream: &UpstreamRecord,
        cycle_key: i64,
        cancel: &CancellationToken,
    ) -> Result<WarmupDispatchResult, WarmupLoopError> {
        let access_token = self.fresh_access_token(upstream).await?;
        self.dispatch_and_classify(upstream, &access_token, cycle_key, cancel)
            .await
    }

    async fn handle_warmup_result(
        &self,
        upstream: UpstreamRecord,
        candidate_cycle_key: i64,
        result: WarmupDispatchResult,
        cancel: &CancellationToken,
    ) -> Result<(), WarmupLoopError> {
        match result.outcome {
            WarmupResult::Success { cycle_key }
            | WarmupResult::WindowAlreadyActive { cycle_key } => {
                self.write_cycle_success(upstream.id, cycle_key).await?;
            }
            WarmupResult::RetryableTransient => {
                self.schedule_backoff(upstream.id).await?;
                self.release_warmup_lease(upstream.id).await;
            }
            WarmupResult::AbandonCyclePermanent(WarmupAbandonReason::AuthFailed) => {
                self.retry_after_refresh(upstream, candidate_cycle_key, cancel)
                    .await?;
            }
            WarmupResult::AbandonCyclePermanent(reason) => {
                tracing::warn!(target: "warmup", upstream_id = %upstream.id, reason = reason.as_str(), action = "cycle_abandoned");
                self.schedule_next_cycle(upstream.id, candidate_cycle_key)
                    .await?;
                self.release_warmup_lease(upstream.id).await;
            }
        }
        Ok(())
    }

    async fn retry_after_refresh(
        &self,
        upstream: UpstreamRecord,
        candidate_cycle_key: i64,
        cancel: &CancellationToken,
    ) -> Result<(), WarmupLoopError> {
        let access_token = match self.refresh_access_token(upstream.id).await {
            Ok(access_token) => access_token,
            Err(_error) => {
                tracing::warn!(target: "warmup", upstream_id = %upstream.id, reason = "auth_failed", action = "cycle_abandoned");
                self.schedule_next_cycle(upstream.id, candidate_cycle_key)
                    .await?;
                self.release_warmup_lease(upstream.id).await;
                return Ok(());
            }
        };
        let refreshed = self
            .stores
            .upstreams
            .get_by_id(upstream.id)
            .await?
            .ok_or(WarmupLoopError::MissingUpstream)?;
        let result = self
            .dispatch_and_classify(&refreshed, &access_token, candidate_cycle_key, cancel)
            .await?;
        match result.outcome {
            WarmupResult::Success { cycle_key }
            | WarmupResult::WindowAlreadyActive { cycle_key } => {
                self.write_cycle_success(upstream.id, cycle_key).await?;
            }
            WarmupResult::RetryableTransient => {
                self.schedule_backoff(upstream.id).await?;
                self.release_warmup_lease(upstream.id).await;
            }
            WarmupResult::AbandonCyclePermanent(reason) => {
                tracing::warn!(target: "warmup", upstream_id = %upstream.id, reason = reason.as_str(), action = "cycle_abandoned");
                self.schedule_next_cycle(upstream.id, candidate_cycle_key)
                    .await?;
                self.release_warmup_lease(upstream.id).await;
            }
        }
        Ok(())
    }

    async fn dispatch_and_classify(
        &self,
        upstream: &UpstreamRecord,
        access_token: &str,
        candidate_cycle_key: i64,
        cancel: &CancellationToken,
    ) -> Result<WarmupDispatchResult, WarmupLoopError> {
        let jitter_ms =
            stable_jitter_ms(upstream.id, u64::try_from(candidate_cycle_key).unwrap_or(0));
        let dialect_runtime = upstream
            .warmup_dialect_plugin
            .as_ref()
            .and(self.runtime.as_ref());
        let dialect_plugin_used = dialect_runtime.is_some();
        tracing::info!(target: "warmup", upstream_id = %upstream.id, holder = %self.replica_id, cycle_key = %candidate_cycle_key, jitter_ms = %jitter_ms, dialect_plugin_used = dialect_plugin_used, action = "dispatch_start");
        let timeout = Duration::from_secs(WARMUP_REQUEST_TIMEOUT_SECS);

        let mut dispatch_outcome: Option<(StatusCode, Vec<UnifiedQuotaObservation>)> = None;
        if let Some(runtime) = dialect_runtime {
            let dialect_result = tokio::select! {
                _ = cancel.cancelled() => return Err(WarmupLoopError::Cancelled),
                result = tokio::time::timeout(timeout, crate::warmup::dialect::dispatch_warmup_with_dialect(
                    runtime,
                    &self.stores,
                    &self.data_dir,
                    self.aead.clone(),
                    self.lazy_refresher.clone(),
                    upstream,
                    &self.client,
                )) => result,
            };
            match dialect_result {
                Ok(Ok(outcome)) => {
                    let observations = parse_anthropic_unified_headers(&outcome.headers);
                    dispatch_outcome = Some((outcome.status, observations));
                }
                Ok(Err(crate::warmup::dialect::WarmupDispatchError::MissingPlugin)) => {
                    // Fall through to the standard OAuth dispatch path below.
                }
                Ok(Err(_)) | Err(_) => {
                    // Treat dialect plugin failures and timeouts as transient
                    // per D1; the next tick will retry under backoff. No
                    // cycle_abandoned event is emitted for transient failures.
                    dispatch_outcome = Some((StatusCode::BAD_GATEWAY, Vec::new()));
                }
            }
        }

        let (status, observations) = if let Some(outcome) = dispatch_outcome {
            outcome
        } else {
            let base_url = upstream_base_url(upstream)?;
            tokio::select! {
                _ = cancel.cancelled() => return Err(WarmupLoopError::Cancelled),
                result = tokio::time::timeout(timeout, dispatch_warmup(&self.client, access_token, &base_url, &self.replica_id)) => {
                    result.unwrap_or((StatusCode::BAD_GATEWAY, Vec::new()))
                }
            }
        };
        self.enqueue_observations(upstream.id, &observations)
            .await?;
        let outcome = classify_response(status, &observations, candidate_cycle_key);
        tracing::info!(target: "warmup", upstream_id = %upstream.id, holder = %self.replica_id, status = %status, outcome = ?outcome, action = "dispatch_result");
        Ok(WarmupDispatchResult { outcome })
    }

    async fn latest_five_hour(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<SubscriptionQuotaLatestRecord>> {
        let latest = self
            .stores
            .upstream_subscription_quotas
            .list_latest_subscription_quota_for_upstreams(&[upstream_id])
            .await?;
        Ok(latest
            .into_iter()
            .filter(|record| record.window == SubscriptionQuotaWindow::FiveHour)
            .max_by_key(|record| record.observed_at_unix_millis))
    }

    async fn fresh_access_token(
        &self,
        upstream: &UpstreamRecord,
    ) -> Result<String, WarmupLoopError> {
        let bundle = decrypt_bundle(upstream, &self.aead)?;
        let now = self.db_now_unix_secs().await?;
        let now = u64::try_from(now).unwrap_or_default();
        if bundle.expires_at_unix_secs > now.saturating_add(TOKEN_REFRESH_LOOKAHEAD_SECS) {
            return Ok(bundle.access_token);
        }
        self.refresh_access_token(upstream.id).await
    }

    async fn refresh_access_token(&self, upstream_id: Uuid) -> Result<String, WarmupLoopError> {
        self.lazy_refresher
            .refresh_one(upstream_id)
            .await
            .map_err(|error| WarmupLoopError::Refresh(error.to_string()))?;
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream_id)
            .await?
            .ok_or(WarmupLoopError::MissingUpstream)?;
        Ok(decrypt_bundle(&upstream, &self.aead)?.access_token)
    }

    async fn enqueue_observations(
        &self,
        upstream_id: Uuid,
        observations: &[UnifiedQuotaObservation],
    ) -> Result<(), WarmupLoopError> {
        let observed_at_unix_millis = self.db_now_unix_millis().await?;
        for observation in observations.iter().cloned() {
            let record =
                unified_observation_to_record(upstream_id, observation, observed_at_unix_millis);
            let _ = self.subscription_quota_sink.enqueue(record);
        }
        Ok(())
    }

    async fn write_cycle_success(
        &self,
        upstream_id: Uuid,
        cycle_key: i64,
    ) -> Result<(), WarmupLoopError> {
        let next_warmup_at =
            schedule_for_cycle(upstream_id, cycle_key.saturating_add(FIVE_HOURS_SECS))?;
        let written = self
            .stores
            .upstreams
            .write_warmup_cycle_key(
                upstream_id,
                &self.replica_id,
                cycle_key,
                Some(next_warmup_at),
            )
            .await?;
        if written {
            self.reset_backoff(upstream_id);
            tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %self.replica_id, cycle_key = %cycle_key, next_warmup_at = ?next_warmup_at, action = "cycle_key_written");
        }
        Ok(())
    }

    async fn schedule_backoff(&self, upstream_id: Uuid) -> Result<(), WarmupLoopError> {
        let delay = {
            let mut backoffs = self.backoffs.lock();
            backoffs
                .entry(upstream_id)
                .or_default()
                .next()
                .unwrap_or(Duration::from_secs(600))
        };
        let now = self.db_now_unix_secs().await?;
        let delay_secs = i64::try_from(delay.as_secs()).unwrap_or(i64::MAX);
        let next_warmup_at = unix_secs_datetime(now.saturating_add(delay_secs))?;
        self.write_warmup_next_at(upstream_id, next_warmup_at)
            .await?;
        Ok(())
    }

    async fn schedule_next_cycle(
        &self,
        upstream_id: Uuid,
        cycle_key: i64,
    ) -> Result<(), WarmupLoopError> {
        let next_warmup_at =
            schedule_for_cycle(upstream_id, cycle_key.saturating_add(FIVE_HOURS_SECS))?;
        self.write_warmup_next_at(upstream_id, next_warmup_at)
            .await?;
        Ok(())
    }

    async fn write_warmup_next_at(
        &self,
        upstream_id: Uuid,
        next_warmup_at: DateTime<Utc>,
    ) -> StorageResult<bool> {
        self.stores
            .upstreams
            .write_warmup_next_at(upstream_id, &self.replica_id, next_warmup_at)
            .await
    }

    async fn release_warmup_lease(&self, upstream_id: Uuid) {
        let holder = &self.replica_id;
        let _ = self
            .stores
            .upstreams
            .release_warmup_lease(upstream_id, holder)
            .await;
    }

    async fn sleep_jitter(
        &self,
        upstream_id: Uuid,
        cycle_key: i64,
        cancel: &CancellationToken,
    ) -> Result<(), WarmupLoopError> {
        let cycle_key = u64::try_from(cycle_key).map_err(|_| WarmupLoopError::InvalidCycleKey)?;
        let sleep = tokio::time::sleep(Duration::from_millis(stable_jitter_ms(
            upstream_id,
            cycle_key,
        )));
        tokio::pin!(sleep);
        tokio::select! {
            _ = cancel.cancelled() => Err(WarmupLoopError::Cancelled),
            _ = &mut sleep => Ok(()),
        }
    }

    async fn db_now_datetime(&self) -> StorageResult<DateTime<Utc>> {
        unix_secs_datetime(self.db_now_unix_secs().await?).map_err(|error| StorageError::Fatal {
            message: error.to_string(),
        })
    }

    async fn db_now_unix_secs(&self) -> StorageResult<i64> {
        self.stores.upstreams.warmup_now_unix_secs().await
    }

    async fn db_now_unix_millis(&self) -> StorageResult<u64> {
        let now = self.db_now_unix_secs().await?;
        let millis = now.checked_mul(1_000).ok_or_else(|| StorageError::Fatal {
            message: "db now millis overflow".to_owned(),
        })?;
        u64::try_from(millis).map_err(|_| StorageError::Fatal {
            message: "negative db now millis".to_owned(),
        })
    }

    fn reset_backoff(&self, upstream_id: Uuid) {
        if let Some(backoff) = self.backoffs.lock().get_mut(&upstream_id) {
            backoff.reset();
        }
    }
}

pub fn spawn_upstream_warmup_loop(
    warmup_loop: UpstreamWarmupLoop,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(Arc::new(warmup_loop).run(cancel))
}

#[derive(Debug)]
struct WarmupDispatchResult {
    outcome: WarmupResult,
}

#[derive(Debug, thiserror::Error)]
enum WarmupLoopError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("missing oauth credentials")]
    MissingCredentials,
    #[error("oauth upstream not found after refresh")]
    MissingUpstream,
    #[error("oauth decrypt failed")]
    Decrypt,
    #[error("oauth refresh failed: {0}")]
    Refresh(String),
    #[error("warmup request cancelled")]
    Cancelled,
    #[error("invalid warmup cycle key")]
    InvalidCycleKey,
    #[error("invalid warmup timestamp")]
    InvalidTimestamp,
    #[error("invalid anthropic base URL: {0}")]
    InvalidBaseUrl(String),
}

fn warmup_due_candidate(record: &UpstreamRecord, now: DateTime<Utc>) -> bool {
    // warmup costs real Anthropic budget per fire; usage polling is read-only - so warmup respects operator disable, polling does not.
    record.enabled
        && record.warmup_enabled
        && record.kind == UpstreamKind::AnthropicOauth
        && record.deleted_at_unix_secs.is_none()
        && record
            .next_warmup_at
            .as_ref()
            .is_some_and(|next| *next <= now)
}

fn decrypt_bundle(
    upstream: &UpstreamRecord,
    aead: &AeadService,
) -> Result<OAuthTokenBundle, WarmupLoopError> {
    upstream
        .oauth_credentials
        .as_ref()
        .ok_or(WarmupLoopError::MissingCredentials)?
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| WarmupLoopError::Decrypt)
}

fn schedule_for_cycle(
    upstream_id: Uuid,
    resets_at_unix_secs: i64,
) -> Result<DateTime<Utc>, WarmupLoopError> {
    let resets_at =
        u64::try_from(resets_at_unix_secs).map_err(|_| WarmupLoopError::InvalidCycleKey)?;
    let jitter_ms = stable_jitter_ms(upstream_id, resets_at);
    let base = unix_secs_datetime(resets_at_unix_secs)?;
    base.checked_add_signed(chrono::Duration::seconds(
        WARMUP_POST_RESET_GUARD_SECS as i64,
    ))
    .and_then(|value| value.checked_add_signed(chrono::Duration::milliseconds(jitter_ms as i64)))
    .ok_or(WarmupLoopError::InvalidTimestamp)
}

fn unix_secs_datetime(unix_secs: i64) -> Result<DateTime<Utc>, WarmupLoopError> {
    Utc.timestamp_opt(unix_secs, 0)
        .single()
        .ok_or(WarmupLoopError::InvalidTimestamp)
}

fn upstream_base_url(upstream: &UpstreamRecord) -> Result<Url, WarmupLoopError> {
    match upstream.base_url.clone() {
        Some(base_url) => Ok(base_url),
        None => Url::parse(DEFAULT_ANTHROPIC_BASE_URL)
            .map_err(|error| WarmupLoopError::InvalidBaseUrl(error.to_string())),
    }
}

fn warmup_http_client() -> WarmupHttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}
