use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_config::OAuthUsagePollerConfig;
use cc_lb_core::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_core::{
    EstimatorConfig, PollScheduleEstimator, ReplicaIdentity, SubscriptionQuotaSink,
    ThrottleObservation, percent_to_utilization_fraction,
};
use cc_lb_signer_anthropic_oauth::LazyRefreshHandle;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    StorageError, StorageResult, SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaWindow, UpstreamRecord,
};
use http::{HeaderMap, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::dynamic_view_builder::Stores;
use crate::refresh::LazyRefresher;
use crate::subscription_quota_cache::SubscriptionQuotaCache;

const USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
const PAGE_SIZE: usize = 100;
const TOKEN_REFRESH_LOOKAHEAD_SECS: u64 = 60;
const SWEEP_TICK_SECS: u64 = 15;

type UsageHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

pub struct OAuthUsagePoller {
    pub replica_id: ReplicaIdentity,
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Arc<LazyRefresher>,
    pub estimator: PollScheduleEstimator,
    pub subscription_quota_sink: SubscriptionQuotaSink,
    pub subscription_quota_cache: Arc<SubscriptionQuotaCache>,
    pub config: OAuthUsagePollerConfig,
    pub client: UsageHttpClient,
}

impl OAuthUsagePoller {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        replica_id: ReplicaIdentity,
        stores: Arc<Stores>,
        aead: Arc<AeadService>,
        lazy_refresher: Arc<LazyRefresher>,
        subscription_quota_sink: SubscriptionQuotaSink,
        subscription_quota_cache: Arc<SubscriptionQuotaCache>,
        config: OAuthUsagePollerConfig,
    ) -> Self {
        let estimator = PollScheduleEstimator::new(EstimatorConfig {
            bootstrap_attempts: config.bootstrap_attempts,
            bootstrap_default_interval_secs: config.bootstrap_default_interval_secs,
            safety_factor: 1.0 / f64::from(config.safety_divisor.max(1)),
            min_interval_secs: config.min_interval_secs,
            max_interval_secs: config.max_interval_secs,
            fallback_interval_secs: config.fallback_interval_secs,
            history_capacity: config.history_capacity as usize,
            throttle_ladder_secs: config.throttle_ladder_secs.clone(),
            rate_limit_window_secs: config.rate_limit_window_secs,
            rate_limit_capacity: config.rate_limit_capacity as usize,
            rate_limit_safety_secs: config.rate_limit_safety_secs,
        });
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self {
            replica_id,
            stores,
            aead,
            lazy_refresher,
            estimator,
            subscription_quota_sink,
            subscription_quota_cache,
            config,
            client,
        }
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(SWEEP_TICK_SECS));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }
            if let Err(error) = self.sweep_once(&cancel).await {
                tracing::warn!(error = %error, "oauth usage poll sweep failed");
            }
        }
    }

    pub async fn sweep_once(&self, cancel: &CancellationToken) -> StorageResult<()> {
        let mut after = None;
        loop {
            let page = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                page = self.stores.upstreams.list(after, PAGE_SIZE) => page?,
            };
            if page.is_empty() {
                return Ok(());
            }
            after = page.last().map(|record| record.id);
            for upstream in page.into_iter().filter(is_usage_poll_candidate) {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                let now = SystemTime::now();
                if !oauth_usage_poll_is_due(&self.estimator, upstream.id, now) {
                    continue;
                }
                let claimed = self
                    .stores
                    .upstreams
                    .claim_refresh_lease(
                        upstream.id,
                        self.replica_id.id,
                        self.config.lease_ttl_secs,
                    )
                    .await?;
                if !claimed {
                    continue;
                }
                let stagger = tokio::time::sleep(Duration::from_millis(self.config.stagger_ms));
                tokio::pin!(stagger);
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = &mut stagger => {}
                }
                let upstream_id = upstream.id;
                if let Err(error) = self.poll_one(upstream, cancel).await {
                    let failure_observed_at = SystemTime::now();
                    match &error {
                        UsagePollError::Status(status) => {
                            self.estimator.record_status_failure(
                                upstream_id,
                                failure_observed_at,
                                status.as_u16(),
                            );
                        }
                        _ => self
                            .estimator
                            .record_network_failure(upstream_id, failure_observed_at),
                    }
                    tracing::warn!(upstream_id = %upstream_id, error = %error, "oauth usage poll failed");
                }
            }
        }
    }

    async fn poll_one(
        &self,
        upstream: UpstreamRecord,
        cancel: &CancellationToken,
    ) -> Result<(), UsagePollError> {
        let access_token = self.fresh_access_token(&upstream).await?;
        let user_agent = self.user_agent().await?;
        let observed_at = SystemTime::now();
        let response = self.fetch_usage(&access_token, &user_agent, cancel).await?;
        if response.status == StatusCode::TOO_MANY_REQUESTS {
            let observation = throttle_observation(&response.headers, observed_at, &self.config);
            self.estimator.record_throttle(upstream.id, observation);
            tracing::debug!(upstream_id = %upstream.id, status = %response.status, "oauth usage poll throttled");
            return Ok(());
        }
        if !response.status.is_success() {
            return Err(UsagePollError::Status(response.status));
        }

        let usage: UsageResponse = serde_json::from_slice(&response.body)
            .map_err(|error| UsagePollError::Parse(error.to_string()))?;
        self.estimator.record_success(upstream.id, observed_at);
        let observed_at_unix_millis = system_time_to_unix_millis(observed_at);
        for record in usage_to_records(upstream.id, usage, observed_at_unix_millis) {
            self.subscription_quota_cache
                .upsert_observation(upstream.id, &record);
            let _ = self.subscription_quota_sink.enqueue(record);
        }
        Ok(())
    }

    async fn fresh_access_token(
        &self,
        upstream: &UpstreamRecord,
    ) -> Result<String, UsagePollError> {
        let bundle = decrypt_bundle(upstream, &self.aead)?;
        let now = now_unix_secs();
        if bundle.expires_at_unix_secs > now.saturating_add(TOKEN_REFRESH_LOOKAHEAD_SECS) {
            return Ok(bundle.access_token);
        }
        self.lazy_refresher
            .refresh_one(upstream.id)
            .await
            .map_err(|error| UsagePollError::Refresh(error.to_string()))?;
        let upstream = self
            .stores
            .upstreams
            .get_by_id(upstream.id)
            .await?
            .ok_or(UsagePollError::MissingUpstream)?;
        Ok(decrypt_bundle(&upstream, &self.aead)?.access_token)
    }

    async fn user_agent(&self) -> Result<String, UsagePollError> {
        if let Some(override_value) = self.config.user_agent_override.as_ref() {
            return Ok(override_value.clone());
        }
        let version = self
            .stores
            .anthropic_compatibility_kv
            .get_compatibility_kv(CLAUDE_CODE_STABLE_VERSION_KEY)
            .await?
            .map(|record| record.value)
            .unwrap_or_else(|| CLAUDE_CODE_STABLE_VERSION_FALLBACK.to_owned());
        Ok(claude_code_user_agent(&version))
    }

    async fn fetch_usage(
        &self,
        access_token: &str,
        user_agent: &str,
        cancel: &CancellationToken,
    ) -> Result<UsageFetchResponse, UsagePollError> {
        let request = Request::get(USAGE_URL)
            .header("Authorization", format!("Bearer {access_token}"))
            .header("User-Agent", user_agent)
            .header("anthropic-beta", "oauth-2025-04-20")
            .header("Accept", "application/json")
            .body(Full::new(Bytes::new()))
            .map_err(|error| UsagePollError::Http(error.to_string()))?;
        let timeout = Duration::from_secs(self.config.request_timeout_secs.max(1));
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(UsagePollError::Cancelled),
            response = tokio::time::timeout(timeout, self.client.request(request)) => {
                response
                    .map_err(|_| UsagePollError::Timeout)?
                    .map_err(|error| UsagePollError::Http(error.to_string()))?
            }
        };
        let status = response.status();
        let headers = response.headers().clone();
        let body = tokio::select! {
            _ = cancel.cancelled() => return Err(UsagePollError::Cancelled),
            body = tokio::time::timeout(timeout, response.into_body().collect()) => {
                body
                    .map_err(|_| UsagePollError::Timeout)?
                    .map_err(|error| UsagePollError::Http(error.to_string()))?
                    .to_bytes()
            }
        };
        Ok(UsageFetchResponse {
            status,
            headers,
            body,
        })
    }
}

pub fn spawn_oauth_usage_poller(
    poller: OAuthUsagePoller,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(Arc::new(poller).run(cancel))
}

pub fn is_usage_poll_candidate(record: &UpstreamRecord) -> bool {
    // `enabled` is intentionally not checked: it gates routing, not quota polling.
    record.kind == UpstreamKind::AnthropicOauth
        && record.deleted_at_unix_secs.is_none()
        && record.oauth_credentials.is_some()
}

fn decrypt_bundle(
    upstream: &UpstreamRecord,
    aead: &AeadService,
) -> Result<OAuthTokenBundle, UsagePollError> {
    upstream
        .oauth_credentials
        .as_ref()
        .ok_or(UsagePollError::MissingCredentials)?
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| UsagePollError::Decrypt)
}

#[derive(Debug, thiserror::Error)]
enum UsagePollError {
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
    #[error("oauth usage endpoint returned {0}")]
    Status(StatusCode),
    #[error("oauth usage request failed: {0}")]
    Http(String),
    #[error("oauth usage request timed out")]
    Timeout,
    #[error("oauth usage response parse failed: {0}")]
    Parse(String),
    #[error("oauth usage poll cancelled")]
    Cancelled,
}

struct UsageFetchResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Debug, Deserialize)]
struct UsageResponse {
    #[serde(default, alias = "5h")]
    five_hour: Option<WindowUsage>,
    #[serde(default, alias = "7d")]
    seven_day: Option<WindowUsage>,
    #[serde(default, alias = "7d_sonnet")]
    seven_day_sonnet: Option<WindowUsage>,
    #[serde(default, alias = "7d_opus")]
    seven_day_opus: Option<WindowUsage>,
    #[serde(default)]
    extra_usage: Option<ExtraUsage>,
}

#[derive(Debug, Deserialize)]
struct WindowUsage {
    utilization: Option<f64>,
    resets_at: Option<ResetsAt>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ResetsAt {
    Number(f64),
    String(String),
}

#[derive(Debug, Deserialize)]
struct ExtraUsage {
    enabled: Option<bool>,
    monthly_limit: Option<f64>,
    used_credits: Option<f64>,
}

fn usage_to_records(
    upstream_id: Uuid,
    usage: UsageResponse,
    observed_at_unix_millis: u64,
) -> Vec<SubscriptionQuotaObservationRecord> {
    let mut records = Vec::new();
    for (window, usage) in [
        (SubscriptionQuotaWindow::FiveHour, usage.five_hour),
        (SubscriptionQuotaWindow::SevenDay, usage.seven_day),
        (
            SubscriptionQuotaWindow::SevenDaySonnet,
            usage.seven_day_sonnet,
        ),
        (SubscriptionQuotaWindow::SevenDayOpus, usage.seven_day_opus),
    ] {
        if let Some(usage) = usage {
            records.push(sample_record(
                upstream_id,
                window,
                observed_at_unix_millis,
                usage.utilization.map(percent_to_utilization_fraction),
                usage.resets_at.and_then(resets_at_unix_secs),
                None,
            ));
        }
    }
    if let Some(extra_usage) = usage.extra_usage {
        records.push(sample_record(
            upstream_id,
            SubscriptionQuotaWindow::Overage,
            observed_at_unix_millis,
            None,
            None,
            Some(extra_usage),
        ));
    }
    records
}

fn sample_record(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    observed_at_unix_millis: u64,
    utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
    extra_usage: Option<ExtraUsage>,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Api,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization,
        status: None,
        resets_at_unix_secs,
        surpassed_threshold: None,
        representative_claim: None,
        disabled_reason: None,
        extra_usage_enabled: extra_usage.as_ref().and_then(|extra| extra.enabled),
        extra_usage_monthly_limit: extra_usage.as_ref().and_then(|extra| extra.monthly_limit),
        extra_usage_used_credits: extra_usage.as_ref().and_then(|extra| extra.used_credits),
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

fn oauth_usage_poll_is_due(
    estimator: &PollScheduleEstimator,
    upstream_id: Uuid,
    now: SystemTime,
) -> bool {
    estimator.is_due(upstream_id, now)
}

fn resets_at_unix_secs(value: ResetsAt) -> Option<u64> {
    match value {
        ResetsAt::Number(value) if value.is_finite() && value >= 0.0 => Some(value as u64),
        ResetsAt::Number(_) => None,
        ResetsAt::String(value) => chrono::DateTime::parse_from_rfc3339(value.trim())
            .ok()
            .and_then(|dt| u64::try_from(dt.timestamp()).ok()),
    }
}

fn throttle_observation(
    headers: &HeaderMap,
    observed_at: SystemTime,
    config: &OAuthUsagePollerConfig,
) -> ThrottleObservation {
    let window_secs = headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(config.fallback_interval_secs.max(1));
    ThrottleObservation {
        observed_at,
        window_secs,
        capacity: 1,
    }
}

fn system_time_to_unix_millis(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn system_time_to_unix_secs(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_unix_secs() -> u64 {
    system_time_to_unix_secs(SystemTime::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(seconds)
    }

    fn sustained_estimator() -> PollScheduleEstimator {
        PollScheduleEstimator::new(EstimatorConfig {
            bootstrap_attempts: 0,
            bootstrap_default_interval_secs: 60,
            safety_factor: 1.0,
            min_interval_secs: 60,
            max_interval_secs: 3_600,
            fallback_interval_secs: 60,
            history_capacity: 8,
            throttle_ladder_secs: vec![300, 300, 300, 300, 300],
            rate_limit_window_secs: 300,
            rate_limit_capacity: 5,
            rate_limit_safety_secs: 5,
        })
    }

    #[test]
    fn parses_iso8601_resets_at() {
        let usage: UsageResponse = serde_json::from_str(
            r#"{"five_hour":{"utilization":0.5,"resets_at":"2026-06-03T12:00:00Z"}}"#,
        )
        .expect("usage parses");
        let records = usage_to_records(Uuid::nil(), usage, 1000);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].window, SubscriptionQuotaWindow::FiveHour);
        assert_eq!(records[0].resets_at_unix_secs, Some(1_780_488_000));
    }

    #[test]
    fn parses_numeric_resets_at_and_skips_missing_windows() {
        let usage: UsageResponse =
            serde_json::from_str(r#"{"seven_day":{"utilization":0.25,"resets_at":1800000000}}"#)
                .expect("usage parses");
        let records = usage_to_records(Uuid::nil(), usage, 1000);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].window, SubscriptionQuotaWindow::SevenDay);
        assert_eq!(records[0].resets_at_unix_secs, Some(1_800_000_000));
    }

    #[test]
    fn percent_utilization_is_normalized() {
        let usage: UsageResponse = serde_json::from_str(
            r#"{"five_hour":{"utilization":10},"seven_day":{"utilization":25},"seven_day_sonnet":{"utilization":2},"seven_day_opus":{"utilization":100}}"#,
        )
        .expect("usage parses");
        let records = usage_to_records(Uuid::nil(), usage, 1000);
        assert_eq!(records[0].utilization, Some(0.10));
        assert_eq!(records[1].utilization, Some(0.25));
        assert_eq!(records[2].utilization, Some(0.02));
        assert_eq!(records[3].utilization, Some(1.0));
        assert_eq!(percent_to_utilization_fraction(125.0), 1.0);
        assert_eq!(percent_to_utilization_fraction(-10.0), 0.0);
    }

    #[test]
    fn oauth_usage_poll_is_due_only_consults_estimator() {
        let upstream_id = Uuid::new_v4();
        let estimator = sustained_estimator();
        estimator.record_success(upstream_id, at(1_000));

        assert!(!oauth_usage_poll_is_due(&estimator, upstream_id, at(1_059)));
        assert!(oauth_usage_poll_is_due(&estimator, upstream_id, at(1_060)));
    }

    #[test]
    fn oauth_usage_poll_is_due_blocks_sixth_within_sliding_window() {
        let upstream_id = Uuid::new_v4();
        let estimator = sustained_estimator();
        estimator.record_success(upstream_id, at(0));
        estimator.record_success(upstream_id, at(60));
        estimator.record_success(upstream_id, at(120));
        estimator.record_success(upstream_id, at(180));
        estimator.record_success(upstream_id, at(240));

        assert!(!oauth_usage_poll_is_due(&estimator, upstream_id, at(300)));
        assert!(!oauth_usage_poll_is_due(&estimator, upstream_id, at(304)));
        assert!(oauth_usage_poll_is_due(&estimator, upstream_id, at(305)));
    }
}
