use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
const NEAR_RESET_HINT_WINDOW_SECS: u64 = 300;
const NEAR_RESET_HINT_MIN_INTERVAL_SECS: u64 = 30;
const RECENT_STATUS_FAILURE_BACKOFF_SECS: u64 = 60;
const FIVE_HOUR_QUOTA_SNAPSHOT_MAX_STALENESS_SECS: u64 = 5 * 60 * 60;

type UsageHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

pub struct OAuthUsagePoller {
    pub replica_id: ReplicaIdentity,
    pub stores: Arc<Stores>,
    pub aead: Arc<AeadService>,
    pub lazy_refresher: Arc<LazyRefresher>,
    pub estimator: PollScheduleEstimator,
    last_status_failure_at: Mutex<HashMap<Uuid, Instant>>,
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
            last_status_failure_at: Mutex::new(HashMap::new()),
            subscription_quota_sink,
            subscription_quota_cache,
            config,
            client,
        }
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let tick_secs = NEAR_RESET_HINT_MIN_INTERVAL_SECS;
        let mut interval = tokio::time::interval(Duration::from_secs(tick_secs));
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
                let monotonic_now = Instant::now();
                if !oauth_usage_poll_is_due(
                    &self.estimator,
                    self.subscription_quota_cache.as_ref(),
                    upstream.id,
                    now,
                    monotonic_now,
                    self.last_status_failure_at(upstream.id),
                ) {
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
                            self.record_status_failure_at(upstream_id, Instant::now());
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
            tracing::warn!(upstream_id = %upstream.id, status = %response.status, "oauth usage poll throttled");
            return Ok(());
        }
        if !response.status.is_success() {
            return Err(UsagePollError::Status(response.status));
        }

        let usage: UsageResponse = serde_json::from_slice(&response.body)
            .map_err(|error| UsagePollError::Parse(error.to_string()))?;
        self.estimator.record_success(upstream.id, observed_at);
        self.clear_status_failure_at(upstream.id);
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

    fn last_status_failure_at(&self, upstream_id: Uuid) -> Option<Instant> {
        self.last_status_failure_at
            .lock()
            .expect("oauth usage poller status failure lock poisoned")
            .get(&upstream_id)
            .copied()
    }

    fn record_status_failure_at(&self, upstream_id: Uuid, observed_at: Instant) {
        self.last_status_failure_at
            .lock()
            .expect("oauth usage poller status failure lock poisoned")
            .insert(upstream_id, observed_at);
    }

    fn clear_status_failure_at(&self, upstream_id: Uuid) {
        self.last_status_failure_at
            .lock()
            .expect("oauth usage poller status failure lock poisoned")
            .remove(&upstream_id);
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
    subscription_quota_cache: &SubscriptionQuotaCache,
    upstream_id: Uuid,
    now: SystemTime,
    monotonic_now: Instant,
    last_status_failure_at: Option<Instant>,
) -> bool {
    estimator.is_due(upstream_id, now)
        || near_reset_hint_is_due(
            estimator,
            subscription_quota_cache,
            upstream_id,
            now,
            monotonic_now,
            last_status_failure_at,
        )
}

fn near_reset_hint_is_due(
    estimator: &PollScheduleEstimator,
    subscription_quota_cache: &SubscriptionQuotaCache,
    upstream_id: Uuid,
    now: SystemTime,
    monotonic_now: Instant,
    last_status_failure_at: Option<Instant>,
) -> bool {
    estimator
        .last_success_at(upstream_id)
        .and_then(|last_success_at| now.duration_since(last_success_at).ok())
        .is_some_and(|elapsed| elapsed >= Duration::from_secs(NEAR_RESET_HINT_MIN_INTERVAL_SECS))
        && latest_five_hour_reset_is_near(subscription_quota_cache, upstream_id, now)
        && !recent_status_failure_is_active(last_status_failure_at, monotonic_now)
}

fn recent_status_failure_is_active(
    last_status_failure_at: Option<Instant>,
    monotonic_now: Instant,
) -> bool {
    last_status_failure_at.is_some_and(|last_status_failure_at| {
        monotonic_now
            .checked_duration_since(last_status_failure_at)
            .is_none_or(|elapsed| elapsed < Duration::from_secs(RECENT_STATUS_FAILURE_BACKOFF_SECS))
    })
}

fn latest_five_hour_reset_is_near(
    subscription_quota_cache: &SubscriptionQuotaCache,
    upstream_id: Uuid,
    now: SystemTime,
) -> bool {
    let now_unix_millis = system_time_to_unix_millis(now);
    let now_unix_secs = now_unix_millis / 1_000;
    subscription_quota_cache
        .snapshot_for_upstream(
            upstream_id,
            now_unix_millis,
            FIVE_HOUR_QUOTA_SNAPSHOT_MAX_STALENESS_SECS,
        )
        .into_iter()
        .find(|snapshot| snapshot.window == SubscriptionQuotaWindow::FiveHour.as_str())
        .and_then(|snapshot| snapshot.resets_at_unix_secs)
        .and_then(|resets_at_unix_secs| resets_at_unix_secs.checked_sub(now_unix_secs))
        .is_some_and(|secs_until_reset| {
            secs_until_reset > 0 && secs_until_reset < NEAR_RESET_HINT_WINDOW_SECS
        })
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

    fn slow_estimator() -> PollScheduleEstimator {
        PollScheduleEstimator::new(EstimatorConfig {
            bootstrap_attempts: 1,
            bootstrap_default_interval_secs: 60,
            safety_factor: 1.0,
            min_interval_secs: 1,
            max_interval_secs: 3_600,
            fallback_interval_secs: 900,
            history_capacity: 8,
            throttle_ladder_secs: vec![60, 300, 900],
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
    fn near_reset_hint_is_due_within_thirty_seconds_of_last_poll() {
        let upstream_id = Uuid::new_v4();
        let cache = SubscriptionQuotaCache::new();
        let estimator = slow_estimator();
        estimator.record_success(upstream_id, at(1_000));

        let boundary_record = sample_record(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            1_000_000,
            Some(0.9),
            Some(1_330),
            None,
        );
        cache.upsert_observation(upstream_id, &boundary_record);

        let too_early = at(1_029);
        assert!(!estimator.is_due(upstream_id, too_early));
        assert!(!oauth_usage_poll_is_due(
            &estimator,
            &cache,
            upstream_id,
            too_early,
            Instant::now(),
            None,
        ));

        let boundary = at(1_030);
        assert!(!estimator.is_due(upstream_id, boundary));
        assert!(!oauth_usage_poll_is_due(
            &estimator,
            &cache,
            upstream_id,
            boundary,
            Instant::now(),
            None,
        ));

        let near_reset_record = sample_record(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            1_000_001,
            Some(0.9),
            Some(1_130),
            None,
        );
        cache.upsert_observation(upstream_id, &near_reset_record);

        let due_at = at(1_030);
        assert!(!estimator.is_due(upstream_id, due_at));
        assert!(oauth_usage_poll_is_due(
            &estimator,
            &cache,
            upstream_id,
            due_at,
            Instant::now(),
            None,
        ));
    }

    #[test]
    fn near_reset_hint_pauses_for_sixty_seconds_after_status_failure() {
        let upstream_id = Uuid::new_v4();
        let cache = SubscriptionQuotaCache::new();
        let estimator = slow_estimator();
        estimator.record_success(upstream_id, at(1_000));
        let near_reset_record = sample_record(
            upstream_id,
            SubscriptionQuotaWindow::FiveHour,
            1_000_000,
            Some(0.9),
            Some(1_130),
            None,
        );
        cache.upsert_observation(upstream_id, &near_reset_record);

        let due_at = at(1_030);
        let status_failure_at = Instant::now();
        estimator.record_status_failure(upstream_id, due_at, 401);

        assert!(!near_reset_hint_is_due(
            &estimator,
            &cache,
            upstream_id,
            due_at,
            status_failure_at,
            Some(status_failure_at),
        ));
        assert!(!near_reset_hint_is_due(
            &estimator,
            &cache,
            upstream_id,
            due_at,
            status_failure_at + Duration::from_secs(59),
            Some(status_failure_at),
        ));
        assert!(near_reset_hint_is_due(
            &estimator,
            &cache,
            upstream_id,
            due_at,
            status_failure_at + Duration::from_secs(60),
            Some(status_failure_at),
        ));
    }
}
