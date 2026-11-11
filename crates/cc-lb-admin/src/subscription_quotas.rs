#![allow(clippy::result_large_err, clippy::manual_clamp)]

mod series;

use std::collections::{BTreeMap, HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use cc_lb_clock::Clock;
use cc_lb_control::DynamicViewHolder;
use cc_lb_domain::{
    SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaCredits, SubscriptionQuotaDataState,
};
use cc_lb_quota::plan_capacity::{PRO_CAPACITY_RATIO, plan_capacity_ratio};
use cc_lb_storage_api::{
    OrganizationMetadataRecord, POOL_QUOTA_POLICY_VERSION, PoolQuotaChartPointRecord,
    PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, PoolQuotaSnapshotSummaryRecord,
    Provider as PoolQuotaProvider, Storage, StorageError, SubscriptionQuotaBucket,
    SubscriptionQuotaProviderLot, SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSeriesQuery, SubscriptionQuotaSource,
    SubscriptionQuotaSourceMerge, SubscriptionQuotaWindow, UpstreamRecord, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UsageTokenInterval, upstream::UpstreamKind,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;
use series::list_subscription_quota_series;

const STORE_PAGE_LIMIT: usize = 1_000;
const DEFAULT_SERIES_BUCKET_SECS: u64 = 300;
const DEFAULT_SERIES_MAX_POINTS: u32 = 1_000;
const MAX_SERIES_MAX_POINTS: u32 = 10_000;
const MAX_SERIES_UPSTREAMS: usize = 50;
const RESET_DROP_THRESHOLD: f64 = 0.5;
const GAP_MARKER_MULTIPLIER: u64 = 2;
const POOL_SNAPSHOT_HEARTBEAT_SECS: i64 = 3_600;
const POOL_HISTORY_GAP_SLACK_SECS: i64 = 300;
const CAPACITY_CAVEAT: &str = "capacity is inferred from proxy tokens and quota utilization; Anthropic quota units are not directly exposed";
const HEADER_FALLBACK_CAVEAT: &str = "utilization is the arithmetic mean of header observations; capacity-weighted estimate is unavailable because cc-lb has not proxied enough traffic to back-solve provider capacity";
const PLAN_RATIO_CAVEAT: &str = "utilization is weighted by subscription plan ratios from Anthropic metadata; 5h and 7d use the same documented plan ratios";
const STALE_DATA_CAVEAT: &str =
    "latest observation is older than max_staleness_secs; analysis may be outdated";

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/subscription-quotas/latest", get(handle_latest))
        .route("/admin/v1/subscription-quotas/series", get(handle_series))
        .route(
            "/admin/v1/subscription-quotas/aggregate",
            get(handle_aggregate),
        )
        .route(
            "/admin/v1/subscription-quotas/pool-history",
            get(handle_pool_history),
        )
}

#[derive(Debug, Serialize, Deserialize)]
struct LatestQuery {
    upstream_ids: Option<String>,
    windows: Option<String>,
    source: Option<String>,
    max_staleness_secs: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LatestResponse {
    now_unix_secs: u64,
    max_staleness_secs: u64,
    upstreams: Vec<LatestUpstreamResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LatestUpstreamResponse {
    upstream_id: String,
    upstream_name: String,
    windows: Vec<LatestWindowResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LatestWindowResponse {
    window: String,
    state: String,
    source: Option<String>,
    utilization: Option<f64>,
    status: Option<String>,
    resets_at_unix_secs: Option<u64>,
    surpassed_threshold: Option<f64>,
    representative_claim: Option<String>,
    disabled_reason: Option<String>,
    extra_usage_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    credits: Option<SubscriptionQuotaCredits>,
    extra_usage_monthly_limit: Option<f64>,
    extra_usage_used_credits: Option<f64>,
    observed_at_unix_millis: Option<u64>,
    age_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fallback_available: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    overage_in_use: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    overage_period_monthly_utilization: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    upgrade_paths: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SeriesQuery {
    upstream_ids: Option<String>,
    windows: Option<String>,
    source: Option<String>,
    since_unix_secs: u64,
    until_unix_secs: u64,
    bucket_secs: Option<u64>,
    max_points_per_series: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SeriesResponse {
    since_unix_secs: u64,
    until_unix_secs: u64,
    bucket_secs: u64,
    source: String,
    series: Vec<SeriesResponseItem>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SeriesResponseItem {
    upstream_id: String,
    upstream_name: String,
    window: String,
    buckets: Vec<SeriesBucketResponse>,
    markers: Vec<SeriesMarkerResponse>,
}

// Slim wire shape: the dashboard plots only these two fields.
#[derive(Debug, Serialize, Deserialize)]
struct SeriesBucketResponse {
    bucket_start_unix_secs: u64,
    utilization_last: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SeriesMarkerResponse {
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    at_unix_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from_unix_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to_unix_secs: Option<u64>,
}

fn parse_pool_quota_provider(raw: Option<&str>) -> Result<PoolQuotaProvider, Response> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("anthropic") => Ok(PoolQuotaProvider::Anthropic),
        Some("openai") => Ok(PoolQuotaProvider::Openai),
        Some("gemini") => Ok(PoolQuotaProvider::Gemini),
        Some(value) => Err(bad_request(
            "invalid_provider",
            format!("unsupported provider: {value}"),
        )),
    }
}

const fn pool_provider_kind(provider: PoolQuotaProvider) -> UpstreamKind {
    match provider {
        PoolQuotaProvider::Anthropic => UpstreamKind::AnthropicOauth,
        PoolQuotaProvider::Openai => UpstreamKind::OpenaiOauth,
        PoolQuotaProvider::Gemini => UpstreamKind::GeminiOauth,
    }
}

const fn pool_provider_weighting(provider: PoolQuotaProvider) -> &'static str {
    match provider {
        PoolQuotaProvider::Anthropic => "plan",
        PoolQuotaProvider::Openai | PoolQuotaProvider::Gemini => "equal",
    }
}
#[derive(Debug)]
struct AggregateBuildParams {
    upstream_ids: Option<Vec<Uuid>>,
    windows: Vec<SubscriptionQuotaWindow>,
    source: SubscriptionQuotaSourceMerge,
    max_staleness_secs: u64,
    provider: PoolQuotaProvider,
}

#[derive(Debug, Serialize, Deserialize)]
struct AggregateQuery {
    upstream_ids: Option<String>,
    windows: Option<String>,
    source: Option<String>,
    max_staleness_secs: Option<u64>,
    provider: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcLbOAuthUsageResponse {
    #[serde(rename = "5h", skip_serializing_if = "Option::is_none")]
    pub five_hour: Option<CcLbOAuthWindowUsage>,
    #[serde(rename = "7d", skip_serializing_if = "Option::is_none")]
    pub seven_day: Option<CcLbOAuthWindowUsage>,
    #[serde(rename = "7d_sonnet", skip_serializing_if = "Option::is_none")]
    pub seven_day_sonnet: Option<CcLbOAuthWindowUsage>,
    #[serde(rename = "7d_opus", skip_serializing_if = "Option::is_none")]
    pub seven_day_opus: Option<CcLbOAuthWindowUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits: Option<Vec<CcLbOAuthUsageLimit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_usage: Option<CcLbOAuthExtraUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcLbOAuthWindowUsage {
    pub utilization: Option<f64>,
    pub resets_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcLbOAuthExtraUsage {
    pub enabled: Option<bool>,
    pub monthly_limit: Option<f64>,
    pub used_credits: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcLbOAuthUsageLimit {
    pub kind: String,
    pub percent: Option<f64>,
    pub resets_at: u64,
    pub scope: CcLbOAuthUsageLimitScope,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcLbOAuthUsageLimitScope {
    pub model: CcLbOAuthUsageLimitModel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcLbOAuthUsageLimitModel {
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateResponse {
    now_unix_secs: u64,
    window_anchor_unix_secs: u64,
    max_staleness_secs: u64,
    upstream_count: usize,
    weighting: String,
    windows: Vec<AggregateWindowResponse>,
    caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateWindowResponse {
    window: String,
    cc_window_start_unix_secs: u64,
    cc_window_reset_unix_secs: u64,
    used_tokens: u64,
    utilization: Option<f64>,
    utilization_percent: Option<f64>,
    capacity_to_now_tokens_estimate: Option<f64>,
    projected_capacity_tokens_estimate: Option<f64>,
    remaining_to_now_tokens_estimate: Option<f64>,
    confidence: String,
    contributing_upstreams: usize,
    stale_upstreams: usize,
    missing_capacity_upstreams: usize,
    /// Pool members with this window; explicit provider absence is not missing data.
    eligible_upstreams: usize,
    provider_lots: Vec<AggregateProviderLotResponse>,
    caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateProviderLotResponse {
    upstream_id: String,
    upstream_name: String,
    window: String,
    source: Option<String>,
    state: String,
    provider_start_unix_secs: Option<u64>,
    provider_reset_unix_secs: Option<u64>,
    observed_at_unix_millis: Option<u64>,
    utilization: Option<f64>,
    capacity_estimate_tokens: Option<f64>,
    used_before_cc_window_tokens: u64,
    capacity_to_now_tokens_estimate: Option<f64>,
    projected_capacity_tokens_estimate: Option<f64>,
    confidence: String,
    capacity_ratio: f64,
}

async fn handle_latest(
    State(state): State<AdminState>,
    Query(query): Query<LatestQuery>,
) -> Response {
    match build_latest_response(&state, query).await {
        Ok(response) => Json(response).into_response(),
        Err(response) => response,
    }
}

async fn handle_series(
    State(state): State<AdminState>,
    Query(query): Query<SeriesQuery>,
) -> Response {
    match build_series_response(&state, query).await {
        Ok(response) => Json(response).into_response(),
        Err(response) => response,
    }
}

async fn handle_aggregate(
    State(state): State<AdminState>,
    Query(query): Query<AggregateQuery>,
) -> Response {
    match build_aggregate_response(&state, query).await {
        Ok(response) => Json(response).into_response(),
        Err(response) => response,
    }
}

async fn handle_pool_history(
    State(state): State<AdminState>,
    Query(query): Query<PoolHistoryQuery>,
) -> Response {
    match build_pool_history_response(&state, query).await {
        Ok(response) => Json(response).into_response(),
        Err(response) => response,
    }
}

pub const POOLED_HISTORY_WINDOWS: &[SubscriptionQuotaWindow] = &[
    SubscriptionQuotaWindow::FiveHour,
    SubscriptionQuotaWindow::SevenDay,
    SubscriptionQuotaWindow::SevenDayFable,
    SubscriptionQuotaWindow::GeminiFiveHour,
    SubscriptionQuotaWindow::GeminiSevenDay,
    SubscriptionQuotaWindow::ThirdPartyFiveHour,
    SubscriptionQuotaWindow::ThirdPartySevenDay,
];

#[derive(Debug, Deserialize)]
struct PoolHistoryQuery {
    windows: Option<String>,
    since_unix_secs: Option<i64>,
    until_unix_secs: Option<i64>,
    relative_range_secs: Option<i64>,
    window_quantum_secs: Option<i64>,
    series_projection: Option<String>,
    max_points_per_series: Option<usize>,
    provider: Option<String>,
}

#[derive(Debug, Serialize)]
struct PoolHistoryResponse {
    now_unix_secs: i64,
    weighting: &'static str,
    windows: Vec<PoolHistoryWindowResponse>,
}

#[derive(Debug, Serialize)]
struct PoolHistoryWindowResponse {
    window: String,
    latest: Option<PoolHistoryPoint>,
    series: PoolHistorySeries,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum PoolHistorySeries {
    Full(Vec<PoolHistoryPoint>),
    Chart(Vec<PoolHistorySeriesPoint>),
}

#[derive(Debug, Clone, Serialize)]
struct PoolHistoryPoint {
    snapshot_at_unix_secs: i64,
    utilization: Option<f64>,
    utilization_percent: Option<f64>,
    contributing_upstreams: i64,
    eligible_upstreams: i64,
    stale_upstreams: i64,
    missing_observation_upstreams: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    oldest_observed_at_unix_millis: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_staleness_secs: Option<u64>,
    max_observed_at_unix_millis: Option<i64>,
}

#[derive(Debug, Serialize)]
struct PoolHistorySeriesPoint {
    snapshot_at_unix_secs: i64,
    utilization_percent: Option<f64>,
}

async fn build_pool_history_response(
    state: &AdminState,
    query: PoolHistoryQuery,
) -> Result<PoolHistoryResponse, Response> {
    let provider = parse_pool_quota_provider(query.provider.as_deref())?;
    let mut windows = parse_pool_history_windows(query.windows.as_deref(), provider)?;
    let storage = storage(state)?;
    let compact_series = match query.series_projection.as_deref() {
        None | Some("full") => false,
        Some("chart") => true,
        Some(value) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("unknown pool history series projection: {value}"),
            )
                .into_response());
        }
    };
    let now_unix_secs = (now_unix_millis(&*state.clock) / 1_000) as i64;
    let default_lookback_secs: i64 = 6 * 60 * 60;
    let (since_unix_secs, until_unix_secs) = if let Some(range_secs) = query.relative_range_secs {
        if range_secs <= 0 {
            return Err(bad_request(
                "invalid_relative_range",
                "pool history relative_range_secs must be positive",
            ));
        }
        if query.since_unix_secs.is_some() || query.until_unix_secs.is_some() {
            return Err(bad_request(
                "ambiguous_history_range",
                "pool history relative_range_secs cannot be combined with absolute bounds",
            ));
        }
        let quantum_secs = query.window_quantum_secs.unwrap_or(0);
        if quantum_secs < 0 {
            return Err(bad_request(
                "invalid_window_quantum",
                "pool history window_quantum_secs must be nonnegative",
            ));
        }
        let invalid_range = || {
            bad_request(
                "invalid_relative_range",
                "pool history relative range exceeds supported timestamp bounds",
            )
        };
        let until = if quantum_secs == 0 {
            now_unix_secs
        } else {
            let remainder = now_unix_secs.rem_euclid(quantum_secs);
            now_unix_secs
                .checked_add(if remainder == 0 {
                    0
                } else {
                    quantum_secs - remainder
                })
                .ok_or_else(invalid_range)?
        };
        let since = until
            .checked_sub(range_secs)
            .and_then(|value| value.checked_sub(quantum_secs))
            .and_then(|value| {
                value.checked_sub(POOL_SNAPSHOT_HEARTBEAT_SECS + POOL_HISTORY_GAP_SLACK_SECS)
            })
            .ok_or_else(invalid_range)?;
        (since, until)
    } else {
        (
            query
                .since_unix_secs
                .unwrap_or(now_unix_secs.saturating_sub(default_lookback_secs)),
            query.until_unix_secs.unwrap_or(now_unix_secs),
        )
    };
    let bucket_secs = match query.max_points_per_series {
        None => None,
        Some(_) if !compact_series => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "pool history max_points_per_series requires series_projection=chart".to_owned(),
            )
                .into_response());
        }
        Some(value) if !(2..=10_000).contains(&value) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "pool history max_points_per_series must be between 2 and 10000".to_owned(),
            )
                .into_response());
        }
        Some(value) => {
            let divisor = i64::try_from(value - 1).expect("validated point limit fits i64");
            let range_secs = until_unix_secs.saturating_sub(since_unix_secs);
            Some(range_secs.saturating_add(divisor - 1) / divisor.max(1))
        }
    };
    if provider == PoolQuotaProvider::Anthropic
        && windows.contains(&SubscriptionQuotaWindow::FiveHour)
    {
        let upstreams = pool_quota_upstreams(
            list_all_upstreams_storage(storage)
                .await
                .map_err(storage_error)?,
            None,
            UpstreamKind::AnthropicOauth,
        );
        let ids = upstreams
            .iter()
            .map(|upstream| upstream.id)
            .collect::<Vec<_>>();
        let latest = storage
            .list_latest_subscription_quota_for_upstreams(&ids)
            .await
            .map_err(storage_error)?;
        let view = state.dynamic_view.load();
        let now = now_unix_millis(&*state.clock);
        let max_staleness = view.subscription_quota_routing_max_staleness_secs;
        if !upstreams.is_empty()
            && upstreams.iter().all(|upstream| {
                let snapshots = view.subscription_quota_cache.snapshot_for_upstream(
                    upstream.id,
                    now,
                    max_staleness,
                );
                five_hour_is_absent(
                    latest
                        .iter()
                        .filter(|sample| sample.upstream_id == upstream.id),
                    snapshots.iter().find(|snapshot| snapshot.window == "5h"),
                    now,
                    max_staleness,
                )
            })
        {
            windows.retain(|window| *window != SubscriptionQuotaWindow::FiveHour);
        }
    }

    let mut latest_by_window = if provider == PoolQuotaProvider::Anthropic {
        storage
            .list_latest_pool_quota_snapshot_summaries(provider, &windows)
            .await
            .map_err(storage_error)?
            .iter()
            .map(|record| (record.window, pool_history_point_from_record(record)))
            .collect::<BTreeMap<_, _>>()
    } else {
        build_provider_pool_quota_snapshots(
            storage,
            &state.dynamic_view,
            provider,
            &windows,
            &*state.clock,
        )
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(|snapshot| {
            let mut point = pool_history_point_from_record(&PoolQuotaSnapshotSummaryRecord::from(
                &snapshot.record,
            ));
            point.oldest_observed_at_unix_millis = snapshot.oldest_observed_at_unix_millis;
            point.max_staleness_secs = Some(snapshot.max_staleness_secs);
            (snapshot.record.window, point)
        })
        .collect()
    };

    let mut full_series_by_window =
        BTreeMap::<SubscriptionQuotaWindow, Vec<PoolHistoryPoint>>::new();
    let mut chart_series_by_window =
        BTreeMap::<SubscriptionQuotaWindow, Vec<PoolHistorySeriesPoint>>::new();
    if compact_series {
        let records = storage
            .list_pool_quota_chart_points_in_range(
                provider,
                &windows,
                since_unix_secs,
                until_unix_secs,
                bucket_secs.filter(|value| *value > 0),
            )
            .await
            .map_err(storage_error)?;
        for record in records {
            let window = record.window;
            chart_series_by_window
                .entry(window)
                .or_default()
                .push(pool_history_series_point_from_record(&record));
        }
    } else {
        let records = storage
            .list_pool_quota_snapshot_summaries_in_range(
                provider,
                &windows,
                since_unix_secs,
                until_unix_secs,
            )
            .await
            .map_err(storage_error)?;
        for record in records {
            let window = record.window;
            full_series_by_window
                .entry(window)
                .or_default()
                .push(pool_history_point_from_record(&record));
        }
    }

    let response_windows = windows
        .into_iter()
        .map(|window| PoolHistoryWindowResponse {
            window: window.as_str().to_owned(),
            latest: latest_by_window.remove(&window),
            series: if compact_series {
                PoolHistorySeries::Chart(chart_series_by_window.remove(&window).unwrap_or_default())
            } else {
                PoolHistorySeries::Full(full_series_by_window.remove(&window).unwrap_or_default())
            },
        })
        .collect();

    Ok(PoolHistoryResponse {
        now_unix_secs,
        weighting: pool_provider_weighting(provider),
        windows: response_windows,
    })
}

fn parse_pool_history_windows(
    raw: Option<&str>,
    provider: PoolQuotaProvider,
) -> Result<Vec<SubscriptionQuotaWindow>, Response> {
    let default_windows = match provider {
        PoolQuotaProvider::Anthropic | PoolQuotaProvider::Openai => vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ],
        PoolQuotaProvider::Gemini => vec![
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaWindow::GeminiSevenDay,
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            SubscriptionQuotaWindow::ThirdPartySevenDay,
        ],
    };
    let Some(value) = raw else {
        return Ok(default_windows);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(default_windows);
    }
    let mut windows = Vec::new();
    for part in trimmed.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        let window = SubscriptionQuotaWindow::from_str(token).ok_or_else(|| {
            bad_request(
                "invalid_window",
                format!("unknown pool history window: {token}"),
            )
        })?;
        if !provider_quota_windows(pool_provider_kind(provider)).contains(&window) {
            return Err(bad_request(
                "unsupported_pool_window",
                format!("window {token} is not supported for this provider"),
            ));
        }
        if !windows.contains(&window) {
            windows.push(window);
        }
    }
    if windows.is_empty() {
        Ok(default_windows)
    } else {
        Ok(windows)
    }
}

fn pool_history_point_from_record(record: &PoolQuotaSnapshotSummaryRecord) -> PoolHistoryPoint {
    PoolHistoryPoint {
        snapshot_at_unix_secs: record.snapshot_at_unix_secs,
        utilization: record.utilization,
        utilization_percent: record.utilization.map(|value| value * 100.0),
        contributing_upstreams: record.contributing_upstreams,
        eligible_upstreams: record.eligible_upstreams,
        stale_upstreams: record.stale_upstreams,
        missing_observation_upstreams: record.missing_observation_upstreams,
        oldest_observed_at_unix_millis: None,
        max_staleness_secs: None,
        max_observed_at_unix_millis: record.max_observed_at_unix_millis,
    }
}
fn pool_history_series_point_from_record(
    record: &PoolQuotaChartPointRecord,
) -> PoolHistorySeriesPoint {
    PoolHistorySeriesPoint {
        snapshot_at_unix_secs: record.snapshot_at_unix_secs,
        utilization_percent: record.utilization.map(|value| value * 100.0),
    }
}

pub async fn build_cc_lb_oauth_usage_response(
    storage: &dyn Storage,
    dynamic_view: &DynamicViewHolder,
    clock: &dyn Clock,
) -> Result<CcLbOAuthUsageResponse, StorageError> {
    let aggregate = build_cc_lb_aggregate_response(
        storage,
        dynamic_view,
        None,
        vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::SevenDaySonnet,
            SubscriptionQuotaWindow::SevenDayOpus,
            SubscriptionQuotaWindow::SevenDayFable,
        ],
        SubscriptionQuotaSourceMerge::Merged,
        dynamic_view
            .load()
            .subscription_quota_routing_max_staleness_secs,
        clock,
    )
    .await?;
    let mut response = oauth_usage_from_aggregate(&aggregate);
    response.extra_usage = build_cc_lb_oauth_extra_usage(
        storage,
        dynamic_view,
        dynamic_view
            .load()
            .subscription_quota_routing_max_staleness_secs,
        clock,
    )
    .await?;
    Ok(response)
}

async fn build_latest_response(
    state: &AdminState,
    query: LatestQuery,
) -> Result<LatestResponse, Response> {
    let storage = storage(state)?;
    let source = parse_source_merge(query.source.as_deref())?;
    let windows = parse_windows_or_all(query.windows.as_deref())?;
    let window_filter = windows
        .iter()
        .map(|window| window.as_str())
        .collect::<HashSet<_>>();
    let max_staleness_secs = query.max_staleness_secs.unwrap_or_else(|| {
        state
            .dynamic_view
            .load()
            .subscription_quota_routing_max_staleness_secs
    });
    let now_unix_millis = now_unix_millis(&*state.clock);
    let upstreams = upstreams_for_optional_query(storage, query.upstream_ids.as_deref()).await?;
    let dynamic_view = state.dynamic_view.load();
    let mut response_upstreams = Vec::with_capacity(upstreams.len());

    for upstream in upstreams {
        let snapshots = dynamic_view.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis,
            max_staleness_secs,
        );
        let windows = snapshots
            .into_iter()
            .filter(|snapshot| window_filter.contains(snapshot.window.as_str()))
            .filter(|snapshot| provider_supports_window(upstream.kind, &snapshot.window))
            .filter(|snapshot| snapshot_matches_source(snapshot, source))
            .map(|snapshot| latest_window_response(snapshot, now_unix_millis))
            .collect();
        response_upstreams.push(LatestUpstreamResponse {
            upstream_id: upstream.id.to_string(),
            upstream_name: upstream.name,
            windows,
        });
    }

    Ok(LatestResponse {
        now_unix_secs: now_unix_millis / 1_000,
        max_staleness_secs,
        upstreams: response_upstreams,
    })
}

async fn build_series_response(
    state: &AdminState,
    query: SeriesQuery,
) -> Result<SeriesResponse, Response> {
    let storage = storage(state)?;
    let source = parse_source_merge(query.source.as_deref())?;
    let windows = parse_windows_or_default(query.windows.as_deref())?;
    let bucket_secs = query
        .bucket_secs
        .unwrap_or(DEFAULT_SERIES_BUCKET_SECS)
        .max(1);
    let max_points_per_series = query
        .max_points_per_series
        .unwrap_or(DEFAULT_SERIES_MAX_POINTS);
    validate_time_range(query.since_unix_secs, query.until_unix_secs)?;
    validate_series_guardrails(
        query.since_unix_secs,
        query.until_unix_secs,
        bucket_secs,
        max_points_per_series,
        "increase bucket_secs or max_points_per_series; requested range exceeds max_points_per_series * 2 buckets",
    )?;

    let upstreams = upstreams_for_optional_query(storage, query.upstream_ids.as_deref()).await?;
    validate_upstream_count(upstreams.len())?;
    let upstream_names = upstream_name_map(&upstreams);
    let upstream_kinds = upstreams
        .iter()
        .map(|upstream| (upstream.id, upstream.kind))
        .collect::<HashMap<_, _>>();
    let mut windows = windows;
    if query
        .windows
        .as_deref()
        .is_none_or(|raw| raw.trim().is_empty())
        && upstreams
            .iter()
            .any(|upstream| upstream.kind == UpstreamKind::GeminiOauth)
    {
        windows.extend([
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaWindow::GeminiSevenDay,
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            SubscriptionQuotaWindow::ThirdPartySevenDay,
        ]);
    }
    let upstream_ids = upstreams
        .into_iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();

    let series = list_subscription_quota_series(
        storage,
        SubscriptionQuotaSeriesQuery {
            upstream_ids,
            windows,
            sources: sources_for_merge(source),
            since_unix_millis: query.since_unix_secs.saturating_mul(1_000),
            until_unix_millis: query.until_unix_secs.saturating_mul(1_000),
            bucket_secs,
            max_points_per_series,
            source_merge: source,
        },
    )
    .await
    .map_err(storage_error)?
    .into_iter()
    .filter(|series| {
        upstream_kinds
            .get(&series.upstream_id)
            .is_some_and(|kind| provider_supports_window(*kind, series.window.as_str()))
    })
    .map(|series| {
        let markers = build_markers(&series.buckets, bucket_secs);
        SeriesResponseItem {
            upstream_id: series.upstream_id.to_string(),
            upstream_name: upstream_names
                .get(&series.upstream_id)
                .cloned()
                .unwrap_or_default(),
            window: series.window.as_str().to_owned(),
            buckets: series.buckets.iter().map(series_bucket_response).collect(),
            markers,
        }
    })
    .collect();

    Ok(SeriesResponse {
        since_unix_secs: query.since_unix_secs,
        until_unix_secs: query.until_unix_secs,
        bucket_secs,
        source: source.as_str().to_owned(),
        series,
    })
}

async fn build_aggregate_response(
    state: &AdminState,
    query: AggregateQuery,
) -> Result<AggregateResponse, Response> {
    let storage = storage(state)?;
    let provider = parse_pool_quota_provider(query.provider.as_deref())?;
    let source = parse_source_merge(query.source.as_deref())?;
    let windows = parse_windows_or_provider_default(query.windows.as_deref(), provider)?;
    let upstream_ids = match query.upstream_ids.as_deref() {
        Some(value) if !value.trim().is_empty() => Some(parse_upstream_ids(value)?),
        _ => None,
    };
    let max_staleness_secs = query.max_staleness_secs.unwrap_or_else(|| {
        state
            .dynamic_view
            .load()
            .subscription_quota_routing_max_staleness_secs
    });

    build_cc_lb_aggregate_response_for_provider(
        storage,
        &state.dynamic_view,
        AggregateBuildParams {
            upstream_ids,
            windows,
            source,
            max_staleness_secs,
            provider,
        },
        &*state.clock,
    )
    .await
    .map_err(storage_error)
}

pub async fn build_cc_lb_aggregate_response(
    storage: &dyn Storage,
    dynamic_view: &DynamicViewHolder,
    upstream_ids: Option<Vec<Uuid>>,
    windows: Vec<SubscriptionQuotaWindow>,
    source: SubscriptionQuotaSourceMerge,
    max_staleness_secs: u64,
    clock: &dyn Clock,
) -> Result<AggregateResponse, StorageError> {
    build_cc_lb_aggregate_response_for_provider(
        storage,
        dynamic_view,
        AggregateBuildParams {
            upstream_ids,
            windows,
            source,
            max_staleness_secs,
            provider: PoolQuotaProvider::Anthropic,
        },
        clock,
    )
    .await
}

async fn build_cc_lb_aggregate_response_for_provider(
    storage: &dyn Storage,
    dynamic_view: &DynamicViewHolder,
    params: AggregateBuildParams,
    clock: &dyn Clock,
) -> Result<AggregateResponse, StorageError> {
    let AggregateBuildParams {
        upstream_ids,
        windows,
        source,
        max_staleness_secs,
        provider,
    } = params;
    let weighting = pool_provider_weighting(provider);
    let provider_kind = pool_provider_kind(provider);
    let now_unix_millis = now_unix_millis(clock);
    let now_unix_secs = now_unix_millis / 1_000;
    let requested = upstream_ids.map(|ids| ids.into_iter().collect::<HashSet<_>>());
    let mut upstreams = pool_quota_upstreams(
        list_all_upstreams_storage(storage).await?,
        requested.as_ref(),
        provider_kind,
    );
    upstreams.sort_by_key(|upstream| upstream.id);

    let duration_windows = windows
        .iter()
        .copied()
        .filter(|window| window_secs(*window).is_some())
        .collect::<Vec<_>>();
    let query_start = duration_windows
        .iter()
        .filter_map(|window| window_secs(*window))
        .map(|secs| cc_window_bounds(now_unix_secs, secs).0)
        .min()
        .unwrap_or(now_unix_secs);
    let upstream_by_id = upstreams
        .iter()
        .map(|upstream| (upstream.id, upstream))
        .collect::<HashMap<_, _>>();
    let subscription_metadata = storage.list_upstream_subscription_metadata().await?;
    let durable_latest = if upstreams.is_empty() || windows.is_empty() {
        Vec::new()
    } else {
        let upstream_ids = upstreams
            .iter()
            .map(|upstream| upstream.id)
            .collect::<Vec<_>>();
        storage
            .list_latest_subscription_quota_for_upstreams(&upstream_ids)
            .await?
    };
    let organization_metadata = storage.list_organization_metadata().await?;
    let capacity_ratios =
        capacity_ratios_by_upstream(&subscription_metadata, &organization_metadata);

    let provider_lots = if upstreams.is_empty() || duration_windows.is_empty() {
        Vec::new()
    } else {
        storage
            .list_subscription_quota_provider_lots(SubscriptionQuotaProviderLotQuery {
                upstream_ids: upstreams.iter().map(|upstream| upstream.id).collect(),
                windows: duration_windows.clone(),
                sources: sources_for_merge(source),
                since_unix_millis: query_start.saturating_mul(1_000),
                until_unix_millis: now_unix_millis,
                source_merge: source,
                evaluation_unix_secs: now_unix_secs,
            })
            .await?
    };

    let dynamic_view = dynamic_view.load();
    let allowed_sources = sources_for_merge(source);
    let mut latest_candidates = Vec::new();
    let mut absent_five_hour = HashSet::new();
    for upstream in &upstreams {
        let snapshots = dynamic_view.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis,
            max_staleness_secs,
        );
        if provider == PoolQuotaProvider::Anthropic
            && five_hour_is_absent(
                durable_latest.iter().filter(|sample| {
                    sample.upstream_id == upstream.id && allowed_sources.contains(&sample.source)
                }),
                snapshots.iter().find(|snapshot| {
                    snapshot.window == "5h" && snapshot_matches_source(snapshot, source)
                }),
                now_unix_millis,
                max_staleness_secs,
            )
        {
            absent_five_hour.insert(upstream.id);
        }
        for snapshot in snapshots {
            if !snapshot_matches_source(&snapshot, source) {
                continue;
            }
            let Some(window) = SubscriptionQuotaWindow::from_str(&snapshot.window) else {
                continue;
            };
            if !windows.contains(&window) {
                continue;
            }
            latest_candidates.push((
                upstream.id,
                window,
                snapshot,
                capacity_ratios
                    .get(&upstream.id)
                    .copied()
                    .unwrap_or(PRO_CAPACITY_RATIO),
            ));
        }
    }

    type FreshCandidate = (u64, SubscriptionQuotaDataState, Option<f64>, Option<u64>);
    let mut freshest_observed: HashMap<(Uuid, SubscriptionQuotaWindow), Vec<FreshCandidate>> =
        HashMap::new();
    for (upstream_id, window, snapshot, _) in &latest_candidates {
        let Some(observed_at) = snapshot.observed_at_unix_millis else {
            continue;
        };
        if matches!(
            snapshot.state,
            SubscriptionQuotaDataState::Absent | SubscriptionQuotaDataState::Unobserved
        ) {
            continue;
        }
        freshest_observed
            .entry((*upstream_id, *window))
            .or_default()
            .push((
                observed_at,
                snapshot.state,
                snapshot.utilization,
                snapshot.resets_at_unix_secs,
            ));
    }
    for sample in &durable_latest {
        if sample.sample_kind != SubscriptionQuotaSampleKind::Sample
            || !allowed_sources.contains(&sample.source)
            || !windows.contains(&sample.window)
        {
            continue;
        }
        let observed_at = sample.observed_at_unix_millis;
        let state = if now_unix_millis.saturating_sub(observed_at)
            > max_staleness_secs.saturating_mul(1_000)
        {
            SubscriptionQuotaDataState::Stale
        } else {
            SubscriptionQuotaDataState::Fresh
        };
        freshest_observed
            .entry((sample.upstream_id, sample.window))
            .or_default()
            .push((
                observed_at,
                state,
                sample.utilization,
                sample.resets_at_unix_secs,
            ));
    }

    let mut lot_inputs = provider_lots
        .iter()
        .filter_map(|lot| {
            let upstream = upstream_by_id.get(&lot.upstream_id)?;
            if lot.window == SubscriptionQuotaWindow::FiveHour
                && absent_five_hour.contains(&lot.upstream_id)
            {
                return None;
            }
            Some(provider_lot_input_from_storage(
                upstream,
                lot,
                capacity_ratios
                    .get(&lot.upstream_id)
                    .copied()
                    .unwrap_or(PRO_CAPACITY_RATIO),
            ))
        })
        .collect::<Vec<_>>();
    let mut latest_lot_idx_by_key: HashMap<(Uuid, SubscriptionQuotaWindow), (u64, usize)> =
        HashMap::new();
    for (idx, input) in lot_inputs.iter().enumerate() {
        let obs = input.observed_at_unix_millis.unwrap_or(0);
        latest_lot_idx_by_key
            .entry((input.upstream_id, input.window))
            .and_modify(|(prev_obs, prev_idx)| {
                if obs >= *prev_obs {
                    *prev_obs = obs;
                    *prev_idx = idx;
                }
            })
            .or_insert((obs, idx));
    }
    for (key, (_, idx)) in latest_lot_idx_by_key {
        let Some(candidates) = freshest_observed.get(&key) else {
            continue;
        };
        let input = &mut lot_inputs[idx];
        let current_obs = input.observed_at_unix_millis.unwrap_or(0);
        if let Some(&(freshest_obs, freshest_state, _, _)) = candidates
            .iter()
            .filter(|&&(obs, _, util, reset)| {
                let util_matches = match (util, input.utilization) {
                    (Some(left), Some(right)) => (left - right).abs() < 0.001,
                    (None, None) => true,
                    _ => false,
                };
                let reset_matches = match (reset, input.provider_reset) {
                    (Some(left), Some(right)) => left.abs_diff(right) <= 60,
                    (None, None) => true,
                    _ => false,
                };
                obs >= current_obs && util_matches && reset_matches
            })
            .max_by_key(|&&(obs, _, _, _)| obs)
        {
            input.observed_at_unix_millis = Some(freshest_obs);
            input.state = freshest_state;
        }
    }
    let covered_by_provider_lots = lot_inputs
        .iter()
        .map(|input| (input.upstream_id, input.window))
        .collect::<HashSet<_>>();
    lot_inputs.extend(
        latest_candidates
            .iter()
            .filter(|(upstream_id, window, _, _)| {
                window_secs(*window).is_some()
                    && !covered_by_provider_lots.contains(&(*upstream_id, *window))
                    && !(*window == SubscriptionQuotaWindow::FiveHour
                        && absent_five_hour.contains(upstream_id))
            })
            .filter_map(|(upstream_id, window, snapshot, capacity_ratio)| {
                upstream_by_id.get(upstream_id).map(|upstream| {
                    provider_lot_input_from_snapshot(
                        upstream,
                        *window,
                        snapshot,
                        *capacity_ratio,
                        now_unix_secs,
                    )
                })
            }),
    );
    let mut non_duration_lot_inputs = latest_candidates
        .iter()
        .filter(|(_, window, _, _)| window_secs(*window).is_none())
        .filter_map(|(upstream_id, window, snapshot, capacity_ratio)| {
            upstream_by_id.get(upstream_id).map(|upstream| {
                provider_lot_input_from_snapshot(
                    upstream,
                    *window,
                    snapshot,
                    *capacity_ratio,
                    now_unix_secs,
                )
            })
        })
        .collect::<Vec<_>>();
    non_duration_lot_inputs.extend(
        durable_latest
            .iter()
            .filter(|sample| {
                sample.sample_kind == SubscriptionQuotaSampleKind::Sample
                    && allowed_sources.contains(&sample.source)
                    && window_secs(sample.window).is_none()
            })
            .filter_map(|sample| {
                upstream_by_id.get(&sample.upstream_id).map(|upstream| {
                    provider_lot_input_from_sample(
                        upstream,
                        sample,
                        max_staleness_secs,
                        now_unix_millis,
                        capacity_ratios
                            .get(&sample.upstream_id)
                            .copied()
                            .unwrap_or(PRO_CAPACITY_RATIO),
                    )
                })
            }),
    );

    let mut intervals = Vec::new();
    let mut push_interval = |upstream_id, start_unix_secs, end_unix_secs| {
        let interval_id = u64::try_from(intervals.len()).unwrap_or(u64::MAX);
        intervals.push(UsageTokenInterval {
            interval_id,
            upstream_id,
            start_unix_secs,
            end_unix_secs,
        });
        interval_id
    };
    let lot_interval_ids = lot_inputs
        .iter()
        .map(|input| {
            let secs = window_secs(input.window).expect("aggregate windows are duration-backed");
            let (cc_start, cc_reset) = cc_window_bounds(now_unix_secs, secs);
            let provider_reset = input.provider_reset.or(Some(cc_reset));
            let provider_sample_end = provider_reset
                .unwrap_or(now_unix_secs)
                .min(input.evaluation_unix_secs)
                .min(now_unix_secs);
            AggregateProviderLotIntervalIds {
                provider_tokens: input
                    .provider_start
                    .map(|start| push_interval(input.upstream_id, start, provider_sample_end)),
                used_before_cc_window: input
                    .provider_start
                    .map(|start| push_interval(input.upstream_id, start, cc_start)),
            }
        })
        .collect::<Vec<_>>();
    let window_interval_ids = duration_windows
        .iter()
        .copied()
        .map(|window| {
            let secs = window_secs(window).expect("aggregate windows are duration-backed");
            let (cc_start, _) = cc_window_bounds(now_unix_secs, secs);
            let ids = upstreams
                .iter()
                .filter(|upstream| {
                    window != SubscriptionQuotaWindow::FiveHour
                        || !absent_five_hour.contains(&upstream.id)
                })
                .map(|upstream| push_interval(upstream.id, cc_start, now_unix_secs))
                .collect::<Vec<_>>();
            (window, ids)
        })
        .collect::<HashMap<_, _>>();
    let (canonical_intervals, canonical_ids_by_key) =
        canonicalize_usage_token_intervals(&intervals);
    let canonical_token_sums = storage
        .sum_usage_tokens_for_intervals(&canonical_intervals)
        .await?
        .into_iter()
        .map(|sum| (sum.interval_id, sum.tokens))
        .collect::<HashMap<_, _>>();
    let token_sums =
        scatter_usage_token_sums(&intervals, &canonical_ids_by_key, &canonical_token_sums);

    let mut aggregate_windows = Vec::new();
    for window in windows {
        let ineligible_upstreams = if window == SubscriptionQuotaWindow::FiveHour {
            absent_five_hour.len()
        } else {
            0
        };
        if ineligible_upstreams > 0 && ineligible_upstreams == upstreams.len() {
            continue;
        }
        let window_lots = if window_secs(window).is_some() {
            lot_inputs
                .iter()
                .zip(&lot_interval_ids)
                .filter(|(input, _)| input.window == window)
                .map(|(input, interval_ids)| {
                    build_provider_lot_response(
                        input,
                        ProviderLotTokenSums {
                            provider_tokens: interval_ids
                                .provider_tokens
                                .and_then(|id| token_sums.get(&id).copied())
                                .unwrap_or(0),
                            used_before_cc_window: interval_ids
                                .used_before_cc_window
                                .and_then(|id| token_sums.get(&id).copied())
                                .unwrap_or(0),
                        },
                        now_unix_secs,
                    )
                })
                .collect::<Vec<_>>()
        } else {
            non_duration_lot_inputs
                .iter()
                .filter(|input| input.window == window)
                .map(|input| {
                    build_provider_lot_response(
                        input,
                        ProviderLotTokenSums {
                            provider_tokens: 0,
                            used_before_cc_window: 0,
                        },
                        now_unix_secs,
                    )
                })
                .collect::<Vec<_>>()
        };
        let used_tokens = window_interval_ids
            .get(&window)
            .into_iter()
            .flatten()
            .filter_map(|interval_id| token_sums.get(interval_id))
            .copied()
            .sum();
        let mut response = build_aggregate_window_response(
            window,
            &window_lots,
            used_tokens,
            now_unix_secs,
            weighting,
        );
        response.eligible_upstreams = upstreams.len().saturating_sub(ineligible_upstreams);
        aggregate_windows.push(response);
    }

    let caveats = vec![
        CAPACITY_CAVEAT.to_owned(),
        "cc-lb /api/oauth/usage preserves Anthropic OAuth usage response shape; detailed cc-lb fields are exposed only on this admin aggregate endpoint".to_owned(),
    ];

    Ok(AggregateResponse {
        now_unix_secs,
        window_anchor_unix_secs: 0,
        max_staleness_secs,
        upstream_count: upstreams.len(),
        weighting: weighting.to_owned(),
        windows: aggregate_windows,
        caveats,
    })
}

pub fn pool_quota_snapshots_from_aggregate(
    aggregate: &AggregateResponse,
    snapshot_at_unix_secs: i64,
    computed_at_unix_millis: i64,
) -> Vec<PoolQuotaSnapshotRecord> {
    aggregate
        .windows
        .iter()
        .filter_map(|window_response| {
            let window = SubscriptionQuotaWindow::from_str(window_response.window.as_str())?;
            if !POOLED_HISTORY_WINDOWS.contains(&window) {
                return None;
            }
            let contributing = window_response
                .provider_lots
                .iter()
                .filter(|lot| lot.utilization.is_some())
                .collect::<Vec<_>>();
            let weighted_utilization_sum = contributing
                .iter()
                .map(|lot| lot.utilization.unwrap_or(0.0) * lot.capacity_ratio)
                .sum::<f64>();
            let capacity_ratio_sum = contributing
                .iter()
                .map(|lot| lot.capacity_ratio)
                .sum::<f64>();
            let header_contributing = contributing
                .iter()
                .filter(|lot| lot.source.as_deref() == Some("header"))
                .count();
            let api_contributing = contributing
                .iter()
                .filter(|lot| lot.source.as_deref() == Some("api"))
                .count();
            let max_observed_at_unix_millis = contributing
                .iter()
                .filter_map(|lot| lot.observed_at_unix_millis)
                .max()
                .and_then(|millis| i64::try_from(millis).ok());
            Some(PoolQuotaSnapshotRecord {
                provider: PoolQuotaProvider::Anthropic,
                snapshot_at_unix_secs,
                window,
                utilization: window_response.utilization,
                weighted_utilization_sum,
                capacity_ratio_sum,
                eligible_upstreams: i64::try_from(window_response.eligible_upstreams)
                    .unwrap_or(i64::MAX),
                contributing_upstreams: i64::try_from(window_response.contributing_upstreams)
                    .unwrap_or(i64::MAX),
                stale_upstreams: i64::try_from(window_response.stale_upstreams).unwrap_or(i64::MAX),
                missing_observation_upstreams: i64::try_from(
                    window_response.missing_capacity_upstreams,
                )
                .unwrap_or(i64::MAX),
                missing_metadata_upstreams: 0,
                header_contributing_upstreams: i64::try_from(header_contributing)
                    .unwrap_or(i64::MAX),
                api_contributing_upstreams: i64::try_from(api_contributing).unwrap_or(i64::MAX),
                max_observed_at_unix_millis,
                computed_at_unix_millis,
                policy_version: POOL_QUOTA_POLICY_VERSION,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
struct ProviderPoolObservation {
    observed_at_unix_millis: u64,
    utilization: Option<f64>,
    absent: bool,
    api: bool,
}

fn provider_pool_observation<'a>(
    durable: impl Iterator<Item = &'a SubscriptionQuotaSample>,
    cached: Option<&SubscriptionQuotaCandidateSnapshot>,
    now_unix_millis: u64,
    max_staleness_secs: u64,
) -> Option<ProviderPoolObservation> {
    let rank = |observation: &ProviderPoolObservation| {
        (
            now_unix_millis.saturating_sub(observation.observed_at_unix_millis)
                <= max_staleness_secs.saturating_mul(1_000),
            observation.observed_at_unix_millis,
            observation.api,
        )
    };
    let durable = durable.map(|sample| ProviderPoolObservation {
        observed_at_unix_millis: sample.observed_at_unix_millis,
        utilization: sample.utilization,
        absent: sample.sample_kind == SubscriptionQuotaSampleKind::Absent,
        api: sample.source == SubscriptionQuotaSource::Api,
    });
    let cached = cached
        .filter(|snapshot| snapshot.state != SubscriptionQuotaDataState::Unobserved)
        .and_then(|snapshot| {
            Some(ProviderPoolObservation {
                observed_at_unix_millis: snapshot.observed_at_unix_millis?,
                utilization: snapshot.utilization,
                absent: snapshot.state == SubscriptionQuotaDataState::Absent,
                api: snapshot.source.as_deref() == Some("api"),
            })
        });
    // Select a whole observation: newer absence or missing utilization must
    // never borrow an older numeric value and its apparent freshness.
    durable.chain(cached).max_by_key(rank)
}

struct ProviderPoolSnapshot {
    record: PoolQuotaSnapshotRecord,
    oldest_observed_at_unix_millis: Option<i64>,
    max_staleness_secs: u64,
}

async fn build_provider_pool_quota_snapshots(
    storage: &dyn Storage,
    dynamic_view: &DynamicViewHolder,
    provider: PoolQuotaProvider,
    windows: &[SubscriptionQuotaWindow],
    clock: &dyn Clock,
) -> Result<Vec<ProviderPoolSnapshot>, StorageError> {
    let upstreams = pool_quota_upstreams(
        list_all_upstreams_storage(storage).await?,
        None,
        pool_provider_kind(provider),
    );
    let ids = upstreams
        .iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();
    let durable = if ids.is_empty() {
        Vec::new()
    } else {
        storage
            .list_latest_subscription_quota_for_upstreams(&ids)
            .await?
    };
    let mut durable_by_window = HashMap::with_capacity(durable.len());
    for sample in &durable {
        let slots = durable_by_window
            .entry((sample.upstream_id, sample.window))
            .or_insert([None; 2]);
        slots[usize::from(sample.source == SubscriptionQuotaSource::Api)] = Some(sample);
    }
    let now = now_unix_millis(clock);
    let computed_at_unix_millis = i64::try_from(now).unwrap_or(i64::MAX);
    let view = dynamic_view.load();
    let max_staleness_secs = view.subscription_quota_routing_max_staleness_secs;
    let mut snapshots = windows
        .iter()
        .map(|window| ProviderPoolSnapshot {
            record: PoolQuotaSnapshotRecord {
                provider,
                snapshot_at_unix_secs: computed_at_unix_millis / 1_000,
                window: *window,
                utilization: None,
                weighted_utilization_sum: 0.0,
                capacity_ratio_sum: 0.0,
                eligible_upstreams: i64::try_from(upstreams.len()).unwrap_or(i64::MAX),
                contributing_upstreams: 0,
                stale_upstreams: 0,
                missing_observation_upstreams: 0,
                missing_metadata_upstreams: 0,
                header_contributing_upstreams: 0,
                api_contributing_upstreams: 0,
                max_observed_at_unix_millis: None,
                computed_at_unix_millis,
                policy_version: POOL_QUOTA_POLICY_VERSION,
            },
            oldest_observed_at_unix_millis: None,
            max_staleness_secs,
        })
        .collect::<Vec<_>>();
    for upstream in upstreams {
        let cached = view.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now,
            max_staleness_secs,
        );
        for snapshot in &mut snapshots {
            let record = &mut snapshot.record;
            let observation = provider_pool_observation(
                durable_by_window
                    .get(&(upstream.id, record.window))
                    .into_iter()
                    .flat_map(|samples| samples.iter().copied().flatten()),
                cached
                    .iter()
                    .find(|candidate| candidate.window == record.window.as_str()),
                now,
                max_staleness_secs,
            );
            let Some(observation) = observation else {
                record.missing_observation_upstreams += 1;
                continue;
            };
            if observation.absent {
                record.eligible_upstreams -= 1;
                continue;
            }
            let observed = i64::try_from(observation.observed_at_unix_millis).unwrap_or(i64::MAX);
            snapshot.oldest_observed_at_unix_millis = Some(
                snapshot
                    .oldest_observed_at_unix_millis
                    .map_or(observed, |oldest| oldest.min(observed)),
            );
            if now.saturating_sub(observation.observed_at_unix_millis)
                > max_staleness_secs.saturating_mul(1_000)
            {
                record.stale_upstreams += 1;
                continue;
            }
            let Some(utilization) = observation.utilization else {
                record.missing_observation_upstreams += 1;
                continue;
            };
            record.weighted_utilization_sum += utilization;
            record.capacity_ratio_sum += 1.0;
            record.contributing_upstreams += 1;
            if observation.api {
                record.api_contributing_upstreams += 1;
            } else {
                record.header_contributing_upstreams += 1;
            }
            record.max_observed_at_unix_millis = Some(
                record
                    .max_observed_at_unix_millis
                    .map_or(observed, |latest| latest.max(observed)),
            );
        }
    }
    for snapshot in &mut snapshots {
        let record = &mut snapshot.record;
        if record.contributing_upstreams > 0 {
            record.utilization =
                Some(record.weighted_utilization_sum / record.contributing_upstreams as f64);
        }
    }
    Ok(snapshots)
}

/// Persist one provider at the existing pool-history tick. Anthropic retains
/// its weighted aggregate; other providers share the HTTP latest builder.
pub async fn record_provider_pool_quota_snapshots_now(
    storage: &dyn Storage,
    dynamic_view: &DynamicViewHolder,
    provider: PoolQuotaProvider,
    clock: &dyn Clock,
) -> Result<(), StorageError> {
    if provider == PoolQuotaProvider::Anthropic {
        let max_staleness_secs = dynamic_view
            .load()
            .subscription_quota_routing_max_staleness_secs;
        let aggregate = build_cc_lb_aggregate_response(
            storage,
            dynamic_view,
            None,
            POOLED_HISTORY_WINDOWS.to_vec(),
            SubscriptionQuotaSourceMerge::Merged,
            max_staleness_secs,
            clock,
        )
        .await?;
        return record_pool_quota_snapshots_now(storage, &aggregate, clock).await;
    }
    let windows = provider_quota_windows(pool_provider_kind(provider))
        .iter()
        .copied()
        .filter(|window| POOLED_HISTORY_WINDOWS.contains(window))
        .collect::<Vec<_>>();
    let records =
        build_provider_pool_quota_snapshots(storage, dynamic_view, provider, &windows, clock)
            .await?
            .into_iter()
            .map(|snapshot| snapshot.record)
            .collect::<Vec<_>>();
    record_pool_quota_snapshot_records(storage, provider, &records).await
}

pub async fn record_pool_quota_snapshots_now(
    storage: &dyn Storage,
    aggregate: &AggregateResponse,
    clock: &dyn Clock,
) -> Result<(), StorageError> {
    let computed_at_unix_millis = i64::try_from(now_unix_millis(clock)).unwrap_or(i64::MAX);
    let snapshot_at_unix_secs = computed_at_unix_millis / 1_000;
    let records = pool_quota_snapshots_from_aggregate(
        aggregate,
        snapshot_at_unix_secs,
        computed_at_unix_millis,
    );
    record_pool_quota_snapshot_records(storage, PoolQuotaProvider::Anthropic, &records).await
}

async fn record_pool_quota_snapshot_records(
    storage: &dyn Storage,
    provider: PoolQuotaProvider,
    records: &[PoolQuotaSnapshotRecord],
) -> Result<(), StorageError> {
    if records.is_empty() {
        return Ok(());
    }
    let windows = records
        .iter()
        .map(|record| record.window)
        .collect::<Vec<_>>();
    let latest = storage
        .list_latest_pool_quota_snapshot_summaries(provider, &windows)
        .await?;
    let should_record = records.iter().any(|record| {
        should_record_pool_quota_snapshot(
            record,
            latest
                .iter()
                .find(|summary| summary.window == record.window),
        )
    });
    if !should_record {
        return Ok(());
    }
    PoolQuotaHistoryStore::record_pool_quota_snapshots(storage, records).await
}

fn should_record_pool_quota_snapshot(
    record: &PoolQuotaSnapshotRecord,
    latest: Option<&PoolQuotaSnapshotSummaryRecord>,
) -> bool {
    let Some(latest) = latest else {
        return true;
    };
    let utilization_unchanged = match (record.utilization, latest.utilization) {
        (Some(current), Some(previous)) => (current - previous).abs() < 0.001,
        (None, None) => true,
        _ => false,
    };
    !(record
        .snapshot_at_unix_secs
        .saturating_sub(latest.snapshot_at_unix_secs)
        < POOL_SNAPSHOT_HEARTBEAT_SECS
        && utilization_unchanged
        && record.eligible_upstreams == latest.eligible_upstreams
        && record.contributing_upstreams == latest.contributing_upstreams
        && record.stale_upstreams == latest.stale_upstreams
        && record.missing_observation_upstreams == latest.missing_observation_upstreams
        && record.missing_metadata_upstreams == latest.missing_metadata_upstreams
        && record.header_contributing_upstreams == latest.header_contributing_upstreams
        && record.api_contributing_upstreams == latest.api_contributing_upstreams
        && record.policy_version == latest.policy_version)
}

fn build_aggregate_window_response(
    window: SubscriptionQuotaWindow,
    lots: &[AggregateProviderLotResponse],
    used_tokens: u64,
    now_unix_secs: u64,
    weighting: &str,
) -> AggregateWindowResponse {
    let latest_lots = latest_lot_per_upstream(lots);
    let (cc_start, cc_reset) = window_secs(window)
        .map(|secs| cc_window_bounds(now_unix_secs, secs))
        .unwrap_or((0, 0));
    let capacity_to_now = sum_optional(lots.iter().map(|lot| lot.capacity_to_now_tokens_estimate));
    let projected_capacity = sum_optional(
        lots.iter()
            .map(|lot| lot.projected_capacity_tokens_estimate),
    );
    let weighted_utilization = capacity_to_now.and_then(|capacity| {
        if capacity > 0.0 {
            Some((used_tokens as f64 / capacity).clamp(0.0, 1.0))
        } else {
            None
        }
    });
    let equal_utilization = if weighting == "equal" {
        let values = latest_lots
            .iter()
            .filter_map(|lot| lot.utilization)
            .collect::<Vec<_>>();
        (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
    } else {
        None
    };

    let mut latest_header_per_upstream: HashMap<&str, (u64, f64, f64)> = HashMap::new();
    for lot in lots {
        let Some(utilization) = lot.utilization else {
            continue;
        };
        let observed_at = lot.observed_at_unix_millis.unwrap_or(0);
        latest_header_per_upstream
            .entry(lot.upstream_id.as_str())
            .and_modify(|(prev_ts, prev_value, prev_ratio)| {
                if observed_at >= *prev_ts {
                    *prev_ts = observed_at;
                    *prev_value = utilization;
                    *prev_ratio = lot.capacity_ratio;
                }
            })
            .or_insert((observed_at, utilization, lot.capacity_ratio));
    }
    let plan_weighted_utilization = (weighting == "plan")
        .then(|| {
            if latest_header_per_upstream.is_empty() {
                return None;
            }
            let weighted_sum: f64 = latest_header_per_upstream
                .values()
                .map(|(_, utilization, ratio)| utilization * ratio)
                .sum();
            let ratio_sum: f64 = latest_header_per_upstream
                .values()
                .map(|(_, _, ratio)| *ratio)
                .sum();
            (ratio_sum > 0.0).then(|| (weighted_sum / ratio_sum).clamp(0.0, 1.0))
        })
        .flatten();

    let utilization = equal_utilization
        .or(plan_weighted_utilization)
        .or(weighted_utilization);

    let stale_upstreams = latest_lots
        .iter()
        .filter(|lot| lot.state == "stale")
        .count();

    let missing_capacity_upstreams = latest_lots
        .iter()
        .filter(|lot| {
            if weighting == "equal" || plan_weighted_utilization.is_some() {
                lot.utilization.is_none()
            } else {
                lot.capacity_estimate_tokens.is_none()
            }
        })
        .count();
    let contributing_upstreams = if weighting == "equal" || plan_weighted_utilization.is_some() {
        latest_lots
            .iter()
            .filter(|lot| lot.utilization.is_some())
            .count()
    } else if weighted_utilization.is_some() {
        lots.iter()
            .filter(|lot| lot.capacity_to_now_tokens_estimate.unwrap_or(0.0) > 0.0)
            .map(|lot| lot.upstream_id.as_str())
            .collect::<HashSet<_>>()
            .len()
    } else {
        lots.iter()
            .filter(|lot| lot.utilization.is_some())
            .map(|lot| lot.upstream_id.as_str())
            .collect::<HashSet<_>>()
            .len()
    };

    let mut caveats = vec![CAPACITY_CAVEAT.to_owned()];
    if weighting == "equal" {
        caveats.push(
            "utilization is the arithmetic mean of available provider observations".to_owned(),
        );
    } else if plan_weighted_utilization.is_some() {
        caveats.push(PLAN_RATIO_CAVEAT.to_owned());
    } else if weighted_utilization.is_none() {
        caveats.push(HEADER_FALLBACK_CAVEAT.to_owned());
    } else if missing_capacity_upstreams > 0 {
        caveats.push(
            "some upstreams lack enough utilization/proxy-token history to infer capacity"
                .to_owned(),
        );
    }
    if stale_upstreams > 0 {
        caveats.push(STALE_DATA_CAVEAT.to_owned());
    }

    AggregateWindowResponse {
        window: window.as_str().to_owned(),
        cc_window_start_unix_secs: cc_start,
        cc_window_reset_unix_secs: cc_reset,
        used_tokens,
        utilization,
        utilization_percent: utilization.map(|value| value * 100.0),
        capacity_to_now_tokens_estimate: capacity_to_now,
        projected_capacity_tokens_estimate: projected_capacity,
        remaining_to_now_tokens_estimate: capacity_to_now
            .map(|capacity| (capacity - used_tokens as f64).max(0.0)),
        confidence: if weighting == "equal" {
            aggregate_equal_confidence(&latest_lots)
        } else {
            aggregate_confidence(lots, weighted_utilization, plan_weighted_utilization)
        },
        contributing_upstreams,
        stale_upstreams,
        missing_capacity_upstreams,
        eligible_upstreams: latest_lots.len(),
        provider_lots: latest_lots,
        caveats,
    }
}

/// Collapse a series of provider lots (one per observation) to the latest lot per upstream
/// (max observed_at_unix_millis). The aggregate response should expose pool-current state,
/// not raw history; series queries live on a separate endpoint.
fn latest_lot_per_upstream(
    lots: &[AggregateProviderLotResponse],
) -> Vec<AggregateProviderLotResponse> {
    let mut latest_idx: HashMap<&str, (u64, usize)> = HashMap::new();
    for (idx, lot) in lots.iter().enumerate() {
        let observed = lot.observed_at_unix_millis.unwrap_or(0);
        latest_idx
            .entry(lot.upstream_id.as_str())
            .and_modify(|(prev_ts, prev_idx)| {
                if observed >= *prev_ts {
                    *prev_ts = observed;
                    *prev_idx = idx;
                }
            })
            .or_insert((observed, idx));
    }
    let mut result: Vec<AggregateProviderLotResponse> = latest_idx
        .into_values()
        .map(|(_, idx)| lots[idx].clone())
        .collect();
    result.sort_by(|a, b| a.upstream_name.cmp(&b.upstream_name));
    result
}

#[derive(Clone, Copy)]
struct AggregateProviderLotInput<'a> {
    upstream_id: Uuid,
    upstream_name: &'a str,
    window: SubscriptionQuotaWindow,
    source: Option<&'a str>,
    state: SubscriptionQuotaDataState,
    provider_start: Option<u64>,
    provider_reset: Option<u64>,
    observed_at_unix_millis: Option<u64>,
    evaluation_unix_secs: u64,
    utilization: Option<f64>,
    capacity_ratio: f64,
}

fn provider_lot_input_from_snapshot<'a>(
    upstream: &'a UpstreamRecord,
    window: SubscriptionQuotaWindow,
    snapshot: &'a SubscriptionQuotaCandidateSnapshot,
    capacity_ratio: f64,
    now_unix_secs: u64,
) -> AggregateProviderLotInput<'a> {
    let cc_reset = window_secs(window).map(|secs| cc_window_bounds(now_unix_secs, secs).1);
    AggregateProviderLotInput {
        upstream_id: upstream.id,
        upstream_name: upstream.name.as_str(),
        window,
        source: snapshot.source.as_deref(),
        state: snapshot.state,
        provider_start: provider_window_start_unix_secs(window, snapshot, now_unix_secs),
        provider_reset: snapshot.resets_at_unix_secs.or(cc_reset),
        observed_at_unix_millis: snapshot.observed_at_unix_millis,
        evaluation_unix_secs: now_unix_secs,
        utilization: snapshot.utilization,
        capacity_ratio,
    }
}

fn provider_lot_input_from_sample<'a>(
    upstream: &'a UpstreamRecord,
    sample: &'a SubscriptionQuotaSample,
    max_staleness_secs: u64,
    now_unix_millis: u64,
    capacity_ratio: f64,
) -> AggregateProviderLotInput<'a> {
    let state = if now_unix_millis.saturating_sub(sample.observed_at_unix_millis)
        > max_staleness_secs.saturating_mul(1_000)
    {
        SubscriptionQuotaDataState::Stale
    } else {
        SubscriptionQuotaDataState::Fresh
    };
    AggregateProviderLotInput {
        upstream_id: upstream.id,
        upstream_name: upstream.name.as_str(),
        window: sample.window,
        source: Some(sample.source.as_str()),
        state,
        provider_start: sample
            .resets_at_unix_secs
            .and_then(|reset| window_secs(sample.window).map(|secs| reset.saturating_sub(secs))),
        provider_reset: sample.resets_at_unix_secs,
        observed_at_unix_millis: Some(sample.observed_at_unix_millis),
        evaluation_unix_secs: now_unix_millis / 1_000,
        utilization: sample.utilization,
        capacity_ratio,
    }
}
fn provider_lot_input_from_storage<'a>(
    upstream: &'a UpstreamRecord,
    lot: &'a SubscriptionQuotaProviderLot,
    capacity_ratio: f64,
) -> AggregateProviderLotInput<'a> {
    AggregateProviderLotInput {
        upstream_id: upstream.id,
        upstream_name: upstream.name.as_str(),
        window: lot.window,
        source: Some(lot.source.as_str()),
        state: SubscriptionQuotaDataState::Fresh,
        provider_start: lot.provider_start_unix_secs,
        provider_reset: lot.provider_reset_unix_secs,
        observed_at_unix_millis: Some(lot.observed_at_unix_millis),
        evaluation_unix_secs: lot.evaluation_unix_secs,
        utilization: Some(lot.utilization),
        capacity_ratio,
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct UsageTokenIntervalKey {
    upstream_id: Uuid,
    start_unix_secs: u64,
    end_unix_secs: u64,
}

fn usage_token_interval_key(interval: &UsageTokenInterval) -> UsageTokenIntervalKey {
    UsageTokenIntervalKey {
        upstream_id: interval.upstream_id,
        start_unix_secs: interval.start_unix_secs,
        end_unix_secs: interval.end_unix_secs,
    }
}

fn canonicalize_usage_token_intervals(
    intervals: &[UsageTokenInterval],
) -> (Vec<UsageTokenInterval>, HashMap<UsageTokenIntervalKey, u64>) {
    let mut canonical_intervals = Vec::with_capacity(intervals.len());
    let mut canonical_ids_by_key = HashMap::with_capacity(intervals.len());
    for interval in intervals {
        let key = usage_token_interval_key(interval);
        if let std::collections::hash_map::Entry::Vacant(entry) = canonical_ids_by_key.entry(key) {
            canonical_intervals.push(*interval);
            entry.insert(interval.interval_id);
        }
    }
    (canonical_intervals, canonical_ids_by_key)
}

fn scatter_usage_token_sums(
    intervals: &[UsageTokenInterval],
    canonical_ids_by_key: &HashMap<UsageTokenIntervalKey, u64>,
    canonical_token_sums: &HashMap<u64, u64>,
) -> HashMap<u64, u64> {
    let mut token_sums = HashMap::with_capacity(intervals.len());
    for interval in intervals {
        let tokens = canonical_ids_by_key
            .get(&usage_token_interval_key(interval))
            .and_then(|canonical_id| canonical_token_sums.get(canonical_id))
            .copied()
            .unwrap_or(0);
        token_sums.insert(interval.interval_id, tokens);
    }
    token_sums
}

#[derive(Clone, Copy)]
struct AggregateProviderLotIntervalIds {
    provider_tokens: Option<u64>,
    used_before_cc_window: Option<u64>,
}

#[derive(Clone, Copy)]
struct ProviderLotTokenSums {
    provider_tokens: u64,
    used_before_cc_window: u64,
}

fn build_provider_lot_response(
    input: &AggregateProviderLotInput<'_>,
    token_sums: ProviderLotTokenSums,
    now_unix_secs: u64,
) -> AggregateProviderLotResponse {
    let window = input.window;
    let (cc_start, cc_reset) = window_secs(window)
        .map(|secs| cc_window_bounds(now_unix_secs, secs))
        .unwrap_or((0, 0));
    let provider_start = input.provider_start;
    let provider_reset = input
        .provider_reset
        .or_else(|| (cc_reset > 0).then_some(cc_reset));
    let provider_tokens = token_sums.provider_tokens;
    let capacity_estimate = match (input.utilization, provider_tokens) {
        (Some(utilization), tokens) if utilization > 0.0 && tokens > 0 => {
            Some(tokens as f64 / utilization)
        }
        _ => None,
    };
    let used_before_cc_window = token_sums.used_before_cc_window;
    let capacity_to_now = match (provider_start, provider_reset, capacity_estimate) {
        (Some(start), Some(reset), Some(capacity)) => Some(capacity_contribution_for_prefix(
            start,
            reset,
            capacity,
            used_before_cc_window as f64,
            cc_start,
            cc_reset,
            now_unix_secs,
        )),
        _ => None,
    };
    let projected_capacity = match (provider_start, provider_reset, capacity_estimate) {
        (Some(start), Some(reset), Some(capacity)) => Some(capacity_contribution_for_prefix(
            start,
            reset,
            capacity,
            used_before_cc_window as f64,
            cc_start,
            cc_reset,
            cc_reset,
        )),
        _ => None,
    };
    let confidence = if capacity_estimate.is_none() {
        "low"
    } else if input.state == SubscriptionQuotaDataState::Fresh {
        "estimated"
    } else {
        "stale"
    };

    AggregateProviderLotResponse {
        upstream_id: input.upstream_id.to_string(),
        upstream_name: input.upstream_name.to_owned(),
        window: window.as_str().to_owned(),
        source: input.source.map(str::to_owned),
        state: data_state_str(input.state).to_owned(),
        provider_start_unix_secs: provider_start,
        provider_reset_unix_secs: provider_reset,
        observed_at_unix_millis: input.observed_at_unix_millis,
        utilization: input.utilization,
        capacity_estimate_tokens: capacity_estimate,
        used_before_cc_window_tokens: used_before_cc_window,
        capacity_to_now_tokens_estimate: capacity_to_now,
        projected_capacity_tokens_estimate: projected_capacity,
        confidence: confidence.to_owned(),
        capacity_ratio: input.capacity_ratio,
    }
}

fn oauth_usage_from_aggregate(aggregate: &AggregateResponse) -> CcLbOAuthUsageResponse {
    let mut response = CcLbOAuthUsageResponse {
        five_hour: None,
        seven_day: None,
        seven_day_sonnet: None,
        seven_day_opus: None,
        limits: None,
        extra_usage: None,
    };
    for window in &aggregate.windows {
        let usage = CcLbOAuthWindowUsage {
            utilization: window.utilization_percent,
            resets_at: Some(window.cc_window_reset_unix_secs),
        };
        match window.window.as_str() {
            "5h" => response.five_hour = Some(usage),
            "7d" => response.seven_day = Some(usage),
            "7d_sonnet" => response.seven_day_sonnet = Some(usage),
            "7d_opus" => response.seven_day_opus = Some(usage),
            "7d_fable" => {
                response.limits = Some(vec![CcLbOAuthUsageLimit {
                    kind: "weekly_scoped".to_owned(),
                    percent: window.utilization_percent,
                    resets_at: window.cc_window_reset_unix_secs,
                    scope: CcLbOAuthUsageLimitScope {
                        model: CcLbOAuthUsageLimitModel {
                            display_name: "Fable".to_owned(),
                        },
                    },
                    is_active: true,
                }]);
            }
            _ => {}
        }
    }
    response
}

async fn build_cc_lb_oauth_extra_usage(
    storage: &dyn Storage,
    dynamic_view: &DynamicViewHolder,
    max_staleness_secs: u64,
    clock: &dyn Clock,
) -> Result<Option<CcLbOAuthExtraUsage>, StorageError> {
    let upstreams = list_all_upstreams_storage(storage)
        .await?
        .into_iter()
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| upstream.kind == UpstreamKind::AnthropicOauth)
        .collect::<Vec<_>>();
    let now_unix_millis = now_unix_millis(clock);
    let dynamic_view = dynamic_view.load();
    let mut enabled = None;
    let mut monthly_limit = 0.0;
    let mut saw_monthly_limit = false;
    let mut used_credits = 0.0;
    let mut saw_used_credits = false;

    for upstream in upstreams {
        for snapshot in dynamic_view.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis,
            max_staleness_secs,
        ) {
            if SubscriptionQuotaWindow::from_str(&snapshot.window)
                != Some(SubscriptionQuotaWindow::Overage)
            {
                continue;
            }
            if let Some(value) = snapshot.extra_usage_enabled {
                enabled = Some(enabled.unwrap_or(false) || value);
            }
            if let Some(value) = snapshot.extra_usage_monthly_limit {
                monthly_limit += value;
                saw_monthly_limit = true;
            }
            if let Some(value) = snapshot.extra_usage_used_credits {
                used_credits += value;
                saw_used_credits = true;
            }
        }
    }

    if enabled.is_none() && !saw_monthly_limit && !saw_used_credits {
        return Ok(None);
    }
    Ok(Some(CcLbOAuthExtraUsage {
        enabled,
        monthly_limit: saw_monthly_limit.then_some(monthly_limit),
        used_credits: saw_used_credits.then_some(used_credits),
    }))
}

fn capacity_contribution_for_prefix(
    provider_start: u64,
    provider_reset: u64,
    limit_estimate: f64,
    consumed_before_entry: f64,
    cc_start: u64,
    cc_reset: u64,
    prefix_end: u64,
) -> f64 {
    if provider_start >= cc_reset || provider_reset <= cc_start {
        return 0.0;
    }
    let entry = provider_start.max(cc_start);
    let exit = provider_reset.min(cc_reset).min(prefix_end);
    if entry >= exit {
        return 0.0;
    }
    (limit_estimate - consumed_before_entry).max(0.0)
}

fn provider_window_start_unix_secs(
    window: SubscriptionQuotaWindow,
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    now_unix_secs: u64,
) -> Option<u64> {
    let secs = window_secs(window)?;
    Some(
        snapshot
            .resets_at_unix_secs
            .map(|reset| reset.saturating_sub(secs))
            .unwrap_or_else(|| cc_window_bounds(now_unix_secs, secs).0),
    )
}

fn cc_window_bounds(now_unix_secs: u64, window_secs: u64) -> (u64, u64) {
    let start = now_unix_secs - (now_unix_secs % window_secs);
    (start, start.saturating_add(window_secs))
}

fn window_secs(window: SubscriptionQuotaWindow) -> Option<u64> {
    match window {
        SubscriptionQuotaWindow::FiveHour => Some(5 * 3_600),
        SubscriptionQuotaWindow::SevenDay
        | SubscriptionQuotaWindow::SevenDaySonnet
        | SubscriptionQuotaWindow::SevenDayOpus
        | SubscriptionQuotaWindow::SevenDayFable => Some(7 * 24 * 3_600),
        SubscriptionQuotaWindow::GeminiFiveHour | SubscriptionQuotaWindow::ThirdPartyFiveHour => {
            Some(5 * 3_600)
        }
        SubscriptionQuotaWindow::GeminiSevenDay | SubscriptionQuotaWindow::ThirdPartySevenDay => {
            Some(7 * 24 * 3_600)
        }
        SubscriptionQuotaWindow::Overage | SubscriptionQuotaWindow::Unified => None,
    }
}

fn sum_optional(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let mut saw_value = false;
    let total = values.fold(0.0, |acc, value| match value {
        Some(value) => {
            saw_value = true;
            acc + value
        }
        None => acc,
    });
    saw_value.then_some(total)
}

fn capacity_ratios_by_upstream(
    subscription_metadata: &[UpstreamSubscriptionMetadataRecord],
    organization_metadata: &[OrganizationMetadataRecord],
) -> HashMap<Uuid, f64> {
    let organizations = organization_metadata
        .iter()
        .map(|record| (record.organization_uuid.as_str(), record))
        .collect::<HashMap<_, _>>();
    subscription_metadata
        .iter()
        .filter_map(|record| {
            let organization = record
                .organization_uuid
                .as_deref()
                .and_then(|organization_uuid| organizations.get(organization_uuid).copied())?;
            Some((
                record.upstream_id,
                plan_capacity_ratio(
                    organization.organization_type.as_deref(),
                    organization.rate_limit_tier.as_deref(),
                    organization.seat_tier.as_deref(),
                ),
            ))
        })
        .collect()
}

fn aggregate_confidence(
    lots: &[AggregateProviderLotResponse],
    weighted_utilization: Option<f64>,
    plan_weighted_utilization: Option<f64>,
) -> String {
    if lots.is_empty() {
        return "low".to_owned();
    }
    if lots.iter().any(|lot| lot.state == "stale") {
        return "stale".to_owned();
    }
    if plan_weighted_utilization.is_some() {
        return "plan_weighted".to_owned();
    }
    if weighted_utilization.is_some() {
        if lots
            .iter()
            .any(|lot| lot.capacity_estimate_tokens.is_none())
        {
            return "partial".to_owned();
        }
        return "estimated".to_owned();
    }
    "low".to_owned()
}
fn aggregate_equal_confidence(lots: &[AggregateProviderLotResponse]) -> String {
    if lots.is_empty() {
        return "low".to_owned();
    }
    if lots.iter().any(|lot| lot.state == "stale") {
        return "stale".to_owned();
    }
    if lots.iter().any(|lot| lot.utilization.is_none()) {
        return "partial".to_owned();
    }
    "observed".to_owned()
}

fn build_markers(
    buckets: &[SubscriptionQuotaBucket],
    bucket_secs: u64,
) -> Vec<SeriesMarkerResponse> {
    let observed = buckets
        .iter()
        .filter(|bucket| bucket.observed)
        .collect::<Vec<_>>();
    let mut markers = Vec::new();
    for pair in observed.windows(2) {
        let [previous, current] = pair else {
            continue;
        };
        if previous.resets_at_unix_secs_last != current.resets_at_unix_secs_last
            && let Some(reset) = previous.resets_at_unix_secs_last
        {
            markers.push(SeriesMarkerResponse {
                kind: "reset".to_owned(),
                at_unix_secs: Some(reset),
                from_unix_secs: None,
                to_unix_secs: None,
            });
        }
        if let (Some(previous_utilization), Some(current_utilization)) =
            (previous.utilization_last, current.utilization_last)
            && previous_utilization - current_utilization >= RESET_DROP_THRESHOLD
        {
            markers.push(SeriesMarkerResponse {
                kind: "reset".to_owned(),
                at_unix_secs: Some(current.bucket_start_unix_secs),
                from_unix_secs: None,
                to_unix_secs: None,
            });
        }
        let expected_next = previous.bucket_start_unix_secs.saturating_add(bucket_secs);
        if current
            .bucket_start_unix_secs
            .saturating_sub(previous.bucket_start_unix_secs)
            > GAP_MARKER_MULTIPLIER.saturating_mul(bucket_secs)
        {
            markers.push(SeriesMarkerResponse {
                kind: "gap".to_owned(),
                at_unix_secs: None,
                from_unix_secs: Some(expected_next),
                to_unix_secs: Some(current.bucket_start_unix_secs),
            });
        }
    }
    markers
}

async fn upstreams_for_optional_query(
    storage: &dyn Storage,
    raw: Option<&str>,
) -> Result<Vec<UpstreamRecord>, Response> {
    let requested_ids = match raw {
        Some(value) if !value.trim().is_empty() => Some(parse_upstream_ids(value)?),
        _ => None,
    };
    let requested = requested_ids.map(|ids| ids.into_iter().collect::<HashSet<_>>());
    Ok(list_all_upstreams(storage)
        .await?
        .into_iter()
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| !provider_quota_windows(upstream.kind).is_empty())
        .filter(|upstream| {
            requested
                .as_ref()
                .is_none_or(|ids| ids.contains(&upstream.id))
        })
        .collect())
}

fn provider_quota_windows(kind: UpstreamKind) -> &'static [SubscriptionQuotaWindow] {
    match kind {
        UpstreamKind::AnthropicOauth => &[
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::SevenDaySonnet,
            SubscriptionQuotaWindow::SevenDayOpus,
            SubscriptionQuotaWindow::SevenDayFable,
            SubscriptionQuotaWindow::Overage,
            SubscriptionQuotaWindow::Unified,
        ],
        UpstreamKind::OpenaiOauth => &[
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::Overage,
            SubscriptionQuotaWindow::Unified,
        ],
        UpstreamKind::GeminiOauth => &[
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaWindow::GeminiSevenDay,
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            SubscriptionQuotaWindow::ThirdPartySevenDay,
        ],
        _ => &[],
    }
}

fn provider_supports_window(kind: UpstreamKind, window: &str) -> bool {
    SubscriptionQuotaWindow::from_str(window)
        .is_some_and(|window| provider_quota_windows(kind).contains(&window))
}

fn pool_quota_upstreams(
    upstreams: Vec<UpstreamRecord>,
    requested: Option<&HashSet<Uuid>>,
    provider_kind: UpstreamKind,
) -> Vec<UpstreamRecord> {
    upstreams
        .into_iter()
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| upstream.kind == provider_kind)
        .filter(|upstream| provider_kind == UpstreamKind::AnthropicOauth || upstream.enabled)
        .filter(|upstream| {
            requested
                .map(|ids| ids.contains(&upstream.id))
                .unwrap_or(true)
        })
        .collect()
}

async fn list_all_upstreams(storage: &dyn Storage) -> Result<Vec<UpstreamRecord>, Response> {
    list_all_upstreams_storage(storage)
        .await
        .map_err(storage_error)
}

async fn list_all_upstreams_storage(
    storage: &dyn Storage,
) -> Result<Vec<UpstreamRecord>, StorageError> {
    let mut after = None;
    let mut all = Vec::new();
    loop {
        let page = UpstreamStore::list(storage, after, STORE_PAGE_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        let page_len = page.len();
        all.extend(page);
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    Ok(all)
}

/// Successful provider enumeration is authoritative even after it ages out;
/// missing headers never create an absence. Use the cache's fresh-first merge
/// ordering for durable fallback, with API winning equal-timestamp ties.
pub(crate) fn five_hour_is_absent<'a>(
    samples: impl Iterator<Item = &'a SubscriptionQuotaSample>,
    snapshot: Option<&SubscriptionQuotaCandidateSnapshot>,
    now_unix_millis: u64,
    max_staleness_secs: u64,
) -> bool {
    let rank = |observed_at: u64, api: bool| {
        (
            now_unix_millis.saturating_sub(observed_at) <= max_staleness_secs.saturating_mul(1_000),
            observed_at,
            api,
        )
    };
    let durable = samples
        .filter(|sample| sample.window == SubscriptionQuotaWindow::FiveHour)
        .max_by_key(|sample| {
            rank(
                sample.observed_at_unix_millis,
                sample.source == SubscriptionQuotaSource::Api,
            )
        });
    let cached = snapshot.filter(|snapshot| {
        snapshot.state != SubscriptionQuotaDataState::Unobserved
            && snapshot.observed_at_unix_millis.is_some()
    });
    if let Some(cached) = cached
        && durable.is_none_or(|sample| {
            rank(
                cached.observed_at_unix_millis.unwrap_or(0),
                cached.source.as_deref() == Some("api"),
            ) >= rank(
                sample.observed_at_unix_millis,
                sample.source == SubscriptionQuotaSource::Api,
            )
        })
    {
        return cached.state == SubscriptionQuotaDataState::Absent;
    }
    durable.is_some_and(|sample| sample.sample_kind == SubscriptionQuotaSampleKind::Absent)
}

fn latest_window_response(
    snapshot: SubscriptionQuotaCandidateSnapshot,
    now_unix_millis: u64,
) -> LatestWindowResponse {
    let age_secs = snapshot
        .observed_at_unix_millis
        .map(|observed| now_unix_millis.saturating_sub(observed) / 1_000);
    LatestWindowResponse {
        window: snapshot.window,
        state: data_state_str(snapshot.state).to_owned(),
        source: snapshot.source,
        utilization: snapshot.utilization,
        status: snapshot.status,
        resets_at_unix_secs: snapshot.resets_at_unix_secs,
        surpassed_threshold: snapshot.surpassed_threshold,
        representative_claim: snapshot.representative_claim,
        disabled_reason: snapshot.disabled_reason,
        extra_usage_enabled: snapshot.extra_usage_enabled,
        extra_usage_monthly_limit: snapshot.extra_usage_monthly_limit,
        extra_usage_used_credits: snapshot.extra_usage_used_credits,
        credits: snapshot.credits,
        observed_at_unix_millis: snapshot.observed_at_unix_millis,
        age_secs,
        fallback_available: snapshot.fallback_available,
        overage_in_use: snapshot.overage_in_use,
        overage_period_monthly_utilization: snapshot.overage_period_monthly_utilization,
        upgrade_paths: snapshot.upgrade_paths,
    }
}

fn series_bucket_response(bucket: &SubscriptionQuotaBucket) -> SeriesBucketResponse {
    SeriesBucketResponse {
        bucket_start_unix_secs: bucket.bucket_start_unix_secs,
        utilization_last: bucket.utilization_last,
    }
}

fn snapshot_matches_source(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    source: SubscriptionQuotaSourceMerge,
) -> bool {
    match source {
        SubscriptionQuotaSourceMerge::Merged => true,
        SubscriptionQuotaSourceMerge::Header => snapshot.source.as_deref() == Some("header"),
        SubscriptionQuotaSourceMerge::Api => snapshot.source.as_deref() == Some("api"),
    }
}

fn parse_windows_or_all(raw: Option<&str>) -> Result<Vec<SubscriptionQuotaWindow>, Response> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => parse_windows(value),
        None => Ok(SubscriptionQuotaWindow::all().to_vec()),
    }
}

fn parse_windows_or_default(raw: Option<&str>) -> Result<Vec<SubscriptionQuotaWindow>, Response> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => parse_windows(value),
        None => Ok(vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ]),
    }
}
fn parse_windows_or_provider_default(
    raw: Option<&str>,
    provider: PoolQuotaProvider,
) -> Result<Vec<SubscriptionQuotaWindow>, Response> {
    let defaults = match provider {
        PoolQuotaProvider::Anthropic => vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
            SubscriptionQuotaWindow::SevenDaySonnet,
            SubscriptionQuotaWindow::SevenDayOpus,
            SubscriptionQuotaWindow::SevenDayFable,
        ],
        PoolQuotaProvider::Openai => vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ],
        PoolQuotaProvider::Gemini => vec![
            SubscriptionQuotaWindow::GeminiFiveHour,
            SubscriptionQuotaWindow::GeminiSevenDay,
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            SubscriptionQuotaWindow::ThirdPartySevenDay,
        ],
    };
    let windows = match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => parse_windows(value)?,
        None => defaults,
    };
    if windows
        .iter()
        .any(|window| !provider_quota_windows(pool_provider_kind(provider)).contains(window))
    {
        return Err(bad_request(
            "unsupported_pool_window",
            "window is not supported for the selected provider",
        ));
    }
    Ok(windows)
}

fn parse_windows(raw: &str) -> Result<Vec<SubscriptionQuotaWindow>, Response> {
    let mut windows = Vec::new();
    for item in split_csv(raw) {
        let window = SubscriptionQuotaWindow::from_str(item)
            .ok_or_else(|| bad_request("invalid_window", format!("unsupported window: {item}")))?;
        windows.push(window);
    }
    if windows.is_empty() {
        return Err(bad_request("invalid_window", "no windows provided"));
    }
    Ok(windows)
}

fn parse_source_merge(raw: Option<&str>) -> Result<SubscriptionQuotaSourceMerge, Response> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => SubscriptionQuotaSourceMerge::from_str(value)
            .ok_or_else(|| bad_request("invalid_source", format!("unsupported source: {value}"))),
        None => Ok(SubscriptionQuotaSourceMerge::Merged),
    }
}

fn parse_upstream_ids(raw: &str) -> Result<Vec<Uuid>, Response> {
    let mut ids = Vec::new();
    for item in split_csv(raw) {
        let id = item
            .parse::<Uuid>()
            .map_err(|error| bad_request("invalid_upstream_id", error.to_string()))?;
        ids.push(id);
    }
    if ids.is_empty() {
        return Err(bad_request(
            "invalid_upstream_ids",
            "no upstream ids provided",
        ));
    }
    Ok(ids)
}

fn split_csv(raw: &str) -> impl Iterator<Item = &str> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn validate_time_range(since_unix_secs: u64, until_unix_secs: u64) -> Result<(), Response> {
    if until_unix_secs <= since_unix_secs {
        return Err(bad_request(
            "invalid_time_range",
            "until_unix_secs must be greater than since_unix_secs",
        ));
    }
    Ok(())
}

fn validate_series_guardrails(
    since_unix_secs: u64,
    until_unix_secs: u64,
    bucket_secs: u64,
    max_points_per_series: u32,
    bucket_range_detail: &'static str,
) -> Result<(), Response> {
    if max_points_per_series > MAX_SERIES_MAX_POINTS {
        return Err(bad_request(
            "max_points_per_series_too_large",
            "max_points_per_series must be <= 10000",
        ));
    }
    let bucket_count = until_unix_secs
        .saturating_sub(since_unix_secs)
        .div_ceil(bucket_secs);
    if bucket_count > u64::from(max_points_per_series).saturating_mul(2) {
        return Err(bad_request("bucket_range_too_large", bucket_range_detail));
    }
    Ok(())
}

fn validate_upstream_count(count: usize) -> Result<(), Response> {
    if count > MAX_SERIES_UPSTREAMS {
        return Err(bad_request(
            "too_many_upstreams",
            "upstream_ids must contain at most 50 ids",
        ));
    }
    Ok(())
}

fn storage(state: &AdminState) -> Result<&dyn Storage, Response> {
    state
        .storage
        .as_deref()
        .ok_or_else(|| service_unavailable("storage_unavailable"))
}

fn sources_for_merge(source: SubscriptionQuotaSourceMerge) -> Vec<SubscriptionQuotaSource> {
    match source {
        SubscriptionQuotaSourceMerge::Header => vec![SubscriptionQuotaSource::Header],
        SubscriptionQuotaSourceMerge::Api => vec![SubscriptionQuotaSource::Api],
        SubscriptionQuotaSourceMerge::Merged => vec![
            SubscriptionQuotaSource::Header,
            SubscriptionQuotaSource::Api,
        ],
    }
}

fn upstream_name_map(upstreams: &[UpstreamRecord]) -> HashMap<Uuid, String> {
    upstreams
        .iter()
        .map(|upstream| (upstream.id, upstream.name.clone()))
        .collect()
}

fn data_state_str(state: SubscriptionQuotaDataState) -> &'static str {
    match state {
        SubscriptionQuotaDataState::Fresh => "fresh",
        SubscriptionQuotaDataState::Stale => "stale",
        SubscriptionQuotaDataState::Absent => "absent",
        SubscriptionQuotaDataState::Unobserved => "unobserved",
    }
}

fn storage_error(error: StorageError) -> Response {
    tracing::error!(%error, "subscription quota admin storage operation failed");
    internal_error("storage_error")
}

fn bad_request(error: &str, detail: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": error, "detail": detail.into() })),
    )
        .into_response()
}

fn service_unavailable(error: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": error })),
    )
        .into_response()
}

fn internal_error(error: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": error })),
    )
        .into_response()
}

fn now_unix_millis(clock: &dyn Clock) -> u64 {
    cc_lb_clock::unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_windows_are_provider_scoped() {
        for window in SubscriptionQuotaWindow::all() {
            let id = window.as_str();
            let gemini = matches!(
                window,
                SubscriptionQuotaWindow::GeminiFiveHour
                    | SubscriptionQuotaWindow::GeminiSevenDay
                    | SubscriptionQuotaWindow::ThirdPartyFiveHour
                    | SubscriptionQuotaWindow::ThirdPartySevenDay
            );
            assert_eq!(
                provider_supports_window(UpstreamKind::AnthropicOauth, id),
                !gemini
            );
            assert_eq!(
                provider_supports_window(UpstreamKind::GeminiOauth, id),
                gemini
            );
            assert_eq!(
                provider_supports_window(UpstreamKind::OpenaiOauth, id),
                matches!(
                    window,
                    SubscriptionQuotaWindow::FiveHour
                        | SubscriptionQuotaWindow::SevenDay
                        | SubscriptionQuotaWindow::Overage
                        | SubscriptionQuotaWindow::Unified
                )
            );
            assert!(!provider_supports_window(UpstreamKind::GeminiApiKey, id));
        }
    }

    #[test]
    fn usage_token_intervals_deduplicate_exact_full_keys_and_scatter() {
        let upstream_a = Uuid::from_u128(1);
        let upstream_b = Uuid::from_u128(2);
        let intervals = vec![
            UsageTokenInterval {
                interval_id: 10,
                upstream_id: upstream_a,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 11,
                upstream_id: upstream_a,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 12,
                upstream_id: upstream_a,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 13,
                upstream_id: upstream_a,
                start_unix_secs: 100,
                end_unix_secs: 201,
            },
            UsageTokenInterval {
                interval_id: 14,
                upstream_id: upstream_b,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 15,
                upstream_id: upstream_a,
                start_unix_secs: 99,
                end_unix_secs: 200,
            },
        ];

        let (canonical, canonical_ids_by_key) = canonicalize_usage_token_intervals(&intervals);
        assert_eq!(
            canonical
                .iter()
                .map(|interval| interval.interval_id)
                .collect::<Vec<_>>(),
            vec![10, 13, 14, 15]
        );

        let canonical_sums = vec![(15, 3), (14, 7), (13, 11), (10, 42)]
            .into_iter()
            .collect::<HashMap<_, _>>();
        let scattered =
            scatter_usage_token_sums(&intervals, &canonical_ids_by_key, &canonical_sums);

        assert_eq!(scattered.len(), intervals.len());
        assert_eq!(scattered.get(&10), Some(&42));
        assert_eq!(scattered.get(&11), Some(&42));
        assert_eq!(scattered.get(&12), Some(&42));
        assert_eq!(scattered.get(&13), Some(&11));
        assert_eq!(scattered.get(&14), Some(&7));
        assert_eq!(scattered.get(&15), Some(&3));
    }

    #[test]
    fn usage_token_intervals_scatter_zero_for_no_match_and_empty_input() {
        let upstream_id = Uuid::from_u128(3);
        let intervals = vec![
            UsageTokenInterval {
                interval_id: 20,
                upstream_id,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 21,
                upstream_id,
                start_unix_secs: 100,
                end_unix_secs: 200,
            },
            UsageTokenInterval {
                interval_id: 22,
                upstream_id,
                start_unix_secs: 100,
                end_unix_secs: 201,
            },
        ];
        let (canonical, canonical_ids_by_key) = canonicalize_usage_token_intervals(&intervals);
        let scattered =
            scatter_usage_token_sums(&intervals, &canonical_ids_by_key, &HashMap::new());

        assert_eq!(canonical.len(), 2);
        assert_eq!(
            scattered,
            vec![(20, 0), (21, 0), (22, 0)]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );

        let (canonical, canonical_ids_by_key) = canonicalize_usage_token_intervals(&[]);
        assert!(canonical.is_empty());
        assert!(scatter_usage_token_sums(&[], &canonical_ids_by_key, &HashMap::new()).is_empty());
    }

    fn snapshot_for_deadband() -> PoolQuotaSnapshotRecord {
        PoolQuotaSnapshotRecord {
            provider: PoolQuotaProvider::Anthropic,
            snapshot_at_unix_secs: 10_000,
            window: SubscriptionQuotaWindow::FiveHour,
            utilization: Some(0.0),
            weighted_utilization_sum: 0.0,
            capacity_ratio_sum: 1.0,
            eligible_upstreams: 2,
            contributing_upstreams: 1,
            stale_upstreams: 0,
            missing_observation_upstreams: 1,
            missing_metadata_upstreams: 0,
            header_contributing_upstreams: 1,
            api_contributing_upstreams: 0,
            max_observed_at_unix_millis: Some(10_000_000),
            computed_at_unix_millis: 10_000_000,
            policy_version: POOL_QUOTA_POLICY_VERSION,
        }
    }

    #[test]
    fn pool_snapshot_deadband_keeps_first_sample_changes_and_hourly_heartbeat() {
        let mut record = snapshot_for_deadband();
        assert!(should_record_pool_quota_snapshot(&record, None));
        let latest = PoolQuotaSnapshotSummaryRecord::from(&record);
        assert!(!should_record_pool_quota_snapshot(&record, Some(&latest)));
        record.snapshot_at_unix_secs += 3_599;
        record.utilization = Some(0.000_999);
        assert!(!should_record_pool_quota_snapshot(&record, Some(&latest)));
        record.utilization = Some(0.001);
        assert!(should_record_pool_quota_snapshot(&record, Some(&latest)));
        record.utilization = Some(0.0);
        record.snapshot_at_unix_secs += 1;
        assert!(should_record_pool_quota_snapshot(&record, Some(&latest)));
    }

    #[test]
    fn pool_snapshot_deadband_preserves_missing_utilization_transitions() {
        let mut record = snapshot_for_deadband();
        let mut latest = PoolQuotaSnapshotSummaryRecord::from(&record);
        record.utilization = None;
        assert!(should_record_pool_quota_snapshot(&record, Some(&latest)));
        latest.utilization = None;
        assert!(!should_record_pool_quota_snapshot(&record, Some(&latest)));
        record.utilization = Some(0.0);
        assert!(should_record_pool_quota_snapshot(&record, Some(&latest)));
    }

    #[test]
    fn pool_snapshot_deadband_preserves_all_population_and_policy_changes() {
        let original = snapshot_for_deadband();
        let latest = PoolQuotaSnapshotSummaryRecord::from(&original);
        let changes: [fn(&mut PoolQuotaSnapshotRecord); 8] = [
            |record| record.eligible_upstreams += 1,
            |record| record.contributing_upstreams += 1,
            |record| record.stale_upstreams += 1,
            |record| record.missing_observation_upstreams += 1,
            |record| record.missing_metadata_upstreams += 1,
            |record| record.header_contributing_upstreams += 1,
            |record| record.api_contributing_upstreams += 1,
            |record| record.policy_version += 1,
        ];
        for change in changes {
            let mut record = original.clone();
            change(&mut record);
            assert!(should_record_pool_quota_snapshot(&record, Some(&latest)));
        }
        let mut record = original;
        record.max_observed_at_unix_millis = Some(10_001_000);
        record.computed_at_unix_millis = 10_001_000;
        record.weighted_utilization_sum = 0.000_01;
        assert!(!should_record_pool_quota_snapshot(&record, Some(&latest)));
    }

    #[test]
    fn quota_data_states_preserve_absent_and_unobserved() {
        assert_eq!(data_state_str(SubscriptionQuotaDataState::Absent), "absent");
        assert_eq!(
            data_state_str(SubscriptionQuotaDataState::Unobserved),
            "unobserved"
        );
    }

    #[test]
    fn prefix_capacity_does_not_borrow_future_provider_lot() {
        let cc_start = 10_000;
        let cc_reset = cc_start + 5 * 3_600;
        let before_future_reset = cc_start + 3_600;
        let after_future_reset = cc_start + 3 * 3_600;

        let active_capacity = capacity_contribution_for_prefix(
            cc_start - 3_600,
            cc_start + 4 * 3_600,
            100.0,
            30.0,
            cc_start,
            cc_reset,
            before_future_reset,
        );
        let future_capacity = capacity_contribution_for_prefix(
            cc_start + 2 * 3_600,
            cc_start + 7 * 3_600,
            100.0,
            0.0,
            cc_start,
            cc_reset,
            before_future_reset,
        );
        let future_after_start = capacity_contribution_for_prefix(
            cc_start + 2 * 3_600,
            cc_start + 7 * 3_600,
            100.0,
            0.0,
            cc_start,
            cc_reset,
            after_future_reset,
        );

        assert_eq!(active_capacity, 70.0);
        assert_eq!(future_capacity, 0.0);
        assert_eq!(future_after_start, 100.0);
    }

    #[test]
    fn aggregate_window_counts_historical_provider_lots_after_reset() {
        let upstream_id = Uuid::new_v4();
        let upstream = upstream_record(upstream_id);
        let inputs = [
            AggregateProviderLotInput {
                upstream_id,
                upstream_name: upstream.name.as_str(),
                window: SubscriptionQuotaWindow::FiveHour,
                source: Some("merged"),
                state: SubscriptionQuotaDataState::Fresh,
                provider_start: Some(27_000),
                provider_reset: Some(45_000),
                observed_at_unix_millis: Some(44_000_000),
                evaluation_unix_secs: 44_000,
                utilization: Some(0.5),
                capacity_ratio: PRO_CAPACITY_RATIO,
            },
            AggregateProviderLotInput {
                upstream_id,
                upstream_name: upstream.name.as_str(),
                window: SubscriptionQuotaWindow::FiveHour,
                source: Some("merged"),
                state: SubscriptionQuotaDataState::Fresh,
                provider_start: Some(45_000),
                provider_reset: Some(63_000),
                observed_at_unix_millis: Some(50_000_000),
                evaluation_unix_secs: 50_000,
                utilization: Some(0.5),
                capacity_ratio: PRO_CAPACITY_RATIO,
            },
        ];
        let lots = inputs
            .iter()
            .zip([
                ProviderLotTokenSums {
                    provider_tokens: 50,
                    used_before_cc_window: 0,
                },
                ProviderLotTokenSums {
                    provider_tokens: 100,
                    used_before_cc_window: 0,
                },
            ])
            .map(|(input, sums)| build_provider_lot_response(input, sums, 50_000))
            .collect::<Vec<_>>();

        let response = build_aggregate_window_response(
            SubscriptionQuotaWindow::FiveHour,
            &lots,
            150,
            50_000,
            "plan",
        );

        assert_eq!(lots.len(), 2);
        assert_eq!(response.used_tokens, 150);
        assert_eq!(response.capacity_to_now_tokens_estimate, Some(300.0));
        assert_eq!(response.utilization_percent, Some(50.0));
    }

    #[test]
    fn pool_quota_upstreams_includes_disabled_oauth_upstreams() {
        let enabled_oauth_id = Uuid::new_v4();
        let disabled_oauth_id = Uuid::new_v4();
        let non_oauth_id = Uuid::new_v4();
        let mut disabled_oauth = upstream_record(disabled_oauth_id);
        disabled_oauth.enabled = false;
        let mut non_oauth = upstream_record(non_oauth_id);
        non_oauth.kind = UpstreamKind::AnthropicApiKey;

        let upstreams = pool_quota_upstreams(
            vec![upstream_record(enabled_oauth_id), disabled_oauth, non_oauth],
            None,
            UpstreamKind::AnthropicOauth,
        );

        let ids: HashSet<Uuid> = upstreams.iter().map(|upstream| upstream.id).collect();
        assert_eq!(upstreams.len(), 2);
        assert!(ids.contains(&enabled_oauth_id));
        assert!(ids.contains(&disabled_oauth_id));
        assert!(!ids.contains(&non_oauth_id));
    }

    #[test]
    fn aggregate_uses_plan_ratio_weighted_latest_observations() {
        let lots = vec![
            lot_with_ratio("team-standard", Some(0.50), Some(1_000), None, None, 1.25),
            lot_with_ratio("team-premium", Some(0.10), Some(1_000), None, None, 6.25),
        ];
        let response = build_aggregate_window_response(
            SubscriptionQuotaWindow::FiveHour,
            &lots,
            0,
            now_unix_secs_test(),
            "plan",
        );

        let utilization = response
            .utilization
            .expect("plan-ratio fallback should populate utilization");
        assert!((utilization - (1.25 / 7.5)).abs() < f64::EPSILON);
        assert_eq!(response.confidence, "plan_weighted");
        assert_eq!(response.contributing_upstreams, 2);
        assert!(response.caveats.iter().any(|c| c.contains("plan ratios")));
    }

    #[test]
    fn pool_quota_snapshots_from_aggregate_includes_fable() {
        let aggregate = AggregateResponse {
            now_unix_secs: 1_800_000_000,
            window_anchor_unix_secs: 0,
            max_staleness_secs: 300,
            upstream_count: 1,
            weighting: "plan".to_owned(),
            windows: vec![aggregate_window("7d_fable", 1_800_604_800, 28.0)],
            caveats: Vec::new(),
        };

        let records =
            pool_quota_snapshots_from_aggregate(&aggregate, 1_800_000_000, 1_800_000_000_000);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].window, SubscriptionQuotaWindow::SevenDayFable);
        assert_eq!(records[0].utilization, Some(0.28));
    }

    #[test]
    fn oauth_usage_response_serializes_anthropic_shape_only() {
        let aggregate = AggregateResponse {
            now_unix_secs: 1_000,
            window_anchor_unix_secs: 0,
            max_staleness_secs: 60,
            upstream_count: 2,
            weighting: "plan".to_owned(),
            windows: vec![
                aggregate_window("5h", 1_800, 25.0),
                aggregate_window("7d", 604_800, 40.0),
                aggregate_window("7d_sonnet", 604_800, 30.0),
                aggregate_window("7d_opus", 604_800, 10.0),
                aggregate_window("7d_fable", 604_800, 28.0),
            ],
            caveats: vec!["admin only".to_owned()],
        };

        let mut response = oauth_usage_from_aggregate(&aggregate);
        response.extra_usage = Some(CcLbOAuthExtraUsage {
            enabled: Some(true),
            monthly_limit: Some(500.0),
            used_credits: Some(125.0),
        });
        let value = serde_json::to_value(response).unwrap();

        assert_eq!(value["5h"]["utilization"], 25.0);
        assert_eq!(value["5h"]["resets_at"], 1_800);
        assert_eq!(value["7d"]["utilization"], 40.0);
        assert_eq!(value["7d_sonnet"]["utilization"], 30.0);
        assert_eq!(value["7d_opus"]["utilization"], 10.0);
        assert_eq!(value["extra_usage"]["enabled"], true);
        assert_eq!(value["extra_usage"]["monthly_limit"], 500.0);
        assert_eq!(value["extra_usage"]["used_credits"], 125.0);
        let limits = value["limits"].as_array().expect("limits are returned");
        assert_eq!(limits.len(), 1);
        assert_eq!(limits[0]["kind"], "weekly_scoped");
        assert_eq!(limits[0]["percent"], 28.0);
        assert_eq!(limits[0]["resets_at"], 604_800);
        assert_eq!(limits[0]["scope"]["model"]["display_name"], "Fable");
        assert_eq!(limits[0]["is_active"], true);
        assert!(value.get("five_hour").is_none());
        assert!(value.get("seven_day_fable").is_none());
        assert!(value.get("windows").is_none());
        assert!(value.get("caveats").is_none());
    }

    fn aggregate_window(
        window: &str,
        reset_unix_secs: u64,
        utilization_percent: f64,
    ) -> AggregateWindowResponse {
        AggregateWindowResponse {
            window: window.to_owned(),
            cc_window_start_unix_secs: 0,
            cc_window_reset_unix_secs: reset_unix_secs,
            used_tokens: 10,
            utilization: Some(utilization_percent / 100.0),
            utilization_percent: Some(utilization_percent),
            capacity_to_now_tokens_estimate: Some(40.0),
            projected_capacity_tokens_estimate: Some(40.0),
            remaining_to_now_tokens_estimate: Some(30.0),
            confidence: "estimated".to_owned(),
            contributing_upstreams: 1,
            stale_upstreams: 0,
            missing_capacity_upstreams: 0,
            eligible_upstreams: 1,
            provider_lots: Vec::new(),
            caveats: Vec::new(),
        }
    }

    fn upstream_record(id: Uuid) -> UpstreamRecord {
        UpstreamRecord {
            id,
            name: "oauth-upstream".to_owned(),
            kind: UpstreamKind::AnthropicOauth,
            enabled: true,
            revision: 1,
            oauth_token_generation: 0,
            created_at_unix_secs: 0,
            updated_at_unix_secs: 0,
            ..UpstreamRecord::default()
        }
    }

    fn lot(
        upstream_id: &str,
        utilization: Option<f64>,
        observed_at_unix_millis: Option<u64>,
        capacity_estimate_tokens: Option<f64>,
        capacity_to_now_tokens_estimate: Option<f64>,
    ) -> AggregateProviderLotResponse {
        lot_with_ratio(
            upstream_id,
            utilization,
            observed_at_unix_millis,
            capacity_estimate_tokens,
            capacity_to_now_tokens_estimate,
            PRO_CAPACITY_RATIO,
        )
    }

    fn lot_with_ratio(
        upstream_id: &str,
        utilization: Option<f64>,
        observed_at_unix_millis: Option<u64>,
        capacity_estimate_tokens: Option<f64>,
        capacity_to_now_tokens_estimate: Option<f64>,
        capacity_ratio: f64,
    ) -> AggregateProviderLotResponse {
        AggregateProviderLotResponse {
            upstream_id: upstream_id.to_owned(),
            upstream_name: format!("{upstream_id}-name"),
            window: "5h".to_owned(),
            source: Some("merged".to_owned()),
            state: "fresh".to_owned(),
            provider_start_unix_secs: Some(0),
            provider_reset_unix_secs: Some(18_000),
            observed_at_unix_millis,
            utilization,
            capacity_estimate_tokens,
            used_before_cc_window_tokens: 0,
            capacity_to_now_tokens_estimate,
            projected_capacity_tokens_estimate: capacity_to_now_tokens_estimate,
            confidence: "estimated".to_owned(),
            capacity_ratio,
        }
    }

    #[test]
    fn aggregate_falls_back_to_header_mean_when_capacity_missing() {
        let lots = vec![
            lot("u-1", Some(0.10), Some(100_000), None, None),
            lot("u-2", Some(0.30), Some(100_000), None, None),
            lot("u-3", None, None, None, None),
        ];
        let response = build_aggregate_window_response(
            SubscriptionQuotaWindow::FiveHour,
            &lots,
            0,
            now_unix_secs_test(),
            "plan",
        );
        let utilization = response
            .utilization
            .expect("fallback should populate utilization");
        assert!((utilization - 0.20).abs() < f64::EPSILON);
        assert_eq!(response.confidence, "plan_weighted");
        assert_eq!(response.contributing_upstreams, 2);
        assert_eq!(response.missing_capacity_upstreams, 1);
        assert!(response.caveats.iter().any(|c| c.contains("plan ratios")));
    }

    #[test]
    fn aggregate_ignores_historical_idle_lots_for_missing_capacity_and_staleness() {
        let mut historical_idle = lot("u-1", Some(0.0), Some(1_000), None, None);
        historical_idle.state = "stale".to_owned();
        let current_active = lot(
            "u-1",
            Some(0.50),
            Some(2_000),
            Some(1_000_000.0),
            Some(500_000.0),
        );
        let response = build_aggregate_window_response(
            SubscriptionQuotaWindow::FiveHour,
            &[historical_idle, current_active],
            500_000,
            now_unix_secs_test(),
            "plan",
        );
        assert_eq!(response.stale_upstreams, 0);
        assert_eq!(response.missing_capacity_upstreams, 0);
        assert_eq!(response.contributing_upstreams, 1);
    }

    #[test]
    fn aggregate_uses_latest_observation_per_upstream_for_fallback() {
        let lots = vec![
            lot("u-1", Some(0.40), Some(1_000), None, None),
            lot("u-1", Some(0.60), Some(2_000), None, None),
            lot("u-2", Some(0.20), Some(1_500), None, None),
        ];
        let response = build_aggregate_window_response(
            SubscriptionQuotaWindow::FiveHour,
            &lots,
            0,
            now_unix_secs_test(),
            "plan",
        );
        let utilization = response
            .utilization
            .expect("fallback should populate utilization");
        assert!((utilization - 0.40).abs() < f64::EPSILON);
        assert_eq!(response.confidence, "plan_weighted");
        assert_eq!(response.contributing_upstreams, 2);
    }

    #[test]
    fn aggregate_prefers_capacity_weighted_when_any_lot_has_capacity() {
        let lots = vec![lot(
            "u-1",
            Some(0.50),
            Some(1_000),
            Some(1_000_000.0),
            Some(500_000.0),
        )];
        let response = build_aggregate_window_response(
            SubscriptionQuotaWindow::FiveHour,
            &lots,
            0,
            now_unix_secs_test(),
            "plan",
        );
        assert!(response.utilization.is_some());
        assert_eq!(response.confidence, "plan_weighted");
        assert_eq!(response.contributing_upstreams, 1);
    }

    fn now_unix_secs_test() -> u64 {
        1_780_000_000
    }
}
