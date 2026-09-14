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
use cc_lb_domain::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_quota::plan_capacity::{PRO_CAPACITY_RATIO, plan_capacity_ratio};
use cc_lb_storage_api::{
    OrganizationMetadataRecord, POOL_QUOTA_POLICY_VERSION, PoolQuotaChartPointRecord,
    PoolQuotaHistoryStore, PoolQuotaSnapshotRecord, PoolQuotaSnapshotSummaryRecord, Storage,
    StorageError, SubscriptionQuotaBucket, SubscriptionQuotaProviderLot,
    SubscriptionQuotaProviderLotQuery, SubscriptionQuotaSeriesQuery, SubscriptionQuotaSource,
    SubscriptionQuotaSourceMerge, SubscriptionQuotaWindow, UpstreamRecord, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, UsageRollup, UsageRollupResolution, UsageTokenInterval,
    upstream::UpstreamKind,
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
const ANALYSIS_BUCKET_SECS: u64 = 60;
const ANALYSIS_MAX_POINTS: u32 = 10_000;
const RESET_DROP_THRESHOLD: f64 = 0.5;
const GAP_MARKER_MULTIPLIER: u64 = 2;
const PROXY_RATE_LOOKBACK_SECS: u64 = 3_600;
const CAPACITY_CAVEAT: &str = "capacity is inferred from proxy tokens and quota utilization; Anthropic quota units are not directly exposed";
const OUTSIDE_TRAFFIC_CAVEAT: &str =
    "actual_account_burn includes traffic outside this cc-lb instance";
const HEADER_ONLY_CAVEAT: &str =
    "analysis is limited to header-derived observations; API polling data is sparse";
const HEADER_FALLBACK_CAVEAT: &str = "utilization is the arithmetic mean of header observations; capacity-weighted estimate is unavailable because cc-lb has not proxied enough traffic to back-solve provider capacity";
const PLAN_RATIO_CAVEAT: &str = "utilization is weighted by subscription plan ratios from Anthropic metadata; 5h and 7d use the same documented plan ratios";
const STALE_DATA_CAVEAT: &str =
    "latest observation is older than max_staleness_secs; analysis may be outdated";

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/subscription-quotas/latest", get(handle_latest))
        .route("/admin/subscription-quotas/latest", get(handle_latest))
        .route("/admin/v1/subscription-quotas/series", get(handle_series))
        .route("/admin/subscription-quotas/series", get(handle_series))
        .route(
            "/admin/v1/subscription-quotas/aggregate",
            get(handle_aggregate),
        )
        .route(
            "/admin/subscription-quotas/aggregate",
            get(handle_aggregate),
        )
        .route(
            "/admin/v1/subscription-quotas/analysis",
            get(handle_analysis),
        )
        .route("/admin/subscription-quotas/analysis", get(handle_analysis))
        .route(
            "/admin/v1/subscription-quotas/pool-history",
            get(handle_pool_history),
        )
        .route(
            "/admin/subscription-quotas/pool-history",
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

// Slim wire shape: dashboard plots only these two; internal analysis paths
// read SubscriptionQuotaBucket directly from cc-lb-storage-api, not this.
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

#[derive(Debug, Serialize, Deserialize)]
struct AnalysisQuery {
    upstream_ids: Option<String>,
    windows: Option<String>,
    since_unix_secs: u64,
    until_unix_secs: u64,
    source: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AggregateQuery {
    upstream_ids: Option<String>,
    windows: Option<String>,
    source: Option<String>,
    max_staleness_secs: Option<u64>,
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

#[derive(Debug, Serialize, Deserialize)]
struct AnalysisResponse {
    since_unix_secs: u64,
    until_unix_secs: u64,
    now_unix_secs: u64,
    max_staleness_secs: u64,
    upstreams: Vec<AnalysisUpstreamResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AnalysisUpstreamResponse {
    upstream_id: String,
    upstream_name: String,
    windows: Vec<AnalysisWindowResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AnalysisWindowResponse {
    window: String,
    current_utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
    data_state: String,
    actual_account_burn: BurnResponse,
    proxy_projected_burn: ProxyBurnResponse,
    deficit: Option<DeficitResponse>,
    caveats: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BurnResponse {
    utilization_per_second: Option<f64>,
    utilization_per_hour: Option<f64>,
    eta_to_limit_secs: Option<u64>,
    resets_before_limit: Option<bool>,
    confidence: String,
    interval_count: usize,
    reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProxyBurnResponse {
    proxy_tokens_per_second: Option<f64>,
    proxy_tokens_per_hour: Option<f64>,
    effective_limit_tokens_estimate: Option<f64>,
    utilization_per_hour: Option<f64>,
    eta_to_limit_secs: Option<u64>,
    resets_before_limit: Option<bool>,
    confidence: String,
    interval_count: usize,
    reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DeficitResponse {
    projected_proxy_tokens_window: f64,
    effective_limit_tokens_estimate: f64,
    shortfall_tokens: f64,
    recommended_multiplier: f64,
    confidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalysisObservation {
    bucket_start_unix_secs: u64,
    utilization_last: f64,
    resets_at_unix_secs_last: Option<u64>,
    observed_at_unix_millis_last: u64,
    sources_seen: Vec<SubscriptionQuotaSource>,
}

#[derive(Debug, Clone, Copy)]
struct ObservationCycle<'a> {
    observations: &'a [AnalysisObservation],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UtilizationInterval {
    start_unix_secs: u64,
    end_unix_secs: u64,
    delta_utilization: f64,
    slope_per_second: f64,
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

async fn handle_analysis(
    State(state): State<AdminState>,
    Query(query): Query<AnalysisQuery>,
) -> Response {
    match build_analysis_response(&state, query).await {
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
];

#[derive(Debug, Deserialize)]
struct PoolHistoryQuery {
    windows: Option<String>,
    since_unix_secs: Option<i64>,
    until_unix_secs: Option<i64>,
    series_projection: Option<String>,
    max_points_per_series: Option<usize>,
}

#[derive(Debug, Serialize)]
struct PoolHistoryResponse {
    now_unix_secs: i64,
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

#[derive(Debug, Serialize)]
struct PoolHistoryPoint {
    snapshot_at_unix_secs: i64,
    utilization: Option<f64>,
    utilization_percent: Option<f64>,
    contributing_upstreams: i64,
    eligible_upstreams: i64,
    stale_upstreams: i64,
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
    let storage = storage(state)?;
    let windows = parse_pool_history_windows(query.windows.as_deref())?;
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
    let since_unix_secs = query
        .since_unix_secs
        .unwrap_or(now_unix_secs.saturating_sub(default_lookback_secs));
    let until_unix_secs = query.until_unix_secs.unwrap_or(now_unix_secs);
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

    let latest_records = storage
        .list_latest_pool_quota_snapshot_summaries(&windows)
        .await
        .map_err(storage_error)?;
    let mut latest_by_window = latest_records
        .into_iter()
        .map(|record| {
            let window = record.window;
            let point = pool_history_point_from_record(&record);
            (window, point)
        })
        .collect::<BTreeMap<_, _>>();

    let mut full_series_by_window =
        BTreeMap::<SubscriptionQuotaWindow, Vec<PoolHistoryPoint>>::new();
    let mut chart_series_by_window =
        BTreeMap::<SubscriptionQuotaWindow, Vec<PoolHistorySeriesPoint>>::new();
    if compact_series {
        let records = storage
            .list_pool_quota_chart_points_in_range(
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
            .list_pool_quota_snapshot_summaries_in_range(&windows, since_unix_secs, until_unix_secs)
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
        windows: response_windows,
    })
}

fn parse_pool_history_windows(raw: Option<&str>) -> Result<Vec<SubscriptionQuotaWindow>, Response> {
    let default_windows = vec![
        SubscriptionQuotaWindow::FiveHour,
        SubscriptionQuotaWindow::SevenDay,
    ];
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
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("unknown pool history window: {token}"),
            )
                .into_response()
        })?;
        if !POOLED_HISTORY_WINDOWS.contains(&window) {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("pool history only supports 5h, 7d, and 7d_fable, got {token}"),
            )
                .into_response());
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

async fn build_analysis_response(
    state: &AdminState,
    query: AnalysisQuery,
) -> Result<AnalysisResponse, Response> {
    let storage = storage(state)?;
    let source = parse_source_merge(query.source.as_deref())?;
    let windows = parse_windows_or_default(query.windows.as_deref())?;
    validate_time_range(query.since_unix_secs, query.until_unix_secs)?;
    validate_series_guardrails(
        query.since_unix_secs,
        query.until_unix_secs,
        ANALYSIS_BUCKET_SECS,
        ANALYSIS_MAX_POINTS,
        "narrow the requested time range; analysis supports at most 20000 one-minute buckets",
    )?;
    let upstreams = upstreams_for_optional_query(storage, query.upstream_ids.as_deref()).await?;
    validate_upstream_count(upstreams.len())?;
    let requested_upstream_ids: Vec<Uuid> = upstreams.iter().map(|u| u.id).collect();
    let upstream_names = upstream_name_map(&upstreams);
    let rollups = storage
        .query_usage_rollups_for_upstreams_in_range(
            &requested_upstream_ids,
            UsageRollupResolution::Minute,
            query.since_unix_secs,
            query.until_unix_secs,
        )
        .await
        .map_err(storage_error)?;
    let mut rollups_by_upstream = BTreeMap::<Uuid, Vec<UsageRollup>>::new();
    for rollup in rollups {
        rollups_by_upstream
            .entry(rollup.upstream_id)
            .or_default()
            .push(rollup);
    }
    let quota_series = list_subscription_quota_series(
        storage,
        SubscriptionQuotaSeriesQuery {
            upstream_ids: requested_upstream_ids.clone(),
            windows: windows.clone(),
            sources: sources_for_merge(source),
            since_unix_millis: query.since_unix_secs.saturating_mul(1_000),
            until_unix_millis: query.until_unix_secs.saturating_mul(1_000),
            bucket_secs: ANALYSIS_BUCKET_SECS,
            max_points_per_series: ANALYSIS_MAX_POINTS,
            source_merge: source,
        },
    )
    .await
    .map_err(storage_error)?;
    let max_staleness_secs = state
        .dynamic_view
        .load()
        .subscription_quota_routing_max_staleness_secs;
    let latest_by_upstream_window = latest_cache_by_upstream_window(
        state,
        &requested_upstream_ids,
        query.until_unix_secs.saturating_mul(1_000),
        max_staleness_secs,
    );

    let mut windows_by_upstream: BTreeMap<Uuid, Vec<AnalysisWindowResponse>> = BTreeMap::new();
    for series in quota_series {
        let observations = checkpoint_observations(&series.buckets);
        let latest = latest_by_upstream_window.get(&(series.upstream_id, series.window));
        let rollups_for_upstream = rollups_by_upstream
            .get(&series.upstream_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let response = build_analysis_window(
            series.window,
            latest,
            &observations,
            rollups_for_upstream,
            query.since_unix_secs,
            query.until_unix_secs,
        );
        windows_by_upstream
            .entry(series.upstream_id)
            .or_default()
            .push(response);
    }

    let upstreams = requested_upstream_ids
        .into_iter()
        .map(|upstream_id| AnalysisUpstreamResponse {
            upstream_id: upstream_id.to_string(),
            upstream_name: upstream_names
                .get(&upstream_id)
                .cloned()
                .unwrap_or_default(),
            windows: windows_by_upstream.remove(&upstream_id).unwrap_or_default(),
        })
        .collect();

    Ok(AnalysisResponse {
        since_unix_secs: query.since_unix_secs,
        until_unix_secs: query.until_unix_secs,
        now_unix_secs: query.until_unix_secs,
        max_staleness_secs,
        upstreams,
    })
}

async fn build_aggregate_response(
    state: &AdminState,
    query: AggregateQuery,
) -> Result<AggregateResponse, Response> {
    let storage = storage(state)?;
    let source = parse_source_merge(query.source.as_deref())?;
    let windows = parse_windows_or_default(query.windows.as_deref())?;
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

    build_cc_lb_aggregate_response(
        storage,
        &state.dynamic_view,
        upstream_ids,
        windows,
        source,
        max_staleness_secs,
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
    let now_unix_millis = now_unix_millis(clock);
    let now_unix_secs = now_unix_millis / 1_000;
    let requested = upstream_ids.map(|ids| ids.into_iter().collect::<HashSet<_>>());
    let mut upstreams = pool_quota_upstreams(
        list_all_upstreams_storage(storage).await?,
        requested.as_ref(),
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
    let mut latest_candidates = Vec::new();
    for upstream in &upstreams {
        for snapshot in dynamic_view.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis,
            max_staleness_secs,
        ) {
            if !snapshot_matches_source(&snapshot, source) {
                continue;
            }
            let Some(window) = SubscriptionQuotaWindow::from_str(&snapshot.window) else {
                continue;
            };
            if !windows.contains(&window) || window_secs(window).is_none() {
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

    let mut lot_inputs = provider_lots
        .iter()
        .filter_map(|lot| {
            let upstream = upstream_by_id.get(&lot.upstream_id)?;
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
    let covered_by_provider_lots = lot_inputs
        .iter()
        .map(|input| (input.upstream_id, input.window))
        .collect::<HashSet<_>>();
    lot_inputs.extend(
        latest_candidates
            .iter()
            .filter(|(upstream_id, window, _, _)| {
                !covered_by_provider_lots.contains(&(*upstream_id, *window))
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
                .map(|upstream| push_interval(upstream.id, cc_start, now_unix_secs))
                .collect::<Vec<_>>();
            (window, ids)
        })
        .collect::<HashMap<_, _>>();
    let token_sums = storage
        .sum_usage_tokens_for_intervals(&intervals)
        .await?
        .into_iter()
        .map(|sum| (sum.interval_id, sum.tokens))
        .collect::<HashMap<_, _>>();

    let mut aggregate_windows = Vec::new();
    for window in duration_windows {
        let window_lots = lot_inputs
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
            .collect::<Vec<_>>();
        let used_tokens = window_interval_ids
            .get(&window)
            .into_iter()
            .flatten()
            .filter_map(|interval_id| token_sums.get(interval_id))
            .copied()
            .sum();
        aggregate_windows.push(build_aggregate_window_response(
            window,
            &window_lots,
            used_tokens,
            now_unix_secs,
        ));
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
        windows: aggregate_windows,
        caveats,
    })
}

pub fn pool_quota_snapshots_from_aggregate(
    aggregate: &AggregateResponse,
    snapshot_at_unix_secs: i64,
    computed_at_unix_millis: i64,
) -> Vec<PoolQuotaSnapshotRecord> {
    let eligible_upstreams = i64::try_from(aggregate.upstream_count).unwrap_or(i64::MAX);
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
                snapshot_at_unix_secs,
                window,
                utilization: window_response.utilization,
                weighted_utilization_sum,
                capacity_ratio_sum,
                eligible_upstreams,
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
    if records.is_empty() {
        return Ok(());
    }
    PoolQuotaHistoryStore::record_pool_quota_snapshots(storage, &records).await
}

fn build_aggregate_window_response(
    window: SubscriptionQuotaWindow,
    lots: &[AggregateProviderLotResponse],
    used_tokens: u64,
    now_unix_secs: u64,
) -> AggregateWindowResponse {
    let secs = window_secs(window).expect("aggregate windows are duration-backed");
    let (cc_start, cc_reset) = cc_window_bounds(now_unix_secs, secs);
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
    let plan_weighted_utilization = if latest_header_per_upstream.is_empty() {
        None
    } else {
        let weighted_sum: f64 = latest_header_per_upstream
            .values()
            .map(|(_, utilization, ratio)| utilization * ratio)
            .sum();
        let ratio_sum: f64 = latest_header_per_upstream
            .values()
            .map(|(_, _, ratio)| *ratio)
            .sum();
        (ratio_sum > 0.0).then(|| (weighted_sum / ratio_sum).clamp(0.0, 1.0))
    };

    let utilization = plan_weighted_utilization.or(weighted_utilization);

    let stale_upstreams = lots
        .iter()
        .filter(|lot| lot.state == "stale")
        .map(|lot| lot.upstream_id.as_str())
        .collect::<HashSet<_>>()
        .len();
    let missing_capacity_upstreams = lots
        .iter()
        .filter(|lot| lot.capacity_estimate_tokens.is_none())
        .map(|lot| lot.upstream_id.as_str())
        .collect::<HashSet<_>>()
        .len();
    let contributing_upstreams = if plan_weighted_utilization.is_some() {
        latest_header_per_upstream.len()
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
    if plan_weighted_utilization.is_some() {
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
        confidence: aggregate_confidence(lots, weighted_utilization, plan_weighted_utilization),
        contributing_upstreams,
        stale_upstreams,
        missing_capacity_upstreams,
        provider_lots: latest_lot_per_upstream(lots),
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
    let secs = window_secs(window).expect("aggregate windows are duration-backed");
    let (_, cc_reset) = cc_window_bounds(now_unix_secs, secs);
    AggregateProviderLotInput {
        upstream_id: upstream.id,
        upstream_name: upstream.name.as_str(),
        window,
        source: snapshot.source.as_deref(),
        state: snapshot.state,
        provider_start: provider_window_start_unix_secs(window, snapshot, now_unix_secs),
        provider_reset: snapshot.resets_at_unix_secs.or(Some(cc_reset)),
        observed_at_unix_millis: snapshot.observed_at_unix_millis,
        evaluation_unix_secs: now_unix_secs,
        utilization: snapshot.utilization,
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
    let secs = window_secs(window).expect("aggregate windows are duration-backed");
    let (cc_start, cc_reset) = cc_window_bounds(now_unix_secs, secs);
    let provider_start = input.provider_start;
    let provider_reset = input.provider_reset.or(Some(cc_reset));
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

fn build_analysis_window(
    window: SubscriptionQuotaWindow,
    latest: Option<&SubscriptionQuotaCandidateSnapshot>,
    observations: &[AnalysisObservation],
    rollups: &[UsageRollup],
    since_unix_secs: u64,
    until_unix_secs: u64,
) -> AnalysisWindowResponse {
    let current_utilization = latest
        .and_then(|snapshot| snapshot.utilization)
        .or_else(|| {
            observations
                .last()
                .map(|observation| observation.utilization_last)
        });
    let resets_at_unix_secs = latest
        .and_then(|snapshot| snapshot.resets_at_unix_secs)
        .or_else(|| {
            observations
                .last()
                .and_then(|observation| observation.resets_at_unix_secs_last)
        });
    let data_state = latest
        .map(|snapshot| data_state_str(snapshot.state).to_owned())
        .unwrap_or_else(|| "unobserved".to_owned());
    let cycles = split_reset_cycles(observations);
    let intervals = valid_utilization_intervals(&cycles);
    let actual_account_burn = infer_actual_account_burn(
        &intervals,
        current_utilization,
        resets_at_unix_secs,
        until_unix_secs,
    );
    let proxy_projected_burn = infer_proxy_projected_burn(
        &intervals,
        rollups,
        current_utilization,
        resets_at_unix_secs,
        since_unix_secs,
        until_unix_secs,
    );
    let deficit = deficit_for_window(window, &proxy_projected_burn);
    let caveats = analysis_caveats(
        &actual_account_burn,
        &proxy_projected_burn,
        observations,
        &data_state,
    );

    AnalysisWindowResponse {
        window: window.as_str().to_owned(),
        current_utilization,
        resets_at_unix_secs,
        data_state,
        actual_account_burn,
        proxy_projected_burn,
        deficit,
        caveats,
    }
}

fn infer_actual_account_burn(
    intervals: &[UtilizationInterval],
    current_utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
    now_unix_secs: u64,
) -> BurnResponse {
    let slopes = intervals
        .iter()
        .map(|interval| interval.slope_per_second)
        .collect::<Vec<_>>();
    let Some(slope) = median(slopes) else {
        return BurnResponse {
            utilization_per_second: None,
            utilization_per_hour: None,
            eta_to_limit_secs: None,
            resets_before_limit: None,
            confidence: "low".to_owned(),
            interval_count: 0,
            reason: Some("insufficient_growth_intervals".to_owned()),
        };
    };
    let eta_to_limit_secs =
        current_utilization.and_then(|utilization| eta_to_limit(utilization, slope));
    BurnResponse {
        utilization_per_second: Some(slope),
        utilization_per_hour: Some(slope * 3_600.0),
        eta_to_limit_secs,
        resets_before_limit: eta_to_limit_secs
            .map(|eta| resets_before_limit(eta, resets_at_unix_secs, now_unix_secs)),
        confidence: confidence_for_interval_count(intervals.len()).to_owned(),
        interval_count: intervals.len(),
        reason: None,
    }
}

fn infer_proxy_projected_burn(
    intervals: &[UtilizationInterval],
    rollups: &[UsageRollup],
    current_utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
    since_unix_secs: u64,
    until_unix_secs: u64,
) -> ProxyBurnResponse {
    let capacities = intervals
        .iter()
        .filter_map(|interval| {
            let tokens =
                tokens_in_interval(rollups, interval.start_unix_secs, interval.end_unix_secs);
            if tokens == 0 || interval.delta_utilization <= 0.0 {
                None
            } else {
                Some(tokens as f64 / interval.delta_utilization)
            }
        })
        .collect::<Vec<_>>();
    let Some(effective_capacity) = median(capacities) else {
        return ProxyBurnResponse {
            proxy_tokens_per_second: None,
            proxy_tokens_per_hour: None,
            effective_limit_tokens_estimate: None,
            utilization_per_hour: None,
            eta_to_limit_secs: None,
            resets_before_limit: None,
            confidence: "low".to_owned(),
            interval_count: 0,
            reason: Some("insufficient_growth_intervals".to_owned()),
        };
    };
    let lookback_secs = until_unix_secs
        .saturating_sub(since_unix_secs)
        .min(PROXY_RATE_LOOKBACK_SECS)
        .max(1);
    let lookback_start = until_unix_secs.saturating_sub(lookback_secs);
    let lookback_tokens = tokens_in_interval(rollups, lookback_start, until_unix_secs);
    let proxy_tokens_per_second = lookback_tokens as f64 / lookback_secs as f64;
    let utilization_per_second_projected = if effective_capacity > 0.0 {
        proxy_tokens_per_second / effective_capacity
    } else {
        0.0
    };
    let eta_to_limit_secs = current_utilization
        .and_then(|utilization| eta_to_limit(utilization, utilization_per_second_projected));
    ProxyBurnResponse {
        proxy_tokens_per_second: Some(proxy_tokens_per_second),
        proxy_tokens_per_hour: Some(proxy_tokens_per_second * 3_600.0),
        effective_limit_tokens_estimate: Some(effective_capacity),
        utilization_per_hour: Some(utilization_per_second_projected * 3_600.0),
        eta_to_limit_secs,
        resets_before_limit: eta_to_limit_secs
            .map(|eta| resets_before_limit(eta, resets_at_unix_secs, until_unix_secs)),
        confidence: confidence_for_interval_count(intervals.len()).to_owned(),
        interval_count: intervals.len(),
        reason: None,
    }
}

fn deficit_for_window(
    window: SubscriptionQuotaWindow,
    proxy_burn: &ProxyBurnResponse,
) -> Option<DeficitResponse> {
    let window_secs = match window {
        SubscriptionQuotaWindow::FiveHour => 5 * 3_600,
        SubscriptionQuotaWindow::SevenDay
        | SubscriptionQuotaWindow::SevenDaySonnet
        | SubscriptionQuotaWindow::SevenDayOpus
        | SubscriptionQuotaWindow::SevenDayFable => 7 * 24 * 3_600,
        SubscriptionQuotaWindow::Overage | SubscriptionQuotaWindow::Unified => return None,
    };
    let proxy_tokens_per_second = proxy_burn.proxy_tokens_per_second?;
    let effective_limit_tokens_estimate = proxy_burn.effective_limit_tokens_estimate?;
    if effective_limit_tokens_estimate <= 0.0 {
        return None;
    }
    let projected_proxy_tokens_window = proxy_tokens_per_second * window_secs as f64;
    let shortfall_tokens =
        (projected_proxy_tokens_window - effective_limit_tokens_estimate).max(0.0);
    let recommended_multiplier =
        round_to_one_decimal(projected_proxy_tokens_window / effective_limit_tokens_estimate);
    Some(DeficitResponse {
        projected_proxy_tokens_window,
        effective_limit_tokens_estimate,
        shortfall_tokens,
        recommended_multiplier,
        confidence: proxy_burn.confidence.clone(),
    })
}

fn split_reset_cycles(observations: &[AnalysisObservation]) -> Vec<ObservationCycle<'_>> {
    debug_assert!(observations.windows(2).all(|pair| {
        pair[0].observed_at_unix_millis_last <= pair[1].observed_at_unix_millis_last
    }));
    if observations.is_empty() {
        return Vec::new();
    }

    let mut cycles = Vec::new();
    let mut cycle_start = 0;
    for current in 1..observations.len() {
        if starts_new_cycle(&observations[current - 1], &observations[current]) {
            cycles.push(ObservationCycle {
                observations: &observations[cycle_start..current],
            });
            cycle_start = current;
        }
    }
    cycles.push(ObservationCycle {
        observations: &observations[cycle_start..],
    });
    cycles
}

fn starts_new_cycle(previous: &AnalysisObservation, current: &AnalysisObservation) -> bool {
    let reset_changed = match (
        previous.resets_at_unix_secs_last,
        current.resets_at_unix_secs_last,
    ) {
        (Some(left), Some(right)) => left.abs_diff(right) > 60,
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    };
    let synthetic_reset =
        previous.utilization_last - current.utilization_last >= RESET_DROP_THRESHOLD;
    reset_changed || synthetic_reset
}

fn valid_utilization_intervals(cycles: &[ObservationCycle<'_>]) -> Vec<UtilizationInterval> {
    let mut intervals = Vec::new();
    for cycle in cycles {
        for pair in cycle.observations.windows(2) {
            let [previous, current] = pair else {
                continue;
            };
            let delta_time_secs = current
                .observed_at_unix_millis_last
                .saturating_sub(previous.observed_at_unix_millis_last)
                / 1_000;
            let delta_utilization = current.utilization_last - previous.utilization_last;
            if delta_utilization < 0.0 || delta_time_secs == 0 {
                continue;
            }
            intervals.push(UtilizationInterval {
                start_unix_secs: previous.observed_at_unix_millis_last / 1_000,
                end_unix_secs: current.observed_at_unix_millis_last / 1_000,
                delta_utilization,
                slope_per_second: delta_utilization / delta_time_secs as f64,
            });
        }
    }
    intervals
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

fn checkpoint_observations(buckets: &[SubscriptionQuotaBucket]) -> Vec<AnalysisObservation> {
    buckets
        .iter()
        .filter(|bucket| bucket.observed && bucket.sample_count > 0)
        .filter_map(analysis_observation_from_bucket)
        .collect()
}

fn analysis_observation_from_bucket(
    bucket: &SubscriptionQuotaBucket,
) -> Option<AnalysisObservation> {
    Some(AnalysisObservation {
        bucket_start_unix_secs: bucket.bucket_start_unix_secs,
        utilization_last: bucket.utilization_last?,
        resets_at_unix_secs_last: bucket.resets_at_unix_secs_last,
        observed_at_unix_millis_last: bucket.observed_at_unix_millis_last?,
        sources_seen: bucket.sources_seen.clone(),
    })
}

fn analysis_caveats(
    actual_account_burn: &BurnResponse,
    proxy_projected_burn: &ProxyBurnResponse,
    observations: &[AnalysisObservation],
    data_state: &str,
) -> Vec<String> {
    let mut caveats = vec![CAPACITY_CAVEAT.to_owned()];
    if let (Some(actual), Some(proxy)) = (
        actual_account_burn.utilization_per_second,
        proxy_projected_burn
            .utilization_per_hour
            .map(|value| value / 3_600.0),
    ) && proxy > 0.0
        && actual > 1.5 * proxy
    {
        caveats.push(OUTSIDE_TRAFFIC_CAVEAT.to_owned());
    }
    let header_count = observations
        .iter()
        .filter(|observation| {
            observation
                .sources_seen
                .contains(&SubscriptionQuotaSource::Header)
        })
        .count();
    let api_count = observations
        .iter()
        .filter(|observation| {
            observation
                .sources_seen
                .contains(&SubscriptionQuotaSource::Api)
        })
        .count();
    if header_count > 0 && api_count == 0 {
        caveats.push(HEADER_ONLY_CAVEAT.to_owned());
    }
    if data_state == "stale" {
        caveats.push(STALE_DATA_CAVEAT.to_owned());
    }
    caveats
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
    Ok(pool_quota_upstreams(
        list_all_upstreams(storage).await?,
        requested.as_ref(),
    ))
}

fn pool_quota_upstreams(
    upstreams: Vec<UpstreamRecord>,
    requested: Option<&HashSet<Uuid>>,
) -> Vec<UpstreamRecord> {
    upstreams
        .into_iter()
        .filter(|upstream| upstream.deleted_at_unix_secs.is_none())
        .filter(|upstream| upstream.kind == UpstreamKind::AnthropicOauth)
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

fn latest_cache_by_upstream_window(
    state: &AdminState,
    upstream_ids: &[Uuid],
    now_unix_millis: u64,
    max_staleness_secs: u64,
) -> HashMap<(Uuid, SubscriptionQuotaWindow), SubscriptionQuotaCandidateSnapshot> {
    let dynamic_view = state.dynamic_view.load();
    let mut latest = HashMap::new();
    for upstream_id in upstream_ids {
        for snapshot in dynamic_view.subscription_quota_cache.snapshot_for_upstream(
            *upstream_id,
            now_unix_millis,
            max_staleness_secs,
        ) {
            if let Some(window) = SubscriptionQuotaWindow::from_str(&snapshot.window) {
                latest.insert((*upstream_id, window), snapshot);
            }
        }
    }
    latest
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

fn tokens_in_interval(rollups: &[UsageRollup], start_unix_secs: u64, end_unix_secs: u64) -> u64 {
    rollups
        .iter()
        .filter(|rollup| {
            rollup.bucket_start >= start_unix_secs && rollup.bucket_start <= end_unix_secs
        })
        .map(proxy_tokens)
        .sum()
}

fn proxy_tokens(rollup: &UsageRollup) -> u64 {
    rollup
        .input_tokens
        .saturating_add(rollup.output_tokens)
        .saturating_add(rollup.cache_creation_input_tokens)
        .saturating_add(rollup.cache_read_input_tokens)
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[middle - 1] + values[middle]) / 2.0)
    } else {
        Some(values[middle])
    }
}

fn eta_to_limit(current_utilization: f64, slope: f64) -> Option<u64> {
    if slope <= 0.0 || current_utilization >= 1.0 {
        return None;
    }
    Some(((1.0 - current_utilization) / slope).ceil() as u64)
}

fn resets_before_limit(
    eta_to_limit_secs: u64,
    resets_at_unix_secs: Option<u64>,
    now_unix_secs: u64,
) -> bool {
    resets_at_unix_secs
        .map(|reset| eta_to_limit_secs > reset.saturating_sub(now_unix_secs))
        .unwrap_or(false)
}

fn confidence_for_interval_count(count: usize) -> &'static str {
    if count >= 8 {
        "high"
    } else if count >= 3 {
        "medium"
    } else {
        "low"
    }
}

fn round_to_one_decimal(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
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
    fn quota_data_states_preserve_absent_and_unobserved() {
        assert_eq!(data_state_str(SubscriptionQuotaDataState::Absent), "absent");
        assert_eq!(
            data_state_str(SubscriptionQuotaDataState::Unobserved),
            "unobserved"
        );
    }

    #[test]
    fn proxy_tokens_includes_cache_creation_and_read_tokens() {
        let rollup = UsageRollup {
            resolution: UsageRollupResolution::Minute,
            bucket_start: 0,
            principal: String::new(),
            upstream_id: Uuid::from_u128(1),
            upstream_name: String::new(),
            model: String::new(),
            request_count: 1,
            input_tokens: 50,
            output_tokens: 200,
            cache_creation_input_tokens: 1_000,
            cache_read_input_tokens: 40_000,
            error_count: 0,
            latency_count: 0,
            latency_ms_sum: 0,
            latency_ms_min: None,
            latency_ms_max: None,
            proxy_setup_ms_count: 0,
            proxy_setup_ms_sum: 0,
            shape_ms_count: 0,
            shape_ms_sum: 0,
            sign_ms_count: 0,
            sign_ms_sum: 0,
            upstream_ttfb_ms_count: 0,
            upstream_ttfb_ms_sum: 0,
            upstream_body_ms_count: 0,
            upstream_body_ms_sum: 0,
            virtual_cost_micros: 0,
        };

        assert_eq!(proxy_tokens(&rollup), 41_250);
    }

    #[test]
    fn reset_cycle_splitter_detects_resets_at_change() {
        let observations = vec![
            observation(0, 0.10, Some(1_000)),
            observation(60, 0.20, Some(1_000)),
            observation(120, 0.30, Some(1_120)),
            observation(180, 0.40, Some(1_120)),
        ];

        let cycles = split_reset_cycles(&observations);

        assert_eq!(cycles.len(), 2);
        assert_eq!(cycles[0].observations.len(), 2);
        assert_eq!(cycles[1].observations.len(), 2);
        assert!(std::ptr::eq(
            cycles[0].observations.as_ptr(),
            observations.as_ptr()
        ));
    }

    #[test]
    fn slope_inference_returns_medium_confidence_with_five_valid_intervals() {
        let observations = (0..=5)
            .map(|idx| observation(idx * 60, 0.10 + idx as f64 * 0.05, Some(1_000)))
            .collect::<Vec<_>>();
        let cycles = split_reset_cycles(&observations);
        let intervals = valid_utilization_intervals(&cycles);

        let burn = infer_actual_account_burn(&intervals, Some(0.35), Some(3_600), 0);

        assert_eq!(burn.confidence, "medium");
        assert_eq!(burn.interval_count, 5);
        assert!(burn.utilization_per_hour.unwrap() > 2.9);
        assert!(burn.reason.is_none());
    }

    #[test]
    fn recommended_multiplier_calculates_from_synthetic_proxy_capacity() {
        let proxy_burn = ProxyBurnResponse {
            proxy_tokens_per_second: Some(200.0),
            proxy_tokens_per_hour: Some(720_000.0),
            effective_limit_tokens_estimate: Some(1_500_000.0),
            utilization_per_hour: Some(0.48),
            eta_to_limit_secs: Some(1_000),
            resets_before_limit: Some(false),
            confidence: "medium".to_owned(),
            interval_count: 5,
            reason: None,
        };

        let deficit = deficit_for_window(SubscriptionQuotaWindow::FiveHour, &proxy_burn).unwrap();

        assert_eq!(deficit.projected_proxy_tokens_window, 3_600_000.0);
        assert_eq!(deficit.shortfall_tokens, 2_100_000.0);
        assert_eq!(deficit.recommended_multiplier, 2.4);
        assert_eq!(deficit.confidence, "medium");
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
        let upstream_id = Uuid::from_u128(2);
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

        let response =
            build_aggregate_window_response(SubscriptionQuotaWindow::FiveHour, &lots, 150, 50_000);

        assert_eq!(lots.len(), 2);
        assert_eq!(response.used_tokens, 150);
        assert_eq!(response.capacity_to_now_tokens_estimate, Some(300.0));
        assert_eq!(response.utilization_percent, Some(50.0));
    }

    #[test]
    fn pool_quota_upstreams_includes_disabled_oauth_upstreams() {
        let enabled_oauth_id = Uuid::from_u128(3);
        let disabled_oauth_id = Uuid::from_u128(4);
        let non_oauth_id = Uuid::from_u128(5);
        let mut disabled_oauth = upstream_record(disabled_oauth_id);
        disabled_oauth.enabled = false;
        let mut non_oauth = upstream_record(non_oauth_id);
        non_oauth.kind = UpstreamKind::AnthropicApiKey;

        let upstreams = pool_quota_upstreams(
            vec![upstream_record(enabled_oauth_id), disabled_oauth, non_oauth],
            None,
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

    fn observation(
        at_unix_secs: u64,
        utilization: f64,
        resets_at_unix_secs: Option<u64>,
    ) -> AnalysisObservation {
        AnalysisObservation {
            bucket_start_unix_secs: at_unix_secs,
            utilization_last: utilization,
            resets_at_unix_secs_last: resets_at_unix_secs,
            observed_at_unix_millis_last: at_unix_secs * 1_000,
            sources_seen: vec![SubscriptionQuotaSource::Header],
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
    fn aggregate_window_fallback_table() {
        struct Case {
            case: &'static str,
            lots: fn() -> Vec<AggregateProviderLotResponse>,
            expected_utilization: Option<f64>,
            expected_contributing_upstreams: usize,
            expected_caveat: Option<&'static str>,
        }

        let cases = [
            Case {
                case: "aggregate_falls_back_to_header_mean_when_capacity_missing",
                lots: || {
                    vec![
                        lot("u-1", Some(0.10), Some(100_000), None, None),
                        lot("u-2", Some(0.30), Some(100_000), None, None),
                        lot("u-3", None, None, None, None),
                    ]
                },
                expected_utilization: Some(0.20),
                expected_contributing_upstreams: 2,
                expected_caveat: Some("plan ratios"),
            },
            Case {
                case: "aggregate_uses_latest_observation_per_upstream_for_fallback",
                lots: || {
                    vec![
                        lot("u-1", Some(0.40), Some(1_000), None, None),
                        lot("u-1", Some(0.60), Some(2_000), None, None),
                        lot("u-2", Some(0.20), Some(1_500), None, None),
                    ]
                },
                expected_utilization: Some(0.40),
                expected_contributing_upstreams: 2,
                expected_caveat: None,
            },
            Case {
                case: "aggregate_prefers_capacity_weighted_when_any_lot_has_capacity",
                lots: || {
                    vec![lot(
                        "u-1",
                        Some(0.50),
                        Some(1_000),
                        Some(1_000_000.0),
                        Some(500_000.0),
                    )]
                },
                expected_utilization: None,
                expected_contributing_upstreams: 1,
                expected_caveat: None,
            },
        ];

        for case in cases {
            let lots = (case.lots)();
            let response = build_aggregate_window_response(
                SubscriptionQuotaWindow::FiveHour,
                &lots,
                0,
                now_unix_secs_test(),
            );

            if let Some(expected_utilization) = case.expected_utilization {
                let utilization = response.utilization.unwrap_or_else(|| {
                    panic!("fallback should populate utilization; case={}", case.case)
                });
                assert!(
                    (utilization - expected_utilization).abs() < f64::EPSILON,
                    "case={}",
                    case.case
                );
            } else {
                assert!(response.utilization.is_some(), "case={}", case.case);
            }
            assert_eq!(response.confidence, "plan_weighted", "case={}", case.case);
            assert_eq!(
                response.contributing_upstreams, case.expected_contributing_upstreams,
                "case={}",
                case.case
            );
            if let Some(expected_caveat) = case.expected_caveat {
                assert!(
                    response
                        .caveats
                        .iter()
                        .any(|caveat| caveat.contains(expected_caveat)),
                    "case={}",
                    case.case
                );
            }
        }
    }

    fn now_unix_secs_test() -> u64 {
        1_780_000_000
    }
}
