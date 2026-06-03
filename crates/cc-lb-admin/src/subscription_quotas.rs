#![allow(clippy::result_large_err, clippy::manual_clamp)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use cc_lb_plugin_api::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_storage_api::{
    Storage, StorageError, SubscriptionQuotaBucket, SubscriptionQuotaSeriesQuery,
    SubscriptionQuotaSource, SubscriptionQuotaSourceMerge, SubscriptionQuotaStatus,
    SubscriptionQuotaWindow, UpstreamRecord, UpstreamStore, UsageRollup, UsageRollupResolution,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;

const STORE_PAGE_LIMIT: usize = 1_000;
const DEFAULT_SERIES_BUCKET_SECS: u64 = 300;
const DEFAULT_SERIES_MAX_POINTS: u32 = 1_000;
const MAX_SERIES_MAX_POINTS: u32 = 10_000;
const MAX_SERIES_UPSTREAMS: usize = 50;
const ANALYSIS_BUCKET_SECS: u64 = 60;
const ANALYSIS_MAX_POINTS: u32 = 10_000;
const RESET_DROP_THRESHOLD: f64 = 0.5;
const GAP_MARKER_MULTIPLIER: u64 = 2;
const CYCLE_GAP_MULTIPLIER: u64 = 4;
const PROXY_RATE_LOOKBACK_SECS: u64 = 3_600;
const CAPACITY_CAVEAT: &str = "capacity is inferred from proxy tokens and quota utilization; Anthropic quota units are not directly exposed";
const OUTSIDE_TRAFFIC_CAVEAT: &str =
    "actual_account_burn includes traffic outside this cc-lb instance";
const HEADER_ONLY_CAVEAT: &str =
    "analysis is limited to header-derived observations; API polling data is sparse";
const STALE_DATA_CAVEAT: &str =
    "latest observation is older than max_staleness_secs; analysis may be outdated";

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/subscription-quotas/latest", get(handle_latest))
        .route("/admin/subscription-quotas/latest", get(handle_latest))
        .route("/admin/v1/subscription-quotas/series", get(handle_series))
        .route("/admin/subscription-quotas/series", get(handle_series))
        .route(
            "/admin/v1/subscription-quotas/analysis",
            get(handle_analysis),
        )
        .route("/admin/subscription-quotas/analysis", get(handle_analysis))
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
    surpassed_threshold: Option<bool>,
    representative_claim: Option<String>,
    disabled_reason: Option<String>,
    extra_usage_enabled: Option<bool>,
    extra_usage_monthly_limit: Option<f64>,
    extra_usage_used_credits: Option<f64>,
    observed_at_unix_millis: Option<u64>,
    age_secs: Option<u64>,
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

#[derive(Debug, Serialize, Deserialize)]
struct SeriesBucketResponse {
    bucket_start_unix_secs: u64,
    observed: bool,
    sample_count: u32,
    utilization_min: Option<f64>,
    utilization_avg: Option<f64>,
    utilization_max: Option<f64>,
    utilization_last: Option<f64>,
    status_last: Option<String>,
    resets_at_unix_secs_last: Option<u64>,
    observed_at_unix_millis_last: Option<u64>,
    sources_seen: Vec<String>,
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
    sample_count: usize,
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
    sample_count: usize,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObservationCycle {
    observations: Vec<AnalysisObservation>,
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
    let now_unix_millis = now_unix_millis();
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
    )?;

    let upstreams = upstreams_for_optional_query(storage, query.upstream_ids.as_deref()).await?;
    validate_upstream_count(upstreams.len())?;
    let upstream_names = upstream_name_map(&upstreams);
    let upstream_ids = upstreams
        .into_iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();

    let series = storage
        .list_subscription_quota_series(SubscriptionQuotaSeriesQuery {
            upstream_ids,
            windows,
            sources: sources_for_merge(source),
            since_unix_millis: query.since_unix_secs.saturating_mul(1_000),
            until_unix_millis: query.until_unix_secs.saturating_mul(1_000),
            bucket_secs,
            max_points_per_series,
            source_merge: source,
        })
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
    let upstreams = upstreams_for_optional_query(storage, query.upstream_ids.as_deref()).await?;
    validate_upstream_count(upstreams.len())?;
    let requested_upstream_ids: Vec<Uuid> = upstreams.iter().map(|u| u.id).collect();
    let upstream_names = upstream_name_map(&upstreams);
    let rollups = storage
        .query_usage_rollups_in_range(
            UsageRollupResolution::Minute,
            query.since_unix_secs,
            query.until_unix_secs,
        )
        .await
        .map_err(storage_error)?;
    let quota_series = storage
        .list_subscription_quota_series(SubscriptionQuotaSeriesQuery {
            upstream_ids: requested_upstream_ids.clone(),
            windows: windows.clone(),
            sources: sources_for_merge(source),
            since_unix_millis: query.since_unix_secs.saturating_mul(1_000),
            until_unix_millis: query.until_unix_secs.saturating_mul(1_000),
            bucket_secs: ANALYSIS_BUCKET_SECS,
            max_points_per_series: ANALYSIS_MAX_POINTS,
            source_merge: source,
        })
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
        let observations = analysis_observations(&series.buckets);
        let latest = latest_by_upstream_window.get(&(series.upstream_id, series.window));
        let rollups_for_upstream = rollups
            .iter()
            .filter(|rollup| rollup.upstream_id == series.upstream_id)
            .cloned()
            .collect::<Vec<_>>();
        let response = build_analysis_window(
            series.window,
            latest,
            &observations,
            &rollups_for_upstream,
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
        .unwrap_or_else(|| "missing".to_owned());
    let cycles = split_reset_cycles(observations, ANALYSIS_BUCKET_SECS);
    let intervals = valid_utilization_intervals(&cycles, ANALYSIS_BUCKET_SECS);
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
            sample_count: 0,
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
        confidence: confidence_for_sample_count(intervals.len()).to_owned(),
        sample_count: intervals.len(),
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
            sample_count: 0,
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
        confidence: confidence_for_sample_count(intervals.len()).to_owned(),
        sample_count: intervals.len(),
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
        | SubscriptionQuotaWindow::SevenDayOpus => 7 * 24 * 3_600,
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

fn split_reset_cycles(
    observations: &[AnalysisObservation],
    bucket_secs: u64,
) -> Vec<ObservationCycle> {
    let mut sorted = observations.to_vec();
    sorted.sort_by_key(|observation| observation.observed_at_unix_millis_last);
    let mut cycles = Vec::new();
    let mut current = Vec::new();

    for observation in sorted {
        if let Some(previous) = current.last()
            && starts_new_cycle(previous, &observation, bucket_secs)
        {
            cycles.push(ObservationCycle {
                observations: std::mem::take(&mut current),
            });
        }
        current.push(observation);
    }

    if !current.is_empty() {
        cycles.push(ObservationCycle {
            observations: current,
        });
    }
    cycles
}

fn starts_new_cycle(
    previous: &AnalysisObservation,
    current: &AnalysisObservation,
    bucket_secs: u64,
) -> bool {
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
    let elapsed_secs = current
        .observed_at_unix_millis_last
        .saturating_sub(previous.observed_at_unix_millis_last)
        / 1_000;
    reset_changed
        || synthetic_reset
        || elapsed_secs > CYCLE_GAP_MULTIPLIER.saturating_mul(bucket_secs)
}

fn valid_utilization_intervals(
    cycles: &[ObservationCycle],
    bucket_secs: u64,
) -> Vec<UtilizationInterval> {
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
            if delta_utilization <= 0.0
                || delta_time_secs == 0
                || delta_time_secs > CYCLE_GAP_MULTIPLIER.saturating_mul(bucket_secs)
            {
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

fn analysis_observations(buckets: &[SubscriptionQuotaBucket]) -> Vec<AnalysisObservation> {
    buckets
        .iter()
        .filter(|bucket| bucket.observed)
        .filter_map(|bucket| {
            Some(AnalysisObservation {
                bucket_start_unix_secs: bucket.bucket_start_unix_secs,
                utilization_last: bucket.utilization_last?,
                resets_at_unix_secs_last: bucket.resets_at_unix_secs_last,
                observed_at_unix_millis_last: bucket.observed_at_unix_millis_last?,
                sources_seen: bucket.sources_seen.clone(),
            })
        })
        .collect()
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
    let all = list_all_upstreams(storage).await?;
    Ok(match requested_ids {
        Some(ids) => {
            let requested = ids.into_iter().collect::<HashSet<_>>();
            all.into_iter()
                .filter(|upstream| requested.contains(&upstream.id))
                .collect()
        }
        None => all,
    })
}

async fn list_all_upstreams(storage: &dyn Storage) -> Result<Vec<UpstreamRecord>, Response> {
    let mut after = None;
    let mut all = Vec::new();
    loop {
        let page = UpstreamStore::list(storage, after, STORE_PAGE_LIMIT)
            .await
            .map_err(storage_error)?;
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
    }
}

fn series_bucket_response(bucket: &SubscriptionQuotaBucket) -> SeriesBucketResponse {
    SeriesBucketResponse {
        bucket_start_unix_secs: bucket.bucket_start_unix_secs,
        observed: bucket.observed,
        sample_count: bucket.sample_count,
        utilization_min: bucket.utilization_min,
        utilization_avg: bucket.utilization_avg,
        utilization_max: bucket.utilization_max,
        utilization_last: bucket.utilization_last,
        status_last: bucket.status_last.map(status_str).map(str::to_owned),
        resets_at_unix_secs_last: bucket.resets_at_unix_secs_last,
        observed_at_unix_millis_last: bucket.observed_at_unix_millis_last,
        sources_seen: bucket
            .sources_seen
            .iter()
            .map(|source| source.as_str().to_owned())
            .collect(),
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
        return Err(bad_request(
            "bucket_range_too_large",
            "increase bucket_secs or max_points_per_series; requested range exceeds max_points_per_series * 2 buckets",
        ));
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
    rollup.input_tokens.saturating_add(rollup.output_tokens)
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

fn confidence_for_sample_count(count: usize) -> &'static str {
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
        SubscriptionQuotaDataState::Missing => "missing",
    }
}

fn status_str(status: SubscriptionQuotaStatus) -> &'static str {
    match status {
        SubscriptionQuotaStatus::Allowed => "allowed",
        SubscriptionQuotaStatus::AllowedWarning => "allowed_warning",
        SubscriptionQuotaStatus::Rejected => "rejected",
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

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_cycle_splitter_detects_resets_at_change() {
        let observations = vec![
            observation(0, 0.10, Some(1_000)),
            observation(60, 0.20, Some(1_000)),
            observation(120, 0.30, Some(1_120)),
            observation(180, 0.40, Some(1_120)),
        ];

        let cycles = split_reset_cycles(&observations, 60);

        assert_eq!(cycles.len(), 2);
        assert_eq!(cycles[0].observations.len(), 2);
        assert_eq!(cycles[1].observations.len(), 2);
    }

    #[test]
    fn slope_inference_returns_medium_confidence_with_five_valid_intervals() {
        let observations = (0..=5)
            .map(|idx| observation(idx * 60, 0.10 + idx as f64 * 0.05, Some(1_000)))
            .collect::<Vec<_>>();
        let cycles = split_reset_cycles(&observations, 60);
        let intervals = valid_utilization_intervals(&cycles, 60);

        let burn = infer_actual_account_burn(&intervals, Some(0.35), Some(3_600), 0);

        assert_eq!(burn.confidence, "medium");
        assert_eq!(burn.sample_count, 5);
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
            sample_count: 5,
            reason: None,
        };

        let deficit = deficit_for_window(SubscriptionQuotaWindow::FiveHour, &proxy_burn).unwrap();

        assert_eq!(deficit.projected_proxy_tokens_window, 3_600_000.0);
        assert_eq!(deficit.shortfall_tokens, 2_100_000.0);
        assert_eq!(deficit.recommended_multiplier, 2.4);
        assert_eq!(deficit.confidence, "medium");
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
}
