#![allow(clippy::result_large_err, clippy::manual_clamp)]

mod credits;
mod series;

use std::collections::{BTreeMap, HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use cc_lb_control::gemini_period_unknown_quota::{
    GeminiQuotaGroupKind, GeminiRawMeterOrigin, load_gemini_period_unknown_quota,
};
use cc_lb_control::subscription_quotas::{
    AggregateBuildParams, AggregateResponse, POOL_SNAPSHOT_HEARTBEAT_SECS,
    build_cc_lb_aggregate_response_for_provider, build_provider_pool_quota_snapshots,
    current_quota_observation, data_state_str, index_current_quota_samples,
    list_all_upstreams_storage, now_unix_millis, pool_provider_kind, pool_provider_weighting,
    pool_quota_upstreams, provider_quota_windows, provider_supports_window,
    snapshot_matches_source, sources_for_merge,
};
use cc_lb_domain::{
    SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaCredits, SubscriptionQuotaDataState,
};
use cc_lb_storage_api::{
    PoolQuotaChartPointRecord, PoolQuotaSnapshotSummaryRecord, Provider as PoolQuotaProvider,
    Storage, StorageError, SubscriptionQuotaBucket, SubscriptionQuotaSample,
    SubscriptionQuotaSampleKind, SubscriptionQuotaSeriesQuery, SubscriptionQuotaSource,
    SubscriptionQuotaSourceMerge, SubscriptionQuotaWindow, UpstreamRecord, upstream::UpstreamKind,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::AdminState;
use series::list_subscription_quota_series;

const DEFAULT_SERIES_BUCKET_SECS: u64 = 300;
const DEFAULT_SERIES_MAX_POINTS: u32 = 1_000;
const MAX_SERIES_MAX_POINTS: u32 = 10_000;
const MAX_SERIES_UPSTREAMS: usize = 50;
const RESET_DROP_THRESHOLD: f64 = 0.5;
const GAP_MARKER_MULTIPLIER: u64 = 2;
const POOL_HISTORY_GAP_SLACK_SECS: i64 = 300;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/subscription-quotas/latest", get(handle_latest))
        .route("/admin/v1/subscription-quotas/series", get(handle_series))
        .route(
            "/admin/v1/subscription-quotas/credit-history",
            get(credits::handle_history),
        )
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    period_unknown_meters: Vec<LatestPeriodUnknownMeterResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    raw_summary: Option<LatestRawQuotaResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    raw_fallback: Option<LatestRawQuotaResponse>,
}

/// Opaque provider evidence with its own successful observation time.
#[derive(Debug, Serialize, Deserialize)]
struct LatestRawQuotaResponse {
    observed_at_unix_millis: u64,
    body: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct LatestPeriodUnknownMeterResponse {
    group: GeminiQuotaGroupKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bucket_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    known_period: Option<String>,
    remaining_fraction: f64,
    utilization: f64,
    reset_time_unix_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remaining_amount: Option<String>,
    origin: GeminiRawMeterOrigin,
    source: String,
    state: String,
    observed_at_unix_millis: u64,
    age_secs: u64,
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

#[derive(Debug, Serialize, Deserialize)]
struct AggregateQuery {
    upstream_ids: Option<String>,
    windows: Option<String>,
    source: Option<String>,
    max_staleness_secs: Option<u64>,
    provider: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    credits: Option<credits::CreditPoolHistory>,
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
    let history_now_unix_millis = now_unix_millis(&*state.clock);
    let now_unix_secs = (history_now_unix_millis / 1_000) as i64;
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
    if provider == PoolQuotaProvider::Anthropic {
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
        let durable_by_window = index_current_quota_samples(
            &latest,
            &sources_for_merge(SubscriptionQuotaSourceMerge::Merged),
        );
        let mut absent_counts = HashMap::<SubscriptionQuotaWindow, usize>::new();
        for upstream in &upstreams {
            let snapshots = view.subscription_quota_cache.snapshot_for_upstream(
                upstream.id,
                now,
                max_staleness,
            );
            for window in &windows {
                if current_quota_observation(
                    &durable_by_window,
                    (upstream.id, *window),
                    &snapshots,
                    SubscriptionQuotaSourceMerge::Merged,
                    now,
                    max_staleness,
                )
                .is_some_and(|observation| observation.absent)
                {
                    *absent_counts.entry(*window).or_default() += 1;
                }
            }
        }
        windows.retain(|window| {
            upstreams.is_empty()
                || absent_counts.get(window).copied().unwrap_or(0) != upstreams.len()
        });
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
    let credits = if provider == PoolQuotaProvider::Openai {
        let upstreams = pool_quota_upstreams(
            list_all_upstreams(storage).await?,
            None,
            UpstreamKind::OpenaiOauth,
        );
        Some(
            credits::build_history(
                state,
                upstreams,
                since_unix_secs.max(0) as u64,
                until_unix_secs.max(0) as u64,
                history_now_unix_millis,
                query
                    .max_points_per_series
                    .unwrap_or(DEFAULT_SERIES_MAX_POINTS as usize),
            )
            .await?
            .pool,
        )
    } else {
        None
    };

    Ok(PoolHistoryResponse {
        now_unix_secs,
        weighting: pool_provider_weighting(provider),
        windows: response_windows,
        credits,
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
    let include_period_unknown_meters = source != SubscriptionQuotaSourceMerge::Header
        && (query.windows.is_none()
            || windows.iter().any(|window| {
                matches!(
                    window,
                    SubscriptionQuotaWindow::GeminiFiveHour
                        | SubscriptionQuotaWindow::GeminiSevenDay
                        | SubscriptionQuotaWindow::ThirdPartyFiveHour
                        | SubscriptionQuotaWindow::ThirdPartySevenDay
                )
            }));
    let upstreams = upstreams_for_optional_query(storage, query.upstream_ids.as_deref()).await?;
    let dynamic_view = state.dynamic_view.load();
    let db_only_split = state.dynamic_view_rebinder.is_none();
    let mut durable_latest_by_upstream = HashMap::<Uuid, Vec<SubscriptionQuotaSample>>::new();
    if db_only_split || source != SubscriptionQuotaSourceMerge::Header {
        let durable_ids = upstreams
            .iter()
            .filter(|upstream| {
                matches!(
                    upstream.kind,
                    UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
                ) || (db_only_split && upstream.kind == UpstreamKind::AnthropicOauth)
            })
            .map(|upstream| upstream.id)
            .collect::<Vec<_>>();
        if !durable_ids.is_empty() {
            // A serving replica may have a cold cache after a restart, and a
            // DB-only split admin process has no local cache at all. Load durable
            // latest rows as the read-side source/fallback while cache
            // publication preserves the existing timestamp fence.
            for sample in storage
                .list_latest_subscription_quota_for_upstreams(&durable_ids)
                .await
                .map_err(storage_error)?
            {
                dynamic_view
                    .subscription_quota_cache
                    .upsert_durable_if_newer(&sample);
                durable_latest_by_upstream
                    .entry(sample.upstream_id)
                    .or_default()
                    .push(sample);
            }
        }
    }
    let mut response_upstreams = Vec::with_capacity(upstreams.len());

    for upstream in upstreams {
        let mut snapshots = dynamic_view.subscription_quota_cache.snapshot_for_upstream(
            upstream.id,
            now_unix_millis,
            max_staleness_secs,
        );
        let cache_has_no_observations = snapshots.is_empty()
            || (db_only_split
                && snapshots
                    .iter()
                    .all(|snapshot| snapshot.state == SubscriptionQuotaDataState::Unobserved));
        if cache_has_no_observations && !provider_quota_windows(upstream.kind).is_empty() {
            let durable_samples = durable_latest_by_upstream
                .get(&upstream.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            snapshots =
                cc_lb_control::subscription_quota_snapshots::candidate_snapshots_from_samples(
                    durable_samples,
                    now_unix_millis,
                    max_staleness_secs,
                );
        }
        let mut period_unknown_meters = Vec::new();
        let mut raw_summary = None;
        let mut raw_fallback = None;
        if upstream.kind == UpstreamKind::GeminiOauth && include_period_unknown_meters {
            let mut credential_profile = None;
            let record = load_gemini_period_unknown_quota(storage, upstream.id)
                .await
                .map_err(storage_error)?
                .filter(|record| match upstream.oauth_credentials.as_ref() {
                    Some(credentials) => credentials
                        .decrypt(&state.aead, upstream.id.as_bytes())
                        .map(|bundle| {
                            credential_profile = Some(bundle.oauth_profile);
                            record.binding.matches_credential(
                                bundle.account_id.as_deref(),
                                bundle.project_id.as_deref(),
                                upstream.oauth_token_generation,
                            )
                        })
                        .unwrap_or(false),
                    None => record.binding.is_unscoped(),
                });
            if let Some(record) = record {
                // Raw provider bodies require a real owner binding; legacy
                // unscoped meter records remain displayable, but never raw JSON.
                if record.binding.identity_sha256.is_some()
                    && let Some(responses) = record.raw_responses
                    && responses.oauth_profile.is_some()
                    && responses.oauth_profile == credential_profile
                {
                    raw_summary = responses
                        .summary
                        .zip(responses.summary_observed_at_unix_millis)
                        .map(|(body, observed_at_unix_millis)| LatestRawQuotaResponse {
                            observed_at_unix_millis,
                            body,
                        });
                    raw_fallback = responses
                        .fallback
                        .zip(responses.fallback_observed_at_unix_millis)
                        .map(|(body, observed_at_unix_millis)| LatestRawQuotaResponse {
                            observed_at_unix_millis,
                            body,
                        });
                }
                for (group, group_record, canonical_windows) in [
                    (
                        GeminiQuotaGroupKind::Gemini,
                        record.gemini,
                        ["gemini_5h", "gemini_7d"],
                    ),
                    (
                        GeminiQuotaGroupKind::ThirdParty,
                        record.third_party,
                        ["third_party_5h", "third_party_7d"],
                    ),
                ] {
                    let Some(group_record) = group_record else {
                        continue;
                    };
                    let latest_canonical_observed_at = snapshots
                        .iter()
                        .filter(|snapshot| {
                            snapshot.state != SubscriptionQuotaDataState::Unobserved
                                && canonical_windows.contains(&snapshot.window.as_str())
                        })
                        .filter_map(|snapshot| snapshot.observed_at_unix_millis)
                        .chain(
                            durable_latest_by_upstream
                                .get(&upstream.id)
                                .into_iter()
                                .flatten()
                                .filter(|sample| {
                                    canonical_windows.contains(&sample.window.as_str())
                                })
                                .map(|sample| sample.observed_at_unix_millis),
                        )
                        .max();
                    // Same-poll absence and raw meters share a timestamp; only a
                    // strictly newer canonical observation supersedes the raw group.
                    if latest_canonical_observed_at.is_some_and(|observed_at| {
                        observed_at > group_record.observed_at_unix_millis
                    }) {
                        continue;
                    }
                    let age_millis =
                        now_unix_millis.saturating_sub(group_record.observed_at_unix_millis);
                    let state = if age_millis <= max_staleness_secs.saturating_mul(1_000) {
                        "fresh"
                    } else {
                        "stale"
                    };
                    period_unknown_meters.extend(group_record.meters.into_iter().map(|meter| {
                        LatestPeriodUnknownMeterResponse {
                            group,
                            model_id: meter.model_id,
                            bucket_id: meter.bucket_id,
                            known_period: meter.known_period,
                            remaining_fraction: meter.remaining_fraction,
                            utilization: (1.0 - meter.remaining_fraction).clamp(0.0, 1.0),
                            reset_time_unix_secs: meter.reset_time_unix_secs,
                            remaining_amount: meter.remaining_amount,
                            origin: meter.origin,
                            source: "api".to_owned(),
                            state: state.to_owned(),
                            observed_at_unix_millis: group_record.observed_at_unix_millis,
                            age_secs: age_millis / 1_000,
                        }
                    }));
                }
            }
        }
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
            period_unknown_meters,
            raw_summary,
            raw_fallback,
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

async fn list_all_upstreams(storage: &dyn Storage) -> Result<Vec<UpstreamRecord>, Response> {
    list_all_upstreams_storage(storage)
        .await
        .map_err(storage_error)
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

fn upstream_name_map(upstreams: &[UpstreamRecord]) -> HashMap<Uuid, String> {
    upstreams
        .iter()
        .map(|upstream| (upstream.id, upstream.name.clone()))
        .collect()
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
