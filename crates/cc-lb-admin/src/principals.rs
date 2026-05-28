use std::{collections::BTreeMap, time::Duration};

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use humantime::parse_duration;
use serde::{Deserialize, Serialize};

use crate::{AdminState, management::ManagementError};
use cc_lb_core::api_keys::limit_engine::{IdentityFilter, PrincipalLimitsSnapshot};
use cc_lb_storage_redb::{StorageError, UsageRollup, UsageRollupResolution};

#[derive(Debug, Clone, Deserialize)]
pub struct KeyUsageQuery {
    pub range: Option<String>,
    pub step: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardUsageResponse {
    pub range: String,
    pub step: String,
    pub group_by: String,
    pub window_start_unix_secs: u64,
    pub window_end_unix_secs: u64,
    pub series: Vec<UsageSeries>,
    pub observed: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct UsageSeries {
    pub bucket_start_unix_secs: u64,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd_micros: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PrincipalUsageQuery {
    pub range: Option<String>,
    pub step: Option<String>,
    pub group_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrincipalUsageResponse {
    pub range: &'static str,
    pub step: &'static str,
    pub group_by: &'static str,
    pub window_start_unix_secs: u64,
    pub window_end_unix_secs: u64,
    pub series: Vec<PrincipalUsageSeries>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated_series_count: Option<u64>,
    pub observed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrincipalUsageSeries {
    pub key: String,
    pub buckets: Vec<PrincipalUsageBucket>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PrincipalUsageBucket {
    pub bucket_start_unix_secs: u64,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub error_count: u64,
    pub virtual_cost_micros: u64,
    pub latency_ms_sum: u64,
    pub latency_count: u64,
}

pub async fn principal_key_usage(
    State(state): State<AdminState>,
    Path((principal_id, key_id)): Path<(String, String)>,
    Query(query): Query<KeyUsageQuery>,
) -> Result<Json<DashboardUsageResponse>, ManagementError> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;

    if state.principal_view.load().get(&principal_id).is_none() {
        return Err(ManagementError::UnknownPrincipal);
    }

    if storage.get_api_key(&principal_id, &key_id)?.is_none() {
        return Err(ManagementError::UnknownApiKey);
    }

    let range_raw = query.range.unwrap_or_else(|| "1h".to_owned());
    let step_raw = query.step.unwrap_or_else(|| "1h".to_owned());
    let range = parse_duration(&range_raw).map_err(|error| ManagementError::InvalidRequest {
        message: format!("invalid range: {error}"),
    })?;
    let max_range = Duration::from_secs(7 * 24 * 60 * 60);
    if range > max_range {
        return Err(ManagementError::InvalidRequest {
            message:
                "range exceeds maximum 7 days for per-key usage; use principal-level /usage instead"
                    .to_owned(),
        });
    }
    let step = parse_duration(&step_raw).map_err(|error| ManagementError::InvalidRequest {
        message: format!("invalid step: {error}"),
    })?;

    let now_ms = now_unix_ms()?;
    let range_ms = duration_to_ms(range).max(1);
    let step_ms = duration_to_ms(step).max(1);
    let range_start_ms = now_ms.saturating_sub(range_ms);
    let range_end_ms = now_ms;
    let bucket_count = range_ms
        .saturating_add(step_ms.saturating_sub(1))
        .div_ceil(step_ms)
        .max(1);

    let events = storage.query_request_events(range_start_ms, range_end_ms, usize::MAX)?;

    let mut aggregates: BTreeMap<u64, UsageSeries> = BTreeMap::new();
    for event in events.into_iter().filter(|event| {
        event.principal_id.as_deref() == Some(principal_id.as_str())
            && event.key_id.as_deref() == Some(key_id.as_str())
    }) {
        let event_ts_ms = event.ts_ms.unwrap_or_else(|| event.ts.saturating_mul(1000));
        let bucket_offset = event_ts_ms.saturating_sub(range_start_ms) / step_ms;
        let bucket_offset = bucket_offset.min(bucket_count - 1);
        let bucket_start_ms = range_start_ms.saturating_add(bucket_offset.saturating_mul(step_ms));
        let entry = aggregates
            .entry(bucket_start_ms)
            .or_insert_with(|| UsageSeries {
                bucket_start_unix_secs: bucket_start_ms / 1000,
                ..UsageSeries::default()
            });
        entry.request_count += 1;
        entry.input_tokens += event.input_tokens.unwrap_or(0)
            + event.cache_creation_input_tokens.unwrap_or(0)
            + event.cache_read_input_tokens.unwrap_or(0);
        entry.output_tokens += event.output_tokens.unwrap_or(0);
        entry.cost_usd_micros += event.cost_usd_micros.unwrap_or(0);
    }

    let mut series = Vec::new();
    for bucket_index in 0..bucket_count {
        let bucket_start_ms = range_start_ms.saturating_add(bucket_index.saturating_mul(step_ms));
        series.push(aggregates.remove(&bucket_start_ms).unwrap_or(UsageSeries {
            bucket_start_unix_secs: bucket_start_ms / 1000,
            ..UsageSeries::default()
        }));
    }

    Ok(Json(DashboardUsageResponse {
        range: range_raw,
        step: step_raw,
        group_by: "key".to_owned(),
        window_start_unix_secs: range_start_ms / 1000,
        window_end_unix_secs: range_end_ms / 1000,
        series,
        observed: true,
    }))
}

pub async fn principal_usage(
    State(state): State<AdminState>,
    Path(principal_id): Path<String>,
    Query(query): Query<PrincipalUsageQuery>,
) -> Response {
    if query.group_by.is_some() {
        return principal_usage_error_response(PrincipalUsageError::InvalidGroupBy);
    }

    match build_principal_usage(&state, &principal_id, query) {
        Ok(response) => Json(response).into_response(),
        Err(error) => principal_usage_error_response(error),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PrincipalLimitsQuery {
    pub identity: Option<String>,
}

pub async fn principal_limits(
    State(state): State<AdminState>,
    Path(principal_id): Path<String>,
    Query(query): Query<PrincipalLimitsQuery>,
) -> Result<Json<PrincipalLimitsSnapshot>, StatusCode> {
    let view = state.principal_view.load();
    if view.get(&principal_id).is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let identity_filter = match query.identity.as_deref().unwrap_or("all") {
        "principal" => IdentityFilter::Principal,
        "api_key" => IdentityFilter::ApiKey,
        "all" => IdentityFilter::All,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    Ok(Json(state.limit_engine.snapshot_for_principal(
        &view,
        &principal_id,
        identity_filter,
    )))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrincipalUsageRange {
    FifteenMinutes,
    OneHour,
    SixHours,
    TwentyFourHours,
    SevenDays,
}

#[derive(Debug)]
enum PrincipalUsageError {
    StorageUnavailable,
    UnknownPrincipal,
    InvalidGroupBy,
    InvalidRange,
    InvalidStep,
    StepTooFineForRange,
    Storage(StorageError),
}

#[derive(Debug)]
struct PrincipalUsageAccumulator {
    total_request_count: u64,
    buckets: Vec<PrincipalUsageBucket>,
}

const MAX_PRINCIPAL_USAGE_BUCKETS_PER_SERIES: u64 = 1_440;
const MAX_PRINCIPAL_USAGE_GROUPED_SERIES: usize = 20;

fn build_principal_usage(
    state: &AdminState,
    principal_id: &str,
    query: PrincipalUsageQuery,
) -> Result<PrincipalUsageResponse, PrincipalUsageError> {
    if state.principal_view.load().get(principal_id).is_none() {
        return Err(PrincipalUsageError::UnknownPrincipal);
    }
    let storage = state
        .storage
        .as_ref()
        .ok_or(PrincipalUsageError::StorageUnavailable)?;
    let range = parse_principal_usage_range(query.range.as_deref())?;
    let step = parse_principal_usage_step(query.step.as_deref(), range)?;
    validate_principal_usage_step(range, step)?;

    let now_unix_secs = current_unix_secs();
    let (window_start_unix_secs, window_end_unix_secs) =
        principal_usage_window(range, step, now_unix_secs);
    let rollups = storage
        .query_usage_rollups_in_range(step, window_start_unix_secs, window_end_unix_secs)
        .map_err(PrincipalUsageError::Storage)?
        .into_iter()
        .filter(|rollup| rollup.principal == principal_id)
        .collect::<Vec<_>>();
    let observed = !rollups.is_empty();
    let (series, truncated_series_count) =
        build_principal_usage_series(&rollups, window_start_unix_secs, window_end_unix_secs, step);

    Ok(PrincipalUsageResponse {
        range: range.as_str(),
        step: step.as_str(),
        group_by: "model",
        window_start_unix_secs,
        window_end_unix_secs,
        series,
        truncated_series_count,
        observed,
    })
}

fn parse_principal_usage_range(
    raw: Option<&str>,
) -> Result<PrincipalUsageRange, PrincipalUsageError> {
    match raw.unwrap_or("1h") {
        "15m" => Ok(PrincipalUsageRange::FifteenMinutes),
        "1h" => Ok(PrincipalUsageRange::OneHour),
        "6h" => Ok(PrincipalUsageRange::SixHours),
        "24h" => Ok(PrincipalUsageRange::TwentyFourHours),
        "7d" => Ok(PrincipalUsageRange::SevenDays),
        _ => Err(PrincipalUsageError::InvalidRange),
    }
}

fn parse_principal_usage_step(
    raw: Option<&str>,
    range: PrincipalUsageRange,
) -> Result<UsageRollupResolution, PrincipalUsageError> {
    match raw {
        Some("minute") => Ok(UsageRollupResolution::Minute),
        Some("hour") => Ok(UsageRollupResolution::Hour),
        Some(_) => Err(PrincipalUsageError::InvalidStep),
        None => Ok(auto_principal_usage_step(range)),
    }
}

fn auto_principal_usage_step(range: PrincipalUsageRange) -> UsageRollupResolution {
    match range {
        PrincipalUsageRange::FifteenMinutes
        | PrincipalUsageRange::OneHour
        | PrincipalUsageRange::SixHours => UsageRollupResolution::Minute,
        PrincipalUsageRange::TwentyFourHours | PrincipalUsageRange::SevenDays => {
            UsageRollupResolution::Hour
        }
    }
}

fn validate_principal_usage_step(
    range: PrincipalUsageRange,
    step: UsageRollupResolution,
) -> Result<(), PrincipalUsageError> {
    let width = principal_usage_step_width_secs(step);
    let range_secs = range.as_secs();
    let bucket_count = range_secs / width;
    if bucket_count == 0
        || !range_secs.is_multiple_of(width)
        || bucket_count > MAX_PRINCIPAL_USAGE_BUCKETS_PER_SERIES
    {
        return Err(PrincipalUsageError::StepTooFineForRange);
    }
    Ok(())
}

fn principal_usage_window(
    range: PrincipalUsageRange,
    step: UsageRollupResolution,
    now_unix_secs: u64,
) -> (u64, u64) {
    let width = principal_usage_step_width_secs(step);
    let window_end_unix_secs = now_unix_secs - (now_unix_secs % width);
    let window_start_unix_secs = window_end_unix_secs.saturating_sub(range.as_secs());
    (window_start_unix_secs, window_end_unix_secs)
}

fn build_principal_usage_series(
    rollups: &[UsageRollup],
    window_start_unix_secs: u64,
    window_end_unix_secs: u64,
    step: UsageRollupResolution,
) -> (Vec<PrincipalUsageSeries>, Option<u64>) {
    let mut groups: BTreeMap<String, PrincipalUsageAccumulator> = BTreeMap::new();
    for rollup in rollups {
        let entry =
            groups
                .entry(rollup.model.clone())
                .or_insert_with(|| PrincipalUsageAccumulator {
                    total_request_count: 0,
                    buckets: zero_filled_principal_usage_buckets(
                        window_start_unix_secs,
                        window_end_unix_secs,
                        step,
                    ),
                });
        entry.total_request_count += rollup.request_count;
        add_principal_usage_rollup_to_buckets(
            &mut entry.buckets,
            rollup,
            window_start_unix_secs,
            step,
        );
    }

    let mut ranked = groups.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_key, left), (right_key, right)| {
        right
            .total_request_count
            .cmp(&left.total_request_count)
            .then_with(|| left_key.cmp(right_key))
    });

    let truncated_count = ranked
        .len()
        .saturating_sub(MAX_PRINCIPAL_USAGE_GROUPED_SERIES);
    let series = ranked
        .into_iter()
        .take(MAX_PRINCIPAL_USAGE_GROUPED_SERIES)
        .map(|(key, group)| PrincipalUsageSeries {
            key,
            buckets: group.buckets,
        })
        .collect();
    let truncated_series_count = if truncated_count == 0 {
        None
    } else {
        Some(truncated_count as u64)
    };
    (series, truncated_series_count)
}

fn zero_filled_principal_usage_buckets(
    window_start_unix_secs: u64,
    window_end_unix_secs: u64,
    step: UsageRollupResolution,
) -> Vec<PrincipalUsageBucket> {
    let width = principal_usage_step_width_secs(step);
    let mut buckets = Vec::new();
    let mut bucket_start_unix_secs = window_start_unix_secs;
    while bucket_start_unix_secs < window_end_unix_secs {
        buckets.push(PrincipalUsageBucket {
            bucket_start_unix_secs,
            ..PrincipalUsageBucket::default()
        });
        bucket_start_unix_secs += width;
    }
    buckets
}

fn add_principal_usage_rollup_to_buckets(
    buckets: &mut [PrincipalUsageBucket],
    rollup: &UsageRollup,
    window_start_unix_secs: u64,
    step: UsageRollupResolution,
) {
    let width = principal_usage_step_width_secs(step);
    let Some(offset) = rollup.bucket_start.checked_sub(window_start_unix_secs) else {
        return;
    };
    let index = (offset / width) as usize;
    let Some(bucket) = buckets.get_mut(index) else {
        return;
    };
    if bucket.bucket_start_unix_secs != rollup.bucket_start {
        return;
    }

    bucket.request_count += rollup.request_count;
    bucket.input_tokens += rollup.input_tokens;
    bucket.output_tokens += rollup.output_tokens;
    bucket.error_count += rollup.error_count;
    bucket.virtual_cost_micros += rollup.virtual_cost_micros;
    bucket.latency_ms_sum += rollup.latency_ms_sum;
    bucket.latency_count += rollup.latency_count;
}

fn principal_usage_error_response(error: PrincipalUsageError) -> Response {
    let (status, error_code) = match error {
        PrincipalUsageError::StorageUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable")
        }
        PrincipalUsageError::UnknownPrincipal => (StatusCode::NOT_FOUND, "unknown_principal"),
        PrincipalUsageError::InvalidGroupBy => (StatusCode::BAD_REQUEST, "invalid_group_by"),
        PrincipalUsageError::InvalidRange => (StatusCode::BAD_REQUEST, "invalid_range"),
        PrincipalUsageError::InvalidStep => (StatusCode::BAD_REQUEST, "invalid_step"),
        PrincipalUsageError::StepTooFineForRange => {
            (StatusCode::BAD_REQUEST, "step_too_fine_for_range")
        }
        PrincipalUsageError::Storage(source) => {
            tracing::error!(error = %source, "principal admin usage query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "storage_error")
        }
    };
    (status, Json(serde_json::json!({ "error": error_code }))).into_response()
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl PrincipalUsageRange {
    fn as_str(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::SixHours => "6h",
            Self::TwentyFourHours => "24h",
            Self::SevenDays => "7d",
        }
    }

    fn as_secs(self) -> u64 {
        match self {
            Self::FifteenMinutes => 15 * 60,
            Self::OneHour => 60 * 60,
            Self::SixHours => 6 * 60 * 60,
            Self::TwentyFourHours => 24 * 60 * 60,
            Self::SevenDays => 7 * 24 * 60 * 60,
        }
    }
}

fn principal_usage_step_width_secs(step: UsageRollupResolution) -> u64 {
    match step {
        UsageRollupResolution::Minute => 60,
        UsageRollupResolution::Hour => 60 * 60,
    }
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn now_unix_ms() -> Result<u64, ManagementError> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ManagementError::InvalidRequest {
            message: "system clock is before unix epoch".to_owned(),
        })?
        .as_millis()
        .min(u128::from(u64::MAX)) as u64)
}
