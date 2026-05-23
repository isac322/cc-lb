use std::{collections::BTreeMap, time::Duration};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use humantime::parse_duration;
use serde::{Deserialize, Serialize};

use crate::{management::ManagementError, AdminState};
use cc_lb_core::api_keys::limit_engine::{IdentityFilter, PrincipalLimitsSnapshot};

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

    let events = storage.query_request_events_by_principal_in_range(
        &principal_id,
        range_start_ms,
        range_end_ms,
    )?;

    let mut aggregates: BTreeMap<u64, UsageSeries> = BTreeMap::new();
    for event in events.into_iter().filter(|event| event.key_id == key_id) {
        let bucket_offset = event.ts_ms.saturating_sub(range_start_ms) / step_ms;
        let bucket_offset = bucket_offset.min(bucket_count - 1);
        let bucket_start_ms = range_start_ms.saturating_add(bucket_offset.saturating_mul(step_ms));
        let entry = aggregates
            .entry(bucket_start_ms)
            .or_insert_with(|| UsageSeries {
                bucket_start_unix_secs: bucket_start_ms / 1000,
                ..UsageSeries::default()
            });
        entry.request_count += 1;
        entry.input_tokens +=
            event.input_tokens + event.cache_creation_input_tokens + event.cache_read_input_tokens;
        entry.output_tokens += event.output_tokens;
        entry.cost_usd_micros += event.cost_usd_micros;
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

#[derive(Debug, Clone, Deserialize)]
pub struct PrincipalLimitsQuery {
    pub identity: Option<String>,
}

pub async fn principal_limits(
    State(state): State<AdminState>,
    Path(principal_id): Path<String>,
    Query(query): Query<PrincipalLimitsQuery>,
) -> Result<Json<PrincipalLimitsSnapshot>, StatusCode> {
    if state.principal_view.load().get(&principal_id).is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let identity_filter = match query.identity.as_deref().unwrap_or("all") {
        "principal" => IdentityFilter::Principal,
        "api_key" => IdentityFilter::ApiKey,
        "all" => IdentityFilter::All,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    Ok(Json(
        state
            .limit_engine
            .snapshot_for_principal(&principal_id, identity_filter),
    ))
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
