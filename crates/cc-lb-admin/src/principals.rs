use std::time::Duration;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use humantime::parse_duration;
use serde::{Deserialize, Serialize};

use crate::{AdminState, management::ManagementError};
use cc_lb_clock::Clock;
use cc_lb_storage_api::RequestEventKeyUsageQuery;

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
    let key_store = state
        .key_store
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;

    if state
        .dynamic_view
        .load()
        .principal_view
        .get(&principal_id)
        .is_none()
    {
        return Err(ManagementError::UnknownPrincipal);
    }

    if key_store.get(&principal_id, &key_id).await?.is_none() {
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
                "range exceeds maximum 7 days for per-key usage; use /admin/v1/dashboard/usage instead"
                    .to_owned(),
        });
    }
    let step = parse_duration(&step_raw).map_err(|error| ManagementError::InvalidRequest {
        message: format!("invalid step: {error}"),
    })?;

    let now_ms = now_unix_ms(&*state.clock)?;
    let range_ms = duration_to_ms(range).max(1);
    let step_ms = duration_to_ms(step).max(1);
    let range_start_ms = now_ms.saturating_sub(range_ms);
    let range_end_ms = now_ms;
    let bucket_count = range_ms
        .saturating_add(step_ms.saturating_sub(1))
        .div_ceil(step_ms)
        .max(1);

    let series = storage
        .request_event_key_usage(&RequestEventKeyUsageQuery {
            principal_id: principal_id.clone(),
            key_id: key_id.clone(),
            range_start_ms,
            range_end_ms,
            step_ms,
            bucket_count,
        })
        .await?
        .into_iter()
        .map(|bucket| UsageSeries {
            bucket_start_unix_secs: bucket.bucket_start_unix_secs,
            request_count: bucket.request_count,
            input_tokens: bucket.input_tokens,
            output_tokens: bucket.output_tokens,
            cost_usd_micros: bucket.cost_usd_micros,
        })
        .collect();

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

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn now_unix_ms(clock: &dyn Clock) -> Result<u64, ManagementError> {
    Ok(cc_lb_clock::unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64)
}
