use axum::{Json, http::StatusCode, response::IntoResponse};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use cc_lb_storage_api::{
    CacheKeepaliveSessionCursor, CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery,
};

use super::RawCacheKeepaliveQuery;

const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 100;
const DAY_MS: u64 = 24 * 60 * 60 * 1_000;

pub(super) struct CacheKeepaliveQuery {
    pub(super) limit: u32,
    pub(super) storage: CacheKeepaliveSessionListQuery,
}

pub(super) struct QueryError(&'static str);

impl QueryError {
    pub(super) fn into_response(self) -> axum::response::Response {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": self.0 })),
        )
            .into_response()
    }
}

pub(super) fn parse_query(
    raw: RawCacheKeepaliveQuery,
    principal_id: &str,
    now_ms: u64,
) -> Result<CacheKeepaliveQuery, QueryError> {
    let limit = parse_limit(raw.limit.as_deref())?;
    let horizon_start_ms = parse_horizon(raw.horizon.as_deref(), now_ms)?;
    let filter = parse_filter(raw.status.as_deref(), raw.error.as_deref())?;
    let cursor = raw.cursor.as_deref().map(decode_cursor).transpose()?;
    Ok(CacheKeepaliveQuery {
        limit,
        storage: CacheKeepaliveSessionListQuery {
            principal_id: principal_id.to_owned(),
            horizon_start_ms,
            filter,
            cursor,
            limit,
        },
    })
}

pub(super) fn encode_cursor(cursor: &CacheKeepaliveSessionCursor) -> Result<String, QueryError> {
    serde_json::to_vec(cursor)
        .map(|payload| URL_SAFE_NO_PAD.encode(payload))
        .map_err(|_| error_response("cache_keepalive_cursor_encode_failed"))
}

fn parse_limit(value: Option<&str>) -> Result<u32, QueryError> {
    let limit = value
        .map(|value| value.parse::<u32>())
        .transpose()
        .map_err(|_| error_response("invalid_cache_keepalive_limit"))?
        .unwrap_or(DEFAULT_LIMIT);
    if limit > MAX_LIMIT {
        return Err(error_response("invalid_cache_keepalive_limit"));
    }
    Ok(limit)
}

fn parse_horizon(value: Option<&str>, now_ms: u64) -> Result<Option<u64>, QueryError> {
    match value.unwrap_or("24h") {
        "24h" => Ok(Some(now_ms.saturating_sub(DAY_MS))),
        "7d" => Ok(Some(now_ms.saturating_sub(DAY_MS.saturating_mul(7)))),
        "all" => Ok(None),
        _ => Err(error_response("invalid_cache_keepalive_horizon")),
    }
}

fn parse_filter(
    status: Option<&str>,
    error: Option<&str>,
) -> Result<CacheKeepaliveSessionFilter, QueryError> {
    let status = match status.unwrap_or("all") {
        "all" => CacheKeepaliveSessionFilter::All,
        "renewed" => CacheKeepaliveSessionFilter::Renewed,
        "scheduled" => CacheKeepaliveSessionFilter::Scheduled,
        "capped" => CacheKeepaliveSessionFilter::Capped,
        "expired" => CacheKeepaliveSessionFilter::Expired,
        "not_tracked" => CacheKeepaliveSessionFilter::NotTracked,
        "error" => CacheKeepaliveSessionFilter::Error,
        _ => return Err(error_response("invalid_cache_keepalive_status")),
    };
    match error {
        None | Some("false") => Ok(status),
        Some("true") if matches!(status, CacheKeepaliveSessionFilter::All) => {
            Ok(CacheKeepaliveSessionFilter::Error)
        }
        Some("true") if matches!(status, CacheKeepaliveSessionFilter::Error) => Ok(status),
        Some("true") => Err(error_response("invalid_cache_keepalive_filter")),
        Some(_) => Err(error_response("invalid_cache_keepalive_error")),
    }
}

fn decode_cursor(value: &str) -> Result<CacheKeepaliveSessionCursor, QueryError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| error_response("invalid_cache_keepalive_cursor"))?;
    serde_json::from_slice(&bytes).map_err(|_| error_response("invalid_cache_keepalive_cursor"))
}

fn error_response(code: &'static str) -> QueryError {
    QueryError(code)
}
