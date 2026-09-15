use axum::{Json, http::StatusCode, response::IntoResponse};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use cc_lb_storage_api::{
    CacheKeepaliveSessionCursor, CacheKeepaliveSessionFilter, CacheKeepaliveSessionListQuery,
};
use serde::{Deserialize, Deserializer, Serialize};

use super::RawCacheKeepaliveQuery;

const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 100;
const DAY_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(super) enum CacheKeepaliveHorizon {
    #[serde(rename = "24h")]
    Hours24,
    #[serde(rename = "7d")]
    Days7,
    #[serde(rename = "all")]
    All,
}

impl CacheKeepaliveHorizon {
    fn start_ms(self, now_ms: u64) -> Option<u64> {
        match self {
            Self::Hours24 => Some(now_ms.saturating_sub(DAY_MS)),
            Self::Days7 => Some(now_ms.saturating_sub(DAY_MS.saturating_mul(7))),
            Self::All => None,
        }
    }

    fn matches_start(self, start_ms: Option<u64>) -> bool {
        match self {
            Self::Hours24 | Self::Days7 => start_ms.is_some(),
            Self::All => start_ms.is_none(),
        }
    }
}

pub(super) struct CacheKeepaliveQuery {
    pub(super) horizon: CacheKeepaliveHorizon,
    pub(super) limit: u32,
    pub(super) storage: CacheKeepaliveSessionListQuery,
}

#[derive(Default)]
enum CursorHorizonField {
    #[default]
    Missing,
    Present(CacheKeepaliveHorizon),
}

impl<'de> Deserialize<'de> for CursorHorizonField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CacheKeepaliveHorizon::deserialize(deserializer).map(Self::Present)
    }
}

#[derive(Deserialize)]
struct DecodedCursor {
    #[serde(default)]
    horizon: CursorHorizonField,
    #[serde(flatten)]
    storage: CacheKeepaliveSessionCursor,
}

#[derive(Serialize)]
struct CursorEnvelope<'a> {
    horizon: CacheKeepaliveHorizon,
    #[serde(flatten)]
    storage: &'a CacheKeepaliveSessionCursor,
}

pub(super) enum QueryError {
    Invalid(&'static str),
    CursorMismatch,
}

impl QueryError {
    pub(super) fn into_response(self) -> axum::response::Response {
        match self {
            Self::Invalid(code) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": code })),
            )
                .into_response(),
            Self::CursorMismatch => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalid_input",
                    "field": "cache_keepalive_session_cursor",
                    "reason": "cursor does not match principal, horizon, or filter",
                })),
            )
                .into_response(),
        }
    }
}

pub(super) fn parse_query(
    raw: RawCacheKeepaliveQuery,
    principal_id: &str,
    now_ms: u64,
) -> Result<CacheKeepaliveQuery, QueryError> {
    let limit = parse_limit(raw.limit.as_deref())?;
    let requested_horizon = parse_horizon(raw.horizon.as_deref())?;
    let requested_horizon_start_ms = requested_horizon.start_ms(now_ms);
    let filter = parse_filter(raw.status.as_deref(), raw.error.as_deref())?;
    let decoded_cursor = raw.cursor.as_deref().map(decode_cursor).transpose()?;
    let (horizon_start_ms, cursor) = match decoded_cursor {
        Some(decoded) => match decoded.horizon {
            CursorHorizonField::Present(cursor_horizon) => {
                if cursor_horizon != requested_horizon
                    || !cursor_horizon.matches_start(decoded.storage.horizon_start_ms)
                {
                    return Err(QueryError::CursorMismatch);
                }
                (decoded.storage.horizon_start_ms, Some(decoded.storage))
            }
            CursorHorizonField::Missing => (requested_horizon_start_ms, Some(decoded.storage)),
        },
        None => (requested_horizon_start_ms, None),
    };
    Ok(CacheKeepaliveQuery {
        horizon: requested_horizon,
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

pub(super) fn encode_cursor(
    cursor: &CacheKeepaliveSessionCursor,
    horizon: CacheKeepaliveHorizon,
) -> Result<String, QueryError> {
    if !horizon.matches_start(cursor.horizon_start_ms) {
        return Err(error_response("cache_keepalive_cursor_encode_failed"));
    }
    serde_json::to_vec(&CursorEnvelope {
        horizon,
        storage: cursor,
    })
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

fn parse_horizon(value: Option<&str>) -> Result<CacheKeepaliveHorizon, QueryError> {
    match value.unwrap_or("24h") {
        "24h" => Ok(CacheKeepaliveHorizon::Hours24),
        "7d" => Ok(CacheKeepaliveHorizon::Days7),
        "all" => Ok(CacheKeepaliveHorizon::All),
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

fn decode_cursor(value: &str) -> Result<DecodedCursor, QueryError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| error_response("invalid_cache_keepalive_cursor"))?;
    serde_json::from_slice(&bytes).map_err(|_| error_response("invalid_cache_keepalive_cursor"))
}

fn error_response(code: &'static str) -> QueryError {
    QueryError::Invalid(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW_MS: u64 = 1_730_000_100_000;

    fn raw_query(horizon: Option<&str>, cursor: Option<String>) -> RawCacheKeepaliveQuery {
        RawCacheKeepaliveQuery {
            limit: Some("1".to_owned()),
            cursor,
            horizon: horizon.map(str::to_owned),
            status: None,
            error: None,
        }
    }

    fn parse(raw: RawCacheKeepaliveQuery, principal_id: &str, now_ms: u64) -> CacheKeepaliveQuery {
        match parse_query(raw, principal_id, now_ms) {
            Ok(query) => query,
            Err(_) => panic!("query should parse"),
        }
    }

    fn encode(cursor: &CacheKeepaliveSessionCursor, horizon: CacheKeepaliveHorizon) -> String {
        match encode_cursor(cursor, horizon) {
            Ok(encoded) => encoded,
            Err(_) => panic!("cursor should encode"),
        }
    }

    #[test]
    fn tagged_cursor_reuses_the_original_horizon_start_after_time_advances() {
        let first = parse(raw_query(Some("24h"), None), "principal-a", NOW_MS);
        let original_horizon_start_ms = first.storage.horizon_start_ms;
        let storage_cursor = CacheKeepaliveSessionCursor {
            principal_id: "principal-a".to_owned(),
            horizon_start_ms: original_horizon_start_ms,
            filter: CacheKeepaliveSessionFilter::All,
            last_message_at_ms: NOW_MS - 1_000,
            entry_id: "decision:cursor-row".to_owned(),
        };
        let tagged_cursor = encode(&storage_cursor, first.horizon);

        let next = parse(
            raw_query(None, Some(tagged_cursor)),
            "principal-a",
            NOW_MS + 60 * 60 * 1_000,
        );

        assert_eq!(next.horizon, CacheKeepaliveHorizon::Hours24);
        assert_eq!(next.storage.horizon_start_ms, original_horizon_start_ms);
        assert!(next.storage.validate_cursor().is_ok());
    }

    #[test]
    fn legacy_cursor_keeps_same_cutoff_validation_while_tagged_cursor_survives_advance() {
        let first = parse(raw_query(Some("7d"), None), "principal-a", NOW_MS);
        let storage_cursor = CacheKeepaliveSessionCursor {
            principal_id: "principal-a".to_owned(),
            horizon_start_ms: first.storage.horizon_start_ms,
            filter: CacheKeepaliveSessionFilter::All,
            last_message_at_ms: NOW_MS - 1_000,
            entry_id: "session:cursor-row".to_owned(),
        };
        let legacy_cursor = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&storage_cursor).expect("legacy cursor JSON should encode"));
        let same_cutoff = parse(
            raw_query(Some("7d"), Some(legacy_cursor.clone())),
            "principal-a",
            NOW_MS,
        );
        assert!(same_cutoff.storage.validate_cursor().is_ok());

        let moved_legacy = parse(
            raw_query(Some("7d"), Some(legacy_cursor)),
            "principal-a",
            NOW_MS + 60 * 60 * 1_000,
        );
        assert!(moved_legacy.storage.validate_cursor().is_err());

        let tagged_cursor = encode(&storage_cursor, first.horizon);
        let moved_tagged = parse(
            raw_query(Some("7d"), Some(tagged_cursor)),
            "principal-a",
            NOW_MS + 60 * 60 * 1_000,
        );
        assert!(moved_tagged.storage.validate_cursor().is_ok());
        assert_eq!(
            moved_tagged.storage.horizon_start_ms,
            first.storage.horizon_start_ms
        );
    }
}
