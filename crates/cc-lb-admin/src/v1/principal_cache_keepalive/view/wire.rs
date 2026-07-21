use serde::Serialize;

use crate::cache_keepalive_view::status::CacheKeepaliveViewState;

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveListResponse {
    pub(crate) summary: CacheKeepaliveSummaryResponse,
    pub(crate) rows: Vec<CacheKeepaliveRowResponse>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveSummaryResponse {
    pub(crate) renewing_now: u64,
    pub(crate) sessions_last_5m: u64,
    pub(crate) renewals_fired: u64,
    pub(crate) cost_saved: f64,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveRowResponse {
    pub(crate) id: String,
    pub(crate) last_message_at_ms: u64,
    pub(crate) state: CacheKeepaliveViewState,
    pub(crate) ttl: Option<&'static str>,
    pub(crate) attempts: Option<u32>,
    pub(crate) max_attempts: u32,
    pub(crate) reason: String,
    pub(crate) generation: u64,
    pub(crate) upstream: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) net_pnl: f64,
    pub(crate) session_key_hash: String,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveDetailResponse {
    #[serde(flatten)]
    pub(crate) row: CacheKeepaliveRowResponse,
    pub(crate) renewal_tokens: u64,
    pub(crate) total_avoided: f64,
    pub(crate) total_spent: f64,
    pub(crate) total_renewals: u32,
    pub(crate) is_last_pending: bool,
    pub(crate) turns: Vec<CacheKeepaliveTurnResponse>,
    pub(crate) config_snapshot: Option<CacheKeepaliveConfigSnapshotResponse>,
    pub(crate) raw_record: CacheKeepaliveRawRecordResponse,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveTurnResponse {
    pub(crate) turn_number: u32,
    pub(crate) renewals: u32,
    pub(crate) followed_up: bool,
    pub(crate) label: String,
    pub(crate) time_ms: u64,
    pub(crate) pnl: Option<f64>,
    pub(crate) pending: bool,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveConfigSnapshotResponse {
    pub(crate) lead_5m: u32,
    pub(crate) lead_1h: u32,
    pub(crate) max_renewals: u32,
    pub(crate) max_duration: u64,
    pub(crate) snapshot_bytes: u32,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveRawRecordResponse {
    pub(crate) session_key_hash: String,
    pub(crate) principal_id: String,
    pub(crate) upstream_id: Option<String>,
    pub(crate) generation: u64,
    pub(crate) renewal_count: u32,
    pub(crate) ttl: Option<&'static str>,
    pub(crate) status: String,
    pub(crate) enqueue_state: Option<String>,
}
