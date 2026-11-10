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
    pub(crate) retained_successful_hits: u64,
    pub(crate) cost_saved: Option<f64>,
    pub(crate) total_spent: f64,
    pub(crate) total_avoided: Option<f64>,
    pub(crate) unlisted_sessions: u64,
    pub(crate) unlisted_renewals: u64,
    pub(crate) unlisted_spent: f64,
    pub(crate) evidence_status: String,
    pub(crate) unknown_cycles: u64,
    pub(crate) retained_from_ms: Option<u64>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_reason: Option<String>,
    pub(crate) generation: u64,
    pub(crate) upstream: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) is_active: bool,
    pub(crate) retained_renewals: u32,
    pub(crate) net_pnl: Option<f64>,
    pub(crate) total_spent: f64,
    pub(crate) total_avoided: Option<f64>,
    pub(crate) evidence_status: String,
    pub(crate) session_key_hash: String,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveDetailResponse {
    #[serde(flatten)]
    pub(crate) row: CacheKeepaliveRowResponse,
    pub(crate) renewal_tokens: u64,
    pub(crate) total_renewals_current_cycle: u32,
    pub(crate) is_last_pending: bool,
    pub(crate) turns: Vec<CacheKeepaliveTurnResponse>,
    pub(crate) gaps: Vec<CacheKeepaliveGapResponse>,
    pub(crate) config_snapshot: Option<CacheKeepaliveConfigSnapshotResponse>,
    pub(crate) raw_record: CacheKeepaliveRawRecordResponse,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveTurnResponse {
    pub(crate) turn_number: u32,
    pub(crate) renewals: u32,
    pub(crate) followed_up: Option<bool>,
    pub(crate) evidence_status: String,
    pub(crate) label: String,
    pub(crate) time_ms: u64,
    pub(crate) pnl: Option<f64>,
    pub(crate) pending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct CacheKeepaliveGapResponse {
    pub(crate) cycle_id: Option<String>,
    pub(crate) pause_at_ms: u64,
    pub(crate) cached_tokens: u64,
    pub(crate) pulses: Vec<CacheKeepalivePulseResponse>,
    pub(crate) ending_kind: &'static str,
    pub(crate) ending_at_ms: u64,
    pub(crate) total_duration_sec: u64,
    pub(crate) since_last_renewal_sec: Option<u64>,
    pub(crate) spent_usd: f64,
    pub(crate) avoided_usd: Option<f64>,
    pub(crate) net_pnl_usd: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct CacheKeepalivePulseResponse {
    pub(crate) pulse_number: u32,
    pub(crate) time_ms: u64,
    pub(crate) offset_sec: u64,
    pub(crate) since_prev_renewal_sec: Option<u64>,
    pub(crate) spent_usd: Option<f64>,
    pub(crate) hit: bool,
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
