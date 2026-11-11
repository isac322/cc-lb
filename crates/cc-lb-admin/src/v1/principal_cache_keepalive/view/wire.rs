use serde::Serialize;

use crate::cache_keepalive_view::status::CacheKeepaliveViewState;

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveListResponse {
    pub(crate) summary: Option<CacheKeepaliveSummaryResponse>,
    pub(crate) groups: Option<Vec<CacheKeepaliveGroupResponse>>,
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
    /// `-unlisted_spent`, or null when any unlisted renewal has an unknown cost.
    pub(crate) unlisted_net: Option<f64>,
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
    pub(crate) cancel_reason: Option<String>,
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
    pub(crate) group_key: String,
    pub(crate) group_label: String,
    pub(crate) group_kind: &'static str,
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveGroupResponse {
    pub(crate) key: String,
    pub(crate) label: String,
    pub(crate) kind: &'static str,
    pub(crate) sessions: u64,
    pub(crate) net_pnl: Option<f64>,
    pub(crate) total_spent: f64,
    pub(crate) total_avoided: Option<f64>,
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
    pub(crate) follow_up_outcomes: CacheKeepaliveFollowUpOutcomesResponse,
    pub(crate) config_snapshot: Option<CacheKeepaliveConfigSnapshotResponse>,
    pub(crate) raw_record: CacheKeepaliveRawRecordResponse,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct CacheKeepaliveFollowUpOutcomesResponse {
    pub(crate) observed: u64,
    pub(crate) pending: u64,
    pub(crate) no_follow_up: u64,
    pub(crate) unknown: u64,
}

impl CacheKeepaliveFollowUpOutcomesResponse {
    pub(super) fn from_endings(endings: impl IntoIterator<Item = &'static str>) -> Self {
        let mut counts = Self::default();
        for ending in endings {
            let count = match ending {
                "followup_hit" => &mut counts.observed,
                "scheduled_waiting" | "followup_pending" => &mut counts.pending,
                "capped_no_followup" | "ttl_expired_no_followup" => &mut counts.no_follow_up,
                _ => &mut counts.unknown,
            };
            *count = count.saturating_add(1);
        }
        counts
    }
}

#[cfg(test)]
#[test]
fn gap_outcome_counts_preserve_every_ending_and_unknown() {
    assert_eq!(
        CacheKeepaliveFollowUpOutcomesResponse::from_endings([
            "followup_hit",
            "scheduled_waiting",
            "followup_pending",
            "capped_no_followup",
            "ttl_expired_no_followup",
            "followup_unknown",
            "future_unrecognized_ending",
        ]),
        CacheKeepaliveFollowUpOutcomesResponse {
            observed: 1,
            pending: 2,
            no_follow_up: 2,
            unknown: 2,
        }
    );
    assert_eq!(
        CacheKeepaliveFollowUpOutcomesResponse::from_endings([]),
        CacheKeepaliveFollowUpOutcomesResponse::default()
    );
}

#[derive(Serialize)]
pub(crate) struct CacheKeepaliveTurnResponse {
    pub(crate) cycle_id: Option<String>,
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
