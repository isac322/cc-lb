use cc_lb_storage_api::{
    CacheKeepaliveSessionListItem, CacheKeepaliveSessionStatus, CacheKeepaliveTerminalReason,
};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheKeepaliveViewState {
    Renewed,
    Scheduled,
    Capped,
    Expired,
    NotTracked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CacheKeepaliveReasonDisplay {
    Reason { reason: String },
    ErrorOnly { error: String },
    ErrorWithReason { error: String, reason: String },
}

pub fn state_from_item(item: &CacheKeepaliveSessionListItem) -> CacheKeepaliveViewState {
    if item.is_decision() {
        return CacheKeepaliveViewState::NotTracked;
    }
    match (item.status, item.terminal_reason) {
        (Some(CacheKeepaliveSessionStatus::Active), _) => active_state(item.refresh_count),
        (
            Some(CacheKeepaliveSessionStatus::Terminal),
            Some(CacheKeepaliveTerminalReason::Expired | CacheKeepaliveTerminalReason::CacheMiss),
        ) => CacheKeepaliveViewState::Expired,
        (
            Some(CacheKeepaliveSessionStatus::Terminal),
            Some(
                CacheKeepaliveTerminalReason::MaxRefreshes
                | CacheKeepaliveTerminalReason::MaxDuration,
            ),
        ) => CacheKeepaliveViewState::Capped,
        (Some(CacheKeepaliveSessionStatus::Terminal), _) | (None, _) => {
            active_state(item.refresh_count)
        }
    }
}

const fn active_state(refresh_count: Option<u32>) -> CacheKeepaliveViewState {
    match refresh_count {
        Some(refresh_count) if refresh_count > 0 => CacheKeepaliveViewState::Renewed,
        _ => CacheKeepaliveViewState::Scheduled,
    }
}

pub fn reason_display(reason: &str, error: Option<&str>) -> CacheKeepaliveReasonDisplay {
    match error {
        Some(error) if error_overlaps_reason(error, reason) => {
            CacheKeepaliveReasonDisplay::ErrorOnly {
                error: error.to_owned(),
            }
        }
        Some(error) => CacheKeepaliveReasonDisplay::ErrorWithReason {
            error: error.to_owned(),
            reason: reason.to_owned(),
        },
        None => CacheKeepaliveReasonDisplay::Reason {
            reason: reason.to_owned(),
        },
    }
}

pub fn next_renewal_display(
    state: CacheKeepaliveViewState,
    run_at_ms: u64,
    now_ms: u64,
) -> Option<String> {
    let label = match state {
        CacheKeepaliveViewState::Scheduled => "first renewal",
        CacheKeepaliveViewState::Renewed => "next renewal",
        CacheKeepaliveViewState::Capped
        | CacheKeepaliveViewState::Expired
        | CacheKeepaliveViewState::NotTracked => return None,
    };
    let seconds = run_at_ms.saturating_sub(now_ms) / 1_000;
    let timing = if seconds == 0 {
        "due now".to_owned()
    } else {
        format_duration(seconds)
    };
    Some(format!("{label} in {timing}"))
}

pub const fn turn_action(followed_up: bool, pending: bool) -> &'static str {
    match (followed_up, pending) {
        (_, true) => "waiting for follow-up",
        (true, false) => "cache used by follow-up",
        (false, false) => "no follow-up (loss)",
    }
}

fn error_overlaps_reason(error: &str, reason: &str) -> bool {
    let error = error.trim().to_ascii_lowercase();
    let reason = reason.trim().to_ascii_lowercase();
    !error.is_empty() && !reason.is_empty() && (error.contains(&reason) || reason.contains(&error))
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    match (hours, minutes, seconds) {
        (0, 0, seconds) => format!("{seconds}s"),
        (0, minutes, 0) => format!("{minutes}m"),
        (0, minutes, seconds) => format!("{minutes}m {seconds}s"),
        (hours, 0, 0) => format!("{hours}h"),
        (hours, minutes, 0) => format!("{hours}h {minutes}m"),
        (hours, minutes, seconds) => format!("{hours}h {minutes}m {seconds}s"),
    }
}
