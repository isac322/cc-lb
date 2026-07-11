use metrics::{counter, histogram};

pub const CANCELLED_TOTAL: &str = "cc_lb_cache_keepalive_cancelled_total";
pub const CLASSIFIER_DECISIONS_TOTAL: &str = "cc_lb_cache_keepalive_classifier_decisions_total";
#[allow(dead_code)]
pub const LLM_LATENCY_SECONDS: &str = "cc_lb_cache_keepalive_llm_latency_seconds";

#[derive(Clone, Copy, Debug)]
pub enum CancelReason {
    NewRequest,
    NoCacheControl,
    MaxRefreshes,
    MaxDuration,
    UserTurnDetected,
    SnapshotTooLarge,
    UpstreamGone,
    Shutdown,
}

impl CancelReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewRequest => "new_request",
            Self::NoCacheControl => "no_cache_control",
            Self::MaxRefreshes => "max_refreshes",
            Self::MaxDuration => "max_duration",
            Self::UserTurnDetected => "user_turn_detected",
            Self::SnapshotTooLarge => "snapshot_too_large",
            Self::UpstreamGone => "upstream_gone",
            Self::Shutdown => "shutdown",
        }
    }
}

pub fn record_cancelled(principal_id: &str, reason: CancelReason) {
    counter!(
        CANCELLED_TOTAL,
        "principal_id" => principal_id.to_owned(),
        "reason" => reason.as_str(),
    )
    .increment(1);
}

pub fn record_classifier_decision(decision: &'static str, source: &'static str) {
    counter!(
        CLASSIFIER_DECISIONS_TOTAL,
        "decision" => decision,
        "source" => source,
    )
    .increment(1);
}

#[allow(dead_code)]
pub fn record_llm_latency(seconds: f64) {
    histogram!(LLM_LATENCY_SECONDS).record(seconds);
}
