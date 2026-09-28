use metrics::counter;

pub const CANCELLED_TOTAL: &str = "cc_lb_cache_keepalive_cancelled_total";
pub const CLASSIFIER_DECISIONS_TOTAL: &str = "cc_lb_cache_keepalive_classifier_decisions_total";

#[derive(Clone, Copy, Debug)]
pub enum CancelReason {
    UserTurnDetected,
    SnapshotTooLarge,
}

impl CancelReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UserTurnDetected => "user_turn_detected",
            Self::SnapshotTooLarge => "snapshot_too_large",
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

pub fn record_classifier_decision(decision: &'static str) {
    counter!(
        CLASSIFIER_DECISIONS_TOTAL,
        "decision" => decision,
        "source" => "heuristic",
    )
    .increment(1);
}
