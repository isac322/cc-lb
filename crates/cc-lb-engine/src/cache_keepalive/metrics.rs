use metrics::counter;

pub const CANCELLED_TOTAL: &str = "cc_lb_cache_keepalive_cancelled_total";
pub const CANCEL_ERRORS_TOTAL: &str = "cc_lb_cache_keepalive_cancel_errors_total";
pub const DISPATCH_SKIPPED_TOTAL: &str = "cc_lb_cache_keepalive_dispatch_skipped_total";
pub const CLASSIFIER_DECISIONS_TOTAL: &str = "cc_lb_cache_keepalive_classifier_decisions_total";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelReason {
    NewRequest,
    UserTurnDetected,
    AmbiguousTurn,
    RetentionRecorded,
    RenewalNotBeneficial,
    SessionIdentityChanged,
    SnapshotTooLarge,
    CachedPrefixTooSmall,
    ZeroTokenRenewalUnsupported,
}

impl CancelReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewRequest => "new_request",
            Self::UserTurnDetected => "user_turn_detected",
            Self::AmbiguousTurn => "ambiguous_turn",
            Self::RetentionRecorded => "retention_recorded",
            Self::RenewalNotBeneficial => "renewal_not_beneficial",
            Self::SessionIdentityChanged => "session_identity_changed",
            Self::SnapshotTooLarge => "snapshot_too_large",
            Self::CachedPrefixTooSmall => "cached_prefix_too_small",
            Self::ZeroTokenRenewalUnsupported => "zero_token_renewal_unsupported",
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

/// Records a cancellation that could not reach durable storage.
///
/// A fence that finds no active generation is the normal outcome for most
/// real requests, so it is not counted here.
pub fn record_cancel_error() {
    counter!(CANCEL_ERRORS_TOTAL).increment(1);
}

/// Records a claimed renewal that was skipped before provider dispatch.
pub fn record_dispatch_skipped(reason: &'static str) {
    counter!(DISPATCH_SKIPPED_TOTAL, "reason" => reason).increment(1);
}

pub fn record_classifier_decision(decision: &'static str) {
    counter!(
        CLASSIFIER_DECISIONS_TOTAL,
        "decision" => decision,
        "source" => "heuristic",
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusBuilder;

    #[test]
    fn cancellation_vocabulary_matches_shared_web_contract() {
        let reasons = [
            CancelReason::NewRequest,
            CancelReason::UserTurnDetected,
            CancelReason::AmbiguousTurn,
            CancelReason::RetentionRecorded,
            CancelReason::RenewalNotBeneficial,
            CancelReason::SessionIdentityChanged,
            CancelReason::SnapshotTooLarge,
            CancelReason::CachedPrefixTooSmall,
            CancelReason::ZeroTokenRenewalUnsupported,
        ];
        // The source declaration guards completeness without adding a public
        // enum-iteration API used only by tests. A new variant must join this
        // serializer contract, not merely the as_str match.
        let declaration = include_str!("metrics.rs")
            .split_once("pub enum CancelReason {")
            .expect("canonical cancellation enum")
            .1
            .split_once('}')
            .expect("unit enum closing brace")
            .0;
        let declared: Vec<&str> = declaration
            .lines()
            .map(str::trim)
            .filter_map(|line| line.strip_suffix(','))
            .collect();
        let enumerated: Vec<String> = reasons.iter().map(|reason| format!("{reason:?}")).collect();
        assert_eq!(enumerated, declared, "enumerate every canonical variant");

        let contract: serde_json::Value = serde_json::from_str(include_str!(
            "../../../cc-lb-admin/web/src/lib/__contract__/cache_keepalive_vocabulary.json"
        ))
        .expect("shared keepalive vocabulary JSON");
        let wire: Vec<&str> = reasons.iter().map(|reason| reason.as_str()).collect();
        assert_eq!(serde_json::json!(wire), contract["cancel_reasons"]);
    }

    #[test]
    fn cancel_error_and_dispatch_skip_counters_are_distinct() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            record_cancel_error();
            record_dispatch_skipped("not_authorized");
        });
        let rendered = handle.render();
        assert!(rendered.contains("cc_lb_cache_keepalive_cancel_errors_total"));
        assert!(rendered.contains("cc_lb_cache_keepalive_dispatch_skipped_total"));
        assert!(rendered.contains("reason=\"not_authorized\""));
        assert!(!rendered.contains("cc_lb_cache_keepalive_cancelled_total"));
    }
}
