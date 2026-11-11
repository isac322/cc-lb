use cc_lb_storage_api::{
    CacheKeepaliveSessionEntrySource, CacheKeepaliveSessionListItem, CacheKeepaliveSessionStatus,
    CacheKeepaliveTerminalReason, CacheTtl,
};
use uuid::Uuid;

use super::status::{
    CacheKeepaliveReasonDisplay, CacheKeepaliveViewState, next_renewal_display, reason_display,
    state_from_item, turn_action,
};

fn item(
    source: CacheKeepaliveSessionEntrySource,
    status: Option<CacheKeepaliveSessionStatus>,
    refresh_count: Option<u32>,
    terminal_reason: Option<CacheKeepaliveTerminalReason>,
) -> CacheKeepaliveSessionListItem {
    CacheKeepaliveSessionListItem {
        id: "session".to_owned(),
        source,
        session_key_hash: Some("session".to_owned()),
        principal_id: "principal".to_owned(),
        upstream_id: Uuid::nil(),
        cycle_id: None,
        last_message_at_ms: 1_730_000_000_000,
        ttl: CacheTtl::Ttl5m,
        generation: 42,
        refresh_count,
        has_renewal_in_horizon: refresh_count.is_some_and(|count| count > 0),
        status,
        enqueue_state: None,
        terminal_reason,
        cancel_reason: None,
        request_incomplete: false,
        decision: None,
        reason: "agent-in-turn (tool_use: `bash`) — first renewal in 4m 30s".to_owned(),
        display_reason: None,
        error: None,
        config_snapshot: None,
    }
}

#[test]
fn cache_keepalive_status_keeps_error_orthogonal_and_uses_frozen_display_contract() {
    // Given: active, terminal, and decision-only rows with a frozen reason.
    let renewed = item(
        CacheKeepaliveSessionEntrySource::Session,
        Some(CacheKeepaliveSessionStatus::Active),
        Some(1),
        None,
    );
    let scheduled = item(
        CacheKeepaliveSessionEntrySource::Session,
        Some(CacheKeepaliveSessionStatus::Active),
        Some(0),
        None,
    );
    let capped = item(
        CacheKeepaliveSessionEntrySource::Session,
        Some(CacheKeepaliveSessionStatus::Terminal),
        Some(12),
        Some(CacheKeepaliveTerminalReason::MaxRefreshes),
    );
    let expired = item(
        CacheKeepaliveSessionEntrySource::Session,
        Some(CacheKeepaliveSessionStatus::Terminal),
        Some(8),
        Some(CacheKeepaliveTerminalReason::Expired),
    );
    let renewed_with_error = item(
        CacheKeepaliveSessionEntrySource::Session,
        Some(CacheKeepaliveSessionStatus::Terminal),
        Some(1),
        Some(CacheKeepaliveTerminalReason::DispatchError),
    );
    let cancelled_without_refresh = item(
        CacheKeepaliveSessionEntrySource::Session,
        Some(CacheKeepaliveSessionStatus::Terminal),
        Some(0),
        Some(CacheKeepaliveTerminalReason::Cancelled),
    );
    let cancelled_after_refresh = item(
        CacheKeepaliveSessionEntrySource::Session,
        Some(CacheKeepaliveSessionStatus::Terminal),
        Some(2),
        Some(CacheKeepaliveTerminalReason::Cancelled),
    );
    let not_tracked = item(CacheKeepaliveSessionEntrySource::Decision, None, None, None);

    // When: the server derives presentation state, reason, action, and renewal timing.
    let states = [
        state_from_item(&renewed),
        state_from_item(&scheduled),
        state_from_item(&capped),
        state_from_item(&expired),
        state_from_item(&not_tracked),
    ];
    let deduplicated = reason_display(
        "renewal dispatch unavailable",
        Some("renewal dispatch unavailable"),
    );
    let separate_error =
        reason_display("max renewals reached", Some("renewal dispatch unavailable"));
    let next = next_renewal_display(CacheKeepaliveViewState::Scheduled, 270_000, 0);

    // Then: base state never becomes Error, and exact UX strings remain derivable.
    assert_eq!(
        states,
        [
            CacheKeepaliveViewState::Renewed,
            CacheKeepaliveViewState::Scheduled,
            CacheKeepaliveViewState::Capped,
            CacheKeepaliveViewState::Expired,
            CacheKeepaliveViewState::NotTracked,
        ]
    );
    assert_eq!(
        state_from_item(&renewed_with_error),
        CacheKeepaliveViewState::Failed
    );
    assert_eq!(
        state_from_item(&cancelled_without_refresh),
        CacheKeepaliveViewState::NotTracked
    );
    assert_eq!(
        state_from_item(&cancelled_after_refresh),
        CacheKeepaliveViewState::Cancelled
    );
    assert_eq!(
        deduplicated,
        CacheKeepaliveReasonDisplay::ErrorOnly {
            error: "renewal dispatch unavailable".to_owned(),
        }
    );
    assert_eq!(
        separate_error,
        CacheKeepaliveReasonDisplay::ErrorWithReason {
            error: "renewal dispatch unavailable".to_owned(),
            reason: "max renewals reached".to_owned(),
        }
    );
    assert_eq!(next.as_deref(), Some("first renewal in 4m 30s"));
    assert_eq!(
        reason_display("max duration reached (4h)", None),
        CacheKeepaliveReasonDisplay::Reason {
            reason: "max duration reached (4h)".to_owned(),
        }
    );
    assert_eq!(
        reason_display("TTL expired before follow-up", None),
        CacheKeepaliveReasonDisplay::Reason {
            reason: "TTL expired before follow-up".to_owned(),
        }
    );
    assert_eq!(
        reason_display("user turn (stop_reason=end_turn)", None),
        CacheKeepaliveReasonDisplay::Reason {
            reason: "user turn (stop_reason=end_turn)".to_owned(),
        }
    );
    assert_eq!(turn_action(false, true), "waiting for follow-up");
    assert_eq!(turn_action(true, false), "cache used by follow-up");
    assert_eq!(turn_action(false, false), "no follow-up (loss)");
}

#[test]
fn request_markers_have_neutral_states_and_no_renewal_schedule() {
    for refresh_count in [0, 2] {
        let fresh = item(
            CacheKeepaliveSessionEntrySource::Session,
            Some(CacheKeepaliveSessionStatus::InRequest),
            Some(refresh_count),
            None,
        );
        assert_eq!(state_from_item(&fresh), CacheKeepaliveViewState::InRequest);
        assert_eq!(
            next_renewal_display(state_from_item(&fresh), 300_000, 0),
            None
        );
        let mut expired = item(
            CacheKeepaliveSessionEntrySource::Session,
            Some(CacheKeepaliveSessionStatus::Terminal),
            Some(refresh_count),
            Some(CacheKeepaliveTerminalReason::Stale),
        );
        expired.request_incomplete = true;
        assert_eq!(state_from_item(&expired), CacheKeepaliveViewState::Expired);
        assert_eq!(
            next_renewal_display(state_from_item(&expired), 300_000, 0),
            None
        );
        expired.request_incomplete = false;
        assert_eq!(state_from_item(&expired), CacheKeepaliveViewState::Failed);
    }
}

#[test]
fn cancellation_requires_a_horizon_turn_independent_of_all_time_count() {
    for refresh_count in [0, 1, 12] {
        for has_turn in [false, true] {
            let mut cancelled = item(
                CacheKeepaliveSessionEntrySource::Session,
                Some(CacheKeepaliveSessionStatus::Terminal),
                Some(refresh_count),
                Some(CacheKeepaliveTerminalReason::Cancelled),
            );
            cancelled.has_renewal_in_horizon = has_turn;
            assert_eq!(
                state_from_item(&cancelled),
                if has_turn {
                    CacheKeepaliveViewState::Cancelled
                } else {
                    CacheKeepaliveViewState::NotTracked
                }
            );
            cancelled.status = Some(CacheKeepaliveSessionStatus::InRequest);
            assert_eq!(
                state_from_item(&cancelled),
                CacheKeepaliveViewState::InRequest
            );
            cancelled.status = Some(CacheKeepaliveSessionStatus::Terminal);
            cancelled.request_incomplete = true;
            assert_eq!(
                state_from_item(&cancelled),
                CacheKeepaliveViewState::Expired
            );
        }
    }
}
