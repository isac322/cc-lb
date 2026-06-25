use cc_lb_plugin_api::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_storage_api::UpstreamRecord;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{SubscriptionQuotaStatus, SubscriptionQuotaWindow, WarmupAttemptReason};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WarmupPreflightSkip {
    pub(super) reason: WarmupAttemptReason,
    pub(super) cycle_key: Option<i64>,
}

pub(super) fn warmup_preflight_skip_reason(
    upstream: &UpstreamRecord,
) -> Option<WarmupAttemptReason> {
    if upstream.deleted_at_unix_secs.is_some() {
        return Some(WarmupAttemptReason::UpstreamDeleted);
    }
    if upstream.kind != UpstreamKind::AnthropicOauth
        || !upstream.enabled
        || !upstream.warmup_enabled
    {
        return Some(WarmupAttemptReason::UpstreamDisabled);
    }
    if upstream.oauth_credentials.is_none() {
        return Some(WarmupAttemptReason::OauthCredentialsMissing);
    }
    None
}

pub(super) fn quota_preflight_skip_from_snapshots(
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    now_unix_secs: i64,
) -> Option<WarmupPreflightSkip> {
    const SEVEN_DAY_WINDOWS: [SubscriptionQuotaWindow; 3] = [
        SubscriptionQuotaWindow::SevenDay,
        SubscriptionQuotaWindow::SevenDaySonnet,
        SubscriptionQuotaWindow::SevenDayOpus,
    ];

    for window in SEVEN_DAY_WINDOWS {
        if let Some(cycle_key) = snapshots
            .iter()
            .find(|snapshot| quota_snapshot_matches_window(snapshot, window))
            .filter(|snapshot| quota_snapshot_is_fresh(snapshot))
            .filter(|snapshot| quota_snapshot_is_exhausted(snapshot))
            .and_then(|snapshot| quota_snapshot_future_reset(snapshot, now_unix_secs))
        {
            return Some(WarmupPreflightSkip {
                reason: WarmupAttemptReason::SevenDayQuotaExhausted,
                cycle_key: Some(cycle_key),
            });
        }
    }

    snapshots
        .iter()
        .find(|snapshot| quota_snapshot_matches_window(snapshot, SubscriptionQuotaWindow::FiveHour))
        .filter(|snapshot| quota_snapshot_is_fresh(snapshot))
        .and_then(|snapshot| quota_snapshot_future_reset(snapshot, now_unix_secs))
        .map(|cycle_key| WarmupPreflightSkip {
            reason: WarmupAttemptReason::WindowAlreadyActive,
            cycle_key: Some(cycle_key),
        })
}

fn quota_snapshot_matches_window(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    window: SubscriptionQuotaWindow,
) -> bool {
    SubscriptionQuotaWindow::from_str(&snapshot.window) == Some(window)
}

fn quota_snapshot_is_fresh(snapshot: &SubscriptionQuotaCandidateSnapshot) -> bool {
    snapshot.state == SubscriptionQuotaDataState::Fresh
}

fn quota_snapshot_is_exhausted(snapshot: &SubscriptionQuotaCandidateSnapshot) -> bool {
    let rejected = snapshot
        .status
        .as_deref()
        .and_then(SubscriptionQuotaStatus::from_str)
        == Some(SubscriptionQuotaStatus::Rejected);
    rejected
        || snapshot
            .utilization
            .is_some_and(|utilization| utilization >= 1.0)
}

fn quota_snapshot_future_reset(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    now_unix_secs: i64,
) -> Option<i64> {
    snapshot
        .resets_at_unix_secs
        .and_then(|resets_at| i64::try_from(resets_at).ok())
        .filter(|resets_at| *resets_at > now_unix_secs)
}
