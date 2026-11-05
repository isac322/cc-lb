use cc_lb_domain::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_quota::UnifiedQuotaObservation;
use cc_lb_storage_api::UpstreamRecord;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, WarmupPermanentFailureReason,
    WarmupSkipReason,
};

const QUOTA_PREFLIGHT_MAX_AGE_SECS: i64 = 180;
const FIVE_HOUR_WINDOW_SECS: i64 = 5 * 60 * 60;
const SEVEN_DAY_WINDOW_SECS: i64 = 7 * 24 * 60 * 60;
const ANTHROPIC_TARGET_WINDOWS: &[SubscriptionQuotaWindow] = &[
    SubscriptionQuotaWindow::FiveHour,
    SubscriptionQuotaWindow::SevenDay,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WarmupPreflightReject {
    Skip(WarmupSkipReason),
    PermanentFailure(WarmupPermanentFailureReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WarmupQuotaPreflightDecision {
    ActiveWindow { cycle_key: i64 },
    SevenDayQuotaExhausted { cycle_key: i64 },
    SendWarmup,
    Unknown,
}

pub(super) fn warmup_preflight_reject(upstream: &UpstreamRecord) -> Option<WarmupPreflightReject> {
    if upstream.deleted_at_unix_secs.is_some() {
        return Some(WarmupPreflightReject::Skip(
            WarmupSkipReason::UpstreamDeleted,
        ));
    }
    if upstream.kind != UpstreamKind::AnthropicOauth || !upstream.warmup_enabled {
        return Some(WarmupPreflightReject::Skip(
            WarmupSkipReason::UpstreamDisabled,
        ));
    }
    if upstream.oauth_credentials.is_none() {
        return Some(WarmupPreflightReject::PermanentFailure(
            WarmupPermanentFailureReason::OauthCredentialsMissing,
        ));
    }
    None
}

pub(super) fn quota_preflight_decision_from_snapshots(
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    now_unix_secs: i64,
) -> WarmupQuotaPreflightDecision {
    if let Some(cycle_key) = weekly_windows().iter().find_map(|window| {
        snapshots
            .iter()
            .find(|snapshot| quota_snapshot_matches_window(snapshot, *window))
            .filter(|snapshot| quota_snapshot_is_usable(snapshot, now_unix_secs))
            .filter(|snapshot| quota_snapshot_is_exhausted(snapshot))
            .and_then(|snapshot| quota_snapshot_future_reset(snapshot, now_unix_secs))
    }) {
        return WarmupQuotaPreflightDecision::SevenDayQuotaExhausted { cycle_key };
    }

    let mut present_target = false;
    let mut unknown_target = false;
    let mut earliest_active_reset = None;

    for window in target_windows_for_kind() {
        let Some(snapshot) = snapshots
            .iter()
            .find(|snapshot| quota_snapshot_matches_window(snapshot, *window))
        else {
            unknown_target = true;
            continue;
        };

        // An explicitly absent target is not a target for this account. This is
        // how weekly-only Claude accounts warm their available 7d window.
        if snapshot.state == SubscriptionQuotaDataState::Absent {
            continue;
        }
        present_target = true;

        if !quota_snapshot_is_usable(snapshot, now_unix_secs) {
            unknown_target = true;
            continue;
        }

        let Some(reset) = quota_snapshot_future_reset(snapshot, now_unix_secs) else {
            return WarmupQuotaPreflightDecision::SendWarmup;
        };
        earliest_active_reset =
            Some(earliest_active_reset.map_or(reset, |current: i64| current.min(reset)));
    }

    if unknown_target && earliest_active_reset.is_some() {
        return WarmupQuotaPreflightDecision::SendWarmup;
    }
    if unknown_target || !present_target {
        WarmupQuotaPreflightDecision::Unknown
    } else {
        WarmupQuotaPreflightDecision::ActiveWindow {
            cycle_key: earliest_active_reset.expect("present target has a reset"),
        }
    }
}

pub(super) fn next_warmup_evaluation_after_success(
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    observations: &[UnifiedQuotaObservation],
    now_unix_secs: i64,
) -> i64 {
    let response_reset = target_windows_for_kind()
        .iter()
        .flat_map(|window| {
            observations
                .iter()
                .filter(move |observation| observation.window == *window)
                .filter_map(|observation| observation.resets_at_unix_secs)
                .filter_map(|reset| i64::try_from(reset).ok())
                .filter(|reset| *reset > now_unix_secs)
        })
        .min();

    let snapshot_reset = target_windows_for_kind()
        .iter()
        .filter_map(|window| {
            snapshots
                .iter()
                .find(|snapshot| quota_snapshot_matches_window(snapshot, *window))
                .filter(|snapshot| quota_snapshot_is_usable(snapshot, now_unix_secs))
                .and_then(|snapshot| quota_snapshot_future_reset(snapshot, now_unix_secs))
        })
        .min();

    if let Some(reset) = response_reset.into_iter().chain(snapshot_reset).min() {
        return reset;
    }

    let shortest_present_duration = target_windows_for_kind()
        .iter()
        .filter(|window| {
            let response_present = observations
                .iter()
                .any(|observation| observation.window == **window);
            let snapshot_present = snapshots
                .iter()
                .find(|snapshot| quota_snapshot_matches_window(snapshot, **window))
                .is_none_or(|snapshot| snapshot.state != SubscriptionQuotaDataState::Absent);
            response_present || snapshot_present
        })
        .map(|window| quota_window_duration_secs(*window))
        .min()
        .unwrap_or(FIVE_HOUR_WINDOW_SECS);
    now_unix_secs.saturating_add(shortest_present_duration)
}

fn target_windows_for_kind() -> &'static [SubscriptionQuotaWindow] {
    ANTHROPIC_TARGET_WINDOWS
}

fn weekly_windows() -> &'static [SubscriptionQuotaWindow] {
    &[
        SubscriptionQuotaWindow::SevenDay,
        SubscriptionQuotaWindow::SevenDaySonnet,
        SubscriptionQuotaWindow::SevenDayOpus,
        SubscriptionQuotaWindow::SevenDayFable,
    ]
}

fn quota_window_duration_secs(window: SubscriptionQuotaWindow) -> i64 {
    match window {
        SubscriptionQuotaWindow::FiveHour => FIVE_HOUR_WINDOW_SECS,
        SubscriptionQuotaWindow::SevenDay
        | SubscriptionQuotaWindow::SevenDaySonnet
        | SubscriptionQuotaWindow::SevenDayOpus
        | SubscriptionQuotaWindow::SevenDayFable => SEVEN_DAY_WINDOW_SECS,
        _ => FIVE_HOUR_WINDOW_SECS,
    }
}

fn quota_snapshot_matches_window(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    window: SubscriptionQuotaWindow,
) -> bool {
    SubscriptionQuotaWindow::from_str(&snapshot.window) == Some(window)
}

fn quota_snapshot_is_usable(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    now_unix_secs: i64,
) -> bool {
    snapshot.state == SubscriptionQuotaDataState::Fresh
        && snapshot
            .observed_at_unix_millis
            .and_then(|observed_at| i64::try_from(observed_at / 1_000).ok())
            .is_some_and(|observed_at| {
                let age_secs = now_unix_secs - observed_at;
                (0..=QUOTA_PREFLIGHT_MAX_AGE_SECS).contains(&age_secs)
            })
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
