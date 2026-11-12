use cc_lb_domain::{SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState};
use cc_lb_quota::UnifiedQuotaObservation;
use cc_lb_storage_api::UpstreamRecord;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, WarmupLane, WarmupPermanentFailureReason,
    WarmupSkipReason,
};

const QUOTA_PREFLIGHT_MAX_AGE_SECS: i64 = 180;
const FIVE_HOUR_WINDOW_SECS: i64 = 5 * 60 * 60;
const SEVEN_DAY_WINDOW_SECS: i64 = 7 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WarmupPreflightReject {
    Skip(WarmupSkipReason),
    PermanentFailure(WarmupPermanentFailureReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WarmupQuotaPreflightDecision {
    ActiveWindow {
        cycle_key: i64,
    },
    SevenDayQuotaExhausted {
        cycle_key: i64,
    },
    /// Every lane target window is explicitly reported absent: the account has
    /// no quota in this group, so nothing is sent.
    TargetsAbsent,
    SendWarmup,
    Unknown,
}

pub(super) fn warmup_preflight_reject(upstream: &UpstreamRecord) -> Option<WarmupPreflightReject> {
    if upstream.deleted_at_unix_secs.is_some() {
        return Some(WarmupPreflightReject::Skip(
            WarmupSkipReason::UpstreamDeleted,
        ));
    }
    if !matches!(
        upstream.kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ) || !upstream.warmup_enabled
    {
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
    kind: UpstreamKind,
    lane: WarmupLane,
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    now_unix_secs: i64,
) -> WarmupQuotaPreflightDecision {
    if let Some(cycle_key) = lane.weekly_windows(kind).iter().find_map(|window| {
        snapshots
            .iter()
            .find(|snapshot| quota_snapshot_matches_window(snapshot, *window))
            .filter(|snapshot| quota_snapshot_is_usable(snapshot, now_unix_secs))
            .filter(|snapshot| quota_snapshot_is_exhausted(snapshot))
            .and_then(|snapshot| quota_snapshot_future_reset(snapshot, now_unix_secs))
    }) {
        return WarmupQuotaPreflightDecision::SevenDayQuotaExhausted { cycle_key };
    }

    let target_windows = lane.target_windows(kind);
    // Only Gemini reports whole quota groups as absent; Anthropic and OpenAI
    // keep sending when every target is absent.
    if kind == UpstreamKind::GeminiOauth
        && target_windows.iter().all(|window| {
            snapshots.iter().any(|snapshot| {
                quota_snapshot_matches_window(snapshot, *window)
                    && snapshot.state == SubscriptionQuotaDataState::Absent
            })
        })
    {
        return WarmupQuotaPreflightDecision::TargetsAbsent;
    }

    let mut earliest_active_reset = None;

    for window in target_windows {
        let Some(snapshot) = snapshots
            .iter()
            .find(|snapshot| quota_snapshot_matches_window(snapshot, *window))
        else {
            continue;
        };

        // Explicitly absent targets and unknown samples cannot invalidate a
        // known active target. A fresh idle target still requests warm-up.
        if snapshot.state == SubscriptionQuotaDataState::Absent
            || !quota_snapshot_is_usable(snapshot, now_unix_secs)
        {
            continue;
        }

        let Some(reset) = quota_snapshot_future_reset(snapshot, now_unix_secs) else {
            return WarmupQuotaPreflightDecision::SendWarmup;
        };
        // OpenAI and Gemini may report a full rolling duration before any
        // usage starts; Anthropic's future resets remain authoritative.
        if matches!(kind, UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth)
            && quota_snapshot_is_unused_rolling_window(snapshot, *window, reset)
        {
            return WarmupQuotaPreflightDecision::SendWarmup;
        }
        earliest_active_reset =
            Some(earliest_active_reset.map_or(reset, |current: i64| current.min(reset)));
    }

    earliest_active_reset.map_or(WarmupQuotaPreflightDecision::Unknown, |cycle_key| {
        WarmupQuotaPreflightDecision::ActiveWindow { cycle_key }
    })
}

pub(super) fn next_warmup_evaluation_after_success(
    kind: UpstreamKind,
    lane: WarmupLane,
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    observations: &[UnifiedQuotaObservation],
    now_unix_secs: i64,
) -> i64 {
    let mut earliest_reset = None;
    let mut shortest_present_duration = None;
    let mut unknown_target = false;

    for window in lane.target_windows(kind) {
        let snapshot = snapshots
            .iter()
            .find(|snapshot| quota_snapshot_matches_window(snapshot, *window));
        let response_present = observations
            .iter()
            .any(|observation| observation.window == *window);
        if !response_present
            && snapshot.is_some_and(|snapshot| snapshot.state == SubscriptionQuotaDataState::Absent)
        {
            continue;
        }

        let duration = quota_window_duration_secs(*window);
        shortest_present_duration =
            Some(shortest_present_duration.map_or(duration, |current: i64| current.min(duration)));
        let response_reset = observations
            .iter()
            .filter(|observation| observation.window == *window)
            .filter_map(|observation| observation.resets_at_unix_secs)
            .filter_map(|reset| i64::try_from(reset).ok())
            .filter(|reset| *reset > now_unix_secs)
            .min();
        // Response observations describe the completed request and supersede
        // older cached state, including when no future reset was returned.
        let target_reset = if response_present {
            response_reset
        } else {
            snapshot
                .filter(|snapshot| quota_snapshot_is_usable(snapshot, now_unix_secs))
                .and_then(|snapshot| quota_snapshot_future_reset(snapshot, now_unix_secs))
        };
        match target_reset {
            Some(reset) => {
                earliest_reset =
                    Some(earliest_reset.map_or(reset, |current: i64| current.min(reset)));
            }
            None => unknown_target = true,
        }
    }

    let fallback =
        now_unix_secs.saturating_add(shortest_present_duration.unwrap_or(SEVEN_DAY_WINDOW_SECS));
    match earliest_reset {
        Some(reset) if unknown_target => reset.min(fallback),
        Some(reset) => reset,
        None => fallback,
    }
}

fn quota_window_duration_secs(window: SubscriptionQuotaWindow) -> i64 {
    match window {
        SubscriptionQuotaWindow::FiveHour
        | SubscriptionQuotaWindow::GeminiFiveHour
        | SubscriptionQuotaWindow::ThirdPartyFiveHour => FIVE_HOUR_WINDOW_SECS,
        SubscriptionQuotaWindow::SevenDay
        | SubscriptionQuotaWindow::SevenDaySonnet
        | SubscriptionQuotaWindow::SevenDayOpus
        | SubscriptionQuotaWindow::SevenDayFable
        | SubscriptionQuotaWindow::GeminiSevenDay
        | SubscriptionQuotaWindow::ThirdPartySevenDay => SEVEN_DAY_WINDOW_SECS,
        SubscriptionQuotaWindow::Overage | SubscriptionQuotaWindow::Unified => {
            unreachable!("unsupported warmup quota window: {window:?}")
        }
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

fn quota_snapshot_is_unused_rolling_window(
    snapshot: &SubscriptionQuotaCandidateSnapshot,
    window: SubscriptionQuotaWindow,
    reset: i64,
) -> bool {
    snapshot.utilization == Some(0.0)
        && reset.saturating_sub(observed_at_unix_secs(snapshot).unwrap_or(reset))
            >= quota_window_duration_secs(window) - 60
}

fn observed_at_unix_secs(snapshot: &SubscriptionQuotaCandidateSnapshot) -> Option<i64> {
    snapshot
        .observed_at_unix_millis
        .and_then(|observed_at| i64::try_from(observed_at / 1_000).ok())
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
