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
const WARMUP_WINDOW_TOL_SECS: i64 = 3;
pub(super) const WARMUP_WINDOW_MIN_GAP_SECS: i64 = 10;

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
/// Classify the lane's target windows from fresh readings observed after the
/// warm-up completed.
///
/// Gemini reports an unused window as a rolling one: zero usage and a reset of
/// `poll + D` that moves with every poll. Once a window runs its reset is
/// fixed, either `request + D` or, when the request follows a just-expired
/// window, `previous reset + D`. A window is therefore running when its reset
/// is clearly earlier than `poll + D`, or when it shows usage. The two are only
/// separable once the poll is `MIN_GAP - TOL` after the request.
/// A running window whose reset does not reach past its own reset known before
/// dispatch (`previous_resets`) was already running, so it is not evidence of
/// a start. A window with no known previous reset counts once it is running.
///
/// Any target that is still rolling makes the result `NotStarted`, so a busy
/// weekly window cannot hide an unstarted five-hour window.
pub(super) fn post_warmup_window_check(
    kind: UpstreamKind,
    lane: WarmupLane,
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
    previous_resets: &[(SubscriptionQuotaWindow, i64)],
    request_completed_at_unix_secs: i64,
) -> cc_lb_admin::WarmupWindowCheck {
    let mut observations = Vec::new();
    let mut not_started = Vec::new();
    let mut advanced = 0usize;
    let mut unknown = false;
    for window in lane.target_windows(kind) {
        let Some(snapshot) = snapshots
            .iter()
            .find(|s| quota_snapshot_matches_window(s, *window))
        else {
            unknown = true;
            continue;
        };
        if snapshot.state == SubscriptionQuotaDataState::Absent {
            continue;
        }
        let Some(observed_at_unix_secs) = observed_at_unix_secs(snapshot) else {
            unknown = true;
            continue;
        };
        if snapshot.state != SubscriptionQuotaDataState::Fresh
            || observed_at_unix_secs < request_completed_at_unix_secs
        {
            unknown = true;
            continue;
        }
        observations.push(UnifiedQuotaObservation {
            window: *window,
            utilization: snapshot.utilization,
            status: snapshot
                .status
                .as_deref()
                .and_then(SubscriptionQuotaStatus::from_str),
            resets_at_unix_secs: snapshot.resets_at_unix_secs,
            ..UnifiedQuotaObservation::default()
        });
        let Some(reset) = quota_snapshot_future_reset(snapshot, observed_at_unix_secs) else {
            unknown = true;
            continue;
        };
        let rolling_reset = observed_at_unix_secs + quota_window_duration_secs(*window);
        let separable = observed_at_unix_secs
            >= request_completed_at_unix_secs + WARMUP_WINDOW_MIN_GAP_SECS - WARMUP_WINDOW_TOL_SECS;
        if snapshot
            .utilization
            .is_some_and(|utilization| utilization > 0.0)
            || reset < rolling_reset - WARMUP_WINDOW_TOL_SECS
        {
            let previous = previous_resets
                .iter()
                .find(|(previous_window, _)| previous_window == window)
                .map(|(_, previous_reset)| *previous_reset);
            if previous.is_none_or(|previous| reset > previous + WARMUP_WINDOW_TOL_SECS) {
                advanced += 1;
            }
        } else if separable && snapshot.utilization == Some(0.0) {
            not_started.push(*window);
        } else {
            unknown = true;
        }
    }
    if !not_started.is_empty() {
        cc_lb_admin::WarmupWindowCheck::NotStarted {
            windows: not_started,
        }
    } else if advanced > 0 && !unknown {
        cc_lb_admin::WarmupWindowCheck::Started { observations }
    } else {
        cc_lb_admin::WarmupWindowCheck::Unknown
    }
}

/// The reset each of the lane's target windows reported before dispatch, for
/// [`post_warmup_window_check`]. Absent windows and windows without a reset
/// are left out.
pub(super) fn target_window_resets(
    kind: UpstreamKind,
    lane: WarmupLane,
    snapshots: &[SubscriptionQuotaCandidateSnapshot],
) -> Vec<(SubscriptionQuotaWindow, i64)> {
    lane.target_windows(kind)
        .iter()
        .filter_map(|window| {
            let snapshot = snapshots
                .iter()
                .find(|snapshot| quota_snapshot_matches_window(snapshot, *window))
                .filter(|snapshot| snapshot.state != SubscriptionQuotaDataState::Absent)?;
            let reset = i64::try_from(snapshot.resets_at_unix_secs?).ok()?;
            Some((*window, reset))
        })
        .collect()
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
