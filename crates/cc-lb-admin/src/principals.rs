use cc_lb_config::Config;
use cc_lb_storage_api::{
    PrincipalLimitIdentityKind, PrincipalLimitKind, PrincipalLimitState, Storage, StorageError,
    UsageRollupResolution,
};
use serde::Serialize;

use crate::dashboard::{self, DashboardRange, DashboardUsageResponse, UsageGroupBy};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PrincipalLimitsResponse {
    pub principal_id: String,
    pub observed: bool,
    pub identities: Vec<PrincipalLimitIdentityResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PrincipalLimitIdentityResponse {
    pub identity_kind: &'static str,
    pub identity_value: Option<String>,
    pub account_observed: bool,
    pub windows: Vec<PrincipalLimitWindowResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PrincipalLimitWindowResponse {
    pub window: String,
    pub snapshots: Vec<PrincipalLimitSnapshotResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PrincipalLimitSnapshotResponse {
    pub kind: &'static str,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
    pub observed_at_unix_secs: u64,
    pub stored_at_unix_secs: u64,
    pub observed: bool,
}

#[derive(Debug)]
pub(crate) enum T9Error {
    UnknownPrincipal,
    InvalidPrincipalId,
    InvalidRange,
    InvalidStep,
    StepTooFineForRange,
    Storage(StorageError),
}

pub(crate) async fn build_principal_usage(
    storage: &dyn Storage,
    config: &Config,
    principal_id: &str,
    range: DashboardRange,
    step: UsageRollupResolution,
    now_unix_secs: u64,
) -> Result<DashboardUsageResponse, T9Error> {
    ensure_principal(config, principal_id)?;
    dashboard::validate_step_for_range(range, step).map_err(map_dashboard_query_error)?;

    let (window_start_unix_secs, window_end_unix_secs) =
        dashboard::build_window_for_step(range, step, now_unix_secs);
    let rollups = storage
        .query_usage_rollups_in_range(step, window_start_unix_secs, window_end_unix_secs)
        .await?
        .into_iter()
        .filter(|rollup| rollup.principal == principal_id)
        .collect::<Vec<_>>();
    let observed = !rollups.is_empty();
    let (series, truncated_series_count) = dashboard::build_usage_series(
        UsageGroupBy::Model,
        &rollups,
        window_start_unix_secs,
        window_end_unix_secs,
        step,
    );

    Ok(DashboardUsageResponse {
        range: range.as_str(),
        step: step.as_str(),
        group_by: UsageGroupBy::Model.as_str(),
        window_start_unix_secs,
        window_end_unix_secs,
        series,
        truncated_series_count,
        observed,
    })
}

pub(crate) async fn build_principal_limits(
    storage: &dyn Storage,
    config: &Config,
    principal_id: &str,
    _now_unix_secs: u64,
) -> Result<PrincipalLimitsResponse, T9Error> {
    ensure_principal(config, principal_id)?;

    let states = storage.list_principal_limit_states(principal_id).await?;
    let observed = !states.is_empty();

    Ok(PrincipalLimitsResponse {
        principal_id: principal_id.to_owned(),
        observed,
        identities: build_limit_identities(states),
    })
}

fn ensure_principal(config: &Config, principal_id: &str) -> Result<(), T9Error> {
    if !is_valid_principal_id(principal_id) {
        return Err(T9Error::InvalidPrincipalId);
    }
    if !config.principals.contains_key(principal_id) {
        return Err(T9Error::UnknownPrincipal);
    }
    Ok(())
}

fn is_valid_principal_id(principal_id: &str) -> bool {
    !principal_id.is_empty()
        && principal_id.len() <= 256
        && principal_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':' | '@'))
}

fn build_limit_identities(states: Vec<PrincipalLimitState>) -> Vec<PrincipalLimitIdentityResponse> {
    let mut identities = Vec::<IdentityAccumulator>::new();
    for state in states {
        match identities.iter_mut().find(|identity| {
            identity.identity_kind == state.identity_kind
                && identity.identity_value == state.identity_value
        }) {
            Some(identity) => identity.states.push(state),
            None => identities.push(IdentityAccumulator {
                identity_kind: state.identity_kind,
                identity_value: state.identity_value.clone(),
                states: vec![state],
            }),
        }
    }

    identities.sort_by(|left, right| {
        identity_rank(left.identity_kind)
            .cmp(&identity_rank(right.identity_kind))
            .then_with(|| {
                identity_sort_value(&left.identity_value)
                    .cmp(identity_sort_value(&right.identity_value))
            })
    });

    identities
        .into_iter()
        .map(|identity| PrincipalLimitIdentityResponse {
            identity_kind: identity.identity_kind.as_str(),
            identity_value: identity.identity_value,
            account_observed: identity.identity_kind == PrincipalLimitIdentityKind::Account,
            windows: build_limit_windows(identity.states),
        })
        .collect()
}

fn build_limit_windows(states: Vec<PrincipalLimitState>) -> Vec<PrincipalLimitWindowResponse> {
    let mut windows = Vec::<WindowAccumulator>::new();
    for state in states {
        match windows
            .iter_mut()
            .find(|window| window.window == state.window)
        {
            Some(window) => window.states.push(state),
            None => windows.push(WindowAccumulator {
                window: state.window.clone(),
                states: vec![state],
            }),
        }
    }

    windows.sort_by(|left, right| {
        window_rank(&left.window)
            .cmp(&window_rank(&right.window))
            .then_with(|| left.window.cmp(&right.window))
    });

    windows
        .into_iter()
        .map(|mut window| {
            window.states.sort_by(|left, right| {
                kind_rank(left.kind)
                    .cmp(&kind_rank(right.kind))
                    .then_with(|| left.kind.as_str().cmp(right.kind.as_str()))
                    .then_with(|| left.observed_at_unix_secs.cmp(&right.observed_at_unix_secs))
                    .then_with(|| left.stored_at_unix_secs.cmp(&right.stored_at_unix_secs))
            });
            PrincipalLimitWindowResponse {
                window: window.window,
                snapshots: window
                    .states
                    .into_iter()
                    .map(|state| PrincipalLimitSnapshotResponse {
                        kind: state.kind.as_str(),
                        limit: state.limit,
                        remaining: state.remaining,
                        reset: state.reset,
                        observed_at_unix_secs: state.observed_at_unix_secs,
                        stored_at_unix_secs: state.stored_at_unix_secs,
                        observed: state.limit.is_some() || state.remaining.is_some(),
                    })
                    .collect(),
            }
        })
        .collect()
}

fn map_dashboard_query_error(error: dashboard::DashboardQueryError) -> T9Error {
    match error {
        dashboard::DashboardQueryError::InvalidRange => T9Error::InvalidRange,
        dashboard::DashboardQueryError::InvalidStep => T9Error::InvalidStep,
        dashboard::DashboardQueryError::InvalidGroupBy => T9Error::InvalidStep,
        dashboard::DashboardQueryError::StepTooFineForRange => T9Error::StepTooFineForRange,
    }
}

fn identity_rank(identity_kind: PrincipalLimitIdentityKind) -> u8 {
    match identity_kind {
        PrincipalLimitIdentityKind::Account => 0,
        PrincipalLimitIdentityKind::Credential => 1,
        PrincipalLimitIdentityKind::Unobserved => 2,
    }
}

fn identity_sort_value(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("")
}

fn window_rank(window: &str) -> u8 {
    match window {
        "5h" => 0,
        "weekly" => 1,
        _ => 2,
    }
}

fn kind_rank(kind: PrincipalLimitKind) -> u8 {
    match kind {
        PrincipalLimitKind::Requests => 0,
        PrincipalLimitKind::InputTokens => 1,
        PrincipalLimitKind::OutputTokens => 2,
        PrincipalLimitKind::Tokens => 3,
    }
}

impl T9Error {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::UnknownPrincipal => "unknown_principal",
            Self::InvalidPrincipalId => "invalid_principal_id",
            Self::InvalidRange => "invalid_range",
            Self::InvalidStep => "invalid_step",
            Self::StepTooFineForRange => "step_too_fine_for_range",
            Self::Storage(_) => "storage_error",
        }
    }
}

impl From<StorageError> for T9Error {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

struct IdentityAccumulator {
    identity_kind: PrincipalLimitIdentityKind,
    identity_value: Option<String>,
    states: Vec<PrincipalLimitState>,
}

struct WindowAccumulator {
    window: String,
    states: Vec<PrincipalLimitState>,
}
