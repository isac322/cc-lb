//! Per-upstream subscription limit-reset ("cedar_ember" coupon) endpoints.
//!
//! `GET /admin/v1/upstreams/{id}/limit-resets` serves the snapshot collected
//! by the scheduler's usage poll (`GET /api/oauth/usage?cedar_ember=1`), so
//! it never calls the provider itself; `POST .../limit-resets/claim`
//! dispatches a single reset claim, serialized per upstream by a durable
//! compare-and-put epoch fence. Coupon eligibility is provider-owned: the
//! backend only caches the polled snapshot, invents no eligibility, and
//! never retries the claim POST. A timeout or mid-flight transport failure
//! on the claim is reported as `claim_outcome_unknown` (504) because the
//! provider may have consumed the grant; only a connect failure is a
//! definite non-delivery.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use cc_lb_control::anthropic_metadata::{
    CedarEmberClaimOutcome, CedarEmberClaimRequest, CedarEmberError, CedarEmberIdentityRecord,
    CedarEmberPollRecord, CedarEmberStatus, cedar_ember_epoch_fence, cedar_ember_epoch_meta_key,
    cedar_ember_identity_meta_key, cedar_ember_meta_key, claim_cedar_ember_reset,
    fetch_oauth_profile_at, is_valid_grant_id, make_metadata_http_client,
    settled_cedar_ember_epoch,
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
use cc_lb_scheduler::worker::{CronJob, SchedulerPushTask};
use cc_lb_storage_api::{Storage, UpstreamKind, UpstreamRecord, UpstreamStore};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::upstreams::{
    UpstreamError, decrypt_oauth_bundle, fresh_enough_access_token, metadata_user_agent,
    storage_arc, upstream_base_url,
};
use crate::{
    AdminState,
    audit::{AdminAuditEvent, record_admin_audit},
    auth::AdminIdentity,
};

#[derive(Debug, Serialize)]
pub(crate) struct LimitResetsResponse {
    /// Null until the metadata hook has observed the OAuth identity; the
    /// claim endpoint re-verifies the live identity regardless.
    account_id: Option<String>,
    organization_id: Option<String>,
    cedar_ember: Option<CedarEmberStatusResponse>,
}

#[derive(Debug, Serialize)]
struct CedarEmberStatusResponse {
    eligible: bool,
    ineligible_reason: Option<String>,
    at_limit: bool,
    /// Provider window names mapped to the cc-lb quota vocabulary
    /// (`five_hour` -> `5h`, `seven_day*` -> `7d*`).
    exhausted: Vec<String>,
    grants: Vec<LimitResetGrantResponse>,
    next_grant_id: Option<String>,
    weekly_resets_at: Option<String>,
    cooldown_until: Option<String>,
}

#[derive(Debug, Serialize)]
struct LimitResetGrantResponse {
    id: String,
    label: String,
    resets_total: Option<u32>,
    resets_left: u32,
    starts_at: Option<String>,
    ends_at: Option<String>,
    clears: Vec<String>,
    paused: bool,
    usable_now: bool,
    use_requires_limit: bool,
    percent_used: BTreeMap<String, u64>,
    blocking: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LimitResetClaimBody {
    account_id: String,
    organization_id: String,
    grant_id: String,
    request_id: String,
}

#[derive(Debug, Serialize)]
struct LimitResetClaimResponse {
    /// Normalized provider outcome; never an arbitrary provider string.
    result: &'static str,
    reason: Option<String>,
    cleared: Vec<String>,
    resets_left: Option<u32>,
    weekly_resets_at: Option<String>,
    cooldown_until: Option<String>,
}

pub(crate) async fn get_upstream_limit_resets(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
) -> Result<Json<LimitResetsResponse>, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if upstream.kind != UpstreamKind::AnthropicOauth || upstream.oauth_credentials.is_none() {
        return Err(UpstreamError::NotOauthUpstream);
    }
    let fingerprint = upstream.oauth_credential_fingerprint();
    let coupon_key = cedar_ember_meta_key(upstream_id);
    let identity_key = cedar_ember_identity_meta_key(upstream_id);
    let epoch_key = cedar_ember_epoch_meta_key(upstream_id);
    let (coupon_raw, identity_raw, epoch_raw) = tokio::join!(
        storage.get_meta_value(&coupon_key),
        storage.get_meta_value(&identity_key),
        storage.get_meta_value(&epoch_key),
    );
    let coupon = coupon_raw?.and_then(|raw| {
        serde_json::from_str::<CedarEmberPollRecord>(&raw)
            .map_err(|error| {
                tracing::warn!(%upstream_id, %error, "cedar_ember snapshot unreadable");
                error
            })
            .ok()
    });
    let identity = identity_raw?.and_then(|raw| {
        serde_json::from_str::<CedarEmberIdentityRecord>(&raw)
            .map_err(|error| {
                tracing::warn!(%upstream_id, %error, "cedar_ember identity unreadable");
                error
            })
            .ok()
    });
    // `None` = a claim fence is pending or the value is unreadable: fail
    // closed and serve no coupon.
    let current_epoch = settled_cedar_ember_epoch(epoch_raw?.as_deref());
    let now_unix_millis =
        cc_lb_clock::unix_millis(state.clock.now()).min(u128::from(u64::MAX)) as u64;
    let max_staleness_secs = state
        .dynamic_view
        .load()
        .subscription_quota_routing_max_staleness_secs;

    // Identity is served only while bound to the current credential — a
    // rotated credential's account may differ.
    let (account_id, organization_id) = match identity {
        Some(record) if Some(record.credential_fingerprint) == fingerprint => {
            (Some(record.account_id), Some(record.organization_id))
        }
        _ => (None, None),
    };
    // The coupon is served only while fresh, bound to the current credential,
    // backed by a known identity, and stamped with the still-current
    // invalidation epoch — anything else reports `null` rather than a
    // possibly-consumed or foreign-account coupon.
    let cedar_ember = match (coupon, account_id.as_ref(), fingerprint) {
        (Some(record), Some(_), Some(fingerprint))
            if record.credential_fingerprint == fingerprint
                && current_epoch == Some(record.epoch)
                && now_unix_millis.saturating_sub(record.observed_at_unix_millis)
                    <= max_staleness_secs.saturating_mul(1_000) =>
        {
            record.status.map(|status| cedar_ember_response(&status))
        }
        _ => None,
    };

    Ok(Json(LimitResetsResponse {
        account_id,
        organization_id,
        cedar_ember,
    }))
}

pub(crate) async fn claim_upstream_limit_reset(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
    Extension(identity): Extension<AdminIdentity>,
    Json(body): Json<LimitResetClaimBody>,
) -> Result<Response, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    let target_upstream = upstream.name.clone();
    // Audit every outcome, including errors: a 504/timeout may still have
    // consumed the grant provider-side, so the request_id trail must exist
    // even when the handler returns an error.
    let result = claim_limit_reset_inner(&state, storage, upstream, &body).await;
    let (status, outcome) = match &result {
        Ok(response) => (response.status().as_u16(), "ok"),
        Err(error) => (error.status().as_u16(), error.error_code()),
    };
    let action = "upstream_limit_reset_claim";
    let route = format!("/admin/v1/upstreams/{upstream_id}/limit-resets/claim");
    if let Err(error) = record_admin_audit(
        &state,
        AdminAuditEvent {
            identity: Some(&identity),
            system_component: None,
            action,
            route: &route,
            target_principal_id: None,
            target_upstream: Some(&target_upstream),
            api_key_id: None,
            status,
            payload: Some(json!({
                "grant_id": body.grant_id,
                "request_id": body.request_id,
                "outcome": outcome,
            })),
        },
    )
    .await
    {
        tracing::error!(error = %error, action, "admin audit write failed");
        return Err(UpstreamError::AuditWriteFailed);
    }
    result
}

/// 409 for a claim refused before dispatch because the upstream's epoch is
/// not settled: another claim holds the fence, an abandoned fence awaits
/// poll recovery, or the value is unreadable. Nothing was sent.
fn claim_in_progress() -> UpstreamError {
    UpstreamError::Conflict {
        detail: "a limit-reset claim for this upstream is in progress or awaiting a fresh usage observation; refetch limit-resets".to_owned(),
    }
}

async fn claim_limit_reset_inner(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream: UpstreamRecord,
    body: &LimitResetClaimBody,
) -> Result<Response, UpstreamError> {
    if !is_valid_grant_id(&body.grant_id) {
        return Err(UpstreamError::BadRequest {
            error: "invalid_grant_id",
            detail: "grant_id must match ^[a-z0-9_-]{1,40}$".to_owned(),
        });
    }
    if Uuid::parse_str(&body.request_id).is_err() {
        return Err(UpstreamError::BadRequest {
            error: "invalid_request_id",
            detail: "request_id must be a UUID".to_owned(),
        });
    }
    if upstream.kind != UpstreamKind::AnthropicOauth {
        return Err(UpstreamError::NotOauthUpstream);
    }
    let bundle = decrypt_oauth_bundle(state, &upstream)?;
    let access_token = fresh_enough_access_token(state, storage.clone(), &upstream, bundle).await?;
    let user_agent = metadata_user_agent(storage.as_ref()).await?;
    let base_url = upstream_base_url(&upstream)?;
    let client = make_metadata_http_client();
    let cancel = CancellationToken::new();

    // The submitted account/org must still match the live OAuth identity;
    // cached metadata records are deliberately not consulted.
    let profile = fetch_oauth_profile_at(&client, &base_url, &access_token, &user_agent, &cancel)
        .await
        .map_err(provider_read_error)?;
    let (account_id, organization_id) = live_identity(&profile)?;
    if body.account_id != account_id || body.organization_id != organization_id {
        return Err(UpstreamError::StaleIdentity);
    }
    // Durable per-attempt fence before the POST, compare-and-put over the
    // settled epoch just read: every snapshot is unservable (on every
    // replica) while it is current, and only one claim per upstream can hold
    // it. Any unsettled value — another claim in flight, an abandoned fence
    // awaiting poll recovery, or an unreadable value — refuses the claim
    // without dispatching, so a grant is never claimed again before a fresh
    // post-claim observation. If the fence cannot be written, nothing is
    // dispatched and nothing has been consumed.
    let epoch_key = cedar_ember_epoch_meta_key(upstream.id);
    let settled =
        storage
            .get_meta_value(&epoch_key)
            .await
            .map_err(|error| UpstreamError::Internal {
                detail: format!("cedar_ember epoch read failed: {error}"),
            })?;
    if settled_cedar_ember_epoch(settled.as_deref()).is_none() {
        return Err(claim_in_progress());
    }
    let now_unix_millis =
        cc_lb_clock::unix_millis(state.clock.now()).min(u128::from(u64::MAX)) as u64;
    let fence = cedar_ember_epoch_fence(Uuid::new_v4(), now_unix_millis);
    let fenced = storage
        .compare_and_put_meta_value(&epoch_key, settled.as_deref(), &fence)
        .await
        .map_err(|error| UpstreamError::Internal {
            detail: format!("cedar_ember claim fence write failed: {error}"),
        })?;
    if !fenced {
        return Err(claim_in_progress());
    }
    let result = claim_cedar_ember_reset(
        &client,
        &base_url,
        &access_token,
        &user_agent,
        CedarEmberClaimRequest {
            org_uuid: &organization_id,
            grant_id: &body.grant_id,
            request_id: &body.request_id,
        },
        &cancel,
    )
    .await;

    // Settle our own fence with a fresh epoch after any outcome. Snapshots
    // from polls issued before the claim carry the old epoch and polls during
    // it wrote none (the fence was pending), so neither can match. If this
    // write fails the fence stays pending — fail closed until the poll's
    // expired-fence recovery settles it after a fresh observation — and the
    // failure surfaces instead of a silent success. A lost compare-and-put
    // means that recovery already replaced our fence, which is equally safe.
    let settled = storage
        .compare_and_put_meta_value(&epoch_key, Some(&fence), &Uuid::new_v4().to_string())
        .await
        .map_err(|error| UpstreamError::Internal {
            detail: format!("cedar_ember invalidation write failed: {error}"),
        })?;
    if !settled {
        tracing::warn!(upstream_id = %upstream.id, "cedar_ember claim fence was already recovered");
    }
    let result = result.map_err(provider_claim_error)?;

    if result.result == CedarEmberClaimOutcome::Reset {
        trigger_usage_poll(state);
    }

    Ok(Json(LimitResetClaimResponse {
        result: claim_outcome_str(result.result),
        reason: result.reason,
        cleared: result
            .cleared
            .unwrap_or_default()
            .iter()
            .map(|name| quota_window_name(name))
            .collect(),
        resets_left: result.resets_left,
        weekly_resets_at: result.weekly_resets_at,
        cooldown_until: result.cooldown_until,
    })
    .into_response())
}

/// Enqueues the existing oauth_usage_poll cron tick so the quota pipeline
/// re-observes the upstream after a successful reset. Best-effort: the claim
/// already succeeded, and the regular tick still runs on its own interval.
fn trigger_usage_poll(state: &AdminState) {
    let Some(scheduler) = state.scheduler.clone() else {
        return;
    };
    let tick_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    tokio::spawn(async move {
        let task = SchedulerPushTask {
            args: CronJob::OAuthUsagePoll(OAuthUsagePollCronJob::new(tick_unix_secs)),
            idempotency_key: Some(format!("oauth_usage_poll:claim:{tick_unix_secs}")),
            run_at_unix_secs: None,
            max_attempts: None,
        };
        match scheduler.push_cron_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => {}
            Err(error) => {
                tracing::warn!(%error, "post-claim oauth usage poll enqueue failed");
            }
        }
    });
}

fn live_identity(
    profile: &cc_lb_control::anthropic_metadata::ProfileResponse,
) -> Result<(String, String), UpstreamError> {
    let account_id = profile
        .account
        .as_ref()
        .and_then(|account| account.uuid.clone())
        .ok_or(UpstreamError::ProviderMalformed {
            detail: "oauth profile missing account.uuid".to_owned(),
        })?;
    let organization_id = profile
        .organization
        .as_ref()
        .and_then(|organization| organization.uuid.clone())
        .ok_or(UpstreamError::ProviderMalformed {
            detail: "oauth profile missing organization.uuid".to_owned(),
        })?;
    Ok((account_id, organization_id))
}

fn cedar_ember_response(status: &CedarEmberStatus) -> CedarEmberStatusResponse {
    CedarEmberStatusResponse {
        eligible: status.eligible,
        ineligible_reason: status.ineligible_reason.clone(),
        at_limit: status.at_limit.unwrap_or(false),
        exhausted: status
            .exhausted
            .iter()
            .flatten()
            .map(|name| quota_window_name(name))
            .collect(),
        grants: status
            .grants
            .iter()
            .map(|grant| LimitResetGrantResponse {
                id: grant.id.clone(),
                label: grant.label.clone().unwrap_or_default(),
                // Optional per the provider schema; forwarded as null rather
                // than fabricated as 0 when absent.
                resets_total: grant.resets_total,
                resets_left: grant.resets_left,
                starts_at: grant.starts_at.clone(),
                ends_at: grant.ends_at.clone(),
                clears: grant
                    .clears
                    .iter()
                    .flatten()
                    .map(|name| quota_window_name(name))
                    .collect(),
                paused: grant.paused.unwrap_or(false),
                usable_now: grant.usable_now.unwrap_or(false),
                // Provider default is conservative: a grant assumed to require
                // the limit is never offered early.
                use_requires_limit: grant.use_requires_limit.unwrap_or(true),
                percent_used: grant
                    .percent_used
                    .iter()
                    .flatten()
                    .filter_map(|(name, value)| {
                        value
                            .as_u64()
                            .filter(|percent| *percent <= 100)
                            .map(|percent| (quota_window_name(name), percent))
                    })
                    .collect(),
                blocking: grant.blocking.clone().unwrap_or_default(),
            })
            .collect(),
        next_grant_id: status.next_grant_id.clone(),
        weekly_resets_at: status.weekly_resets_at.clone(),
        cooldown_until: status.cooldown_until.clone(),
    }
}

/// Maps provider window names to the cc-lb quota vocabulary
/// (`SubscriptionQuotaWindow::as_str`): `five_hour` -> `5h`,
/// `seven_day`/`seven_day_*` -> `7d`/`7d_*`. Unknown names pass through.
fn quota_window_name(provider_name: &str) -> String {
    if provider_name == "five_hour" {
        "5h".to_owned()
    } else if provider_name == "seven_day" {
        "7d".to_owned()
    } else if let Some(rest) = provider_name.strip_prefix("seven_day_") {
        format!("7d_{rest}")
    } else {
        provider_name.to_owned()
    }
}

fn claim_outcome_str(outcome: CedarEmberClaimOutcome) -> &'static str {
    match outcome {
        CedarEmberClaimOutcome::Reset => "reset",
        CedarEmberClaimOutcome::AlreadyUsed => "already_used",
        CedarEmberClaimOutcome::NotLimited => "not_limited",
        CedarEmberClaimOutcome::Cooldown => "cooldown",
        CedarEmberClaimOutcome::Ineligible => "ineligible",
        CedarEmberClaimOutcome::Unavailable => "unavailable",
        CedarEmberClaimOutcome::Unknown => "unknown",
    }
}

/// Read-path failures on the claim's live profile fetch: the provider
/// answered nothing usable, so a definite error is safe — nothing was
/// consumed.
fn provider_read_error(error: CedarEmberError) -> UpstreamError {
    match error {
        CedarEmberError::Timeout | CedarEmberError::Cancelled => UpstreamError::ProviderTimeout,
        CedarEmberError::ProviderStatus(status) => UpstreamError::ProviderError { status },
        // The serde message can embed provider field values; the API response
        // and logs only ever see this static category.
        CedarEmberError::Parse(_) => UpstreamError::ProviderMalformed {
            detail: "provider response failed schema validation".to_owned(),
        },
        CedarEmberError::Connect(detail)
        | CedarEmberError::Transport(detail)
        | CedarEmberError::Request(detail) => UpstreamError::ProviderUnreachable { detail },
    }
}

/// Claim-path failures: once the request may have reached the provider the
/// outcome is indeterminate, so timeout/cancel/mid-flight transport, an
/// unreadable 2xx body, and any provider 5xx all surface as
/// `claim_outcome_unknown` — never a fabricated failure. Only a provider
/// 4xx (definite rejection, forwarded as a 4xx so clients treat it as
/// final) or a connect failure (request never sent) are definite.
fn provider_claim_error(error: CedarEmberError) -> UpstreamError {
    match error {
        CedarEmberError::Connect(detail) => UpstreamError::ProviderUnreachable { detail },
        CedarEmberError::ProviderStatus(status) if status < 500 => {
            UpstreamError::ProviderRejected { status }
        }
        CedarEmberError::ProviderStatus(_)
        | CedarEmberError::Timeout
        | CedarEmberError::Cancelled
        | CedarEmberError::Transport(_)
        | CedarEmberError::Parse(_) => UpstreamError::ClaimOutcomeUnknown,
        CedarEmberError::Request(detail) => UpstreamError::Internal { detail },
    }
}
