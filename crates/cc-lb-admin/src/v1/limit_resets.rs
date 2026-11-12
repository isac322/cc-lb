//! Cached Anthropic and OpenAI subscription limit-reset endpoints.
//!
//! Fleet and per-upstream GETs serve usage-poll snapshots without calling
//! either provider. `POST .../limit-resets/claim` revalidates the provider
//! identity and dispatches a single selected grant/credit claim,
//! serialized per upstream by a durable
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
    CedarEmberClaimOutcome, CedarEmberClaimRequest, CedarEmberClaimResult, CedarEmberError,
    CedarEmberIdentityRecord, CedarEmberPollRecord, CedarEmberStatus, cedar_ember_epoch_fence,
    cedar_ember_epoch_meta_key, cedar_ember_identity_meta_key, cedar_ember_meta_key,
    claim_cedar_ember_reset, fetch_oauth_profile_at, is_valid_grant_id, make_metadata_http_client,
    settled_cedar_ember_epoch,
};
use cc_lb_control::openai_limit_resets::{
    OpenAiLimitResetAuth, OpenAiLimitResetConsumeOutcome, OpenAiLimitResetConsumeRequest,
    OpenAiLimitResetConsumeResult, OpenAiLimitResetError, OpenAiLimitResetInventory,
    OpenAiLimitResetPollRecord, consume_openai_limit_reset_credit, list_openai_limit_reset_credits,
    openai_limit_reset_epoch_fence, openai_limit_reset_epoch_meta_key, openai_limit_reset_meta_key,
    settled_openai_limit_reset_epoch,
};
use cc_lb_storage_api::{
    Storage, SubscriptionQuotaSample, UpstreamKind, UpstreamRecord, UpstreamStore,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::post_commit::complete_after_commit;
use super::upstreams::{
    UpstreamError, decrypt_oauth_bundle, fresh_enough_access_token, metadata_user_agent,
    storage_arc, upstream_base_url,
};
use crate::{
    AdminState, LimitResetUsageRefresh,
    audit::{AdminAuditEvent, record_admin_audit},
    auth::AdminIdentity,
};

#[derive(Debug, Serialize)]
pub(crate) struct LimitResetsResponse {
    provider: &'static str,
    /// Null until the metadata hook has observed the OAuth identity; the
    /// claim endpoint re-verifies the live identity regardless.
    account_id: Option<String>,
    organization_id: Option<String>,
    cedar_ember: Option<CedarEmberStatusResponse>,
    openai: Option<OpenAiLimitResetInventory>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FleetLimitResetsResponse {
    upstreams: Vec<UpstreamLimitResetsResponse>,
}

#[derive(Debug, Serialize)]
struct UpstreamLimitResetsResponse {
    upstream_id: Uuid,
    #[serde(flatten)]
    snapshot: LimitResetsResponse,
}

#[derive(Debug, Serialize)]
struct CedarEmberStatusResponse {
    eligible: bool,
    ineligible_reason: Option<String>,
    at_limit: Option<bool>,
    /// Provider window names mapped to the cc-lb quota vocabulary
    /// (`five_hour` -> `5h`, `seven_day*` -> `7d*`).
    exhausted: Option<Vec<String>>,
    grants: Vec<LimitResetGrantResponse>,
    next_grant_id: Option<String>,
    weekly_resets_at: Option<String>,
    cooldown_until: Option<String>,
}

#[derive(Debug, Serialize)]
struct LimitResetGrantResponse {
    id: String,
    label: String,
    #[serde(rename = "type")]
    grant_type: Option<String>,
    gives_back_weekly: Option<bool>,
    unavailable_reason: Option<String>,
    resets_total: Option<u32>,
    resets_left: u32,
    starts_at: Option<String>,
    ends_at: Option<String>,
    clears: Vec<String>,
    paused: bool,
    usable_now: bool,
    use_requires_limit: bool,
    percent_used: BTreeMap<String, serde_json::Number>,
    blocking: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LimitResetClaimBody {
    account_id: String,
    organization_id: Option<String>,
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
    windows_reset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage_refresh: Option<LimitResetUsageRefresh>,
}

pub(crate) async fn list_upstream_limit_resets(
    State(state): State<AdminState>,
) -> Result<Json<FleetLimitResetsResponse>, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstreams: Vec<_> = super::upstreams::list_all_upstreams(&state)
        .await?
        .into_iter()
        .filter(|upstream| {
            upstream.enabled
                && upstream.deleted_at_unix_secs.is_none()
                && matches!(
                    upstream.kind,
                    UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth
                )
                && upstream.oauth_credentials.is_some()
        })
        .collect();
    let upstream_ids: Vec<_> = upstreams.iter().map(|upstream| upstream.id).collect();
    let mut latest_by_upstream = BTreeMap::<Uuid, Vec<SubscriptionQuotaSample>>::new();
    if !upstream_ids.is_empty() {
        for sample in storage
            .list_latest_subscription_quota_for_upstreams(&upstream_ids)
            .await?
        {
            latest_by_upstream
                .entry(sample.upstream_id)
                .or_default()
                .push(sample);
        }
    }
    let mut snapshots = Vec::with_capacity(upstreams.len());
    for upstream in upstreams {
        let latest = latest_by_upstream.remove(&upstream.id).unwrap_or_default();
        snapshots.push(UpstreamLimitResetsResponse {
            upstream_id: upstream.id,
            snapshot: load_cached_limit_resets(&state, storage.as_ref(), &upstream, &latest)
                .await?,
        });
    }
    Ok(Json(FleetLimitResetsResponse {
        upstreams: snapshots,
    }))
}

pub(crate) async fn get_upstream_limit_resets(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
) -> Result<Json<LimitResetsResponse>, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if !matches!(
        upstream.kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth
    ) || upstream.oauth_credentials.is_none()
    {
        return Err(UpstreamError::NotOauthUpstream);
    }
    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&[upstream_id])
        .await?;
    Ok(Json(
        load_cached_limit_resets(&state, storage.as_ref(), &upstream, &latest).await?,
    ))
}

/// Loads only cached metadata. Callers batch quota samples before invoking
/// this loader; missing, stale, rotated or fenced observations remain null.
pub(crate) async fn load_cached_limit_resets(
    state: &AdminState,
    storage: &dyn Storage,
    upstream: &UpstreamRecord,
    latest: &[SubscriptionQuotaSample],
) -> Result<LimitResetsResponse, UpstreamError> {
    if upstream.kind == UpstreamKind::OpenaiOauth {
        return load_cached_openai_limit_resets(state, storage, upstream).await;
    }
    let upstream_id = upstream.id;
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
    let mut cedar_ember = match (coupon, account_id.as_ref(), fingerprint) {
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
    if let Some(status) = &mut cedar_ember {
        let snapshots = state
            .dynamic_view
            .load()
            .subscription_quota_cache
            .snapshot_for_upstream(upstream_id, now_unix_millis, max_staleness_secs);
        if crate::subscription_quotas::five_hour_is_absent(
            latest.iter(),
            snapshots.iter().find(|snapshot| snapshot.window == "5h"),
            now_unix_millis,
            max_staleness_secs,
        ) {
            remove_absent_five_hour(status);
        }
    }

    Ok(LimitResetsResponse {
        provider: "anthropic",
        account_id,
        organization_id,
        cedar_ember,
        openai: None,
    })
}

async fn load_cached_openai_limit_resets(
    state: &AdminState,
    storage: &dyn Storage,
    upstream: &UpstreamRecord,
) -> Result<LimitResetsResponse, UpstreamError> {
    let bundle = decrypt_oauth_bundle(state, upstream)?;
    let account_id = bundle
        .account_id
        .filter(|id| !id.trim().is_empty())
        .or_else(|| cc_lb_oauth_protocol::account_id_from_access_token(&bundle.access_token));
    let inventory_key = openai_limit_reset_meta_key(upstream.id);
    let epoch_key = openai_limit_reset_epoch_meta_key(upstream.id);
    let (raw, epoch) = tokio::join!(
        storage.get_meta_value(&inventory_key),
        storage.get_meta_value(&epoch_key),
    );
    let current_epoch = settled_openai_limit_reset_epoch(epoch?.as_deref());
    let record = raw?.and_then(|raw| serde_json::from_str::<OpenAiLimitResetPollRecord>(&raw).ok());
    let now = cc_lb_clock::unix_millis(state.clock.now()).min(u128::from(u64::MAX)) as u64;
    let max_age = state
        .dynamic_view
        .load()
        .subscription_quota_routing_max_staleness_secs
        .saturating_mul(1_000);
    let openai = record
        .filter(|record| {
            Some(record.credential_fingerprint) == upstream.oauth_credential_fingerprint()
                && record.oauth_token_generation == upstream.oauth_token_generation
                && Some(record.account_id.as_str()) == account_id.as_deref()
                && current_epoch == Some(record.epoch)
                && now.saturating_sub(record.observed_at_unix_millis) <= max_age
        })
        .and_then(|record| record.inventory);
    Ok(LimitResetsResponse {
        provider: "openai",
        account_id,
        organization_id: None,
        cedar_ember: None,
        openai,
    })
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
    // Dispatch, fence settlement, usage refresh and audit belong to one
    // continuation, so dropping the HTTP request cannot abandon a dispatched claim.
    complete_after_commit(async move {
        let dispatched =
            dispatch_limit_reset_claim(&state, storage.clone(), &upstream, &body).await;
        let result = match dispatched {
            Ok((fence, claim)) => {
                settle_limit_reset_claim(&state, storage.as_ref(), upstream.id, fence, claim).await
            }
            Err(error) => Err(error),
        };
        // Audit every outcome, including errors: a 504/timeout may still have
        // consumed the grant provider-side, so the request_id trail must exist
        // even when the handler returns an error.
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
    })
    .await?
}

/// The durable epoch fence a dispatched claim holds until it is settled.
struct ClaimFence {
    epoch_key: String,
    fence: String,
}

/// 409 for a claim refused before dispatch because the upstream's epoch is
/// not settled: another claim holds the fence, an abandoned fence awaits
/// poll recovery, or the value is unreadable. Nothing was sent.
fn claim_in_progress() -> UpstreamError {
    UpstreamError::Conflict {
        detail: "a limit-reset claim for this upstream is in progress or awaiting a fresh usage observation; refetch limit-resets".to_owned(),
    }
}

/// Validates the claim, acquires the epoch fence and dispatches the provider
/// claim. Returns the held fence with the raw claim result; any error means
/// nothing was dispatched.
async fn dispatch_limit_reset_claim(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream: &UpstreamRecord,
    body: &LimitResetClaimBody,
) -> Result<(ClaimFence, Result<LimitResetClaimResponse, UpstreamError>), UpstreamError> {
    if upstream.kind == UpstreamKind::OpenaiOauth {
        return dispatch_openai_limit_reset_claim(state, storage, upstream, body).await;
    }
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
    if body
        .organization_id
        .as_deref()
        .is_none_or(|id| id.trim().is_empty())
    {
        return Err(UpstreamError::BadRequest {
            error: "invalid_organization_id",
            detail: "organization_id is required for Anthropic limit resets".to_owned(),
        });
    }
    let bundle = decrypt_oauth_bundle(state, upstream)?;
    let access_token = fresh_enough_access_token(state, storage.clone(), upstream, bundle).await?;
    let user_agent = metadata_user_agent(storage.as_ref()).await?;
    let base_url = upstream_base_url(upstream)?;
    let client = make_metadata_http_client();
    let cancel = CancellationToken::new();

    // The submitted account/org must still match the live OAuth identity;
    // cached metadata records are deliberately not consulted.
    let profile = fetch_oauth_profile_at(&client, &base_url, &access_token, &user_agent, &cancel)
        .await
        .map_err(provider_read_error)?;
    let (account_id, organization_id) = live_identity(&profile)?;
    if body.account_id != account_id
        || body.organization_id.as_deref() != Some(organization_id.as_str())
    {
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
    Ok((
        ClaimFence { epoch_key, fence },
        result
            .map(anthropic_claim_response)
            .map_err(provider_claim_error),
    ))
}

async fn dispatch_openai_limit_reset_claim(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream: &UpstreamRecord,
    body: &LimitResetClaimBody,
) -> Result<(ClaimFence, Result<LimitResetClaimResponse, UpstreamError>), UpstreamError> {
    if Uuid::parse_str(&body.request_id).is_err() {
        return Err(UpstreamError::BadRequest {
            error: "invalid_request_id",
            detail: "request_id must be a UUID".to_owned(),
        });
    }
    if body.grant_id.is_empty()
        || body.grant_id.len() > 1_024
        || body.grant_id.chars().any(char::is_control)
    {
        return Err(UpstreamError::BadRequest {
            error: "invalid_grant_id",
            detail: "grant_id must be a nonempty provider credit ID".to_owned(),
        });
    }
    let bundle = decrypt_oauth_bundle(state, upstream)?;
    let access_token = fresh_enough_access_token(state, storage.clone(), upstream, bundle).await?;
    // Refresh can rotate the bundle/account; bind to the credential actually
    // stored now rather than a cached inventory or the pre-refresh bundle.
    let current = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    let bundle = decrypt_oauth_bundle(state, &current)?;
    let account_id = bundle
        .account_id
        .filter(|id| !id.trim().is_empty())
        .or_else(|| cc_lb_oauth_protocol::account_id_from_access_token(&bundle.access_token))
        .ok_or(UpstreamError::StaleIdentity)?;
    if body.account_id != account_id || bundle.access_token != access_token {
        return Err(UpstreamError::StaleIdentity);
    }
    let client = make_metadata_http_client();
    let base_url = current.base_url.clone().unwrap_or_else(|| {
        url::Url::parse("https://chatgpt.com/backend-api/codex")
            .expect("static Codex OAuth base URL")
    });
    let cancel = CancellationToken::new();
    let auth = OpenAiLimitResetAuth {
        access_token: &access_token,
        account_id: &account_id,
        user_agent: "codex-cli",
    };
    let epoch_key = openai_limit_reset_epoch_meta_key(upstream.id);
    let epoch = storage.get_meta_value(&epoch_key).await?;
    if settled_openai_limit_reset_epoch(epoch.as_deref()).is_none() {
        return Err(claim_in_progress());
    }
    let inventory = list_openai_limit_reset_credits(&client, &base_url, auth, &cancel)
        .await
        .map_err(openai_read_error)?;
    if !inventory.credits.iter().any(|credit| {
        credit.id == body.grant_id
            && credit.status == "available"
            && credit.is_supported_by_plan == Some(true)
    }) {
        return Err(UpstreamError::Conflict {
            detail: "selected credit is unavailable or its plan support is unknown; refetch limit-resets".to_owned(),
        });
    }
    if UpstreamStore::read_oauth_token_generation(storage.as_ref(), upstream.id).await?
        != Some(current.oauth_token_generation)
    {
        return Err(UpstreamError::StaleIdentity);
    }
    let now = cc_lb_clock::unix_millis(state.clock.now()).min(u128::from(u64::MAX)) as u64;
    let fence = openai_limit_reset_epoch_fence(Uuid::new_v4(), now);
    if !storage
        .compare_and_put_meta_value(&epoch_key, epoch.as_deref(), &fence)
        .await?
    {
        return Err(claim_in_progress());
    }
    let result = consume_openai_limit_reset_credit(
        &client,
        &base_url,
        auth,
        OpenAiLimitResetConsumeRequest {
            redeem_request_id: &body.request_id,
            credit_id: &body.grant_id,
        },
        &cancel,
    )
    .await
    .map(openai_claim_response)
    .map_err(openai_claim_error);
    Ok((ClaimFence { epoch_key, fence }, result))
}

fn anthropic_claim_response(result: CedarEmberClaimResult) -> LimitResetClaimResponse {
    LimitResetClaimResponse {
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
        windows_reset: None,
        usage_refresh: None,
    }
}

fn openai_claim_response(result: OpenAiLimitResetConsumeResult) -> LimitResetClaimResponse {
    LimitResetClaimResponse {
        result: match result.code {
            OpenAiLimitResetConsumeOutcome::Reset => "reset",
            OpenAiLimitResetConsumeOutcome::AlreadyRedeemed => "already_used",
            OpenAiLimitResetConsumeOutcome::NothingToReset => "not_limited",
            OpenAiLimitResetConsumeOutcome::NoCredit => "unavailable",
        },
        reason: None,
        cleared: Vec::new(),
        resets_left: None,
        weekly_resets_at: None,
        cooldown_until: None,
        windows_reset: result.windows_reset,
        usage_refresh: None,
    }
}

fn openai_read_error(error: OpenAiLimitResetError) -> UpstreamError {
    match error {
        OpenAiLimitResetError::Timeout | OpenAiLimitResetError::Cancelled => {
            UpstreamError::ProviderTimeout
        }
        OpenAiLimitResetError::ProviderStatus(status) => UpstreamError::ProviderError { status },
        OpenAiLimitResetError::Parse(_) => UpstreamError::ProviderMalformed {
            detail: "provider response failed schema validation".to_owned(),
        },
        OpenAiLimitResetError::Connect(detail)
        | OpenAiLimitResetError::Transport(detail)
        | OpenAiLimitResetError::Request(detail) => UpstreamError::ProviderUnreachable { detail },
    }
}

fn openai_claim_error(error: OpenAiLimitResetError) -> UpstreamError {
    if error.consume_outcome_is_uncertain() {
        return UpstreamError::ClaimOutcomeUnknown;
    }
    match error {
        OpenAiLimitResetError::Connect(detail) => UpstreamError::ProviderUnreachable { detail },
        OpenAiLimitResetError::ProviderStatus(status) => UpstreamError::ProviderRejected { status },
        OpenAiLimitResetError::Request(detail) => UpstreamError::Internal { detail },
        _ => UpstreamError::ClaimOutcomeUnknown,
    }
}

/// Settles the claim's own fence and maps the provider outcome.
async fn settle_limit_reset_claim(
    state: &AdminState,
    storage: &dyn Storage,
    upstream_id: Uuid,
    ClaimFence { epoch_key, fence }: ClaimFence,
    result: Result<LimitResetClaimResponse, UpstreamError>,
) -> Result<Response, UpstreamError> {
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
        tracing::warn!(upstream_id = %upstream_id, "cedar_ember claim fence was already recovered");
    }
    let mut result = result?;
    if result.result == "reset" || result.result == "already_used" {
        let cancel = CancellationToken::new();
        result.usage_refresh = Some(match &state.limit_reset_usage_refresh {
            Some(refresh) => match tokio::time::timeout(
                std::time::Duration::from_secs(30),
                refresh.refresh_upstream_usage(upstream_id, &cancel),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_) => {
                    cancel.cancel();
                    LimitResetUsageRefresh::Failed
                }
            },
            None => LimitResetUsageRefresh::Failed,
        });
    }
    Ok(Json(result).into_response())
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

fn remove_absent_five_hour(status: &mut CedarEmberStatusResponse) {
    if let Some(exhausted) = &mut status.exhausted {
        exhausted.retain(|window| window != "5h");
    }
    status.grants.retain(|grant| {
        !grant.clears.iter().any(|window| window == "5h")
            || grant.clears.iter().any(|window| window != "5h")
    });
    for grant in &mut status.grants {
        grant
            .blocking
            .retain(|window| window != "5h" && window != "five_hour");
        grant.clears.retain(|window| window != "5h");
        grant.percent_used.remove("5h");
    }
    if status
        .next_grant_id
        .as_ref()
        .is_some_and(|id| !status.grants.iter().any(|grant| &grant.id == id))
    {
        status.next_grant_id = None;
    }
}

fn cedar_ember_response(status: &CedarEmberStatus) -> CedarEmberStatusResponse {
    CedarEmberStatusResponse {
        eligible: status.eligible,
        ineligible_reason: status.ineligible_reason.clone(),
        at_limit: status.at_limit,
        exhausted: status
            .exhausted
            .as_ref()
            .map(|windows| windows.iter().map(|name| quota_window_name(name)).collect()),
        grants: status
            .grants
            .iter()
            .map(|grant| LimitResetGrantResponse {
                id: grant.id.clone(),
                label: grant.label.clone().unwrap_or_default(),
                grant_type: grant.grant_type.clone(),
                gives_back_weekly: grant.gives_back_weekly,
                unavailable_reason: grant.unavailable_reason.clone(),
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
                        value.as_number().and_then(|number| {
                            number
                                .as_f64()
                                .filter(|percent| (0.0..=100.0).contains(percent))
                                .map(|_| (quota_window_name(name), number.clone()))
                        })
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

#[cfg(test)]
mod absent_five_hour_tests {
    use super::*;

    #[test]
    fn absent_five_hour_removes_only_five_hour_coupon_nudges() {
        let provider: CedarEmberStatus = serde_json::from_value(serde_json::json!({
            "eligible": true,
            "at_limit": true,
            "exhausted": ["five_hour", "seven_day"],
            "next_grant_id": "five-only",
            "grants": [
                {
                    "id": "five-only", "resets_left": 1,
                    "clears": ["five_hour"], "usable_now": true,
                    "percent_used": {"five_hour": 100}
                },
                {
                    "id": "weekly", "resets_left": 1,
                    "clears": ["five_hour", "seven_day"], "usable_now": true,
                    "percent_used": {"five_hour": 100, "seven_day": 100}
                }
            ]
        }))
        .expect("provider status fixture");
        let mut response = cedar_ember_response(&provider);
        remove_absent_five_hour(&mut response);
        assert_eq!(response.at_limit, Some(true));
        assert_eq!(response.exhausted, Some(vec!["7d".to_owned()]));
        assert_eq!(response.grants.len(), 1);
        let weekly = response.grants.first().expect("weekly grant is retained");
        assert_eq!(weekly.id, "weekly");
        assert_eq!(weekly.clears, ["7d"]);
        assert_eq!(
            weekly.percent_used.get("7d"),
            Some(&serde_json::Number::from(100))
        );
        assert!(!weekly.percent_used.contains_key("5h"));
        assert!(response.next_grant_id.is_none());

        response.exhausted = Some(vec!["5h".to_owned()]);
        remove_absent_five_hour(&mut response);
        assert_eq!(response.at_limit, Some(true));
    }

    #[test]
    fn absent_five_hour_preserves_all_provider_gate_states() {
        for at_limit in [Some(true), Some(false), None] {
            let provider: CedarEmberStatus = serde_json::from_value(json!({
                "eligible": true,
                "at_limit": at_limit,
                "exhausted": null,
                "grants": [{
                    "id": "weekly", "resets_left": 1,
                    "clears": ["five_hour", "seven_day"],
                    "percent_used": {"five_hour": 100, "seven_day": 42.5}
                }]
            }))
            .expect("provider status fixture");
            let mut response = cedar_ember_response(&provider);
            remove_absent_five_hour(&mut response);
            assert_eq!(response.at_limit, at_limit);
            assert!(response.exhausted.is_none());
            assert_eq!(response.grants[0].clears, ["7d"]);
            assert_eq!(
                response.grants[0].percent_used["7d"],
                serde_json::Number::from_f64(42.5).unwrap()
            );
        }
    }
}
