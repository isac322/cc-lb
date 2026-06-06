use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use cc_lb_core::api_keys::secret;
use cc_lb_storage_api::organization_metadata::OrganizationMetadataRecord;
use cc_lb_storage_api::types::KeyStatus;
use cc_lb_storage_api::upstream_subscription_metadata::UpstreamSubscriptionMetadataRecord;
use cc_lb_storage_api::upstream_subscription_quota::{
    SubscriptionQuotaLatestRecord, SubscriptionQuotaWindow,
};
use cc_lb_storage_api::{
    OrganizationMetadataStore, PrincipalRecord, PrincipalStore, Storage,
    UpstreamSubscriptionMetadataStore, UpstreamSubscriptionQuotaStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::app::ProxyState;

type HandlerResult<T> = Result<Json<T>, (StatusCode, Json<Value>)>;

#[derive(Serialize)]
pub(crate) struct UsageBody {
    five_hour: Option<WindowBody>,
    seven_day: Option<WindowBody>,
    seven_day_opus: Option<WindowBody>,
    seven_day_sonnet: Option<WindowBody>,
    extra_usage: ExtraUsageBody,
}

#[derive(Serialize)]
struct WindowBody {
    utilization: Option<f64>,
    resets_at: Option<String>,
}

#[derive(Serialize)]
struct ExtraUsageBody {
    is_enabled: bool,
    monthly_limit: Option<f64>,
    used_credits: Option<f64>,
    utilization: Option<f64>,
}

#[derive(Serialize)]
pub(crate) struct RolesBody {
    organization_uuid: Option<String>,
    organization_role: Option<String>,
    subscription_type: &'static str,
}

#[derive(Serialize)]
pub(crate) struct ProfileBody {
    account: AccountBody,
    organization: Option<OrganizationBody>,
}

#[derive(Serialize)]
struct AccountBody {
    uuid: String,
    email: Option<String>,
    display_name: Option<String>,
}

#[derive(Serialize)]
struct OrganizationBody {
    uuid: String,
    name: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct TokenRequest {
    grant_type: String,
    refresh_token: String,
    #[allow(dead_code)]
    client_id: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct TokenBody {
    access_token: String,
    refresh_token: String,
    token_type: &'static str,
    expires_in: u64,
}

struct AuthContext {
    principal: PrincipalRecord,
}

pub(crate) async fn usage_handler(
    State(state): State<ProxyState>,
    headers: HeaderMap,
) -> HandlerResult<UsageBody> {
    let auth = authenticate(&state, &headers).await?;
    if auth.principal.allowed_upstreams.is_empty() {
        return Ok(Json(empty_usage()));
    }

    let records = UpstreamSubscriptionQuotaStore::list_latest_subscription_quota_for_upstreams(
        state.storage.as_ref(),
        &auth.principal.allowed_upstreams,
    )
    .await
    .map_err(storage_error)?;

    Ok(Json(UsageBody {
        five_hour: aggregate_window(&records, SubscriptionQuotaWindow::FiveHour),
        seven_day: aggregate_window(&records, SubscriptionQuotaWindow::SevenDay),
        seven_day_opus: aggregate_window(&records, SubscriptionQuotaWindow::SevenDayOpus),
        seven_day_sonnet: aggregate_window(&records, SubscriptionQuotaWindow::SevenDaySonnet),
        extra_usage: aggregate_extra_usage(&records),
    }))
}

pub(crate) async fn roles_handler(
    State(state): State<ProxyState>,
    headers: HeaderMap,
) -> HandlerResult<RolesBody> {
    let auth = authenticate(&state, &headers).await?;
    let Some((metadata, organization)) =
        first_metadata_pair(&state.storage, &auth.principal).await?
    else {
        return Ok(Json(default_roles()));
    };

    Ok(Json(RolesBody {
        organization_uuid: metadata.organization_uuid,
        organization_role: normalized_role(metadata.organization_role),
        subscription_type: subscription_type(organization.as_ref()),
    }))
}

pub(crate) async fn profile_handler(
    State(state): State<ProxyState>,
    headers: HeaderMap,
) -> HandlerResult<ProfileBody> {
    let auth = authenticate(&state, &headers).await?;
    let Some((_, Some(organization))) =
        first_metadata_pair(&state.storage, &auth.principal).await?
    else {
        return Ok(Json(ProfileBody {
            account: AccountBody {
                uuid: auth.principal.id.to_string(),
                email: None,
                display_name: None,
            },
            organization: None,
        }));
    };

    Ok(Json(ProfileBody {
        account: AccountBody {
            uuid: organization
                .account_uuid
                .clone()
                .unwrap_or_else(|| auth.principal.id.to_string()),
            email: organization.account_email.clone(),
            display_name: organization.account_display_name.clone(),
        },
        organization: Some(OrganizationBody {
            uuid: organization.organization_uuid,
            name: organization.organization_name,
        }),
    }))
}

// Tier 2 stub: Claude Code currently probes this endpoint, but cc-lb has no persisted account settings model yet.
pub(crate) async fn account_settings_handler(
    State(state): State<ProxyState>,
    headers: HeaderMap,
) -> HandlerResult<Value> {
    let _auth = authenticate(&state, &headers).await?;
    Ok(Json(json!({})))
}

pub(crate) async fn token_handler(
    State(state): State<ProxyState>,
    Json(request): Json<TokenRequest>,
) -> Result<Json<TokenBody>, (StatusCode, Json<Value>)> {
    if request.grant_type != "refresh_token" {
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
            "only refresh_token is supported",
        ));
    }

    if secret::parse(&request.refresh_token).is_err() {
        return Err(oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "invalid refresh_token format",
        ));
    }

    let mut headers = HeaderMap::new();
    let value = HeaderValue::from_str(&request.refresh_token).map_err(|_| {
        oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "invalid refresh_token format",
        )
    })?;
    headers.insert("x-api-key", value);

    let view = state.lifecycle.dynamic_view().load();
    let Some(authn) = &state.builtin_authn else {
        return Err(rejected_refresh_token());
    };
    let success = authn
        .authenticate(&headers, &view.principal_view)
        .await
        .map_err(|_| rejected_refresh_token())?;
    if success.record.status != KeyStatus::Active {
        return Err(rejected_refresh_token());
    }

    // Tier 3 follow-up: replace this compatibility stub with real refresh-token rotation and access-token minting.
    Ok(Json(TokenBody {
        access_token: request.refresh_token.clone(),
        refresh_token: request.refresh_token,
        token_type: "Bearer",
        expires_in: 31_536_000,
    }))
}

async fn authenticate(state: &ProxyState, headers: &HeaderMap) -> HandlerResult<AuthContext> {
    let view = state.lifecycle.dynamic_view().load();
    let Some(authn) = &state.builtin_authn else {
        return Err(invalid_token("api key storage unavailable"));
    };
    let success = authn
        .authenticate(headers, &view.principal_view)
        .await
        .map_err(|source| invalid_token(&source.to_string()))?;
    let principal_id = Uuid::parse_str(&success.principal_id)
        .map_err(|_| server_error("authenticated principal id is not a uuid"))?;
    let principal = PrincipalStore::get_by_id(state.storage.as_ref(), principal_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| server_error("authenticated principal is unavailable"))?;

    Ok(Json(AuthContext { principal }))
}

async fn first_metadata_pair(
    storage: &Arc<dyn Storage>,
    principal: &PrincipalRecord,
) -> Result<
    Option<(
        UpstreamSubscriptionMetadataRecord,
        Option<OrganizationMetadataRecord>,
    )>,
    (StatusCode, Json<Value>),
> {
    let Some(upstream_id) = principal.allowed_upstreams.first().copied() else {
        return Ok(None);
    };
    let Some(metadata) = UpstreamSubscriptionMetadataStore::get_upstream_subscription_metadata(
        storage.as_ref(),
        upstream_id,
    )
    .await
    .map_err(storage_error)?
    else {
        return Ok(None);
    };
    let Some(organization_uuid) = metadata.organization_uuid.as_deref() else {
        return Ok(None);
    };
    let organization =
        OrganizationMetadataStore::get_organization_metadata(storage.as_ref(), organization_uuid)
            .await
            .map_err(storage_error)?;

    Ok(Some((metadata, organization)))
}

fn aggregate_window(
    records: &[SubscriptionQuotaLatestRecord],
    window: SubscriptionQuotaWindow,
) -> Option<WindowBody> {
    let mut saw_window = false;
    let mut utilization: Option<f64> = None;
    let mut reset: Option<u64> = None;
    let now = now_secs();

    for record in records.iter().filter(|record| record.window == window) {
        saw_window = true;
        if let Some(value) = record.utilization {
            let clamped = clamp_unit(value);
            utilization = Some(utilization.map_or(clamped, |current| current.max(clamped)));
        }
        if let Some(candidate) = record.resets_at_unix_secs.filter(|value| *value > now) {
            reset = Some(reset.map_or(candidate, |current| current.min(candidate)));
        }
    }

    if !saw_window || (utilization.is_none() && reset.is_none()) {
        return None;
    }

    Some(WindowBody {
        utilization,
        resets_at: reset.map(unix_to_iso8601),
    })
}

fn aggregate_extra_usage(records: &[SubscriptionQuotaLatestRecord]) -> ExtraUsageBody {
    let mut latest_by_upstream: HashMap<Uuid, &SubscriptionQuotaLatestRecord> = HashMap::new();
    for record in records.iter().filter(|record| has_extra_usage(record)) {
        latest_by_upstream
            .entry(record.upstream_id)
            .and_modify(|current| {
                if record.observed_at_unix_millis > current.observed_at_unix_millis {
                    *current = record;
                }
            })
            .or_insert(record);
    }

    let mut is_enabled = false;
    let mut monthly_limit = None;
    let mut used_credits = None;

    for record in latest_by_upstream.values() {
        is_enabled |= record.extra_usage_enabled.unwrap_or(false);
        if let Some(value) = record.extra_usage_monthly_limit {
            monthly_limit = Some(monthly_limit.unwrap_or(0.0) + value);
        }
        if let Some(value) = record.extra_usage_used_credits {
            used_credits = Some(used_credits.unwrap_or(0.0) + value);
        }
    }

    let utilization = match (monthly_limit, used_credits) {
        (Some(limit), Some(used)) if limit > 0.0 => Some(clamp_unit(used / limit)),
        _ => None,
    };

    ExtraUsageBody {
        is_enabled,
        monthly_limit,
        used_credits,
        utilization,
    }
}

fn has_extra_usage(record: &SubscriptionQuotaLatestRecord) -> bool {
    record.extra_usage_enabled.is_some()
        || record.extra_usage_monthly_limit.is_some()
        || record.extra_usage_used_credits.is_some()
}

fn empty_usage() -> UsageBody {
    UsageBody {
        five_hour: None,
        seven_day: None,
        seven_day_opus: None,
        seven_day_sonnet: None,
        extra_usage: ExtraUsageBody {
            is_enabled: false,
            monthly_limit: None,
            used_credits: None,
            utilization: None,
        },
    }
}

fn default_roles() -> RolesBody {
    RolesBody {
        organization_uuid: None,
        organization_role: None,
        subscription_type: "pro",
    }
}

fn normalized_role(role: Option<String>) -> Option<String> {
    match role.as_deref() {
        Some("admin" | "member") => role,
        _ => None,
    }
}

fn subscription_type(organization: Option<&OrganizationMetadataRecord>) -> &'static str {
    let Some(organization) = organization else {
        return "pro";
    };
    let billing_type = organization.billing_type.as_deref().unwrap_or_default();
    let billing_type = billing_type.to_ascii_lowercase();
    if billing_type.contains("max") {
        "max"
    } else if billing_type.contains("team") {
        "team"
    } else if organization.organization_type.as_deref() == Some("enterprise") {
        "enterprise"
    } else {
        "pro"
    }
}

fn clamp_unit(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

fn invalid_token(description: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error":"invalid_token","error_description":description})),
    )
}

fn rejected_refresh_token() -> (StatusCode, Json<Value>) {
    oauth_error(
        StatusCode::UNAUTHORIZED,
        "invalid_grant",
        "refresh_token rejected",
    )
}

fn oauth_error(
    status: StatusCode,
    error: &'static str,
    description: &'static str,
) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({"error":error,"error_description":description})),
    )
}

fn storage_error(source: cc_lb_storage_api::StorageError) -> (StatusCode, Json<Value>) {
    server_error(&source.to_string())
}

fn server_error(description: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":"server_error","error_description":description})),
    )
}

fn unix_to_iso8601(timestamp: u64) -> String {
    chrono::DateTime::from_timestamp(timestamp as i64, 0)
        .unwrap_or_default()
        .to_rfc3339()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
