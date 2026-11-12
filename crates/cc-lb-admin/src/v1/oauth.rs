use std::sync::Arc;
use std::time::Duration;

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_aead::{AeadEncryptedField, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::{AnthropicOAuthConfig, AntigravityOAuthConfig, OpenAiOAuthConfig};
use cc_lb_control::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_control::anthropic_metadata::make_metadata_http_client;
use cc_lb_control::{AuditPayload, MetadataRefreshRecords, fetch_metadata_only};
use cc_lb_oauth_protocol::{
    OAuthAccountIdentity, gemini_identity_from_responses, gemini_identity_is_bound,
    oauth_account_from_bootstrap, openai_identity_from_token,
};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::jobs::oauth_usage_poll::OAuthUsagePollCronJob;
use cc_lb_scheduler::worker::{AdaptiveJob, CronJob, SchedulerPushTask};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{
    AuditActorFields, OAuthTokensCreate, OrganizationMetadataRecord, Storage, StorageError,
    StoredOAuthPkceFlow, UpstreamCreate, UpstreamRecord, UpstreamStore,
    UpstreamSubscriptionMetadataRecord, WarmupLane, validate_identifier,
};
use oauth2::{AuthUrl, ClientId, TokenUrl};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use super::post_commit::{PostCommitAborted, complete_after_commit, rebuild_after_audit_failure};
use crate::AdminState;
use crate::audit::{AdminAuditEvent, record_admin_audit};
use crate::auth::AdminIdentity;
use crate::oauth_pkce::{
    HyperOAuthHttpClient, LongLivedFallbackReason, OAuthTokenError, OAuthTokenMode,
    PkceExchangeOutcome, PkceHandshakeState, complete_pkce_flow, complete_pkce_form_flow,
    start_pkce_flow, start_pkce_flow_without_code,
};

const PKCE_FLOW_TTL_SECS: u64 = 900;

const METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OAUTH_UPSTREAM_BASE_URL: &str = "https://api.anthropic.com";

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/oauth/draft/start", post(start_oauth_draft))
        .route("/admin/v1/oauth/draft/complete", post(complete_oauth_draft))
        .route(
            "/admin/v1/upstreams/from-oauth-draft",
            post(create_upstream_from_oauth_draft),
        )
        .route("/admin/v1/upstreams/{id}/oauth/start", post(start_oauth))
        .route(
            "/admin/v1/upstreams/{id}/oauth/complete",
            post(complete_oauth),
        )
        .route(
            "/admin/v1/upstreams/{id}/oauth/status",
            get(get_oauth_status),
        )
}

#[derive(Serialize)]
struct UpstreamOAuthStatusResponse {
    upstream_id: String,
    kind: UpstreamKind,
    has_credentials: bool,
    status: &'static str,
    expires_at_unix_secs: Option<u64>,
    refresh_token_present: bool,
    refresh_token_expires_at_unix_secs: Option<u64>,
    scopes: Vec<String>,
    /// `None` when there are no decryptable credentials.
    mode: Option<OAuthTokenMode>,
    can_refresh: bool,
    #[serde(flatten)]
    account_identity: OAuthAccountIdentity,
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum OAuthProvider {
    #[default]
    Anthropic,
    Openai,
    Gemini,
}

#[derive(Deserialize)]
struct StartRequest {
    #[serde(default)]
    provider: OAuthProvider,
    #[serde(default)]
    flow: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenAiFlow {
    DeviceCode,
    Browser,
}

fn parse_openai_flow(value: Option<&str>) -> Result<OpenAiFlow, ()> {
    match value.unwrap_or("device_code") {
        "device_code" => Ok(OpenAiFlow::DeviceCode),
        "browser" => Ok(OpenAiFlow::Browser),
        _ => Err(()),
    }
}

fn unsupported_flow_response() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "unsupported_flow" })),
    )
        .into_response()
}
// `revision` is echoed so the frontend can refresh its cached `If-Match`
// before a cancel-cleanup DELETE. start_oauth does not currently mutate
// the upstream, but the contract stays correct if it ever does.
#[derive(Serialize)]
struct StartResponse {
    authorize_url: Option<String>,
    state_token: String,
    revision: u64,
    provider: OAuthProvider,
    flow: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    poll_interval_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at_unix_secs: Option<u64>,
}
#[derive(Serialize)]
struct DraftStartResponse {
    authorize_url: Option<String>,
    state_token: String,
    provider: OAuthProvider,
    flow: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    poll_interval_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at_unix_secs: Option<u64>,
}

#[derive(Deserialize)]
struct CompleteRequest {
    state_token: String,
    #[serde(default)]
    code: Option<String>,
}

#[derive(Deserialize)]
struct CreateFromDraftRequest {
    state_token: String,
    name: String,
    #[serde(default)]
    base_url: Option<Url>,
}

#[derive(Serialize)]
struct CompleteResponse {
    upstream_id: Uuid,
    expires_at_unix_secs: u64,
    access_token_fingerprint: String,
    mode: OAuthTokenMode,
    long_lived_fallback: bool,
    fallback_reason: Option<LongLivedFallbackReason>,
    granted_expires_in_secs: Option<u64>,
}

#[derive(Serialize)]
struct DraftCompleteResponse {
    state_token: String,
    suggested_name: String,
    subscription_metadata: Option<UpstreamSubscriptionMetadataRecord>,
    organization_metadata: Option<OrganizationMetadataRecord>,
    mode: OAuthTokenMode,
    long_lived_fallback: bool,
    fallback_reason: Option<LongLivedFallbackReason>,
    granted_expires_in_secs: Option<u64>,
    provider: OAuthProvider,
    #[serde(flatten)]
    account_identity: OAuthAccountIdentity,
}

#[derive(Serialize)]
struct UpstreamResponse {
    id: String,
    name: String,
    kind: UpstreamKind,
    base_url: Option<Url>,
    enabled: bool,
    warmup_enabled: bool,
    warmup_dialect_plugin: Option<UpstreamWarmupDialectPlugin>,
    spec_revision: u64,
    status: UpstreamStatusResponse,
}

#[derive(Serialize)]
struct UpstreamStatusResponse {
    last_apply_error: Option<String>,
    last_apply_at_unix_secs: Option<u64>,
    last_warmup_at_unix_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct DeviceState {
    device_auth_id: String,
    user_code: String,
    poll_interval_secs: u64,
    expires_at_unix_secs: u64,
    #[serde(default)]
    authorization_code: Option<String>,
    #[serde(default)]
    code_verifier: Option<String>,
    #[serde(default)]
    pending_attempts: u32,
}

#[derive(Clone, Serialize, Deserialize)]
struct InFlightPkce {
    #[serde(default)]
    provider: OAuthProvider,
    handshake: PkceHandshakeState,
    created_at_unix_secs: u64,
    initiated_by: AuditActorFields,
    target: PkceTarget,
    #[serde(default)]
    device: Option<DeviceState>,
}

#[derive(Clone, Serialize, Deserialize)]
enum PkceTarget {
    ExistingUpstream {
        upstream_id: Uuid,
        upstream_name: String,
        expected_revision: u64,
    },
    PendingDraft {
        completed: Option<Box<DraftCompletion>>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct DraftCompletion {
    encrypted_tokens: EncryptedOAuthTokens,
    subscription_metadata_record: UpstreamSubscriptionMetadataRecord,
    organization_metadata_record: Option<OrganizationMetadataRecord>,
    fetched_at_unix_secs: u64,
}

#[derive(Serialize, Deserialize)]
struct StateToken {
    upstream_id: Uuid,
    nonce: String,
}

async fn start_oauth(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path(upstream_id): Path<Uuid>,
    Json(payload): Json<StartRequest>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let upstream = match UpstreamStore::get_by_id(storage.as_ref(), upstream_id).await {
        Ok(Some(upstream)) => upstream,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "upstream_not_found" })),
            )
                .into_response();
        }
        Err(error) => return storage_error_response(&error),
    };
    if !matches!(
        upstream.kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "wrong_kind" })),
        )
            .into_response();
    }
    if upstream.kind == UpstreamKind::OpenaiOauth {
        let flow = match parse_openai_flow(payload.flow.as_deref()) {
            Ok(flow) => flow,
            Err(()) => return unsupported_flow_response(),
        };
        let storage = Arc::clone(storage);
        return match complete_after_commit(async move {
            start_provider_oauth(
                &state,
                storage.as_ref(),
                &identity,
                upstream,
                upstream_id,
                Some(flow),
            )
            .await
        })
        .await
        {
            Ok(response) => response,
            Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
    }
    if payload
        .flow
        .as_deref()
        .is_some_and(|flow| !(upstream.kind == UpstreamKind::GeminiOauth && flow == "browser"))
    {
        return unsupported_flow_response();
    }
    if upstream.kind == UpstreamKind::GeminiOauth {
        let storage = Arc::clone(storage);
        return match complete_after_commit(async move {
            start_provider_oauth(
                &state,
                storage.as_ref(),
                &identity,
                upstream,
                upstream_id,
                None,
            )
            .await
        })
        .await
        {
            Ok(response) => response,
            Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
    }
    let upstream_revision = upstream.revision;

    let oauth = state
        .config
        .oauth
        .anthropic
        .clone()
        .unwrap_or_else(AnthropicOAuthConfig::default);
    let authorize_endpoint = match AuthUrl::new(oauth.auth_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let token_endpoint = match TokenUrl::new(oauth.token_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let scopes = oauth.scopes.clone();
    let handshake = start_pkce_flow(
        ClientId::new(oauth.client_id.clone()),
        authorize_endpoint,
        token_endpoint,
        scopes,
        oauth.redirect_uri.clone(),
    );
    let mut handshake_state = handshake.into_state();
    let state_token = match encode_state(upstream_id) {
        Ok(state_token) => state_token,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    handshake_state
        .authorize_url
        .query_pairs_mut()
        .append_pair("state", &state_token);

    let now = cc_lb_clock::unix_secs(state.clock.now());
    let in_flight = InFlightPkce {
        provider: OAuthProvider::Anthropic,
        handshake: handshake_state.clone(),
        created_at_unix_secs: now,
        initiated_by: identity.audit_fields(),
        target: PkceTarget::ExistingUpstream {
            upstream_id,
            upstream_name: upstream.name.clone(),
            expected_revision: upstream.revision,
        },
        device: None,
    };
    if let Err(response) =
        persist_pkce_flow(&state, storage.as_ref(), &state_token, &in_flight).await
    {
        return response;
    }

    let target_upstream = upstream.name.clone();
    let audit_action = AuditPayload::UpstreamOauthStart {
        upstream_id: upstream_id.to_string(),
        upstream_name: upstream.name,
    }
    .to_string();
    let route = format!("/admin/v1/upstreams/{upstream_id}/oauth/start");
    let authorize_url = handshake_state.authorize_url.to_string();
    match complete_after_commit(async move {
        if let Err(error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action: &audit_action,
                route: &route,
                target_principal_id: None,
                target_upstream: Some(&target_upstream),
                api_key_id: None,
                status: StatusCode::OK.as_u16(),
                payload: None,
            },
        )
        .await
        {
            tracing::error!(
                error = %error,
                action = %audit_action,
                "admin audit write failed"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "audit_write_failed" })),
            )
                .into_response();
        }

        Json(StartResponse {
            authorize_url: Some(authorize_url),
            state_token,
            revision: upstream_revision,
            provider: OAuthProvider::Anthropic,
            flow: "paste_code",
            verification_url: None,
            user_code: None,
            poll_interval_secs: None,
            expires_at_unix_secs: None,
        })
        .into_response()
    })
    .await
    {
        Ok(response) => response,
        Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn start_oauth_draft(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(payload): Json<StartRequest>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    if payload.provider == OAuthProvider::Openai {
        let flow = match parse_openai_flow(payload.flow.as_deref()) {
            Ok(flow) => flow,
            Err(()) => return unsupported_flow_response(),
        };
        let config = state.config.oauth.openai.clone().unwrap_or_default();
        if flow == OpenAiFlow::Browser {
            let auth_url = match AuthUrl::new(config.authorize_url.to_string()) {
                Ok(value) => value,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let token_url = match TokenUrl::new(config.token_url.to_string()) {
                Ok(value) => value,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let state_token = match encode_state(Uuid::nil()) {
                Ok(value) => value,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };
            let mut handshake = start_pkce_flow_without_code(
                ClientId::new(config.client_id.clone()),
                auth_url,
                token_url,
                vec![
                    "openid".to_owned(),
                    "profile".to_owned(),
                    "email".to_owned(),
                    "offline_access".to_owned(),
                ],
                config.browser_redirect_uri.clone(),
            )
            .into_state();
            // Keep these OpenAI Codex CLI parameters in sync with
            // `build_authorize_url` in github.com/openai/codex:
            // https://github.com/openai/codex/blob/main/codex-rs/login/src/server.rs#L580-L625
            handshake
                .authorize_url
                .query_pairs_mut()
                .append_pair("id_token_add_organizations", "true")
                .append_pair("codex_cli_simplified_flow", "true")
                .append_pair("state", &state_token);
            let now = cc_lb_clock::unix_secs(state.clock.now());
            let in_flight = InFlightPkce {
                provider: OAuthProvider::Openai,
                handshake: handshake.clone(),
                created_at_unix_secs: now,
                initiated_by: identity.audit_fields(),
                target: PkceTarget::PendingDraft { completed: None },
                device: None,
            };
            if let Err(response) =
                persist_pkce_flow(&state, storage.as_ref(), &state_token, &in_flight).await
            {
                return response;
            }
            return Json(DraftStartResponse {
                authorize_url: Some(handshake.authorize_url.to_string()),
                state_token,
                provider: OAuthProvider::Openai,
                flow: "paste_code",
                verification_url: None,
                user_code: None,
                poll_interval_secs: None,
                expires_at_unix_secs: None,
            })
            .into_response();
        }
        let device =
            match request_openai_device_code(&config, cc_lb_clock::unix_secs(state.clock.now()))
                .await
            {
                Ok(device) => device,
                Err(error) => {
                    return (
                        if error == "device_login_unavailable" {
                            StatusCode::NOT_FOUND
                        } else {
                            StatusCode::BAD_GATEWAY
                        },
                        Json(json!({ "error": error })),
                    )
                        .into_response();
                }
            };
        let auth_url = match AuthUrl::new(config.auth_url.to_string()) {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let token_url = match TokenUrl::new(config.token_url.to_string()) {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let handshake_state = start_pkce_flow_without_code(
            ClientId::new(config.client_id.clone()),
            auth_url,
            token_url,
            Vec::new(),
            config.device_redirect_uri,
        )
        .into_state();
        let state_token = match encode_state(Uuid::nil()) {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let now = cc_lb_clock::unix_secs(state.clock.now());
        let in_flight = InFlightPkce {
            provider: OAuthProvider::Openai,
            handshake: handshake_state,
            created_at_unix_secs: now,
            initiated_by: identity.audit_fields(),
            target: PkceTarget::PendingDraft { completed: None },
            device: Some(device.clone()),
        };
        if let Err(response) =
            persist_pkce_flow(&state, storage.as_ref(), &state_token, &in_flight).await
        {
            return response;
        }
        return Json(DraftStartResponse {
            authorize_url: None,
            state_token,
            provider: OAuthProvider::Openai,
            flow: "device_code",
            verification_url: Some(config.device_verification_url.to_string()),
            user_code: Some(device.user_code),
            poll_interval_secs: Some(device.poll_interval_secs),
            expires_at_unix_secs: Some(device.expires_at_unix_secs),
        })
        .into_response();
    }
    if payload
        .flow
        .as_deref()
        .is_some_and(|flow| payload.provider != OAuthProvider::Gemini || flow != "browser")
    {
        return unsupported_flow_response();
    }
    if payload.provider == OAuthProvider::Gemini {
        let Some(config) = state.config.oauth.antigravity.clone() else {
            return provider_flow_error(
                OAuthProvider::Gemini,
                "oauth_client_not_configured",
                StatusCode::BAD_REQUEST,
            );
        };
        if config.missing_client_credentials().is_some() {
            return provider_flow_error(
                OAuthProvider::Gemini,
                "oauth_client_not_configured",
                StatusCode::BAD_REQUEST,
            );
        }
        let auth_url = match AuthUrl::new(config.authorize_url.to_string()) {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let token_url = match TokenUrl::new(config.token_url.to_string()) {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        let scopes = config.scopes.clone();
        let mut handshake = start_pkce_flow_without_code(
            ClientId::new(config.client_id.clone()),
            auth_url,
            token_url,
            scopes,
            config.redirect_uri.clone(),
        )
        .into_state();
        handshake
            .authorize_url
            .query_pairs_mut()
            .append_pair("access_type", "offline")
            .append_pair("prompt", "consent");
        let state_token = match encode_state(Uuid::nil()) {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        handshake
            .authorize_url
            .query_pairs_mut()
            .append_pair("state", &state_token);
        let now = cc_lb_clock::unix_secs(state.clock.now());
        let in_flight = InFlightPkce {
            provider: OAuthProvider::Gemini,
            handshake: handshake.clone(),
            created_at_unix_secs: now,
            initiated_by: identity.audit_fields(),
            target: PkceTarget::PendingDraft { completed: None },
            device: None,
        };
        if let Err(response) =
            persist_pkce_flow(&state, storage.as_ref(), &state_token, &in_flight).await
        {
            return response;
        }
        return Json(DraftStartResponse {
            authorize_url: Some(handshake.authorize_url.to_string()),
            state_token,
            provider: OAuthProvider::Gemini,
            flow: "browser",
            verification_url: None,
            user_code: None,
            poll_interval_secs: None,
            expires_at_unix_secs: None,
        })
        .into_response();
    }
    let oauth = state
        .config
        .oauth
        .anthropic
        .clone()
        .unwrap_or_else(AnthropicOAuthConfig::default);
    let authorize_endpoint = match AuthUrl::new(oauth.auth_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let token_endpoint = match TokenUrl::new(oauth.token_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let scopes = oauth.scopes.clone();
    let handshake = start_pkce_flow(
        ClientId::new(oauth.client_id.clone()),
        authorize_endpoint,
        token_endpoint,
        scopes,
        oauth.redirect_uri.clone(),
    );
    let mut handshake_state = handshake.into_state();
    let state_token = match encode_state(Uuid::nil()) {
        Ok(state_token) => state_token,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    handshake_state
        .authorize_url
        .query_pairs_mut()
        .append_pair("state", &state_token);

    let now = cc_lb_clock::unix_secs(state.clock.now());
    let in_flight = InFlightPkce {
        provider: OAuthProvider::Anthropic,
        handshake: handshake_state.clone(),
        created_at_unix_secs: now,
        initiated_by: identity.audit_fields(),
        target: PkceTarget::PendingDraft { completed: None },
        device: None,
    };
    if let Err(response) =
        persist_pkce_flow(&state, storage.as_ref(), &state_token, &in_flight).await
    {
        return response;
    }

    let authorize_url = handshake_state.authorize_url.to_string();
    match complete_after_commit(async move {
        if let Err(error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action: "upstream_oauth_draft_start",
                route: "/admin/v1/oauth/draft/start",
                target_principal_id: None,
                target_upstream: None,
                api_key_id: None,
                status: StatusCode::OK.as_u16(),
                payload: None,
            },
        )
        .await
        {
            tracing::error!(
                error = %error,
                action = "upstream_oauth_draft_start",
                "admin audit write failed"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "audit_write_failed" })),
            )
                .into_response();
        }

        Json(DraftStartResponse {
            authorize_url: Some(authorize_url),
            state_token,
            provider: OAuthProvider::Anthropic,
            flow: "paste_code",
            verification_url: None,
            user_code: None,
            poll_interval_secs: None,
            expires_at_unix_secs: None,
        })
        .into_response()
    })
    .await
    {
        Ok(response) => response,
        Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn complete_oauth_draft(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(payload): Json<CompleteRequest>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let in_flight = match load_pkce_flow(&state, storage.as_ref(), &payload.state_token).await {
        Ok(in_flight) => in_flight,
        Err(response) => return response,
    };
    let Some(in_flight) = in_flight else {
        return invalid_state_response(
            "state token expired or already used — restart the OAuth flow",
        );
    };
    match &in_flight.target {
        PkceTarget::PendingDraft { completed: None } => {}
        PkceTarget::PendingDraft { completed: Some(_) } => {
            return invalid_state_response("OAuth draft has already been completed");
        }
        PkceTarget::ExistingUpstream { .. } => {
            return invalid_state_response("state token is for an existing upstream");
        }
    }
    let initiated_by = in_flight.initiated_by.actor.clone();

    let provider = in_flight.provider;
    let storage = Arc::clone(storage);
    let response = async {
        let (
            credentials,
            account_identity,
            mode,
            long_lived_fallback,
            fallback_reason,
            granted_expires_in_secs,
        ) = if provider == OAuthProvider::Openai {
            let config = state.config.oauth.openai.clone().unwrap_or_default();
            let (handshake, authorization_code, redirect_uri) =
                if let Some(device) = in_flight.device.clone() {
                    if cc_lb_clock::unix_secs(state.clock.now()) >= device.expires_at_unix_secs {
                        return provider_flow_error(
                            provider,
                            "device_code_expired",
                            StatusCode::BAD_REQUEST,
                        );
                    }
                    let poll = match poll_openai_device_code(&config, &device).await {
                        Ok(poll) => poll,
                        Err(error) => return oauth_provider_error_response(provider, error),
                    };
                    let (authorization_code, code_verifier) = match poll {
                        OpenAiDevicePoll::Pending => {
                            let mut pending = in_flight;
                            if let Some(device) = pending.device.as_mut() {
                                device.pending_attempts = device.pending_attempts.saturating_add(1);
                            }
                            if let Err(response) = persist_pkce_flow(
                                &state,
                                storage.as_ref(),
                                &payload.state_token,
                                &pending,
                            )
                            .await
                            {
                                return response;
                            }
                            return (
                                StatusCode::ACCEPTED,
                                Json(json!({
                                    "status": "pending",
                                    "poll_interval_secs": device.poll_interval_secs
                                })),
                            )
                                .into_response();
                        }
                        OpenAiDevicePoll::SlowDown => {
                            return provider_flow_error(
                                provider,
                                "slow_down",
                                StatusCode::TOO_MANY_REQUESTS,
                            );
                        }
                        OpenAiDevicePoll::Expired => {
                            return provider_flow_error(
                                provider,
                                "device_code_expired",
                                StatusCode::BAD_REQUEST,
                            );
                        }
                        OpenAiDevicePoll::Denied => {
                            return provider_flow_error(
                                provider,
                                "access_denied",
                                StatusCode::BAD_REQUEST,
                            );
                        }
                        OpenAiDevicePoll::Authorized {
                            authorization_code,
                            code_verifier,
                        } => (authorization_code, code_verifier),
                    };
                    (
                        in_flight.handshake.clone().with_verifier(code_verifier),
                        authorization_code,
                        config.device_redirect_uri.clone(),
                    )
                } else {
                    let Some(code) = payload.code.as_deref() else {
                        return invalid_state_response("authorization code is required");
                    };
                    let authorization_code =
                        match parse_openai_browser_code(code, &payload.state_token) {
                            Ok(code) => code,
                            Err("state_mismatch") => {
                                return provider_flow_error(
                                    provider,
                                    "state_mismatch",
                                    StatusCode::BAD_REQUEST,
                                );
                            }
                            Err(_) => {
                                return invalid_state_response("authorization code is required");
                            }
                        };
                    (
                        in_flight.handshake.clone(),
                        authorization_code,
                        config.browser_redirect_uri.clone(),
                    )
                };
            let (credentials, account_identity) = match complete_pkce_form_flow(
                handshake,
                authorization_code,
                None,
                redirect_uri,
                Arc::new(HyperOAuthHttpClient::new()),
                &*state.clock,
            )
            .await
            {
                Ok((credentials, account_identity)) => (credentials, account_identity),
                Err(error) => {
                    return oauth_provider_error_response(
                        provider,
                        ProviderCompletionError::token(error),
                    );
                }
            };
            (
                credentials,
                account_identity,
                OAuthTokenMode::Refreshing,
                false,
                None,
                None,
            )
        } else if provider == OAuthProvider::Gemini {
            let Some(config) = state.config.oauth.antigravity.clone() else {
                return provider_flow_error(
                    provider,
                    "oauth_client_not_configured",
                    StatusCode::BAD_REQUEST,
                );
            };
            if config.missing_client_credentials().is_some() {
                return provider_flow_error(
                    provider,
                    "oauth_client_not_configured",
                    StatusCode::BAD_REQUEST,
                );
            }
            let Some(input) = payload.code.as_deref() else {
                return invalid_state_response("authorization code is required");
            };
            let code = match parse_gemini_browser_code(
                input,
                &payload.state_token,
                &config.redirect_uri,
            ) {
                Ok(code) => code,
                Err("state_mismatch") => {
                    return provider_flow_error(
                        provider,
                        "state_mismatch",
                        StatusCode::BAD_REQUEST,
                    );
                }
                Err(_) => return invalid_state_response("invalid authorization callback"),
            };
            let (credentials, account_identity) = match complete_gemini_oauth(
                in_flight.handshake.clone(),
                code,
                &config,
                &*state.clock,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => return oauth_provider_error_response(provider, error),
            };
            (
                credentials,
                account_identity,
                OAuthTokenMode::Refreshing,
                false,
                None,
                None,
            )
        } else {
            let handshake = match in_flight.handshake.clone().into_handshake() {
                Ok(handshake) => handshake,
                Err(_) => {
                    return invalid_state_response(
                        "stored PKCE handshake is corrupted — restart the OAuth flow",
                    );
                }
            };
            let Some(code) = payload.code.as_deref() else {
                return invalid_state_response("authorization code is required");
            };
            let code = normalize_oauth_code(code);
            match complete_pkce_flow(
                handshake,
                code,
                payload.state_token.clone(),
                Arc::new(HyperOAuthHttpClient::new()),
                &*state.clock,
            )
            .await
            {
                Ok(PkceExchangeOutcome {
                    credentials,
                    mode,
                    long_lived_fallback,
                    fallback_reason,
                    granted_expires_in_secs,
                }) => (
                    credentials,
                    OAuthAccountIdentity::default(),
                    mode,
                    long_lived_fallback,
                    fallback_reason,
                    granted_expires_in_secs,
                ),
                Err(error) => return token_exchange_error_response(error),
            }
        };
        if fallback_reason == Some(LongLivedFallbackReason::Clamped) {
            tracing::warn!(
                initiated_by = %initiated_by,
                ?granted_expires_in_secs,
                "oauth draft complete: provider granted a shorter token lifetime than \
                 requested; storing as a refreshing credential"
            );
        }

        // The refresh token is stored even for long-lived credentials: it may be
        // useful later, but it must never be used to refresh (that would revoke
        // the 365-day access token). `mode.never_refresh()` is the single source
        // of truth — a clamped grant is already demoted to refreshing mode by the
        // exchange, so it is stored refreshable without a second check here.
        let bundle = OAuthTokenBundle {
            access_token: credentials.access_token,
            refresh_token: credentials.refresh_token,
            // Always the lifetime the token endpoint granted; the requested
            // lifetime is never substituted.
            expires_at_unix_secs: credentials.expires_at,
            refresh_token_expires_at_unix_secs: credentials.refresh_token_expires_at_unix_secs,
            account_id: account_identity
                .account_id
                .clone()
                .or(account_identity.account_email.clone()),
            project_id: account_identity.project_id.clone(),
            oauth_profile: if provider == OAuthProvider::Gemini {
                cc_lb_aead::GeminiOAuthProfile::Antigravity
            } else {
                cc_lb_aead::GeminiOAuthProfile::default()
            },
            scopes: credentials.scopes,
            never_refresh: mode.never_refresh(),
        };
        let encrypted_tokens = match AeadEncryptedField::<OAuthTokenBundle>::encrypt(
            &state.aead,
            &bundle,
            Uuid::nil().as_bytes(),
        ) {
            Ok(encrypted) => encrypted,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

        let version = storage
            .get_compatibility_kv(CLAUDE_CODE_STABLE_VERSION_KEY)
            .await
            .map(|record| record.map(|record| record.value))
            .unwrap_or(None)
            .unwrap_or_else(|| CLAUDE_CODE_STABLE_VERSION_FALLBACK.to_owned());
        let user_agent = claude_code_user_agent(&version);
        let client = make_metadata_http_client();
        let cancel = CancellationToken::new();
        let records = if provider == OAuthProvider::Anthropic {
            match tokio::time::timeout(
                METADATA_REFRESH_TIMEOUT,
                fetch_metadata_only(
                    &client,
                    Uuid::nil(),
                    &bundle.access_token,
                    &user_agent,
                    &cancel,
                    &*state.clock,
                ),
            )
            .await
            {
                Ok(records) => records,
                Err(_) => {
                    return (
                        StatusCode::GATEWAY_TIMEOUT,
                        Json(json!({ "error": "metadata_refresh_timeout" })),
                    )
                        .into_response();
                }
            }
        } else {
            provider_metadata_records(
                Uuid::nil(),
                &account_identity,
                cc_lb_clock::unix_millis(state.clock.now()),
            )
        };
        let suggested_name = account_identity
            .account_email
            .as_deref()
            .and_then(email_local_part)
            .map(str::to_owned)
            .unwrap_or_else(|| {
                records
                    .organization_metadata_record
                    .as_ref()
                    .map(|record| suggest_name(record, record.account_email.as_deref()))
                    .unwrap_or_else(|| "OAuth Upstream".to_owned())
            });
        let completion = DraftCompletion {
            encrypted_tokens,
            subscription_metadata_record: records.subscription_metadata_record.clone(),
            organization_metadata_record: records.organization_metadata_record.clone(),
            fetched_at_unix_secs: cc_lb_clock::unix_secs(state.clock.now()),
        };

        let mut in_flight = in_flight;
        match &mut in_flight.target {
            PkceTarget::PendingDraft { completed } if completed.is_none() => {
                *completed = Some(Box::new(completion));
            }
            PkceTarget::PendingDraft { .. } => {
                return invalid_state_response("OAuth draft has already been completed");
            }
            PkceTarget::ExistingUpstream { .. } => {
                return invalid_state_response("state token is for an existing upstream");
            }
        }
        if let Err(response) =
            persist_pkce_flow(&state, storage.as_ref(), &payload.state_token, &in_flight).await
        {
            return response;
        }

        let state_token = payload.state_token;
        match complete_after_commit(async move {
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action: "upstream_oauth_draft_complete",
                    route: "/admin/v1/oauth/draft/complete",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: Some(json!({ "initiated_by": initiated_by })),
                },
            )
            .await
            {
                tracing::error!(
                    error = %error,
                    action = "upstream_oauth_draft_complete",
                    "admin audit write failed"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "audit_write_failed" })),
                )
                    .into_response();
            }

            Json(DraftCompleteResponse {
                state_token,
                suggested_name,
                subscription_metadata: Some(records.subscription_metadata_record),
                organization_metadata: records.organization_metadata_record,
                mode,
                long_lived_fallback,
                fallback_reason,
                granted_expires_in_secs,
                provider: in_flight.provider,
                account_identity,
            })
            .into_response()
        })
        .await
        {
            Ok(response) => response,
            Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
    .await;
    finish_provider_completion(provider, response)
}

async fn create_upstream_from_oauth_draft(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(payload): Json<CreateFromDraftRequest>,
) -> Response {
    if let Err(error) = validate_identifier("upstream.name", &payload.name) {
        let (field, reason) = match error {
            StorageError::InvalidInput { field, reason } => (field, reason),
            other => ("upstream.name".to_owned(), other.to_string()),
        };
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_input", "field": field, "reason": reason })),
        )
            .into_response();
    }
    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let in_flight = match load_pkce_flow(&state, storage.as_ref(), &payload.state_token).await {
        Ok(in_flight) => in_flight,
        Err(response) => return response,
    };
    let draft = in_flight.and_then(|flow| match flow.target {
        PkceTarget::PendingDraft {
            completed: Some(completion),
        } => Some((flow.provider, *completion, flow.initiated_by.actor)),
        _ => None,
    });
    let Some((provider, completion, initiated_by)) = draft else {
        return invalid_state_response("OAuth draft is missing or incomplete");
    };
    let name = payload.name.clone();
    let base_url = match payload.base_url {
        Some(base_url) => Some(base_url),
        None => {
            let value = match provider {
                OAuthProvider::Anthropic => DEFAULT_OAUTH_UPSTREAM_BASE_URL,
                OAuthProvider::Openai => "https://chatgpt.com/backend-api/codex",
                OAuthProvider::Gemini => "https://daily-cloudcode-pa.googleapis.com",
            };
            match Url::parse(value) {
                Ok(base_url) => Some(base_url),
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    };
    // The draft tokens are sealed with the nil AAD; rebind them to the
    // preallocated upstream id before the insert so the spec row and its
    // credential land in one storage transaction. No upstream exists yet, so a
    // crypto failure only has to discard the PKCE flow.
    let id = Uuid::new_v4();
    let bundle = match completion
        .encrypted_tokens
        .decrypt(state.aead.as_ref(), Uuid::nil().as_bytes())
    {
        Ok(bundle) => bundle,
        Err(error) => {
            return discard_oauth_draft(
                storage.as_ref(),
                &payload.state_token,
                format!("draft token decrypt failed: {error}"),
                None,
            )
            .await;
        }
    };
    let tokens = match AeadEncryptedField::<OAuthTokenBundle>::encrypt(
        state.aead.as_ref(),
        &bundle,
        id.as_bytes(),
    ) {
        Ok(encrypted) => encrypted,
        Err(error) => {
            return discard_oauth_draft(
                storage.as_ref(),
                &payload.state_token,
                format!("draft token re-encrypt failed: {error}"),
                None,
            )
            .await;
        }
    };
    let created = match UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            id,
            name: payload.name,
            kind: match provider {
                OAuthProvider::Anthropic => UpstreamKind::AnthropicOauth,
                OAuthProvider::Openai => UpstreamKind::OpenaiOauth,
                OAuthProvider::Gemini => UpstreamKind::GeminiOauth,
            },
            base_url,
            api_key_ciphertext: None,
            oauth_tokens: Some(OAuthTokensCreate {
                tokens,
                never_refresh: bundle.never_refresh,
            }),
            // All OAuth providers support scheduler-managed warmup; dialect
            // plugins remain an explicit Anthropic-only option.
            warmup_enabled: true,
            warmup_dialect_plugin: None,
            workspace_id: None,
            api_key_access: None,
        },
    )
    .await
    {
        Ok(created) => created,
        Err(error @ StorageError::Conflict { .. }) => {
            match UpstreamStore::get_by_name(storage.as_ref(), &name).await {
                Ok(Some(existing)) => {
                    return (
                        StatusCode::CONFLICT,
                        Json(json!({
                            "error": "upstream_name_conflict",
                            "name": name,
                            "existing_upstream_id": existing.id,
                            "detail": "An active upstream already uses this name."
                        })),
                    )
                        .into_response();
                }
                Ok(None) => return storage_error_response(&error),
                Err(lookup_error) => return storage_error_response(&lookup_error),
            }
        }
        Err(error) => return storage_error_response(&error),
    };
    let storage = Arc::clone(storage);
    let state_token = payload.state_token;
    match complete_after_commit(async move {
        if let Err(error) = seed_oauth_bootstrap_tasks(&state, &created).await {
            return rollback_oauth_draft_creation(
                &state,
                storage.as_ref(),
                &state_token,
                created.id,
                format!("oauth scheduler seed failed: {error}"),
            )
            .await;
        }
        let _fetched_at_unix_secs = completion.fetched_at_unix_secs;
        let mut subscription_record = completion.subscription_metadata_record;
        subscription_record.upstream_id = created.id;
        if let Err(error) = storage
            .put_upstream_subscription_metadata(&subscription_record)
            .await
        {
            return rollback_oauth_draft_creation(
                &state,
                storage.as_ref(),
                &state_token,
                created.id,
                format!("subscription metadata storage failed: {error}"),
            )
            .await;
        }
        if let Some(organization_record) = completion.organization_metadata_record.as_ref()
            && let Err(error) = storage.put_organization_metadata(organization_record).await
        {
            return rollback_oauth_draft_creation(
                &state,
                storage.as_ref(),
                &state_token,
                created.id,
                format!("organization metadata storage failed: {error}"),
            )
            .await;
        }

        if let Err(response) = delete_pkce_flow(storage.as_ref(), &state_token).await {
            // The upstream and its metadata are committed even though PKCE
            // cleanup failed; keep routing in step before returning the error.
            rebuild_after_committed_failure(&state, "oauth draft pkce cleanup").await;
            return response;
        }

        let target_upstream = created.name.clone();
        if let Err(error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action: "upstream_create_from_oauth_draft",
                route: "/admin/v1/upstreams/from-oauth-draft",
                target_principal_id: None,
                target_upstream: Some(&target_upstream),
                api_key_id: None,
                status: StatusCode::CREATED.as_u16(),
                payload: Some(json!({ "initiated_by": initiated_by })),
            },
        )
        .await
        {
            tracing::error!(
                error = %error,
                action = "upstream_create_from_oauth_draft",
                "admin audit write failed"
            );
            rebuild_after_audit_failure(&state).await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "audit_write_failed" })),
            )
                .into_response();
        }

        let mut response = (StatusCode::CREATED, Json(upstream_response(&created))).into_response();
        if let Ok(location) = HeaderValue::from_str(&format!("/admin/v1/upstreams/{}", created.id))
        {
            response.headers_mut().insert(header::LOCATION, location);
        }
        add_dynamic_rebind_headers(&mut response, &state).await;
        response
    })
    .await
    {
        Ok(response) => response,
        Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn complete_oauth(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path(upstream_id): Path<Uuid>,
    Json(payload): Json<CompleteRequest>,
) -> Response {
    let decoded = match decode_state(&payload.state_token) {
        Ok(decoded) => decoded,
        Err(_) => {
            tracing::warn!(%upstream_id, "oauth complete: state token failed base64/json decode");
            return invalid_state_response("state token could not be decoded");
        }
    };
    if decoded.upstream_id != upstream_id {
        tracing::warn!(
            %upstream_id,
            state_upstream_id = %decoded.upstream_id,
            "oauth complete: decoded state upstream_id mismatches path"
        );
        return invalid_state_response("state token does not match this upstream");
    }
    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let in_flight = match load_pkce_flow(&state, storage.as_ref(), &payload.state_token).await {
        Ok(in_flight) => in_flight,
        Err(response) => return response,
    };
    let Some(in_flight) = in_flight else {
        tracing::warn!(
            %upstream_id,
            "oauth complete: state token not in pkce flow store (expired or already consumed)"
        );
        return invalid_state_response(
            "state token expired or already used — restart the OAuth flow",
        );
    };
    let (target_upstream_id, upstream_name, expected_revision) = match &in_flight.target {
        PkceTarget::ExistingUpstream {
            upstream_id,
            upstream_name,
            expected_revision,
        } => (*upstream_id, upstream_name.clone(), *expected_revision),
        PkceTarget::PendingDraft { .. } => {
            return invalid_state_response("state token is for an OAuth draft");
        }
    };
    if target_upstream_id != upstream_id {
        return invalid_state_response("state token does not match this upstream");
    }

    let current = match UpstreamStore::get_by_id(storage.as_ref(), upstream_id).await {
        Ok(Some(upstream)) => upstream,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return storage_error_response(&error),
    };
    if matches!(
        current.kind,
        UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ) {
        let storage = Arc::clone(storage);
        return match complete_after_commit(async move {
            complete_provider_oauth(
                &state,
                storage.as_ref(),
                &identity,
                upstream_id,
                current,
                in_flight,
                payload,
            )
            .await
        })
        .await
        {
            Ok(response) => response,
            Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
    }
    if current.kind != UpstreamKind::AnthropicOauth {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "wrong_kind",
                "detail": "this upstream is not configured for Anthropic OAuth",
            })),
        )
            .into_response();
    }

    let handshake = match in_flight.handshake.into_handshake() {
        Ok(handshake) => handshake,
        Err(_) => {
            return invalid_state_response(
                "stored PKCE handshake is corrupted — restart the OAuth flow",
            );
        }
    };
    // Anthropic's callback shows `<code>#<state>`; users may also paste the full callback URL.
    let Some(code) = payload.code.as_deref() else {
        return invalid_state_response("authorization code is required");
    };
    let code = normalize_oauth_code(code);
    let PkceExchangeOutcome {
        credentials,
        mode,
        long_lived_fallback,
        fallback_reason,
        granted_expires_in_secs,
    } = match complete_pkce_flow(
        handshake,
        code,
        payload.state_token.clone(),
        Arc::new(HyperOAuthHttpClient::new()),
        &*state.clock,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::warn!(%upstream_id, %error, "oauth complete: token exchange rejected by provider");
            return token_exchange_error_response(error);
        }
    };
    if fallback_reason == Some(LongLivedFallbackReason::Clamped) {
        tracing::warn!(
            %upstream_id,
            ?granted_expires_in_secs,
            "oauth complete: provider granted a shorter token lifetime than \
             requested; storing as a refreshing credential"
        );
    }

    if let Err(response) = delete_pkce_flow(storage.as_ref(), &payload.state_token).await {
        return response;
    }

    let access_token_fingerprint = access_token_fingerprint(&credentials.access_token);
    // The refresh token is stored even for long-lived credentials: it may be
    // useful later, but it must never be used to refresh (that would revoke
    // the 365-day access token). `mode.never_refresh()` is the single source
    // of truth — a clamped grant is already demoted to refreshing mode by the
    // exchange, so it is stored refreshable without a second check here.
    let bundle = OAuthTokenBundle {
        access_token: credentials.access_token,
        refresh_token: credentials.refresh_token,
        // Always the lifetime the token endpoint granted; the requested
        // lifetime is never substituted.
        expires_at_unix_secs: credentials.expires_at,
        refresh_token_expires_at_unix_secs: credentials.refresh_token_expires_at_unix_secs,
        account_id: None,
        project_id: None,
        oauth_profile: cc_lb_aead::GeminiOAuthProfile::default(),
        scopes: credentials.scopes,
        never_refresh: mode.never_refresh(),
    };
    let encrypted = match cc_lb_aead::AeadEncryptedField::<OAuthTokenBundle>::encrypt(
        &state.aead,
        &bundle,
        upstream_id.as_bytes(),
    ) {
        Ok(encrypted) => encrypted,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let updated = match storage
        .store_oauth_tokens(
            upstream_id,
            expected_revision,
            encrypted,
            bundle.never_refresh,
        )
        .await
    {
        Ok(updated) => updated,
        Err(StorageError::Conflict { .. }) => {
            let current_revision = current_revision(storage.as_ref(), upstream_id)
                .await
                .unwrap_or(current.revision);
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "stale_revision", "current_revision": current_revision })),
            )
                .into_response();
        }
        Err(error) => return storage_error_response(&error),
    };

    match complete_after_commit(async move {
        if let Err(error) =
            seed_oauth_reconnect_tasks(&state, &updated, bundle.expires_at_unix_secs).await
        {
            // The new credential is committed even though seeding failed;
            // keep routing in step before returning the seed error.
            rebuild_after_committed_failure(&state, "oauth reconnect scheduler seeding").await;
            return scheduler_error_response(&error);
        }

        let target_upstream = upstream_name.clone();
        let audit_action = AuditPayload::UpstreamOauthComplete {
            upstream_id: upstream_id.to_string(),
            upstream_name,
            expires_at_unix_secs: bundle.expires_at_unix_secs,
            access_token_fingerprint: access_token_fingerprint.clone(),
        }
        .to_string();
        let route = format!("/admin/v1/upstreams/{upstream_id}/oauth/complete");
        if let Err(error) = record_admin_audit(
            &state,
            AdminAuditEvent {
                identity: Some(&identity),
                system_component: None,
                action: &audit_action,
                route: &route,
                target_principal_id: None,
                target_upstream: Some(&target_upstream),
                api_key_id: None,
                status: StatusCode::OK.as_u16(),
                payload: None,
            },
        )
        .await
        {
            tracing::error!(
                error = %error,
                action = %audit_action,
                "admin audit write failed"
            );
            rebuild_after_audit_failure(&state).await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "audit_write_failed" })),
            )
                .into_response();
        }

        let mut response = Json(CompleteResponse {
            upstream_id: updated.id,
            expires_at_unix_secs: bundle.expires_at_unix_secs,
            access_token_fingerprint,
            mode,
            long_lived_fallback,
            fallback_reason,
            granted_expires_in_secs,
        })
        .into_response();
        add_dynamic_rebind_headers(&mut response, &state).await;
        response
    })
    .await
    {
        Ok(response) => response,
        Err(PostCommitAborted) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn get_oauth_status(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let upstream = match UpstreamStore::get_by_id(storage.as_ref(), upstream_id).await {
        Ok(Some(upstream)) => upstream,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return storage_error_response(&error),
    };

    if !matches!(
        upstream.kind,
        UpstreamKind::AnthropicOauth | UpstreamKind::OpenaiOauth | UpstreamKind::GeminiOauth
    ) {
        return Json(UpstreamOAuthStatusResponse {
            upstream_id: upstream.id.to_string(),
            kind: upstream.kind,
            has_credentials: false,
            status: "wrong_kind",
            expires_at_unix_secs: None,
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: None,
            scopes: Vec::new(),
            mode: None,
            can_refresh: false,
            account_identity: OAuthAccountIdentity::default(),
        })
        .into_response();
    }

    let Some(encrypted) = upstream.oauth_credentials.as_ref() else {
        return Json(UpstreamOAuthStatusResponse {
            upstream_id: upstream.id.to_string(),
            kind: upstream.kind,
            has_credentials: false,
            status: "missing",
            expires_at_unix_secs: None,
            refresh_token_present: false,
            refresh_token_expires_at_unix_secs: None,
            scopes: Vec::new(),
            mode: None,
            can_refresh: false,
            account_identity: OAuthAccountIdentity::default(),
        })
        .into_response();
    };

    match encrypted.decrypt(state.aead.as_ref(), upstream.id.as_bytes()) {
        Ok(bundle) => {
            let account_identity =
                provider_account_identity(storage.as_ref(), &upstream, &bundle).await;
            let now = cc_lb_clock::unix_secs(state.clock.now());
            let status = if bundle.expires_at_unix_secs <= now {
                "expired"
            } else {
                "active"
            };
            Json(UpstreamOAuthStatusResponse {
                upstream_id: upstream.id.to_string(),
                kind: upstream.kind,
                has_credentials: true,
                status,
                expires_at_unix_secs: Some(bundle.expires_at_unix_secs),
                refresh_token_present: !bundle.refresh_token.is_empty(),
                // The 30-day refresh-token clock is meaningless for a
                // long-lived credential and must not drive a UI warning.
                refresh_token_expires_at_unix_secs: if bundle.never_refresh {
                    None
                } else {
                    bundle.refresh_token_expires_at_unix_secs
                },
                scopes: bundle.scopes,
                mode: Some(OAuthTokenMode::from_never_refresh(bundle.never_refresh)),
                can_refresh: !bundle.never_refresh,
                account_identity,
            })
            .into_response()
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                %upstream_id,
                "upstream oauth credential decrypt failed"
            );
            Json(UpstreamOAuthStatusResponse {
                upstream_id: upstream.id.to_string(),
                kind: upstream.kind,
                has_credentials: true,
                status: "corrupted",
                expires_at_unix_secs: None,
                refresh_token_present: false,
                refresh_token_expires_at_unix_secs: None,
                scopes: Vec::new(),
                mode: None,
                can_refresh: false,
                account_identity: OAuthAccountIdentity::default(),
            })
            .into_response()
        }
    }
}

fn provider_metadata_records(
    upstream_id: Uuid,
    account_identity: &OAuthAccountIdentity,
    observed_at_unix_millis: u128,
) -> MetadataRefreshRecords {
    MetadataRefreshRecords {
        subscription_metadata_record: UpstreamSubscriptionMetadataRecord {
            upstream_id,
            organization_uuid: None,
            organization_role: None,
            workspace_role: None,
            observed_at_unix_millis: i64::try_from(observed_at_unix_millis).unwrap_or(i64::MAX),
            last_error: None,
            raw_roles: None,
            // Retain only display metadata, never the original ID token or provider response.
            raw_bootstrap: Some(json!({ "oauth_account": account_identity }).to_string()),
        },
        organization_metadata_record: None,
    }
}

pub(super) async fn provider_account_identity(
    storage: &dyn Storage,
    upstream: &UpstreamRecord,
    bundle: &OAuthTokenBundle,
) -> OAuthAccountIdentity {
    let metadata = storage
        .get_upstream_subscription_metadata(upstream.id)
        .await
        .ok()
        .flatten();
    let (mut identity, metadata_generation) = oauth_account_from_bootstrap(
        metadata
            .as_ref()
            .and_then(|record| record.raw_bootstrap.as_deref()),
    );
    match upstream.kind {
        UpstreamKind::OpenaiOauth => {
            let claims = openai_identity_from_token(&bundle.access_token);
            let account_id = bundle
                .account_id
                .clone()
                .or_else(|| claims.account_id.clone());
            if account_id.is_none() || identity.account_id != account_id {
                identity = OAuthAccountIdentity::default();
            }
            if claims.account_id == account_id {
                identity.account_email = identity.account_email.or(claims.account_email);
                identity.account_name = identity.account_name.or(claims.account_name);
                identity.plan = identity.plan.or(claims.plan);
            }
            identity.account_id = account_id;
        }
        UpstreamKind::GeminiOauth => {
            if !gemini_identity_is_bound(
                &identity,
                bundle.account_id.as_deref(),
                bundle.project_id.as_deref(),
                metadata_generation,
                upstream.oauth_token_generation,
            ) {
                identity = OAuthAccountIdentity::default();
            }
            identity.account_email = identity.account_email.or_else(|| {
                bundle
                    .account_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|email| !email.is_empty())
                    .map(str::to_owned)
            });
            identity.project_id = bundle.project_id.clone();
        }
        _ => {}
    }
    identity
}

#[allow(clippy::result_large_err)]
async fn persist_pkce_flow(
    state: &AdminState,
    storage: &dyn Storage,
    state_token: &str,
    in_flight: &InFlightPkce,
) -> Result<(), Response> {
    let encrypted =
        AeadEncryptedField::<InFlightPkce>::encrypt(&state.aead, in_flight, state_token.as_bytes())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
    let flow = StoredOAuthPkceFlow {
        state_token: state_token.to_owned(),
        encrypted_payload: encrypted.ciphertext().to_vec(),
        created_at_unix_secs: in_flight.created_at_unix_secs,
        expires_at_unix_secs: in_flight
            .created_at_unix_secs
            .saturating_add(PKCE_FLOW_TTL_SECS),
    };
    storage
        .put_pkce_flow(&flow)
        .await
        .map_err(|error| storage_error_response(&error))
}

#[allow(clippy::result_large_err)]
async fn load_pkce_flow(
    state: &AdminState,
    storage: &dyn Storage,
    state_token: &str,
) -> Result<Option<InFlightPkce>, Response> {
    let now = cc_lb_clock::unix_secs(state.clock.now());
    let Some(flow) = storage
        .get_pkce_flow(state_token, now)
        .await
        .map_err(|error| storage_error_response(&error))?
    else {
        return Ok(None);
    };
    let in_flight = AeadEncryptedField::<InFlightPkce>::from_ciphertext(flow.encrypted_payload)
        .decrypt(&state.aead, state_token.as_bytes())
        .map_err(|_| {
            invalid_state_response("stored PKCE handshake is corrupted — restart the OAuth flow")
        })?;
    Ok(Some(in_flight))
}

#[allow(clippy::result_large_err)]
async fn delete_pkce_flow(storage: &dyn Storage, state_token: &str) -> Result<(), Response> {
    storage
        .delete_pkce_flow(state_token)
        .await
        .map_err(|error| storage_error_response(&error))
}

fn encode_state(upstream_id: Uuid) -> Result<String, serde_json::Error> {
    let token = StateToken {
        upstream_id,
        nonce: Uuid::new_v4().to_string(),
    };
    serde_json::to_vec(&token).map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_state(value: &str) -> Result<StateToken, ()> {
    let bytes = URL_SAFE_NO_PAD.decode(value).map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}

fn invalid_state_response(detail: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "invalid_state", "detail": detail })),
    )
        .into_response()
}

fn token_exchange_error_response(error: OAuthTokenError) -> Response {
    match error {
        OAuthTokenError::TokenEndpoint {
            status,
            code,
            description,
        } => {
            // Pass through Anthropic's structured OAuth2 error code when present,
            // otherwise default to `invalid_grant` (the most common cause: expired
            // or already-used authorization code, PKCE mismatch).
            let error_code = code.clone().unwrap_or_else(|| "invalid_grant".to_owned());
            let detail = match description.as_deref() {
                Some(desc) => {
                    format!("Anthropic rejected the authorization code ({status}): {desc}")
                }
                None => match code.as_deref() {
                    Some(c) => {
                        format!("Anthropic rejected the authorization code ({status}, {c})")
                    }
                    None => format!("Anthropic rejected the authorization code (HTTP {status})"),
                },
            };
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": error_code,
                    "detail": detail,
                    "provider_status": status.as_u16(),
                })),
            )
                .into_response()
        }
        OAuthTokenError::Http { reason } => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": "token_endpoint_unreachable",
                "detail": format!("Failed to reach Anthropic token endpoint: {reason}"),
            })),
        )
            .into_response(),
        OAuthTokenError::Json { reason } => (
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": "invalid_token_response",
                "detail": format!("Anthropic returned an unparseable token response: {reason}"),
            })),
        )
            .into_response(),
    }
}

fn storage_error_response(error: &StorageError) -> Response {
    match error {
        StorageError::Unavailable { .. } | StorageError::Transient { .. } => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        StorageError::InvalidInput { field, reason } => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_input", "field": field, "detail": reason })),
        )
            .into_response(),
        StorageError::Conflict { .. } => {
            (StatusCode::CONFLICT, Json(json!({ "error": "conflict" }))).into_response()
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn scheduler_error_response(error: &SchedulerError) -> Response {
    tracing::error!(%error, "admin oauth scheduler seed failed");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

async fn seed_oauth_reconnect_tasks(
    state: &AdminState,
    upstream: &UpstreamRecord,
    expires_at_unix_secs: u64,
) -> Result<(), SchedulerError> {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return Ok(());
    };
    let seed_secs = cc_lb_clock::unix_secs(state.clock.now());
    seed_oauth_usage_poll(state, upstream, seed_secs).await?;
    // Reconnect must not immediately refresh the token just exchanged. Seed
    // the next refresh at the same cadence used by the refresh worker. The
    // generation suffix keeps a stale task for an older credential from
    // suppressing this replacement task.
    if !upstream.oauth_never_refresh {
        let task = oauth_reconnect_refresh_task(
            upstream.id,
            expires_at_unix_secs,
            upstream.oauth_token_generation,
        );
        match scheduler.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
    }
    if !WarmupLane::lanes_for_kind(upstream.kind).is_empty()
        && upstream.warmup_enabled
        && upstream.oauth_credentials.is_some()
        && upstream.deleted_at_unix_secs.is_none()
    {
        super::upstreams::push_warmup_bootstrap_tasks(state, upstream, seed_secs).await?;
    }
    Ok(())
}

fn oauth_reconnect_refresh_task(
    upstream_id: Uuid,
    expires_at_unix_secs: u64,
    generation: u64,
) -> SchedulerPushTask<AdaptiveJob> {
    let job = OAuthRefreshJob::new(upstream_id);
    let idempotency_key = format!(
        "{}:generation:{generation}",
        job.idempotency_key(expires_at_unix_secs)
    );
    SchedulerPushTask {
        args: AdaptiveJob::OAuthRefresh(job),
        idempotency_key: Some(idempotency_key),
        run_at_unix_secs: Some(OAuthRefreshJob::run_at_for_expires_at(expires_at_unix_secs)),
        max_attempts: None,
    }
}

async fn seed_oauth_usage_poll(
    state: &AdminState,
    upstream: &UpstreamRecord,
    seed_secs: u64,
) -> Result<(), SchedulerError> {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return Ok(());
    };
    let task = SchedulerPushTask {
        args: CronJob::OAuthUsagePoll(OAuthUsagePollCronJob::new(seed_secs)),
        idempotency_key: Some(format!(
            "oauth_usage_poll:connect:{}:{}",
            upstream.id, upstream.oauth_token_generation
        )),
        run_at_unix_secs: Some(seed_secs),
        max_attempts: None,
    };
    match scheduler.push_cron_task(task).await {
        Ok(()) | Err(SchedulerError::Conflict(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) async fn seed_oauth_bootstrap_tasks(
    state: &AdminState,
    upstream: &UpstreamRecord,
) -> Result<(), SchedulerError> {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return Ok(());
    };
    let seed_secs = cc_lb_clock::unix_secs(state.clock.now());
    seed_oauth_usage_poll(state, upstream, seed_secs).await?;
    // A long-lived (365-day) credential must never be refreshed: scheduling a
    // refresh would revoke the 365-day access token within seconds. Only the
    // OAuth refresh bootstrap is skipped — a long-lived upstream is otherwise
    // fully managed and still receives its warmup bootstrap below.
    if !upstream.oauth_never_refresh {
        for task in oauth_bootstrap_tasks(upstream.id, seed_secs) {
            match scheduler.push_adaptive_task(task).await {
                Ok(()) | Err(SchedulerError::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
        }
    }
    if !WarmupLane::lanes_for_kind(upstream.kind).is_empty()
        && upstream.warmup_enabled
        && upstream.oauth_credentials.is_some()
        && upstream.deleted_at_unix_secs.is_none()
    {
        super::upstreams::push_warmup_bootstrap_tasks(state, upstream, seed_secs).await?;
    }
    Ok(())
}

fn oauth_bootstrap_tasks(upstream_id: Uuid, seed_secs: u64) -> [SchedulerPushTask<AdaptiveJob>; 1] {
    [SchedulerPushTask {
        args: AdaptiveJob::OAuthRefresh(OAuthRefreshJob::new(upstream_id)),
        idempotency_key: Some(format!(
            "adaptive:oauth_refresh:{upstream_id}:bootstrap:{seed_secs}"
        )),
        run_at_unix_secs: Some(seed_secs),
        max_attempts: None,
    }]
}

/// Hard-deletes an upstream that was already created atomically with its
/// OAuth credential when a later onboarding step (bootstrap task seeding or
/// subscription/organization metadata storage) fails, then discards the PKCE
/// draft. Crypto failures happen before the create and never reach this.
/// The serving view is rebuilt after compensation in both outcomes: a failed
/// delete leaves the upstream committed, and a successful delete must not
/// leave a view that another rebuild published meanwhile still serving it.
async fn rollback_oauth_draft_creation(
    state: &AdminState,
    storage: &dyn Storage,
    state_token: &str,
    upstream_id: Uuid,
    detail: String,
) -> Response {
    let rollback_error = UpstreamStore::hard_delete(storage, upstream_id).await.err();
    let response = discard_oauth_draft(storage, state_token, detail, rollback_error).await;
    rebuild_after_committed_failure(state, "oauth draft rollback").await;
    response
}

/// Rebuilds the serving replica's dynamic view from the committed state left
/// after a post-commit step failed (and any compensation ran). The caller
/// still returns that step's original error response unchanged.
pub(super) async fn rebuild_after_committed_failure(state: &AdminState, step: &'static str) {
    if let Err(error) = super::apply_dynamic_view_after_mutation(state).await {
        tracing::error!(
            error = %error,
            step,
            "admin v1 dynamic view rebind deferred after oauth post-commit failure"
        );
    }
}

/// Deletes the PKCE draft flow and returns the 500 `oauth_draft_create_failed`
/// response, folding any cleanup or rollback failure into `detail`.
async fn discard_oauth_draft(
    storage: &dyn Storage,
    state_token: &str,
    detail: String,
    rollback_error: Option<StorageError>,
) -> Response {
    let pkce_delete_error = storage.delete_pkce_flow(state_token).await.err();
    let detail = match pkce_delete_error {
        Some(error) => format!("{detail}; pkce flow cleanup failed: {error}"),
        None => detail,
    };
    let detail = match rollback_error {
        Some(error) => format!("{detail}; rollback delete failed: {error}"),
        None => detail,
    };
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "oauth_draft_create_failed", "detail": detail })),
    )
        .into_response()
}

fn upstream_response(record: &UpstreamRecord) -> UpstreamResponse {
    UpstreamResponse {
        id: record.id.to_string(),
        name: record.name.clone(),
        kind: record.kind,
        base_url: record.base_url.clone(),
        enabled: record.enabled,
        warmup_enabled: record.warmup_enabled,
        warmup_dialect_plugin: record.warmup_dialect_plugin.clone(),
        spec_revision: record.revision,
        status: UpstreamStatusResponse {
            last_apply_error: record.last_apply_error.clone(),
            last_apply_at_unix_secs: record.last_apply_at_unix_secs,
            last_warmup_at_unix_secs: record.last_warmup_at_unix_secs,
        },
    }
}

fn suggest_name(meta: &OrganizationMetadataRecord, account_email: Option<&str>) -> String {
    let rate_suffix = rate_suffix(meta.rate_limit_tier.as_deref());
    if let Some(name) = meta
        .organization_name
        .as_deref()
        .and_then(non_generic_org_name)
    {
        return format!("{name}{rate_suffix}");
    }
    if let (Some(email_local_part), Some(short_type)) = (
        account_email.and_then(email_local_part),
        meta.organization_type.as_deref().and_then(short_org_type),
    ) {
        return format!("{email_local_part}-{short_type}{rate_suffix}");
    }
    meta.organization_type
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "OAuth Upstream".to_owned())
}

fn non_generic_org_name(name: &str) -> Option<&str> {
    let trimmed = name.trim();
    (!trimmed.is_empty() && !trimmed.ends_with("'s Organization")).then_some(trimmed)
}

fn email_local_part(email: &str) -> Option<&str> {
    let local_part = email.split('@').next()?.trim();
    (!local_part.is_empty()).then_some(local_part)
}

fn short_org_type(org_type: &str) -> Option<&'static str> {
    match org_type {
        "claude_max" => Some("max"),
        "claude_team" => Some("team"),
        "claude_pro" => Some("pro"),
        "claude_enterprise" => Some("enterprise"),
        _ => None,
    }
}

fn rate_suffix(rate_limit_tier: Option<&str>) -> &'static str {
    match rate_limit_tier {
        Some("5x") => "-5x",
        Some("20x") => "-20x",
        _ => "",
    }
}

async fn current_revision(storage: &dyn Storage, upstream_id: Uuid) -> Option<u64> {
    UpstreamStore::get_by_id(storage, upstream_id)
        .await
        .ok()
        .flatten()
        .map(|upstream| upstream.revision)
}

fn access_token_fingerprint(access_token: &str) -> String {
    let digest = Sha256::digest(access_token.as_bytes());
    to_hex(&digest[..4])
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn normalize_oauth_code(input: &str) -> String {
    let trimmed = input.trim();
    if let Ok(url) = Url::parse(trimmed)
        && let Some((_, value)) = url.query_pairs().find(|(k, _)| k == "code")
    {
        return value.into_owned();
    }
    trimmed.split('#').next().unwrap_or(trimmed).to_string()
}
fn parse_openai_browser_code(input: &str, expected_state: &str) -> Result<String, &'static str> {
    let trimmed = input.trim();
    if let Ok(url) = Url::parse(trimmed) {
        if let Some(state) = url
            .query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, value)| value.into_owned())
            && state != expected_state
        {
            return Err("state_mismatch");
        }
        return url
            .query_pairs()
            .find(|(key, _)| key == "code")
            .map(|(_, value)| value.into_owned())
            .filter(|code| !code.trim().is_empty())
            .ok_or("missing_code");
    }
    let code = trimmed.split('#').next().unwrap_or(trimmed).trim();
    (!code.is_empty())
        .then(|| code.to_owned())
        .ok_or("missing_code")
}

fn parse_gemini_browser_code(
    input: &str,
    expected_state: &str,
    redirect_uri: &Url,
) -> Result<String, &'static str> {
    let trimmed = input.trim();
    if let Ok(callback) = Url::parse(trimmed) {
        if callback.origin() != redirect_uri.origin() || callback.path() != redirect_uri.path() {
            return Err("invalid_redirect_uri");
        }
        let states = callback
            .query_pairs()
            .filter(|(key, _)| key == "state")
            .collect::<Vec<_>>();
        if states.len() != 1
            || states
                .first()
                .is_none_or(|(_, state)| state != expected_state)
        {
            return Err("state_mismatch");
        }
        let codes = callback
            .query_pairs()
            .filter(|(key, _)| key == "code")
            .collect::<Vec<_>>();
        return codes
            .first()
            .filter(|_| codes.len() == 1)
            .map(|(_, code)| code.trim().to_owned())
            .filter(|code| !code.is_empty())
            .ok_or("missing_code");
    }
    (!trimmed.is_empty() && !trimmed.chars().any(|ch| matches!(ch, '?' | '#' | '&')))
        .then(|| trimmed.to_owned())
        .ok_or("missing_code")
}

async fn request_openai_device_code(
    config: &OpenAiOAuthConfig,
    now: u64,
) -> Result<DeviceState, &'static str> {
    #[derive(Serialize)]
    struct Request<'a> {
        client_id: &'a str,
    }
    #[derive(Deserialize)]
    struct Response {
        device_auth_id: Option<String>,
        user_code: Option<String>,
        #[serde(default)]
        interval: Option<serde_json::Value>,
        #[serde(default)]
        expires_in: Option<u64>,
        #[serde(default)]
        expires_at: Option<String>,
    }
    let response = reqwest::Client::new()
        .post(config.device_usercode_url.clone())
        .json(&Request {
            client_id: &config.client_id,
        })
        .send()
        .await
        .map_err(|_| "device_login_unavailable")?;
    if response.status() == StatusCode::NOT_FOUND {
        return Err("device_login_unavailable");
    }
    if !response.status().is_success() {
        return Err("device_login_failed");
    }
    let body = response
        .json::<Response>()
        .await
        .map_err(|_| "device_login_failed")?;
    let device_auth_id = body.device_auth_id.ok_or("device_login_failed")?;
    let user_code = body.user_code.ok_or("device_login_failed")?;
    let poll_interval_secs = match body.interval {
        Some(value) => value
            .as_u64()
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|interval| interval.parse::<u64>().ok())
            })
            .ok_or("device_login_failed")?,
        None => 5,
    }
    .max(1);
    let expires_at_unix_secs = match body.expires_at {
        Some(value) => chrono::DateTime::parse_from_rfc3339(&value)
            .ok()
            .and_then(|value| u64::try_from(value.timestamp()).ok())
            .ok_or("device_login_failed")?,
        None => now.saturating_add(body.expires_in.unwrap_or(PKCE_FLOW_TTL_SECS)),
    };
    Ok(DeviceState {
        device_auth_id,
        user_code,
        poll_interval_secs,
        expires_at_unix_secs,
        authorization_code: None,
        code_verifier: None,
        pending_attempts: 0,
    })
}

enum OpenAiDevicePoll {
    Pending,
    Authorized {
        authorization_code: String,
        code_verifier: String,
    },
    Denied,
    SlowDown,
    Expired,
}

async fn poll_openai_device_code(
    config: &OpenAiOAuthConfig,
    device: &DeviceState,
) -> Result<OpenAiDevicePoll, ProviderCompletionError> {
    #[derive(Serialize)]
    struct Request<'a> {
        device_auth_id: &'a str,
        user_code: &'a str,
    }
    #[derive(Deserialize)]
    struct Response {
        authorization_code: Option<String>,
        code_verifier: Option<String>,
    }
    let response = reqwest::Client::new()
        .post(config.device_token_url.clone())
        .json(&Request {
            device_auth_id: &device.device_auth_id,
            user_code: &device.user_code,
        })
        .send()
        .await
        .map_err(|_| {
            ProviderCompletionError::failed(
                "device_poll",
                "Device authorization",
                "device_poll_failed",
            )
        })?;
    let status = response.status();
    let body = response.bytes().await.map_err(|_| {
        ProviderCompletionError::failed("device_poll", "Device authorization", "device_poll_failed")
    })?;
    let error_body = serde_json::from_slice::<serde_json::Value>(&body).ok();
    let provider_error = error_body.as_ref().and_then(|value| {
        value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .or_else(|| value.get("code").and_then(serde_json::Value::as_str))
            .filter(|code| {
                code.len() <= 80
                    && *code != device.device_auth_id
                    && *code != device.user_code
                    && code
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
            })
            .map(str::to_owned)
    });
    if status == StatusCode::TOO_MANY_REQUESTS || provider_error.as_deref() == Some("slow_down") {
        return Ok(OpenAiDevicePoll::SlowDown);
    }
    if status == StatusCode::GONE
        || matches!(
            provider_error.as_deref(),
            Some("expired_token" | "expired" | "device_code_expired")
        )
    {
        return Ok(OpenAiDevicePoll::Expired);
    }
    if status == StatusCode::UNAUTHORIZED || provider_error.as_deref() == Some("access_denied") {
        return Ok(OpenAiDevicePoll::Denied);
    }
    if status == StatusCode::FORBIDDEN || status == StatusCode::NOT_FOUND {
        return Ok(OpenAiDevicePoll::Pending);
    }
    if !status.is_success() {
        let mut error = ProviderCompletionError::failed(
            "device_poll",
            "Device authorization",
            "device_poll_failed",
        );
        error.provider_code = provider_error;
        error.provider_status = Some(status.as_u16());
        error.detail = format!(
            "Device authorization was rejected (HTTP {}).",
            status.as_u16()
        );
        return Err(error);
    }
    let body = serde_json::from_slice::<Response>(&body).map_err(|_| {
        ProviderCompletionError::failed("device_poll", "Device authorization", "device_poll_failed")
    })?;
    match (body.authorization_code, body.code_verifier) {
        (Some(authorization_code), Some(code_verifier)) => Ok(OpenAiDevicePoll::Authorized {
            authorization_code,
            code_verifier,
        }),
        _ => Ok(OpenAiDevicePoll::Pending),
    }
}

async fn complete_gemini_oauth(
    handshake: PkceHandshakeState,
    code: String,
    config: &AntigravityOAuthConfig,
    clock: &dyn cc_lb_clock::Clock,
) -> Result<(cc_lb_storage_api::OAuthCredentials, OAuthAccountIdentity), ProviderCompletionError> {
    let (credentials, _) = complete_pkce_form_flow(
        handshake,
        code,
        Some(&config.client_secret),
        config.redirect_uri.clone(),
        Arc::new(HyperOAuthHttpClient::new()),
        clock,
    )
    .await
    .map_err(ProviderCompletionError::token)?;
    let client = reqwest::Client::new();
    let mut identity = gemini_account_identity(&client, config, &credentials.access_token).await?;
    let base = config.code_assist_url.as_str().trim_end_matches('/');
    let endpoint = format!("{}/v1internal", base.trim_end_matches("/v1internal"));
    let load = provider_completion_json(
        client
            .post(format!("{endpoint}:loadCodeAssist"))
            .bearer_auth(&credentials.access_token)
            .header(reqwest::header::USER_AGENT, &config.user_agent)
            .json(&json!({ "metadata": { "ideType": "ANTIGRAVITY" } })),
        "load_code_assist",
        "loadCodeAssist",
        &credentials.access_token,
    )
    .await?;
    let current_tier = load.get("currentTier").filter(|tier| !tier.is_null());
    if current_tier.is_none()
        && load
            .get("ineligibleTiers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|tiers| {
                tiers.iter().any(|tier| {
                    tier.get("reasonCode").and_then(serde_json::Value::as_str)
                        == Some("VALIDATION_REQUIRED")
                        && tier
                            .get("validationUrl")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|url| !url.is_empty())
                })
            })
    {
        let mut error = gemini_setup_error(&load, &credentials);
        error.code = "account_validation_required";
        return Err(error);
    }
    let tier_identity = gemini_identity_from_responses(&load, &serde_json::Value::Null);
    let plan = tier_identity.plan.as_deref();
    let (project_id, plan) = if let Some(project_id) = gemini_project_from_response(&load) {
        (project_id, plan.unwrap_or("free-tier").to_owned())
    } else {
        let default_tier = load
            .get("allowedTiers")
            .and_then(serde_json::Value::as_array)
            .and_then(|tiers| {
                tiers.iter().find(|tier| {
                    tier.get("isDefault").and_then(serde_json::Value::as_bool) == Some(true)
                })
            });
        let tier_id = default_tier
            .and_then(|tier| tier.get("id"))
            .and_then(serde_json::Value::as_str)
            .or(plan)
            .unwrap_or("free-tier");
        let onboard_body = json!({
            "tier_id": tier_id,
            "metadata": { "ide_type": "ANTIGRAVITY", "ide_version": "2.24.0", "ide_name": "antigravity" }
        });
        let mut onboard = provider_completion_json(
            client
                .post(format!("{endpoint}:onboardUser"))
                .bearer_auth(&credentials.access_token)
                .header(reqwest::header::ACCEPT, "*/*")
                .header(
                    reqwest::header::USER_AGENT,
                    format!("{} google-api-nodejs-client/10.3.0", config.user_agent),
                )
                .header("X-Goog-Api-Client", "gl-node/22.21.1")
                .json(&onboard_body),
            "onboard_user",
            "onboardUser",
            &credentials.access_token,
        )
        .await?;
        let operation_name = onboard
            .get("name")
            .and_then(serde_json::Value::as_str)
            .filter(|name| !name.is_empty())
            .map(str::to_owned);
        let mut project_id = onboard
            .get("response")
            .and_then(gemini_project_from_response);
        for _ in 0..5 {
            if onboard.get("done").and_then(serde_json::Value::as_bool) == Some(true) {
                break;
            }
            let Some(name) = operation_name.as_deref() else {
                break;
            };
            tokio::time::sleep(Duration::from_secs(2)).await;
            onboard = provider_completion_json(
                client
                    .get(format!("{endpoint}/{name}"))
                    .bearer_auth(&credentials.access_token)
                    .header(reqwest::header::USER_AGENT, &config.user_agent),
                "onboard_user",
                "onboardUser",
                &credentials.access_token,
            )
            .await?;
            project_id = onboard
                .get("response")
                .and_then(gemini_project_from_response)
                .or(project_id);
        }
        (
            project_id.ok_or_else(|| gemini_setup_error(&load, &credentials))?,
            tier_id.to_owned(),
        )
    };
    identity.plan_name = (tier_identity.plan.as_deref() == Some(plan.as_str()))
        .then_some(tier_identity.plan_name)
        .flatten();
    identity.plan = Some(plan);
    identity.project_id = Some(project_id);
    Ok((credentials, identity))
}

async fn gemini_account_identity(
    client: &reqwest::Client,
    config: &AntigravityOAuthConfig,
    access_token: &str,
) -> Result<OAuthAccountIdentity, ProviderCompletionError> {
    let userinfo = provider_completion_json(
        client
            .get(config.userinfo_url.clone())
            .bearer_auth(access_token)
            .header(reqwest::header::USER_AGENT, &config.user_agent),
        "userinfo",
        "Google userinfo",
        access_token,
    )
    .await?;
    let identity = gemini_identity_from_responses(&serde_json::Value::Null, &userinfo);
    if identity.account_email.is_none() {
        return Err(ProviderCompletionError {
            code: "missing_account_identity",
            stage: "userinfo",
            provider_code: None,
            detail: "Google userinfo did not return an account email.".to_owned(),
            status: StatusCode::BAD_REQUEST,
            provider_status: None,
        });
    }
    Ok(identity)
}

fn gemini_project_id(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    value
        .as_str()
        .or_else(|| value.get("id").and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn gemini_project_from_response(value: &serde_json::Value) -> Option<String> {
    ["cloudaicompanionProject", "projectId", "project"]
        .iter()
        .find_map(|key| gemini_project_id(value.get(key)))
}

fn gemini_setup_error(
    load: &serde_json::Value,
    credentials: &cc_lb_storage_api::OAuthCredentials,
) -> ProviderCompletionError {
    let mut error = ProviderCompletionError::missing_project();
    let Some(tiers) = load
        .get("ineligibleTiers")
        .and_then(serde_json::Value::as_array)
        .filter(|tiers| !tiers.is_empty())
    else {
        return error;
    };
    let reasons = tiers
        .iter()
        .flat_map(|tier| {
            [
                tier.get("reasonMessage").or_else(|| tier.get("reasonCode")),
                tier.get("validationErrorMessage"),
            ]
        })
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
        .map(|reason| {
            let mut reason = reason.to_owned();
            for token in [&credentials.access_token, &credentials.refresh_token] {
                if !token.is_empty() {
                    reason = reason.replace(token, "[redacted]");
                }
            }
            reason
                .chars()
                .filter(|ch| !ch.is_control())
                .take(240)
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(", ");
    error.code = "ineligible_tier";
    error.stage = "load_code_assist";
    error.detail = if reasons.is_empty() {
        "Google reported that this account is ineligible for Code Assist.".to_owned()
    } else {
        format!("Code Assist account eligibility: {reasons}")
    };
    error
}

struct ProviderCompletionError {
    code: &'static str,
    stage: &'static str,
    provider_code: Option<String>,
    detail: String,
    status: StatusCode,
    provider_status: Option<u16>,
}

impl ProviderCompletionError {
    fn token(error: OAuthTokenError) -> Self {
        match error {
            OAuthTokenError::TokenEndpoint {
                status,
                code,
                description,
            } => {
                let reason = description.or_else(|| code.clone());
                Self {
                    code: "token_exchange_rejected",
                    stage: "token_exchange",
                    provider_code: code,
                    detail: reason.map_or_else(
                        || format!("Token exchange was rejected (HTTP {}).", status.as_u16()),
                        |reason| {
                            format!(
                                "Token exchange was rejected (HTTP {}): {reason}",
                                status.as_u16()
                            )
                        },
                    ),
                    status: StatusCode::BAD_REQUEST,
                    provider_status: Some(status.as_u16()),
                }
            }
            OAuthTokenError::Http { .. } => Self::failed(
                "token_exchange",
                "Token endpoint",
                "token_endpoint_unreachable",
            ),
            OAuthTokenError::Json { .. } => {
                Self::failed("token_exchange", "Token endpoint", "invalid_token_response")
            }
        }
    }

    fn failed(stage: &'static str, label: &str, code: &'static str) -> Self {
        Self {
            code,
            stage,
            provider_code: None,
            detail: format!("{label} could not be completed."),
            status: StatusCode::BAD_GATEWAY,
            provider_status: None,
        }
    }

    fn missing_project() -> Self {
        Self {
            code: "missing_project_id",
            stage: "onboard_user",
            provider_code: None,
            detail: "Code Assist did not return a usable project ID.".to_owned(),
            status: StatusCode::BAD_REQUEST,
            provider_status: None,
        }
    }
}

async fn provider_completion_json(
    request: reqwest::RequestBuilder,
    stage: &'static str,
    label: &str,
    access_token: &str,
) -> Result<serde_json::Value, ProviderCompletionError> {
    let failed = || {
        ProviderCompletionError::failed(
            stage,
            label,
            match stage {
                "userinfo" => "userinfo_failed",
                "load_code_assist" => "load_code_assist_failed",
                _ => "onboard_user_failed",
            },
        )
    };
    let response = request.send().await.map_err(|_| failed())?;
    let status = response.status();
    let body = response.bytes().await.map_err(|_| failed())?;
    let value = serde_json::from_slice::<serde_json::Value>(&body).ok();
    if !status.is_success()
        || value
            .as_ref()
            .is_some_and(|value| value.get("error").is_some())
    {
        let error_value = value.as_ref().and_then(|value| value.get("error"));
        let provider_code = error_value
            .and_then(|error| {
                error
                    .as_str()
                    .or_else(|| error.get("status").and_then(serde_json::Value::as_str))
            })
            .filter(|code| {
                code.len() <= 80
                    && *code != access_token
                    && code
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
            })
            .map(str::to_owned);
        let reason = value
            .as_ref()
            .and_then(|value| {
                value
                    .get("error_description")
                    .and_then(serde_json::Value::as_str)
            })
            .or_else(|| {
                error_value
                    .and_then(|value| value.get("message").and_then(serde_json::Value::as_str))
            })
            .or(provider_code.as_deref())
            .map(|reason| {
                reason
                    .replace(access_token, "[redacted]")
                    .chars()
                    .filter(|ch| !ch.is_control())
                    .take(240)
                    .collect::<String>()
            });
        let mut error = failed();
        error.detail = reason.map_or_else(
            || format!("{label} was rejected (HTTP {}).", status.as_u16()),
            |reason| format!("{label} was rejected (HTTP {}): {reason}", status.as_u16()),
        );
        error.provider_code = provider_code;
        error.provider_status = Some(status.as_u16());
        return Err(error);
    }
    value.ok_or_else(failed)
}

#[derive(Clone)]
struct CompletionFailureLog {
    provider: OAuthProvider,
    stage: &'static str,
    provider_code: Option<String>,
    code: &'static str,
}

fn finish_provider_completion(provider: OAuthProvider, response: Response) -> Response {
    if provider != OAuthProvider::Anthropic
        && (response.status().is_client_error() || response.status().is_server_error())
    {
        let failure = response.extensions().get::<CompletionFailureLog>();
        tracing::warn!(
            provider = ?failure.map(|failure| failure.provider).unwrap_or(provider),
            stage = failure.map(|failure| failure.stage).unwrap_or("completion"),
            provider_error_code = failure.and_then(|failure| failure.provider_code.as_deref()).unwrap_or("unknown"),
            error = failure.map(|failure| failure.code).unwrap_or("completion_failed"),
            "OAuth completion failed"
        );
    }
    response
}

fn oauth_provider_error_response(
    provider: OAuthProvider,
    error: ProviderCompletionError,
) -> Response {
    let failure = CompletionFailureLog {
        provider,
        stage: error.stage,
        provider_code: error.provider_code.clone(),
        code: error.code,
    };
    let mut response = (
        error.status,
        Json(json!({
            "error": error.code,
            "detail": error.detail,
            "stage": error.stage,
            "provider_error": error.provider_code,
            "provider_status": error.provider_status,
        })),
    )
        .into_response();
    response.extensions_mut().insert(failure);
    response
}

fn provider_flow_error(
    provider: OAuthProvider,
    code: &'static str,
    status: StatusCode,
) -> Response {
    let detail = match code {
        "oauth_client_not_configured" => {
            "Configure oauth.antigravity.client_id and oauth.antigravity.client_secret before starting Gemini subscription OAuth."
        }
        "device_code_expired" => "The device code expired. Start a new connection.",
        "access_denied" => "The provider denied this connection.",
        "slow_down" => "Wait before checking the device connection again.",
        "state_mismatch" => {
            "The callback belongs to a different connection. Use the current login link."
        }
        "missing_existing_identity" => "The saved account identity could not be verified.",
        "missing_account_identity" => "The provider did not return an account identity.",
        _ => "The connection could not be completed.",
    };
    oauth_provider_error_response(
        provider,
        ProviderCompletionError {
            code,
            stage: if code.starts_with("device_") || matches!(code, "slow_down" | "access_denied") {
                "device_poll"
            } else {
                "identity"
            },
            provider_code: matches!(code, "slow_down" | "access_denied").then(|| code.to_owned()),
            detail: detail.to_owned(),
            status,
            provider_status: None,
        },
    )
}
async fn complete_provider_oauth(
    state: &AdminState,
    storage: &dyn Storage,
    identity: &AdminIdentity,
    upstream_id: Uuid,
    current: UpstreamRecord,
    in_flight: InFlightPkce,
    payload: CompleteRequest,
) -> Response {
    let provider = in_flight.provider;
    let response = async {
        if (current.kind == UpstreamKind::OpenaiOauth
            && in_flight.provider != OAuthProvider::Openai)
            || (current.kind == UpstreamKind::GeminiOauth
                && in_flight.provider != OAuthProvider::Gemini)
        {
            return invalid_state_response("state token provider does not match this upstream");
        }
        let expected_revision = match &in_flight.target {
            PkceTarget::ExistingUpstream {
                expected_revision, ..
            } => *expected_revision,
            PkceTarget::PendingDraft { .. } => {
                return invalid_state_response("state token is for an OAuth draft");
            }
        };
        if expected_revision != current.revision {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "stale_revision", "current_revision": current.revision })),
            )
                .into_response();
        }
        let (credentials, account_identity) = if current.kind == UpstreamKind::OpenaiOauth {
            let config = state.config.oauth.openai.clone().unwrap_or_default();
            let (handshake, authorization_code, redirect_uri) = if let Some(device) =
                in_flight.device.clone()
            {
                let poll_interval_secs = Some(device.poll_interval_secs);
                if cc_lb_clock::unix_secs(state.clock.now()) >= device.expires_at_unix_secs {
                    return provider_flow_error(
                        provider,
                        "device_code_expired",
                        StatusCode::BAD_REQUEST,
                    );
                }
                let poll = match poll_openai_device_code(&config, &device).await {
                    Ok(result) => result,
                    Err(error) => return oauth_provider_error_response(in_flight.provider, error),
                };
                let (authorization_code, code_verifier) = match poll {
                    OpenAiDevicePoll::Pending => {
                        let mut pending = in_flight;
                        if let Some(device) = pending.device.as_mut() {
                            device.pending_attempts = device.pending_attempts.saturating_add(1);
                        }
                        if let Err(response) =
                            persist_pkce_flow(state, storage, &payload.state_token, &pending).await
                        {
                            return response;
                        }
                        return (
                            StatusCode::ACCEPTED,
                            Json(json!({
                                "status": "pending",
                                "poll_interval_secs": poll_interval_secs
                            })),
                        )
                            .into_response();
                    }
                    OpenAiDevicePoll::SlowDown => {
                        return provider_flow_error(
                            provider,
                            "slow_down",
                            StatusCode::TOO_MANY_REQUESTS,
                        );
                    }
                    OpenAiDevicePoll::Expired => {
                        return provider_flow_error(
                            provider,
                            "device_code_expired",
                            StatusCode::BAD_REQUEST,
                        );
                    }
                    OpenAiDevicePoll::Denied => {
                        return provider_flow_error(
                            provider,
                            "access_denied",
                            StatusCode::BAD_REQUEST,
                        );
                    }
                    OpenAiDevicePoll::Authorized {
                        authorization_code,
                        code_verifier,
                    } => (authorization_code, code_verifier),
                };
                (
                    in_flight.handshake.clone().with_verifier(code_verifier),
                    authorization_code,
                    config.device_redirect_uri.clone(),
                )
            } else {
                let Some(code) = payload.code.as_deref() else {
                    return invalid_state_response("authorization code is required");
                };
                let authorization_code = match parse_openai_browser_code(code, &payload.state_token)
                {
                    Ok(code) => code,
                    Err("state_mismatch") => {
                        return provider_flow_error(
                            provider,
                            "state_mismatch",
                            StatusCode::BAD_REQUEST,
                        );
                    }
                    Err(_) => {
                        return invalid_state_response("authorization code is required");
                    }
                };
                (
                    in_flight.handshake.clone(),
                    authorization_code,
                    config.browser_redirect_uri.clone(),
                )
            };
            let (credentials, account_identity) = match complete_pkce_form_flow(
                handshake,
                authorization_code,
                None,
                redirect_uri,
                Arc::new(HyperOAuthHttpClient::new()),
                &*state.clock,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => {
                    return oauth_provider_error_response(
                        in_flight.provider,
                        ProviderCompletionError::token(error),
                    );
                }
            };
            (credentials, account_identity)
        } else {
            let Some(config) = state.config.oauth.antigravity.clone() else {
                return provider_flow_error(
                    provider,
                    "oauth_client_not_configured",
                    StatusCode::BAD_REQUEST,
                );
            };
            if config.missing_client_credentials().is_some() {
                return provider_flow_error(
                    provider,
                    "oauth_client_not_configured",
                    StatusCode::BAD_REQUEST,
                );
            }
            let Some(code) = payload.code.as_deref() else {
                return invalid_state_response("authorization code is required");
            };
            let (credentials, account_identity) = match complete_gemini_oauth(
                in_flight.handshake,
                match parse_gemini_browser_code(code, &payload.state_token, &config.redirect_uri) {
                    Ok(code) => code,
                    Err("state_mismatch") => {
                        return provider_flow_error(
                            provider,
                            "state_mismatch",
                            StatusCode::BAD_REQUEST,
                        );
                    }
                    Err(_) => return invalid_state_response("invalid authorization callback"),
                },
                &config,
                &*state.clock,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => return oauth_provider_error_response(in_flight.provider, error),
            };
            (credentials, account_identity)
        };
        let account_id = account_identity
            .account_id
            .clone()
            .or(account_identity.account_email.clone());
        let Some(mut old_bundle) = current.oauth_credentials.as_ref().and_then(|tokens| {
            tokens
                .decrypt(state.aead.as_ref(), upstream_id.as_bytes())
                .ok()
        }) else {
            return provider_flow_error(
                provider,
                "missing_existing_identity",
                StatusCode::CONFLICT,
            );
        };
        if old_bundle
            .account_id
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        {
            old_bundle.account_id = if current.kind == UpstreamKind::OpenaiOauth {
                cc_lb_oauth_protocol::account_id_from_access_token(&old_bundle.access_token)
            } else {
                let config = state.config.oauth.antigravity.clone().unwrap_or_default();
                gemini_account_identity(&reqwest::Client::new(), &config, &old_bundle.access_token)
                    .await
                    .ok()
                    .and_then(|identity| identity.account_email)
            };
        }
        let Some(old_account) = old_bundle.account_id.as_deref() else {
            return provider_flow_error(
                provider,
                "missing_existing_identity",
                StatusCode::CONFLICT,
            );
        };
        let Some(new_account) = account_id.filter(|value| !value.trim().is_empty()) else {
            return provider_flow_error(
                provider,
                "missing_account_identity",
                StatusCode::BAD_REQUEST,
            );
        };
        if current.kind == UpstreamKind::OpenaiOauth && new_account != old_account {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "account_mismatch",
                    "detail": "Reconnect with the same ChatGPT account as the saved upstream.",
                    "previous_account_id": old_bundle.account_id,
                    "account_id": new_account,
                })),
            )
                .into_response();
        }
        if current.kind == UpstreamKind::GeminiOauth
            && !new_account.eq_ignore_ascii_case(old_account)
        {
            return (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "account_mismatch",
                    "detail": "Reconnect with the same Google account as the saved upstream.",
                    "previous_email": old_bundle.account_id,
                    "email": new_account,
                })),
            )
                .into_response();
        }
        old_bundle.access_token = credentials.access_token;
        old_bundle.refresh_token = credentials.refresh_token;
        old_bundle.expires_at_unix_secs = credentials.expires_at;
        old_bundle.refresh_token_expires_at_unix_secs =
            credentials.refresh_token_expires_at_unix_secs;
        old_bundle.account_id = Some(new_account);
        old_bundle.project_id = account_identity.project_id.clone();
        old_bundle.scopes = credentials.scopes;
        old_bundle.never_refresh = false;
        if current.kind == UpstreamKind::GeminiOauth {
            old_bundle.oauth_profile = cc_lb_aead::GeminiOAuthProfile::Antigravity;
        }
        let encrypted = match AeadEncryptedField::<OAuthTokenBundle>::encrypt(
            &state.aead,
            &old_bundle,
            upstream_id.as_bytes(),
        ) {
            Ok(value) => value,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        if let Err(response) = delete_pkce_flow(storage, &payload.state_token).await {
            return response;
        }
        let updated = match storage
            .store_oauth_tokens(
                upstream_id,
                current.revision,
                encrypted,
                old_bundle.never_refresh,
            )
            .await
        {
            Ok(value) => value,
            Err(StorageError::Conflict { .. }) => {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "stale_revision" })),
                )
                    .into_response();
            }
            Err(error) => return storage_error_response(&error),
        };
        let records = provider_metadata_records(
            upstream_id,
            &account_identity,
            cc_lb_clock::unix_millis(state.clock.now()),
        );
        if let Err(error) = storage
            .put_upstream_subscription_metadata(&records.subscription_metadata_record)
            .await
        {
            rebuild_after_committed_failure(state, "oauth reconnect identity persistence").await;
            return storage_error_response(&error);
        }
        if let Err(error) =
            seed_oauth_reconnect_tasks(state, &updated, old_bundle.expires_at_unix_secs).await
        {
            rebuild_after_committed_failure(state, "oauth reconnect scheduler seeding").await;
            return scheduler_error_response(&error);
        }
        let access_token_fingerprint = access_token_fingerprint(&old_bundle.access_token);
        let audit_action = AuditPayload::UpstreamOauthComplete {
            upstream_id: upstream_id.to_string(),
            upstream_name: current.name.clone(),
            expires_at_unix_secs: old_bundle.expires_at_unix_secs,
            access_token_fingerprint: access_token_fingerprint.clone(),
        }
        .to_string();
        let route = format!("/admin/v1/upstreams/{upstream_id}/oauth/complete");
        if let Err(error) = record_admin_audit(
            state,
            AdminAuditEvent {
                identity: Some(identity),
                system_component: None,
                action: &audit_action,
                route: &route,
                target_principal_id: None,
                target_upstream: Some(&current.name),
                api_key_id: None,
                status: StatusCode::OK.as_u16(),
                payload: None,
            },
        )
        .await
        {
            tracing::error!(error = %error, "admin audit write failed");
            rebuild_after_audit_failure(state).await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "audit_write_failed" })),
            )
                .into_response();
        }
        let mut response = Json(CompleteResponse {
            upstream_id: updated.id,
            expires_at_unix_secs: old_bundle.expires_at_unix_secs,
            access_token_fingerprint,
            mode: OAuthTokenMode::Refreshing,
            long_lived_fallback: false,
            fallback_reason: None,
            granted_expires_in_secs: None,
        })
        .into_response();
        add_dynamic_rebind_headers(&mut response, state).await;
        response
    }
    .await;
    finish_provider_completion(provider, response)
}
async fn start_provider_oauth(
    state: &AdminState,
    storage: &dyn Storage,
    identity: &AdminIdentity,
    upstream: UpstreamRecord,
    upstream_id: Uuid,
    requested_flow: Option<OpenAiFlow>,
) -> Response {
    let provider = if upstream.kind == UpstreamKind::OpenaiOauth {
        OAuthProvider::Openai
    } else {
        OAuthProvider::Gemini
    };
    let state_token = match encode_state(upstream_id) {
        Ok(value) => value,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let now = cc_lb_clock::unix_secs(state.clock.now());
    let (handshake, device, verification_url) = if provider == OAuthProvider::Openai {
        let config = state.config.oauth.openai.clone().unwrap_or_default();
        if requested_flow == Some(OpenAiFlow::Browser) {
            let (Ok(auth), Ok(token)) = (
                AuthUrl::new(config.authorize_url.to_string()),
                TokenUrl::new(config.token_url.to_string()),
            ) else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            let mut handshake = start_pkce_flow_without_code(
                ClientId::new(config.client_id),
                auth,
                token,
                vec![
                    "openid".to_owned(),
                    "profile".to_owned(),
                    "email".to_owned(),
                    "offline_access".to_owned(),
                ],
                config.browser_redirect_uri,
            )
            .into_state();
            // Keep these OpenAI Codex CLI parameters in sync with
            // `build_authorize_url` in github.com/openai/codex:
            // https://github.com/openai/codex/blob/main/codex-rs/login/src/server.rs#L580-L625
            handshake
                .authorize_url
                .query_pairs_mut()
                .append_pair("id_token_add_organizations", "true")
                .append_pair("codex_cli_simplified_flow", "true")
                .append_pair("state", &state_token);
            (handshake, None, None)
        } else {
            let device = match request_openai_device_code(&config, now).await {
                Ok(device) => device,
                Err(error) => {
                    return (
                        if error == "device_login_unavailable" {
                            StatusCode::NOT_FOUND
                        } else {
                            StatusCode::BAD_GATEWAY
                        },
                        Json(json!({ "error": error })),
                    )
                        .into_response();
                }
            };
            let (Ok(auth), Ok(token)) = (
                AuthUrl::new(config.auth_url.to_string()),
                TokenUrl::new(config.token_url.to_string()),
            ) else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            (
                start_pkce_flow_without_code(
                    ClientId::new(config.client_id),
                    auth,
                    token,
                    Vec::new(),
                    config.device_redirect_uri,
                )
                .into_state(),
                Some(device),
                Some(config.device_verification_url.to_string()),
            )
        }
    } else {
        let Some(config) = state.config.oauth.antigravity.clone() else {
            return provider_flow_error(
                provider,
                "oauth_client_not_configured",
                StatusCode::BAD_REQUEST,
            );
        };
        if config.missing_client_credentials().is_some() {
            return provider_flow_error(
                provider,
                "oauth_client_not_configured",
                StatusCode::BAD_REQUEST,
            );
        }
        let (Ok(auth), Ok(token)) = (
            AuthUrl::new(config.authorize_url.to_string()),
            TokenUrl::new(config.token_url.to_string()),
        ) else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let mut handshake = start_pkce_flow_without_code(
            ClientId::new(config.client_id),
            auth,
            token,
            config.scopes,
            config.redirect_uri,
        )
        .into_state();
        handshake
            .authorize_url
            .query_pairs_mut()
            .append_pair("access_type", "offline")
            .append_pair("prompt", "consent")
            .append_pair("state", &state_token);
        (handshake, None, None)
    };
    let in_flight = InFlightPkce {
        provider,
        handshake: handshake.clone(),
        created_at_unix_secs: now,
        initiated_by: identity.audit_fields(),
        target: PkceTarget::ExistingUpstream {
            upstream_id,
            upstream_name: upstream.name.clone(),
            expected_revision: upstream.revision,
        },
        device: device.clone(),
    };
    if let Err(response) = persist_pkce_flow(state, storage, &state_token, &in_flight).await {
        return response;
    }
    let audit_action = AuditPayload::UpstreamOauthStart {
        upstream_id: upstream_id.to_string(),
        upstream_name: upstream.name.clone(),
    }
    .to_string();
    let route = format!("/admin/v1/upstreams/{upstream_id}/oauth/start");
    if record_admin_audit(
        state,
        AdminAuditEvent {
            identity: Some(identity),
            system_component: None,
            action: &audit_action,
            route: &route,
            target_principal_id: None,
            target_upstream: Some(&upstream.name),
            api_key_id: None,
            status: StatusCode::OK.as_u16(),
            payload: None,
        },
    )
    .await
    .is_err()
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "audit_write_failed" })),
        )
            .into_response();
    }
    Json(StartResponse {
        authorize_url: device
            .is_none()
            .then(|| handshake.authorize_url.to_string()),
        state_token,
        revision: upstream.revision,
        provider,
        flow: if device.is_some() {
            "device_code"
        } else if provider == OAuthProvider::Gemini {
            "browser"
        } else {
            "paste_code"
        },
        verification_url,
        user_code: device.as_ref().map(|value| value.user_code.clone()),
        poll_interval_secs: device.as_ref().map(|value| value.poll_interval_secs),
        expires_at_unix_secs: device.map(|value| value.expires_at_unix_secs),
    })
    .into_response()
}
#[cfg(test)]
mod tests {
    use cc_lb_storage_api::OrganizationMetadataRecord;
    use uuid::Uuid;

    use super::{oauth_bootstrap_tasks, suggest_name};

    #[test]
    fn suggest_name_uses_non_generic_org_name_with_rate_suffix() {
        let meta = org_meta(
            Some("Example Org"),
            Some("claude_team"),
            Some("5x"),
            Some("operator@example.com"),
        );

        assert_eq!(
            suggest_name(&meta, meta.account_email.as_deref()),
            "Example Org-5x"
        );
    }

    #[test]
    fn suggest_name_falls_back_to_email_and_short_org_type_for_generic_org() {
        let meta = org_meta(
            Some("Example's Organization"),
            Some("claude_max"),
            Some("20x"),
            Some("operator@example.com"),
        );

        assert_eq!(
            suggest_name(&meta, meta.account_email.as_deref()),
            "operator-max-20x"
        );
    }

    #[test]
    fn oauth_bootstrap_tasks_seed_refresh_with_seed_suffix() {
        let upstream_id =
            Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("uuid parses");
        let seed_secs = 123_456;

        let [refresh] = oauth_bootstrap_tasks(upstream_id, seed_secs);

        assert_eq!(
            refresh.idempotency_key.as_deref(),
            Some("adaptive:oauth_refresh:12345678-1234-5678-1234-567812345678:bootstrap:123456")
        );
        assert_eq!(refresh.run_at_unix_secs, Some(seed_secs));
        let cc_lb_scheduler::worker::AdaptiveJob::OAuthRefresh(refresh_job) = refresh.args else {
            panic!("expected oauth refresh job");
        };
        assert_eq!(refresh_job.upstream_id, upstream_id);
    }

    fn org_meta(
        organization_name: Option<&str>,
        organization_type: Option<&str>,
        rate_limit_tier: Option<&str>,
        account_email: Option<&str>,
    ) -> OrganizationMetadataRecord {
        OrganizationMetadataRecord {
            organization_uuid: "org-1".to_owned(),
            organization_name: organization_name.map(str::to_owned),
            organization_type: organization_type.map(str::to_owned),
            rate_limit_tier: rate_limit_tier.map(str::to_owned),
            seat_tier: None,
            has_extra_usage_enabled: None,
            billing_type: None,
            subscription_created_at_unix_secs: None,
            account_email: account_email.map(str::to_owned),
            account_display_name: None,
            account_uuid: None,
            overage_credit_amount_minor_units: None,
            overage_credit_currency: None,
            overage_credit_granted: None,
            overage_credit_eligible: None,
            observed_at_unix_millis: 0,
            last_error: None,
            raw_profile: None,
            raw_overage_grant: None,
        }
    }
}
