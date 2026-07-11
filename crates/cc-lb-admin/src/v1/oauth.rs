use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_aead::{AeadEncryptedField, EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_control::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_control::anthropic_metadata::make_metadata_http_client;
use cc_lb_control::{AuditEntry, AuditPayload, fetch_metadata_only};
use cc_lb_scheduler::error::SchedulerError;
use cc_lb_scheduler::jobs::oauth_refresh::OAuthRefreshJob;
use cc_lb_scheduler::worker::{AdaptiveJob, SchedulerPushTask};
use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamWarmupDialectPlugin};
use cc_lb_storage_api::{
    OrganizationMetadataRecord, Storage, StorageError, UpstreamCreate, UpstreamRecord,
    UpstreamStore, UpstreamSubscriptionMetadataRecord, validate_identifier,
};
use oauth2::{AuthUrl, ClientId, TokenUrl};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use crate::AdminState;
use crate::oauth_pkce::{
    HyperOAuthHttpClient, OAuthTokenError, PkceHandshakeState, complete_pkce_flow, start_pkce_flow,
};

type PkceFlows = Arc<Mutex<HashMap<String, InFlightPkce>>>;

static PKCE_FLOWS: OnceLock<PkceFlows> = OnceLock::new();

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
    scopes: Vec<String>,
}

#[derive(Deserialize)]
struct StartRequest {}

// `revision` is echoed so the frontend can refresh its cached `If-Match`
// before a cancel-cleanup DELETE. start_oauth does not currently mutate
// the upstream, but the contract stays correct if it ever does.
#[derive(Serialize)]
struct StartResponse {
    authorize_url: String,
    state_token: String,
    revision: u64,
}

#[derive(Serialize)]
struct DraftStartResponse {
    authorize_url: String,
    state_token: String,
}

#[derive(Deserialize)]
struct CompleteRequest {
    state_token: String,
    code: String,
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
}

#[derive(Serialize)]
struct DraftCompleteResponse {
    state_token: String,
    suggested_name: String,
    subscription_metadata: UpstreamSubscriptionMetadataRecord,
    organization_metadata: Option<OrganizationMetadataRecord>,
}

#[derive(Serialize)]
struct UpstreamResponse {
    id: String,
    name: String,
    kind: UpstreamKind,
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

#[derive(Clone)]
struct InFlightPkce {
    handshake: PkceHandshakeState,
    created_at_unix_secs: u64,
    target: PkceTarget,
}

#[derive(Clone)]
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

#[derive(Clone)]
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
    Path(upstream_id): Path<Uuid>,
    Json(_payload): Json<StartRequest>,
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
    if upstream.kind != UpstreamKind::AnthropicOauth {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "wrong_kind" })),
        )
            .into_response();
    }
    let upstream_revision = upstream.revision;

    let config = state.config.current_config();
    let claude_default;
    let oauth = match config.oauth.anthropic.as_ref() {
        Some(oauth) => oauth,
        None => {
            claude_default = claude_code_default_oauth();
            &claude_default
        }
    };
    let authorize_endpoint = match AuthUrl::new(oauth.auth_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let token_endpoint = match TokenUrl::new(oauth.token_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let handshake = start_pkce_flow(
        ClientId::new(oauth.client_id.clone()),
        authorize_endpoint,
        token_endpoint,
        oauth.scopes.clone(),
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

    let now = cc_lb_engine::clock::unix_secs(state.clock.now());
    let in_flight = InFlightPkce {
        handshake: handshake_state.clone(),
        created_at_unix_secs: now,
        target: PkceTarget::ExistingUpstream {
            upstream_id,
            upstream_name: upstream.name.clone(),
            expected_revision: upstream.revision,
        },
    };
    match pkce_flows().lock() {
        Ok(mut flows) => {
            flows.retain(|_, flow| {
                now.saturating_sub(flow.created_at_unix_secs) < PKCE_FLOW_TTL_SECS
            });
            flows.insert(state_token.clone(), in_flight);
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    enqueue_upstream_audit(
        &state,
        AuditPayload::UpstreamOauthStart {
            upstream_id: upstream_id.to_string(),
            upstream_name: upstream.name,
        },
        upstream_id,
        200,
    );

    Json(StartResponse {
        authorize_url: handshake_state.authorize_url.to_string(),
        state_token,
        revision: upstream_revision,
    })
    .into_response()
}

async fn start_oauth_draft(State(state): State<AdminState>) -> Response {
    let config = state.config.current_config();
    let claude_default;
    let oauth = match config.oauth.anthropic.as_ref() {
        Some(oauth) => oauth,
        None => {
            claude_default = claude_code_default_oauth();
            &claude_default
        }
    };
    let authorize_endpoint = match AuthUrl::new(oauth.auth_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let token_endpoint = match TokenUrl::new(oauth.token_url.to_string()) {
        Ok(url) => url,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let handshake = start_pkce_flow(
        ClientId::new(oauth.client_id.clone()),
        authorize_endpoint,
        token_endpoint,
        oauth.scopes.clone(),
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

    let now = cc_lb_engine::clock::unix_secs(state.clock.now());
    let in_flight = InFlightPkce {
        handshake: handshake_state.clone(),
        created_at_unix_secs: now,
        target: PkceTarget::PendingDraft { completed: None },
    };
    match pkce_flows().lock() {
        Ok(mut flows) => {
            flows.retain(|_, flow| {
                now.saturating_sub(flow.created_at_unix_secs) < PKCE_FLOW_TTL_SECS
            });
            flows.insert(state_token.clone(), in_flight);
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    Json(DraftStartResponse {
        authorize_url: handshake_state.authorize_url.to_string(),
        state_token,
    })
    .into_response()
}

async fn complete_oauth_draft(
    State(state): State<AdminState>,
    Json(payload): Json<CompleteRequest>,
) -> Response {
    let in_flight = match pkce_flows().lock() {
        Ok(flows) => flows.get(&payload.state_token).cloned(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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

    let handshake = match in_flight.handshake.into_handshake() {
        Ok(handshake) => handshake,
        Err(_) => {
            return invalid_state_response(
                "stored PKCE handshake is corrupted — restart the OAuth flow",
            );
        }
    };
    let code = normalize_oauth_code(&payload.code);
    let credentials = match complete_pkce_flow(
        handshake,
        code,
        payload.state_token.clone(),
        Arc::new(HyperOAuthHttpClient::new()),
        &*state.clock,
    )
    .await
    {
        Ok(credentials) => credentials,
        Err(error) => return token_exchange_error_response(error),
    };

    let bundle = OAuthTokenBundle {
        access_token: credentials.access_token,
        refresh_token: credentials.refresh_token,
        expires_at_unix_secs: credentials.expires_at,
        scopes: credentials.scopes,
    };
    let encrypted_tokens = match AeadEncryptedField::<OAuthTokenBundle>::encrypt(
        &state.aead,
        &bundle,
        Uuid::nil().as_bytes(),
    ) {
        Ok(encrypted) => encrypted,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
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
    let records = match tokio::time::timeout(
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
    };
    let suggested_name = records
        .organization_metadata_record
        .as_ref()
        .map(|record| suggest_name(record, record.account_email.as_deref()))
        .unwrap_or_else(|| "OAuth Upstream".to_owned());
    let completion = DraftCompletion {
        encrypted_tokens,
        subscription_metadata_record: records.subscription_metadata_record.clone(),
        organization_metadata_record: records.organization_metadata_record.clone(),
        fetched_at_unix_secs: cc_lb_engine::clock::unix_secs(state.clock.now()),
    };

    match pkce_flows().lock() {
        Ok(mut flows) => match flows.get_mut(&payload.state_token) {
            Some(InFlightPkce {
                target: PkceTarget::PendingDraft { completed },
                ..
            }) if completed.is_none() => {
                *completed = Some(Box::new(completion));
            }
            Some(InFlightPkce {
                target: PkceTarget::PendingDraft { .. },
                ..
            }) => return invalid_state_response("OAuth draft has already been completed"),
            Some(_) => return invalid_state_response("state token is for an existing upstream"),
            None => {
                return invalid_state_response(
                    "state token expired or already used — restart the OAuth flow",
                );
            }
        },
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    Json(DraftCompleteResponse {
        state_token: payload.state_token,
        suggested_name,
        subscription_metadata: records.subscription_metadata_record,
        organization_metadata: records.organization_metadata_record,
    })
    .into_response()
}

async fn create_upstream_from_oauth_draft(
    State(state): State<AdminState>,
    Json(payload): Json<CreateFromDraftRequest>,
) -> Response {
    if let Err(error) = validate_identifier("name", &payload.name) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid_name", "detail": error.to_string() })),
        )
            .into_response();
    }
    let completion = match pkce_flows().lock() {
        Ok(flows) => flows
            .get(&payload.state_token)
            .and_then(|flow| match &flow.target {
                PkceTarget::PendingDraft {
                    completed: Some(completion),
                } => Some(completion.clone()),
                _ => None,
            }),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let Some(completion) = completion else {
        return invalid_state_response("OAuth draft is missing or incomplete");
    };
    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let base_url = match payload.base_url {
        Some(base_url) => Some(base_url),
        None => match Url::parse(DEFAULT_OAUTH_UPSTREAM_BASE_URL) {
            Ok(base_url) => Some(base_url),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
    };
    let created = match UpstreamStore::create(
        storage.as_ref(),
        UpstreamCreate {
            name: payload.name,
            kind: UpstreamKind::AnthropicOauth,
            base_url,
            api_key_ciphertext: None,
            oauth_token_generation: None,
            warmup_enabled: false,
            warmup_dialect_plugin: None,
        },
    )
    .await
    {
        Ok(created) => created,
        Err(error) => return storage_error_response(&error),
    };
    let bundle = match completion
        .encrypted_tokens
        .decrypt(state.aead.as_ref(), Uuid::nil().as_bytes())
    {
        Ok(bundle) => bundle,
        Err(error) => {
            return rollback_oauth_draft_creation(
                storage.as_ref(),
                &payload.state_token,
                created.id,
                format!("draft token decrypt failed: {error}"),
            )
            .await;
        }
    };
    let encrypted_tokens = match AeadEncryptedField::<OAuthTokenBundle>::encrypt(
        state.aead.as_ref(),
        &bundle,
        created.id.as_bytes(),
    ) {
        Ok(encrypted) => encrypted,
        Err(error) => {
            return rollback_oauth_draft_creation(
                storage.as_ref(),
                &payload.state_token,
                created.id,
                format!("draft token re-encrypt failed: {error}"),
            )
            .await;
        }
    };
    let updated = match storage
        .store_oauth_tokens(created.id, created.revision, encrypted_tokens)
        .await
    {
        Ok(updated) => updated,
        Err(error) => {
            return rollback_oauth_draft_creation(
                storage.as_ref(),
                &payload.state_token,
                created.id,
                format!("oauth token storage failed: {error}"),
            )
            .await;
        }
    };
    if let Err(error) = seed_oauth_bootstrap_tasks(&state, &updated).await {
        return rollback_oauth_draft_creation(
            storage.as_ref(),
            &payload.state_token,
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
            storage.as_ref(),
            &payload.state_token,
            created.id,
            format!("subscription metadata storage failed: {error}"),
        )
        .await;
    }
    if let Some(organization_record) = completion.organization_metadata_record.as_ref()
        && let Err(error) = storage.put_organization_metadata(organization_record).await
    {
        return rollback_oauth_draft_creation(
            storage.as_ref(),
            &payload.state_token,
            created.id,
            format!("organization metadata storage failed: {error}"),
        )
        .await;
    }

    if let Ok(mut flows) = pkce_flows().lock() {
        flows.remove(&payload.state_token);
    }

    let mut response = (StatusCode::CREATED, Json(upstream_response(&updated))).into_response();
    if let Ok(location) = HeaderValue::from_str(&format!("/admin/v1/upstreams/{}", updated.id)) {
        response.headers_mut().insert(header::LOCATION, location);
    }
    add_dynamic_rebind_headers(&mut response, &state).await;
    response
}

async fn complete_oauth(
    State(state): State<AdminState>,
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
    let in_flight = match pkce_flows().lock() {
        Ok(flows) => flows.get(&payload.state_token).cloned(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let Some(in_flight) = in_flight else {
        let size = pkce_flows().lock().map(|flows| flows.len()).unwrap_or(0);
        tracing::warn!(
            %upstream_id,
            pkce_flows_size = size,
            "oauth complete: state token not in pkce flow store (process restart or already consumed)"
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

    let Some(storage) = state.storage.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let current = match UpstreamStore::get_by_id(storage.as_ref(), upstream_id).await {
        Ok(Some(upstream)) => upstream,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return storage_error_response(&error),
    };
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
    let code = normalize_oauth_code(&payload.code);
    let credentials = match complete_pkce_flow(
        handshake,
        code,
        payload.state_token.clone(),
        Arc::new(HyperOAuthHttpClient::new()),
        &*state.clock,
    )
    .await
    {
        Ok(credentials) => credentials,
        Err(error) => {
            tracing::warn!(%upstream_id, %error, "oauth complete: token exchange rejected by provider");
            return token_exchange_error_response(error);
        }
    };

    if let Ok(mut flows) = pkce_flows().lock() {
        flows.remove(&payload.state_token);
    }

    let access_token_fingerprint = access_token_fingerprint(&credentials.access_token);
    let bundle = OAuthTokenBundle {
        access_token: credentials.access_token,
        refresh_token: credentials.refresh_token,
        expires_at_unix_secs: credentials.expires_at,
        scopes: credentials.scopes,
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
        .store_oauth_tokens(upstream_id, expected_revision, encrypted)
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

    if let Err(error) = seed_oauth_bootstrap_tasks(&state, &updated).await {
        return scheduler_error_response(&error);
    }

    enqueue_upstream_audit(
        &state,
        AuditPayload::UpstreamOauthComplete {
            upstream_id: upstream_id.to_string(),
            upstream_name,
            expires_at_unix_secs: bundle.expires_at_unix_secs,
            access_token_fingerprint: access_token_fingerprint.clone(),
        },
        upstream_id,
        200,
    );

    let mut response = Json(CompleteResponse {
        upstream_id: updated.id,
        expires_at_unix_secs: bundle.expires_at_unix_secs,
        access_token_fingerprint,
    })
    .into_response();
    add_dynamic_rebind_headers(&mut response, &state).await;
    response
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

    if upstream.kind != UpstreamKind::AnthropicOauth {
        return Json(UpstreamOAuthStatusResponse {
            upstream_id: upstream.id.to_string(),
            kind: upstream.kind,
            has_credentials: false,
            status: "wrong_kind",
            expires_at_unix_secs: None,
            refresh_token_present: false,
            scopes: Vec::new(),
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
            scopes: Vec::new(),
        })
        .into_response();
    };

    match encrypted.decrypt(state.aead.as_ref(), upstream.id.as_bytes()) {
        Ok(bundle) => {
            let now = cc_lb_engine::clock::unix_secs(state.clock.now());
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
                scopes: bundle.scopes,
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
                scopes: Vec::new(),
            })
            .into_response()
        }
    }
}

fn pkce_flows() -> &'static PkceFlows {
    PKCE_FLOWS.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
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

async fn seed_oauth_bootstrap_tasks(
    state: &AdminState,
    upstream: &UpstreamRecord,
) -> Result<(), SchedulerError> {
    let Some(scheduler) = state.scheduler.as_ref() else {
        return Ok(());
    };
    let seed_secs = cc_lb_engine::clock::unix_secs(state.clock.now());
    for task in oauth_bootstrap_tasks(upstream.id, seed_secs) {
        match scheduler.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
    }
    if upstream.kind == UpstreamKind::AnthropicOauth
        && upstream.warmup_enabled
        && upstream.oauth_credentials.is_some()
        && upstream.deleted_at_unix_secs.is_none()
    {
        let task = super::upstreams::warmup_bootstrap_task(upstream.id, seed_secs);
        match scheduler.push_adaptive_task(task).await {
            Ok(()) | Err(SchedulerError::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
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

async fn rollback_oauth_draft_creation(
    storage: &dyn Storage,
    state_token: &str,
    upstream_id: Uuid,
    detail: String,
) -> Response {
    let rollback_error = UpstreamStore::hard_delete(storage, upstream_id).await.err();
    if let Ok(mut flows) = pkce_flows().lock() {
        flows.remove(state_token);
    }
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

fn enqueue_upstream_audit(
    state: &AdminState,
    payload: AuditPayload,
    upstream_id: Uuid,
    status: u16,
) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = cc_lb_engine::clock::unix_secs(state.clock.now());
    let _ = audit_sink.try_enqueue(AuditEntry {
        ts,
        request_id: format!("admin-upstream-oauth-{upstream_id}-{ts}"),
        principal_id: String::new(),
        route: "admin_v1_upstream_oauth".to_owned(),
        upstream: upstream_id.to_string(),
        model: None,
        status,
        input_tokens: None,
        output_tokens: None,
        duration_ms: 0,
        agent_label: None,
        api_key_id: None,
        cost_usd_micros: None,
        limit_violation: None,
        admin_action: Some(payload.to_string()),
        actor: Some("admin".to_owned()),
        kind: None,
        payload: None,
    });
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

fn claude_code_default_oauth() -> AnthropicOAuthConfig {
    AnthropicOAuthConfig {
        client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e".to_owned(),
        auth_url: Url::parse("https://claude.ai/oauth/authorize").expect("valid url"),
        token_url: Url::parse("https://console.anthropic.com/v1/oauth/token").expect("valid url"),
        redirect_uri: Url::parse("https://console.anthropic.com/oauth/code/callback")
            .expect("valid url"),
        scopes: vec![
            "org:create_api_key".to_owned(),
            "user:profile".to_owned(),
            "user:inference".to_owned(),
        ],
    }
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
            Some(
                "adaptive:oauth_refresh:12345678-1234-5678-1234-567812345678:bootstrap:123456"
            )
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
