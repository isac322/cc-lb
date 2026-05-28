use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cc_lb_aead::OAuthTokenBundle;
use cc_lb_core::{AuditEntry, AuditPayload};
use cc_lb_storage_api::{Storage, StorageError, UpstreamStore};
use cc_lb_storage_api::upstream::UpstreamKind;
use oauth2::{AuthUrl, ClientId, TokenUrl};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::AdminState;
use crate::oauth_pkce::{
    HyperOAuthHttpClient, PkceHandshakeState, complete_pkce_flow, start_pkce_flow,
};

type PkceFlows = Arc<Mutex<HashMap<String, InFlightPkce>>>;

static PKCE_FLOWS: OnceLock<PkceFlows> = OnceLock::new();

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/upstreams/{id}/oauth/start", post(start_oauth))
        .route(
            "/admin/v1/upstreams/{id}/oauth/complete",
            post(complete_oauth),
        )
}

#[derive(Deserialize)]
struct StartRequest {}

#[derive(Serialize)]
struct StartResponse {
    authorize_url: String,
    state_token: String,
}

#[derive(Deserialize)]
struct CompleteRequest {
    state_token: String,
    code: String,
}

#[derive(Serialize)]
struct CompleteResponse {
    upstream_id: Uuid,
    expires_at_unix_secs: u64,
    access_token_fingerprint: String,
}

#[derive(Clone)]
struct InFlightPkce {
    upstream_id: Uuid,
    upstream_name: String,
    expected_revision: u64,
    handshake: PkceHandshakeState,
    created_at_unix_secs: u64,
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
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return storage_error_response(&error),
    };
    if upstream.kind != UpstreamKind::AnthropicOauth {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "wrong_kind" })),
        )
            .into_response();
    }

    let config = state.config.current_config();
    let Some(oauth) = config.oauth.anthropic.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "oauth_not_configured" })),
        )
            .into_response();
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

    let in_flight = InFlightPkce {
        upstream_id,
        upstream_name: upstream.name.clone(),
        expected_revision: upstream.revision,
        handshake: handshake_state.clone(),
        created_at_unix_secs: now_unix_secs(),
    };
    // M-R5: redb-backed runtime management is single-process, so v1 PKCE state is in-process.
    match pkce_flows().lock() {
        Ok(mut flows) => {
            let now = now_unix_secs();
            flows.retain(|_, flow| now.saturating_sub(flow.created_at_unix_secs) < 900);
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
    })
    .into_response()
}

async fn complete_oauth(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
    Json(payload): Json<CompleteRequest>,
) -> Response {
    let decoded = match decode_state(&payload.state_token) {
        Ok(decoded) => decoded,
        Err(_) => return invalid_state_response(),
    };
    if decoded.upstream_id != upstream_id {
        return invalid_state_response();
    }
    let in_flight = match pkce_flows().lock() {
        Ok(mut flows) => flows.remove(&payload.state_token),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let Some(in_flight) = in_flight else {
        return invalid_state_response();
    };
    if in_flight.upstream_id != upstream_id {
        return invalid_state_response();
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
            Json(json!({ "error": "wrong_kind" })),
        )
            .into_response();
    }

    let handshake = match in_flight.handshake.into_handshake() {
        Ok(handshake) => handshake,
        Err(_) => return invalid_state_response(),
    };
    let credentials = match complete_pkce_flow(
        handshake,
        payload.code,
        Arc::new(HyperOAuthHttpClient::new()),
    )
    .await
    {
        Ok(credentials) => credentials,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid_grant" })),
            )
                .into_response();
        }
    };

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
        .store_oauth_tokens(upstream_id, in_flight.expected_revision, encrypted)
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

    enqueue_upstream_audit(
        &state,
        AuditPayload::UpstreamOauthComplete {
            upstream_id: upstream_id.to_string(),
            upstream_name: in_flight.upstream_name,
            expires_at_unix_secs: bundle.expires_at_unix_secs,
            access_token_fingerprint: access_token_fingerprint.clone(),
        },
        upstream_id,
        200,
    );

    Json(CompleteResponse {
        upstream_id: updated.id,
        expires_at_unix_secs: bundle.expires_at_unix_secs,
        access_token_fingerprint,
    })
    .into_response()
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

fn invalid_state_response() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": "invalid_state" })),
    )
        .into_response()
}

fn storage_error_response(error: &StorageError) -> Response {
    match error {
        StorageError::Unavailable { .. } | StorageError::Transient { .. } => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        StorageError::Conflict { .. } => {
            (StatusCode::CONFLICT, Json(json!({ "error": "conflict" }))).into_response()
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
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
    let ts = now_unix_secs();
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
    });
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
