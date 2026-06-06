use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ETAG, IF_MATCH},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cc_lb_aead::{AeadEncryptedField, OAuthTokenBundle};
use cc_lb_core::anthropic_compat::{
    CLAUDE_CODE_STABLE_VERSION_FALLBACK, CLAUDE_CODE_STABLE_VERSION_KEY, claude_code_user_agent,
};
use cc_lb_core::{AuditEntry, AuditPayload, make_metadata_http_client, run_metadata_refresh};
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    OrganizationMetadataRecord, Storage, StorageError, UpstreamCreate,
    UpstreamRecord, UpstreamStore, UpstreamSubscriptionMetadataRecord, UpstreamUpdate,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::add_dynamic_rebind_headers;
use crate::AdminState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const STORE_PAGE_LIMIT: usize = 1000;
const METADATA_REFRESH_LOOKAHEAD_SECS: u64 = 60;
const METADATA_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

pub fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/upstreams",
            post(create_upstream).get(list_upstreams),
        )
        .route(
            "/admin/v1/upstreams/{id}",
            get(get_upstream)
                .put(update_upstream)
                .delete(delete_upstream),
        )
        .route(
            "/admin/v1/upstreams/{id}/subscription-metadata",
            get(get_upstream_subscription_metadata),
        )
        .route(
            "/admin/v1/upstreams/{id}/subscription-metadata/refresh",
            post(refresh_upstream_subscription_metadata),
        )
        .route("/admin/v1/upstreams/{id}/enable", post(enable_upstream))
        .route("/admin/v1/upstreams/{id}/disable", post(disable_upstream))
}

#[derive(Debug, Deserialize)]
struct UpstreamCreateBody {
    name: String,
    kind: UpstreamKind,
    #[serde(default)]
    base_url: Option<Url>,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    api_key_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpstreamUpdateBody {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    base_url: Option<Url>,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    api_key_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct UpstreamResponse {
    id: String,
    name: String,
    kind: UpstreamKind,
    enabled: bool,
    revision: u64,
}

#[derive(Debug, Serialize)]
struct SubscriptionMetadataResponse {
    upstream_id: String,
    subscription_metadata: Option<UpstreamSubscriptionMetadataRecord>,
    organization_metadata: Option<OrganizationMetadataRecord>,
}

enum UpstreamError {
    StorageUnavailable,
    NotFound,
    BadRequest { error: &'static str, detail: String },
    MissingIfMatch,
    StaleRevision { current_revision: u64 },
    Conflict { detail: String },
    NotOauthUpstream,
    CredentialDecrypt,
    RefreshUnavailable,
    RefreshFailed { detail: String },
    MetadataRefreshTimeout,
    Storage(StorageError),
}

impl IntoResponse for UpstreamError {
    fn into_response(self) -> Response {
        match self {
            Self::StorageUnavailable => (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({ "error": "storage_unavailable" })),
            )
                .into_response(),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "upstream_not_found" })),
            )
                .into_response(),
            Self::BadRequest { error, detail } => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": error, "detail": detail })),
            )
                .into_response(),
            Self::MissingIfMatch => (
                StatusCode::PRECONDITION_REQUIRED,
                Json(json!({ "error": "if_match_required" })),
            )
                .into_response(),
            Self::StaleRevision { current_revision } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "stale_revision", "current_revision": current_revision })),
            )
                .into_response(),
            Self::Conflict { detail } => (
                StatusCode::CONFLICT,
                Json(json!({ "error": "conflict", "detail": detail })),
            )
                .into_response(),
            Self::NotOauthUpstream => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "not_oauth_upstream" })),
            )
                .into_response(),
            Self::CredentialDecrypt => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "oauth_credential_decrypt_failed" })),
            )
                .into_response(),
            Self::RefreshUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "oauth_refresh_unavailable" })),
            )
                .into_response(),
            Self::RefreshFailed { detail } => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "oauth_refresh_failed", "detail": detail })),
            )
                .into_response(),
            Self::MetadataRefreshTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({ "error": "metadata_refresh_timeout" })),
            )
                .into_response(),
            Self::Storage(error) => {
                tracing::error!(error = %error, "admin v1 upstream storage operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

impl From<StorageError> for UpstreamError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Conflict { message } => Self::Conflict { detail: message },
            other => Self::Storage(other),
        }
    }
}

async fn create_upstream(
    State(state): State<AdminState>,
    Json(body): Json<UpstreamCreateBody>,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let api_key_ciphertext = api_key_ciphertext_for_create(&state, &body)?;
    let kind = body.kind;
    let created = UpstreamStore::create(
        storage,
        UpstreamCreate {
            name: body.name,
            kind,
            base_url: body.base_url,
            api_key_ciphertext,
        },
    )
    .await?;
    enqueue_upstream_audit(
        &state,
        &created,
        AuditPayload::UpstreamCreate {
            upstream_id: created.id.to_string(),
            kind: created.kind.as_str().to_owned(),
        },
    );

    let mut response = (StatusCode::CREATED, Json(upstream_response(&created))).into_response();
    response.headers_mut().insert(
        axum::http::header::LOCATION,
        HeaderValue::from_str(&format!("/admin/v1/upstreams/{}", created.id)).map_err(|error| {
            UpstreamError::BadRequest {
                error: "invalid_location",
                detail: error.to_string(),
            }
        })?,
    );
    add_dynamic_rebind_headers(&mut response, &state).await;
    Ok(response)
}

async fn list_upstreams(
    State(state): State<AdminState>,
    Query(query): Query<ListQuery>,
) -> Result<Response, UpstreamError> {
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let all = list_all_upstreams(&state).await?;
    let total = all.len();
    let page = all
        .into_iter()
        .filter(|record| {
            query
                .after
                .as_ref()
                .is_none_or(|after| record.id.to_string() > *after)
        })
        .take(limit)
        .map(|record| upstream_response(&record))
        .collect::<Vec<_>>();

    let mut response = Json(json!({ "upstreams": page })).into_response();
    response.headers_mut().insert(
        "X-Total-Count",
        HeaderValue::from_str(&total.to_string()).map_err(|error| UpstreamError::BadRequest {
            error: "invalid_total_count",
            detail: error.to_string(),
        })?,
    );
    Ok(response)
}

async fn get_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, UpstreamError> {
    let record = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    let mut response = Json(upstream_response(&record)).into_response();
    response
        .headers_mut()
        .insert(axum::http::header::ETAG, etag_value(record.revision)?);
    Ok(response)
}

async fn get_upstream_subscription_metadata(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<SubscriptionMetadataResponse>, UpstreamError> {
    let storage = storage(&state)?;
    let upstream_id = id
        .parse::<Uuid>()
        .map_err(|error| UpstreamError::BadRequest {
            error: "invalid_upstream_id",
            detail: error.to_string(),
        })?;
    subscription_metadata_response(storage, upstream_id)
        .await
        .map(Json)
}

async fn refresh_upstream_subscription_metadata(
    State(state): State<AdminState>,
    Path(upstream_id): Path<Uuid>,
) -> Result<Json<SubscriptionMetadataResponse>, UpstreamError> {
    let storage = storage_arc(&state)?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream_id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if upstream.kind != UpstreamKind::AnthropicOauth || upstream.oauth_credentials.is_none() {
        return Err(UpstreamError::NotOauthUpstream);
    }

    let bundle = decrypt_oauth_bundle(&state, &upstream)?;
    let access_token =
        fresh_enough_access_token(&state, storage.clone(), &upstream, bundle).await?;
    let user_agent = metadata_user_agent(storage.as_ref()).await?;
    let client = make_metadata_http_client();
    let cancel = CancellationToken::new();
    tokio::time::timeout(
        METADATA_REFRESH_TIMEOUT,
        run_metadata_refresh(
            storage.clone(),
            &client,
            upstream_id,
            &access_token,
            &user_agent,
            &cancel,
        ),
    )
    .await
    .map_err(|_| UpstreamError::MetadataRefreshTimeout)?
    .map_err(|error| UpstreamError::RefreshFailed {
        detail: error.to_string(),
    })?;

    subscription_metadata_response(storage.as_ref(), upstream_id)
        .await
        .map(Json)
}

async fn update_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<UpstreamUpdateBody>,
) -> Result<Response, UpstreamError> {
    let expected_revision = parse_if_match(&headers)?;
    let current = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        return Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        });
    }

    let fields_changed = changed_fields(&body);
    let api_key_ciphertext = api_key_ciphertext_for_update(&state, &current, &body)?;
    let storage = storage(&state)?;
    match UpstreamStore::update(
        storage,
        current.id,
        expected_revision,
        UpstreamUpdate {
            name: body.name,
            base_url: body.base_url,
            api_key_ciphertext,
        },
    )
    .await
    {
        Ok(updated) => {
            enqueue_upstream_audit(
                &state,
                &updated,
                AuditPayload::UpstreamUpdate {
                    upstream_id: updated.id.to_string(),
                    fields_changed,
                },
            );
            let mut response = Json(upstream_response(&updated)).into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            Ok(response)
        }
        Err(StorageError::Conflict { message }) => {
            stale_or_conflict(&state, &id, expected_revision, message).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn enable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, true).await
}

async fn disable_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    set_enabled(state, id, headers, false).await
}

async fn delete_upstream(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, UpstreamError> {
    let expected_revision = parse_if_match(&headers)?;
    let current = find_upstream(&state, &id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        return Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        });
    }

    let storage = storage(&state)?;
    match UpstreamStore::soft_delete(storage, current.id, expected_revision).await {
        Ok(()) => {
            enqueue_upstream_audit(
                &state,
                &current,
                AuditPayload::UpstreamDelete {
                    upstream_id: current.id.to_string(),
                },
            );
            let mut response = StatusCode::NO_CONTENT.into_response();
            add_dynamic_rebind_headers(&mut response, &state).await;
            Ok(response)
        }
        Err(StorageError::Conflict { message }) => {
            stale_or_conflict(&state, &id, expected_revision, message).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn set_enabled(
    state: AdminState,
    id: String,
    headers: HeaderMap,
    enabled: bool,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let expected_revision = parse_if_match(&headers)?;
    let Ok(id) = id.parse() else {
        return Err(UpstreamError::BadRequest {
            error: "invalid_upstream_id",
            detail: "invalid uuid".to_owned(),
        });
    };

    let updated = match UpstreamStore::set_enabled(storage, id, expected_revision, enabled).await {
        Ok(updated) => updated,
        Err(StorageError::Conflict { message }) => {
            return stale_or_conflict(&state, &id.to_string(), expected_revision, message).await;
        }
        Err(error) => return Err(error.into()),
    };

    enqueue_upstream_audit(
        &state,
        &updated,
        if enabled {
            AuditPayload::UpstreamEnable {
                upstream_id: updated.id.to_string(),
            }
        } else {
            AuditPayload::UpstreamDisable {
                upstream_id: updated.id.to_string(),
            }
        },
    );

    let mut response = respond_with_etag(updated);
    crate::v1::add_dynamic_rebind_headers(&mut response, &state).await;
    Ok(response)
}

async fn stale_or_conflict(
    state: &AdminState,
    id: &str,
    expected_revision: u64,
    detail: String,
) -> Result<Response, UpstreamError> {
    let current = find_upstream(state, id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    if current.revision != expected_revision {
        Err(UpstreamError::StaleRevision {
            current_revision: current.revision,
        })
    } else {
        Err(UpstreamError::Conflict { detail })
    }
}

fn respond_with_etag(record: UpstreamRecord) -> Response {
    let mut response = Json(upstream_response(&record)).into_response();
    if let Ok(etag) = etag_value(record.revision) {
        response.headers_mut().insert(ETAG, etag);
    }
    response
}

fn storage(state: &AdminState) -> Result<&dyn Storage, UpstreamError> {
    state
        .storage
        .as_deref()
        .ok_or(UpstreamError::StorageUnavailable)
}

fn storage_arc(state: &AdminState) -> Result<Arc<dyn Storage>, UpstreamError> {
    state
        .storage
        .clone()
        .ok_or(UpstreamError::StorageUnavailable)
}

async fn subscription_metadata_response(
    storage: &dyn Storage,
    upstream_id: Uuid,
) -> Result<SubscriptionMetadataResponse, UpstreamError> {
    let subscription_metadata = storage
        .get_upstream_subscription_metadata(upstream_id)
        .await?;
    let organization_metadata = match subscription_metadata
        .as_ref()
        .and_then(|metadata| metadata.organization_uuid.as_deref())
    {
        Some(organization_uuid) => storage.get_organization_metadata(organization_uuid).await?,
        None => None,
    };
    Ok(SubscriptionMetadataResponse {
        upstream_id: upstream_id.to_string(),
        subscription_metadata,
        organization_metadata,
    })
}

async fn fresh_enough_access_token(
    state: &AdminState,
    storage: Arc<dyn Storage>,
    upstream: &UpstreamRecord,
    bundle: OAuthTokenBundle,
) -> Result<String, UpstreamError> {
    let now = unix_now_secs();
    if bundle.expires_at_unix_secs > now.saturating_add(METADATA_REFRESH_LOOKAHEAD_SECS) {
        return Ok(bundle.access_token);
    }
    let refresher = state
        .lazy_refresher
        .as_ref()
        .ok_or(UpstreamError::RefreshUnavailable)?;
    refresher
        .refresh_one(upstream.id)
        .await
        .map_err(|error| UpstreamError::RefreshFailed {
            detail: error.to_string(),
        })?;
    let upstream = UpstreamStore::get_by_id(storage.as_ref(), upstream.id)
        .await?
        .ok_or(UpstreamError::NotFound)?;
    Ok(decrypt_oauth_bundle(state, &upstream)?.access_token)
}

fn decrypt_oauth_bundle(
    state: &AdminState,
    upstream: &UpstreamRecord,
) -> Result<OAuthTokenBundle, UpstreamError> {
    upstream
        .oauth_credentials
        .as_ref()
        .ok_or(UpstreamError::NotOauthUpstream)?
        .decrypt(state.aead.as_ref(), upstream.id.as_bytes())
        .map_err(|_| UpstreamError::CredentialDecrypt)
}

async fn metadata_user_agent(storage: &dyn Storage) -> Result<String, UpstreamError> {
    let version = storage
        .get_compatibility_kv(CLAUDE_CODE_STABLE_VERSION_KEY)
        .await?
        .map(|record| record.value)
        .unwrap_or_else(|| CLAUDE_CODE_STABLE_VERSION_FALLBACK.to_owned());
    Ok(claude_code_user_agent(&version))
}

fn upstream_response(record: &UpstreamRecord) -> UpstreamResponse {
    UpstreamResponse {
        id: record.id.to_string(),
        name: record.name.clone(),
        kind: record.kind,
        enabled: record.enabled,
        revision: record.revision,
    }
}

fn parse_if_match(headers: &HeaderMap) -> Result<u64, UpstreamError> {
    let value = headers.get(IF_MATCH).ok_or(UpstreamError::MissingIfMatch)?;
    let raw = value.to_str().map_err(|error| UpstreamError::BadRequest {
        error: "invalid_if_match",
        detail: error.to_string(),
    })?;
    let trimmed = raw.trim();
    let revision = trimmed
        .strip_prefix("W/\"")
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            trimmed
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
        })
        .unwrap_or(trimmed);
    revision
        .parse::<u64>()
        .map_err(|error| UpstreamError::BadRequest {
            error: "invalid_if_match",
            detail: error.to_string(),
        })
}

fn etag_value(revision: u64) -> Result<HeaderValue, UpstreamError> {
    HeaderValue::from_str(&format!("W/\"{revision}\"")).map_err(|error| UpstreamError::BadRequest {
        error: "invalid_etag",
        detail: error.to_string(),
    })
}

fn api_key_ciphertext_for_create(
    state: &AdminState,
    body: &UpstreamCreateBody,
) -> Result<Option<Vec<u8>>, UpstreamError> {
    let plaintext = body
        .api_key_value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let env_name = body
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match body.kind {
        UpstreamKind::AnthropicApiKey => match (plaintext, env_name) {
            (Some(_), Some(_)) => Err(UpstreamError::BadRequest {
                error: "conflicting_api_key",
                detail: "provide either api_key_value or api_key_env, not both".to_owned(),
            }),
            (Some(value), None) => {
                encrypt_plaintext_value(state, value, body.name.as_bytes()).map(Some)
            }
            (None, Some(env)) => encrypt_env_value(state, env, body.name.as_bytes()).map(Some),
            (None, None) => Err(UpstreamError::BadRequest {
                error: "missing_api_key",
                detail: "anthropic_api_key upstreams require api_key_value or api_key_env"
                    .to_owned(),
            }),
        },
        UpstreamKind::AnthropicOauth => {
            if plaintext.is_some() || env_name.is_some() {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key",
                    detail: "anthropic_oauth upstreams do not accept api_key_value or api_key_env"
                        .to_owned(),
                });
            }
            Ok(None)
        }
    }
}

fn api_key_ciphertext_for_update(
    state: &AdminState,
    current: &UpstreamRecord,
    body: &UpstreamUpdateBody,
) -> Result<Option<Vec<u8>>, UpstreamError> {
    let plaintext = body
        .api_key_value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let env_name = body
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match (plaintext, env_name) {
        (Some(_), Some(_)) => Err(UpstreamError::BadRequest {
            error: "conflicting_api_key",
            detail: "provide either api_key_value or api_key_env, not both".to_owned(),
        }),
        (None, None) => Ok(None),
        (value_opt, env_opt) => {
            if !matches!(current.kind, UpstreamKind::AnthropicApiKey) {
                return Err(UpstreamError::BadRequest {
                    error: "unexpected_api_key",
                    detail: format!(
                        "{} upstreams do not accept api_key_value or api_key_env",
                        current.kind.as_str()
                    ),
                });
            }
            if let Some(value) = value_opt {
                encrypt_plaintext_value(state, value, current.id.as_bytes()).map(Some)
            } else if let Some(env) = env_opt {
                encrypt_env_value(state, env, current.id.as_bytes()).map(Some)
            } else {
                Ok(None)
            }
        }
    }
}

fn encrypt_env_value(
    state: &AdminState,
    env_name: &str,
    aad: &[u8],
) -> Result<Vec<u8>, UpstreamError> {
    let value = std::env::var(env_name).map_err(|error| UpstreamError::BadRequest {
        error: "missing_api_key_env_value",
        detail: format!("{env_name}: {error}"),
    })?;
    encrypt_plaintext_value(state, &value, aad)
}

fn encrypt_plaintext_value(
    state: &AdminState,
    plaintext: &str,
    aad: &[u8],
) -> Result<Vec<u8>, UpstreamError> {
    let value = plaintext.to_owned();
    let encrypted = AeadEncryptedField::<String>::encrypt(state.aead.as_ref(), &value, aad)
        .map_err(|error| UpstreamError::BadRequest {
            error: "credential_encryption_failed",
            detail: error.to_string(),
        })?;
    Ok(encrypted.ciphertext().to_vec())
}

fn changed_fields(body: &UpstreamUpdateBody) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if body.name.is_some() {
        fields.push("name");
    }
    if body.base_url.is_some() {
        fields.push("base_url");
    }
    if body.api_key_env.is_some() || body.api_key_value.is_some() {
        fields.push("api_key_ciphertext");
    }
    fields
}

async fn find_upstream(
    state: &AdminState,
    id: &str,
) -> Result<Option<UpstreamRecord>, UpstreamError> {
    Ok(list_all_upstreams(state)
        .await?
        .into_iter()
        .find(|record| record.id.to_string() == id))
}

async fn list_all_upstreams(state: &AdminState) -> Result<Vec<UpstreamRecord>, UpstreamError> {
    let storage = storage(state)?;
    let mut after = None;
    let mut all = Vec::new();
    loop {
        let page = UpstreamStore::list(storage, after, STORE_PAGE_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        let page_len = page.len();
        all.extend(page);
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    Ok(all)
}

fn enqueue_upstream_audit(state: &AdminState, upstream: &UpstreamRecord, payload: AuditPayload) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = unix_now_secs();
    let action = payload.to_string();
    let mut entry: AuditEntry = payload.into();
    entry.ts = ts;
    entry.request_id = format!("admin-upstream-{}-{ts}", upstream.id);
    entry.principal_id = "admin".to_owned();
    entry.route = "admin_v1_upstreams".to_owned();
    entry.upstream = upstream.name.clone();
    entry.status = 200;
    entry.duration_ms = 0;
    entry.actor = Some("admin".to_owned());
    entry.admin_action = Some(action);
    let _ = audit_sink.try_enqueue(entry);
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
