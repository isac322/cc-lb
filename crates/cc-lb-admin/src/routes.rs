use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::{delete, get, post},
};
use cc_lb_storage_api::{Storage, StorageError};
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    AdminState,
    auth::require_admin_auth,
    principals::{principal_key_usage, principal_limits, principal_usage},
};

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct Assets;

pub fn build_router(state: AdminState) -> Router {
    let protected_routes = Router::new()
        .route("/admin/principals/{id}/usage", get(principal_usage))
        .route("/admin/principals/{id}/limits", get(principal_limits))
        .route("/admin/v1/principals/{id}/usage", get(principal_usage))
        .route("/admin/v1/principals/{id}/limits", get(principal_limits))
        .route("/admin/principals/{id}/keys/{key_id}", get(get_api_key))
        .route(
            "/admin/principals/{id}/keys/{key_id}/revoke",
            post(revoke_api_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/disable",
            post(disable_api_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/enable",
            post(enable_api_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/usage",
            get(principal_key_usage),
        )
        .route("/admin/audit", get(query_audit))
        .route("/admin/status", get(crate::status::handler))
        .route("/admin/killswitch", post(set_killswitch))
        .route("/admin/killswitch", delete(clear_killswitch))
        .route("/admin/config/current", get(get_config))
        .route("/admin/config/schema", get(get_config_schema))
        .route(
            "/admin/config/draft",
            get(get_config_draft).put(put_config_draft),
        )
        .route("/admin/config/draft/validate", post(validate_config_draft))
        .route("/admin/config/apply", post(apply_config_draft))
        .route("/admin/config/history", get(get_config_history))
        .route("/admin/config/diff", get(get_config_diff))
        .route("/admin/config/reload", post(reload_config))
        .merge(crate::dashboard_routes::router())
        .merge(crate::events_routes::router())
        .merge(crate::credentials::router())
        .merge(crate::v1::plugins::router())
        .merge(crate::v1::plugins_wasm::router())
        .merge(crate::v1::oauth::router())
        .merge(crate::v1::principals::router())
        .merge(crate::v1::keys::router())
        .merge(crate::v1::status::router())
        .merge(crate::v1::upstreams::router())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_auth,
        ));

    Router::new()
        .merge(protected_routes)
        .route("/", get(serve_index))
        .route("/{*file}", get(serve_asset))
        .route("/admin/health", get(health))
        .with_state(state)
}

async fn get_api_key(
    State(state): State<AdminState>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let key_store = state
        .key_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;
    let record = key_store
        .get(&principal_id, &key_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(json!({
        "id": key_id,
        "principal_id": principal_id,
        "label": record.label,
        "status": record.status,
        "last_4": record.last_4,
        "expires_at_unix_secs": record.expires_at_unix_secs,
    })))
}

async fn disable_api_key(
    State(state): State<AdminState>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    mutate_api_key(&state, &principal_id, &key_id, false).await
}

async fn enable_api_key(
    State(state): State<AdminState>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    mutate_api_key(&state, &principal_id, &key_id, true).await
}

async fn revoke_api_key(
    State(state): State<AdminState>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let key_store = state
        .key_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;
    key_store
        .revoke(&principal_id, &key_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "status": "ok" })))
}

async fn mutate_api_key(
    state: &AdminState,
    principal_id: &str,
    key_id: &str,
    enable: bool,
) -> Result<Json<Value>, StatusCode> {
    let key_store = state
        .key_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;
    let result = if enable {
        key_store.enable(principal_id, key_id).await
    } else {
        key_store.disable(principal_id, key_id).await
    };
    result.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "status": "ok" })))
}

async fn serve_index() -> impl IntoResponse {
    serve_asset(Path("index.html".to_string())).await
}

async fn serve_asset(Path(file): Path<String>) -> impl IntoResponse {
    if let Some(content) = Assets::get(&file) {
        let mime = mime_guess::from_path(&file).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response();
    }
    if file.starts_with("admin/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    match Assets::get("index.html") {
        Some(content) => (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            content.data,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn health(State(state): State<AdminState>) -> Json<Value> {
    let uptime = state.start_time.elapsed().as_secs();
    let version = env!("CARGO_PKG_VERSION");
    let git_sha = option_env!("CC_LB_GIT_SHA").unwrap_or("unknown");

    Json(json!({
        "status": "ok",
        "version": version,
        "git_sha": git_sha,
        "uptime_secs": uptime,
    }))
}

#[derive(Deserialize)]
struct AuditQuery {
    principal_id: Option<String>,
    since: Option<u64>,
    until: Option<u64>,
    limit: Option<usize>,
}

async fn query_audit(
    State(state): State<AdminState>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<Value>, StatusCode> {
    let Some(storage) = &state.storage else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };

    let since = query.since.unwrap_or(0);
    let until = query.until.unwrap_or(u64::MAX);
    let limit = query.limit.unwrap_or(100).min(1000);

    let entries = storage
        .query_audit(query.principal_id.as_deref(), since, until, limit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({ "entries": entries })))
}

async fn set_killswitch(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    let Some(storage) = &state.storage else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };
    storage
        .set_killswitch_enabled(true)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "status": "ok", "killswitch": true })))
}

async fn clear_killswitch(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    let Some(storage) = &state.storage else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };
    storage
        .set_killswitch_enabled(false)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "status": "ok", "killswitch": false })))
}

async fn get_config(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    let config = state.config.current_config();
    let mut config_json =
        serde_json::to_value(&*config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(storage) = config_json.get_mut("storage")
        && let Some(obj) = storage.as_object_mut()
    {
        obj.insert("oauth_aead_key_env".to_string(), json!("[REDACTED]"));
    }
    if let Some(admin) = config_json.get_mut("admin")
        && let Some(obj) = admin.as_object_mut()
    {
        obj.insert("token_env".to_string(), json!("[REDACTED]"));
    }
    Ok(Json(config_json))
}

async fn put_config_draft(
    State(state): State<AdminState>,
    Json(request): Json<crate::settings::PutConfigDraftRequest>,
) -> axum::response::Response {
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(error) => return settings_error_response(error, true),
    };
    let draft_value = request.draft.clone();
    match crate::settings::put_draft(storage, request, unix_now_secs()).await {
        Ok(response) => {
            if let Ok(config) = serde_json::from_value::<cc_lb_config::Config>(draft_value) {
                let _ = state.config.put_draft_config(config);
            }
            Json(response).into_response()
        }
        Err(error) => settings_error_response(error, true),
    }
}

async fn apply_config_draft(State(state): State<AdminState>) -> axum::response::Response {
    match state.config.apply_draft_config() {
        Ok(_config) => Json(json!({
            "status": "applied",
        }))
        .into_response(),
        Err(error) => config_draft_error_response(error),
    }
}

fn config_draft_error_response(error: crate::ConfigDraftError) -> axum::response::Response {
    let status = match error {
        crate::ConfigDraftError::Unavailable => StatusCode::NOT_IMPLEMENTED,
        crate::ConfigDraftError::MissingDraft => StatusCode::NOT_FOUND,
        crate::ConfigDraftError::Invalid(_) => StatusCode::BAD_REQUEST,
    };
    (
        status,
        Json(json!({ "error": { "message": error.to_string() } })),
    )
        .into_response()
}

async fn get_config_schema() -> axum::response::Response {
    match crate::settings::schema_response() {
        Ok(response) => ([(header::CACHE_CONTROL, "max-age=60")], Json(response)).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

async fn get_config_draft(State(state): State<AdminState>) -> axum::response::Response {
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(error) => return settings_error_response(error, false),
    };
    match crate::settings::get_draft(storage).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

async fn validate_config_draft(
    State(state): State<AdminState>,
    Json(request): Json<crate::settings::ValidateConfigDraftRequest>,
) -> axum::response::Response {
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(crate::settings::SettingsError::StorageUnavailable) => {
            return Json(crate::settings::ValidateConfigDraftResponse {
                valid: false,
                revision: request.expected_revision,
                error: Some("draft_missing".to_owned()),
            })
            .into_response();
        }
        Err(error) => return settings_error_response(error, false),
    };
    match crate::settings::validate_draft(storage, request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

#[derive(Deserialize)]
struct ConfigHistoryQuery {
    limit: Option<usize>,
}

async fn get_config_history(
    State(state): State<AdminState>,
    Query(query): Query<ConfigHistoryQuery>,
) -> axum::response::Response {
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(error) => return settings_error_response(error, false),
    };
    match crate::settings::list_history(storage, query.limit.unwrap_or(20)).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

#[derive(Deserialize)]
struct ConfigDiffQuery {
    from_revision: u64,
    to_revision: u64,
}

async fn get_config_diff(
    State(state): State<AdminState>,
    Query(query): Query<ConfigDiffQuery>,
) -> axum::response::Response {
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(error) => return settings_error_response(error, false),
    };
    match crate::settings::diff_history(storage, query.from_revision, query.to_revision).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

fn config_storage(state: &AdminState) -> Result<&dyn Storage, crate::settings::SettingsError> {
    state
        .storage
        .as_deref()
        .ok_or(crate::settings::SettingsError::StorageUnavailable)
}

fn settings_error_response(
    error: crate::settings::SettingsError,
    include_current_revision: bool,
) -> axum::response::Response {
    match error {
        crate::settings::SettingsError::StorageUnavailable => {
            storage_unavailable_response("storage_unavailable")
        }
        crate::settings::SettingsError::Storage(source) => {
            tracing::error!(error = %source, "admin config storage operation failed");
            storage_error_response(&source, "storage_error")
        }
        crate::settings::SettingsError::StaleDraftRevision { current } => {
            if include_current_revision {
                (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "stale_draft_revision", "current_revision": current })),
                )
                    .into_response()
            } else {
                dashboard_error(StatusCode::CONFLICT, "stale_draft_revision")
            }
        }
        crate::settings::SettingsError::UnvalidatedRevision => {
            dashboard_error(StatusCode::CONFLICT, "unvalidated_revision")
        }
        crate::settings::SettingsError::ValidationFailed { detail } => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "validation_failed", "detail": detail })),
        )
            .into_response(),
        crate::settings::SettingsError::ConfigPathMissing => {
            dashboard_error(StatusCode::SERVICE_UNAVAILABLE, "config_path_missing")
        }
        crate::settings::SettingsError::ConfigWatcherMissing => {
            dashboard_error(StatusCode::SERVICE_UNAVAILABLE, "config_watcher_missing")
        }
        crate::settings::SettingsError::ApplyWriteFailed { detail } => {
            tracing::error!(error = %detail, "admin config apply write failed");
            dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "apply_write_failed")
        }
        crate::settings::SettingsError::ReloadFailed { detail } => {
            tracing::error!(error = %detail, "admin config apply reload failed");
            dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "reload_failed")
        }
        crate::settings::SettingsError::UnknownRevision { missing } => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown_revision", "missing": missing })),
        )
            .into_response(),
        crate::settings::SettingsError::Schema(source) => {
            tracing::error!(error = %source, "admin config schema serialization failed");
            dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "schema_error")
        }
    }
}

fn storage_error_response(source: &StorageError, error: &str) -> axum::response::Response {
    tracing::error!(%source, "admin storage operation failed");
    dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, error)
}

fn storage_unavailable_response(error: &str) -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        Json(json!({ "error": error })),
    )
        .into_response()
}

fn dashboard_error(status: StatusCode, error: &str) -> axum::response::Response {
    (status, Json(json!({ "error": error }))).into_response()
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn reload_config(State(_state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    #[cfg(unix)]
    {
        if let Err(e) =
            nix::sys::signal::kill(nix::unistd::Pid::this(), nix::sys::signal::Signal::SIGHUP)
        {
            tracing::error!("failed to send SIGHUP: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
    Ok(Json(json!({ "status": "ok", "reloading": true })))
}
