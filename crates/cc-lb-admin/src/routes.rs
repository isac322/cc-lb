use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Path, Query, State},
    http::{HeaderValue, Response, StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};

use cc_lb_storage_api::{AuditQueryScope, Storage, StorageError};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    AdminState,
    audit::{AdminAuditEvent, record_admin_audit},
    auth::{AdminAction, AdminIdentity, authorize, require_admin_auth},
    principals::{principal_key_usage, principal_limits, principal_usage},
    static_assets::{serve_asset, serve_index},
};

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
        .route("/admin/v1/audit", get(query_audit))
        .route("/admin/v1/config/editor", get(get_config_editor))
        .route(
            "/admin/v1/config/draft",
            get(get_config_draft).put(put_config_draft),
        )
        .route(
            "/admin/v1/config/draft/validate",
            post(validate_config_draft),
        )
        .route("/admin/v1/config/save", post(save_config_file))
        .route(
            "/admin/v1/config/draft/download",
            post(download_config_draft),
        )
        .route("/admin/v1/config/history", get(get_config_history))
        .route(
            "/admin/v1/dashboard/summary",
            get(crate::dashboard_routes::handle_dashboard_summary),
        )
        .route(
            "/admin/v1/dashboard/usage",
            get(crate::dashboard_routes::handle_dashboard_usage),
        )
        .route(
            "/admin/v1/events/recent",
            get(crate::events_routes::handle_recent_events),
        )
        .route(
            "/admin/v1/events/histogram",
            get(crate::events_routes::handle_events_histogram),
        )
        .route(
            "/admin/events/histogram",
            get(crate::events_routes::handle_events_histogram),
        )
        .route(
            "/admin/v1/events/delta",
            get(crate::events_routes::handle_events_delta),
        )
        .route(
            "/admin/v1/events/stream",
            get(crate::events_routes::handle_events_stream),
        )
        .merge(crate::dashboard_routes::router())
        .merge(crate::events_routes::router())
        .merge(crate::events_detail_route::router())
        .merge(crate::subscription_quotas::router())
        .merge(crate::scheduler::router())
        .merge(crate::v1::plugins::router())
        .merge(crate::v1::plugins_wasm::router())
        .merge(crate::v1::oauth::router())
        .merge(crate::v1::principals::router())
        .merge(crate::v1::keys::router())
        .merge(crate::v1::router::router())
        .merge(crate::v1::status::router())
        .merge(crate::v1::upstreams::router())
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_auth,
        ))
        .layer(middleware::map_response(json_extractor_rejection));

    Router::new()
        .merge(protected_routes)
        .route("/", get(serve_index))
        .route("/{*file}", get(serve_asset))
        .route("/admin/health", get(health))
        .with_state(state)
}

async fn json_extractor_rejection(response: Response<Body>) -> Response<Body> {
    let status = response.status();
    if !matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::UNSUPPORTED_MEDIA_TYPE
            | StatusCode::UNPROCESSABLE_ENTITY
    ) || !response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/plain"))
    {
        return response;
    }

    let (parts, body) = response.into_parts();
    let message = body
        .collect()
        .await
        .map(|collected| String::from_utf8_lossy(&collected.to_bytes()).into_owned())
        .unwrap_or_else(|_| "request validation failed".to_owned());
    let field = validation_field_from_message(&message);
    let body = Json(json!({
        "error": "validation_failed",
        "field": field,
        "message": message,
    }));
    let mut response = (parts.status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn validation_field_from_message(message: &str) -> Option<String> {
    message
        .split('`')
        .nth(1)
        .filter(|field| !field.is_empty())
        .map(ToOwned::to_owned)
}

async fn get_api_key(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&identity, AdminAction::SensitiveRead).map_err(|_| StatusCode::FORBIDDEN)?;
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
    Extension(identity): Extension<AdminIdentity>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    mutate_api_key(&state, &identity, &principal_id, &key_id, false).await
}

async fn enable_api_key(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    mutate_api_key(&state, &identity, &principal_id, &key_id, true).await
}

async fn revoke_api_key(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path((principal_id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    let Some(key_store) = state.key_store.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    if key_store.revoke(&principal_id, &key_id).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let action = "principal_key_revoke";
    let route = format!("/admin/principals/{principal_id}/keys/{key_id}/revoke");
    if let Err(error) = record_admin_audit(
        &state,
        AdminAuditEvent {
            identity: Some(&identity),
            system_component: None,
            action,
            route: &route,
            target_principal_id: Some(&principal_id),
            target_upstream: None,
            api_key_id: Some(&key_id),
            status: StatusCode::OK.as_u16(),
            payload: None,
        },
    )
    .await
    {
        return audit_write_failed_response(action, &error);
    }

    Json(json!({ "status": "ok" })).into_response()
}

async fn mutate_api_key(
    state: &AdminState,
    identity: &AdminIdentity,
    principal_id: &str,
    key_id: &str,
    enable: bool,
) -> axum::response::Response {
    let Some(key_store) = state.key_store.as_ref() else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };
    let result = if enable {
        key_store.enable(principal_id, key_id).await
    } else {
        key_store.disable(principal_id, key_id).await
    };
    if result.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let operation = if enable { "enable" } else { "disable" };
    let action = if enable {
        "principal_key_enable"
    } else {
        "principal_key_disable"
    };
    let route = format!("/admin/principals/{principal_id}/keys/{key_id}/{operation}");
    if let Err(error) = record_admin_audit(
        state,
        AdminAuditEvent {
            identity: Some(identity),
            system_component: None,
            action,
            route: &route,
            target_principal_id: Some(principal_id),
            target_upstream: None,
            api_key_id: Some(key_id),
            status: StatusCode::OK.as_u16(),
            payload: None,
        },
    )
    .await
    {
        return audit_write_failed_response(action, &error);
    }

    Json(json!({ "status": "ok" })).into_response()
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
    after: Option<u64>,
    until: Option<u64>,
    limit: Option<usize>,
    actor_authority: Option<String>,
    actor_subject: Option<String>,
    #[serde(default)]
    admin_only: bool,
}

async fn query_audit(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Query(query): Query<AuditQuery>,
) -> axum::response::Response {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return dashboard_error(StatusCode::FORBIDDEN, "forbidden");
    }
    if query.actor_authority.is_some() != query.actor_subject.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "validation_failed",
                "field": "actor_subject",
                "message": "actor_authority and actor_subject must be provided together",
            })),
        )
            .into_response();
    }
    if query.principal_id.is_some() && query.actor_authority.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "validation_failed",
                "field": "principal_id",
                "message": "principal_id cannot be combined with actor identity filters",
            })),
        )
            .into_response();
    }
    let Some(storage) = &state.storage else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
    };

    let since = query.since.unwrap_or(0).max(
        query
            .after
            .map(|after| after.saturating_add(1))
            .unwrap_or(0),
    );
    let until = query.until.unwrap_or(u64::MAX);
    let limit = query.limit.unwrap_or(100).min(1000);

    let result = {
        let scope = match (
            query.actor_authority.as_deref(),
            query.actor_subject.as_deref(),
        ) {
            (Some(authority), Some(subject)) => AuditQueryScope::Actor { authority, subject },
            (None, None) => query
                .principal_id
                .as_deref()
                .map_or(AuditQueryScope::All, AuditQueryScope::Principal),
            _ => unreachable!("actor query fields were validated above"),
        };
        storage
            .query_recent_audit(scope, since, until, limit, query.admin_only)
            .await
    };
    let entries = match result {
        Ok(entries) => entries,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let action = "audit_query";
    if let Err(error) = record_admin_audit(
        &state,
        AdminAuditEvent {
            identity: Some(&identity),
            system_component: None,
            action,
            route: "/admin/v1/audit",
            target_principal_id: None,
            target_upstream: None,
            api_key_id: None,
            status: StatusCode::OK.as_u16(),
            payload: Some(json!({
                "principal_id": query.principal_id,
                "since": since,
                "until": until,
                "limit": limit,
                "actor_subject": query.actor_subject,
                "admin_only": query.admin_only,
            })),
        },
    )
    .await
    {
        return audit_write_failed_response(action, &error);
    }

    Json(json!({ "entries": entries })).into_response()
}

async fn get_config_editor(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
) -> axum::response::Response {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return dashboard_error(StatusCode::FORBIDDEN, "forbidden");
    }
    match crate::settings::editor_response(&state).await {
        Ok(response) => {
            let action = "config_editor_read";
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/editor",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: None,
                },
            )
            .await
            {
                return audit_write_failed_response(action, &error);
            }
            ([(header::CACHE_CONTROL, "no-store")], Json(response)).into_response()
        }
        Err(error) => settings_error_response(error, false),
    }
}

async fn put_config_draft(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(request): Json<crate::settings::PutConfigDraftRequest>,
) -> axum::response::Response {
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(error) => return settings_error_response(error, true),
    };
    match crate::settings::put_draft(storage, request, cc_lb_clock::unix_secs(state.clock.now()))
        .await
    {
        Ok(response) => {
            let action = "config_draft_put";
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/draft",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: None,
                },
            )
            .await
            {
                return audit_write_failed_response(action, &error);
            }
            Json(response).into_response()
        }
        Err(error) => settings_error_response(error, true),
    }
}

async fn get_config_draft(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
) -> axum::response::Response {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return dashboard_error(StatusCode::FORBIDDEN, "forbidden");
    }
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(error) => return settings_error_response(error, false),
    };
    match crate::settings::get_draft(storage, &*state.clock).await {
        Ok(response) => {
            let action = "config_draft_read";
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/draft",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: None,
                },
            )
            .await
            {
                return audit_write_failed_response(action, &error);
            }
            ([(header::CACHE_CONTROL, "no-store")], Json(response)).into_response()
        }
        Err(error) => settings_error_response(error, false),
    }
}

async fn validate_config_draft(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(request): Json<crate::settings::ValidateConfigDraftRequest>,
) -> axum::response::Response {
    match crate::settings::validate_draft(&state, request).await {
        Ok(response) => {
            let action = "config_draft_validate";
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/draft/validate",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: None,
                },
            )
            .await
            {
                return audit_write_failed_response(action, &error);
            }
            Json(response).into_response()
        }
        Err(error) => settings_error_response(error, true),
    }
}

async fn save_config_file(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(request): Json<crate::settings::SaveConfigFileRequest>,
) -> axum::response::Response {
    if authorize(&identity, AdminAction::Write).is_err() {
        return dashboard_error(StatusCode::FORBIDDEN, "forbidden");
    }
    let saved_at_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    match crate::settings::save_config_file(
        &state,
        &identity.provider_id,
        request,
        saved_at_unix_secs,
    )
    .await
    {
        Ok(response) => {
            let action = "config_save";
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/save",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: Some(json!({
                        "revision": response.revision,
                        "saved_at_unix_secs": response.saved_at_unix_secs,
                        "fingerprint": response.fingerprint.clone(),
                        "restart_required": true,
                    })),
                },
            )
            .await
            {
                tracing::error!(
                    %error,
                    action,
                    "config file was saved but its success audit could not be recorded"
                );
            }
            Json(response).into_response()
        }
        Err(error) => {
            let status = settings_error_status(&error);
            let reason = settings_error_code(&error);
            let action = "config_save_failed";
            if let Err(audit_error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/save",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: status.as_u16(),
                    payload: Some(json!({ "reason": reason })),
                },
            )
            .await
            {
                return audit_write_failed_response(action, &audit_error);
            }
            settings_error_response(error, true)
        }
    }
}

async fn download_config_draft(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Json(request): Json<crate::settings::DownloadConfigDraftRequest>,
) -> axum::response::Response {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return dashboard_error(StatusCode::FORBIDDEN, "forbidden");
    }
    match crate::settings::download_config_draft(&state, request).await {
        Ok(bytes) => {
            let action = "config_download";
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/draft/download",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: None,
                },
            )
            .await
            {
                return audit_write_failed_response(action, &error);
            }
            (
                [
                    (header::CACHE_CONTROL, "no-store"),
                    (header::CONTENT_TYPE, "application/toml; charset=utf-8"),
                    (
                        header::CONTENT_DISPOSITION,
                        "attachment; filename=\"cc-lb.toml\"",
                    ),
                ],
                bytes,
            )
                .into_response()
        }
        Err(error) => settings_error_response(error, true),
    }
}

#[derive(Deserialize)]
struct ConfigHistoryQuery {
    limit: Option<usize>,
}

async fn get_config_history(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Query(query): Query<ConfigHistoryQuery>,
) -> axum::response::Response {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return dashboard_error(StatusCode::FORBIDDEN, "forbidden");
    }
    let storage = match config_storage(&state) {
        Ok(storage) => storage,
        Err(error) => return settings_error_response(error, false),
    };
    let limit = query.limit.unwrap_or(20);
    match crate::settings::list_history(storage, limit).await {
        Ok(response) => {
            let action = "config_history_read";
            if let Err(error) = record_admin_audit(
                &state,
                AdminAuditEvent {
                    identity: Some(&identity),
                    system_component: None,
                    action,
                    route: "/admin/v1/config/history",
                    target_principal_id: None,
                    target_upstream: None,
                    api_key_id: None,
                    status: StatusCode::OK.as_u16(),
                    payload: Some(json!({ "limit": limit })),
                },
            )
            .await
            {
                return audit_write_failed_response(action, &error);
            }
            Json(response).into_response()
        }
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
        crate::settings::SettingsError::DraftNotValidated => {
            dashboard_error(StatusCode::CONFLICT, "draft_not_validated")
        }
        crate::settings::SettingsError::FileChanged {
            current_fingerprint,
        } => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "file_changed",
                "current_fingerprint": current_fingerprint,
            })),
        )
            .into_response(),
        crate::settings::SettingsError::FileNotWritable(reason) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "file_not_writable", "reason": reason })),
        )
            .into_response(),
        crate::settings::SettingsError::ConfigPathMissing => {
            dashboard_error(StatusCode::CONFLICT, "config_path_missing")
        }
        crate::settings::SettingsError::SelfLockoutConfirmationRequired => {
            dashboard_error(StatusCode::CONFLICT, "self_lockout_confirmation_required")
        }
        crate::settings::SettingsError::Validation(report) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "validation_failed", "validation": report })),
        )
            .into_response(),
        crate::settings::SettingsError::Io(source) => {
            tracing::error!(error = %source, "admin config file operation failed");
            dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "config_io_error")
        }
        crate::settings::SettingsError::Config(_) => {
            tracing::error!("admin config processing failed");
            dashboard_error(StatusCode::BAD_REQUEST, "config_invalid")
        }
        crate::settings::SettingsError::Schema(source) => {
            tracing::error!(error = %source, "admin config schema serialization failed");
            dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "schema_error")
        }
    }
}

fn settings_error_status(error: &crate::settings::SettingsError) -> StatusCode {
    match error {
        crate::settings::SettingsError::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        crate::settings::SettingsError::Storage(_)
        | crate::settings::SettingsError::Io(_)
        | crate::settings::SettingsError::Schema(_) => StatusCode::INTERNAL_SERVER_ERROR,
        crate::settings::SettingsError::StaleDraftRevision { .. }
        | crate::settings::SettingsError::DraftNotValidated
        | crate::settings::SettingsError::FileChanged { .. }
        | crate::settings::SettingsError::FileNotWritable(_)
        | crate::settings::SettingsError::ConfigPathMissing
        | crate::settings::SettingsError::SelfLockoutConfirmationRequired => StatusCode::CONFLICT,
        crate::settings::SettingsError::Validation(_)
        | crate::settings::SettingsError::Config(_) => StatusCode::BAD_REQUEST,
    }
}

fn settings_error_code(error: &crate::settings::SettingsError) -> &'static str {
    match error {
        crate::settings::SettingsError::StorageUnavailable => "storage_unavailable",
        crate::settings::SettingsError::Storage(_) => "storage_error",
        crate::settings::SettingsError::StaleDraftRevision { .. } => "stale_draft_revision",
        crate::settings::SettingsError::DraftNotValidated => "draft_not_validated",
        crate::settings::SettingsError::FileChanged { .. } => "file_changed",
        crate::settings::SettingsError::FileNotWritable(_) => "file_not_writable",
        crate::settings::SettingsError::ConfigPathMissing => "config_path_missing",
        crate::settings::SettingsError::SelfLockoutConfirmationRequired => {
            "self_lockout_confirmation_required"
        }
        crate::settings::SettingsError::Validation(_) => "validation_failed",
        crate::settings::SettingsError::Io(_) => "config_io_error",
        crate::settings::SettingsError::Config(_) => "config_invalid",
        crate::settings::SettingsError::Schema(_) => "schema_error",
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

fn audit_write_failed_response(action: &str, error: &StorageError) -> axum::response::Response {
    tracing::error!(%error, action, "admin audit write failed");
    dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "audit_write_failed")
}
