use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use bytes::Bytes;
use cc_lb_config::Config;
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    AdminState,
    auth::require_admin_auth,
    management,
    principals::{principal_key_usage, principal_limits},
};

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct Assets;

pub fn build_router(state: AdminState) -> Router {
    let protected_routes = Router::new()
        .route("/admin/principals", get(list_principals))
        .route("/admin/principals/{id}/limits", get(principal_limits))
        .route(
            "/admin/principals/{id}/keys",
            get(list_principal_keys).post(issue_principal_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}",
            get(get_principal_key).patch(update_principal_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/revoke",
            post(revoke_principal_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/disable",
            post(disable_principal_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/enable",
            post(enable_principal_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/usage",
            get(principal_key_usage),
        )
        .route("/admin/audit", get(query_audit))
        .route("/admin/upstreams", get(list_upstreams))
        .route("/admin/upstreams/{name}/drain", post(drain_upstream))
        .route("/admin/killswitch", post(set_killswitch))
        .route("/admin/killswitch", delete(clear_killswitch))
        .route("/admin/oauth/{id}", get(crate::oauth::oauth_status))
        .route("/admin/oauth/start", post(crate::oauth::start_oauth))
        .route("/admin/oauth/complete", post(crate::oauth::complete_oauth))
        .route("/admin/config/current", get(get_config))
        .route("/admin/config/draft", put(put_config_draft))
        .route("/admin/config/apply", post(apply_config_draft))
        .route("/admin/config/reload", post(reload_config))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_auth,
        ));

    Router::new()
        .route("/", get(serve_index))
        .route("/{*file}", get(serve_asset))
        .route("/admin/health", get(health))
        .merge(protected_routes)
        .with_state(state)
}

async fn serve_index() -> impl IntoResponse {
    serve_asset(Path("index.html".to_string())).await
}

async fn serve_asset(Path(file): Path<String>) -> impl IntoResponse {
    match Assets::get(&file) {
        Some(content) => {
            let mime = mime_guess::from_path(&file).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
        }
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

async fn list_principals(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    let config = state.config.current_config();
    let mut principals = Vec::new();
    for (id, spec) in &config.principals {
        principals.push(json!({
            "id": id,
            "allowed_models": spec.allowed_models,
        }));
    }
    Ok(Json(json!({ "principals": principals })))
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
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({ "entries": entries })))
}

async fn list_upstreams(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    let config = state.config.current_config();
    let mut upstreams = Vec::new();
    for (name, spec) in &config.upstreams {
        upstreams.push(json!({
            "name": name,
            "kind": spec.kind,
        }));
    }
    Ok(Json(json!({ "upstreams": upstreams })))
}

async fn drain_upstream(
    State(_state): State<AdminState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    Ok(Json(json!({ "status": "ok", "drained": name })))
}

async fn set_killswitch(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    let Some(storage) = &state.storage else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };
    storage
        .set_killswitch_enabled(true)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "status": "ok", "killswitch": true })))
}

async fn clear_killswitch(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    let Some(storage) = &state.storage else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };
    storage
        .set_killswitch_enabled(false)
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
    body: Bytes,
) -> axum::response::Response {
    let config = match parse_config_draft(&body) {
        Ok(config) => config,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": { "message": message } })),
            )
                .into_response();
        }
    };

    match state.config.put_draft_config(config) {
        Ok(()) => Json(json!({ "status": "draft_saved" })).into_response(),
        Err(error) => config_draft_error_response(error),
    }
}

async fn apply_config_draft(State(state): State<AdminState>) -> axum::response::Response {
    match state.config.apply_draft_config() {
        Ok(config) => Json(json!({
            "status": "applied",
            "principal_count": config.principals.len(),
        }))
        .into_response(),
        Err(error) => config_draft_error_response(error),
    }
}

fn parse_config_draft(body: &[u8]) -> Result<Config, String> {
    match serde_yaml::from_slice::<Config>(body) {
        Ok(config) => Ok(config),
        Err(yaml_error) => serde_json::from_slice::<Config>(body).map_err(|json_error| {
            format!("failed to parse draft as YAML ({yaml_error}) or JSON ({json_error})")
        }),
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

async fn issue_principal_key(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(request): Json<management::IssueKeyRequest>,
) -> axum::response::Response {
    match management::issue_principal_key(&state, id, request) {
        Ok(response) => (StatusCode::CREATED, Json(response)).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn list_principal_keys(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match management::list_principal_keys(&state, id) {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn get_principal_key(
    State(state): State<AdminState>,
    Path((id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    match management::get_principal_key(&state, id, key_id) {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn update_principal_key(
    State(state): State<AdminState>,
    Path((id, key_id)): Path<(String, String)>,
    Json(request): Json<management::UpdateKeyRequest>,
) -> axum::response::Response {
    match management::update_principal_key(&state, id, key_id, request) {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn revoke_principal_key(
    State(state): State<AdminState>,
    Path((id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    match management::revoke_principal_key(&state, id, key_id) {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn disable_principal_key(
    State(state): State<AdminState>,
    Path((id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    match management::disable_principal_key(&state, id, key_id) {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn enable_principal_key(
    State(state): State<AdminState>,
    Path((id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    match management::enable_principal_key(&state, id, key_id) {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

fn management_error_response(error: management::ManagementError) -> axum::response::Response {
    error.into_response()
}
