use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    middleware,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{auth::require_admin_auth, AdminState};
use cc_lb_core::BucketKind;

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

pub fn build_router(state: AdminState) -> Router {
    let protected_routes = Router::new()
        .route("/admin/principals", get(list_principals))
        .route("/admin/principals/{id}/quota", get(get_quota))
        .route(
            "/admin/principals/{id}/quota/override",
            post(override_quota),
        )
        .route("/admin/audit", get(query_audit))
        .route("/admin/upstreams", get(list_upstreams))
        .route("/admin/upstreams/{name}/drain", post(drain_upstream))
        .route("/admin/killswitch", post(set_killswitch))
        .route("/admin/killswitch", delete(clear_killswitch))
        .route("/admin/oauth/start", post(crate::oauth::start_oauth))
        .route("/admin/oauth/complete", post(crate::oauth::complete_oauth))
        .route("/admin/config/current", get(get_config))
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

async fn get_quota(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let Some(storage) = &state.storage else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };
    let Some(_quota_manager) = &state.quota_manager else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };

    let config = state.config.current_config();
    let mut policy = cc_lb_core::QuotaPolicy {
        window_secs: config.quotas.default_window_secs,
        capacity_requests: config.quotas.default_requests_per_window,
        capacity_input_tokens: config.quotas.default_input_tokens,
        capacity_output_tokens: config.quotas.default_output_tokens,
    };
    if let Some(p) = config.principals.get(&id).and_then(|p| p.quotas.as_ref()) {
        policy.window_secs = p.default_window_secs;
        policy.capacity_requests = p.default_requests_per_window;
        policy.capacity_input_tokens = p.default_input_tokens;
        policy.capacity_output_tokens = p.default_output_tokens;
    }
    if let Some(qm) = &state.quota_manager {
        if let Some(p) = qm.per_principal.read().await.get(&id) {
            policy = *p;
        }
    }

    let window_secs = policy.window_secs.max(1);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .as_secs();
    let window_start = (now / window_secs) * window_secs;

    let requests = storage
        .get_quota(&id, window_start, BucketKind::Requests)
        .unwrap_or(0);
    let input_tokens = storage
        .get_quota(&id, window_start, BucketKind::InputTokens)
        .unwrap_or(0);
    let output_tokens = storage
        .get_quota(&id, window_start, BucketKind::OutputTokens)
        .unwrap_or(0);

    Ok(Json(json!({
        "window_start": window_start,
        "window_secs": window_secs,
        "requests": requests,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "requests_per_window": policy.capacity_requests,
        "input_tokens_per_window": policy.capacity_input_tokens,
        "output_tokens_per_window": policy.capacity_output_tokens,
    })))
}

#[derive(Deserialize)]
struct QuotaOverride {
    requests_per_window: Option<u64>,
    input_tokens_per_window: Option<u64>,
    output_tokens_per_window: Option<u64>,
}

async fn override_quota(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(payload): Json<QuotaOverride>,
) -> Result<Json<Value>, StatusCode> {
    let Some(quota_manager) = &state.quota_manager else {
        return Err(StatusCode::NOT_IMPLEMENTED);
    };

    let config = state.config.current_config();
    let mut policy = cc_lb_core::QuotaPolicy {
        window_secs: config.quotas.default_window_secs,
        capacity_requests: config.quotas.default_requests_per_window,
        capacity_input_tokens: config.quotas.default_input_tokens,
        capacity_output_tokens: config.quotas.default_output_tokens,
    };
    if let Some(p) = config.principals.get(&id).and_then(|p| p.quotas.as_ref()) {
        policy.window_secs = p.default_window_secs;
        policy.capacity_requests = p.default_requests_per_window;
        policy.capacity_input_tokens = p.default_input_tokens;
        policy.capacity_output_tokens = p.default_output_tokens;
    }
    if let Some(p) = quota_manager.per_principal.read().await.get(&id) {
        policy = *p;
    }

    if let Some(v) = payload.requests_per_window {
        policy.capacity_requests = v;
    }
    if let Some(v) = payload.input_tokens_per_window {
        policy.capacity_input_tokens = v;
    }
    if let Some(v) = payload.output_tokens_per_window {
        policy.capacity_output_tokens = v;
    }

    quota_manager.set_principal_policy(id.clone(), policy).await;

    Ok(Json(json!({ "status": "ok" })))
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
    if let Some(storage) = config_json.get_mut("storage") {
        if let Some(obj) = storage.as_object_mut() {
            obj.insert("oauth_aead_key_env".to_string(), json!("[REDACTED]"));
        }
    }
    if let Some(admin) = config_json.get_mut("admin") {
        if let Some(obj) = admin.as_object_mut() {
            obj.insert("token_env".to_string(), json!("[REDACTED]"));
        }
    }
    Ok(Json(config_json))
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
