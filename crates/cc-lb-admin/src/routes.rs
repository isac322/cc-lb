use std::collections::HashMap;
use std::convert::Infallible;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    middleware,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post, put},
};
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast::error::RecvError;

use crate::{
    AdminState, auth::require_admin_auth, dashboard, events, management, principals, status,
};
use cc_lb_core::{BucketKind, record_dashboard_sse_lagged};
use cc_lb_storage_api::{StorageError, UsageRollupResolution};

#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct Assets;

pub fn build_router(state: AdminState) -> Router {
    let protected_routes = Router::new()
        .route(
            "/admin/principals",
            get(list_principals).post(create_principal),
        )
        .route("/admin/principals/{id}", put(update_principal))
        .route("/admin/principals/{id}/disable", post(disable_principal))
        .route("/admin/principals/{id}/enable", post(enable_principal))
        .route(
            "/admin/principals/{id}/allowed_models",
            put(update_principal_allowed_models),
        )
        .route(
            "/admin/principals/{id}/keys",
            get(list_principal_keys).post(issue_principal_key),
        )
        .route(
            "/admin/principals/{id}/keys/{key_id}/revoke",
            post(revoke_principal_key),
        )
        .route("/admin/credentials", get(list_credentials))
        .route(
            "/admin/credentials/{principal_id}/{provider}/rotate",
            post(rotate_credential),
        )
        .route(
            "/admin/credentials/{principal_id}/{provider}/revoke",
            post(revoke_credential),
        )
        .route("/admin/principals/{id}/quota", get(get_quota))
        .route(
            "/admin/principals/{id}/quota/override",
            post(override_quota),
        )
        .route("/admin/audit", get(query_audit))
        .route("/admin/upstreams", get(list_upstreams))
        .route("/admin/upstreams/{name}/health", get(upstream_health))
        .route("/admin/upstreams/{name}/drain", post(drain_upstream))
        .route("/admin/plugins", get(plugins_status))
        .route("/admin/killswitch", post(set_killswitch))
        .route("/admin/killswitch", delete(clear_killswitch))
        .route("/admin/oauth/{id}", get(crate::oauth::oauth_status))
        .route("/admin/oauth/start", post(crate::oauth::start_oauth))
        .route("/admin/oauth/complete", post(crate::oauth::complete_oauth))
        .route("/admin/oauth/status", get(oauth_status))
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
        .route("/admin/dashboard/summary", get(dashboard_summary))
        .route("/admin/usage", get(dashboard_usage))
        .route("/admin/principals/{id}/usage", get(principal_usage))
        .route("/admin/principals/{id}/limits", get(principal_limits))
        .route("/admin/events/recent", get(recent_events))
        .route("/admin/events/stream", get(stream_events))
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

async fn serve_index() -> Response {
    serve_spa_index()
}

async fn serve_asset(Path(file): Path<String>) -> Response {
    let file = file.trim_start_matches('/');
    if file == "admin" || file.starts_with("admin/") {
        return StatusCode::NOT_FOUND.into_response();
    }

    serve_embedded_file(file).unwrap_or_else(serve_spa_index)
}

fn serve_spa_index() -> Response {
    serve_embedded_file("index.html").unwrap_or_else(|| StatusCode::NOT_FOUND.into_response())
}

fn serve_embedded_file(file: &str) -> Option<Response> {
    Assets::get(file).map(|content| {
        (
            [
                (header::CONTENT_TYPE, mime_type(file)),
                (header::CACHE_CONTROL, cache_control(file)),
            ],
            content.data,
        )
            .into_response()
    })
}

fn mime_type(file: &str) -> &'static str {
    let extension = file
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .unwrap_or_default();
    if extension.eq_ignore_ascii_case("html") {
        "text/html; charset=utf-8"
    } else if extension.eq_ignore_ascii_case("js") || extension.eq_ignore_ascii_case("mjs") {
        "text/javascript"
    } else if extension.eq_ignore_ascii_case("css") {
        "text/css"
    } else if extension.eq_ignore_ascii_case("svg") {
        "image/svg+xml"
    } else if extension.eq_ignore_ascii_case("json") || extension.eq_ignore_ascii_case("map") {
        "application/json"
    } else if extension.eq_ignore_ascii_case("ico") {
        "image/x-icon"
    } else if extension.eq_ignore_ascii_case("woff") {
        "font/woff"
    } else if extension.eq_ignore_ascii_case("woff2") {
        "font/woff2"
    } else if extension.eq_ignore_ascii_case("ttf") {
        "font/ttf"
    } else {
        "application/octet-stream"
    }
}

fn cache_control(file: &str) -> &'static str {
    if file == "index.html" || file == "dev-bootstrap.html" {
        "no-cache"
    } else if is_immutable_hashed_asset(file) {
        "public, max-age=31536000, immutable"
    } else {
        "public, max-age=300"
    }
}

fn is_immutable_hashed_asset(file: &str) -> bool {
    let Some(name) = file.strip_prefix("assets/") else {
        return false;
    };
    let Some((stem, extension)) = name.rsplit_once('.') else {
        return false;
    };
    stem.contains('-')
        && (extension.eq_ignore_ascii_case("js")
            || extension.eq_ignore_ascii_case("css")
            || extension.eq_ignore_ascii_case("woff2"))
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

async fn dashboard_summary(
    State(state): State<AdminState>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let range = match dashboard_range(&query) {
        Ok(range) => range,
        Err(error) => return dashboard_error(StatusCode::BAD_REQUEST, error.as_str()),
    };
    match dashboard::build_dashboard_summary(state.storage.as_ref(), range, unix_now_secs()).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => {
            tracing::error!(%error, "dashboard summary query failed");
            storage_error_response(&error, "storage_error")
        }
    }
}

async fn dashboard_usage(
    State(state): State<AdminState>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let range = match dashboard_range(&query) {
        Ok(range) => range,
        Err(error) => return dashboard_error(StatusCode::BAD_REQUEST, error.as_str()),
    };
    let step = match query.get("step") {
        Some(step) => match dashboard::parse_step(step) {
            Ok(step) => step,
            Err(error) => return dashboard_error(StatusCode::BAD_REQUEST, error.as_str()),
        },
        None => dashboard::auto_step(range),
    };
    let group_by = match query.get("group_by") {
        Some(group_by) => match dashboard::parse_group_by(group_by) {
            Ok(group_by) => group_by,
            Err(error) => return dashboard_error(StatusCode::BAD_REQUEST, error.as_str()),
        },
        None => dashboard::UsageGroupBy::None,
    };

    match dashboard::build_dashboard_usage_checked(
        state.storage.as_ref(),
        range,
        step,
        group_by,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(dashboard::DashboardBuildError::Query(error)) => {
            dashboard_error(StatusCode::BAD_REQUEST, error.as_str())
        }
        Err(dashboard::DashboardBuildError::Storage(error)) => {
            tracing::error!(%error, "dashboard usage query failed");
            storage_error_response(&error, "storage_error")
        }
    }
}

async fn principal_usage(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    if query.contains_key("group_by") {
        return dashboard_error(StatusCode::BAD_REQUEST, "invalid_group_by");
    }
    let range = match principal_range(&query) {
        Ok(range) => range,
        Err(error) => return principal_error(error),
    };
    let step = match principal_step(&query, range) {
        Ok(step) => step,
        Err(error) => return principal_error(error),
    };
    let config = state.config.current_config();

    match principals::build_principal_usage(
        state.storage.as_ref(),
        &config,
        &id,
        range,
        step,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => principal_error(error),
    }
}

async fn principal_limits(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let config = state.config.current_config();
    match principals::build_principal_limits(state.storage.as_ref(), &config, &id, unix_now_secs())
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => principal_error(error),
    }
}

fn principal_range(
    query: &HashMap<String, String>,
) -> Result<dashboard::DashboardRange, principals::T9Error> {
    match query.get("range") {
        Some(range) => dashboard::parse_range(range).map_err(|_| principals::T9Error::InvalidRange),
        None => Ok(dashboard::DashboardRange::OneHour),
    }
}

fn principal_step(
    query: &HashMap<String, String>,
    range: dashboard::DashboardRange,
) -> Result<UsageRollupResolution, principals::T9Error> {
    match query.get("step") {
        Some(step) => dashboard::parse_step(step).map_err(|_| principals::T9Error::InvalidStep),
        None => Ok(dashboard::auto_step(range)),
    }
}

fn principal_error(error: principals::T9Error) -> axum::response::Response {
    let error_code = error.as_str();
    match error {
        principals::T9Error::UnknownPrincipal => dashboard_error(StatusCode::NOT_FOUND, error_code),
        principals::T9Error::InvalidPrincipalId
        | principals::T9Error::InvalidRange
        | principals::T9Error::InvalidStep
        | principals::T9Error::StepTooFineForRange => {
            dashboard_error(StatusCode::BAD_REQUEST, error_code)
        }
        principals::T9Error::Storage(ref source) => {
            tracing::error!(error = %source, "principal admin query failed");
            storage_error_response(source, error_code)
        }
    }
}

fn dashboard_range(
    query: &HashMap<String, String>,
) -> Result<dashboard::DashboardRange, dashboard::DashboardQueryError> {
    match query.get("range") {
        Some(range) => {
            dashboard::parse_range(range).map_err(|_| dashboard::DashboardQueryError::InvalidRange)
        }
        None => Ok(dashboard::DashboardRange::OneHour),
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn dashboard_error(status: StatusCode, error: &str) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}

fn storage_error_response(source: &StorageError, error: &str) -> Response {
    match source {
        StorageError::Unavailable { .. } => storage_unavailable_response(error),
        _ => dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

fn storage_unavailable_response(error: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "1")],
        Json(json!({ "error": error })),
    )
        .into_response()
}

async fn upstream_health(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    let config = state.config.current_config();
    match status::build_upstream_health(
        state.storage.as_ref(),
        &config,
        state.breaker_registry.as_deref(),
        state.bulkhead_registry.as_deref(),
        state.drain_controller.as_ref(),
        &name,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error @ status::StatusBuildError::UnknownUpstream) => {
            dashboard_error(StatusCode::NOT_FOUND, error.as_str())
        }
        Err(status::StatusBuildError::Storage(source)) => {
            tracing::error!(error = %source, "admin upstream health query failed");
            storage_error_response(&source, "storage_error")
        }
    }
}

async fn plugins_status(State(state): State<AdminState>) -> Json<status::PluginsStatusResponse> {
    let config = state.config.current_config();
    Json(status::build_plugins_status(
        &config,
        state.plugin_runtime_status.as_deref(),
    ))
}

async fn oauth_status(State(state): State<AdminState>) -> axum::response::Response {
    let config = state.config.current_config();
    match status::build_oauth_status(
        state.storage.as_ref(),
        state.aead.as_ref(),
        &config,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(status::StatusBuildError::Storage(source)) => {
            tracing::error!(error = %source, "admin oauth status query failed");
            storage_error_response(&source, "storage_error")
        }
        Err(status::StatusBuildError::UnknownUpstream) => {
            dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error")
        }
    }
}

async fn recent_events(
    State(state): State<AdminState>,
    Query(query): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let params = match events::parse_recent_params(&query) {
        Ok(params) => params,
        Err(error) => return events_error(error),
    };

    match events::build_recent_events_payload(state.storage.as_ref(), &params).await {
        Ok(payload) => Json(payload).into_response(),
        Err(error) => events_error(error),
    }
}

fn events_error(error: events::EventsError) -> axum::response::Response {
    let error_code = error.as_str();
    match error {
        events::EventsError::Storage(ref source) => {
            tracing::error!(error = %source, "admin events query failed");
            storage_error_response(source, error_code)
        }
        _ => dashboard_error(StatusCode::BAD_REQUEST, error_code),
    }
}

async fn stream_events(
    State(state): State<AdminState>,
    Query(query): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let filters = match events::parse_stream_filters(&query) {
        Ok(filters) => filters,
        Err(error) => return events_error(error),
    };
    let mut receiver = state.dashboard_broadcaster.subscribe();
    let stream = async_stream::stream! {
        let mut event_id = 1_u64;
        loop {
            match receiver.recv().await {
                Ok(request_event) => {
                    if !events::apply_filters_to_event(&request_event, &filters) {
                        continue;
                    }
                    let data = match serde_json::to_string(&request_event) {
                        Ok(data) => data,
                        Err(source) => {
                            tracing::warn!(error = %source, "dashboard SSE request event serialization failed");
                            continue;
                        }
                    };
                    yield Ok::<Event, Infallible>(Event::default()
                        .event("request")
                        .id(event_id.to_string())
                        .data(data));
                    event_id = event_id.saturating_add(1);
                }
                Err(RecvError::Lagged(skipped)) => {
                    record_dashboard_sse_lagged(skipped);
                    yield Ok::<Event, Infallible>(Event::default().comment("lagged"));
                }
                Err(RecvError::Closed) => break,
            }
        }
    };

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("ping"),
        )
        .into_response()
}

async fn create_principal(
    State(state): State<AdminState>,
    Json(request): Json<management::CreatePrincipalRequest>,
) -> axum::response::Response {
    match management::create_principal(
        state.storage.as_ref(),
        state.config.as_ref(),
        request,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn update_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(request): Json<management::UpdatePrincipalRequest>,
) -> axum::response::Response {
    match management::update_principal(
        state.storage.as_ref(),
        state.config.as_ref(),
        id,
        request,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn disable_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match management::set_principal_disabled(
        state.storage.as_ref(),
        state.config.as_ref(),
        id,
        true,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn enable_principal(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match management::set_principal_disabled(
        state.storage.as_ref(),
        state.config.as_ref(),
        id,
        false,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn update_principal_allowed_models(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(request): Json<management::AllowedModelsRequest>,
) -> axum::response::Response {
    match management::update_allowed_models(
        state.storage.as_ref(),
        state.config.as_ref(),
        id,
        request,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn issue_principal_key(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Json(request): Json<management::IssueKeyRequest>,
) -> axum::response::Response {
    match management::issue_principal_key(
        state.storage.as_ref(),
        state.aead.as_ref(),
        state.config.as_ref(),
        id,
        request,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn list_principal_keys(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    match management::list_principal_keys(
        state.storage.as_ref(),
        state.aead.as_ref(),
        state.config.as_ref(),
        id,
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn revoke_principal_key(
    State(state): State<AdminState>,
    Path((id, key_id)): Path<(String, String)>,
) -> axum::response::Response {
    match management::revoke_principal_key(
        state.storage.as_ref(),
        state.aead.as_ref(),
        state.config.as_ref(),
        id,
        key_id,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn list_credentials(State(state): State<AdminState>) -> axum::response::Response {
    let config = state.config.current_config();
    match management::list_credentials(
        state.storage.as_ref(),
        state.aead.as_ref(),
        &config,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn rotate_credential(
    State(state): State<AdminState>,
    Path((principal_id, provider)): Path<(String, String)>,
) -> axum::response::Response {
    let config = state.config.current_config();
    match management::rotate_credential(
        state.storage.as_ref(),
        state.aead.as_ref(),
        &config,
        principal_id,
        provider,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn revoke_credential(
    State(state): State<AdminState>,
    Path((principal_id, provider)): Path<(String, String)>,
) -> axum::response::Response {
    let config = state.config.current_config();
    match management::revoke_credential(
        state.storage.as_ref(),
        state.aead.as_ref(),
        &config,
        principal_id,
        provider,
        unix_now_secs(),
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => management_error_response(error),
    }
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

async fn get_quota(State(state): State<AdminState>, Path(id): Path<String>) -> Response {
    let storage = state.storage.as_ref();
    let Some(_quota_manager) = &state.quota_manager else {
        return StatusCode::NOT_IMPLEMENTED.into_response();
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
    if let Some(qm) = &state.quota_manager
        && let Some(p) = qm.per_principal.read().await.get(&id)
    {
        policy = *p;
    }

    let window_secs = policy.window_secs.max(1);
    let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let window_start = (now / window_secs) * window_secs;

    let requests = match storage
        .get_quota(&id, window_start, BucketKind::Requests)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "admin quota query failed");
            return storage_error_response(&error, "storage_error");
        }
    };
    let input_tokens = match storage
        .get_quota(&id, window_start, BucketKind::InputTokens)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "admin quota query failed");
            return storage_error_response(&error, "storage_error");
        }
    };
    let output_tokens = match storage
        .get_quota(&id, window_start, BucketKind::OutputTokens)
        .await
    {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "admin quota query failed");
            return storage_error_response(&error, "storage_error");
        }
    };

    Json(json!({
        "window_start": window_start,
        "window_secs": window_secs,
        "requests": requests,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "requests_per_window": policy.capacity_requests,
        "input_tokens_per_window": policy.capacity_input_tokens,
        "output_tokens_per_window": policy.capacity_output_tokens,
    }))
    .into_response()
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

async fn query_audit(State(state): State<AdminState>, Query(query): Query<AuditQuery>) -> Response {
    let storage = state.storage.as_ref();

    let since = query.since.unwrap_or(0);
    let until = query.until.unwrap_or(u64::MAX);
    let limit = query.limit.unwrap_or(100).min(1000);

    match storage
        .query_audit(query.principal_id.as_deref(), since, until, limit)
        .await
    {
        Ok(entries) => Json(json!({ "entries": entries })).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "admin audit query failed");
            storage_error_response(&error, "storage_error")
        }
    }
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

async fn set_killswitch(State(state): State<AdminState>) -> Response {
    let storage = state.storage.as_ref();
    match storage.set_killswitch_enabled(true).await {
        Ok(()) => Json(json!({ "status": "ok", "killswitch": true })).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "admin killswitch write failed");
            storage_error_response(&error, "storage_error")
        }
    }
}

async fn clear_killswitch(State(state): State<AdminState>) -> Response {
    let storage = state.storage.as_ref();
    match storage.set_killswitch_enabled(false).await {
        Ok(()) => Json(json!({ "status": "ok", "killswitch": false })).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "admin killswitch write failed");
            storage_error_response(&error, "storage_error")
        }
    }
}

async fn get_config(State(state): State<AdminState>) -> Result<Json<Value>, StatusCode> {
    crate::settings::current_config_response(
        state.config.as_ref(),
        effective_config_revision_unix_secs(&state),
    )
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn get_config_schema() -> axum::response::Response {
    match crate::settings::schema_response() {
        Ok(response) => ([(header::CACHE_CONTROL, "max-age=60")], Json(response)).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

async fn get_config_draft(State(state): State<AdminState>) -> axum::response::Response {
    match crate::settings::get_draft(state.storage.as_ref()).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

async fn put_config_draft(
    State(state): State<AdminState>,
    Json(request): Json<crate::settings::PutConfigDraftRequest>,
) -> axum::response::Response {
    match crate::settings::put_draft(state.storage.as_ref(), request, unix_now_secs()).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, true),
    }
}

async fn validate_config_draft(
    State(state): State<AdminState>,
    Json(request): Json<crate::settings::ValidateConfigDraftRequest>,
) -> axum::response::Response {
    match crate::settings::validate_draft(state.storage.as_ref(), request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, false),
    }
}

async fn apply_config_draft(
    State(state): State<AdminState>,
    Json(request): Json<crate::settings::ApplyConfigRequest>,
) -> axum::response::Response {
    match crate::settings::apply_config(
        state.storage.as_ref(),
        state.config_path.as_deref(),
        state.config_watcher.as_deref(),
        request,
        unix_now_secs(),
    )
    .await
    {
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
    match crate::settings::list_history(state.storage.as_ref(), query.limit.unwrap_or(20)).await {
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
    match crate::settings::diff_history(
        state.storage.as_ref(),
        query.from_revision,
        query.to_revision,
    )
    .await
    {
        Ok(response) => Json(response).into_response(),
        Err(error) => settings_error_response(error, false),
    }
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

fn management_error_response(error: management::ManagementError) -> axum::response::Response {
    match error {
        management::ManagementError::StorageUnavailable => {
            storage_unavailable_response("storage_unavailable")
        }
        management::ManagementError::Settings(error) => settings_error_response(error, false),
        management::ManagementError::Storage(source) => {
            tracing::error!(error = %source, "admin management storage operation failed");
            storage_error_response(&source, "storage_error")
        }
        management::ManagementError::Json(source) => {
            tracing::error!(error = %source, "admin management json operation failed");
            dashboard_error(StatusCode::INTERNAL_SERVER_ERROR, "json_error")
        }
        management::ManagementError::PrincipalExists => {
            dashboard_error(StatusCode::CONFLICT, "principal_exists")
        }
        management::ManagementError::UnknownPrincipal => {
            dashboard_error(StatusCode::NOT_FOUND, "unknown_principal")
        }
        management::ManagementError::UnknownApiKey => {
            dashboard_error(StatusCode::NOT_FOUND, "unknown_api_key")
        }
        management::ManagementError::InvalidDraftPrincipal => {
            dashboard_error(StatusCode::CONFLICT, "invalid_draft_principal")
        }
        management::ManagementError::RotateUnsupported { kind } => (
            StatusCode::METHOD_NOT_ALLOWED,
            Json(json!({ "error": "rotate_unsupported", "kind": kind })),
        )
            .into_response(),
    }
}

fn effective_config_revision_unix_secs(state: &AdminState) -> u64 {
    state.config_started_at_unix_secs
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
