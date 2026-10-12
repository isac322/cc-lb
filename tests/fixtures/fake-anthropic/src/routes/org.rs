//! Anthropic org Admin API routes, answered per key policy. Keys without a
//! policy, and bound keys, have no org access and get 403.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde_json::{Value, json};

use super::helpers::{error_response, json_response};
use super::key_policy::{FakeApiKey, FakeWorkspace, KeyPolicy, header_value, policy_for, serve};
use super::state::AppState;

const CREATED_AT: &str = "2026-01-01T00:00:00Z";
pub(crate) const USAGE_REPORT_PATH: &str = "/v1/organizations/usage_report/messages";

/// The key's policy when it has org access.
fn org_policy(state: &AppState, headers: &HeaderMap) -> Option<KeyPolicy> {
    policy_for(state, headers).filter(|policy| policy.bound_workspace.is_none())
}

fn forbidden() -> Response {
    error_response(
        StatusCode::FORBIDDEN,
        "permission_error",
        "Your API key does not have permission to use the Admin API.",
    )
}

pub(crate) async fn list_workspaces(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let Some(policy) = org_policy(&state, &headers) else {
        return forbidden();
    };
    let items: Vec<Value> = policy.workspaces.iter().map(workspace_json).collect();
    serve(&policy.workspaces_list, || list_page(items)).await
}

pub(crate) async fn list_api_keys(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let Some(policy) = org_policy(&state, &headers) else {
        return forbidden();
    };
    let items: Vec<Value> = policy.api_key_items.iter().map(api_key_json).collect();
    serve(&policy.api_keys, || list_page(items)).await
}

pub(crate) async fn usage_report(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let Some(policy) = org_policy(&state, &headers) else {
        return forbidden();
    };
    // The recorder already holds this request, so the count is 1-based.
    let key = header_value(&headers, "x-api-key").unwrap_or_default();
    let served = state
        .config
        .recorder
        .snapshot()
        .iter()
        .filter(|request| {
            request.path == USAGE_REPORT_PATH && request.header("x-api-key") == Some(key)
        })
        .count();
    let page = served
        .checked_sub(1)
        .and_then(|index| {
            policy
                .usage_pages
                .get(index)
                .or_else(|| policy.usage_pages.last())
        })
        .cloned()
        .unwrap_or_else(|| json!({"data": [], "has_more": false, "next_page": null}));
    if let Some(pause) = state.config.usage_report_pause.clone() {
        pause.pause_response().await;
    }
    serve(&policy.usage_report, || json_response(StatusCode::OK, page)).await
}

fn list_page(items: Vec<Value>) -> Response {
    let id_at = |item: Option<&Value>| item.and_then(|item| item.get("id")).cloned();
    let first_id = id_at(items.first());
    let last_id = id_at(items.last());
    json_response(
        StatusCode::OK,
        json!({"data": items, "has_more": false, "first_id": first_id, "last_id": last_id}),
    )
}

fn workspace_json(workspace: &FakeWorkspace) -> Value {
    json!({
        "id": workspace.id,
        "type": "workspace",
        "name": workspace.name,
        "created_at": CREATED_AT,
        "archived_at": null,
        "display_color": "#6C5BB9"
    })
}

fn api_key_json(key: &FakeApiKey) -> Value {
    let workspace_id = key
        .scope
        .as_ref()
        .and_then(|scope| scope.get("workspace_id"))
        .cloned()
        .unwrap_or(Value::Null);
    json!({
        "id": key.id,
        "type": "api_key",
        "name": key.id,
        "partial_key_hint": key.partial_key_hint,
        "status": "active",
        "created_at": CREATED_AT,
        "created_by": {"id": "user_fake", "type": "user"},
        "workspace_id": workspace_id,
        "principal": key.principal,
        "scope": key.scope
    })
}
