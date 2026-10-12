//! `GET /admin/v1/upstreams/{id}/workspaces`: lists the workspaces the stored
//! key's org exposes (plan §1.10, S3b). It reads a secret-backed resource, so
//! the handler also authorizes `SensitiveRead` (D-AUTH04).

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cc_lb_control::anthropic_api_key::{
    org_probe::OrgApiError, remediation::operator_message, wire::AnthropicWorkspace,
};
use cc_lb_storage_api::RemediationCode;
use serde::Serialize;
use serde_json::json;

use super::recheck::{load_api_key_upstream, stored_api_key};
use crate::v1::upstreams::{UpstreamError, storage, upstream_base_url};
use crate::{
    AdminState,
    auth::{AdminAction, AdminIdentity, authorize},
};

#[derive(Debug, Serialize)]
struct UpstreamWorkspacesResponse {
    workspaces: Vec<AnthropicWorkspace>,
}

pub(crate) async fn list_upstream_workspaces(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path(id): Path<String>,
) -> Result<Response, UpstreamError> {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return Ok((StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response());
    }
    let storage = storage(&state)?;
    let record = load_api_key_upstream(storage, &id).await?;
    let base_url = upstream_base_url(&record)?;
    // The host rule comes before the decrypt: a custom host gets zero
    // outbound requests (WSL-03).
    if !state.anthropic_org_api.applies_to(&base_url) {
        return Err(remediation(
            StatusCode::FORBIDDEN,
            RemediationCode::NonAnthropicBaseUrl,
        ));
    }
    let key = stored_api_key(&state, &record)?;
    let listed = state.anthropic_org_api.list_workspaces(key.as_str()).await;
    drop(key);
    match listed {
        Ok(workspaces) => Ok(Json(UpstreamWorkspacesResponse { workspaces }).into_response()),
        Err(error) => {
            tracing::info!(upstream_id = %record.id, %error, "workspace list failed");
            Err(list_error(error))
        }
    }
}

fn list_error(error: OrgApiError) -> UpstreamError {
    match error {
        OrgApiError::Denied { .. } => {
            remediation(StatusCode::FORBIDDEN, RemediationCode::WorkspaceListDenied)
        }
        OrgApiError::Timeout => remediation(
            StatusCode::GATEWAY_TIMEOUT,
            RemediationCode::ProbeUnreachable,
        ),
        OrgApiError::RateLimited { .. }
        | OrgApiError::Upstream { .. }
        | OrgApiError::Unreachable
        | OrgApiError::Malformed => {
            remediation(StatusCode::BAD_GATEWAY, RemediationCode::ProbeUnreachable)
        }
    }
}

fn remediation(status: StatusCode, code: RemediationCode) -> UpstreamError {
    UpstreamError::Remediation {
        status,
        code,
        message: operator_message(code).to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_and_code(error: UpstreamError) -> (StatusCode, RemediationCode) {
        match error {
            UpstreamError::Remediation { status, code, .. } => (status, code),
            other => panic!("expected a remediation error, got {other:?}"),
        }
    }

    // WSL-02 / WSL-05: D-Z3 statuses for every org API failure.
    #[test]
    fn org_failures_map_to_d_z3_statuses() {
        assert_eq!(
            status_and_code(list_error(OrgApiError::Denied { status: 403 })),
            (StatusCode::FORBIDDEN, RemediationCode::WorkspaceListDenied)
        );
        assert_eq!(
            status_and_code(list_error(OrgApiError::Timeout)),
            (
                StatusCode::GATEWAY_TIMEOUT,
                RemediationCode::ProbeUnreachable
            )
        );
        for error in [
            OrgApiError::RateLimited {
                retry_after_secs: Some(1),
            },
            OrgApiError::Upstream { status: 500 },
            OrgApiError::Unreachable,
            OrgApiError::Malformed,
        ] {
            assert_eq!(
                status_and_code(list_error(error)),
                (StatusCode::BAD_GATEWAY, RemediationCode::ProbeUnreachable)
            );
        }
    }
}
