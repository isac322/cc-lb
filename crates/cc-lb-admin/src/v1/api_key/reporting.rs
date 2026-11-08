//! Admin handlers for the deployment-scoped OpenAI organization credential and
//! project listing. These routes never use a serving upstream credential or
//! base URL.

use axum::{
    Extension, Json, Router,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use cc_lb_control::{OpenAiOrgApi, OpenAiOrgApiError, OpenAiOrganizationProject};
use cc_lb_storage_api::{
    OPENAI_ORG_REPORTING_CREDENTIAL_AAD, OpenAiOrganizationReportingCredentialStatus,
    OpenAiOrganizationReportingCredentialWrite,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::super::upstreams::{UpstreamError, storage};
use crate::{
    AdminState,
    auth::{AdminAction, AdminIdentity, authorize},
};

/// The singleton is deployment-scoped. It deliberately does not contain an
/// upstream id, so serving credentials cannot be used as organization keys.
const MAX_ADMINISTRATOR_KEY_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
struct PutCredentialBody {
    /// Aliases keep the additive API tolerant of the names used by existing
    /// operators while the canonical field remains explicit.
    #[serde(alias = "admin_key", alias = "api_key", alias = "key")]
    administrator_key: String,
}

#[derive(Debug, Serialize)]
struct CredentialMetadataResponse {
    scope: &'static str,
    redacted_hint: Option<String>,
    status: String,
    revision: u64,
    updated_at_unix_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ProjectsQuery {
    #[serde(default)]
    include_archived: bool,
}

#[derive(Debug, Serialize)]
struct ProjectResponse {
    id: String,
    name: String,
    created_at: u64,
    archived_at: Option<u64>,
    status: Option<String>,
    external_key_id: Option<String>,
    residency: Option<String>,
}

#[derive(Debug, Serialize)]
struct ProjectsResponse {
    /// `not_configured`, `denied`, `unavailable`, `failed`, or `configured`.
    status: &'static str,
    projects: Vec<ProjectResponse>,
    complete: bool,
}

pub(crate) fn router() -> Router<AdminState> {
    Router::new()
        .route(
            "/admin/v1/openai/organization-credential",
            get(get_organization_credential).put(put_organization_credential),
        )
        .route("/admin/v1/openai/projects", get(list_projects))
}

async fn get_organization_credential(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Response, UpstreamError> {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return Ok(no_store(
            (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response(),
        ));
    }
    let storage = storage(&state)?;
    let metadata = storage.get_openai_org_reporting_credential().await?;
    let response = credential_metadata_response(metadata.as_ref());
    Ok(no_store(Json(response).into_response()))
}

async fn put_organization_credential(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    headers: HeaderMap,
    Json(body): Json<PutCredentialBody>,
) -> Result<Response, UpstreamError> {
    if authorize(&identity, AdminAction::Write).is_err() {
        return Ok(no_store(
            (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response(),
        ));
    }
    validate_administrator_key(&body.administrator_key)?;

    let storage = storage(&state)?;
    let current = storage.get_openai_org_reporting_credential().await?;
    let expected_revision = match current.as_ref() {
        Some(_) => Some(parse_if_match(&headers)?),
        None => parse_optional_if_match(&headers)?,
    };
    let encrypted_secret_ciphertext = state
        .aead
        .encrypt(
            body.administrator_key.as_bytes(),
            OPENAI_ORG_REPORTING_CREDENTIAL_AAD,
        )
        .map_err(|error| UpstreamError::Internal {
            detail: format!("credential encryption failed: {error}"),
        })?;
    let write = OpenAiOrganizationReportingCredentialWrite {
        encrypted_secret_ciphertext,
        redacted_hint: cc_lb_control::anthropic_api_key::key_hint(&body.administrator_key)
            .unwrap_or_else(|| "...".to_owned()),
        status: OpenAiOrganizationReportingCredentialStatus::Configured,
        expected_revision,
        updated_at_unix_secs: cc_lb_clock::unix_secs(state.clock.now()),
    };
    let saved = storage
        .replace_openai_org_reporting_credential(&write)
        .await?;
    let response = credential_metadata_response(Some(&saved));
    let mut response = no_store(Json(response).into_response());
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("W/\"{}\"", saved.revision)).map_err(|error| {
            UpstreamError::Internal {
                detail: format!("invalid credential revision header: {error}"),
            }
        })?,
    );
    Ok(response)
}

async fn list_projects(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Query(query): Query<ProjectsQuery>,
) -> Result<Response, UpstreamError> {
    if authorize(&identity, AdminAction::SensitiveRead).is_err() {
        return Ok(no_store(
            (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response(),
        ));
    }

    let storage = storage(&state)?;
    let Some(metadata) = storage.get_openai_org_reporting_credential().await? else {
        return Ok(no_store(
            Json(ProjectsResponse {
                status: "not_configured",
                projects: Vec::new(),
                complete: false,
            })
            .into_response(),
        ));
    };
    let ciphertext = storage
        .get_openai_org_reporting_credential_ciphertext()
        .await?
        .ok_or(UpstreamError::ApiKeyCredentialDecrypt)?;
    let plaintext = state
        .aead
        .decrypt(
            &ciphertext.encrypted_secret_ciphertext,
            OPENAI_ORG_REPORTING_CREDENTIAL_AAD,
        )
        .map_err(|error| {
            tracing::warn!(
                ?error,
                "OpenAI organization credential could not be decrypted"
            );
            UpstreamError::ApiKeyCredentialDecrypt
        })?;
    let credential =
        String::from_utf8(plaintext).map_err(|_| UpstreamError::ApiKeyCredentialDecrypt)?;

    let client = OpenAiOrgApi::production().map_err(|error| UpstreamError::Internal {
        detail: format!("OpenAI organization client initialization failed: {error}"),
    })?;
    let result = if query.include_archived {
        client.list_projects_with_archived(&credential).await
    } else {
        client.list_projects(&credential).await
    };
    drop(credential);

    let (status, projects, complete) = match result {
        Ok(listing) => (
            "configured",
            listing.projects.into_iter().map(project_response).collect(),
            listing.complete,
        ),
        Err(error) => {
            let status = reporting_status(error);
            (status, Vec::new(), false)
        }
    };
    // Preserve the latest provider classification for the metadata endpoint;
    // a concurrent replacement wins and is intentionally not overwritten.
    let desired_status = status_from_str(status);
    if desired_status != metadata.status {
        let write = OpenAiOrganizationReportingCredentialWrite {
            encrypted_secret_ciphertext: ciphertext.encrypted_secret_ciphertext,
            redacted_hint: metadata.redacted_hint.clone(),
            status: desired_status,
            expected_revision: Some(metadata.revision),
            updated_at_unix_secs: cc_lb_clock::unix_secs(state.clock.now()),
        };
        if let Err(error) = storage
            .replace_openai_org_reporting_credential(&write)
            .await
        {
            tracing::debug!(
                ?error,
                "OpenAI organization credential status writeback skipped"
            );
        }
    }

    Ok(no_store(
        Json(ProjectsResponse {
            status,
            projects,
            complete,
        })
        .into_response(),
    ))
}

fn credential_metadata_response(
    metadata: Option<&cc_lb_storage_api::OpenAiOrganizationReportingCredentialRecord>,
) -> CredentialMetadataResponse {
    match metadata {
        Some(record) => CredentialMetadataResponse {
            scope: "deployment",
            redacted_hint: Some(record.redacted_hint.clone()),
            status: record.status.as_str().to_owned(),
            revision: record.revision,
            updated_at_unix_secs: Some(record.updated_at_unix_secs),
        },
        None => CredentialMetadataResponse {
            scope: "deployment",
            redacted_hint: None,
            status: "not_configured".to_owned(),
            revision: 0,
            updated_at_unix_secs: None,
        },
    }
}

fn validate_administrator_key(key: &str) -> Result<(), UpstreamError> {
    if key.trim().is_empty() {
        return Err(UpstreamError::InvalidInput {
            field: "administrator_key".to_owned(),
            reason: "must not be empty".to_owned(),
        });
    }
    if key.len() > MAX_ADMINISTRATOR_KEY_BYTES {
        return Err(UpstreamError::InvalidInput {
            field: "administrator_key".to_owned(),
            reason: format!("must be at most {MAX_ADMINISTRATOR_KEY_BYTES} bytes"),
        });
    }
    Ok(())
}

fn parse_optional_if_match(headers: &HeaderMap) -> Result<Option<u64>, UpstreamError> {
    headers
        .get(header::IF_MATCH)
        .map(parse_if_match_value)
        .transpose()
}

fn parse_if_match(headers: &HeaderMap) -> Result<u64, UpstreamError> {
    headers
        .get(header::IF_MATCH)
        .ok_or(UpstreamError::MissingIfMatch)
        .and_then(parse_if_match_value)
}

fn parse_if_match_value(value: &HeaderValue) -> Result<u64, UpstreamError> {
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

fn project_response(project: OpenAiOrganizationProject) -> ProjectResponse {
    ProjectResponse {
        id: project.id,
        name: project.name,
        created_at: project.created_at,
        archived_at: project.archived_at,
        status: project.status,
        external_key_id: project.external_key_id,
        residency: project.residency,
    }
}

fn reporting_status(error: OpenAiOrgApiError) -> &'static str {
    match error {
        OpenAiOrgApiError::Denied { .. } => "denied",
        OpenAiOrgApiError::Timeout
        | OpenAiOrgApiError::Unreachable
        | OpenAiOrgApiError::RateLimited { .. } => "unavailable",
        OpenAiOrgApiError::Upstream { .. }
        | OpenAiOrgApiError::BodyTooLarge
        | OpenAiOrgApiError::MalformedJson => "failed",
    }
}

fn status_from_str(status: &str) -> OpenAiOrganizationReportingCredentialStatus {
    match status {
        "denied" => OpenAiOrganizationReportingCredentialStatus::Denied,
        "unavailable" => OpenAiOrganizationReportingCredentialStatus::Unavailable,
        "failed" => OpenAiOrganizationReportingCredentialStatus::Failed,
        _ => OpenAiOrganizationReportingCredentialStatus::Configured,
    }
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_failures_have_distinct_reporting_states() {
        assert_eq!(
            reporting_status(OpenAiOrgApiError::Denied { status: 403 }),
            "denied"
        );
        assert_eq!(reporting_status(OpenAiOrgApiError::Timeout), "unavailable");
        assert_eq!(reporting_status(OpenAiOrgApiError::MalformedJson), "failed");
    }

    #[test]
    fn key_validation_never_echoes_secret() {
        let error = validate_administrator_key("").expect_err("empty key rejected");
        assert!(matches!(error, UpstreamError::InvalidInput { .. }));
    }
}
