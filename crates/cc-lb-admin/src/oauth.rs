use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use cc_lb_storage_api::StorageError;
use serde_json::json;

use crate::AdminState;

pub async fn oauth_status(
    State(state): State<AdminState>,
    axum::extract::Path(oauth_credential_id): axum::extract::Path<String>,
) -> Response {
    let config = state.config.current_config();
    for (principal_id, principal) in &config.principals {
        let matches_provider = principal
            .credentials_ref
            .as_deref()
            .map(str::trim)
            .is_some_and(|reference| {
                reference == oauth_credential_id
                    || reference
                        .strip_prefix("oauth:")
                        .map(str::trim)
                        .is_some_and(|provider| provider == oauth_credential_id)
            });
        if !matches_provider {
            continue;
        }
        let Some(storage) = state.storage.as_ref() else {
            return StatusCode::NOT_IMPLEMENTED.into_response();
        };
        match storage
            .get_oauth_ciphertext(principal_id, &oauth_credential_id)
            .await
        {
            Ok(Some(_)) => return Json(json!({ "enrolled": true })).into_response(),
            Ok(None) => {}
            Err(error) => {
                tracing::error!(error = %error, "admin oauth status storage operation failed");
                return storage_error_response(&error);
            }
        }
    }

    if let (Some(storage), Some(key_store)) = (state.storage.as_ref(), state.key_store.as_ref()) {
        let records = match key_store.list_all().await {
            Ok(records) => records,
            Err(error) => {
                tracing::error!(error = %error, "admin oauth status managed-key scan failed");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
        for (principal_id, _key_id, record) in records {
            if record.upstream_credential_ref != oauth_credential_id {
                continue;
            }
            match storage
                .get_oauth_ciphertext(&principal_id, &oauth_credential_id)
                .await
            {
                Ok(Some(_)) => return Json(json!({ "enrolled": true })).into_response(),
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(error = %error, "admin oauth status managed-key oauth lookup failed");
                    return storage_error_response(&error);
                }
            }
        }
    }

    Json(json!({ "enrolled": false })).into_response()
}

fn storage_error_response(error: &StorageError) -> Response {
    if error.is_retryable() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            Json(json!({ "error": "storage_error" })),
        )
            .into_response()
    } else {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}
