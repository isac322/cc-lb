use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub type Result<T> = std::result::Result<T, ManagementError>;

#[derive(Debug, thiserror::Error)]
pub enum ManagementError {
    #[error("storage unavailable")]
    StorageUnavailable,
    #[error("unknown principal")]
    UnknownPrincipal,
    #[error("unknown api key")]
    UnknownApiKey,
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error(transparent)]
    Storage(#[from] cc_lb_storage_api::StorageError),
    #[error(transparent)]
    KeyStore(#[from] cc_lb_core::api_keys::key_store::KeyStoreError),
}

impl IntoResponse for ManagementError {
    fn into_response(self) -> Response {
        match self {
            ManagementError::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            ManagementError::UnknownPrincipal => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown_principal" })),
            )
                .into_response(),
            ManagementError::UnknownApiKey => StatusCode::NOT_FOUND.into_response(),
            ManagementError::InvalidRequest { message } => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": message,
                    }
                })),
            )
                .into_response(),
            ManagementError::Storage(error) => {
                tracing::error!(error = %error, "admin management storage operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            ManagementError::KeyStore(error) => {
                tracing::error!(error = %error, "admin management key store operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}
