use axum::{
    Json, Router,
    extract::{Extension, State},
    routing::get,
};
use serde::Serialize;

use crate::AdminState;
use crate::auth::{AdminActorKind, AdminAuthMode, AdminIdentity};

pub fn router() -> Router<AdminState> {
    Router::new().route("/admin/v1/auth/session", get(auth_session))
}

#[derive(Serialize)]
struct AuthSessionResponse {
    authority: String,
    subject: String,
    kind: AdminActorKind,
    provider_id: String,
    email: Option<String>,
    display_name: Option<String>,
    expires_at_unix_secs: Option<u64>,
    auth_mode: AdminAuthMode,
}

async fn auth_session(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
) -> Json<AuthSessionResponse> {
    Json(AuthSessionResponse {
        authority: identity.authority,
        subject: identity.subject,
        kind: identity.kind,
        provider_id: identity.provider_id,
        email: identity.email,
        display_name: identity.display_name,
        expires_at_unix_secs: identity.expires_at_unix_secs,
        auth_mode: state.admin_auth.mode(),
    })
}
