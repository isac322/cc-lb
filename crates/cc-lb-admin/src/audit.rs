use axum::http::Method;
use cc_lb_storage_api::{AuditActorFields, AuditEntry, StorageError};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{AdminState, auth::AdminIdentity};

pub struct AdminAuditEvent<'a> {
    pub identity: Option<&'a AdminIdentity>,
    pub system_component: Option<&'a str>,
    pub action: &'a str,
    pub route: &'a str,
    pub target_principal_id: Option<&'a str>,
    pub target_upstream: Option<&'a str>,
    pub api_key_id: Option<&'a str>,
    pub status: u16,
    pub payload: Option<Value>,
}

pub async fn record_admin_audit(
    state: &AdminState,
    event: AdminAuditEvent<'_>,
) -> Result<(), StorageError> {
    let Some(storage) = &state.storage else {
        return Ok(());
    };

    let actor = event
        .identity
        .map(AdminIdentity::audit_fields)
        .unwrap_or_else(|| AuditActorFields::system(event.system_component.unwrap_or("admin")));
    let AuditActorFields {
        actor,
        authority,
        subject,
        kind,
        email,
    } = actor;

    storage
        .append_audit(&AuditEntry {
            ts: cc_lb_clock::unix_secs(state.clock.now()),
            request_id: Uuid::now_v7().to_string(),
            principal_id: event.target_principal_id.unwrap_or("").to_owned(),
            route: event.route.to_owned(),
            upstream: event.target_upstream.unwrap_or("admin").to_owned(),
            status: event.status,
            input_tokens: Some(0),
            output_tokens: Some(0),
            duration_ms: 0,
            api_key_id: event.api_key_id.map(str::to_owned),
            admin_action: Some(event.action.to_owned()),
            actor: Some(actor),
            actor_authority: Some(authority),
            actor_subject: Some(subject),
            actor_kind: Some(kind),
            actor_email: email,
            payload: event.payload,
            ..Default::default()
        })
        .await
}

pub async fn record_auth_rejected(
    state: &AdminState,
    method: &Method,
    path: &str,
    reason: &str,
) -> Result<(), StorageError> {
    record_admin_audit(
        state,
        AdminAuditEvent {
            identity: None,
            system_component: Some("admin_auth"),
            action: "auth_rejected",
            route: path,
            target_principal_id: None,
            target_upstream: None,
            api_key_id: None,
            status: 401,
            payload: Some(json!({
                "method": method.as_str(),
                "reason": reason,
            })),
        },
    )
    .await
}
