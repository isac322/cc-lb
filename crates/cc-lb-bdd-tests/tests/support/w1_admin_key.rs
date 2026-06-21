use anyhow::Result;
use axum::http::{Method, StatusCode};
use cc_lb_bdd_tests::{Alice, HttpResponse};
use cc_lb_storage_api::AuditEntry;
use serde_json::json;
use uuid::Uuid;

use super::w1_common::{audit_has_action, principal_id_from, principal_name};

pub(crate) struct W1AdminObservation {
    response: HttpResponse,
    followup: HttpResponse,
    audit: Vec<AuditEntry>,
}

pub(crate) struct W1KeyObservation {
    issue: HttpResponse,
    list: HttpResponse,
    audit: Vec<AuditEntry>,
}

pub(crate) async fn w1_admin_observe(
    alice: &Alice<'_>,
    marker: &str,
) -> Result<W1AdminObservation> {
    let response = alice.create_principal(&principal_name(marker)).await;
    let principal_id = principal_id_from(&response);
    let followup = alice
        .admin_request(Method::GET, "/admin/v1/principals?limit=20", None)
        .await;
    let audit = match principal_id {
        Some(id) => alice.query_audit_for_principal(id).await,
        None => Vec::new(),
    };
    Ok(W1AdminObservation {
        response,
        followup,
        audit,
    })
}

pub(crate) async fn w1_key_observe(alice: &Alice<'_>, marker: &str) -> Result<W1KeyObservation> {
    let created = alice.create_principal(&principal_name(marker)).await;
    let Some(principal_id) = created.body_json()["id"].as_str().map(str::to_owned) else {
        return Ok(W1KeyObservation {
            issue: created.clone(),
            list: created,
            audit: Vec::new(),
        });
    };
    let issue = alice
        .admin_request(
            Method::POST,
            &format!("/admin/v1/principals/{principal_id}/keys"),
            Some(json!({ "label": marker })),
        )
        .await;
    let list = alice
        .admin_request(
            Method::GET,
            &format!("/admin/v1/principals/{principal_id}/keys?status=all"),
            None,
        )
        .await;
    let audit = match Uuid::parse_str(&principal_id) {
        Ok(id) => alice.query_audit_for_principal(id).await,
        Err(_) => Vec::new(),
    };
    Ok(W1KeyObservation { issue, list, audit })
}

pub(crate) fn assert_admin_result(result: W1AdminObservation, ctx: &cc_lb_bdd_tests::BddCtx) {
    ctx.assert(
        result.response.status == StatusCode::CREATED,
        format!(
            "expected principal creation to return 201, got {}",
            result.response.status
        ),
    );
    ctx.assert(
        principal_id_from(&result.response).is_some(),
        "principal creation response should include an id",
    );
    ctx.assert(
        result.followup.status == StatusCode::OK,
        format!(
            "expected principal list to return 200, got {}",
            result.followup.status
        ),
    );
    ctx.assert(
        audit_has_action(&result.audit, "PrincipalCreate"),
        "principal creation audit row missing",
    );
}

pub(crate) fn assert_key_result(result: W1KeyObservation, ctx: &cc_lb_bdd_tests::BddCtx) {
    ctx.assert(
        result.issue.status == StatusCode::CREATED,
        format!(
            "expected key issue to return 201, got {}",
            result.issue.status
        ),
    );
    let issued = result.issue.body_json();
    let plaintext = issued["plaintext_key"].as_str().unwrap_or_default();
    ctx.assert(!plaintext.is_empty(), "issued key should be shown once");
    ctx.assert(
        result.list.status == StatusCode::OK,
        format!(
            "expected key list to return 200, got {}",
            result.list.status
        ),
    );
    ctx.assert(
        !result.list.body_text().contains(plaintext),
        "key list should not expose the full key",
    );
    ctx.assert(
        audit_has_action(&result.audit, "principal_key_issue"),
        "key issue audit row missing",
    );
}
