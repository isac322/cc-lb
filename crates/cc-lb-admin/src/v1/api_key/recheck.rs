//! `POST /admin/v1/upstreams/{id}/preflight`: re-probes an API-key upstream's
//! stored key and persists the capability snapshot (plan §1.10, S3b).
//!
//! Also holds the loaders the other stored-key handlers share: resolving the
//! upstream from its path id and decrypting its key.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cc_lb_control::AuditPayload;
use cc_lb_control::anthropic_api_key::{
    NormalizedApiKey,
    preflight::{PreflightInput, PreflightMode, run_preflight},
    remediation::{endpoint_operator_message, operator_message},
    resolve_stored_api_key,
    wire::PreflightVerdict,
};
use cc_lb_storage_api::{
    ApiKeyClass, ApiKeyOrgCapability, RemediationCode, Storage, StorageError, UpstreamKind,
    UpstreamRecord, UpstreamStore,
};
use uuid::Uuid;

use super::poll_enqueue::enqueue_usage_poll;
use super::preflight::{no_store, provider_native_preflight, verdict_label};
use crate::v1::post_commit::complete_after_commit;
use crate::v1::upstreams::{UpstreamError, audit_upstream_action, storage, upstream_base_url};
use crate::{AdminState, auth::AdminIdentity};

/// Live Anthropic API-key upstream named by a path id: 400 for a malformed
/// id, 404 for an unknown or soft-deleted one, 400 `not_api_key_upstream` for
/// OAuth and other provider kinds.
pub(super) async fn load_api_key_upstream(
    storage: &dyn Storage,
    id: &str,
) -> Result<UpstreamRecord, UpstreamError> {
    let record = load_api_key_or_provider_upstream(storage, id).await?;
    if record.kind != UpstreamKind::AnthropicApiKey {
        return Err(UpstreamError::NotApiKeyUpstream);
    }
    Ok(record)
}

pub(super) async fn load_api_key_or_provider_upstream(
    storage: &dyn Storage,
    id: &str,
) -> Result<UpstreamRecord, UpstreamError> {
    let upstream_id = id
        .parse::<Uuid>()
        .map_err(|error| UpstreamError::BadRequest {
            error: "invalid_upstream_id",
            detail: error.to_string(),
        })?;
    let record = UpstreamStore::get_by_id(storage, upstream_id)
        .await?
        .filter(|record| record.deleted_at_unix_secs.is_none())
        .ok_or(UpstreamError::NotFound)?;
    if !matches!(
        record.kind,
        UpstreamKind::AnthropicApiKey | UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        return Err(UpstreamError::NotApiKeyUpstream);
    }
    Ok(record)
}

/// The stored key, decrypted with the upstream id as AAD, exactly as the
/// signer sends it. Paste-time input rules do not apply: a key stored before
/// them still proxies, so every stored-key probe accepts it too. A key the
/// signer could not send either (missing, undecryptable, not a valid header
/// value, pre-#848 encoding) is the flat 500
/// `api_key_credential_decrypt_failed` and carries no key material.
pub(super) fn stored_api_key(
    state: &AdminState,
    record: &UpstreamRecord,
) -> Result<NormalizedApiKey, UpstreamError> {
    resolve_stored_api_key(&state.aead, record).map_err(|error| {
        tracing::warn!(upstream_id = %record.id, ?error, "stored api key could not be resolved");
        UpstreamError::ApiKeyCredentialDecrypt
    })
}

pub(crate) async fn recheck_upstream_api_key(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path(id): Path<String>,
) -> Result<Response, UpstreamError> {
    let storage = storage(&state)?;
    let record = load_api_key_or_provider_upstream(storage, &id).await?;
    let key = stored_api_key(&state, &record)?;
    let base_url = upstream_base_url(&record)?;
    let previous_usage_report = storage
        .get_api_key_details(record.id)
        .await?
        .and_then(|details| details.usage_report_state);

    let now_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    let mut outcome = if matches!(
        record.kind,
        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        provider_native_preflight(&state, &key, &base_url, record.kind, now_unix_secs).await
    } else {
        run_preflight(
            &state.anthropic_org_api,
            PreflightInput {
                key: &key,
                base_url: &base_url,
                workspace_id: None,
                mode: PreflightMode::StoredKey {
                    stored_workspace_id: record.workspace_id.as_deref(),
                },
                now_unix_secs,
            },
        )
        .await
    };
    drop(key);
    // A stored key that is now refused (401/403) leaves the operator a
    // reason on the Access row; the probe only phrases transport failures.
    // The reason names Anthropic only when the key was checked there.
    if outcome.access.last_error.is_none()
        && let Some(primary) = outcome.wire.remediation.first()
        && outcome.wire.verdict == PreflightVerdict::Rejected
    {
        let on_anthropic = state.anthropic_org_api.applies_to(&base_url);
        outcome.access.last_error =
            Some(endpoint_operator_message(primary.code, on_anthropic).to_owned());
    }

    // The snapshot is written against the revision the probe ran for. A PUT
    // that landed meanwhile (new key, workspace, or host) makes the write a
    // Conflict and this result is discarded (CONC-02/03); the answer is still
    // the probe's verdict.
    let persisted = if outcome.transport.is_some() {
        // Unreachable keeps the previous capability snapshot (K3).
        let last_error = outcome
            .access
            .last_error
            .as_deref()
            .unwrap_or(operator_message(RemediationCode::ProbeUnreachable));
        storage
            .record_api_key_probe_failure(
                record.id,
                record.revision,
                last_error,
                &outcome.access.remediation,
                outcome.access.last_attempt_at_unix_secs,
            )
            .await
    } else {
        storage
            .write_api_key_access(record.id, record.revision, &outcome.access)
            .await
    };
    let persisted = match persisted {
        Ok(()) => true,
        Err(StorageError::Conflict { .. }) => {
            tracing::info!(
                upstream_id = %record.id,
                "re-check result discarded: the upstream changed while probing"
            );
            false
        }
        Err(error) => return Err(error.into()),
    };

    // The snapshot write (or its discard) is settled: the poll enqueue and the
    // audit outlive a dropped request.
    complete_after_commit(async move {
        // R2-4: a Re-check that turns usage reports back on polls at once.
        if persisted
            && outcome.access.key_class == ApiKeyClass::OrgScoped
            && outcome.access.usage_report_state == Some(ApiKeyOrgCapability::Ok)
            && previous_usage_report != Some(ApiKeyOrgCapability::Ok)
            && let Some(checked_at_unix_secs) = outcome.access.checked_at_unix_secs
        {
            enqueue_usage_poll(&state, record.id, record.revision, checked_at_unix_secs).await;
        }

        let payload = AuditPayload::UpstreamRecheck {
            upstream_id: record.id.to_string(),
            key_hint: outcome.wire.key_hint.clone(),
            verdict: verdict_label(outcome.wire.verdict).to_owned(),
            remediation: outcome
                .wire
                .remediation
                .iter()
                .map(|remediation| remediation.code.as_str())
                .collect(),
        };
        let route = format!("/admin/v1/upstreams/{}/preflight", record.id);
        audit_upstream_action(
            &state,
            &identity,
            &payload,
            &route,
            Some(&record.name),
            StatusCode::OK,
        )
        .await?;

        Ok(no_store(Json(outcome.wire).into_response()))
    })
    .await?
}
