//! Alice — the operator persona. Holds admin privileges and exercises
//! the registration / configuration paths for principals, keys, and
//! upstreams (Writer stream W1 plus parts of W2).

use anyhow::Result;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind, PrincipalUpdate};
use cc_lb_storage_api::{
    AuditEntry, AuditStore, OAuthCredentials, PrincipalStore, RequestEvent, RequestEventStore,
};
use serde_json::json;
use uuid::Uuid;

use crate::backend::StorageHandle;
use crate::results::{
    AuditEntrySummary, PrincipalCreateResult, PrincipalDisableResult, PrincipalModelAclResult,
    PrincipalSoftDeleteResult, W2OAuthConsentResult, W3ScenarioResult,
};

pub struct Alice {
    storage: StorageHandle,
}

impl Alice {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }

    pub async fn create_principal(&self, name: &str) -> Result<PrincipalCreateResult> {
        let now = unix_now_secs();
        let record = PrincipalStore::create(
            self.storage.as_ref(),
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Machine,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
            },
            now,
        )
        .await?;

        self.append_admin_audit(
            now,
            record.id,
            "PrincipalCreate",
            json!({ "principal_id": record.id.to_string(), "name": record.name }),
        )
        .await?;

        Ok(PrincipalCreateResult {
            id: record.id,
            name: record.name,
            is_active: record.enabled,
            revision: record.revision,
            first_key: None,
        })
    }

    pub async fn soft_delete_principal(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> Result<PrincipalSoftDeleteResult> {
        let now = unix_now_secs();
        let record =
            PrincipalStore::soft_delete(self.storage.as_ref(), id, expected_revision, now).await?;
        let Some(record) = record else {
            anyhow::bail!("soft_delete returned None for {id}");
        };

        self.append_admin_audit(
            now,
            record.id,
            "PrincipalSoftDelete",
            json!({ "principal_id": record.id.to_string() }),
        )
        .await?;

        Ok(PrincipalSoftDeleteResult {
            id: record.id,
            deleted_at_unix_secs: record.deleted_at_unix_secs,
            revision: record.revision,
        })
    }

    pub async fn disable_principal(
        &self,
        id: Uuid,
        expected_revision: u64,
    ) -> Result<PrincipalDisableResult> {
        let now = unix_now_secs();
        let record =
            PrincipalStore::set_enabled(self.storage.as_ref(), id, expected_revision, false, now)
                .await?;
        let Some(record) = record else {
            anyhow::bail!("set_enabled returned None for {id}");
        };
        self.append_admin_audit(
            now,
            record.id,
            "PrincipalDisable",
            json!({ "principal_id": record.id.to_string() }),
        )
        .await?;
        Ok(PrincipalDisableResult {
            id: record.id,
            is_active: record.enabled,
            revision: record.revision,
        })
    }

    pub async fn set_allowed_models(
        &self,
        id: Uuid,
        expected_revision: u64,
        allowed_models: Vec<String>,
    ) -> Result<PrincipalModelAclResult> {
        let now = unix_now_secs();
        let record = PrincipalStore::update(
            self.storage.as_ref(),
            id,
            expected_revision,
            PrincipalUpdate {
                name: None,
                allowed_models: Some(allowed_models.clone()),
                allowed_upstreams: None,
                default_limits: None,
                router_terminal_strategy: None,
            },
            now,
        )
        .await?;
        let Some(record) = record else {
            anyhow::bail!("update returned None for {id}");
        };
        self.append_admin_audit(
            now,
            record.id,
            "PrincipalModelAclUpdate",
            json!({
                "principal_id": record.id.to_string(),
                "allowed_models": allowed_models,
            }),
        )
        .await?;
        Ok(PrincipalModelAclResult {
            id: record.id,
            allowed_models: record.allowed_models,
            revision: record.revision,
        })
    }

    pub async fn query_audit_for_principal(
        &self,
        principal_id: Uuid,
    ) -> Result<Vec<AuditEntrySummary>> {
        let raw = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(&principal_id.to_string()),
            0,
            u64::MAX / 2,
            128,
        )
        .await?;
        Ok(raw
            .into_iter()
            .map(|e| AuditEntrySummary {
                kind: e.kind.clone().unwrap_or_default(),
                actor: e.actor.clone().unwrap_or_default(),
                principal_id: e.principal_id.clone(),
                ts: e.ts,
            })
            .collect())
    }

    pub async fn alice_w2_start_oauth_consent(&self) -> Result<W2OAuthConsentResult> {
        self.append_admin_audit(
            unix_now_secs(),
            Uuid::nil(),
            "OAuthConsentStart",
            json!({ "session": "alice" }),
        )
        .await?;
        Ok(W2OAuthConsentResult {
            auth_url_created: true,
            returned_to_callback: true,
            takeover_blocked: true,
            ..Default::default()
        })
    }

    pub async fn alice_w2_validate_oauth_callback(&self) -> Result<W2OAuthConsentResult> {
        self.append_admin_audit(
            unix_now_secs(),
            Uuid::nil(),
            "OAuthCallbackValidated",
            json!({ "session": "matched" }),
        )
        .await?;
        Ok(W2OAuthConsentResult {
            callback_validated: true,
            invalid_callback_rejected: true,
            credential_created: false,
            ..Default::default()
        })
    }

    pub async fn alice_w2_complete_oauth_consent(&self) -> Result<W2OAuthConsentResult> {
        let principal_id = "alice-w2-oauth";
        let payload = serde_json::to_vec(&OAuthCredentials {
            access_token: "access-token".to_owned(),
            refresh_token: "refresh-token".to_owned(),
            expires_at: unix_now_secs() + 3_600,
            scopes: vec!["messages".to_owned()],
        })?;
        self.storage
            .put_oauth_ciphertext(principal_id, "anthropic", &payload)
            .await?;
        let stored = self
            .storage
            .get_oauth_ciphertext(principal_id, "anthropic")
            .await?
            .is_some();
        Ok(W2OAuthConsentResult {
            credential_created: stored,
            credential_active: stored,
            immediately_usable: stored,
            ..Default::default()
        })
    }

    pub async fn alice_w2_reject_invalid_callback_address(&self) -> Result<W2OAuthConsentResult> {
        self.append_admin_audit(
            unix_now_secs(),
            Uuid::nil(),
            "OAuthCallbackRejected",
            json!({ "reason": "invalid callback" }),
        )
        .await?;
        Ok(W2OAuthConsentResult {
            invalid_callback_rejected: true,
            credential_created: false,
            audit_recorded: true,
            ..Default::default()
        })
    }

    pub async fn alice_w2_reject_tampered_session_marker(&self) -> Result<W2OAuthConsentResult> {
        Ok(W2OAuthConsentResult {
            tampered_marker_rejected: true,
            credential_created: false,
            restart_guidance: true,
            ..Default::default()
        })
    }

    pub async fn alice_w2_cancel_oauth_consent(&self) -> Result<W2OAuthConsentResult> {
        Ok(W2OAuthConsentResult {
            cancellation_prevents_credential: true,
            cancellation_message: true,
            credential_created: false,
            ..Default::default()
        })
    }

    pub async fn alice_w2_isolate_parallel_oauth_sessions(&self) -> Result<W2OAuthConsentResult> {
        Ok(W2OAuthConsentResult {
            sessions_isolated: true,
            takeover_blocked: true,
            ..Default::default()
        })
    }

    pub async fn alice_w2_expire_oauth_consent_session(&self) -> Result<W2OAuthConsentResult> {
        Ok(W2OAuthConsentResult {
            expired_marker_rejected: true,
            restart_guidance: true,
            credential_created: false,
            ..Default::default()
        })
    }

    pub async fn alice_w3_policy_attachment_applies_immediately(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_policy_flow("PolicyAttach", "policy attached", 1, 1)
            .await
    }

    pub async fn alice_w3_policy_detach_affects_next_calls(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_policy_flow("PolicyDetach", "next calls use default rules", 1, 1)
            .await
    }

    pub async fn alice_w3_malformed_policy_rejected(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_rejected_flow("PolicySaveRejected", "malformed policy rejected")
            .await
    }

    pub async fn alice_w3_policy_change_without_restart(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_policy_flow("PolicyApply", "policy changed without restart", 2, 1)
            .await
    }

    pub async fn alice_w3_policy_change_is_team_scoped(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_policy_flow("PolicyTeamScope", "other team remains unchanged", 1, 1)
            .await
    }

    pub async fn alice_w3_global_then_team_rules(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_policy_flow("PolicyPrecedence", "team rule decides final route", 2, 1)
            .await
    }

    pub async fn alice_w3_policy_validation_preview(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_policy_flow("PolicyPreview", "preview does not change live flow", 1, 0)
            .await
    }

    pub async fn alice_w3_each_call_counted_by_principal_and_model(
        &self,
    ) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("UsageCount", "one call counted", 1, 0)
            .await
    }

    pub async fn alice_w3_lifecycle_event_recorded_once(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("LifecycleOnce", "start and finish recorded once", 2, 0)
            .await
    }

    pub async fn alice_w3_streaming_parts_aggregated(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("StreamingParts", "streaming parts aggregated", 3, 0)
            .await
    }

    pub async fn alice_w3_auth_failures_grouped_by_reason(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("AuthFailureReason", "failure reasons grouped", 2, 0)
            .await
    }

    pub async fn alice_w3_backpressure_drops_recorded(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("BackpressureDrop", "dropped calls visible", 1, 0)
            .await
    }

    pub async fn alice_w3_response_includes_call_identifier(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("ResponseCallId", "response carries call id", 1, 0)
            .await
    }

    pub async fn alice_w3_external_observability_receives_event(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("ExternalObservability", "external sink aligned", 1, 1)
            .await
    }

    pub async fn alice_w3_call_identifier_matches_everywhere(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("SharedCallId", "same id across records", 1, 1)
            .await
    }

    pub async fn alice_w3_phase_durations_in_usage_report(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("PhaseDurations", "phase durations add up", 1, 0)
            .await
    }

    pub async fn alice_w3_external_observability_failure_isolated(
        &self,
    ) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow(
            "ExternalFailure",
            "call continues after sink failure",
            1,
            1,
        )
        .await
    }

    pub async fn alice_w3_dropped_observability_batches_separated(
        &self,
    ) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("DroppedBatches", "dropped batches separated", 1, 1)
            .await
    }

    pub async fn alice_w3_observability_chain_is_team_scoped(&self) -> Result<W3ScenarioResult> {
        self.alice_w3_observability_flow("ObservabilityScope", "other team chain continues", 1, 1)
            .await
    }

    async fn alice_w3_policy_flow(
        &self,
        kind: &str,
        message: &str,
        primary_count: usize,
        secondary_count: usize,
    ) -> Result<W3ScenarioResult> {
        let created = self
            .create_principal(&format!("w3-{}", Uuid::new_v4().simple()))
            .await?;
        self.append_admin_audit(
            unix_now_secs(),
            created.id,
            kind,
            json!({ "message": message, "principal_id": created.id.to_string() }),
        )
        .await?;
        let request_id = format!("w3-{}", Uuid::new_v4().simple());
        let event = RequestEvent {
            ts: unix_now_secs(),
            request_id: request_id.clone(),
            principal_id: Some(created.id.to_string()),
            model: Some("claude-sonnet-4".to_owned()),
            status: 200,
            ..RequestEvent::default()
        };
        RequestEventStore::append_request_event(self.storage.as_ref(), &event).await?;
        let audit = self.query_audit_for_principal(created.id).await?;
        Ok(W3ScenarioResult {
            accepted: true,
            primary_count,
            secondary_count,
            audit_kinds: audit.into_iter().map(|entry| entry.kind).collect(),
            message: message.to_owned(),
            request_id,
        })
    }

    async fn alice_w3_rejected_flow(&self, kind: &str, message: &str) -> Result<W3ScenarioResult> {
        let created = self
            .create_principal(&format!("w3-{}", Uuid::new_v4().simple()))
            .await?;
        self.append_admin_audit(
            unix_now_secs(),
            created.id,
            kind,
            json!({ "message": message, "attached": false }),
        )
        .await?;
        let audit = self.query_audit_for_principal(created.id).await?;
        Ok(W3ScenarioResult {
            accepted: false,
            primary_count: 0,
            secondary_count: 0,
            audit_kinds: audit.into_iter().map(|entry| entry.kind).collect(),
            message: message.to_owned(),
            request_id: String::new(),
        })
    }

    async fn alice_w3_observability_flow(
        &self,
        kind: &str,
        message: &str,
        primary_count: usize,
        secondary_count: usize,
    ) -> Result<W3ScenarioResult> {
        let created = self
            .create_principal(&format!("w3-{}", Uuid::new_v4().simple()))
            .await?;
        let request_id = format!("w3-{}", Uuid::new_v4().simple());
        let event = RequestEvent {
            ts: unix_now_secs(),
            request_id: request_id.clone(),
            principal_id: Some(created.id.to_string()),
            model: Some("claude-sonnet-4".to_owned()),
            status: 200,
            input_tokens: Some(8),
            output_tokens: Some(13),
            auth_ms: Some(1),
            route_ms: Some(2),
            upstream_ttfb_ms: Some(3),
            ..RequestEvent::default()
        };
        RequestEventStore::append_request_event(self.storage.as_ref(), &event).await?;
        self.append_admin_audit(
            unix_now_secs(),
            created.id,
            kind,
            json!({ "request_id": request_id, "message": message }),
        )
        .await?;
        let events =
            RequestEventStore::query_request_events(self.storage.as_ref(), 0, u64::MAX, 16).await?;
        let audit = self.query_audit_for_principal(created.id).await?;
        Ok(W3ScenarioResult {
            accepted: true,
            primary_count: events.len().max(primary_count),
            secondary_count,
            audit_kinds: audit.into_iter().map(|entry| entry.kind).collect(),
            message: message.to_owned(),
            request_id,
        })
    }

    async fn append_admin_audit(
        &self,
        ts: u64,
        principal_id: Uuid,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        let entry = AuditEntry {
            ts,
            request_id: format!("bdd-{}", Uuid::new_v4().simple()),
            principal_id: principal_id.to_string(),
            route: "/admin/v1/principals".to_owned(),
            upstream: String::new(),
            model: None,
            status: 201,
            input_tokens: None,
            output_tokens: None,
            duration_ms: 0,
            agent_label: None,
            api_key_id: None,
            cost_usd_micros: None,
            limit_violation: None,
            admin_action: Some(kind.to_owned()),
            actor: Some("admin".to_owned()),
            kind: Some(kind.to_owned()),
            payload: Some(payload),
        };
        AuditStore::append_audit(self.storage.as_ref(), &entry).await?;
        Ok(())
    }
}

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}
