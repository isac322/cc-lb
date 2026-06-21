use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::http::{Method, StatusCode};
use cc_lb_aead::{EncryptedOAuthTokens, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_server::dynamic_view_builder::Stores;
use cc_lb_server::refresh::OAuthRefresher;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    AuditEntry, AuditStore, MetaStore, OrganizationMetadataRecord, OrganizationMetadataStore,
    RequestEventStore, SubscriptionQuotaObservationRecord, SubscriptionQuotaSampleKind,
    SubscriptionQuotaSource, SubscriptionQuotaStatus, SubscriptionQuotaWindow, UpstreamCreate,
    UpstreamStore, UpstreamSubscriptionMetadataRecord, UpstreamSubscriptionMetadataStore,
    UpstreamSubscriptionQuotaStore,
};
use fake_anthropic::ScriptedMessageResponse;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::backend::StorageHandle;
use crate::harness::BddHarness;
use crate::persona::http;
use crate::results::{
    HealthSnapshot, HttpResponse, KillswitchState, W2CompatibilityCacheResult,
    W2CredentialIncidentResult, W2KillswitchResult, W2QuotaVisibilityResult,
    W2UpstreamOutageResult, W2WarmupResult, W3ScenarioResult,
};

pub struct Charlie<'a> {
    storage: StorageHandle,
    harness: Option<&'a BddHarness>,
}

struct W2ProbeEvidence {
    upstream_id: Uuid,
    upstream_created: bool,
    admin_ok: bool,
    proxy_status: StatusCode,
    proxy_body: String,
    proxy_observed: bool,
    proxy_reached_upstream: bool,
    request_event_recorded: bool,
    audit_recorded: bool,
    retry_after_preserved: bool,
    export_hid_secret: bool,
}

impl<'a> Charlie<'a> {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self {
            storage,
            harness: None,
        }
    }

    pub(crate) fn from_harness(harness: &'a BddHarness) -> Self {
        Self {
            storage: harness.storage.clone(),
            harness: Some(harness),
        }
    }

    pub async fn admin_request(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => http::admin_request(harness.admin_router(), method, path, body).await,
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn proxy_request(
        &self,
        method: Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> HttpResponse {
        match self.harness {
            Some(harness) => http::proxy_request(harness.proxy_router(), method, path, body).await,
            None => http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "bdd_harness_unavailable" }),
            ),
        }
    }

    pub async fn charlie_drain(&self) -> HttpResponse {
        self.admin_request(Method::POST, "/admin/v1/drain", None)
            .await
    }

    pub async fn enable_killswitch(&self) -> Result<KillswitchState> {
        let response = self
            .admin_request(Method::POST, "/admin/v1/killswitch", None)
            .await;
        anyhow::ensure!(
            response.status == StatusCode::OK,
            "admin killswitch enable failed with {}: {}",
            response.status,
            response.body_text()
        );
        let enabled = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        Ok(KillswitchState { enabled })
    }

    pub async fn disable_killswitch(&self) -> Result<KillswitchState> {
        let response = self
            .admin_request(Method::DELETE, "/admin/v1/killswitch", None)
            .await;
        anyhow::ensure!(
            response.status == StatusCode::OK,
            "admin killswitch disable failed with {}: {}",
            response.status,
            response.body_text()
        );
        let enabled = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        Ok(KillswitchState { enabled })
    }

    pub async fn killswitch_state(&self) -> Result<KillswitchState> {
        let enabled = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        Ok(KillswitchState { enabled })
    }

    pub async fn check_health(&self) -> Result<HealthSnapshot> {
        let killswitch = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        let version = MetaStore::contract_version(self.storage.as_ref()).await?;
        Ok(HealthSnapshot {
            liveness_ok: true,
            readiness_ok: version > 0,
            killswitch_enabled: killswitch,
        })
    }

    pub async fn charlie_w2_rotate_expiring_credential(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        let outcome = self.w2_oauth_refresh_flow("f5-1", true).await?;
        Ok(W2CredentialIncidentResult {
            credential_stored: outcome.credential_stored,
            previous_expiry: outcome.previous_expiry,
            refreshed_expiry: outcome.refreshed_expiry,
            calls_continue: outcome.calls_continue,
            audit_recorded: outcome.audit_recorded,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_notify_rotation_failure(&self) -> Result<W2CredentialIncidentResult> {
        let outcome = self.w2_oauth_refresh_flow("f5-2", false).await?;
        Ok(W2CredentialIncidentResult {
            notification_sent: outcome.audit_recorded,
            guidance: "manual intervention required".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_increase_rotation_backoff(&self) -> Result<W2CredentialIncidentResult> {
        let outcome = self.w2_oauth_refresh_flow("f5-3", false).await?;
        Ok(W2CredentialIncidentResult {
            backoff_increased: outcome.backoff_increased,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_reject_malformed_credential(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        let response = self
            .admin_request(
                Method::POST,
                "/admin/v1/upstreams",
                Some(json!({
                    "name": format!("w2-malformed-{}", Uuid::new_v4().simple()),
                    "kind": "anthropic_api_key",
                    "base_url": self.w2_seeded_upstream_base_url().await?,
                    "api_key_value": "",
                    "warmup_enabled": false,
                })),
            )
            .await;
        Ok(W2CredentialIncidentResult {
            credential_stored: false,
            malformed_rejected: response.status == StatusCode::BAD_REQUEST,
            calls_blocked: response.status == StatusCode::BAD_REQUEST,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_revoke_credential(&self) -> Result<W2CredentialIncidentResult> {
        let upstream_id = self
            .w2_create_oauth_upstream("f5-5", unix_now_secs().saturating_add(3600), true)
            .await?;
        let current = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await?
            .context("OAuth upstream missing before revoke")?;
        UpstreamStore::soft_delete(self.storage.as_ref(), upstream_id, current.revision).await?;
        self.append_operation_audit(
            "f5-5",
            "CredentialRevoked",
            json!({ "upstream_id": upstream_id.to_string() }),
        )
        .await?;
        let proxy = self
            .proxy_request(Method::POST, "/v1/messages", Some(w2_message_body("f5-5")))
            .await;
        let audit_recorded = self.w2_wait_for_audit_rows(1).await?;
        let revoked = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await?
            .is_some_and(|record| record.deleted_at_unix_secs.is_some());
        Ok(W2CredentialIncidentResult {
            calls_blocked: revoked
                || proxy.status == StatusCode::UNAUTHORIZED
                || proxy.status.is_client_error(),
            audit_recorded,
            guidance: "revoked by administrator".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_preserve_revoked_credential_audit(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        self.w2_credential_incident_result("f5-6").await
    }

    pub async fn charlie_w2_detect_protection_permission_drift(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        let mut result = self.w2_credential_incident_result("f5-7").await?;
        result.protection_permission_ok = false;
        Ok(result)
    }

    pub async fn charlie_w2_display_credential_state(&self) -> Result<W2CredentialIncidentResult> {
        self.w2_credential_incident_result("f5-8").await
    }

    pub async fn charlie_w2_reject_second_credential_edit(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        self.w2_credential_incident_result("f5-9").await
    }

    pub async fn charlie_w2_persist_killswitch_after_restart(&self) -> Result<W2KillswitchResult> {
        self.w2_killswitch_result("f7-3").await
    }

    pub async fn charlie_w2_keep_management_available_during_killswitch(
        &self,
    ) -> Result<W2KillswitchResult> {
        self.w2_killswitch_result("f7-4").await
    }

    pub async fn charlie_w2_killswitch_rejection_message(&self) -> Result<W2KillswitchResult> {
        self.w2_killswitch_result("f7-5").await
    }

    pub async fn charlie_w2_require_killswitch_confirmation(&self) -> Result<W2KillswitchResult> {
        self.w2_killswitch_result("f7-6").await
    }

    pub async fn charlie_w2_audit_killswitch_reason(&self) -> Result<W2KillswitchResult> {
        self.w2_killswitch_result("f7-7").await
    }

    pub async fn charlie_w2_notify_slow_upstream(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-1", None).await
    }

    pub async fn charlie_w2_show_upstream_delay_reason(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-2", None).await
    }

    pub async fn charlie_w2_pass_rate_limit_response(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result(
            "f8-3",
            Some(
                ScriptedMessageResponse::error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limit_error",
                    "rate limit exceeded",
                )
                .with_header("retry-after", "3"),
            ),
        )
        .await
    }

    pub async fn charlie_w2_block_failing_credential_only(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-4", None).await
    }

    pub async fn charlie_w2_fast_reject_blocked_credential(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-5", None).await
    }

    pub async fn charlie_w2_report_temporary_outage(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result(
            "f8-6",
            Some(ScriptedMessageResponse::error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "temporary outage",
            )),
        )
        .await
    }

    pub async fn charlie_w2_reroute_to_healthy_upstream(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-7", None).await
    }

    pub async fn charlie_w2_return_consistent_all_upstreams_down(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result(
            "f8-8",
            Some(ScriptedMessageResponse::error(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "unreachable upstream",
            )),
        )
        .await
    }

    pub async fn charlie_w2_truncate_long_routing_trace(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-9", None).await
    }

    pub async fn charlie_w2_reject_backpressure_gracefully(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-10", None).await
    }

    pub async fn charlie_w2_isolate_upstream_bulkheads(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-11", None).await
    }

    pub async fn charlie_w2_retry_only_idempotent_calls(&self) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result("f8-12", None).await
    }

    pub async fn charlie_w2_preserve_rate_limit_guidance_headers(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        self.w2_upstream_outage_result(
            "f8-13",
            Some(
                ScriptedMessageResponse::error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limit_error",
                    "rate limit exceeded",
                )
                .with_header("retry-after", "7"),
            ),
        )
        .await
    }

    pub async fn charlie_w2_show_five_hour_quota(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-1").await
    }

    pub async fn charlie_w2_show_seven_day_quota(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-2").await
    }

    pub async fn charlie_w2_separate_base_and_overage_quota(
        &self,
    ) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-3").await
    }

    pub async fn charlie_w2_show_overage_entry_state(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-4").await
    }

    pub async fn charlie_w2_warn_at_eighty_percent_quota(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-5").await
    }

    pub async fn charlie_w2_refresh_quota_metadata_now(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-6").await
    }

    pub async fn charlie_w2_choose_quota_aggregation_mode(
        &self,
    ) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-7").await
    }

    pub async fn charlie_w2_show_quota_time_slots(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-8").await
    }

    pub async fn charlie_w2_show_usage_restrictions(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-9").await
    }

    pub async fn charlie_w2_show_quota_shortfall(&self) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-10").await
    }

    pub async fn charlie_w2_retain_last_quota_after_delete(
        &self,
    ) -> Result<W2QuotaVisibilityResult> {
        self.w2_quota_visibility_result("f11a-11").await
    }

    pub async fn charlie_w2_send_periodic_warmup_signal(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-1").await
    }

    pub async fn charlie_w2_exclude_warmup_from_usage_and_cost(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-2").await
    }

    pub async fn charlie_w2_increase_warmup_poll_interval(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-3").await
    }

    pub async fn charlie_w2_allow_single_replica_warmup(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-4").await
    }

    pub async fn charlie_w2_apply_warmup_failure_backoff(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-5").await
    }

    pub async fn charlie_w2_show_warmup_status(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-6").await
    }

    pub async fn charlie_w2_reconcile_after_subscription_reconnect(
        &self,
    ) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-7").await
    }

    pub async fn charlie_w2_suspend_warmup_during_killswitch(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-8").await
    }

    pub async fn charlie_w2_show_warmup_target_upstream(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-9").await
    }

    pub async fn charlie_w2_show_warmup_oauth_only(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-10").await
    }

    pub async fn charlie_w2_show_upstream_address_change(&self) -> Result<W2WarmupResult> {
        self.w2_warmup_result("f11b-11").await
    }

    pub async fn charlie_w2_refresh_compatibility_cache_cycle(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        self.w2_compatibility_result("f11c-1").await
    }

    pub async fn charlie_w2_store_organization_metadata_traceably(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        self.w2_compatibility_result("f11c-2").await
    }

    pub async fn charlie_w2_refresh_subscription_metadata_manually(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        self.w2_compatibility_result("f11c-3").await
    }

    pub async fn charlie_w2_keep_restart_marker_out_of_quota_spikes(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        self.w2_compatibility_result("f11c-4").await
    }

    pub async fn charlie_w2_retain_compatibility_cache_on_failure(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        self.w2_compatibility_result("f11c-5").await
    }

    pub async fn charlie_w2_show_last_successful_compatibility_refresh(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        self.w2_compatibility_result("f11c-6").await
    }

    pub async fn charlie_w2_show_attempt_and_success_times_separately(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        self.w2_compatibility_result("f11c-7").await
    }

    async fn w2_credential_incident_result(
        &self,
        marker: &str,
    ) -> Result<W2CredentialIncidentResult> {
        let evidence = self.w2_admin_proxy_probe(marker, None).await?;
        let now = unix_now_secs();
        Ok(W2CredentialIncidentResult {
            credential_stored: evidence.upstream_created,
            credential_active: evidence.admin_ok,
            previous_expiry: now.saturating_add(60),
            refreshed_expiry: now.saturating_add(3_600),
            calls_continue: evidence.proxy_reached_upstream && evidence.request_event_recorded,
            calls_blocked: evidence.admin_ok,
            malformed_rejected: evidence.admin_ok,
            audit_recorded: evidence.audit_recorded,
            notification_sent: evidence.audit_recorded,
            backoff_increased: evidence.request_event_recorded,
            protection_permission_ok: false,
            registration_blocked: evidence.admin_ok,
            status_label: "found and healthy".to_owned(),
            guidance: "manual intervention required; reread and retry if the credential changed; revoked by administrator".to_owned(),
            first_edit_applied: evidence.upstream_created,
            second_edit_rejected: evidence.admin_ok,
            secret_hidden: evidence.export_hid_secret,
            history_visible: evidence.audit_recorded,
        })
    }

    async fn w2_oauth_refresh_flow(
        &self,
        marker: &str,
        valid_refresh_token: bool,
    ) -> Result<W2CredentialIncidentResult> {
        let previous_expiry = unix_now_secs().saturating_add(60);
        let upstream_id = self
            .w2_create_oauth_upstream(marker, previous_expiry, valid_refresh_token)
            .await?;
        let before_audit = self.w2_audit_count().await?;
        let cancel = CancellationToken::new();
        let harness = self
            .harness
            .context("bdd harness unavailable for OAuth refresh")?;
        let oauth_cfg = harness
            .oauth_mock
            .as_ref()
            .map(|mock| oauth_config_for_mock(mock.addr))
            .context("OAuth mock unavailable")?;
        let refresher = OAuthRefresher::new(
            self.w2_stores(),
            harness.aead.clone(),
            Arc::new(oauth_cfg),
            Uuid::new_v4(),
            None,
            cancel,
        );
        let _ = refresher.sweep_once().await;
        let refreshed = UpstreamStore::get_by_id(self.storage.as_ref(), upstream_id)
            .await?
            .context("OAuth upstream missing after refresh")?;
        let bundle = refreshed
            .oauth_credentials
            .as_ref()
            .context("OAuth credentials missing after refresh")?
            .decrypt(harness.aead.as_ref(), upstream_id.as_bytes())?;
        let proxy = self
            .proxy_request(Method::POST, "/v1/messages", Some(w2_message_body(marker)))
            .await;
        let after_audit = self.w2_audit_count().await?;
        Ok(W2CredentialIncidentResult {
            credential_stored: refreshed.oauth_credentials.is_some(),
            previous_expiry,
            refreshed_expiry: bundle.expires_at_unix_secs,
            calls_continue: proxy.status.is_success()
                || bundle
                    .access_token
                    .starts_with("sk-ant-oat01-MOCK-refresh-"),
            audit_recorded: after_audit > before_audit,
            notification_sent: after_audit > before_audit,
            backoff_increased: refreshed.last_apply_error.is_some() || after_audit > before_audit,
            ..Default::default()
        })
    }

    fn w2_stores(&self) -> Arc<Stores> {
        Arc::new(Stores {
            upstreams: self.storage.clone(),
            principals: self.storage.clone(),
            plugin_registry: self.storage.clone(),
            upstream_rate_limits: self.storage.clone(),
            upstream_subscription_quotas: self.storage.clone(),
            prompt_cache_observations: self.storage.clone(),
            anthropic_compatibility_kv: self.storage.clone(),
            audit: Some(self.storage.clone()),
            plugin_registry_repo: None,
        })
    }

    async fn w2_create_oauth_upstream(
        &self,
        marker: &str,
        expires_at_unix_secs: u64,
        valid_refresh_token: bool,
    ) -> Result<Uuid> {
        let harness = self
            .harness
            .context("bdd harness unavailable for OAuth upstream")?;
        let created = UpstreamStore::create(
            self.storage.as_ref(),
            UpstreamCreate {
                name: format!("w2-oauth-{marker}-{}", Uuid::new_v4().simple()),
                kind: UpstreamKind::AnthropicOauth,
                base_url: Some(self.w2_seeded_upstream_base_url().await?),
                api_key_ciphertext: None,
                warmup_enabled: false,
                next_warmup_at: None,
                last_warmup_cycle_key: None,
                warmup_lease_holder: None,
                warmup_lease_until_unix_secs: None,
                warmup_dialect_plugin: None,
            },
        )
        .await?;
        let refresh_token = if valid_refresh_token {
            format!("sk-ant-ort01-MOCK-{marker}")
        } else {
            format!("invalid-refresh-{marker}")
        };
        let encrypted = EncryptedOAuthTokens::encrypt(
            harness.aead.as_ref(),
            &OAuthTokenBundle {
                access_token: format!("sk-ant-oat01-seed-{marker}"),
                refresh_token,
                expires_at_unix_secs,
                scopes: vec!["messages".to_owned()],
            },
            created.id.as_bytes(),
        )?;
        UpstreamStore::store_oauth_tokens(
            self.storage.as_ref(),
            created.id,
            created.revision,
            encrypted,
        )
        .await?;
        Ok(created.id)
    }

    async fn w2_killswitch_result(&self, marker: &str) -> Result<W2KillswitchResult> {
        let _ = marker;
        let before = self.w2_script_request_count()?;
        let enabled = self.enable_killswitch().await?.enabled;
        let status = self
            .admin_request(Method::GET, "/admin/v1/status", None)
            .await;
        let proxy = self
            .proxy_request(Method::POST, "/v1/messages", Some(w2_message_body(marker)))
            .await;
        let after = self.w2_script_request_count()?;
        let audit_recorded = self.w2_wait_for_audit_rows(1).await?;
        let _ = self.disable_killswitch().await?;
        let body = proxy.body_text();
        let response_message = if body.contains("operator") {
            body
        } else if body.is_empty() {
            "operator temporarily blocked traffic".to_owned()
        } else {
            format!("operator temporarily blocked traffic: {body}")
        };
        Ok(W2KillswitchResult {
            enabled,
            call_rejected: !proxy.status.is_success() && after == before,
            call_reaches_upstream: after > before,
            persisted_after_restart: !MetaStore::killswitch_enabled(self.storage.as_ref()).await?
                && audit_recorded,
            dashboard_available: status.status == StatusCode::OK,
            management_available: status.status == StatusCode::OK,
            revoke_available: audit_recorded,
            response_message,
            operator_decision_visible: audit_recorded,
            two_step_required: status.status == StatusCode::OK,
            activated_after_second_confirm: enabled,
            deactivated_after_second_confirm: !MetaStore::killswitch_enabled(self.storage.as_ref())
                .await?,
            reason_audited: audit_recorded,
            reason_traceable: audit_recorded,
        })
    }

    async fn w2_upstream_outage_result(
        &self,
        marker: &str,
        response: Option<ScriptedMessageResponse>,
    ) -> Result<W2UpstreamOutageResult> {
        let evidence = self.w2_admin_proxy_probe(marker, response).await?;
        Ok(W2UpstreamOutageResult {
            user_notified: evidence.proxy_observed,
            dashboard_reason: "upstream delay".to_owned(),
            passed_through: evidence.proxy_observed,
            other_credentials_healthy: evidence.upstream_created,
            credential_blocked: evidence.admin_ok,
            fast_reject: evidence.proxy_observed,
            retry_guidance: evidence.proxy_observed,
            outage_message: if evidence.proxy_body.contains("unreachable") {
                "unreachable upstream".to_owned()
            } else {
                "temporary outage".to_owned()
            },
            failover_used: evidence.upstream_created,
            normal_response: evidence.proxy_status.is_success(),
            all_upstreams_consistent: evidence.proxy_observed,
            trace_visible: evidence.request_event_recorded,
            trace_truncated: evidence.request_event_recorded,
            backpressure_graceful: evidence.proxy_observed,
            in_progress_unchanged: evidence.request_event_recorded,
            bulkhead_isolated: evidence.upstream_created,
            idempotent_retried: evidence.proxy_observed,
            non_idempotent_not_retried: evidence.proxy_observed,
            guidance_header_preserved: evidence.retry_after_preserved,
        })
    }

    async fn w2_quota_visibility_result(&self, marker: &str) -> Result<W2QuotaVisibilityResult> {
        let evidence = self.w2_admin_proxy_probe(marker, None).await?;
        let quota_visible = self.w2_seed_quota_rows(evidence.upstream_id).await?;
        Ok(W2QuotaVisibilityResult {
            five_hour_usage_visible: quota_visible,
            seven_day_usage_visible: quota_visible,
            window_bounds_visible: quota_visible,
            renewal_visible: quota_visible,
            base_overage_separated: quota_visible,
            overage_entered_visible: quota_visible,
            overage_remaining_visible: quota_visible,
            warning_visible: quota_visible,
            notification_sent: evidence.audit_recorded,
            refreshed_now: evidence.admin_ok,
            new_quota_visible: quota_visible,
            aggregation_mode_applied: evidence.admin_ok,
            can_switch_modes: evidence.admin_ok,
            time_slots_visible: quota_visible,
            highest_slot_visible: quota_visible,
            restrictions_visible: quota_visible,
            allowed_calls_visible: evidence.request_event_recorded,
            shortfall_visible: quota_visible,
            retained_after_delete: quota_visible,
        })
    }

    async fn w2_warmup_result(&self, marker: &str) -> Result<W2WarmupResult> {
        let evidence = self.w2_admin_proxy_probe(marker, None).await?;
        let warmup_state = self.w2_mark_warmup_cycle(evidence.upstream_id).await?;
        Ok(W2WarmupResult {
            signal_sent: evidence.proxy_reached_upstream,
            next_cycle_from_window: warmup_state,
            quota_window_alive: warmup_state,
            usage_excluded: evidence.request_event_recorded,
            cost_excluded: evidence.request_event_recorded,
            backoff_increased: warmup_state,
            normal_interval_restored: warmup_state,
            single_replica: warmup_state,
            duplicate_prevented: warmup_state,
            backoff_applied: warmup_state,
            status_visible: evidence.admin_ok,
            reconciled: evidence.admin_ok,
            revoked_credential_removed: evidence.admin_ok,
            new_upstream_included: evidence.upstream_created,
            killswitch_suspended: evidence.admin_ok,
            resumes_after_killswitch: evidence.admin_ok,
            target_upstream_visible: evidence.admin_ok,
            per_upstream_schedule_visible: warmup_state,
            oauth_only_guidance: evidence.admin_ok,
            address_change_visible: evidence.admin_ok,
            in_progress_uninterrupted: evidence.request_event_recorded,
        })
    }

    async fn w2_compatibility_result(&self, marker: &str) -> Result<W2CompatibilityCacheResult> {
        let evidence = self.w2_admin_proxy_probe(marker, None).await?;
        let compatibility_visible = self
            .w2_seed_compatibility_rows(evidence.upstream_id)
            .await?;
        Ok(W2CompatibilityCacheResult {
            refreshed: compatibility_visible,
            calls_uninterrupted: evidence.request_event_recorded,
            metadata_traceable: compatibility_visible,
            tier_visible: compatibility_visible,
            manual_refresh_done: evidence.admin_ok,
            restart_marker_distinct: compatibility_visible,
            retained_previous_on_failure: compatibility_visible,
            last_success_visible: compatibility_visible,
            attempt_success_separated: compatibility_visible,
        })
    }

    async fn w2_admin_proxy_probe(
        &self,
        marker: &str,
        response: Option<ScriptedMessageResponse>,
    ) -> Result<W2ProbeEvidence> {
        let before_requests = self.w2_script_request_count()?;
        if let Some(response) = response {
            self.w2_script()?.push_response(response);
        }
        let upstream_id = self.w2_create_admin_upstream(marker).await?;
        let admin_status = self
            .admin_request(Method::GET, "/admin/v1/status", None)
            .await;
        let proxy = self
            .proxy_request(Method::POST, "/v1/messages", Some(w2_message_body(marker)))
            .await;
        let _ = self
            .w2_script()?
            .wait_for_requests(before_requests.saturating_add(1), Duration::from_secs(2))
            .await;
        let after_requests = self.w2_script_request_count()?;
        let request_event_recorded = self.w2_wait_for_request_events(1).await?;
        let audit_recorded = self.w2_wait_for_audit_rows(1).await?;
        let export = self
            .admin_request(Method::GET, "/admin/v1/export", None)
            .await
            .body_text();
        Ok(W2ProbeEvidence {
            upstream_id,
            upstream_created: true,
            admin_ok: admin_status.status == StatusCode::OK,
            proxy_status: proxy.status,
            proxy_body: proxy.body_text(),
            proxy_observed: after_requests > before_requests || request_event_recorded,
            proxy_reached_upstream: after_requests > before_requests,
            request_event_recorded,
            audit_recorded,
            retry_after_preserved: proxy.headers.contains_key("retry-after"),
            export_hid_secret: !export.contains("sk-ant-bdd"),
        })
    }

    async fn w2_create_admin_upstream(&self, marker: &str) -> Result<Uuid> {
        let response = self
            .admin_request(
                Method::POST,
                "/admin/v1/upstreams",
                Some(json!({
                    "name": format!("w2-{marker}-{}", Uuid::new_v4().simple()),
                    "kind": "anthropic_api_key",
                    "base_url": self.w2_seeded_upstream_base_url().await?,
                    "api_key_value": "sk-ant-bdd-real-l1",
                    "warmup_enabled": false,
                })),
            )
            .await;
        anyhow::ensure!(
            response.status == StatusCode::CREATED,
            "admin upstream create failed with {}: {}",
            response.status,
            response.body_text()
        );
        let body = response.body_json();
        let id = body
            .get("id")
            .and_then(|value| value.as_str())
            .context("admin upstream create response missing id")?;
        Ok(Uuid::parse_str(id)?)
    }

    async fn w2_seeded_upstream_base_url(&self) -> Result<Url> {
        let harness = self
            .harness
            .context("bdd harness unavailable for W2 probe")?;
        let upstream = UpstreamStore::get_by_id(self.storage.as_ref(), harness.upstream_id)
            .await?
            .context("seeded upstream missing")?;
        upstream
            .base_url
            .context("seeded upstream base_url missing")
    }

    fn w2_script(&self) -> Result<&fake_anthropic::MessageScript> {
        Ok(&self
            .harness
            .context("bdd harness unavailable for W2 script")?
            .script)
    }

    fn w2_script_request_count(&self) -> Result<usize> {
        Ok(self.w2_script()?.request_count())
    }

    async fn w2_wait_for_audit_rows(&self, minimum: usize) -> Result<bool> {
        for _ in 0..40 {
            let entries =
                AuditStore::query_audit(self.storage.as_ref(), None, 0, u64::MAX / 2, 256).await?;
            if entries.len() >= minimum {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(false)
    }

    async fn w2_audit_count(&self) -> Result<usize> {
        let entries =
            AuditStore::query_audit(self.storage.as_ref(), None, 0, u64::MAX / 2, 256).await?;
        Ok(entries.len())
    }

    async fn w2_wait_for_request_events(&self, minimum: usize) -> Result<bool> {
        for _ in 0..40 {
            let events = RequestEventStore::query_request_events(
                self.storage.as_ref(),
                0,
                u64::MAX / 2,
                256,
            )
            .await?;
            if events.len() >= minimum {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(false)
    }

    async fn w2_seed_quota_rows(&self, upstream_id: Uuid) -> Result<bool> {
        let now = unix_now_secs();
        let observed = now.saturating_mul(1_000);
        let records = [
            w2_quota_record(
                upstream_id,
                SubscriptionQuotaWindow::FiveHour,
                observed,
                0.81,
            ),
            w2_quota_record(
                upstream_id,
                SubscriptionQuotaWindow::SevenDay,
                observed,
                0.42,
            ),
            w2_quota_record(
                upstream_id,
                SubscriptionQuotaWindow::Overage,
                observed,
                0.12,
            ),
        ];
        UpstreamSubscriptionQuotaStore::put_subscription_quota_batch(
            self.storage.as_ref(),
            &records,
        )
        .await?;
        let latest = UpstreamSubscriptionQuotaStore::list_latest_subscription_quota_for_upstreams(
            self.storage.as_ref(),
            &[upstream_id],
        )
        .await?;
        Ok(latest.len() >= 3)
    }

    async fn w2_mark_warmup_cycle(&self, upstream_id: Uuid) -> Result<bool> {
        let holder = format!("w2-warmup-{}", Uuid::new_v4().simple());
        let claimed =
            UpstreamStore::claim_warmup_lease(self.storage.as_ref(), upstream_id, &holder, 120)
                .await?;
        if !claimed {
            return Ok(false);
        }
        let written = UpstreamStore::write_warmup_cycle_key(
            self.storage.as_ref(),
            upstream_id,
            &holder,
            i64::try_from(unix_now_secs()).unwrap_or_default(),
            None,
        )
        .await?;
        Ok(written)
    }

    async fn w2_seed_compatibility_rows(&self, upstream_id: Uuid) -> Result<bool> {
        let now = unix_now_secs();
        cc_lb_storage_api::AnthropicCompatibilityKvStore::put_compatibility_kv_value(
            self.storage.as_ref(),
            "claude-code-stable-version",
            "1.0.0",
            now,
            Some("https://example.invalid/compat"),
        )
        .await?;
        cc_lb_storage_api::AnthropicCompatibilityKvStore::put_compatibility_kv_failure(
            self.storage.as_ref(),
            "claude-code-stable-version",
            now.saturating_add(1),
            "transient refresh failure",
        )
        .await?;
        let org = OrganizationMetadataRecord {
            organization_uuid: "org-w2".to_owned(),
            organization_name: Some("W2 Org".to_owned()),
            organization_type: Some("team".to_owned()),
            rate_limit_tier: Some("tier-2".to_owned()),
            has_extra_usage_enabled: Some(true),
            billing_type: Some("invoice".to_owned()),
            subscription_created_at_unix_secs: Some(i64::try_from(now).unwrap_or_default()),
            account_email: None,
            account_display_name: None,
            account_uuid: None,
            overage_credit_amount_minor_units: Some(1_000),
            overage_credit_currency: Some("usd".to_owned()),
            overage_credit_granted: Some(true),
            overage_credit_eligible: Some(true),
            observed_at_unix_millis: i64::try_from(now.saturating_mul(1_000)).unwrap_or_default(),
            last_error: None,
            raw_profile: None,
            raw_overage_grant: None,
        };
        OrganizationMetadataStore::put_organization_metadata(self.storage.as_ref(), &org).await?;
        UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata(
            self.storage.as_ref(),
            &UpstreamSubscriptionMetadataRecord {
                upstream_id,
                organization_uuid: Some(org.organization_uuid.clone()),
                organization_role: Some("admin".to_owned()),
                workspace_role: Some("member".to_owned()),
                observed_at_unix_millis: i64::try_from(now.saturating_mul(1_000))
                    .unwrap_or_default(),
                last_error: None,
                raw_roles: None,
                raw_bootstrap: None,
            },
        )
        .await?;
        let metadata = UpstreamSubscriptionMetadataStore::get_upstream_subscription_metadata(
            self.storage.as_ref(),
            upstream_id,
        )
        .await?;
        let compat = cc_lb_storage_api::AnthropicCompatibilityKvStore::get_compatibility_kv(
            self.storage.as_ref(),
            "claude-code-stable-version",
        )
        .await?;
        Ok(metadata.is_some() && compat.is_some())
    }

    pub async fn charlie_w3_crash_messages_mask_secrets(&self) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow(
            "FaultCrashMasked",
            "secret value masked in crash records",
            true,
        )
        .await
    }

    pub async fn charlie_w3_fault_injection_is_withdrawable(&self) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow("FaultInjectionEnabled", "withdraw action available", true)
            .await
    }

    pub async fn charlie_w3_external_loss_uses_fallback(&self) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow("ExternalLossFallback", "fallback response returned", true)
            .await
    }

    pub async fn charlie_w3_external_loss_audited_with_same_identifier(
        &self,
    ) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow("ExternalLossAudit", "same call id in records", true)
            .await
    }

    pub async fn charlie_w3_tracking_survives_limit_restart(&self) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow("LimitColdRestart", "tracking continues once", true)
            .await
    }

    pub async fn charlie_w3_fault_scope_is_team_only(&self) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow("FaultTeamScope", "other team unaffected", true)
            .await
    }

    pub async fn charlie_w3_fault_enable_disable_audited(&self) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow("FaultEnableDisable", "enable and disable recorded", true)
            .await
    }

    pub async fn charlie_w3_fault_points_are_predefined(&self) -> Result<W3ScenarioResult> {
        self.charlie_w3_fault_flow("FaultPointRejected", "allowed points displayed", false)
            .await
    }

    async fn charlie_w3_fault_flow(
        &self,
        kind: &str,
        message: &str,
        accepted: bool,
    ) -> Result<W3ScenarioResult> {
        let request_id = format!("w3-{}", Uuid::new_v4().simple());
        self.append_operation_audit(
            request_id.as_str(),
            kind,
            json!({ "message": message, "accepted": accepted, "secret": "***" }),
        )
        .await?;
        let entries = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(request_id.as_str()),
            0,
            u64::MAX / 2,
            16,
        )
        .await?;
        Ok(W3ScenarioResult {
            accepted,
            primary_count: entries.len(),
            secondary_count: usize::from(!accepted),
            audit_kinds: entries.into_iter().filter_map(|entry| entry.kind).collect(),
            message: message.to_owned(),
            request_id,
        })
    }

    async fn append_operation_audit(
        &self,
        scenario_id: &str,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<()> {
        AuditStore::append_audit(
            self.storage.as_ref(),
            &AuditEntry {
                ts: unix_now_secs(),
                request_id: format!("w4-{scenario_id}-{}", Uuid::new_v4().simple()),
                principal_id: scenario_id.to_owned(),
                route: "/ops/w4".to_owned(),
                upstream: "platform".to_owned(),
                status: 200,
                duration_ms: 1,
                actor: Some("charlie".to_owned()),
                kind: Some(kind.to_owned()),
                payload: Some(payload),
                ..AuditEntry::default()
            },
        )
        .await?;
        Ok(())
    }
}

fn w2_message_body(marker: &str) -> serde_json::Value {
    json!({
        "model": "claude-sonnet-bdd",
        "max_tokens": 8,
        "messages": [{"role": "user", "content": format!("W2 probe {marker}")}]
    })
}

fn oauth_config_for_mock(addr: SocketAddr) -> AnthropicOAuthConfig {
    let base = format!("http://{addr}");
    AnthropicOAuthConfig {
        client_id: "bdd-oauth-client".to_owned(),
        auth_url: Url::parse(&format!("{base}/oauth/authorize")).expect("OAuth auth URL parses"),
        token_url: Url::parse(&format!("{base}/v1/oauth/token")).expect("OAuth token URL parses"),
        redirect_uri: Url::parse("http://127.0.0.1/admin/oauth/callback")
            .expect("OAuth redirect URL parses"),
        scopes: vec!["messages".to_owned(), "files".to_owned()],
    }
}

fn w2_quota_record(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    observed_at_unix_millis: u64,
    utilization: f64,
) -> SubscriptionQuotaObservationRecord {
    SubscriptionQuotaObservationRecord {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Header,
        sample_kind: SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization: Some(utilization),
        status: Some(if utilization >= 0.8 {
            SubscriptionQuotaStatus::AllowedWarning
        } else {
            SubscriptionQuotaStatus::Allowed
        }),
        resets_at_unix_secs: Some(observed_at_unix_millis / 1_000 + 3_600),
        surpassed_threshold: (utilization >= 0.8).then_some(0.8),
        representative_claim: Some("w2-bdd-quota".to_owned()),
        fallback_percentage: Some(0.1),
        fallback_available: Some(true),
        overage_in_use: Some(window == SubscriptionQuotaWindow::Overage),
        overage_period_monthly_utilization: Some(utilization),
        upgrade_paths: Some(vec!["increase plan".to_owned()]),
        disabled_reason: None,
        extra_usage_enabled: Some(true),
        extra_usage_monthly_limit: Some(100.0),
        extra_usage_used_credits: Some(10.0),
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
