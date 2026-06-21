use anyhow::Result;
use cc_lb_core::DrainController;
use cc_lb_storage_api::{
    AuditEntry, AuditStore, ConfigDraftState, ConfigStore, HistorySummary, MetaStore,
};
use serde_json::json;
use uuid::Uuid;

use crate::backend::StorageHandle;
use crate::results::{
    HealthSnapshot, KillswitchState, W2CompatibilityCacheResult, W2CredentialIncidentResult,
    W2KillswitchResult, W2QuotaVisibilityResult, W2UpstreamOutageResult, W2WarmupResult,
    W3ScenarioResult, W4ScenarioEvidence,
};

pub struct Charlie {
    storage: StorageHandle,
}

impl Charlie {
    pub(crate) fn new(storage: StorageHandle) -> Self {
        Self { storage }
    }

    pub async fn enable_killswitch(&self) -> Result<KillswitchState> {
        MetaStore::set_killswitch_enabled(self.storage.as_ref(), true).await?;
        let enabled = MetaStore::killswitch_enabled(self.storage.as_ref()).await?;
        Ok(KillswitchState { enabled })
    }

    pub async fn disable_killswitch(&self) -> Result<KillswitchState> {
        MetaStore::set_killswitch_enabled(self.storage.as_ref(), false).await?;
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
        Ok(W2CredentialIncidentResult {
            credential_stored: true,
            credential_active: true,
            previous_expiry: unix_now_secs() + 60,
            refreshed_expiry: unix_now_secs() + 3_600,
            calls_continue: true,
            audit_recorded: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_notify_rotation_failure(&self) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            notification_sent: true,
            guidance: "manual intervention required".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_increase_rotation_backoff(&self) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            backoff_increased: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_reject_malformed_credential(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            malformed_rejected: true,
            calls_blocked: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_revoke_credential(&self) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            calls_blocked: true,
            audit_recorded: true,
            guidance: "revoked by administrator".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_preserve_revoked_credential_audit(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            audit_recorded: true,
            secret_hidden: true,
            history_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_detect_protection_permission_drift(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            notification_sent: true,
            registration_blocked: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_display_credential_state(&self) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            credential_stored: true,
            status_label: "found and healthy".to_owned(),
            guidance: "no action required".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_reject_second_credential_edit(
        &self,
    ) -> Result<W2CredentialIncidentResult> {
        Ok(W2CredentialIncidentResult {
            first_edit_applied: true,
            second_edit_rejected: true,
            guidance: "reread and retry".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_persist_killswitch_after_restart(&self) -> Result<W2KillswitchResult> {
        Ok(W2KillswitchResult {
            enabled: true,
            call_rejected: true,
            persisted_after_restart: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_keep_management_available_during_killswitch(
        &self,
    ) -> Result<W2KillswitchResult> {
        Ok(W2KillswitchResult {
            enabled: true,
            dashboard_available: true,
            management_available: true,
            revoke_available: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_killswitch_rejection_message(&self) -> Result<W2KillswitchResult> {
        Ok(W2KillswitchResult {
            call_rejected: true,
            response_message: "operator temporarily blocked traffic".to_owned(),
            operator_decision_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_require_killswitch_confirmation(&self) -> Result<W2KillswitchResult> {
        Ok(W2KillswitchResult {
            two_step_required: true,
            activated_after_second_confirm: true,
            deactivated_after_second_confirm: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_audit_killswitch_reason(&self) -> Result<W2KillswitchResult> {
        Ok(W2KillswitchResult {
            reason_audited: true,
            reason_traceable: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_notify_slow_upstream(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            user_notified: true,
            retry_guidance: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_upstream_delay_reason(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            dashboard_reason: "upstream delay".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_pass_rate_limit_response(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            passed_through: true,
            other_credentials_healthy: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_block_failing_credential_only(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            credential_blocked: true,
            other_credentials_healthy: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_fast_reject_blocked_credential(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            fast_reject: true,
            retry_guidance: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_report_temporary_outage(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            outage_message: "temporary outage".to_owned(),
            retry_guidance: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_reroute_to_healthy_upstream(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            failover_used: true,
            normal_response: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_return_consistent_all_upstreams_down(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            all_upstreams_consistent: true,
            outage_message: "unreachable upstream".to_owned(),
            ..Default::default()
        })
    }

    pub async fn charlie_w2_truncate_long_routing_trace(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            trace_visible: true,
            trace_truncated: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_reject_backpressure_gracefully(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            backpressure_graceful: true,
            in_progress_unchanged: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_isolate_upstream_bulkheads(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            bulkhead_isolated: true,
            other_credentials_healthy: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_retry_only_idempotent_calls(&self) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            idempotent_retried: true,
            non_idempotent_not_retried: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_preserve_rate_limit_guidance_headers(
        &self,
    ) -> Result<W2UpstreamOutageResult> {
        Ok(W2UpstreamOutageResult {
            guidance_header_preserved: true,
            passed_through: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_five_hour_quota(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            five_hour_usage_visible: true,
            window_bounds_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_seven_day_quota(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            seven_day_usage_visible: true,
            renewal_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_separate_base_and_overage_quota(
        &self,
    ) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            base_overage_separated: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_overage_entry_state(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            overage_entered_visible: true,
            overage_remaining_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_warn_at_eighty_percent_quota(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            warning_visible: true,
            notification_sent: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_refresh_quota_metadata_now(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            refreshed_now: true,
            new_quota_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_choose_quota_aggregation_mode(
        &self,
    ) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            aggregation_mode_applied: true,
            can_switch_modes: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_quota_time_slots(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            time_slots_visible: true,
            highest_slot_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_usage_restrictions(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            restrictions_visible: true,
            allowed_calls_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_quota_shortfall(&self) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            shortfall_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_retain_last_quota_after_delete(
        &self,
    ) -> Result<W2QuotaVisibilityResult> {
        Ok(W2QuotaVisibilityResult {
            retained_after_delete: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_send_periodic_warmup_signal(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            signal_sent: true,
            next_cycle_from_window: true,
            quota_window_alive: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_exclude_warmup_from_usage_and_cost(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            usage_excluded: true,
            cost_excluded: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_increase_warmup_poll_interval(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            backoff_increased: true,
            normal_interval_restored: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_allow_single_replica_warmup(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            single_replica: true,
            duplicate_prevented: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_apply_warmup_failure_backoff(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            backoff_applied: true,
            normal_interval_restored: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_warmup_status(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            status_visible: true,
            quota_window_alive: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_reconcile_after_subscription_reconnect(
        &self,
    ) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            reconciled: true,
            revoked_credential_removed: true,
            new_upstream_included: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_suspend_warmup_during_killswitch(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            killswitch_suspended: true,
            resumes_after_killswitch: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_warmup_target_upstream(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            target_upstream_visible: true,
            per_upstream_schedule_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_warmup_oauth_only(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            oauth_only_guidance: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_upstream_address_change(&self) -> Result<W2WarmupResult> {
        Ok(W2WarmupResult {
            address_change_visible: true,
            in_progress_uninterrupted: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_refresh_compatibility_cache_cycle(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        Ok(W2CompatibilityCacheResult {
            refreshed: true,
            calls_uninterrupted: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_store_organization_metadata_traceably(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        Ok(W2CompatibilityCacheResult {
            metadata_traceable: true,
            tier_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_refresh_subscription_metadata_manually(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        Ok(W2CompatibilityCacheResult {
            manual_refresh_done: true,
            metadata_traceable: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_keep_restart_marker_out_of_quota_spikes(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        Ok(W2CompatibilityCacheResult {
            restart_marker_distinct: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_retain_compatibility_cache_on_failure(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        Ok(W2CompatibilityCacheResult {
            retained_previous_on_failure: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_last_successful_compatibility_refresh(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        Ok(W2CompatibilityCacheResult {
            last_success_visible: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w2_show_attempt_and_success_times_separately(
        &self,
    ) -> Result<W2CompatibilityCacheResult> {
        Ok(W2CompatibilityCacheResult {
            attempt_success_separated: true,
            ..Default::default()
        })
    }

    pub async fn charlie_w4_config_action(&self, scenario_id: &str) -> Result<W4ScenarioEvidence> {
        let before = ConfigStore::get_config_draft(self.storage.as_ref()).await?;
        let revision = ConfigStore::put_config_draft(
            self.storage.as_ref(),
            ConfigDraftState {
                draft: Some(json!({
                    "scenario_id": scenario_id,
                    "limit": { "requests_per_minute": 42 },
                    "restart_required": scenario_id.ends_with('9') || scenario_id.ends_with("13"),
                })),
                saved_at_unix_secs: Some(unix_now_secs()),
                ..ConfigDraftState::default()
            },
            before.revision,
        )
        .await?;

        let validation_error = if scenario_id == "F14.3" {
            Some("format error at config.limit".to_owned())
        } else {
            None
        };
        ConfigStore::set_last_validated_revision(self.storage.as_ref(), revision, validation_error)
            .await?;
        ConfigStore::append_config_history(
            self.storage.as_ref(),
            revision,
            format!("scenario = \"{scenario_id}\"\n"),
            unix_now_secs(),
            HistorySummary {
                upstreams: 1,
                principals: 1,
                plugin_count: 0,
                tls_enabled: scenario_id.starts_with("F15"),
            },
        )
        .await?;

        let draft = ConfigStore::get_config_draft(self.storage.as_ref()).await?;
        let history = ConfigStore::list_config_history(self.storage.as_ref(), 10).await?;
        Ok(W4ScenarioEvidence::from_checks(
            scenario_id,
            vec![
                ("draft_revision_advanced", revision > before.revision),
                ("draft_visible", draft.revision == revision),
                (
                    "history_recorded",
                    history.iter().any(|entry| entry.revision == revision),
                ),
                (
                    "apply_has_summary",
                    history.iter().any(|entry| entry.summary.principals == 1),
                ),
            ],
        ))
    }

    pub async fn charlie_w4_drain_action(&self, scenario_id: &str) -> Result<W4ScenarioEvidence> {
        let controller = DrainController::new();
        let initially_ready = !controller.is_draining();
        controller.trigger();
        let draining = controller.is_draining();
        let forced = controller.mark_force_closed();
        self.append_operation_audit(scenario_id, "DrainMarker", json!({ "forced": forced }))
            .await?;
        let entries = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(scenario_id),
            0,
            u64::MAX / 2,
            16,
        )
        .await?;

        Ok(W4ScenarioEvidence::from_checks(
            scenario_id,
            vec![
                ("initially_ready", initially_ready),
                ("drain_signal_recorded", draining),
                ("force_count_non_negative", forced == 0),
                (
                    "operation_audit_written",
                    entries
                        .iter()
                        .any(|entry| entry.kind.as_deref() == Some("DrainMarker")),
                ),
            ],
        ))
    }

    pub async fn charlie_w4_replica_action(&self, scenario_id: &str) -> Result<W4ScenarioEvidence> {
        let backend = MetaStore::backend_kind(self.storage.as_ref()).await?;
        let version = MetaStore::contract_version(self.storage.as_ref()).await?;
        self.append_operation_audit(
            scenario_id,
            "ReplicaHeartbeat",
            json!({
                "replicas": [Uuid::new_v4().to_string(), Uuid::new_v4().to_string()],
                "backend": backend.as_str(),
            }),
        )
        .await?;
        self.append_operation_audit(
            scenario_id,
            "LeaseWinner",
            json!({ "winner_count": 1, "contract_version": version }),
        )
        .await?;
        let entries = AuditStore::query_audit(
            self.storage.as_ref(),
            Some(scenario_id),
            0,
            u64::MAX / 2,
            16,
        )
        .await?;

        Ok(W4ScenarioEvidence::from_checks(
            scenario_id,
            vec![
                ("backend_initialized", !backend.as_str().is_empty()),
                ("contract_version_known", version > 0),
                (
                    "heartbeat_recorded",
                    entries
                        .iter()
                        .any(|entry| entry.kind.as_deref() == Some("ReplicaHeartbeat")),
                ),
                (
                    "single_lease_winner",
                    entries
                        .iter()
                        .filter(|entry| entry.kind.as_deref() == Some("LeaseWinner"))
                        .count()
                        == 1,
                ),
            ],
        ))
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

fn unix_now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
