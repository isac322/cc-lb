//! Result types returned by persona client methods. These are the
//! types referenced verbatim in the `then` closures of `bdd_scenario!`
//! invocations and therefore form part of the test surface.

use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct W1ObservableSpec {
    pub scenario_id: &'static str,
    pub feature: &'static str,
    pub observable: &'static str,
    pub marker: &'static str,
    pub status: u16,
}

impl W1ObservableSpec {
    pub const fn new(
        scenario_id: &'static str,
        feature: &'static str,
        observable: &'static str,
        marker: &'static str,
        status: u16,
    ) -> Self {
        Self {
            scenario_id,
            feature,
            observable,
            marker,
            status,
        }
    }
}

#[derive(Debug, Clone)]
pub struct W1ScenarioEvidence {
    pub scenario_id: String,
    pub feature: String,
    pub observable: String,
    pub marker: String,
    pub status: u16,
    pub audit_count: usize,
    pub request_count: usize,
}

impl W1ScenarioEvidence {
    pub fn is_satisfied(&self) -> bool {
        self.audit_count > 0 && self.request_count > 0
    }

    pub fn expected_summary(&self) -> String {
        format!(
            "scenario={} feature={} observable={} marker={} status={} audit_count>0 request_count>0",
            self.scenario_id, self.feature, self.observable, self.marker, self.status
        )
    }

    pub fn actual_summary(&self) -> String {
        format!(
            "audit_count={} request_count={}",
            self.audit_count, self.request_count
        )
    }
}

#[derive(Debug, Clone)]
pub struct PrincipalCreateResult {
    pub id: Uuid,
    pub name: String,
    pub is_active: bool,
    pub revision: u64,
    pub first_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PrincipalSoftDeleteResult {
    pub id: Uuid,
    pub deleted_at_unix_secs: Option<u64>,
    pub revision: u64,
}

#[derive(Debug, Clone)]
pub struct AuditEntrySummary {
    pub kind: String,
    pub actor: String,
    pub principal_id: String,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub struct PrincipalDisableResult {
    pub id: Uuid,
    pub is_active: bool,
    pub revision: u64,
}

#[derive(Debug, Clone)]
pub struct PrincipalModelAclResult {
    pub id: Uuid,
    pub allowed_models: Vec<String>,
    pub revision: u64,
}

#[derive(Debug, Clone)]
pub struct KillswitchState {
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub liveness_ok: bool,
    pub readiness_ok: bool,
    pub killswitch_enabled: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W2CredentialIncidentResult {
    pub credential_stored: bool,
    pub credential_active: bool,
    pub previous_expiry: u64,
    pub refreshed_expiry: u64,
    pub calls_continue: bool,
    pub calls_blocked: bool,
    pub malformed_rejected: bool,
    pub audit_recorded: bool,
    pub notification_sent: bool,
    pub backoff_increased: bool,
    pub protection_permission_ok: bool,
    pub registration_blocked: bool,
    pub status_label: String,
    pub guidance: String,
    pub first_edit_applied: bool,
    pub second_edit_rejected: bool,
    pub secret_hidden: bool,
    pub history_visible: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W2KillswitchResult {
    pub enabled: bool,
    pub call_rejected: bool,
    pub call_reaches_upstream: bool,
    pub persisted_after_restart: bool,
    pub dashboard_available: bool,
    pub management_available: bool,
    pub revoke_available: bool,
    pub response_message: String,
    pub operator_decision_visible: bool,
    pub two_step_required: bool,
    pub activated_after_second_confirm: bool,
    pub deactivated_after_second_confirm: bool,
    pub reason_audited: bool,
    pub reason_traceable: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W2UpstreamOutageResult {
    pub user_notified: bool,
    pub dashboard_reason: String,
    pub passed_through: bool,
    pub other_credentials_healthy: bool,
    pub credential_blocked: bool,
    pub fast_reject: bool,
    pub retry_guidance: bool,
    pub outage_message: String,
    pub failover_used: bool,
    pub normal_response: bool,
    pub all_upstreams_consistent: bool,
    pub trace_visible: bool,
    pub trace_truncated: bool,
    pub backpressure_graceful: bool,
    pub in_progress_unchanged: bool,
    pub bulkhead_isolated: bool,
    pub idempotent_retried: bool,
    pub non_idempotent_not_retried: bool,
    pub guidance_header_preserved: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W2OAuthConsentResult {
    pub auth_url_created: bool,
    pub returned_to_callback: bool,
    pub takeover_blocked: bool,
    pub callback_validated: bool,
    pub invalid_callback_rejected: bool,
    pub credential_created: bool,
    pub credential_active: bool,
    pub immediately_usable: bool,
    pub audit_recorded: bool,
    pub tampered_marker_rejected: bool,
    pub restart_guidance: bool,
    pub cancellation_prevents_credential: bool,
    pub cancellation_message: bool,
    pub sessions_isolated: bool,
    pub expired_marker_rejected: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W2QuotaVisibilityResult {
    pub five_hour_usage_visible: bool,
    pub seven_day_usage_visible: bool,
    pub window_bounds_visible: bool,
    pub renewal_visible: bool,
    pub base_overage_separated: bool,
    pub overage_entered_visible: bool,
    pub overage_remaining_visible: bool,
    pub warning_visible: bool,
    pub notification_sent: bool,
    pub refreshed_now: bool,
    pub new_quota_visible: bool,
    pub aggregation_mode_applied: bool,
    pub can_switch_modes: bool,
    pub time_slots_visible: bool,
    pub highest_slot_visible: bool,
    pub restrictions_visible: bool,
    pub allowed_calls_visible: bool,
    pub shortfall_visible: bool,
    pub retained_after_delete: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W2WarmupResult {
    pub signal_sent: bool,
    pub next_cycle_from_window: bool,
    pub quota_window_alive: bool,
    pub usage_excluded: bool,
    pub cost_excluded: bool,
    pub backoff_increased: bool,
    pub normal_interval_restored: bool,
    pub single_replica: bool,
    pub duplicate_prevented: bool,
    pub backoff_applied: bool,
    pub status_visible: bool,
    pub reconciled: bool,
    pub revoked_credential_removed: bool,
    pub new_upstream_included: bool,
    pub killswitch_suspended: bool,
    pub resumes_after_killswitch: bool,
    pub target_upstream_visible: bool,
    pub per_upstream_schedule_visible: bool,
    pub oauth_only_guidance: bool,
    pub address_change_visible: bool,
    pub in_progress_uninterrupted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W2CompatibilityCacheResult {
    pub refreshed: bool,
    pub calls_uninterrupted: bool,
    pub metadata_traceable: bool,
    pub tier_visible: bool,
    pub manual_refresh_done: bool,
    pub restart_marker_distinct: bool,
    pub retained_previous_on_failure: bool,
    pub last_success_visible: bool,
    pub attempt_success_separated: bool,
}

#[derive(Debug, Clone, Default)]
pub struct W3ScenarioResult {
    pub accepted: bool,
    pub primary_count: usize,
    pub secondary_count: usize,
    pub audit_kinds: Vec<String>,
    pub message: String,
    pub request_id: String,
}

#[derive(Debug, Clone)]
pub struct W4ScenarioEvidence {
    pub scenario_id: String,
    pub passed: bool,
    pub observations: Vec<String>,
}

impl W4ScenarioEvidence {
    pub fn from_checks(scenario_id: &str, checks: Vec<(&str, bool)>) -> Self {
        let passed = checks.iter().all(|(_, passed)| *passed);
        let observations = checks
            .into_iter()
            .map(|(name, passed)| format!("{name}={passed}"))
            .collect();

        Self {
            scenario_id: scenario_id.to_owned(),
            passed,
            observations,
        }
    }
}
