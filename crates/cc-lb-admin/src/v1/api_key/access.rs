//! `api_key_access` projection of an API-key upstream's stored details
//! (the web `ApiKeyAccessSchema`).

use cc_lb_storage_api::{
    ApiKeyClass, ApiKeyInference, ApiKeyOrgCapability, ApiKeyType, ApiKeyWorkspace,
    UpstreamApiKeyDetailsRecord, UpstreamKind, UpstreamRecord,
};
use serde::Serialize;

/// Capability snapshot rendered on upstream responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ApiKeyAccessResponse {
    pub(crate) verified: bool,
    /// When the snapshot was checked. For an unverified key it is the last
    /// attempt: the web reads it as "Last attempt" and, when set, shows K3
    /// ("Last check failed") instead of K2 ("Key not checked yet").
    pub(crate) checked_at_unix_secs: Option<u64>,
    pub(crate) key_class: ApiKeyClass,
    pub(crate) inference: ApiKeyInference,
    pub(crate) workspace: ApiKeyWorkspace,
    pub(crate) workspace_list: ApiKeyOrgCapability,
    /// The poller-maintained state, or `not_applicable` before the first poll.
    pub(crate) usage_report: ApiKeyOrgCapability,
    pub(crate) key_type: Option<ApiKeyType>,
    pub(crate) last_error: Option<String>,
}

/// OAuth upstreams and API-key upstreams without a details row (never
/// checked) yield `None`.
pub(crate) fn api_key_access_response(
    record: &UpstreamRecord,
    details: Option<&UpstreamApiKeyDetailsRecord>,
) -> Option<ApiKeyAccessResponse> {
    if !matches!(
        record.kind,
        UpstreamKind::AnthropicApiKey | UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        return None;
    }
    let details = details?;
    // An unreachable probe never sets `checked_at` (the key wasn't checked)
    // but records the attempt and its operator-phrased error (I06). Such a
    // key has been tried, so it reads as K3, not as never checked.
    let attempted = (!details.verified && details.last_error.is_some())
        .then_some(details.last_attempt_at_unix_secs)
        .flatten();
    Some(ApiKeyAccessResponse {
        verified: details.verified,
        checked_at_unix_secs: attempted.or(details.checked_at_unix_secs),
        key_class: details.key_class,
        inference: details.capabilities.inference,
        workspace: details.capabilities.workspace.clone(),
        workspace_list: details.capabilities.workspace_list,
        usage_report: details
            .usage_report_state
            .unwrap_or(ApiKeyOrgCapability::NotApplicable),
        key_type: details.capabilities.key_type,
        last_error: details.last_error.clone(),
    })
}

#[cfg(test)]
mod tests {
    use cc_lb_storage_api::{
        ApiKeyCapabilities, ApiKeyResolution, ApiKeyWorkspaceMode, RemediationCode,
    };
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    fn record(kind: UpstreamKind) -> UpstreamRecord {
        UpstreamRecord {
            id: Uuid::from_u128(7),
            name: "primary".to_owned(),
            kind,
            ..UpstreamRecord::default()
        }
    }

    fn details() -> UpstreamApiKeyDetailsRecord {
        UpstreamApiKeyDetailsRecord {
            upstream_id: Uuid::from_u128(7),
            verified: true,
            key_class: ApiKeyClass::OrgScoped,
            key_hint: "sk-ant-api03-...AbCd".to_owned(),
            api_key_id: Some("apikey_01".to_owned()),
            needs_workspace: true,
            capabilities: ApiKeyCapabilities {
                inference: ApiKeyInference::Ok,
                workspace: ApiKeyWorkspace {
                    mode: ApiKeyWorkspaceMode::Selected,
                    id: Some("wrkspc_01".to_owned()),
                    name: Some("Production".to_owned()),
                },
                workspace_list: ApiKeyOrgCapability::Ok,
                api_key_resolution: ApiKeyResolution::Resolved,
                key_type: Some(ApiKeyType::ServiceAccount),
            },
            remediation: vec![RemediationCode::KeyIdUnresolved],
            usage_report_state: Some(ApiKeyOrgCapability::Denied),
            last_error: Some("usage report denied".to_owned()),
            checked_at_unix_secs: Some(1_700),
            last_attempt_at_unix_secs: Some(1_700),
            access_revision: 1,
            usage_poll_failures: 2,
            usage_next_poll_after_unix_secs: Some(2_000),
            usage_last_success_at_unix_secs: Some(1_500),
            usage_last_attempt_at_unix_secs: Some(1_900),
            usage_poll_claim_token: None,
            usage_poll_claim_lease_until_unix_secs: None,
            created_at_unix_secs: 1_000,
            updated_at_unix_secs: 1_900,
        }
    }

    #[test]
    fn maps_details_to_the_access_schema() {
        let access =
            api_key_access_response(&record(UpstreamKind::AnthropicApiKey), Some(&details()));
        assert_eq!(
            access.map(|access| serde_json::to_value(access).ok()),
            Some(Some(json!({
                "verified": true,
                "checked_at_unix_secs": 1_700,
                "key_class": "org_scoped",
                "inference": "ok",
                "workspace": { "mode": "selected", "id": "wrkspc_01", "name": "Production" },
                "workspace_list": "ok",
                "usage_report": "denied",
                "key_type": "service_account",
                "last_error": "usage report denied",
            })))
        );
    }

    #[test]
    fn unpolled_usage_report_reads_not_applicable() {
        let mut details = details();
        details.usage_report_state = None;
        let access =
            api_key_access_response(&record(UpstreamKind::AnthropicApiKey), Some(&details));
        assert_eq!(
            access.map(|access| access.usage_report),
            Some(ApiKeyOrgCapability::NotApplicable)
        );
    }

    #[test]
    fn unreachable_unverified_key_reads_its_last_attempt() {
        let mut details = details();
        details.verified = false;
        details.checked_at_unix_secs = None;
        details.last_attempt_at_unix_secs = Some(1_900);
        details.last_error =
            Some("Couldn't reach the endpoint — the key wasn't checked.".to_owned());
        let access =
            api_key_access_response(&record(UpstreamKind::AnthropicApiKey), Some(&details));
        assert_eq!(
            access.and_then(|access| access.checked_at_unix_secs),
            Some(1_900)
        );
    }

    #[test]
    fn skipped_check_stays_never_checked() {
        let mut details = details();
        details.verified = false;
        details.checked_at_unix_secs = None;
        details.last_attempt_at_unix_secs = Some(1_900);
        details.last_error = None;
        let access =
            api_key_access_response(&record(UpstreamKind::AnthropicApiKey), Some(&details));
        assert_eq!(access.and_then(|access| access.checked_at_unix_secs), None);
    }

    #[test]
    fn oauth_and_unchecked_upstreams_have_no_access() {
        assert_eq!(
            api_key_access_response(&record(UpstreamKind::AnthropicOauth), Some(&details())),
            None
        );
        assert_eq!(
            api_key_access_response(&record(UpstreamKind::AnthropicApiKey), None),
            None
        );
    }
}
