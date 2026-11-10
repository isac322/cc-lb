//! Storage vocabulary for API-key upstreams: the capability/access snapshot
//! (`upstream_api_key_details_v1`) and provider-reported usage buckets
//! (`upstream_api_usage_v1`). Anthropic-specific fields remain for backwards
//! compatibility; provider-neutral project/reporting fields are additive.
//!
//! These types are unrelated to the managed-key (`sk-cclb-*`) usage store in
//! [`crate::api_key_usage`]; every name here is prefixed `ApiKey*` on the
//! upstream side or `UpstreamApiUsage*`.

use async_trait::async_trait;
use cc_lb_domain::Provider;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    StorageError, StorageResult, UpstreamRecord,
    openai_reporting::{
        OpenAiOrganizationReportingCredentialRecord, OpenAiOrganizationReportingCredentialStatus,
        OpenAiOrganizationReportingCredentialWrite,
    },
};

macro_rules! snake_case_vocabulary {
    (
        $(#[$meta:meta])*
        pub enum $name:ident { $($variant:ident => $text:literal),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            /// Wire/storage spelling, identical to the serde representation.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }

            /// Inverse of [`Self::as_str`]; unknown text yields `None`.
            pub fn parse(text: &str) -> Option<Self> {
                match text {
                    $($text => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

snake_case_vocabulary! {
    /// What kind of credential an API-key upstream holds.
    pub enum ApiKeyClass {
        Admin => "admin",
        Enterprise => "enterprise",
        OauthToken => "oauth_token",
        WorkspaceBound => "workspace_bound",
        OrgScoped => "org_scoped",
        Unknown => "unknown",
    }
}

snake_case_vocabulary! {
    /// Stored inference capability. The wire adds `ok_limited`.
    pub enum ApiKeyInference {
        Ok => "ok",
        Denied => "denied",
        Unverified => "unverified",
    }
}

snake_case_vocabulary! {
    /// Result of an organization-API capability probe.
    pub enum ApiKeyOrgCapability {
        Ok => "ok",
        Denied => "denied",
        Error => "error",
        NotApplicable => "not_applicable",
    }
}

snake_case_vocabulary! {
    /// Provider-reported key type.
    pub enum ApiKeyType {
        Personal => "personal",
        ServiceAccount => "service_account",
        Workspace => "workspace",
    }
}

snake_case_vocabulary! {
    /// Whether the key's provider-side id was resolved.
    pub enum ApiKeyResolution {
        Resolved => "resolved",
        Unresolved => "unresolved",
        NotApplicable => "not_applicable",
    }
}

snake_case_vocabulary! {
    /// Provenance of a provider project id on an API-key access snapshot.
    pub enum ProviderProjectSource {
        Configured => "configured",
        AdminResolved => "admin_resolved",
        Unverified => "unverified",
    }
}

snake_case_vocabulary! {
    /// Provider-neutral state of the API-key usage/reporting surface.
    pub enum ProviderReportState {
        Reported => "reported",
        Estimated => "estimated",
        NotConfigured => "not_configured",
        Unavailable => "unavailable",
        Denied => "denied",
        NotApplicable => "not_applicable",
        Stale => "stale",
        Failed => "failed",
    }
}

snake_case_vocabulary! {
    /// How the upstream's Anthropic workspace is determined.
    pub enum ApiKeyWorkspaceMode {
        Bound => "bound",
        Required => "required",
        Selected => "selected",
    }
}

snake_case_vocabulary! {
    /// Subject that provider-reported usage buckets are attributed to.
    pub enum ApiUsageAttribution {
        ApiKey => "api_key",
        Workspace => "workspace",
    }
}

snake_case_vocabulary! {
    /// The 16-code remediation vocabulary (CTR-01). Declaration order is the
    /// [`RemediationCode::ALL`] order.
    pub enum RemediationCode {
        KeyAdmin => "key_admin",
        KeyEnterpriseScoped => "key_enterprise_scoped",
        KeyOat => "key_oat",
        KeyInvalid => "key_invalid",
        KeyForbidden => "key_forbidden",
        WorkspaceRequired => "workspace_required",
        WorkspaceNotFound => "workspace_not_found",
        WorkspaceIdMalformed => "workspace_id_malformed",
        WorkspaceListDenied => "workspace_list_denied",
        KeyWorkspaceScopedUsageUnavailable => "key_workspace_scoped_usage_unavailable",
        RoleInsufficient => "role_insufficient",
        KeyIdUnresolved => "key_id_unresolved",
        UsageNeverChecked => "usage_never_checked",
        NonAnthropicBaseUrl => "non_anthropic_base_url",
        ProbeUnreachable => "probe_unreachable",
        UsagePollFailed => "usage_poll_failed",
    }
}

impl RemediationCode {
    pub const ALL: [RemediationCode; 16] = [
        RemediationCode::KeyAdmin,
        RemediationCode::KeyEnterpriseScoped,
        RemediationCode::KeyOat,
        RemediationCode::KeyInvalid,
        RemediationCode::KeyForbidden,
        RemediationCode::WorkspaceRequired,
        RemediationCode::WorkspaceNotFound,
        RemediationCode::WorkspaceIdMalformed,
        RemediationCode::WorkspaceListDenied,
        RemediationCode::KeyWorkspaceScopedUsageUnavailable,
        RemediationCode::RoleInsufficient,
        RemediationCode::KeyIdUnresolved,
        RemediationCode::UsageNeverChecked,
        RemediationCode::NonAnthropicBaseUrl,
        RemediationCode::ProbeUnreachable,
        RemediationCode::UsagePollFailed,
    ];
}

/// Workspace part of the capability snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyWorkspace {
    pub mode: ApiKeyWorkspaceMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Capability snapshot persisted as `capabilities_json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyCapabilities {
    pub inference: ApiKeyInference,
    pub workspace: ApiKeyWorkspace,
    pub workspace_list: ApiKeyOrgCapability,
    pub api_key_resolution: ApiKeyResolution,
    pub key_type: Option<ApiKeyType>,
}

impl ApiKeyCapabilities {
    /// Snapshot for a key that was never probed: inference unverified,
    /// workspace required, org capabilities not applicable, unknown key type.
    pub fn not_checked() -> Self {
        Self {
            inference: ApiKeyInference::Unverified,
            workspace: ApiKeyWorkspace {
                mode: ApiKeyWorkspaceMode::Required,
                id: None,
                name: None,
            },
            workspace_list: ApiKeyOrgCapability::NotApplicable,
            api_key_resolution: ApiKeyResolution::NotApplicable,
            key_type: None,
        }
    }
}

/// Capability/access column set written by create, PUT, and Re-check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyAccessWrite {
    pub verified: bool,
    pub key_class: ApiKeyClass,
    pub key_hint: String,
    pub api_key_id: Option<String>,
    pub needs_workspace: bool,
    pub capabilities: ApiKeyCapabilities,
    pub remediation: Vec<RemediationCode>,
    pub usage_report_state: Option<ApiKeyOrgCapability>,
    /// Provider kind for this API-key access snapshot. `None` preserves
    /// legacy callers that only populate the Anthropic vocabulary.
    pub provider: Option<Provider>,
    pub project_id: Option<String>,
    pub project_source: Option<ProviderProjectSource>,
    pub provider_report_state: Option<ProviderReportState>,
    /// Redacted, bounded metadata encoded by the provider/reporting adapter.
    /// This field MUST never contain credentials or authorization headers.
    pub provider_report_status_metadata: Option<String>,
    /// Scrubbed of key material, at most 200 characters.
    pub last_error: Option<String>,
    /// `None` when no probe succeeded (create/PUT with skip).
    pub checked_at_unix_secs: Option<u64>,
    pub last_attempt_at_unix_secs: u64,
}

/// Column set written by one usage-poll tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyUsagePollOutcome {
    pub attempted_at_unix_secs: u64,
    /// `Some` demotes/writes `usage_report_state`. Applied only if
    /// `access_revision` still equals `access_revision_seen`; otherwise this
    /// one column is skipped and the rest are written.
    pub usage_report_state: Option<ApiKeyOrgCapability>,
    /// Provider kind associated with this poll's provider-neutral result.
    /// `Some` is applied only when `access_revision` still equals
    /// `access_revision_seen`; `None` leaves the stored value unchanged.
    pub provider: Option<Provider>,
    /// Provider-neutral result of this usage/reporting poll. `Some` is
    /// applied only when `access_revision` still equals `access_revision_seen`;
    /// `None` leaves the stored value unchanged.
    pub provider_report_state: Option<ProviderReportState>,
    /// Redacted, bounded provider status metadata. It MUST never contain
    /// credentials or authorization headers. `Some` is applied only when
    /// `access_revision` still equals `access_revision_seen`; `None` leaves
    /// the stored value unchanged.
    pub provider_report_status_metadata: Option<String>,
    /// `details.access_revision` as read with the poll candidate; `0` when
    /// the candidate had no details row (an inserted row starts at `0`).
    pub access_revision_seen: u64,
    /// Always written.
    pub consecutive_failures: u32,
    /// Always written; `None` clears the schedule. A non-null value never
    /// shortens a later deadline already stored (a concurrent 429's
    /// `retry-after` survives an overlapping success).
    pub next_poll_after_unix_secs: Option<u64>,
    /// `Some` writes; `None` leaves the stored value.
    pub last_success_at_unix_secs: Option<u64>,
    /// Key hint used only when the details row has to be inserted.
    pub legacy_key_hint: Option<String>,
    /// Token of the live poll claim this outcome completes. `Some` makes the
    /// write conditional on still owning the claim and releases it; `None`
    /// writes unconditionally (test seeds only).
    pub claim_token: Option<Uuid>,
    pub buckets: Vec<UpstreamApiUsageBucketWrite>,
}

/// One provider-reported usage bucket to upsert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamApiUsageBucketWrite {
    pub attribution: ApiUsageAttribution,
    pub subject_id: String,
    /// `3600` or `86400`.
    pub bucket_width_secs: u32,
    pub bucket_start_unix_secs: u64,
    /// Provider `model: null` is stored as `""` (rendered "Other").
    pub model: String,
    pub uncached_input_tokens: u64,
    pub cache_creation_5m_input_tokens: u64,
    pub cache_creation_1h_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub output_tokens: u64,
    pub list_cost_micros: u64,
}

/// One stored usage bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamApiUsageBucketRecord {
    pub attribution: ApiUsageAttribution,
    pub subject_id: String,
    pub bucket_width_secs: u32,
    pub bucket_start_unix_secs: u64,
    pub model: String,
    pub uncached_input_tokens: u64,
    pub cache_creation_5m_input_tokens: u64,
    pub cache_creation_1h_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub output_tokens: u64,
    pub list_cost_micros: u64,
    pub observed_at_unix_secs: u64,
}

/// One `upstream_api_key_details_v1` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamApiKeyDetailsRecord {
    pub upstream_id: Uuid,
    pub verified: bool,
    pub key_class: ApiKeyClass,
    pub key_hint: String,
    pub api_key_id: Option<String>,
    pub needs_workspace: bool,
    pub capabilities: ApiKeyCapabilities,
    pub remediation: Vec<RemediationCode>,
    pub usage_report_state: Option<ApiKeyOrgCapability>,
    pub provider: Option<Provider>,
    pub project_id: Option<String>,
    pub project_source: Option<ProviderProjectSource>,
    pub provider_report_state: Option<ProviderReportState>,
    /// Redacted, bounded metadata; never provider credentials or auth headers.
    pub provider_report_status_metadata: Option<String>,
    pub last_error: Option<String>,
    pub checked_at_unix_secs: Option<u64>,
    pub last_attempt_at_unix_secs: Option<u64>,
    /// Bumped by every capability/access write (create, PUT, Re-check);
    /// `0` for a row no access write has touched.
    pub access_revision: u64,
    pub usage_poll_failures: u32,
    pub usage_next_poll_after_unix_secs: Option<u64>,
    pub usage_last_success_at_unix_secs: Option<u64>,
    pub usage_last_attempt_at_unix_secs: Option<u64>,
    /// Owner of the in-flight usage poll, if one holds an unexpired lease.
    pub usage_poll_claim_token: Option<Uuid>,
    pub usage_poll_claim_lease_until_unix_secs: Option<u64>,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
}

/// An upstream due for a usage poll, with its details row if one exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyUsagePollCandidate {
    pub upstream: UpstreamRecord,
    pub details: Option<UpstreamApiKeyDetailsRecord>,
}

/// Range query over one subject's usage buckets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamApiUsageBucketQuery {
    pub upstream_id: Uuid,
    pub attribution: ApiUsageAttribution,
    pub subject_id: String,
    pub bucket_width_secs: u32,
    pub start_unix_secs: u64,
    pub end_unix_secs: u64,
}

/// Latest finalized proxied-request outcome for an upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamRequestOutcome {
    pub status: u16,
    pub upstream_error_type: Option<String>,
    pub upstream_error_message: Option<String>,
    pub upstream_error_code: Option<String>,
    pub observed_at_unix_ms: u64,
}

/// Internal storage seam for the deployment-scoped OpenAI organization
/// reporting credential. This type is intentionally not serializable and its
/// `Debug` implementation redacts the ciphertext.
#[derive(Clone, PartialEq, Eq)]
pub struct OpenAiOrganizationReportingCredentialCiphertextRecord {
    pub encrypted_secret_ciphertext: Vec<u8>,
    pub status: OpenAiOrganizationReportingCredentialStatus,
    pub revision: u64,
}

impl std::fmt::Debug for OpenAiOrganizationReportingCredentialCiphertextRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiOrganizationReportingCredentialCiphertextRecord")
            .field(
                "encrypted_secret_ciphertext",
                &format_args!(
                    "<{} bytes redacted>",
                    self.encrypted_secret_ciphertext.len()
                ),
            )
            .field("status", &self.status)
            .field("revision", &self.revision)
            .finish()
    }
}

#[async_trait]
pub trait UpstreamApiKeyStore: Send + Sync {
    async fn get_api_key_details(
        &self,
        upstream_id: Uuid,
    ) -> StorageResult<Option<UpstreamApiKeyDetailsRecord>>;

    async fn list_api_key_details(
        &self,
        upstream_ids: &[Uuid],
    ) -> StorageResult<Vec<UpstreamApiKeyDetailsRecord>>;

    /// Atomically leases the deployment-scoped OpenAI organization credential
    /// for one bounded poll. The lease is revision-fenced and expires at
    /// `lease_until_unix_secs`; `Ok(false)` means another poll owns it.
    async fn claim_openai_org_reporting_poll(
        &self,
        credential_revision: u64,
        now_unix_secs: u64,
        lease_until_unix_secs: u64,
    ) -> StorageResult<bool> {
        let _ = (credential_revision, now_unix_secs, lease_until_unix_secs);
        Ok(true)
    }

    /// Marks the deployment credential denied after an OpenAI 401/403. The
    /// revision fence prevents an older poll from overwriting a replacement.
    async fn mark_openai_org_reporting_credential_denied(
        &self,
        expected_revision: u64,
        updated_at_unix_secs: u64,
    ) -> StorageResult<()> {
        let _ = (expected_revision, updated_at_unix_secs);
        Err(StorageError::Fatal {
            message:
                "mark_openai_org_reporting_credential_denied is not implemented for this storage backend"
                    .to_owned(),
        })
    }

    /// Returns the encrypted deployment-scoped OpenAI organization reporting
    /// credential for internal consumers that already hold the deployment AEAD.
    /// Backends that do not yet implement the credential table return
    /// `StorageError::Fatal`; this default keeps the existing adapter contract
    /// source-compatible while the adapter slice is staged.
    async fn get_openai_org_reporting_credential_ciphertext(
        &self,
    ) -> StorageResult<Option<OpenAiOrganizationReportingCredentialCiphertextRecord>> {
        Err(StorageError::Fatal {
            message:
                "get_openai_org_reporting_credential_ciphertext is not implemented for this storage backend"
                    .to_owned(),
        })
    }

    /// Returns only the redacted deployment-scoped OpenAI organization
    /// reporting snapshot. The encrypted credential bytes are never included.
    /// Backends that do not yet implement the credential table return
    /// `StorageError::Fatal`; this default keeps the existing adapter contract
    /// source-compatible while the adapter slice is staged.
    async fn get_openai_org_reporting_credential(
        &self,
    ) -> StorageResult<Option<OpenAiOrganizationReportingCredentialRecord>> {
        Err(StorageError::Fatal {
            message:
                "get_openai_org_reporting_credential is not implemented for this storage backend"
                    .to_owned(),
        })
    }

    /// Conditionally replaces the deployment-scoped OpenAI organization
    /// reporting credential. The write carries already-encrypted ciphertext;
    /// adapters persist it without exposing it through public snapshots.
    async fn replace_openai_org_reporting_credential(
        &self,
        write: &OpenAiOrganizationReportingCredentialWrite,
    ) -> StorageResult<OpenAiOrganizationReportingCredentialRecord> {
        let _ = write;
        Err(StorageError::Fatal {
            message:
                "replace_openai_org_reporting_credential is not implemented for this storage backend"
                    .to_owned(),
        })
    }

    /// Re-check write: capability/access columns only, insert-if-missing.
    /// `StorageError::Conflict` when the spec is deleted or its
    /// `spec_revision != expected_spec_revision`.
    async fn write_api_key_access(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        access: &ApiKeyAccessWrite,
    ) -> StorageResult<()>;

    /// Unreachable re-check: writes `last_error`, `remediation_json`, and
    /// `last_attempt_at_unix_secs` only.
    async fn record_api_key_probe_failure(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        last_error: &str,
        remediation: &[RemediationCode],
        attempted_at_unix_secs: u64,
    ) -> StorageResult<()>;

    /// Live, enabled API-key specs (`anthropic_api_key`, `openai_api_key`, or
    /// `gemini_api_key`) LEFT JOIN details. Anthropic candidates use the
    /// legacy eligibility rule (no details row OR `usage_report_state IS
    /// NULL` OR (`state = 'ok'` AND `key_class = 'org_scoped'`)); OpenAI and
    /// Gemini candidates remain eligible until `provider_report_state` is
    /// `denied`, `not_configured`, or `not_applicable` (transient unavailable,
    /// failed, and stale states retry). All candidates must be due and have no
    /// live poll claim. Ordered by oldest usage-poll attempt first
    /// (never-attempted first), then spec id, so a `limit` below the eligible
    /// count still rotates through all of them.
    async fn list_api_key_usage_poll_candidates(
        &self,
        now_unix_secs: u64,
        only_upstream: Option<Uuid>,
        limit: usize,
    ) -> StorageResult<Vec<ApiKeyUsagePollCandidate>>;

    /// Atomically leases one upstream for a usage poll. One short
    /// transaction: validates the live spec revision, inserts the details row
    /// when missing, then claims it iff the row is still poll-eligible
    /// (unclaimed or lease expired) and still due at `now_unix_secs`; no
    /// transaction is held across the poll's HTTP calls. `Ok(true)` means
    /// `claim_token` now owns the row until
    /// `lease_until_unix_secs`; `Ok(false)` means another poller holds it or
    /// the row left the eligible set. `StorageError::Conflict` when the spec
    /// is deleted or disabled or its `spec_revision != expected_spec_revision`.
    async fn claim_api_key_usage_poll(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        claim_token: Uuid,
        lease_until_unix_secs: u64,
        key_hint: &str,
        now_unix_secs: u64,
    ) -> StorageResult<bool>;

    /// Completes a poll: when `outcome.claim_token` is `Some` the write is
    /// conditional on still owning the claim (a reclaimed or access-rewritten
    /// row rejects it with `StorageError::Conflict`) and on the spec still
    /// being enabled, and releases it. A
    /// non-null `next_poll_after_unix_secs` never shortens a later stored
    /// deadline, so a 429 `retry-after` survives an overlapping completion.
    /// `None` claim writes unconditionally and is for test seeds only.
    async fn write_api_key_usage_poll_outcome(
        &self,
        upstream_id: Uuid,
        expected_spec_revision: u64,
        outcome: &ApiKeyUsagePollOutcome,
    ) -> StorageResult<()>;

    async fn query_upstream_api_usage_buckets(
        &self,
        query: &UpstreamApiUsageBucketQuery,
    ) -> StorageResult<Vec<UpstreamApiUsageBucketRecord>>;

    /// Bounded batch: deletes at most `batch_size` rows with
    /// `bucket_start_unix_secs < before_unix_secs`; returns rows removed.
    async fn prune_upstream_api_usage_buckets(
        &self,
        before_unix_secs: u64,
        batch_size: usize,
    ) -> StorageResult<u64>;

    async fn latest_upstream_request_outcome(
        &self,
        upstream_id: Uuid,
        since_unix_ms: u64,
    ) -> StorageResult<Option<UpstreamRequestOutcome>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remediation_codes_round_trip_through_serde_and_parse() {
        for code in RemediationCode::ALL {
            let json = serde_json::to_string(&code).ok();
            assert_eq!(json, Some(format!("\"{}\"", code.as_str())));
            assert_eq!(RemediationCode::parse(code.as_str()), Some(code));
        }
        assert_eq!(RemediationCode::parse("KEY_ADMIN"), None);
    }

    #[test]
    fn not_checked_capabilities_serialize_without_workspace_id() {
        let json = serde_json::to_value(ApiKeyCapabilities::not_checked()).ok();
        assert_eq!(
            json,
            Some(serde_json::json!({
                "inference": "unverified",
                "workspace": { "mode": "required" },
                "workspace_list": "not_applicable",
                "api_key_resolution": "not_applicable",
                "key_type": null,
            }))
        );
    }
}
