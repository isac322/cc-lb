//! Storage vocabulary for Anthropic API-key upstreams: the capability/access
//! snapshot (`upstream_api_key_details_v1`) and provider-reported usage
//! buckets (`upstream_api_usage_v1`).
//!
//! These types are unrelated to the managed-key (`sk-cclb-*`) usage store in
//! [`crate::api_key_usage`]; every name here is prefixed `ApiKey*` on the
//! upstream side or `UpstreamApiUsage*`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{StorageResult, UpstreamRecord};

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
    pub observed_at_unix_ms: u64,
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

    /// Live, enabled `anthropic_api_key` specs LEFT JOIN details where
    /// (no details row OR `usage_report_state IS NULL` OR
    /// (`state = 'ok'` AND `key_class = 'org_scoped'`)) AND
    /// (`usage_next_poll_after_unix_secs IS NULL OR <= now`) AND no live poll
    /// claim (`usage_poll_claim_token IS NULL OR lease <= now`). Ordered by
    /// oldest usage-poll attempt first (never-attempted first), then spec id,
    /// so a `limit` below the eligible count still rotates through all of them.
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
