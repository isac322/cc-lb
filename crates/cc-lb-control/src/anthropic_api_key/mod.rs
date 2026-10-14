//! Shared logic for Anthropic API-key upstreams: key input rules, the local
//! prefix gate, host rules, wire contract, and the org Admin API client.

pub mod api_key_id;
pub mod billing;
pub mod client;
pub mod credential;
pub mod gate;
pub mod hint;
pub mod host;
pub mod normalize;
pub mod org_probe;
pub mod preflight;
mod probe;
pub mod remediation;
pub mod usage_report;
pub mod wire;
pub mod workspace_id;

pub use api_key_id::validate_api_key_id;
pub use billing::{BillingStopKind, classify_billing_stop};
pub use client::{AnthropicOrgApi, ProbeHttpClient, ProbeLimits};
pub use credential::{ApiKeyResolutionError, resolve_api_key, resolve_stored_api_key};
pub use gate::prefix_gate;
pub use hint::key_hint;
pub use host::{org_api_applies, validate_base_url};
pub use normalize::{KeyInputError, MAX_API_KEY_BYTES, NormalizedApiKey, normalize_api_key};
pub use org_probe::OrgApiError;
pub use preflight::{
    PreflightInput, PreflightMode, PreflightOutcome, ProbeTransport, run_preflight,
};
pub use remediation::{endpoint_operator_message, http_status, operator_message};
pub use usage_report::{
    UsageBucketWidth, UsageFilter, UsageReportBucket, UsageReportPage, UsageReportQuery,
    UsageReportResult,
};
pub use wire::{
    AnthropicWorkspace, KeyBillingStop, KeyUsageEnvelope, KeyUsageMonth, KeyUsageSource,
    KeyUsageTotals, PreflightCapabilities, PreflightClassification, PreflightRequest,
    PreflightVerdict, PreflightWireResponse, PreflightWorkspaces, Remediation,
    RemediationErrorBody, RemediationErrorInner, WireInference,
};
pub use workspace_id::validate_workspace_id;

/// Model used by warmup and by the inference probe when no `claude-*` model
/// is listed.
pub const WARMUP_MODEL: &str = "claude-haiku-4-5-20251001";
