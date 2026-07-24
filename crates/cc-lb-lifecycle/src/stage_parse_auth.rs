use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Stage payloads: Parse
// ---------------------------------------------------------------------------

/// Information extracted by the body parser.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ParseInfo {
    pub path: String,
    /// `messages`, `models`, etc.
    pub method: String,
    /// Requested model, if the body specified one.
    pub model: Option<String>,
    /// Whether the client asked for a streaming response (`stream: true`).
    pub stream: bool,
    /// Byte length of the raw request body.
    pub body_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control_block_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_budget_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_breakpoints: Vec<cc_lb_request_log::RequestCacheBreakpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_v3_cache_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_parent_agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_auxiliary_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache_control_message_indices: Vec<u64>,
}

/// Reason the body parser rejected the request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ParseFailure {
    /// Request body exceeded the configured cap.
    BodyTooLarge {
        limit_bytes: u64,
    },
    InvalidJson,
}

// ---------------------------------------------------------------------------
// Stage payloads: Auth
// ---------------------------------------------------------------------------

/// Result of successful authentication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthInfo {
    pub principal_id: String,
    pub key_id: Option<String>,
    pub principal_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ms: Option<u64>,
}

/// Reason authentication failed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AuthFailure {
    /// Token was missing, malformed, or did not match any known key.
    AuthenticationFailed {
        http_status: u16,
        /// Coarse reason label used by the api-key metrics subscriber to
        /// tag `cclb_key_auth_failures_total`. `None` when the caller
        /// cannot classify the failure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Token matched a key but the principal record was not found.
    PrincipalMissing { principal_id: String },
}
