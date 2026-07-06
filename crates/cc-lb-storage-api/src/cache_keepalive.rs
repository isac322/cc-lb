//! Per-principal configuration for the prompt-cache keep-alive feature.
//!
//! When enabled on a principal, the engine tracks per-session state after
//! each proxied `/v1/messages` response that carries a `cache_control`
//! breakpoint, classifies whether the agent's turn is likely still in
//! progress, and periodically fires a synthetic `max_tokens: 0` request to
//! refresh the prompt-cache TTL before it expires.
//!
//! Storage-layer types only. The engine consumes the compiled form via
//! `PrincipalSpecCached::cache_keepalive`.
//!
//! Design summary is captured in
//! `crates/cc-lb-engine/src/cache_keepalive/mod.rs`.

use serde::{Deserialize, Serialize};

/// Root configuration attached to a `PrincipalRecord`.
///
/// `enabled = false` means the feature is off for this principal even if
/// other fields are set. Absence of the field entirely on a stored
/// principal (legacy rows) is equivalent to `None` / disabled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheKeepaliveConfig {
    /// Master switch for the feature on this principal.
    pub enabled: bool,

    /// Fire the keep-alive this many seconds before the 5-minute cache TTL
    /// would expire. Default 30 → schedule at (300 - 30) = 270 s = 4m30s.
    #[serde(default = "default_lead_5m")]
    pub refresh_lead_time_5m_secs: u32,

    /// Fire the keep-alive this many seconds before the 1-hour cache TTL
    /// would expire. Default 300 → schedule at (3600 - 300) = 3300 s = 55m.
    #[serde(default = "default_lead_1h")]
    pub refresh_lead_time_1h_secs: u32,

    /// Hard cap on how many keep-alive fires may happen for a single
    /// session before the tracker gives up. Protects against runaway loops
    /// when a client silently hangs.
    #[serde(default = "default_max_refreshes")]
    pub max_refreshes_per_session: u32,

    /// Wall-clock upper bound (seconds) on how long a single session may
    /// be kept alive from its first schedule. Default 14400 (4 h).
    #[serde(default = "default_max_duration")]
    pub max_total_duration_secs: u64,

    /// Maximum shaped body size (bytes) captured per snapshot. Requests
    /// exceeding this bound are not tracked. Default 512 KiB.
    #[serde(default = "default_snapshot_max_bytes")]
    pub snapshot_max_bytes: u32,

    /// Classifier tuning.
    #[serde(default)]
    pub classifier: ClassifierConfig,
}

/// Tuning knobs for the per-response classifier that decides
/// AgentInTurn / UserTurn / Ambiguous.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ClassifierConfig {
    /// Extra tool names to treat as "wait-for-user" on top of the built-in
    /// list. When a `stop_reason: "tool_use"` response's only client tools
    /// are wait-for-user tools, the session is classified as UserTurn and
    /// no keep-alive is scheduled.
    #[serde(default)]
    pub extra_wait_for_user_tools: Vec<String>,

    /// If true, route `stop_reason: "end_turn"` through the LLM judge as
    /// well. Only turn on for principals running autonomous/self-loop
    /// agents that continue after `end_turn`.
    #[serde(default)]
    pub treat_end_turn_as_ambiguous: bool,

    /// Optional small-LLM judge for Ambiguous classifier decisions.
    /// If None, Ambiguous is treated as AgentInTurn (bias toward keeping
    /// the cache warm — false-positive cost is small).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_judge: Option<LlmJudgeConfig>,
}

/// Optional small-LLM judge invoked when the heuristic classifier returns
/// `Ambiguous`. Runs in the keep-alive background task, never blocks the
/// user-facing response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmJudgeConfig {
    /// Provider identifier understood by the `genai` crate.
    /// Examples: `"anthropic"`, `"openai"`, `"gemini"`, `"kimi"`,
    /// `"moonshot"`, `"zai"`, `"bigmodel"`, `"custom_openai"`.
    pub provider: String,

    /// Model identifier passed to `genai::Client::exec_chat`.
    pub model: String,

    /// Opaque reference into the cc-lb secret store. The engine resolves
    /// this to the raw API key at judge-call time; the raw key never
    /// leaves storage.
    pub api_key_secret_ref: String,

    /// Base URL override for OpenAI-compatible gateways / self-hosted
    /// endpoints. Ignored when the provider has a native adapter and no
    /// override is desired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,

    /// Structured output mode. Not all providers support strict JSON
    /// schema; fall back to `JsonObject` (JSON mode) or `Text` where
    /// needed.
    #[serde(default)]
    pub response_format: JudgeResponseFormat,

    /// Number of tail messages from the request to include in the judge
    /// prompt. Default 4.
    #[serde(default = "default_last_n_messages")]
    pub last_n_messages: usize,

    /// Maximum output tokens for the judge response. Default 128.
    #[serde(default = "default_judge_max_tokens")]
    pub max_tokens: u32,

    /// Sampling temperature. Default 0.0 for deterministic judgment.
    #[serde(default = "default_judge_temperature")]
    pub temperature: f32,

    /// Overall timeout (seconds) for the judge call including network.
    /// On timeout the session is treated as UserTurn (fail-closed).
    #[serde(default = "default_judge_timeout")]
    pub timeout_secs: u32,
}

/// Which structured-output surface to request from the judge model.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JudgeResponseFormat {
    /// Strict JSON schema. Supported by Anthropic Claude, OpenAI recent
    /// models, and newer Kimi (k2.5+). Rejected by Zhipu GLM and older
    /// Moonshot models.
    #[default]
    JsonSpec,
    /// Generic JSON mode. Widely supported. Weaker than `JsonSpec` but
    /// still parseable.
    JsonObject,
    /// Plain text. Last-resort fallback for providers without any JSON
    /// mode; classifier will best-effort parse `agent_in_turn: true/false`
    /// from the reply.
    Text,
}

// -- Default value helpers ---------------------------------------------

fn default_lead_5m() -> u32 {
    30
}
fn default_lead_1h() -> u32 {
    300
}
fn default_max_refreshes() -> u32 {
    12
}
fn default_max_duration() -> u64 {
    14400
}
fn default_snapshot_max_bytes() -> u32 {
    524_288
}
fn default_last_n_messages() -> usize {
    4
}
fn default_judge_max_tokens() -> u32 {
    128
}
fn default_judge_temperature() -> f32 {
    0.0
}
fn default_judge_timeout() -> u32 {
    5
}

// -- Convenience -------------------------------------------------------

impl CacheKeepaliveConfig {
    /// Refresh delay (seconds) to schedule given a cache TTL class.
    ///
    /// Callers pass the TTL parsed from the request's `cache_control`
    /// block (`"5m"` or `"1h"`). The returned value is `TTL_secs -
    /// lead_time`, saturating at 1 s so the timer always fires strictly
    /// before TTL expiry.
    pub fn refresh_delay_secs(&self, ttl: CacheTtl) -> u64 {
        let (ttl_secs, lead) = match ttl {
            CacheTtl::Ttl5m => (300u64, u64::from(self.refresh_lead_time_5m_secs)),
            CacheTtl::Ttl1h => (3600u64, u64::from(self.refresh_lead_time_1h_secs)),
        };
        ttl_secs.saturating_sub(lead).max(1)
    }
}

/// Cache TTL class extracted from a request's `cache_control` block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTtl {
    /// Default 5-minute ephemeral cache.
    Ttl5m,
    /// Extended 1-hour cache (requires `ttl: "1h"` on the breakpoint).
    Ttl1h,
}

impl CacheTtl {
    /// Parse the `ttl` string from a `cache_control` block. Accepts the
    /// two documented values; anything else — including absent `ttl` — is
    /// treated as the default 5-minute cache per Anthropic's 2026-03-06
    /// default change.
    pub fn from_ttl_str(ttl: Option<&str>) -> Self {
        match ttl {
            Some("1h") => Self::Ttl1h,
            _ => Self::Ttl5m,
        }
    }

    pub fn as_secs(self) -> u64 {
        match self {
            Self::Ttl5m => 300,
            Self::Ttl1h => 3600,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_round_trip_via_json() {
        // Storage stores this as JSON. Ensure a minimal payload
        // deserializes and picks up every default.
        let json = r#"{"enabled": true}"#;
        let cfg: CacheKeepaliveConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.refresh_lead_time_5m_secs, 30);
        assert_eq!(cfg.refresh_lead_time_1h_secs, 300);
        assert_eq!(cfg.max_refreshes_per_session, 12);
        assert_eq!(cfg.max_total_duration_secs, 14400);
        assert_eq!(cfg.snapshot_max_bytes, 524_288);
        assert!(cfg.classifier.extra_wait_for_user_tools.is_empty());
        assert!(!cfg.classifier.treat_end_turn_as_ambiguous);
        assert!(cfg.classifier.llm_judge.is_none());
    }

    #[test]
    fn refresh_delay_uses_configured_lead_time() {
        let cfg = CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 30,
            refresh_lead_time_1h_secs: 300,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14400,
            snapshot_max_bytes: 524_288,
            classifier: ClassifierConfig::default(),
        };
        assert_eq!(cfg.refresh_delay_secs(CacheTtl::Ttl5m), 270);
        assert_eq!(cfg.refresh_delay_secs(CacheTtl::Ttl1h), 3300);
    }

    #[test]
    fn refresh_delay_saturates_at_one_second() {
        let cfg = CacheKeepaliveConfig {
            enabled: true,
            refresh_lead_time_5m_secs: 400, // absurdly large
            refresh_lead_time_1h_secs: 4000,
            max_refreshes_per_session: 12,
            max_total_duration_secs: 14400,
            snapshot_max_bytes: 524_288,
            classifier: ClassifierConfig::default(),
        };
        assert_eq!(cfg.refresh_delay_secs(CacheTtl::Ttl5m), 1);
        assert_eq!(cfg.refresh_delay_secs(CacheTtl::Ttl1h), 1);
    }

    #[test]
    fn cache_ttl_defaults_to_5m_when_absent() {
        assert_eq!(CacheTtl::from_ttl_str(None), CacheTtl::Ttl5m);
        assert_eq!(CacheTtl::from_ttl_str(Some("garbage")), CacheTtl::Ttl5m);
        assert_eq!(CacheTtl::from_ttl_str(Some("5m")), CacheTtl::Ttl5m);
        assert_eq!(CacheTtl::from_ttl_str(Some("1h")), CacheTtl::Ttl1h);
    }

    #[test]
    fn llm_judge_config_serialises_response_format() {
        let cfg = LlmJudgeConfig {
            provider: "anthropic".into(),
            model: "claude-haiku-4-5".into(),
            api_key_secret_ref: "secret://principals/x/anthropic".into(),
            base_url: None,
            response_format: JudgeResponseFormat::JsonSpec,
            last_n_messages: 4,
            max_tokens: 128,
            temperature: 0.0,
            timeout_secs: 5,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"response_format\":\"json_spec\""));
    }
}
