use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub trait AuditSink: Send + Sync {
    fn sink_audit(&self, entry: AuditEntry);
}

/// Filters a recent-first audit query before its limit is applied.
#[derive(Debug, Clone, Copy)]
pub enum AuditQueryScope<'a> {
    /// Include every audit entry in the requested time range.
    All,
    /// Include entries for one principal.
    Principal(&'a str),
    /// Include entries attributed to one actor identity.
    Actor {
        /// Actor identity authority.
        authority: &'a str,
        /// Actor identity subject.
        subject: &'a str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuditEntry {
    pub ts: u64,
    pub request_id: String,
    pub principal_id: String,
    pub route: String,
    pub upstream: String,
    pub model: Option<String>,
    pub status: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    pub duration_ms: u64,
    pub agent_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_violation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_authority: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AuditActorFields {
    pub actor: String,
    pub authority: String,
    pub subject: String,
    pub kind: String,
    pub email: Option<String>,
}

impl AuditActorFields {
    pub fn system(component: &str) -> Self {
        Self {
            actor: format!("system:{component}"),
            authority: "cc-lb".to_owned(),
            subject: component.to_owned(),
            kind: "system".to_owned(),
            email: None,
        }
    }
}
