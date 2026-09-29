use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Authenticated caller identity used for quota, audit, and routing decisions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    /// Stable principal identifier, unique within the proxy deployment.
    pub id: String,
    /// Principal category inferred by the authentication plugin.
    pub kind: PrincipalKind,
}

/// Principal categories supported by first-party and custom auth plugins.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// Principal authenticated by an Anthropic-compatible API key.
    ApiKey,
    /// Principal authenticated as an OAuth subject.
    OAuthSubject,
    /// Principal authenticated by an internal key managed by cc-lb.
    InternalKey,
}

/// Compact principal category stored with managed API keys and lifecycle events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKindLite {
    /// Human-operated principal.
    Human,
    /// Machine-operated principal.
    #[default]
    Machine,
}

/// Stable identity of one running cc-lb replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaIdentity {
    /// Replica UUID persisted across process restarts.
    pub id: Uuid,
}
