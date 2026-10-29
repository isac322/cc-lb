use std::fmt;

use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SessionKey {
    pub principal_id: Uuid,
    pub session_id: SessionId,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SessionId {
    ThreadId(String),
    ThreadAgent { thread_id: String, agent_id: String },
    CachePrefixHash(String),
}

impl SessionKey {
    pub fn from_thread_id(principal_id: Uuid, thread_id: impl Into<String>) -> Self {
        Self {
            principal_id,
            session_id: SessionId::ThreadId(thread_id.into()),
        }
    }
    pub fn from_thread_and_agent(
        principal_id: Uuid,
        thread_id: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self {
        Self {
            principal_id,
            session_id: SessionId::ThreadAgent {
                thread_id: thread_id.into(),
                agent_id: agent_id.into(),
            },
        }
    }

    pub fn normalize_agent_id(raw: Option<&str>) -> Option<&str> {
        let trimmed = raw?.trim();
        (!trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("main")).then_some(trimmed)
    }

    pub fn from_cache_prefix_hash(principal_id: Uuid, prefix_hash: impl Into<String>) -> Self {
        Self {
            principal_id,
            session_id: SessionId::CachePrefixHash(prefix_hash.into()),
        }
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.session_id {
            SessionId::ThreadId(id) => write!(f, "{}/thread:{}", self.principal_id, id),
            SessionId::ThreadAgent {
                thread_id,
                agent_id,
            } => write!(
                f,
                "{}/thread_agent:{}:{}:{}",
                self.principal_id,
                thread_id.len(),
                thread_id,
                agent_id
            ),
            SessionId::CachePrefixHash(hash) => {
                write!(f, "{}/prefix:{}", self.principal_id, hash)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_agent_id_ignores_main_and_blank_values() {
        assert_eq!(SessionKey::normalize_agent_id(None), None);
        assert_eq!(SessionKey::normalize_agent_id(Some("")), None);
        assert_eq!(SessionKey::normalize_agent_id(Some("  ")), None);
        assert_eq!(SessionKey::normalize_agent_id(Some("main")), None);
        assert_eq!(SessionKey::normalize_agent_id(Some("MAIN")), None);
        assert_eq!(
            SessionKey::normalize_agent_id(Some("subagent-1")),
            Some("subagent-1")
        );
    }

    #[test]
    fn session_key_display_includes_agent_identity() {
        let principal_id = Uuid::from_u128(1);
        let key = SessionKey::from_thread_and_agent(principal_id, "thread-a", "agent-a");
        assert_eq!(
            key.to_string(),
            "00000000-0000-0000-0000-000000000001/thread_agent:8:thread-a:agent-a"
        );
    }

    #[test]
    fn session_key_display_is_injective_for_delimiters() {
        let principal_id = Uuid::from_u128(1);
        let unscoped = SessionKey::from_thread_id(principal_id, "a/agent:b");
        let scoped = SessionKey::from_thread_and_agent(principal_id, "a", "b");
        assert_ne!(unscoped.to_string(), scoped.to_string());

        let colon_in_thread = SessionKey::from_thread_and_agent(principal_id, "a:1", "b");
        let colon_in_agent = SessionKey::from_thread_and_agent(principal_id, "a", "1:b");
        assert_ne!(colon_in_thread.to_string(), colon_in_agent.to_string());
    }
}
