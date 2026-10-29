use std::fmt;

use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SessionKey {
    pub principal_id: Uuid,
    pub session_id: SessionId,
    pub agent_scope: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SessionId {
    ThreadId(String),
    CachePrefixHash(String),
}

impl SessionKey {
    pub fn from_thread_id(principal_id: Uuid, thread_id: impl Into<String>) -> Self {
        Self {
            principal_id,
            session_id: SessionId::ThreadId(thread_id.into()),
            agent_scope: None,
        }
    }

    pub fn from_thread_and_agent(
        principal_id: Uuid,
        thread_id: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self {
        Self::from_thread_id(principal_id, thread_id).with_agent_scope(Some(agent_id.into()))
    }

    pub fn normalize_agent_id(raw: Option<&str>) -> Option<&str> {
        let trimmed = raw?.trim();
        (!trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("main")).then_some(trimmed)
    }

    pub fn from_cache_prefix_hash(principal_id: Uuid, prefix_hash: impl Into<String>) -> Self {
        Self {
            principal_id,
            session_id: SessionId::CachePrefixHash(prefix_hash.into()),
            agent_scope: None,
        }
    }

    pub fn with_agent_scope(mut self, agent_scope: Option<String>) -> Self {
        self.agent_scope = Self::normalize_agent_id(agent_scope.as_deref()).map(ToOwned::to_owned);
        self
    }
}

fn write_escaped_component(f: &mut fmt::Formatter<'_>, value: &str) -> fmt::Result {
    let mut start = 0;
    for (idx, ch) in value.char_indices() {
        let replacement = match ch {
            '%' => "%25",
            '/' => "%2F",
            _ => continue,
        };
        if start < idx {
            f.write_str(&value[start..idx])?;
        }
        f.write_str(replacement)?;
        start = idx + ch.len_utf8();
    }
    if start < value.len() {
        f.write_str(&value[start..])?;
    }
    Ok(())
}

impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind_tag, raw_id) = match &self.session_id {
            SessionId::ThreadId(id) => ("thread", id.as_str()),
            SessionId::CachePrefixHash(hash) => ("prefix", hash.as_str()),
        };
        write!(f, "{}/{kind_tag}:", self.principal_id)?;
        write_escaped_component(f, raw_id)?;
        if let Some(scope) = self.agent_scope.as_deref() {
            f.write_str("/agent:")?;
            write_escaped_component(f, scope)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_escapes_delimiters_to_prevent_scope_collisions() {
        let principal_id = Uuid::nil();
        let unscoped_with_delimiter =
            SessionKey::from_thread_id(principal_id, "a/agent:b").to_string();
        let scoped = SessionKey::from_thread_id(principal_id, "a")
            .with_agent_scope(Some("b".to_owned()))
            .to_string();
        let pre_escaped = SessionKey::from_thread_id(principal_id, "a%2Fagent:b").to_string();

        assert_eq!(
            unscoped_with_delimiter,
            format!("{principal_id}/thread:a%2Fagent:b")
        );
        assert_eq!(scoped, format!("{principal_id}/thread:a/agent:b"));
        assert_eq!(pre_escaped, format!("{principal_id}/thread:a%252Fagent:b"));
        assert_ne!(unscoped_with_delimiter, scoped);
        assert_ne!(unscoped_with_delimiter, pre_escaped);
    }

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
            "00000000-0000-0000-0000-000000000001/thread:thread-a/agent:agent-a"
        );
    }
}
