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

    /// Extract the original thread identifier from a serialized session key.
    ///
    /// Session keys are intentionally parsed strictly: prefix keys and malformed
    /// or non-reversible values do not identify a request thread.
    pub fn extract_thread_id(serialized: &str) -> Option<String> {
        let (principal, remainder) = serialized.split_once('/')?;
        Uuid::parse_str(principal).ok()?;
        if principal.len() != 36 || principal.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return None;
        }
        let raw_thread = remainder.strip_prefix("thread:")?;
        let (raw_thread, raw_agent) = match raw_thread.split_once("/agent:") {
            Some((thread, agent)) => (thread, Some(agent)),
            None => (raw_thread, None),
        };
        if raw_thread.is_empty()
            || !valid_escaped_component(raw_thread)
            || raw_agent.is_some_and(|agent| agent.is_empty() || !valid_escaped_component(agent))
        {
            return None;
        }
        let mut parts = raw_thread.split('%');
        let mut thread_id = String::with_capacity(raw_thread.len());
        thread_id.push_str(parts.next()?);
        for part in parts {
            if let Some(rest) = part.strip_prefix("25") {
                thread_id.push('%');
                thread_id.push_str(rest);
            } else {
                thread_id.push('/');
                thread_id.push_str(part.strip_prefix("2F")?);
            }
        }
        Some(thread_id)
    }
}

fn valid_escaped_component(value: &str) -> bool {
    !value.contains('/')
        && value
            .split('%')
            .skip(1)
            .all(|part| part.starts_with("25") || part.starts_with("2F"))
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

    #[test]
    fn extract_thread_id_decodes_one_reversible_layer_and_ignores_agent_scope() {
        let principal = Uuid::from_u128(1);
        let key = SessionKey::from_thread_and_agent(principal, "thread/é%2F", "agent/一");
        assert_eq!(
            SessionKey::extract_thread_id(&key.to_string()).as_deref(),
            Some("thread/é%2F")
        );
    }

    #[test]
    fn extract_thread_id_rejects_prefix_and_malformed_keys() {
        let principal = Uuid::from_u128(1);
        assert_eq!(
            SessionKey::extract_thread_id(
                &SessionKey::from_cache_prefix_hash(principal, "prefix").to_string()
            ),
            None
        );
        for suffix in [
            "thread:",
            "thread:bad%2",
            "thread:bad%",
            "thread:bad%20escape",
            "thread:bad%2fescape",
            "thread:raw/slash",
            "thread:thread/agent:",
            "thread:thread/agent:bad%2",
            "thread:thread/agent:agent/agent:other",
            "thread:thread/unrecognized:scope",
        ] {
            assert_eq!(
                SessionKey::extract_thread_id(&format!("{principal}/{suffix}")),
                None,
                "{suffix}"
            );
        }
        for invalid_principal in [
            "not-a-uuid",
            "00000000000000000000000000000001",
            "00000000-0000-0000-0000-00000000000A",
        ] {
            assert_eq!(
                SessionKey::extract_thread_id(&format!("{invalid_principal}/thread:thread")),
                None,
                "{invalid_principal}"
            );
        }
    }
}
