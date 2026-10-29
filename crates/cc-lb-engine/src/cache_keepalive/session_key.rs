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

    pub fn from_cache_prefix_hash(principal_id: Uuid, prefix_hash: impl Into<String>) -> Self {
        Self {
            principal_id,
            session_id: SessionId::CachePrefixHash(prefix_hash.into()),
            agent_scope: None,
        }
    }

    pub fn with_agent_scope(mut self, agent_scope: Option<String>) -> Self {
        self.agent_scope = agent_scope.filter(|scope| !scope.is_empty());
        self
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.session_id, &self.agent_scope) {
            (SessionId::ThreadId(id), None) => write!(f, "{}/thread:{}", self.principal_id, id),
            (SessionId::CachePrefixHash(hash), None) => {
                write!(f, "{}/prefix:{}", self.principal_id, hash)
            }
            (SessionId::ThreadId(id), Some(scope)) => {
                write!(f, "{}/thread:{}/agent:{}", self.principal_id, id, scope)
            }
            (SessionId::CachePrefixHash(hash), Some(scope)) => {
                write!(f, "{}/prefix:{}/agent:{}", self.principal_id, hash, scope)
            }
        }
    }
}
