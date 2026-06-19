use std::collections::HashSet;

use crate::error::SchedulerError;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EntityJobKind {
    Warmup,
    OAuthRefresh,
    OAuthUsagePoll,
    AnthropicCompatRefresh,
    MetadataRefresh,
}

#[derive(Debug, Default)]
pub struct EntityHandlerRegistry {
    registered: HashSet<EntityJobKind>,
}

impl EntityHandlerRegistry {
    pub fn register(&mut self, kind: EntityJobKind) -> Result<(), SchedulerError> {
        if !self.registered.insert(kind) {
            return Err(SchedulerError::Job(format!(
                "duplicate entity handler registration for {kind:?}"
            )));
        }
        Ok(())
    }

    pub fn register_all_entity_handlers() -> Result<Self, SchedulerError> {
        let mut registry = Self::default();
        registry.register(EntityJobKind::Warmup)?;
        registry.register(EntityJobKind::OAuthRefresh)?;
        registry.register(EntityJobKind::OAuthUsagePoll)?;
        registry.register(EntityJobKind::AnthropicCompatRefresh)?;
        registry.register(EntityJobKind::MetadataRefresh)?;
        Ok(registry)
    }
}
