//! Object-safe plugin traits for each proxy lifecycle boundary.

use crate::errors::ObservabilityError;
use crate::types::ObserveEvent;

/// Non-blocking observability hook boundary.
pub trait ObservabilityHook: Send + Sync {
    /// Observes a lifecycle event.
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError>;
}
