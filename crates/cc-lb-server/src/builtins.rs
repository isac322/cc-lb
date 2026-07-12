use cc_lb_observability::{ObservabilityError, ObservabilityHook, ObserveEvent};

#[derive(Clone)]
pub struct NoopObservabilityHook;

impl ObservabilityHook for NoopObservabilityHook {
    fn observe(&self, _event: ObserveEvent) -> Result<(), ObservabilityError> {
        Ok(())
    }
}
