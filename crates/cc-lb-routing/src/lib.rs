//! Routing plugin contracts and their proxy-only request view.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod context;
mod traits;
mod types;

pub use context::RoutingContext;
pub use traits::{FilterError, FilterOutput, FilterPlugin};
pub use types::{PerCandidateReason, RouteDecision};

#[cfg(test)]
mod tests {
    use super::RoutingContext;

    #[test]
    fn routing_context_is_exposed_for_routing_plugins() {
        let _ = core::mem::size_of::<RoutingContext>();
    }
}
