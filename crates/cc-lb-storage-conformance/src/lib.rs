#![allow(deprecated)]

pub mod harness;
pub mod plugin_registry;
pub mod plugin_registry_store;
#[cfg(test)]
mod principal_store;
pub mod scenarios;
#[cfg(test)]
mod upstream_store;

pub use harness::{ConformanceBackend, ConformanceFixture, scenario_applies_to_backend};
pub use plugin_registry as plugin_registry_scenarios;
pub use plugin_registry_store as plugin_registry_store_scenarios;
pub use scenarios::principal_store as principal_store_scenarios;
pub use scenarios::upstream_store as upstream_store_scenarios;
