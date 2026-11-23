//! Pure provider Service Provider Interface (SPI) for isolated provider adapters and registry composition.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod adapter;
mod ids;
mod registry;

pub use adapter::{AuthKindDescriptor, ErrorFormatRequest, ProviderAdapter};
pub use ids::{AuthKindId, AuthMode, ProviderId};
pub use registry::{ProviderRegistry, ProviderRegistryError};
