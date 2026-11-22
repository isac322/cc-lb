//! Wire types + versioning contract between cc-lb host and wasm plugins.
//!
//! Public surface for plugin authors: hook wire types defined per version,
//! schema metadata, and provider-qualified entrypoint helpers.
//!
//! - Router (`filter`): [`v3`] (`WireVersion::V3`)
//! - Shape family (`shape`, `transform_response`, `transform_sse_event`,
//!   `transform_realtime_frame`, `shape_envelope_gemini`): [`v2`] (`WireVersion::V2`)
//!
//! See `docs/plugin-author-guide.md` and `docs/rfc/0001-plugin-runtime-vnext.md`
//! for the versioning + admission-gate contract.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "std")]
pub mod metadata;
pub mod schema;
pub mod v2;
pub mod v3;
#[cfg(feature = "std")]
pub use metadata::{HookMetadata, MetadataError, PluginMetadata};
pub use schema::*;
